"""Submission & Sandboxing Engine — Kubernetes backend.

Mirrors sandbox.py's interface, but provisions each submission as an isolated
Pod via the Kubernetes API (instead of `docker run` via the socket). The same
isolation guarantees are expressed as pod spec: resource limits + a hardened
securityContext.

In-cluster source→image builds (Kaniko/BuildKit pushing to a registry) are the
next step; for now a language maps to a prebuilt reference image already present
in the cluster. The Docker backend still does real per-submission source builds.
"""
import os
import time

from kubernetes import client, config

NAMESPACE = os.environ.get("ARENA_NAMESPACE", "hft-arena")
ENGINE_PORT = 9000

TEMPLATES = os.path.join(os.path.dirname(__file__), "submission_templates")
ENTRY = {"python": "engine.py", "cpp": "engine.cpp"}

# In-cluster builds: Kaniko builds the uploaded source and pushes to this registry.
# REGISTRY_IP lets the build pod resolve the registry host (a Docker-network name
# CoreDNS can't resolve) via a hostAlias. If unset, fall back to a prebuilt image.
BUILD_REGISTRY = os.environ.get("BUILD_REGISTRY")        # e.g. arena-reg:5000
REGISTRY_IP = os.environ.get("REGISTRY_IP")              # e.g. 172.18.0.4
KANIKO_IMAGE = os.environ.get("KANIKO_IMAGE", "gcr.io/kaniko-project/executor:latest")

# Language -> prebuilt reference image (fallback when in-cluster build is off).
IMAGE_FOR = {
    "python": os.environ.get("REF_IMAGE_PY", "arena-ref-py:latest"),
    "cpp": os.environ.get("REF_IMAGE_CPP", "arena-ref-cpp:latest"),
}

try:
    config.load_incluster_config()
except Exception:
    config.load_kube_config()
_v1 = client.CoreV1Api()
_batch = client.BatchV1Api()

_image = {}        # submission_id -> image
_ip = {}           # submission_id -> pod IP (after wait_healthy)


def pod_name(submission_id: str) -> str:
    return f"arena-sub-{submission_id}"


def build_image(submission_id: str, language: str, code: str) -> str:
    """Build the uploaded source in-cluster with Kaniko, pushing to the registry.

    Falls back to a prebuilt reference image if BUILD_REGISTRY/REGISTRY_IP are unset.
    """
    if language not in ENTRY:
        raise ValueError(f"unsupported language for k8s backend: {language}")
    if not (BUILD_REGISTRY and REGISTRY_IP):
        img = IMAGE_FOR[language]            # prebuilt fallback
        _image[submission_id] = img
        return img
    img = _kaniko_build(submission_id, language, code)
    _image[submission_id] = img
    return img


def _kaniko_build(submission_id: str, language: str, code: str) -> str:
    """Assemble a ConfigMap build context + run a Kaniko Job that pushes the image."""
    cm_name = f"arena-ctx-{submission_id}"
    job_name = f"arena-build-{submission_id}"
    dest = f"{BUILD_REGISTRY}/arena-sub-{submission_id}:latest"

    # Build context: template files (Dockerfile, provided libs) + the submitted source.
    tmpl = os.path.join(TEMPLATES, language)
    data = {}
    for fn in os.listdir(tmpl):
        with open(os.path.join(tmpl, fn), "r", encoding="utf-8", errors="replace") as f:
            data[fn] = f.read()
    data[ENTRY[language]] = code

    _delete_cm(cm_name)
    _v1.create_namespaced_config_map(NAMESPACE, client.V1ConfigMap(
        metadata=client.V1ObjectMeta(name=cm_name, labels={"arena": "buildctx"}),
        data=data))

    _delete_job(job_name)
    _batch.create_namespaced_job(NAMESPACE, client.V1Job(
        metadata=client.V1ObjectMeta(name=job_name, labels={"arena": "build"}),
        spec=client.V1JobSpec(
            backoff_limit=0, ttl_seconds_after_finished=120,
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels={"arena": "build"}),
                spec=client.V1PodSpec(
                    restart_policy="Never",
                    # Build pod can't resolve the Docker-network registry name via
                    # CoreDNS — map it to the registry's cluster-network IP.
                    host_aliases=[client.V1HostAlias(
                        ip=REGISTRY_IP, hostnames=[BUILD_REGISTRY.split(":")[0]])],
                    # ConfigMap volumes expose files as symlinks, which Kaniko's COPY
                    # captures as dangling links. Dereference them into an emptyDir
                    # (cp -rL) so the build context has real files.
                    init_containers=[client.V1Container(
                        name="ctx-copy", image="busybox:1.36",
                        command=["sh", "-c", "cp -rL /cm/. /work/"],
                        volume_mounts=[
                            client.V1VolumeMount(name="cm", mount_path="/cm"),
                            client.V1VolumeMount(name="work", mount_path="/work")])],
                    containers=[client.V1Container(
                        name="kaniko", image=KANIKO_IMAGE,
                        args=["--dockerfile=Dockerfile", "--context=dir:///work",
                              f"--destination={dest}", "--insecure",
                              "--insecure-pull", "--skip-tls-verify"],
                        volume_mounts=[client.V1VolumeMount(
                            name="work", mount_path="/work")])],
                    volumes=[
                        client.V1Volume(name="cm", config_map=client.V1ConfigMapVolumeSource(
                            name=cm_name)),
                        client.V1Volume(name="work", empty_dir=client.V1EmptyDirVolumeSource())])))))

    if not _wait_job(job_name, timeout=300):
        log = _job_log(job_name)
        raise RuntimeError(f"kaniko build failed: {log[-400:]}")
    _delete_cm(cm_name)
    return dest


def _wait_job(job_name: str, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        j = _batch.read_namespaced_job(job_name, NAMESPACE)   # avoids jobs/status RBAC
        if j.status.succeeded:
            return True
        if j.status.failed:
            return False
        time.sleep(2)
    return False


def _job_log(job_name: str) -> str:
    try:
        pods = _v1.list_namespaced_pod(NAMESPACE, label_selector="arena=build")
        for p in pods.items:
            if p.metadata.name.startswith(job_name):
                return _v1.read_namespaced_pod_log(p.metadata.name, NAMESPACE)
    except client.ApiException:
        pass
    return ""


def _delete_cm(name: str):
    try:
        _v1.delete_namespaced_config_map(name, NAMESPACE)
    except client.ApiException:
        pass


def _delete_job(name: str):
    try:
        _batch.delete_namespaced_job(name, NAMESPACE, propagation_policy="Background")
    except client.ApiException:
        pass


def launch(submission_id: str, language: str = "python"):
    """Create an isolated submission Pod with the same guarantees as the docker backend."""
    name = pod_name(submission_id)
    _remove_if_exists(name)
    img = _image.get(submission_id) or IMAGE_FOR.get(language)
    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(
            name=name, labels={"arena": "submission", "submission_id": submission_id}),
        spec=client.V1PodSpec(
            restart_policy="Never",
            automount_service_account_token=False,
            # Pod-level hardening
            security_context=client.V1PodSecurityContext(
                run_as_non_root=True, run_as_user=65532,
                seccomp_profile=client.V1SeccompProfile(type="RuntimeDefault")),
            containers=[client.V1Container(
                name="engine", image=img, image_pull_policy="IfNotPresent",
                ports=[client.V1ContainerPort(container_port=ENGINE_PORT)],
                resources=client.V1ResourceRequirements(
                    requests={"cpu": "500m", "memory": "128Mi"},
                    limits={"cpu": "2", "memory": "512Mi"}),   # CPU + hard mem cap
                security_context=client.V1SecurityContext(
                    read_only_root_filesystem=True,
                    allow_privilege_escalation=False,
                    capabilities=client.V1Capabilities(drop=["ALL"])),
                volume_mounts=[client.V1VolumeMount(name="tmp", mount_path="/tmp")],
            )],
            volumes=[client.V1Volume(
                name="tmp", empty_dir=client.V1EmptyDirVolumeSource(
                    medium="Memory", size_limit="16Mi"))],
        ))
    _v1.create_namespaced_pod(NAMESPACE, pod)


def wait_healthy(submission_id: str, port: int = ENGINE_PORT, timeout: float = 90.0) -> bool:
    """Wait until the Pod is Running with an IP and its container is ready."""
    name = pod_name(submission_id)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            p = _v1.read_namespaced_pod(name, NAMESPACE)
        except client.ApiException:
            time.sleep(0.5)
            continue
        ready = any(c.type == "Ready" and c.status == "True"
                    for c in (p.status.conditions or []))
        if p.status.phase == "Running" and p.status.pod_ip and ready:
            _ip[submission_id] = p.status.pod_ip
            return True
        if p.status.phase in ("Failed", "Succeeded"):
            return False
        time.sleep(0.5)
    return False


def target_host(submission_id: str) -> str:
    """Where the bot fleet reaches this submission (Pod IP, cluster-internal)."""
    return _ip.get(submission_id, pod_name(submission_id))


def stop(submission_id: str):
    _remove_if_exists(pod_name(submission_id))
    _ip.pop(submission_id, None)


def logs(submission_id: str, tail: int = 40) -> str:
    try:
        return _v1.read_namespaced_pod_log(
            pod_name(submission_id), NAMESPACE, tail_lines=tail)
    except client.ApiException:
        return ""


def _remove_if_exists(name: str):
    try:
        _v1.delete_namespaced_pod(
            name, NAMESPACE, grace_period_seconds=0,
            body=client.V1DeleteOptions(grace_period_seconds=0))
    except client.ApiException:
        pass
