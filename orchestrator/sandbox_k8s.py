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

# Language -> prebuilt reference image loaded into the cluster.
IMAGE_FOR = {
    "python": os.environ.get("REF_IMAGE_PY", "arena-ref-py:latest"),
    "cpp": os.environ.get("REF_IMAGE_CPP", "arena-ref-cpp:latest"),
}

try:
    config.load_incluster_config()
except Exception:
    config.load_kube_config()
_v1 = client.CoreV1Api()

_image = {}        # submission_id -> image
_ip = {}           # submission_id -> pod IP (after wait_healthy)


def pod_name(submission_id: str) -> str:
    return f"arena-sub-{submission_id}"


def build_image(submission_id: str, language: str, code: str) -> str:
    """K8s mode: map language to a prebuilt reference image (source-build = TODO)."""
    if language not in IMAGE_FOR:
        raise ValueError(f"unsupported language for k8s backend: {language}")
    img = IMAGE_FOR[language]
    _image[submission_id] = img
    return img


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
