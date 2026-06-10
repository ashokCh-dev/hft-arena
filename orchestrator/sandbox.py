"""Submission & Sandboxing Engine.

Builds a Docker image from a per-language template around a contestant's source,
then runs it as a strictly isolated container. All isolation policy lives here so
the security story is auditable in one place.
"""
import io
import os
import shutil
import socket
import tarfile
import tempfile
import time

import docker

TEMPLATES = os.path.join(os.path.dirname(__file__), "submission_templates")
ENTRY = {"python": "engine.py", "cpp": "engine.cpp",
         "go": "engine.go", "rust": "main.rs"}

# Resource limits (CPU pinning + hard memory cap + pid cap) — fair, contained.
SANDBOX_CPUS = float(os.environ.get("SANDBOX_CPUS", "2.0"))
SANDBOX_MEMORY = os.environ.get("SANDBOX_MEMORY", "512m")
SANDBOX_CPUSET = os.environ.get("SANDBOX_CPUSET", "0,1")
SANDBOX_PIDS = int(os.environ.get("SANDBOX_PIDS", "256"))
ARENA_NETWORK = os.environ.get("ARENA_NETWORK", "arena_net")

_client = docker.from_env()


def container_name(submission_id: str) -> str:
    return f"arena-sub-{submission_id}"


def target_host(submission_id: str) -> str:
    """Where the bot fleet reaches this submission (container DNS name on arena_net)."""
    return container_name(submission_id)


def image_tag(submission_id: str) -> str:
    return f"arena-sub-{submission_id}:latest"


def build_image(submission_id: str, language: str, code: str) -> str:
    """Assemble a build context from the language template + submitted source."""
    if language not in ENTRY:
        raise ValueError(f"unsupported language: {language}")
    tmpl = os.path.join(TEMPLATES, language)
    ctx = tempfile.mkdtemp(prefix=f"build-{submission_id}-")
    try:
        # Copy template (Dockerfile + provided libs like crow.h), then the source.
        for fn in os.listdir(tmpl):
            shutil.copy(os.path.join(tmpl, fn), os.path.join(ctx, fn))
        with open(os.path.join(ctx, ENTRY[language]), "w") as f:
            f.write(code)
        tag = image_tag(submission_id)
        image, logs = _client.images.build(path=ctx, tag=tag, rm=True, forcerm=True)
        return tag
    finally:
        shutil.rmtree(ctx, ignore_errors=True)


def launch(submission_id: str, language: str = None):
    """Run the built image with strict isolation. Returns the container."""
    name = container_name(submission_id)
    _remove_if_exists(name)
    return _client.containers.run(
        image_tag(submission_id),
        name=name,
        detach=True,
        network=ARENA_NETWORK,
        # --- isolation / fairness ---
        nano_cpus=int(SANDBOX_CPUS * 1e9),   # --cpus
        cpuset_cpus=SANDBOX_CPUSET,          # CPU pinning
        mem_limit=SANDBOX_MEMORY,            # hard memory cap
        memswap_limit=SANDBOX_MEMORY,        # no swap escape
        pids_limit=SANDBOX_PIDS,             # fork-bomb guard
        cap_drop=["ALL"],                    # drop all Linux capabilities
        security_opt=["no-new-privileges:true"],
        read_only=True,                      # immutable rootfs
        tmpfs={"/tmp": "size=16m"},
        restart_policy={"Name": "no"},
        labels={"arena": "submission", "submission_id": submission_id},
    )


def wait_healthy(submission_id: str, port: int = 9000, timeout: float = 40.0) -> bool:
    """Poll the engine's WS port from the arena network until it accepts TCP."""
    name = container_name(submission_id)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((name, port), timeout=1.0):
                return True
        except OSError:
            time.sleep(0.3)
    return False


def stop(submission_id: str):
    _remove_if_exists(container_name(submission_id))


def logs(submission_id: str, tail: int = 40) -> str:
    try:
        return _client.containers.get(container_name(submission_id)).logs(tail=tail).decode()
    except docker.errors.NotFound:
        return ""


def _remove_if_exists(name: str):
    try:
        c = _client.containers.get(name)
        c.remove(force=True)
    except docker.errors.NotFound:
        pass
