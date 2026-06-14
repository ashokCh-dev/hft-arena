# ADR 0004 — Container sandboxing: Docker socket + Kubernetes API backends

**Status:** Accepted

## Context
Submissions are untrusted, contestant-supplied code that must run isolated
("prevent malicious code execution"), and the platform must run both on a single
host (dev/demo) and on a cloud cluster (the IaC deliverable). We needed a sandbox
abstraction that enforces identical isolation either way.

## Decision
A pluggable sandbox backend (`SANDBOX_BACKEND=docker|k8s`), same guarantees both:
- **docker**: build a per-submission image from a per-language template and
  `docker run` it via the mounted socket; isolation as CLI flags.
- **k8s**: build the source **in-cluster with Kaniko** (ConfigMap context → Job →
  push to registry) and run an isolated **Pod per submission via the API**;
  isolation as pod spec.
Isolation policy (centralized in `orchestrator/sandbox*.py`): CPU pinning, 512 MB +
no-swap, pids cap, drop ALL capabilities, no-new-privileges, read-only rootfs +
tmpfs, non-root user, seccomp `RuntimeDefault`, and a **NetworkPolicy** (k8s) giving
submissions **deny-all egress** + fleet-only ingress.

## Consequences
- **+** One control plane runs unchanged on compose and real Kubernetes (verified
  end-to-end on k3d, including Kaniko in-cluster builds).
- **+** Defense-in-depth across host, resource, and network dimensions — verified by
  the adversarial suite (memory bomb OOM-contained, egress blocked).
- **−** Two sandbox implementations to maintain; the Docker socket is a powerful
  mount (orchestrator is trusted).
- **−** K8s source-build adds registry + Kaniko plumbing (mitigated: prebuilt-image
  fallback when the registry isn't configured).
