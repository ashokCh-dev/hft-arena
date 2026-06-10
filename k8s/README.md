# HFT Arena on Kubernetes (Infrastructure-as-Code)

These manifests deploy the platform's **stateless services** to Kubernetes and
demonstrate horizontal scaling of the distributed load generator. They are the
cloud-native counterpart to the single-host `docker-compose.yml`.

### Verified locally on k3d
```bash
k3d cluster create arena
docker compose build                                   # build platform images
k3d image import hft-arena-orchestrator:latest hft-arena-telemetry:latest \
    hft-arena-bot_fleet:latest arena-ref-py:latest -c arena
kubectl apply -k k8s/                                  # deploy (kustomize)
kubectl -n hft-arena get pods                          # redis, orchestrator, telemetry, bot-fleet x2
kubectl -n hft-arena scale deploy/bot-fleet --replicas=8   # scale the load fleet
kubectl -n hft-arena port-forward svc/orchestrator 8000:8000   # open http://localhost:8000
```
A submit→run then creates an isolated **submission Pod via the K8s API**
(`SANDBOX_BACKEND=k8s`), which the in-cluster bot fleet bombards — verified
end-to-end (score, latency curve, auto-stop).

## What's here
| File | Resource |
|---|---|
| `namespace.yaml` | `hft-arena` namespace |
| `redis.yaml` | Redis Deployment + Service (metrics bus / state) |
| `telemetry.yaml` | Telemetry ingester Deployment |
| `bot-fleet.yaml` | Load generator Deployment + **HorizontalPodAutoscaler** |
| `orchestrator.yaml` | Orchestrator Deployment + Service (ClusterIP :8000) |
| `kustomization.yaml` | ties them together; pin image tags here |

The `bot_fleet` workers self-coordinate through a Redis barrier (each worker
offers `1/N` of the swept load), so scaling `replicas` raises aggregate
throughput **without** distorting the latency-vs-load curve — the same mechanism
that works under `docker compose --scale bot_fleet=N`.

## Sandbox provisioning on Kubernetes (implemented)
On a single host the orchestrator sandboxes submissions via the Docker socket. On
Kubernetes (`SANDBOX_BACKEND=k8s`, see `orchestrator/sandbox_k8s.py`) it instead
creates a **Pod per submission via the Kubernetes API**, with the same isolation
expressed as pod spec: `resources.limits` (CPU/memory), `securityContext`
(`runAsNonRoot`, `runAsUser:65532`, `readOnlyRootFilesystem`,
`allowPrivilegeEscalation:false`, `capabilities.drop:[ALL]`,
`seccompProfile:RuntimeDefault`), and `automountServiceAccountToken:false`. The
bot fleet reaches it at the Pod IP. CPU pinning maps to the node's `static` CPU
manager policy / Guaranteed QoS (see `../terraform/`).

**In-cluster builds (implemented):** with `BUILD_REGISTRY` + `REGISTRY_IP` set, the
orchestrator builds the uploaded source **in-cluster with Kaniko** — it writes the
build context (template Dockerfile + source) to a ConfigMap, runs a Kaniko Job
(an init container dereferences the ConfigMap symlinks into an emptyDir first) that
pushes `arena-reg:5000/arena-sub-<id>` to the cluster registry, then runs the Pod
from that image. No Docker socket anywhere. Create the cluster with a registry:
`k3d cluster create arena --registry-create arena-reg:0.0.0.0:5111`, and set
`REGISTRY_IP` to the registry's cluster-network IP
(`docker inspect arena-reg -f '{{(index .NetworkSettings.Networks "k3d-arena").IPAddress}}'`).

**Network isolation (implemented):** `networkpolicy.yaml` locks down submission
pods — **deny-all egress** (no exfiltration / C2 / cluster scanning) and ingress
only from the bot fleet + orchestrator on :9000. Verified on k3s (which enforces
NetworkPolicy): a submission pod cannot reach the K8s API or the internet, an
unlabeled pod cannot reach the engine, but the fleet can.

See `../terraform/` for provisioning the cluster itself.
