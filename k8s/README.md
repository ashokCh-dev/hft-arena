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

**Remaining:** building the image from uploaded source moves to an in-cluster
builder (BuildKit/**Kaniko**) pushing to a registry; today the k8s backend runs a
prebuilt reference image. A `NetworkPolicy` restricting submission egress to only
the bot fleet is also a quick follow-up. See `../terraform/` for the cluster itself.
