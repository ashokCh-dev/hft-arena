# HFT Arena on Kubernetes (Infrastructure-as-Code)

These manifests deploy the platform's **stateless services** to Kubernetes and
demonstrate horizontal scaling of the distributed load generator. They are the
cloud-native counterpart to the single-host `docker-compose.yml`.

```
kubectl apply -k k8s/                 # deploy everything (kustomize)
kubectl -n hft-arena get pods
kubectl -n hft-arena scale deploy/bot-fleet --replicas=8   # scale the load fleet
kubectl -n hft-arena port-forward svc/orchestrator 8000:8000
# open http://localhost:8000
```

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

## Sandbox provisioning on Kubernetes (design note)
On a single host the orchestrator sandboxes submissions via the Docker socket.
On Kubernetes that path is replaced by the **Kubernetes API**: the orchestrator
creates a short-lived `Job`/`Pod` per submission with the same isolation guarantees
expressed as pod spec —
`resources.limits` (CPU/memory), `securityContext`
(`runAsNonRoot`, `readOnlyRootFilesystem`, `allowPrivilegeEscalation: false`,
`capabilities.drop: [ALL]`), a restrictive `seccompProfile`, and a dedicated
`NetworkPolicy` so only the bot fleet can reach the submission. CPU pinning maps
to the `static` CPU manager policy / Guaranteed QoS. Building images moves to an
in-cluster builder (BuildKit/Kaniko) pushing to a registry. This is the next
implementation step; the manifests here cover the always-on platform tier.
See `../terraform/` for provisioning the cluster itself.
