# HFT Arena

[![CI](https://github.com/ashokCh-dev/hft-arena/actions/workflows/ci.yml/badge.svg)](https://github.com/ashokCh-dev/hft-arena/actions/workflows/ci.yml)

**Distributed Benchmarking & Hosting Platform** for contestant-submitted trading
infrastructure — IICPC Summer Hackathon 2026.

We upload a matching engine; HFT Arena securely containerizes it,
unleashes a distributed fleet of trading bots, measures latency / throughput /
correctness, and streams a live ranked leaderboard.

```
Code Upload → Containerized Sandbox → Distributed Load Test → Real-Time Scoring
```

## Quick start

```bash
cd hft-arena
make up                      # build + start redis, timescaledb, orchestrator, telemetry, bot_fleet
# open http://localhost:8000

make demo                    # submit the Python reference engine and run a load test
./scripts/demo.sh cpp 500 30 # or: submit the C++ engine, 500 bots, 30s
make scale                   # scale the load generator to 3 coordinated workers
TRANSPORT=kafka make up      # run the metrics firehose over Redpanda/Kafka instead of Redis
CONTROL=grpc make up         # run the orchestrator<->fleet control plane over gRPC instead of Redis
WIRE=binary make up          # packed-struct binary wire (~2x lower p50) instead of JSON
make adversarial             # Chaos & Resilience — submission chaos: cheating, mem-bomb, crasher, bursty
make chaos                   # Chaos & Resilience — platform chaos: kill telemetry mid-run, prove self-heal
make down                    # stop everything
# scaling proof: 1->3 bot workers took peak load ~91k -> ~180k ord/s (finding the
# C++ engine's true ceiling) while the latency curve's offered axis stayed correct.
```

On the dashboard you can also paste an engine, pick the language, set bots /
duration, and hit **Deploy & Attack** — or click **Load Python reference** first.

## What's inside

| Path | Component |
|---|---|
| `orchestrator/` | FastAPI control plane: submissions, runs, sandboxing, leaderboard WS, dashboard. Set `ARENA_API_KEY` to require `X-API-Key` on submit/run/stop (reads stay public) |
| `orchestrator/sandbox.py` | all container isolation policy (CPU pin, mem cap, cap-drop, read-only…) |
| `reference_engine_cpp/` | C++ engine over `crow.h` (seeded from hft_arena's `contestant.cpp`) |
| `reference_engine_rust/` | Rust engine (tokio + tungstenite) price-time book |
| `reference_engine_go/` | Go engine (gorilla/websocket) price-time book |
| `reference_engine_py/` | Python price-time book (WS-JSON) — demo + correctness oracle |
| `bot_fleet/` | asyncio pipelined load generator + correctness probe; scalable replicas |
| `telemetry/` | Redis-Streams ingester: p50/p90/p99, TPS, correctness, score; **persists runs to TimescaleDB** + a `runs_rollup` **continuous aggregate** for percentile/jitter trends (`/trends`) |
| `submission_templates/` | per-language sandbox build templates: **cpp, rust, go, python** |
| `docker-compose.yml` | **Infrastructure-as-Code** (Swarm-compatible `deploy.replicas`) |
| `k8s/` | Kubernetes manifests (kustomize): Deployments, Services, RBAC, bot-fleet **HPA** |
| `terraform/` | cloud cluster skeleton (GKE + autoscaling node pool) |
| `docs/ARCHITECTURE.md` | architecture blueprint (microservices, protocols, scoring, isolation) |
| `docs/adr/` | Architecture Decision Records — the *why* behind the key decisions |
| `tests/` | **Chaos & Resilience** — submission chaos (`adversarial.sh`) + platform chaos (`chaos.sh`) |

## Submission contract

Your engine listens on **WebSocket port 9000** and speaks JSON:

```
in : {"t":"limit","id":1,"side":"buy","px":15000,"qty":5,"ts":<ns>}
     {"t":"market","id":2,"side":"sell","qty":3}
     {"t":"cancel","id":3,"target":1}
out: {"ack":1,"ts":<ns>}                                 # sent first; latency target
     {"fill":1,"px":15000,"qty":5,"maker":42}
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full design, scoring
formula, and roadmap.

## Verified end-to-end

Both reference engines run through the complete pipeline with the sandbox
enforcing 2 pinned CPUs / 512 MB / 256 pids / read-only / all-caps-dropped.
Each run does an **open-loop offered-load sweep** (5k→80k ord/s, clean book) for
the latency-vs-load curve, then a **bursty microburst sweep** (same mean rates,
delivered in bursts) that exposes tail/jitter a steady sweep hides, then a
**closed-loop** phase for peak TPS. Latency is scored at a fixed reference load
(10k ord/s) so engines compare apples-to-apples:

| Submission | Peak TPS | Sustains | p50 @10k | p99 @10k | Correctness | Score |
|---|---|---|---|---|---|---|
| cpp-engine | ~88k | 80k ord/s | **0.8 ms** | 2.2 ms | 1.00 | **762** |
| python-engine | ~40k | 20k ord/s | 2.2 ms | 23 ms | 0.90 | 387 |

C++ wins decisively on throughput **and** latency. The sweep also caught a real
bug: crow.h had **Nagle's algorithm** on (25–40 ms latency at low rates) — we
patched `SocketAdaptor` to set `TCP_NODELAY`. The dashboard plots each engine's
latency-vs-load curve live; the saturation knee is visible at the high end.
