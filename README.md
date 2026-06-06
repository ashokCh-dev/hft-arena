# 🚀 HFT Arena

**Distributed Benchmarking & Hosting Platform** for contestant-submitted trading
infrastructure — IICPC Summer Hackathon 2026.

Contestants upload a matching engine; HFT Arena securely containerizes it,
unleashes a distributed fleet of trading bots, measures latency / throughput /
correctness, and streams a live ranked leaderboard.

```
Code Upload → Containerized Sandbox → Distributed Load Test → Real-Time Scoring
```

## Quick start

```bash
cd hft-arena
make up                      # build + start redis, orchestrator, telemetry, bot_fleet
# open http://localhost:8000

make demo                    # submit the Python reference engine and run a load test
./scripts/demo.sh cpp 500 30 # or: submit the C++ engine, 500 bots, 30s
make scale                   # prove the load generator is distributed (3 bot workers)
make down                    # stop everything
```

On the dashboard you can also paste an engine, pick the language, set bots /
duration, and hit **Deploy & Attack** — or click **Load Python reference** first.

## What's inside

| Path | Component |
|---|---|
| `orchestrator/` | FastAPI control plane: submissions, runs, sandboxing, leaderboard WS, dashboard |
| `orchestrator/sandbox.py` | all container isolation policy (CPU pin, mem cap, cap-drop, read-only…) |
| `reference_engine_py/` | Python price-time-priority order book (WS-JSON) — demo submission + oracle |
| `reference_engine_cpp/` | C++ engine over `crow.h` (seeded from hft_arena's `contestant.cpp`) |
| `bot_fleet/` | asyncio pipelined load generator + correctness probe; scalable replicas |
| `telemetry/` | Redis-Streams ingester: p50/p90/p99, TPS, correctness, composite score |
| `submission_templates/` | per-language sandbox Dockerfiles (cpp primary; python; go/rust to come) |
| `docker-compose.yml` | **Infrastructure-as-Code** (Swarm-compatible `deploy.replicas`) |
| `docs/ARCHITECTURE.md` | architecture blueprint (microservices, protocols, scoring, isolation) |

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
Each run is **two-phase**: closed-loop to find **peak TPS**, then open-loop
(paced below capacity) to measure **true latency** at equal offered load:

| Submission | Peak TPS | p50 (open-loop) | p99 (open-loop) | Correctness | Score |
|---|---|---|---|---|---|
| cpp-engine | ~72k | ~18 ms | ~23 ms | 1.00 | 531 |
| python-engine | ~47k | ~35 ms | ~70 ms | 0.99 | 412 |

Open-loop latency cleanly separates the engines at equal load (C++ ~2× faster
on both throughput and tail latency). Absolute latency is still inflated by the
Python load generator's own scheduling overhead — see the roadmap.
