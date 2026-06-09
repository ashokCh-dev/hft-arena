# HFT Arena — Architecture Blueprint

*Distributed Benchmarking & Hosting Platform for contestant-submitted trading
infrastructure. IICPC Summer Hackathon 2026.*

This document is **Deliverable 2**. It describes the microservices, their
inter-service protocols, data stores, the sandbox isolation strategy, and the
scoring model. The running prototype (Deliverable 1) and the
`docker-compose.yml` IaC (Deliverable 3) are in the repository root.

---

## 1. System overview

HFT Arena takes a contestant's matching-engine source, securely containerizes
it, bombards it with a distributed fleet of trading bots, measures latency /
throughput / correctness, and streams a live ranked leaderboard.

```
            ┌─────────────┐  POST /submissions,/runs        ┌──────────────┐
 Browser ──►│ orchestrator│◄─────────────────────────────────│  Dashboard   │
   WS  ◄────│  (FastAPI)  │  serves static index.html  ─────►│ (vanilla JS) │
            └──────┬──────┘                                   └──────────────┘
                   │ Docker SDK over /var/run/docker.sock
                   │ build image from template, run with strict limits
                   ▼
          ┌───────────────────┐  WS-JSON orders   ┌───────────────────────────┐
          │ submission sandbox│◄──────────────────│  bot_fleet (asyncio)      │
          │ contestant engine │  acks / fills ───►│  N pipelined bots / replica│
          │ (price-time book) │                   └─────────────┬─────────────┘
          └───────────────────┘   latency+corr deltas │ XADD arena:samples
                                                       ▼
                          ┌──────────┐  XREAD     ┌───────────────────────────┐
   PUBLISH arena:events◄──│  Redis   │◄───────────│ telemetry ingester        │
   (leaderboard snapshot) │ streams+ │  p50/90/99, TPS, correctness, score    │
                          │ pub/sub  │            └───────────────────────────┘
                          └──────────┘
```

The four required components map 1:1 to four services plus Redis.

| Required component | Service | Tech |
|---|---|---|
| Submission & Sandboxing Engine | `orchestrator` + `submission_templates/` | FastAPI, Docker SDK |
| Distributed Load Generator (Bot Fleet) | `bot_fleet` (scalable replicas) | asyncio + uvloop + websockets |
| Telemetry & Validation Ingester | `telemetry` | asyncio, Redis Streams |
| Real-Time Leaderboard & Analytics | `orchestrator` WS + `static/index.html` | Redis pub/sub, vanilla JS |
| Message/metrics bus + state | `redis` | Redis 7 (Streams, pub/sub, hashes) |

---

## 2. Services

### orchestrator (control plane)
- `POST /submissions {language, code, name}` → builds a Docker image from the
  per-language template around the uploaded source; returns `submission_id`.
- `POST /runs {submission_id, bots, duration}` → launches the sandbox with strict
  limits, waits for the WS port to accept TCP, then publishes a `start` command
  on `arena:control` and schedules an auto-stop.
- `POST /stop` → broadcasts `stop`.
- `GET /` → dashboard;  `WS /ws/leaderboard` → relays `arena:events` to browsers
  and replays the last snapshot to new clients.
- Blocking Docker builds run in a threadpool so the event loop stays responsive.

### submission sandbox (the contestant's engine)
- Built from `submission_templates/<lang>/Dockerfile`. The contestant's source is
  dropped in as the entrypoint (`engine.py` / `engine.cpp`); the platform
  provides the toolchain and standard libs (e.g. `crow.h` for C++).
- Speaks the **WS-JSON order contract** (§4) on port 9000.
- Two reference engines ship as demo submissions and correctness oracles:
  - `reference_engine_py` — asyncio `websockets` price-time book.
  - `reference_engine_cpp` — `crow.h` WebSocket + a `std::map`/`std::list`
    price-time book (seeded from hft_arena's `contestant.cpp`).

### bot_fleet (distributed load generator)
- Each replica subscribes to `arena:control`. On `start`, it runs each load in
  **two phases**:
  - **Phase 1 — latency sweep (open-loop):** a persistent bot pool paces arrivals
    through a series of offered rates (`SWEEP_RATES`, e.g. 5k→80k ord/s),
    independent of acks. This yields the **latency-vs-load curve**: the low-load
    points are clean service-time latency; where `achieved < offered` and latency
    explodes is the **saturation knee**. Closed-loop latency would instead be
    throughput-bound by Little's law (L = λ·W), so we never use it for latency.
    Runs **first**, on a near-empty book, so the measurement is fair across engines.
  - **Phase 2 — throughput (closed-loop):** bots pipeline orders up to an
    in-flight window, saturating the engine to find **peak TPS**. Runs last — its
    saturation explodes the order book, which no longer pollutes latency.
- Per bot, a *sender* emits orders (paced in the sweep) and a *receiver* matches
  acks back to send-times (interleaved fills drain naturally). Each sweep step has
  a settle sub-window (tagged `rate=0`, ignored) so step boundaries don't smear.
  Samples carry their phase + offered rate; telemetry bins latency per rate.
- Order mix: ~85% limit near mid, ~10% market, ~5% cancel.
- **Horizontal scaling (coordinated):** on run start every replica registers in a
  Redis set keyed on `run_id` (a barrier); each then offers `rate / N` of the
  swept load, so the **aggregate** offered load equals the tagged rate no matter
  how many replicas run. One replica is elected leader (the scenario-lock holder)
  to run the correctness probe and emit the run-done event. Measured: 1→3 replicas
  took peak load from ~91k to ~180k ord/s — enough to find the C++ engine's true
  ceiling that a single generator couldn't reach — with the latency curve's
  offered axis staying correct. Works identically under `docker compose
  --scale bot_fleet=N` and a Kubernetes Deployment with `replicas: N` + HPA.
- Latency is bucketed into a 256-bin log-spaced histogram; per-tick **deltas**
  (sent/acked/errors/correctness/histogram) are `XADD`-ed to `arena:samples`.
- Scale horizontally: `docker compose up --scale bot_fleet=3`.

### telemetry (ingester & validator)
- `XREAD`s `arena:samples`, aggregates per `run_id`, and every 500 ms publishes a
  ranked leaderboard snapshot to `arena:events` (and persists best-per-submission
  to a Redis hash).
- Computes p50/p90/p99 from the merged histogram, sliding-window + peak TPS,
  correctness, and the composite score (§5).

---

## 3. Inter-service communication & data stores

| Channel / store | Purpose | Producer → Consumer |
|---|---|---|
| `arena:control` (pub/sub) | run start/stop commands | orchestrator → bot_fleet, telemetry |
| `arena:samples` (stream) | latency/throughput/correctness deltas | bot_fleet → telemetry |
| `arena:events` (pub/sub) | live leaderboard snapshots + status | telemetry/orchestrator → dashboard |
| `arena:snapshot` (string) | last snapshot for new dashboard clients | telemetry → orchestrator |
| `arena:leaderboard` (hash) | best score per submission (persisted) | telemetry |
| `arena:scenario:<run>` (string, NX) | lock so one worker runs the correctness probe | bot_fleet |
| Docker socket | build + run + inspect sandboxes | orchestrator → host daemon |

**Why Redis (and not Kafka/gRPC) for the slice:** Redis Streams give us
consumer offsets and back-pressure for the metric firehose, pub/sub for fan-out,
and hashes for leaderboard state — one tiny container, zero schema overhead. The
sample format is already a stream of immutable delta records, so the swap to
**Redpanda/Kafka** (a topic per metric class) is a transport change, not a model
change. Service-to-service control could likewise move to **gRPC**. See §7.

---

## 4. Submission contract (WebSocket JSON, port 9000)

Bot → engine:
```
{"t":"limit","id":<u64>,"side":"buy"|"sell","px":<int>,"qty":<int>,"ts":<ns>}
{"t":"market","id":<u64>,"side":"buy"|"sell","qty":<int>}
{"t":"cancel","id":<u64>,"target":<resting_order_id>}
```
Engine → bot:
```
{"ack":<id>,"ts":<engine_ns>}                       # always, sent FIRST
{"fill":<id>,"px":<int>,"qty":<int>,"maker":<id>}   # 0+ per aggressive order
{"reject":<id>,"reason":"..."}
```
Latency is measured by the bot as `ack_receipt − send_time` (nanoseconds). The
ack must be sent before fills so latency reflects acknowledgement, not matching.

---

## 5. Scoring model

Per run, the composite score (×1000 for display) is:

```
score = 0.45 · throughput_norm
      + 0.35 · latency_score
      + 0.20 · correctness
      − crash_penalty

throughput_norm = min(1, peak_tps / TARGET_TPS)          # TARGET_TPS=100000
latency_score   = LAT_REF_US / (LAT_REF_US + p99_us)      # LAT_REF_US=2000
                                # p99 from the curve point at a FIXED REF_LOAD
                                # (=10k ord/s) so engines compare apples-to-apples;
                                # the full curve still shows each engine's own knee
correctness     = (corr_pass/corr_total) · (acked/sent)   # priority × reliability
crash_penalty   = 0.3 if reliability < 0.5 else 0
```

- **throughput_norm** rewards peak sustained TPS before failure.
- **latency_score** rewards low tail latency (p99).
- **correctness** = price-time-priority pass rate × order acknowledgement rate.
- **crash_penalty** punishes engines that drop a large fraction of orders.

**Correctness validation.** A probe runs on the *empty* book before load begins
(a Redis `NX` lock elects one fleet worker; the rest wait on a done-flag). It
rests two buys at one price, then aggresses a crossing sell, and asserts the
taker's fills are exactly `[(maker_A,5),(maker_B,1)]` — i.e. the older order
fills first (time priority) with correct fill quantities. A wrong engine fails
the assertion and loses correctness points. (Running on the clean book avoids
the shared-book contention that would otherwise let load orders intercept the
probe's aggressive order by price priority.)

---

## 6. Sandbox isolation strategy

All policy is centralized in `orchestrator/sandbox.py`. Every submission runs
with (verified via `docker inspect`):

| Control | Setting | Defends against |
|---|---|---|
| `--cpus` (NanoCpus) | 2.0 cores | CPU hogging / unfair advantage |
| `--cpuset-cpus` | pinned to `0,1` | noisy-neighbour jitter; fair, repeatable timing |
| `--memory` + `--memory-swap` | 512 MB, no swap | OOM blast radius, swap escape |
| `--pids-limit` | 256 | fork bombs |
| `--cap-drop ALL` | all Linux caps dropped | privilege abuse |
| `--security-opt no-new-privileges` | on | setuid escalation |
| `read_only` rootfs + tmpfs `/tmp` | on | tampering / persistence |
| non-root `USER runner` | in template | container-as-root risks |
| dedicated bridge `arena_net` | isolated | lateral movement; only the fleet reaches it |

The orchestrator drives the host Docker daemon via the mounted socket; build
contexts are uploaded to the daemon, so no shared host path is required.

---

## 7. Roadmap (next iteration / "tomorrow")

- **Transport:** swap Redis Streams → Redpanda/Kafka (topic per metric class);
  control plane → gRPC.
- **Persistence/analytics:** land samples in TimescaleDB for historical
  percentiles and per-run drill-down.
- **Offered-load sweep + latency-vs-load curve** (✅ implemented). Scoring latency
  at a fixed reference load makes engines comparable; the curve shows each one's
  saturation knee. **Finding:** the sweep immediately exposed that the C++ engine
  via crow.h had **Nagle's algorithm** enabled (25–40 ms latency at low message
  rates); we patched crow's `SocketAdaptor` to set `TCP_NODELAY` — a real
  low-latency defect the benchmark caught. Remaining overhead: the Python load
  generator still adds ms-scale scheduling jitter at the high end; next is a
  compiled / multi-process generator.
- **Scale-out & IaC** (✅ implemented). The bot fleet scales horizontally with
  coordinated 1/N load splitting (above). Three IaC layers ship: `docker-compose.yml`
  (single host, Swarm-compatible `deploy.replicas`), `k8s/` (kustomize: Deployments,
  Services, RBAC, and a bot-fleet **HPA**), and `terraform/` (GKE cluster + autoscaling
  node pool). Next: have the orchestrator provision sandboxes via the Kubernetes API
  (Job/Pod per submission with the same isolation as pod spec) instead of the Docker
  socket — RBAC for it is already in `k8s/orchestrator.yaml`.
- **Languages:** Rust/Go submission templates (stubs today).
- **Hardening:** gVisor/Firecracker microVMs, seccomp profiles, egress controls,
  per-submission build caching, auth.
