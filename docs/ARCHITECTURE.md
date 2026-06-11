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

The four required components map 1:1 to four services plus Redis (hot path) and
TimescaleDB (durable run history).

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
- Four reference engines ship as demo submissions / correctness oracles, one per
  supported language — all implement the same WS-JSON price-time book:
  - `reference_engine_cpp` — `crow.h` WebSocket + `std::map`/`std::list` (seeded
    from hft_arena's `contestant.cpp`).
  - `reference_engine_rust` — tokio + tokio-tungstenite + `BTreeMap`/`VecDeque`.
  - `reference_engine_go` — gorilla/websocket + `container/list`.
  - `reference_engine_py` — asyncio `websockets` (also the correctness oracle).

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
| `arena:fleet:<run>` (set) | replica barrier for coordinated 1/N load splitting | bot_fleet |
| `arena:history` (string) | recent runs for the dashboard history panel | telemetry → orchestrator |
| Docker socket | build + run + inspect sandboxes | orchestrator → host daemon |

**Two data stores, by job:**
- **Redis** — coordination + state: pub/sub for control/events fan-out, hashes/sets
  for the live leaderboard and fleet coordination — one tiny container, zero schema
  overhead.
- **Metrics transport (pluggable, `TRANSPORT=redis|kafka`)** — the high-volume
  sample firehose (bot_fleet → telemetry) runs over **Redis Streams** *or*
  **Redpanda/Kafka** (topic `arena.samples`, telemetry consumer group), matching
  the blueprint's "Kafka/Redpanda for metrics." Both verified end-to-end; the
  record format is identical, so it's a true transport swap. Control could likewise
  move to **gRPC** (see §7).
- **TimescaleDB** — durable analytics. Telemetry writes each *completed* run (with
  its full latency-vs-load curve as JSONB) to a `runs` hypertable on the run-done
  event. On startup it recovers the best-per-submission leaderboard from the DB, so
  a telemetry/Redis restart isn't a blank board, and `/history` exposes recent runs.
  Persistence degrades gracefully (telemetry runs Redis-only if the DB is absent).

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
| dedicated bridge `arena_net` (docker) / `NetworkPolicy` (k8s) | network isolation | exfiltration, C2 callback, lateral movement — submission has deny-all egress and is reachable only by the fleet |

**Two interchangeable sandbox backends** (selected by `SANDBOX_BACKEND`), same
isolation either way:
- `docker` (`sandbox.py`) — builds a per-submission image from source and
  `docker run`s it via the mounted socket; isolation as CLI flags.
- `k8s` (`sandbox_k8s.py`) — creates an isolated **Pod per submission via the
  Kubernetes API**; isolation as pod spec (`resources.limits`, `securityContext`:
  `runAsNonRoot`, `readOnlyRootFilesystem`, `allowPrivilegeEscalation:false`,
  `capabilities.drop:[ALL]`, `seccompProfile:RuntimeDefault`, SA token off). The
  bot fleet reaches the submission at the Pod IP. **Verified end-to-end on a k3d
  cluster:** submit → Pod created via API → 2 coordinated fleet replicas load it →
  scored leaderboard → Pod auto-stopped. (In-cluster source→image builds via
  Kaniko are the remaining piece; today the k8s backend runs a prebuilt reference
  image.) RBAC for it is in `k8s/orchestrator.yaml`.

### Adversarial verification (`tests/adversarial.sh`)
The Sandboxing + Validation defenses are continuously tested against hostile
submissions, all verified green:

| Attack (`tests/adversarial/`) | Defense proven |
|---|---|
| `cheater.py` — fast engine that violates price-time priority (LIFO fills) | Validation probe catches it → correctness **0.0**, score collapses |
| `membomb.py` — allocates unbounded memory | `--memory` cap OOM-kills the container (`OOMKilled=true`); host + platform unaffected |
| `crasher.py` — hard-exits mid-load | platform stays up; disconnects become errors → crash penalty; run still scored/persisted |
| legit engine, run after the attacks | scores correctly (**0.99**) — no contamination |

> This suite caught a real bug: correctness was being transmitted to telemetry as
> *reliability only* (the price-time-priority result never reached the score, after
> the sweep refactor moved the probe ahead of the per-phase reporters). The leader
> now emits the probe's counts explicitly. A benchmark you don't attack is a
> benchmark you don't actually trust.

---

## 7. Roadmap (next iteration / "tomorrow")

- **Transport** (✅ Kafka done): the metrics firehose is pluggable Redis Streams ↔
  Redpanda/Kafka (`TRANSPORT=kafka`). Remaining: move the orchestrator↔fleet control
  plane from Redis pub/sub to **gRPC**.
- **Persistence/analytics** (✅ implemented): completed runs land in a TimescaleDB
  hypertable (with curve); leaderboard recovers on restart; `/history` lists recent
  runs. Shipped for **both** deployments — docker-compose and the k8s manifests
  (`k8s/timescaledb.yaml`: Deployment + PVC + Secret), verified persisting +
  recovering in-cluster. Next: continuous aggregates for percentile trends over time.
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
  node pool). The whole platform was deployed to a **k3d cluster** and a full run
  verified in-cluster.
- **K8s-native sandboxing + in-cluster builds** (✅ implemented). The orchestrator
  provisions each submission as an isolated **Pod via the Kubernetes API**
  (`sandbox_k8s.py`, `SANDBOX_BACKEND=k8s`) instead of the Docker socket, and builds
  the uploaded source **in-cluster with Kaniko** (ConfigMap build context → Kaniko
  Job → push to an in-cluster registry → Pod runs the built image). Verified on k3d:
  submit source → Kaniko build → scored run, no Docker socket anywhere.
- **Languages** (✅ implemented). Submission templates + verified reference engines
  for **C++, Rust, Go, Python** — all build from source and pass the price-time
  correctness probe (measured peak: Rust/Go ~94k, C++ ~84k, Python ~51k TPS).
- **Hardening:** gVisor/Firecracker microVMs, seccomp profiles, egress controls,
  per-submission build caching, auth.
