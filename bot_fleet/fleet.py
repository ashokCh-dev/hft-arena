"""Distributed Load Generator (Bot Fleet) worker.

Each replica subscribes to the orchestrator's control channel and, on a run
start, spawns BOTS_PER_WORKER pipelined async WebSocket bots that bombard the
contestant's engine with limit/market/cancel orders. Latency, throughput and
correctness deltas are batched into a Redis Stream for the telemetry ingester.

Pipelined design: per bot, a sender fires orders up to an in-flight window and a
receiver matches acks back to send-times. This both saturates the engine and
measures true ack latency under load (interleaved fills are drained naturally).
"""
import asyncio
import json
import math
import os
import random
import socket
import time

import redis.asyncio as aioredis
import websockets

import scenarios
import wire

# Order wire format: "json" (text frames) or "binary" (packed structs, binary
# frames) — see wire.py. Binary removes JSON from the hot path for max TPS.
WIRE = os.environ.get("WIRE", "json")

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")
BOTS_PER_WORKER = int(os.environ.get("BOTS_PER_WORKER", "250"))
WINDOW = int(os.environ.get("INFLIGHT_WINDOW", "32"))   # outstanding orders per bot
# Offered-load SWEEP (orders/sec, this worker). The open-loop latency phase steps
# through these rates; the low-load points give clean service-time latency and the
# high-load points reveal the saturation knee.
SWEEP_RATES = [int(x) for x in
               os.environ.get("SWEEP_RATES", "5000,10000,20000,40000,80000").split(",")]
STEP_SECS = float(os.environ.get("STEP_SECS", "4"))     # seconds per sweep step
SWEEP_BOTS = int(os.environ.get("SWEEP_BOTS", "120"))   # bot pool for the sweep
WORKER_ID = os.environ.get("HOSTNAME", socket.gethostname())

# Microburst phase: real order flow is bursty (quote storms, coordinated reactions
# to a print), not a smooth Poisson trickle. After the steady sweep we re-offer a
# few rates as microbursts — each bot fires BURST_SIZE orders back-to-back, then
# idles, so the MEAN offered rate matches the steady sweep but the instantaneous
# rate spikes. Tagged phase="burst" so telemetry bins it on a separate curve and
# we can see the tail/jitter blow-up a steady sweep at the same mean rate hides.
# Set BURST_RATES="" to disable.
BURST_RATES = [int(x) for x in
               os.environ.get("BURST_RATES", "10000,20000,40000").split(",") if x.strip()]
BURST_STEP_SECS = float(os.environ.get("BURST_STEP_SECS", "3"))  # seconds per burst step
BURST_SIZE = int(os.environ.get("BURST_SIZE", "20"))    # orders per microburst, per bot

CH_CONTROL = "arena:control"
CH_RUNEVENTS = "arena:runevents"
STREAM = "arena:samples"

# Metrics transport: "redis" (Streams) or "kafka" (Redpanda/Kafka topic). Redis is
# still used for control/coordination either way; only the high-volume sample
# firehose moves to Kafka — matching the blueprint's "Kafka/Redpanda for metrics".
TRANSPORT = os.environ.get("TRANSPORT", "redis")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
SAMPLES_TOPIC = os.environ.get("SAMPLES_TOPIC", "arena.samples")
PRODUCER = None        # AIOKafkaProducer when TRANSPORT == "kafka"

# Control transport for orchestrator->fleet commands: "redis" pub/sub or "grpc".
CONTROL = os.environ.get("CONTROL", "redis")
ORCH_GRPC = os.environ.get("ORCH_GRPC", "orchestrator:50051")
GRPC_STUB = None       # arena ControlStub when CONTROL == "grpc"

# --- latency histogram (log-spaced microseconds) -------------------------------
# Bucket mapping MUST stay in sync with telemetry/metrics.py.
NBUCKETS = 256
SCALE = 16.0


def bucket_of(us: float) -> int:
    b = int(math.log1p(max(us, 0.0)) * SCALE)
    return 255 if b > 255 else (0 if b < 0 else b)


class Stats:
    def __init__(self, run_id):
        self.run_id = run_id
        self.sent = 0
        self.acked = 0
        self.errors = 0
        self.corr_pass = 0
        self.corr_total = 0
        self.hist = [0] * NBUCKETS


MAX_OUTSTANDING = 512   # open-loop overload guard (drop rather than unbounded queue)


def _build_order(cur):
    roll = random.random()
    if roll < 0.85:        # limit near mid
        buy = random.random() < 0.5
        px = 15000 + random.randint(-20, 20)
        qty = random.randint(1, 10)
        if WIRE == "binary":
            return wire.enc_limit(cur, buy, px, qty)
        return ('{"t":"limit","id":%d,"side":"%s","px":%d,"qty":%d}'
                % (cur, "buy" if buy else "sell", px, qty))
    elif roll < 0.95:      # market
        buy = random.random() < 0.5
        qty = random.randint(1, 5)
        if WIRE == "binary":
            return wire.enc_market(cur, buy, qty)
        return ('{"t":"market","id":%d,"side":"%s","qty":%d}'
                % (cur, "buy" if buy else "sell", qty))
    else:                  # cancel a recent order
        target = cur - random.randint(1, 50)
        if WIRE == "binary":
            return wire.enc_cancel(cur, target)
        return ('{"t":"cancel","id":%d,"target":%d}' % (cur, target))


class LoadState:
    """Mutable pacing knob the run loop adjusts while open-loop bots keep running."""
    def __init__(self):
        self.interval = 1.0      # per-bot seconds between sends/bursts (open-loop)
        self.burst = 1           # orders per wake-up: 1 = steady, >1 = microburst


async def bot(target, stats, stop_event, deadline, mode="closed", state=None):
    """One WS bot.

    mode="closed": pipelined up to an in-flight window (saturates -> peak TPS).
    mode="open":   paced at `state.interval` s between wake-ups, arrivals independent
                   of acks, so measured latency reflects true service time. The pace
                   is read live so one bot pool can be swept across offered rates.
                   With `state.burst > 1` each wake-up fires a tight burst of orders
                   (microburst load) instead of a single one.
    """
    try:
        async with websockets.connect(target, ping_interval=None,
                                      open_timeout=10, max_queue=None) as ws:
            sem = asyncio.Semaphore(WINDOW)
            send_times = {}
            outstanding = 0
            oid = random.randint(1, 10_000_000) * 1000  # disjoint id space per bot

            async def receiver():
                nonlocal outstanding
                while True:
                    data = await ws.recv()
                    if WIRE == "binary":
                        if data[0] != wire.T_ACK:   # ignore fills on load bots
                            continue
                        ack_id = wire.dec_ack(data)[0]
                    else:
                        m = json.loads(data)
                        if "ack" not in m:
                            continue
                        ack_id = m["ack"]
                    t0 = send_times.pop(ack_id, None)
                    if t0 is not None:
                        lat_us = (time.perf_counter_ns() - t0) / 1000.0
                        stats.hist[bucket_of(lat_us)] += 1
                        stats.acked += 1
                        outstanding -= 1
                    if mode == "closed":
                        sem.release()

            rx = asyncio.create_task(receiver())
            t_next = time.monotonic()
            while not stop_event.is_set() and time.monotonic() < deadline:
                if mode == "closed":
                    await sem.acquire()
                    burst = 1
                else:                              # open-loop: pace arrivals
                    t_next += state.interval
                    delay = t_next - time.monotonic()
                    if delay > 0:
                        await asyncio.sleep(delay)
                    elif delay < -0.5:             # fell behind (saturated): resync
                        t_next = time.monotonic()  # so we don't build infinite backlog
                    burst = state.burst            # 1 = steady, >1 = microburst
                # Fire one order (steady/closed) or a tight microburst of them.
                for _ in range(burst):
                    if mode != "closed" and outstanding >= MAX_OUTSTANDING:
                        break                      # saturated: drop rest (achieved<offered)
                    oid += 1
                    cur = oid
                    msg = _build_order(cur)
                    send_times[cur] = time.perf_counter_ns()
                    stats.sent += 1
                    outstanding += 1
                    try:
                        await ws.send(msg)
                    except Exception:
                        if mode == "closed":
                            sem.release()
                        raise
            await asyncio.sleep(0.5)               # let in-flight acks drain
            rx.cancel()
    except Exception:
        stats.errors += 1


async def reporter(stats, r, stop_event, tag):
    """Flush per-tick deltas to the Redis Stream, tagged with the live phase/rate.

    `tag` is a dict the caller mutates between sweep steps; baselines start at the
    reporter's creation so histogram deltas are scoped to the current phase.
    """
    last = dict(sent=stats.sent, acked=stats.acked, errors=stats.errors,
                corr_pass=stats.corr_pass, corr_total=stats.corr_total)
    last_hist = list(stats.hist)
    while not stop_event.is_set():
        await asyncio.sleep(0.2)
        await _flush(stats, r, last, last_hist, tag)
    await _flush(stats, r, last, last_hist, tag)  # final flush


async def _flush(stats, r, last, last_hist, tag):
    d_sent = stats.sent - last["sent"]
    d_acked = stats.acked - last["acked"]
    d_err = stats.errors - last["errors"]
    d_cp = stats.corr_pass - last["corr_pass"]
    d_ct = stats.corr_total - last["corr_total"]
    hist_delta = []
    for i in range(NBUCKETS):
        dv = stats.hist[i] - last_hist[i]
        if dv:
            hist_delta.append([i, dv])
            last_hist[i] = stats.hist[i]
    if not (d_sent or d_acked or d_err or d_ct):
        return
    last.update(sent=stats.sent, acked=stats.acked, errors=stats.errors,
                corr_pass=stats.corr_pass, corr_total=stats.corr_total)
    rec = {
        "run_id": stats.run_id, "worker": WORKER_ID, "ts": str(time.time()),
        "phase": tag["phase"], "rate": str(tag["rate"]),
        "sent": str(d_sent), "acked": str(d_acked), "errors": str(d_err),
        "corr_pass": str(d_cp), "corr_total": str(d_ct),
        "hist": json.dumps(hist_delta),
    }
    if TRANSPORT == "kafka":
        await PRODUCER.send_and_wait(SAMPLES_TOPIC, json.dumps(rec).encode())
    else:
        await r.xadd(STREAM, rec, maxlen=100000, approximate=True)


async def run_closed(target, stats, n, duration, r):
    """Closed-loop saturation phase -> peak TPS."""
    stop_event = asyncio.Event()
    deadline = time.monotonic() + duration + 1
    tag = {"phase": "closed", "rate": 0}
    tasks = [asyncio.create_task(bot(target, stats, stop_event, deadline, "closed"))
             for _ in range(n)]
    rep = asyncio.create_task(reporter(stats, r, stop_event, tag))
    await asyncio.sleep(duration)
    stop_event.set()
    await asyncio.gather(*tasks, return_exceptions=True)
    await rep


async def run_sweep(target, stats, n, r, fleet_n=1):
    """Open-loop offered-load sweep -> latency-vs-throughput curve.

    Under horizontal scaling the offered load is split across `fleet_n` workers:
    each worker offers `rate / fleet_n` so the AGGREGATE offered load equals the
    `rate` label that every worker tags. Telemetry sums across workers, so the
    curve's x-axis stays the true aggregate offered load.
    """
    state = LoadState()
    tag = {"phase": "open", "rate": SWEEP_RATES[0]}
    stop_event = asyncio.Event()
    deadline = time.monotonic() + len(SWEEP_RATES) * (STEP_SECS + 1) + 5
    tasks = [asyncio.create_task(bot(target, stats, stop_event, deadline, "open", state))
             for _ in range(n)]
    rep = asyncio.create_task(reporter(stats, r, stop_event, tag))
    for rate in SWEEP_RATES:
        # This worker's share of the aggregate offered rate; per-bot interval.
        share = rate / float(fleet_n)
        # Settle: apply the new pace but tag rate=0 (telemetry ignores it) so the
        # step boundary / pacing transient doesn't smear into the measurement.
        state.interval = max(n, 1) / max(share, 1.0)
        tag["rate"] = 0
        await asyncio.sleep(1.0)
        tag["rate"] = rate                          # tag with the AGGREGATE rate
        await asyncio.sleep(STEP_SECS)
    stop_event.set()
    await asyncio.gather(*tasks, return_exceptions=True)
    await rep


async def run_burst(target, stats, n, r, fleet_n=1):
    """Open-loop MICROBURST sweep -> latency-vs-load curve under bursty arrivals.

    Same mean offered rate as the steady sweep, but each bot fires BURST_SIZE
    orders back-to-back then idles, so instantaneous load spikes to n*BURST_SIZE
    per worker. The per-bot gap is sized so the mean still equals the tagged rate:
    rate/fleet_n = (n * BURST_SIZE) / gap  =>  gap = n*BURST_SIZE / share.
    Tagged phase="burst" so telemetry keeps it on a separate curve.
    """
    if not BURST_RATES:
        return
    state = LoadState()
    state.burst = BURST_SIZE
    tag = {"phase": "burst", "rate": BURST_RATES[0]}
    stop_event = asyncio.Event()
    deadline = time.monotonic() + len(BURST_RATES) * (BURST_STEP_SECS + 1) + 5
    tasks = [asyncio.create_task(bot(target, stats, stop_event, deadline, "open", state))
             for _ in range(n)]
    rep = asyncio.create_task(reporter(stats, r, stop_event, tag))
    for rate in BURST_RATES:
        share = rate / float(fleet_n)
        # Gap between this bot's bursts so the per-worker mean rate == share.
        state.interval = (max(n, 1) * BURST_SIZE) / max(share, 1.0)
        tag["rate"] = 0                             # settle (telemetry ignores rate=0)
        await asyncio.sleep(1.0)
        tag["rate"] = rate                          # tag with the AGGREGATE mean rate
        await asyncio.sleep(BURST_STEP_SECS)
    stop_event.set()
    await asyncio.gather(*tasks, return_exceptions=True)
    await rep


async def run_load(cfg, r):
    target = cfg["target"]
    duration = int(cfg["duration"])               # used as the closed-loop phase length
    n_closed = min(int(cfg.get("bots", BOTS_PER_WORKER)), BOTS_PER_WORKER)
    n_sweep = min(SWEEP_BOTS, BOTS_PER_WORKER)
    stats = Stats(cfg["run_id"])
    run_id = cfg["run_id"]
    budget = (duration + len(SWEEP_RATES) * int(STEP_SECS)
              + len(BURST_RATES) * int(BURST_STEP_SECS) + 60)

    # Fleet barrier: every participating replica registers, then we read the count
    # so the offered-load sweep can be split into equal shares (correct distribution
    # under `docker compose --scale` / a K8s Deployment with >1 replica).
    fleet_key = f"arena:fleet:{run_id}"
    await r.sadd(fleet_key, WORKER_ID)
    await r.expire(fleet_key, budget)

    # Correctness warmup on the clean book — exactly one worker runs it; the rest
    # wait for the done flag so they don't pollute the book mid-probe.
    lock_key = f"arena:scenario:{run_id}"
    done_key = f"arena:scenario_done:{run_id}"
    is_leader = bool(await r.set(lock_key, WORKER_ID, nx=True, ex=budget))
    if is_leader:
        await asyncio.sleep(1.5)                  # barrier: let all replicas register
        await scenarios.run(target, stats, iterations=10, wire_mode=WIRE)
        await r.set(done_key, "1", ex=budget)
        # Transmit the correctness results NOW. The per-phase reporters baseline
        # their counters at creation (after this point), so they would never send
        # the warmup probe's corr counts as a delta — emit them explicitly.
        rec = {
            "run_id": run_id, "worker": WORKER_ID, "ts": str(time.time()),
            "phase": "warmup", "rate": "0", "sent": "0", "acked": "0",
            "errors": "0", "corr_pass": str(stats.corr_pass),
            "corr_total": str(stats.corr_total), "hist": "[]",
        }
        if TRANSPORT == "kafka":
            await PRODUCER.send_and_wait(SAMPLES_TOPIC, json.dumps(rec).encode())
        else:
            await r.xadd(STREAM, rec, maxlen=100000, approximate=True)
    else:
        for _ in range(120):                      # wait up to ~12s for the probe
            if await r.get(done_key):
                break
            await asyncio.sleep(0.1)

    fleet_n = max(1, int(await r.scard(fleet_key)))
    print(f"[bot_fleet:{WORKER_ID}] run {run_id} -> {target} | fleet={fleet_n} | "
          f"sweep {SWEEP_RATES} ord/s x{STEP_SECS}s ({n_sweep} bots/worker), "
          f"burst {BURST_RATES} ord/s (x{BURST_SIZE}) x{BURST_STEP_SECS}s then "
          f"closed {duration}s ({n_closed} bots/worker)", flush=True)

    # Steady sweep FIRST (near-empty book -> clean, fair latency), then the bursty
    # sweep (same mean rates, microbursts), peak-TPS phase LAST (its saturation
    # explodes the book, but that no longer pollutes the latency curves).
    await run_sweep(target, stats, n_sweep, r, fleet_n)
    await run_burst(target, stats, n_sweep, r, fleet_n)
    await run_closed(target, stats, n_closed, duration, r)

    print(f"[bot_fleet:{WORKER_ID}] run {run_id} done: "
          f"sent={stats.sent} acked={stats.acked} errors={stats.errors} "
          f"corr={stats.corr_pass}/{stats.corr_total}", flush=True)
    # Only the leader signals run-done, so a fast replica can't stop the sandbox
    # out from under stragglers still draining their final orders.
    if is_leader:
        if CONTROL == "grpc":
            import arena_pb2
            await GRPC_STUB.ReportDone(arena_pb2.RunEvent(
                run_id=run_id, submission_id=cfg.get("submission_id") or ""))
        else:
            await r.publish(CH_RUNEVENTS, json.dumps({
                "run_id": run_id, "submission_id": cfg.get("submission_id"), "done": True}))


async def _dispatch(cfg, r, current):
    """Apply one start/stop command; returns the (possibly new) current task."""
    if cfg.get("cmd") == "start":
        if current and not current.done():
            return current                # already running
        return asyncio.create_task(run_load(cfg, r))
    if cfg.get("cmd") == "stop" and current:
        current.cancel()
    return current


async def _control_redis(r):
    pubsub = r.pubsub()
    await pubsub.subscribe(CH_CONTROL)
    current = None
    async for m in pubsub.listen():
        if m.get("type") == "message":
            current = await _dispatch(json.loads(m["data"]), r, current)


async def _control_grpc(r):
    """Subscribe to the orchestrator's gRPC command stream (reconnects on drop)."""
    global GRPC_STUB
    import grpc
    import arena_pb2
    import arena_pb2_grpc
    current = None
    while True:
        try:
            channel = grpc.aio.insecure_channel(ORCH_GRPC)
            GRPC_STUB = arena_pb2_grpc.ControlStub(channel)
            async for cmd in GRPC_STUB.Subscribe(arena_pb2.SubscribeRequest(worker=WORKER_ID)):
                cfg = {"cmd": cmd.cmd, "run_id": cmd.run_id, "submission": cmd.submission,
                       "submission_id": cmd.submission_id, "target": cmd.target,
                       "bots": cmd.bots, "duration": cmd.duration}
                current = await _dispatch(cfg, r, current)
        except Exception as exc:
            print(f"[bot_fleet:{WORKER_ID}] gRPC control reconnecting ({exc})", flush=True)
            await asyncio.sleep(2)


async def main():
    global PRODUCER
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    if TRANSPORT == "kafka":
        from aiokafka import AIOKafkaProducer
        PRODUCER = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP,
                                    linger_ms=20, acks=1)
        await PRODUCER.start()
        print(f"[bot_fleet:{WORKER_ID}] kafka producer -> {KAFKA_BOOTSTRAP}", flush=True)
    print(f"[bot_fleet:{WORKER_ID}] ready, BOTS_PER_WORKER={BOTS_PER_WORKER}, "
          f"transport={TRANSPORT}, control={CONTROL}", flush=True)
    if CONTROL == "grpc":
        await _control_grpc(r)
    else:
        await _control_redis(r)


if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
    except ImportError:
        pass
    asyncio.run(main())
