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

CH_CONTROL = "arena:control"
CH_RUNEVENTS = "arena:runevents"
STREAM = "arena:samples"

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
        side = "buy" if random.random() < 0.5 else "sell"
        px = 15000 + random.randint(-20, 20)
        return ('{"t":"limit","id":%d,"side":"%s","px":%d,"qty":%d}'
                % (cur, side, px, random.randint(1, 10)))
    elif roll < 0.95:      # market
        side = "buy" if random.random() < 0.5 else "sell"
        return ('{"t":"market","id":%d,"side":"%s","qty":%d}'
                % (cur, side, random.randint(1, 5)))
    else:                  # cancel a recent order
        return ('{"t":"cancel","id":%d,"target":%d}'
                % (cur, cur - random.randint(1, 50)))


class LoadState:
    """Mutable pacing knob the run loop adjusts while open-loop bots keep running."""
    def __init__(self):
        self.interval = 1.0      # per-bot seconds between sends (open-loop)


async def bot(target, stats, stop_event, deadline, mode="closed", state=None):
    """One WS bot.

    mode="closed": pipelined up to an in-flight window (saturates -> peak TPS).
    mode="open":   paced at `state.interval` s between sends, arrivals independent
                   of acks, so measured latency reflects true service time. The pace
                   is read live so one bot pool can be swept across offered rates.
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
                    m = json.loads(await ws.recv())
                    if "ack" in m:
                        t0 = send_times.pop(m["ack"], None)
                        if t0 is not None:
                            lat_us = (time.perf_counter_ns() - t0) / 1000.0
                            stats.hist[bucket_of(lat_us)] += 1
                            stats.acked += 1
                            outstanding -= 1
                        if mode == "closed":
                            sem.release()
                    # fills ignored by load bots (correctness handled by scenarios)

            rx = asyncio.create_task(receiver())
            t_next = time.monotonic()
            while not stop_event.is_set() and time.monotonic() < deadline:
                if mode == "closed":
                    await sem.acquire()
                else:                              # open-loop: pace arrivals
                    t_next += state.interval
                    delay = t_next - time.monotonic()
                    if delay > 0:
                        await asyncio.sleep(delay)
                    elif delay < -0.5:             # fell behind (saturated): resync
                        t_next = time.monotonic()  # so we don't build infinite backlog
                    if outstanding >= MAX_OUTSTANDING:
                        continue                   # saturated: skip (achieved<offered)
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
    await r.xadd(STREAM, {
        "run_id": stats.run_id, "worker": WORKER_ID, "ts": str(time.time()),
        "phase": tag["phase"], "rate": str(tag["rate"]),
        "sent": str(d_sent), "acked": str(d_acked), "errors": str(d_err),
        "corr_pass": str(d_cp), "corr_total": str(d_ct),
        "hist": json.dumps(hist_delta),
    }, maxlen=100000, approximate=True)


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


async def run_sweep(target, stats, n, r):
    """Open-loop offered-load sweep -> latency-vs-throughput curve."""
    state = LoadState()
    tag = {"phase": "open", "rate": SWEEP_RATES[0]}
    stop_event = asyncio.Event()
    deadline = time.monotonic() + len(SWEEP_RATES) * STEP_SECS + 5
    tasks = [asyncio.create_task(bot(target, stats, stop_event, deadline, "open", state))
             for _ in range(n)]
    rep = asyncio.create_task(reporter(stats, r, stop_event, tag))
    for rate in SWEEP_RATES:
        # Settle: apply the new pace but tag rate=0 (telemetry ignores it) so the
        # step boundary / pacing transient doesn't smear into the measurement.
        state.interval = max(n, 1) / float(rate)   # per-bot interval for offered `rate`
        tag["rate"] = 0
        await asyncio.sleep(1.0)
        tag["rate"] = rate                          # measure steady state
        await asyncio.sleep(STEP_SECS)
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
    budget = duration + len(SWEEP_RATES) * int(STEP_SECS) + 60

    # Correctness warmup on the clean book — exactly one worker runs it; the rest
    # wait for the done flag so they don't pollute the book mid-probe.
    lock_key = f"arena:scenario:{run_id}"
    done_key = f"arena:scenario_done:{run_id}"
    if await r.set(lock_key, WORKER_ID, nx=True, ex=budget):
        await scenarios.run(target, stats, iterations=10)
        await r.set(done_key, "1", ex=budget)
    else:
        for _ in range(80):                       # wait up to ~8s for the probe
            if await r.get(done_key):
                break
            await asyncio.sleep(0.1)

    print(f"[bot_fleet:{WORKER_ID}] run {run_id} -> {target} | sweep {SWEEP_RATES} "
          f"ord/s x{STEP_SECS}s ({n_sweep} bots) then closed {duration}s "
          f"({n_closed} bots)", flush=True)

    # Sweep FIRST (near-empty book -> clean, fair latency), peak-TPS phase LAST
    # (its saturation explodes the book, but that no longer pollutes latency).
    await run_sweep(target, stats, n_sweep, r)
    await run_closed(target, stats, n_closed, duration, r)

    print(f"[bot_fleet:{WORKER_ID}] run {run_id} done: "
          f"sent={stats.sent} acked={stats.acked} errors={stats.errors} "
          f"corr={stats.corr_pass}/{stats.corr_total}", flush=True)
    # Tell the orchestrator the run is over so it can stop the sandbox.
    await r.publish(CH_RUNEVENTS, json.dumps({
        "run_id": run_id, "submission_id": cfg.get("submission_id"), "done": True}))


async def main():
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    pubsub = r.pubsub()
    await pubsub.subscribe(CH_CONTROL)
    print(f"[bot_fleet:{WORKER_ID}] ready, BOTS_PER_WORKER={BOTS_PER_WORKER}",
          flush=True)
    current = None
    async for m in pubsub.listen():
        if m.get("type") != "message":
            continue
        cfg = json.loads(m["data"])
        if cfg.get("cmd") == "start":
            if current and not current.done():
                continue  # already running
            current = asyncio.create_task(run_load(cfg, r))
        elif cfg.get("cmd") == "stop" and current:
            current.cancel()


if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
    except ImportError:
        pass
    asyncio.run(main())
