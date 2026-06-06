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
# Open-loop offered rate (orders/sec, this worker) for the latency phase. Keep it
# below the engine's capacity so latency reflects service time, not queueing.
OPEN_RATE = float(os.environ.get("OPEN_RATE", "15000"))
WORKER_ID = os.environ.get("HOSTNAME", socket.gethostname())

CH_CONTROL = "arena:control"
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


async def bot(target, stats, stop_event, deadline, mode="closed", rate=0.0):
    """One WS bot.

    mode="closed": pipelined up to an in-flight window (saturates -> peak TPS).
    mode="open":   paced at `rate` orders/sec, arrivals independent of acks, so
                   measured latency reflects true service time, not queue depth.
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
            interval = (1.0 / rate) if (mode == "open" and rate > 0) else 0.0
            t_next = time.monotonic()
            while not stop_event.is_set() and time.monotonic() < deadline:
                if mode == "closed":
                    await sem.acquire()
                else:                              # open-loop: pace arrivals
                    t_next += interval
                    delay = t_next - time.monotonic()
                    if delay > 0:
                        await asyncio.sleep(delay)
                    if outstanding >= MAX_OUTSTANDING:
                        stats.errors += 1          # engine can't keep up: drop
                        continue
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


async def reporter(stats, r, stop_event, phase):
    """Flush per-tick deltas to the Redis Stream for telemetry, tagged by phase.

    Baselines start at the phase's beginning, so histogram deltas are scoped to
    this phase — telemetry uses only the open-loop phase for latency percentiles.
    """
    last = dict(sent=stats.sent, acked=stats.acked, errors=stats.errors,
                corr_pass=stats.corr_pass, corr_total=stats.corr_total)
    last_hist = list(stats.hist)
    while not stop_event.is_set():
        await asyncio.sleep(0.2)
        await _flush(stats, r, last, last_hist, phase)
    await _flush(stats, r, last, last_hist, phase)  # final flush


async def _flush(stats, r, last, last_hist, phase):
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
        "phase": phase,
        "sent": str(d_sent), "acked": str(d_acked), "errors": str(d_err),
        "corr_pass": str(d_cp), "corr_total": str(d_ct),
        "hist": json.dumps(hist_delta),
    }, maxlen=100000, approximate=True)


async def run_phase(target, stats, n, duration, mode, rate, r, phase):
    """Spawn n bots for one phase and report tagged samples."""
    stop_event = asyncio.Event()
    deadline = time.monotonic() + duration + 1
    tasks = [asyncio.create_task(bot(target, stats, stop_event, deadline, mode, rate))
             for _ in range(n)]
    rep = asyncio.create_task(reporter(stats, r, stop_event, phase))
    await asyncio.sleep(duration)
    stop_event.set()
    await asyncio.gather(*tasks, return_exceptions=True)
    await rep


async def run_load(cfg, r):
    target = cfg["target"]
    duration = int(cfg["duration"])
    n = int(cfg.get("bots", BOTS_PER_WORKER))
    n = min(n, BOTS_PER_WORKER)  # this worker handles up to its share
    stats = Stats(cfg["run_id"])
    run_id = cfg["run_id"]

    # Correctness warmup on the clean book — exactly one worker runs it; the rest
    # wait for the done flag so they don't pollute the book mid-probe.
    lock_key = f"arena:scenario:{run_id}"
    done_key = f"arena:scenario_done:{run_id}"
    if await r.set(lock_key, WORKER_ID, nx=True, ex=duration + 30):
        await scenarios.run(target, stats, iterations=10)
        await r.set(done_key, "1", ex=duration + 30)
    else:
        for _ in range(80):                      # wait up to ~8s for the probe
            if await r.get(done_key):
                break
            await asyncio.sleep(0.1)

    # Phase A (closed-loop): saturate to find peak TPS.
    # Phase B (open-loop):  pace arrivals below capacity to measure true latency.
    half = max(2, duration // 2)
    rate_per_bot = max(1.0, OPEN_RATE / max(n, 1))
    print(f"[bot_fleet:{WORKER_ID}] run {run_id} -> {target} | "
          f"phase A: {n} bots closed-loop {half}s; "
          f"phase B: open-loop {OPEN_RATE:.0f} ord/s {duration - half}s", flush=True)

    await run_phase(target, stats, n, half, "closed", 0.0, r, "closed")
    await run_phase(target, stats, n, duration - half, "open", rate_per_bot, r, "open")

    print(f"[bot_fleet:{WORKER_ID}] run {run_id} done: "
          f"sent={stats.sent} acked={stats.acked} errors={stats.errors} "
          f"corr={stats.corr_pass}/{stats.corr_total}", flush=True)


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
