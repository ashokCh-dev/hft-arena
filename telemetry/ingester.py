"""Telemetry & Validation Ingester.

Consumes the bot fleet's sample stream from Redis, aggregates per-run latency
percentiles / TPS / correctness, computes the composite score, publishes a live
leaderboard snapshot, and persists every completed run to TimescaleDB (so the
leaderboard survives restarts and we keep historical analytics).
"""
import asyncio
import json
import os
import time

import redis.asyncio as aioredis

import store
from metrics import Agg

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")
STREAM = "arena:samples"
CH_CONTROL = "arena:control"
CH_EVENTS = "arena:events"
CH_RUNEVENTS = "arena:runevents"
KEY_SNAPSHOT = "arena:snapshot"
KEY_LB = "arena:leaderboard"        # persisted best-per-submission
KEY_HISTORY = "arena:history"       # recent runs (for the dashboard history panel)

RUNS = {}          # run_id -> Agg
BEST = {}          # submission -> snapshot (live; seeded from DB on startup)
POOL = None        # asyncpg pool (or None if persistence disabled)


async def control_listener(r):
    """Learn run_id -> submission mapping so samples can be labelled."""
    pubsub = r.pubsub()
    await pubsub.subscribe(CH_CONTROL)
    async for m in pubsub.listen():
        if m.get("type") != "message":
            continue
        cfg = json.loads(m["data"])
        if cfg.get("cmd") == "start":
            RUNS[cfg["run_id"]] = Agg(cfg["run_id"], cfg.get("submission", "?"))


async def persist_listener(r):
    """On run-done, persist the run's final snapshot to TimescaleDB + refresh history."""
    pubsub = r.pubsub()
    await pubsub.subscribe(CH_RUNEVENTS)
    async for m in pubsub.listen():
        if m.get("type") != "message":
            continue
        ev = json.loads(m["data"])
        run_id = ev.get("run_id")
        if not ev.get("done") or run_id not in RUNS:
            continue
        # Give the final sample flush a beat to land, then persist.
        await asyncio.sleep(1.0)
        snap = RUNS[run_id].snapshot()
        try:
            await store.persist_run(POOL, snap)
            await _refresh_history(r)
            print(f"[telemetry] persisted run {run_id} ({snap['submission']}, "
                  f"score={snap['score']})", flush=True)
        except Exception as exc:
            print(f"[telemetry] persist failed: {exc}", flush=True)


async def _refresh_history(r):
    rows = await store.recent_runs(POOL, limit=20)
    await r.set(KEY_HISTORY, json.dumps({"type": "history", "runs": rows}))


async def sample_consumer(r):
    last_id = "$"
    while True:
        resp = await r.xread({STREAM: last_id}, block=500, count=1000)
        if not resp:
            continue
        for _stream, entries in resp:
            for entry_id, fields in entries:
                last_id = entry_id
                run_id = fields.get("run_id")
                agg = RUNS.get(run_id)
                if agg is None:
                    agg = RUNS[run_id] = Agg(run_id)
                agg.add(
                    int(fields.get("sent", 0)),
                    int(fields.get("acked", 0)),
                    int(fields.get("errors", 0)),
                    int(fields.get("corr_pass", 0)),
                    int(fields.get("corr_total", 0)),
                    json.loads(fields.get("hist", "[]")),
                    fields.get("phase", "open"),
                    int(fields.get("rate", 0)),
                    float(fields.get("ts", 0.0)),
                )


async def publisher(r):
    """Every 500ms, push the ranked leaderboard snapshot to the dashboard."""
    while True:
        await asyncio.sleep(0.5)
        # Show each submission's LATEST snapshot. The score legitimately evolves
        # as the offered-load sweep progresses (and settles once the run ends), so
        # "best score ever" would freeze an early, incomplete curve — we want the
        # final, complete one. Most-recent run wins (RUNS preserves insert order).
        for agg in RUNS.values():
            BEST[agg.submission] = agg.snapshot()
        ranked = sorted(BEST.values(), key=lambda s: s["score"], reverse=True)
        if not ranked:
            continue
        payload = {"type": "leaderboard", "ts": time.time(), "rows": ranked}
        msg = json.dumps(payload)
        await r.set(KEY_SNAPSHOT, msg)
        await r.publish(CH_EVENTS, msg)
        if BEST:
            await r.hset(KEY_LB, mapping={s: json.dumps(v) for s, v in BEST.items()})


async def main():
    global POOL, BEST
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    POOL = await store.connect()
    # Recover the leaderboard from history so a restart isn't a blank board.
    BEST = await store.load_best(POOL)
    if BEST:
        print(f"[telemetry] recovered {len(BEST)} submissions from TimescaleDB",
              flush=True)
        await _refresh_history(r)
    print("[telemetry] ingester online", flush=True)
    await asyncio.gather(control_listener(r), persist_listener(r),
                         sample_consumer(r), publisher(r))


if __name__ == "__main__":
    asyncio.run(main())
