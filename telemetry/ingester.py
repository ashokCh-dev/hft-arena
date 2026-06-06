"""Telemetry & Validation Ingester.

Consumes the bot fleet's sample stream from Redis, aggregates per-run latency
percentiles / TPS / correctness, computes the composite score, and publishes a
live leaderboard snapshot that the orchestrator relays to the dashboard.
"""
import asyncio
import json
import os
import time

import redis.asyncio as aioredis

from metrics import Agg

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")
STREAM = "arena:samples"
CH_CONTROL = "arena:control"
CH_EVENTS = "arena:events"
KEY_SNAPSHOT = "arena:snapshot"
KEY_LB = "arena:leaderboard"        # persisted best-per-submission

RUNS = {}          # run_id -> Agg
BEST = {}          # submission -> best snapshot (across runs)


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
        # persist best-per-submission scores
        if BEST:
            await r.hset(KEY_LB, mapping={s: json.dumps(v) for s, v in BEST.items()})


async def main():
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    print("[telemetry] ingester online", flush=True)
    await asyncio.gather(control_listener(r), sample_consumer(r), publisher(r))


if __name__ == "__main__":
    asyncio.run(main())
