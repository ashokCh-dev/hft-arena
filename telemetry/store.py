"""TimescaleDB persistence for completed benchmark runs.

Every finished run (with its latency-vs-load curve) is written to a `runs`
hypertable, so the leaderboard survives restarts and we get historical analytics.
Degrades gracefully: if no DATABASE_URL is set or the DB is unreachable, telemetry
keeps running on Redis alone.
"""
import asyncio
import json
import os

import asyncpg

DATABASE_URL = os.environ.get("DATABASE_URL")

SCHEMA = """
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE TABLE IF NOT EXISTS runs (
    ts                timestamptz NOT NULL DEFAULT now(),
    run_id            text NOT NULL,
    submission        text NOT NULL,
    score             double precision,
    peak_tps          bigint,
    max_sustained_tps bigint,
    ref_load          integer,
    p50_us            double precision,
    p90_us            double precision,
    p99_us            double precision,
    correctness       double precision,
    sent              bigint,
    acked             bigint,
    errors            bigint,
    curve             jsonb
);
SELECT create_hypertable('runs', 'ts', if_not_exists => TRUE);
"""

# Columns selected back out, in the shape the dashboard expects.
_COLS = ("submission", "run_id", "score", "peak_tps", "max_sustained_tps",
         "ref_load", "p50_us", "p90_us", "p99_us", "correctness",
         "sent", "acked", "errors", "curve")


async def connect():
    """Connect + ensure schema. Returns a pool, or None if DB is unavailable."""
    if not DATABASE_URL:
        print("[telemetry] no DATABASE_URL — persistence disabled", flush=True)
        return None
    last = None
    for attempt in range(20):                 # tolerate the DB still booting
        try:
            pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)
            async with pool.acquire() as c:
                await c.execute(SCHEMA)
            print("[telemetry] TimescaleDB connected; schema ready", flush=True)
            return pool
        except Exception as exc:
            last = exc
            await asyncio.sleep(1.0)
    print(f"[telemetry] DB unavailable after retries ({last}); persistence disabled",
          flush=True)
    return None


async def persist_run(pool, s: dict):
    if pool is None:
        return
    async with pool.acquire() as c:
        await c.execute(
            """INSERT INTO runs (run_id, submission, score, peak_tps,
                 max_sustained_tps, ref_load, p50_us, p90_us, p99_us,
                 correctness, sent, acked, errors, curve)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::jsonb)""",
            s["run_id"], s["submission"], s["score"], s["peak_tps"],
            s.get("max_sustained_tps", 0), s.get("ref_load", 0),
            s["p50_us"], s["p90_us"], s["p99_us"], s["correctness"],
            s["sent"], s["acked"], s["errors"], json.dumps(s.get("curve", [])),
        )


async def load_best(pool) -> dict:
    """Best-scoring historical run per submission, to seed the leaderboard."""
    if pool is None:
        return {}
    async with pool.acquire() as c:
        rows = await c.fetch(
            """SELECT DISTINCT ON (submission) submission, run_id, score, peak_tps,
                      max_sustained_tps, ref_load, p50_us, p90_us, p99_us,
                      correctness, sent, acked, errors, curve
               FROM runs ORDER BY submission, score DESC, ts DESC""")
    best = {}
    for row in rows:
        d = {k: row[k] for k in _COLS}
        d["curve"] = json.loads(d["curve"]) if d["curve"] else []
        d["tps"] = 0
        best[d["submission"]] = d
    return best


async def recent_runs(pool, limit=20) -> list:
    """Most recent runs for the history panel."""
    if pool is None:
        return []
    async with pool.acquire() as c:
        rows = await c.fetch(
            """SELECT ts, run_id, submission, score, peak_tps, max_sustained_tps,
                      p50_us, p99_us, correctness
               FROM runs ORDER BY ts DESC LIMIT $1""", limit)
    return [{
        "ts": row["ts"].isoformat(), "run_id": row["run_id"],
        "submission": row["submission"], "score": row["score"],
        "peak_tps": row["peak_tps"], "max_sustained_tps": row["max_sustained_tps"],
        "p50_us": row["p50_us"], "p99_us": row["p99_us"],
        "correctness": row["correctness"],
    } for row in rows]
