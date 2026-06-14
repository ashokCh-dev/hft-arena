"""TimescaleDB persistence for completed benchmark runs.

Every finished run (with its latency-vs-load curve) is written to a `runs`
hypertable, so the leaderboard survives restarts and we get historical analytics.
A TimescaleDB **continuous aggregate** (`runs_rollup`) then rolls runs into time
buckets per submission for percentile-trend + latency-jitter tracking over time
(see `percentile_trends`). Degrades gracefully: if no DATABASE_URL is set or the
DB is unreachable, telemetry keeps running on Redis alone.
"""
import asyncio
import json
import os
import re

import asyncpg

DATABASE_URL = os.environ.get("DATABASE_URL")

# Continuous-aggregate bucket width. Default 1 minute so trends are visible within
# a demo session; set TREND_BUCKET='1 hour' (etc.) for production. Validated before
# it's interpolated into DDL (it can't be a bind parameter inside time_bucket()).
TREND_BUCKET = os.environ.get("TREND_BUCKET", "1 minute")
if not re.fullmatch(r"\d+\s+(second|minute|hour|day)s?", TREND_BUCKET.strip()):
    TREND_BUCKET = "1 minute"

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

# Added after the base table so upgrades over an existing hypertable are seamless.
MIGRATIONS = """
ALTER TABLE runs ADD COLUMN IF NOT EXISTS jitter_us     double precision;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS burst_p99_us  double precision;
"""

# Continuous aggregate: per-submission, per-bucket percentile + jitter trend.
# Must be created OUTSIDE a transaction and on its own (TimescaleDB requirement),
# so it's a separate statement from SCHEMA. real-time aggregation (materialized_only
# = false) folds in not-yet-materialized recent runs, so trends appear immediately.
#   p99_jitter  = run-to-run spread of the tail (stddev of p99 across runs in bucket)
#   tail_spread = avg per-run tail spread (p99-p50)
#   burst_p99   = avg tail under microburst load (vs steady p99)
ROLLUP = f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS runs_rollup
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    submission,
    time_bucket(INTERVAL '{TREND_BUCKET}', ts) AS bucket,
    count(*)                          AS runs,
    avg(p50_us)                       AS p50_avg,
    avg(p90_us)                       AS p90_avg,
    avg(p99_us)                       AS p99_avg,
    max(p99_us)                       AS p99_max,
    stddev_samp(p50_us)               AS p50_jitter,
    stddev_samp(p99_us)               AS p99_jitter,
    avg(COALESCE(jitter_us, p99_us - p50_us)) AS tail_spread_avg,
    avg(burst_p99_us)                 AS burst_p99_avg,
    avg(score)                        AS score_avg
FROM runs
GROUP BY submission, bucket
WITH NO DATA
"""

# Columns selected back out, in the shape the dashboard expects.
_COLS = ("submission", "run_id", "score", "peak_tps", "max_sustained_tps",
         "ref_load", "p50_us", "p90_us", "p99_us", "correctness",
         "sent", "acked", "errors", "curve")


async def _ensure_schema(c):
    """Idempotent schema setup. Each continuous-aggregate step runs on its own."""
    await c.execute(SCHEMA)
    await c.execute(MIGRATIONS)
    await c.execute(ROLLUP)
    try:
        # Materialize older buckets in the background; real-time agg covers fresh
        # data meanwhile. Idempotent guard: the policy already existing is fine.
        await c.execute(
            "SELECT add_continuous_aggregate_policy('runs_rollup', "
            "start_offset => NULL, end_offset => NULL, "
            "schedule_interval => INTERVAL '15 minutes', if_not_exists => TRUE)")
    except Exception as exc:
        print(f"[telemetry] rollup refresh policy skipped: {exc}", flush=True)


async def connect():
    """Connect + ensure schema. Returns a pool, or None if DB is unavailable."""
    if not DATABASE_URL:
        print("[telemetry] no DATABASE_URL — persistence disabled", flush=True)
        return None
    last = None
    for attempt in range(90):                 # tolerate first-boot initdb on a fresh PVC
        try:
            pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)
            async with pool.acquire() as c:
                await _ensure_schema(c)
            print(f"[telemetry] TimescaleDB connected; schema + {TREND_BUCKET} "
                  f"trend rollup ready", flush=True)
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
                 correctness, sent, acked, errors, curve, jitter_us, burst_p99_us)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::jsonb,$15,$16)""",
            s["run_id"], s["submission"], s["score"], s["peak_tps"],
            s.get("max_sustained_tps", 0), s.get("ref_load", 0),
            s["p50_us"], s["p90_us"], s["p99_us"], s["correctness"],
            s["sent"], s["acked"], s["errors"], json.dumps(s.get("curve", [])),
            s.get("jitter_us", 0.0), s.get("burst_p99_us", 0.0),
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


def _r(v, nd=1):
    """Round, treating SQL NULL (e.g. stddev of a single run) as 0."""
    return round(v, nd) if v is not None else 0.0


async def percentile_trends(pool, limit=200) -> list:
    """Per-submission, per-time-bucket percentile + jitter trend from the
    `runs_rollup` continuous aggregate. Newest buckets first.
    """
    if pool is None:
        return []
    async with pool.acquire() as c:
        rows = await c.fetch(
            """SELECT bucket, submission, runs, p50_avg, p90_avg, p99_avg, p99_max,
                      p50_jitter, p99_jitter, tail_spread_avg, burst_p99_avg, score_avg
               FROM runs_rollup ORDER BY bucket DESC LIMIT $1""", limit)
    return [{
        "bucket": row["bucket"].isoformat(),
        "submission": row["submission"],
        "runs": row["runs"],
        "p50_avg": _r(row["p50_avg"]),
        "p90_avg": _r(row["p90_avg"]),
        "p99_avg": _r(row["p99_avg"]),
        "p99_max": _r(row["p99_max"]),
        "p50_jitter": _r(row["p50_jitter"]),     # run-to-run spread of p50
        "p99_jitter": _r(row["p99_jitter"]),     # run-to-run spread of the tail
        "tail_spread_avg": _r(row["tail_spread_avg"]),
        "burst_p99_avg": _r(row["burst_p99_avg"]),
        "score_avg": _r(row["score_avg"]),
    } for row in rows]
