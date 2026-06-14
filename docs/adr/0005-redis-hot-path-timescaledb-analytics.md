# ADR 0005 — Redis for the hot path, TimescaleDB for durable analytics

**Status:** Accepted

## Context
The platform has two very different data needs: a **high-volume, low-latency**
firehose of telemetry samples + live coordination (control, leaderboard fan-out),
and **durable, queryable history** for the leaderboard-after-restart and percentile
trends over time. The blueprint name-drops Kafka/Redpanda, gRPC, Redis, and
TimescaleDB; we wanted the right tool per job, not one store forced into both.

## Decision
- **Redis** = the hot path / coordination: Streams for the sample firehose (consumer
  offsets + back-pressure), pub/sub for control + leaderboard fan-out, hashes/sets
  for live state and the fleet barrier. Tiny, schema-less, fast.
- **TimescaleDB** = durable analytics: each finished run (with its latency curve)
  persists to a `runs` hypertable; a **continuous aggregate** rolls runs into time
  buckets for percentile trends + run-to-run jitter. Leaderboard recovers from it on
  restart.
- Both the metrics transport and control plane are **pluggable** to the
  blueprint-named tech: `TRANSPORT=redis|kafka` (Redpanda topic) and
  `CONTROL=redis|grpc`.

## Consequences
- **+** Each store plays to its strength; the firehose never touches Postgres, and
  analytics never bloat Redis.
- **+** Durability: leaderboard + trends survive restarts (verified); Kafka/gRPC
  prove the architecture isn't Redis-locked.
- **−** More moving parts (two stores, optional Redpanda) and an at-least-once vs
  exactly-once seam between them.
- **−** Telemetry is a single consumer/aggregator (not yet HA) — a deliberate
  simplicity choice; see the Chaos & Resilience tests for its restart behavior.
