# ADR 0002 — Two-phase load: closed-loop throughput + open-loop latency sweep

**Status:** Accepted

## Context
We must report both **peak throughput** and **latency** for each engine. These are
measured at *opposite* operating points (Little's Law, `L = λ·W`): max throughput
requires saturating the engine, but under saturation "latency" is just queue depth;
true service-time latency requires running *below* capacity. Measuring both in one
run gives the classic contaminated benchmark ("180k TPS **and** 2 ms latency" — when
the 2 ms only holds if you weren't doing 180k).

## Decision
Split each run into phases against the same engine:
1. **Open-loop sweep** (first, on a near-empty book): pace arrivals through a series
   of offered rates (`SWEEP_RATES`), independent of acks → the **latency-vs-load
   curve** (clean service time per load; the knee is the saturation point).
2. **Microburst sweep** (added later): same *mean* rates delivered as bursts →
   exposes tail/jitter a steady sweep hides.
3. **Closed-loop** (last): pipeline up to an in-flight window to **saturate** →
   peak TPS. Runs last so its book explosion doesn't pollute the latency curves.

Latency is scored at a **fixed reference load** for apples-to-apples comparison; the
full curve + knee remain available.

## Consequences
- **+** Honest numbers: a real peak-TPS figure *and* a trustworthy latency curve,
  neither corrupting the other.
- **+** The curve/knee is a richer signal than any single number (engine quality =
  where it breaks, not its idle latency).
- **−** Longer runs (three phases) and more telemetry plumbing (per-phase, per-rate
  binning).
- **−** A single fixed reference load under-discriminates engines if chosen too low;
  mitigated by making `REF_LOAD` tunable (and a knee/SLA metric is the future fix).
