# ADR 0003 — Correctness gates the score (multiplicative, not additive)

**Status:** Accepted (supersedes the original additive weighting)

## Context
The composite score combines throughput, latency, and correctness. The first design
was additive: `0.45·throughput + 0.35·latency + 0.20·correctness − crash_penalty`.
The adversarial suite exposed the flaw: a **fast but wrong** engine (the LIFO
"cheater", correctness 0) still scored ~600–800, because it kept its
throughput+latency contribution. A matching engine that violates price-time
priority is worthless at any speed — that ranking is indefensible.

## Decision
Make **correctness multiply** the performance score instead of contributing a slice:
```
score        = correctness · performance · 1000
performance  = 0.6·throughput_norm + 0.4·latency_score        # in [0,1]
correctness  = (price-time pass rate) · (acked / sent)        # in [0,1]
```
Correctness already folds in reliability (`acked/sent`), so the additive
`crash_penalty` is **subsumed and removed** (a crasher's low reliability → low
correctness → scaled-down score).

## Consequences
- **+** A wrong engine collapses to ~0 (verified: cheater 0.0 with performance 0.327;
  correct C++ 649 = 0.979 × 0.663). Speed can't buy past a wrong matcher.
- **+** Simpler: one gate replaces a separate penalty term.
- **−** Correctness now dominates: a small reliability dip scales the *whole* score,
  not 20% of it (intended, but sharper).
- **Note:** the throughput:latency split (0.6/0.4) is still a value choice, not
  provably optimal — it should be validated by sensitivity analysis, and the
  `performance` component is exposed in the snapshot for transparency.
