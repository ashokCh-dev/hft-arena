# Architecture Decision Records

Short records of the significant, hard-to-reverse decisions behind HFT Arena —
the *why*, not just the *what* (which lives in [../ARCHITECTURE.md](../ARCHITECTURE.md)).
Format: [Michael Nygard's ADR template](https://github.com/joelparkerhenderson/architecture-decision-record).

| # | Decision | Status |
|---|---|---|
| [0001](0001-websocket-json-with-pluggable-binary-wire.md) | WebSocket JSON contract, pluggable binary wire | Accepted |
| [0002](0002-two-phase-open-and-closed-loop-load.md) | Two-phase load: closed-loop throughput + open-loop latency sweep | Accepted |
| [0003](0003-multiplicative-correctness-scoring.md) | Correctness gates the score (multiplicative, not additive) | Accepted |
| [0004](0004-container-sandboxing-docker-and-k8s.md) | Container sandboxing: Docker socket + Kubernetes API backends | Accepted |
| [0005](0005-redis-hot-path-timescaledb-analytics.md) | Redis for the hot path, TimescaleDB for durable analytics | Accepted |
