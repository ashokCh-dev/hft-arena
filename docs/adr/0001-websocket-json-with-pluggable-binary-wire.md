# ADR 0001 — WebSocket JSON contract, pluggable binary wire

**Status:** Accepted

## Context
Contestants upload matching engines in C++/Rust/Go/Python. The bot fleet must drive
them with limit/market/cancel orders and measure ack latency. We needed one wire
contract that is (a) trivial to implement in all four languages, (b) debuggable,
and (c) able to measure the *engine*, not the serializer, when we want max
performance. The seed (`hft_arena.zip`) used a raw-TCP 17-byte packed struct — fast
but brittle across languages (manual struct packing, endianness, "must match 17
bytes exactly" footguns).

## Decision
- **WebSocket** transport (framing built-in, browser-debuggable, one server per
  language has a mature library).
- **JSON** as the default contract — every language has it, self-describing,
  evolvable, easy to inspect.
- **Pluggable binary mode** (`WIRE=binary`): a fixed-layout little-endian packed
  struct (REQ 26B / ACK 17B / FILL 25B) over WS *binary* frames. Engines
  **auto-detect** the frame type (binary in → binary out, text in → JSON), so one
  engine serves both modes.

## Consequences
- **+** Low onboarding barrier; JSON tax is identical for every engine so it never
  unfairly favors one.
- **+** Binary mode removes JSON from the hot path → measured ~2× lower p50 on the
  wire-bound engines (C++ 2.4 ms → 1.06 ms, Go 1.28 ms → 0.83 ms); correctness still
  passes in both modes.
- **−** Two code paths in the bot/engine; the binary layout must stay in sync across
  five codebases (documented in `bot_fleet/wire.py`).
- **−** Still on WebSocket, not raw TCP — the theoretical latency floor (no WS
  framing/masking) is left on the table; acceptable for the browser-debuggable win.
