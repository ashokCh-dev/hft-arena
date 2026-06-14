"""Correctness probe: validates price-time priority and fill accuracy.

Runs as a fast warmup against the *empty* book, before the load bots start (a
Redis lock ensures only one fleet worker runs it and the others wait). Because
the book is clean, the outcome is deterministic. Each iteration:

  1. rest BUY maker_A  (5 @ PX)
  2. rest BUY maker_B  (5 @ PX)   -> behind A in time priority
  3. aggress SELL 6 @ PX          -> a correct engine fills A(5) THEN B(1)

We assert the taker's fills are exactly [(maker_A,5),(maker_B,1)] in that order.
A wrong engine (bad priority or bad fill math) fails the assertion. Works over
either wire format (json text or binary frames).
"""
import asyncio
import json

import websockets

import wire

SCEN_ID_BASE = 1_000_000_000_000_000  # disjoint from load-bot order ids


def _limit(wire_mode, oid, buy, px, qty):
    if wire_mode == "binary":
        return wire.enc_limit(oid, buy, px, qty)
    return json.dumps({"t": "limit", "id": oid,
                       "side": "buy" if buy else "sell", "px": px, "qty": qty})


def _cancel(wire_mode, oid, target):
    if wire_mode == "binary":
        return wire.enc_cancel(oid, target)
    return json.dumps({"t": "cancel", "id": oid, "target": target})


async def _expect_fills(ws, taker_id, n, wire_mode):
    got = []
    while len(got) < n:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
        except asyncio.TimeoutError:
            break
        if wire_mode == "binary":
            if raw[0] != wire.T_FILL:
                continue
            oid, _px, qty, maker = wire.dec_fill(raw)
            if oid == taker_id:
                got.append((maker, qty))
        else:
            m = json.loads(raw)
            if "fill" in m and m["fill"] == taker_id:
                got.append((m["maker"], m["qty"]))
    return got


async def run(target, stats, iterations=10, wire_mode="json"):
    """Run a fixed number of correctness iterations on the clean book."""
    try:
        async with websockets.connect(target, ping_interval=None,
                                      max_queue=None) as ws:
            for it in range(1, iterations + 1):
                px = 50000 + it                      # unique level each iteration
                a = SCEN_ID_BASE + it * 10 + 1
                b = SCEN_ID_BASE + it * 10 + 2
                c = SCEN_ID_BASE + it * 10 + 3
                await ws.send(_limit(wire_mode, a, True, px, 5))
                await ws.recv()                      # ack A
                await ws.send(_limit(wire_mode, b, True, px, 5))
                await ws.recv()                      # ack B
                await ws.send(_limit(wire_mode, c, False, px, 6))
                await ws.recv()                      # ack C
                fills = await _expect_fills(ws, c, 2, wire_mode)

                stats.corr_total += 1
                if fills == [(a, 5), (b, 1)]:
                    stats.corr_pass += 1
                # cancel leftover maker B (qty 4) to keep the book clean
                await ws.send(_cancel(wire_mode, c + 1, b))
                try:
                    await asyncio.wait_for(ws.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass
    except Exception:
        return
