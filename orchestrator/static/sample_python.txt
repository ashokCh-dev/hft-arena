#!/usr/bin/env python3
"""HFT Arena — Python reference matching engine (self-contained submission).

Speaks the WS-JSON contract on port 9000:

  bot -> engine:
    {"t":"limit","id":<u64>,"side":"buy"|"sell","px":<int>,"qty":<int>,"ts":<ns>}
    {"t":"market","id":<u64>,"side":...,"qty":<int>}
    {"t":"cancel","id":<u64>,"target":<resting_order_id>}

  engine -> bot:
    {"ack":<id>,"ts":<engine_ns>}            (always, sent first; latency measured to this)
    {"fill":<id>,"px":<int>,"qty":<int>,"maker":<id>}
    {"reject":<id>,"reason":"..."}

A single shared order book is used across all bot connections. Because the
server is single-threaded asyncio and the book operations are synchronous, every
order is processed atomically — no locks needed. The book implements strict
price-time priority (resting orders at a price fill in arrival order).
"""
import asyncio
import json
import os
import struct
import time
from bisect import insort

import websockets

# Binary wire (auto-detected from binary frames): LE packed, no padding.
#   REQ 26B: B type, B side, Q id, i px, i qty, Q target  (type 1=limit 2=market 3=cancel)
#   ACK 17B: B 1, Q id, Q ts        FILL 25B: B 2, Q id, i px, i qty, Q maker
_REQ = struct.Struct("<BBQiiQ")
_ACK = struct.Struct("<BQQ")
_FILL = struct.Struct("<BQiiQ")


class OrderBook:
    """Price-time-priority limit order book.

    Each price level is a dict {order_id: qty}; Python dicts preserve insertion
    order, which gives us time priority for free (oldest resting order first).
    """

    def __init__(self):
        self.bid_levels = {}        # price -> {order_id: qty}
        self.ask_levels = {}        # price -> {order_id: qty}
        self.bid_prices = []        # sorted ascending; best bid = bid_prices[-1]
        self.ask_prices = []        # sorted ascending; best ask = ask_prices[0]
        self.resting = {}           # order_id -> (side, price)

    def _rest(self, side, price, oid, qty):
        levels, prices = (self.bid_levels, self.bid_prices) if side == "buy" \
            else (self.ask_levels, self.ask_prices)
        level = levels.get(price)
        if level is None:
            level = levels[price] = {}
            insort(prices, price)
        level[oid] = level.get(oid, 0) + qty
        self.resting[oid] = (side, price)

    def _remove_level_if_empty(self, side, price):
        levels, prices = (self.bid_levels, self.bid_prices) if side == "buy" \
            else (self.ask_levels, self.ask_prices)
        if not levels.get(price):
            levels.pop(price, None)
            # prices is small; list.remove is fine here
            try:
                prices.remove(price)
            except ValueError:
                pass

    def _match(self, taker_id, taker_side, limit_px, qty):
        """Match an aggressive order against the opposite side. Returns fills."""
        fills = []
        if taker_side == "buy":
            opp_levels, opp_prices, maker_side = self.ask_levels, self.ask_prices, "sell"
            crosses = lambda best: (limit_px is None) or (best <= limit_px)
            best_of = lambda: opp_prices[0] if opp_prices else None
        else:
            opp_levels, opp_prices, maker_side = self.bid_levels, self.bid_prices, "buy"
            crosses = lambda best: (limit_px is None) or (best >= limit_px)
            best_of = lambda: opp_prices[-1] if opp_prices else None

        while qty > 0:
            best = best_of()
            if best is None or not crosses(best):
                break
            level = opp_levels[best]
            for maker_id in list(level):          # insertion order == time priority
                maker_qty = level[maker_id]
                trade = qty if qty < maker_qty else maker_qty
                fills.append((best, trade, maker_id))
                qty -= trade
                maker_qty -= trade
                if maker_qty == 0:
                    del level[maker_id]
                    self.resting.pop(maker_id, None)
                else:
                    level[maker_id] = maker_qty
                if qty == 0:
                    break
            self._remove_level_if_empty(maker_side, best)
        return fills, qty

    def limit(self, oid, side, px, qty):
        fills, remaining = self._match(oid, side, px, qty)
        if remaining > 0:
            self._rest(side, px, oid, remaining)
        return fills

    def market(self, oid, side, qty):
        fills, _remaining = self._match(oid, side, None, qty)
        # Unfilled market quantity is dropped (not rested).
        return fills

    def cancel(self, target):
        loc = self.resting.pop(target, None)
        if loc is None:
            return False
        side, price = loc
        levels = self.bid_levels if side == "buy" else self.ask_levels
        level = levels.get(price)
        if level and target in level:
            del level[target]
            self._remove_level_if_empty(side, price)
        return True


BOOK = OrderBook()


async def handle(ws, *_):
    async for raw in ws:
        # Binary frame -> binary protocol; text frame -> JSON (auto-detect).
        if isinstance(raw, (bytes, bytearray)):
            mtype, side, oid, px, qty, target = _REQ.unpack(raw)
            await ws.send(_ACK.pack(1, oid, time.time_ns()))   # ack first
            if mtype == 1:
                fills = BOOK.limit(oid, "buy" if side == 0 else "sell", px, qty)
            elif mtype == 2:
                fills = BOOK.market(oid, "buy" if side == 0 else "sell", qty)
            else:
                BOOK.cancel(target)
                fills = []
            for fpx, fq, maker in fills:
                await ws.send(_FILL.pack(2, oid, fpx, fq, maker))
            continue

        try:
            m = json.loads(raw)
            t = m["t"]
            oid = m["id"]
        except (ValueError, KeyError):
            continue

        # Acknowledge first — bots measure latency to this ack.
        await ws.send(f'{{"ack":{oid},"ts":{time.time_ns()}}}')

        try:
            if t == "limit":
                fills = BOOK.limit(oid, m["side"], int(m["px"]), int(m["qty"]))
            elif t == "market":
                fills = BOOK.market(oid, m["side"], int(m["qty"]))
            elif t == "cancel":
                BOOK.cancel(m["target"])
                fills = []
            else:
                await ws.send(f'{{"reject":{oid},"reason":"unknown type"}}')
                continue
        except Exception as exc:  # never let one bad order kill the connection
            await ws.send(f'{{"reject":{oid},"reason":"{type(exc).__name__}"}}')
            continue

        for px, q, maker in fills:
            await ws.send(f'{{"fill":{oid},"px":{px},"qty":{q},"maker":{maker}}}')


async def main():
    port = int(os.environ.get("ENGINE_PORT", "9000"))
    # Generous limits so the fleet can hammer the engine.
    async with websockets.serve(handle, "0.0.0.0", port, max_queue=None,
                                ping_interval=None):
        print(f"[reference_engine_py] price-time book listening on :{port}", flush=True)
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
