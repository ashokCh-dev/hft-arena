#!/usr/bin/env python3
"""ADVERSARIAL submission: fast but WRONG.

A matching engine that acks quickly (good latency/throughput) but VIOLATES
price-time priority — at each price level it fills the NEWEST resting order first
(LIFO) instead of the oldest (FIFO). The Telemetry & Validation ingester's
correctness probe should catch this and tank the correctness score, proving the
platform rewards correct engineering, not just speed.
"""
import asyncio
import json
import os
import time
from bisect import insort

import websockets


class CheatBook:
    def __init__(self):
        self.bid_levels, self.ask_levels = {}, {}
        self.bid_prices, self.ask_prices = [], []
        self.resting = {}

    def _rest(self, side, price, oid, qty):
        levels, prices = (self.bid_levels, self.bid_prices) if side == "buy" \
            else (self.ask_levels, self.ask_prices)
        lvl = levels.get(price)
        if lvl is None:
            lvl = levels[price] = {}
            insort(prices, price)
        lvl[oid] = lvl.get(oid, 0) + qty
        self.resting[oid] = (side, price)

    def _match(self, side, limit_px, qty):
        fills = []
        if side == "buy":
            opp, prices, mside = self.ask_levels, self.ask_prices, "sell"
            crosses = lambda b: limit_px is None or b <= limit_px
            best = lambda: prices[0] if prices else None
        else:
            opp, prices, mside = self.bid_levels, self.bid_prices, "buy"
            crosses = lambda b: limit_px is None or b >= limit_px
            best = lambda: prices[-1] if prices else None
        while qty > 0:
            b = best()
            if b is None or not crosses(b):
                break
            lvl = opp[b]
            # >>> THE BUG: newest-first instead of oldest-first (breaks time priority)
            for mid in reversed(list(lvl)):
                trade = min(qty, lvl[mid])
                fills.append((b, trade, mid))
                qty -= trade
                lvl[mid] -= trade
                if lvl[mid] == 0:
                    del lvl[mid]
                    self.resting.pop(mid, None)
                if qty == 0:
                    break
            if not lvl:
                (self.ask_levels if mside == "sell" else self.bid_levels).pop(b, None)
                (self.ask_prices if mside == "sell" else self.bid_prices).remove(b)
        return fills, qty

    def limit(self, oid, side, px, qty):
        fills, rem = self._match(side, px, qty)
        if rem > 0:
            self._rest(side, px, oid, rem)
        return fills

    def market(self, oid, side, qty):
        return self._match(side, None, qty)[0]

    def cancel(self, target):
        loc = self.resting.pop(target, None)
        if not loc:
            return
        side, price = loc
        lvl = (self.bid_levels if side == "buy" else self.ask_levels).get(price)
        if lvl and target in lvl:
            del lvl[target]


BOOK = CheatBook()


async def handle(ws, *_):
    async for raw in ws:
        try:
            m = json.loads(raw); t = m["t"]; oid = m["id"]
        except (ValueError, KeyError):
            continue
        await ws.send(f'{{"ack":{oid},"ts":{time.time_ns()}}}')
        if t == "limit":
            fills = BOOK.limit(oid, m["side"], int(m["px"]), int(m["qty"]))
        elif t == "market":
            fills = BOOK.market(oid, m["side"], int(m["qty"]))
        else:
            BOOK.cancel(m.get("target")); fills = []
        for px, q, maker in fills:
            await ws.send(f'{{"fill":{oid},"px":{px},"qty":{q},"maker":{maker}}}')


async def main():
    port = int(os.environ.get("ENGINE_PORT", "9000"))
    async with websockets.serve(handle, "0.0.0.0", port, max_queue=None,
                                ping_interval=None):
        print(f"[CHEATER] LIFO (wrong) engine on :{port}", flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
