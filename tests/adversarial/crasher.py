#!/usr/bin/env python3
"""ADVERSARIAL submission: unstable engine that crashes under load.

Acks normally, but hard-exits the process after a small number of orders to
simulate a buggy engine that falls over once the bot fleet ramps up. The platform
must stay up; the bot fleet records the disconnects (errors -> crash penalty), and
the run still completes and is scored/persisted.
"""
import asyncio
import json
import os
import time

import websockets

_count = 0


async def handle(ws, *_):
    global _count
    async for raw in ws:
        try:
            m = json.loads(raw)
            await ws.send(f'{{"ack":{m["id"]},"ts":{time.time_ns()}}}')
        except Exception:
            pass
        _count += 1
        if _count > 5000:                    # fall over once the load ramps
            print("[CRASHER] simulated crash", flush=True)
            os._exit(1)


async def main():
    port = int(os.environ.get("ENGINE_PORT", "9000"))
    async with websockets.serve(handle, "0.0.0.0", port, max_queue=None,
                                ping_interval=None):
        print(f"[CRASHER] unstable engine on :{port}", flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
