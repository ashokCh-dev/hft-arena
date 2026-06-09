#!/usr/bin/env python3
"""ADVERSARIAL submission: hostile resource hog.

Binds and passes the health check, then on the first real order it tries to
allocate gigabytes of memory. The sandbox's hard memory limit (512 MB, no swap)
must OOM-kill the container WITHOUT affecting the host or the platform services.
The bot fleet then sees the engine disappear (errors -> crash penalty -> ~0 score),
while redis/orchestrator/telemetry/timescaledb stay healthy.
"""
import asyncio
import json
import os
import time

import websockets

_ballast = []


async def handle(ws, *_):
    async for raw in ws:
        try:
            m = json.loads(raw)
            await ws.send(f'{{"ack":{m["id"]},"ts":{time.time_ns()}}}')
        except Exception:
            pass
        # Attempt to balloon memory far past the cgroup limit -> OOM kill.
        while True:
            _ballast.append(bytearray(50 * 1024 * 1024))  # 50 MB chunks


async def main():
    port = int(os.environ.get("ENGINE_PORT", "9000"))
    async with websockets.serve(handle, "0.0.0.0", port, max_queue=None,
                                ping_interval=None):
        print(f"[MEMBOMB] hostile engine on :{port}", flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
