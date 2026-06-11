"""gRPC control-plane server (orchestrator side).

Fleet workers open a long-lived `Subscribe` stream and receive start/stop
Commands; the leader reports completion via `ReportDone`. This replaces the Redis
pub/sub command channel when CONTROL=grpc (coordination/state stay on Redis).
"""
import asyncio

import grpc

import arena_pb2
import arena_pb2_grpc


class ControlServicer(arena_pb2_grpc.ControlServicer):
    def __init__(self, on_done):
        self._subs = set()        # set[asyncio.Queue] — one per connected worker
        self._on_done = on_done   # async callback(run_id, submission_id)

    async def Subscribe(self, request, context):
        q = asyncio.Queue()
        self._subs.add(q)
        try:
            while True:
                yield await q.get()
        finally:
            self._subs.discard(q)

    async def ReportDone(self, request, context):
        await self._on_done(request.run_id, request.submission_id)
        return arena_pb2.Ack(ok=True)

    async def broadcast(self, cmd: arena_pb2.Command):
        for q in list(self._subs):
            q.put_nowait(cmd)


async def serve(servicer, port=50051):
    server = grpc.aio.server()
    arena_pb2_grpc.add_ControlServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{port}")
    await server.start()
    print(f"[orchestrator] gRPC control server on :{port}", flush=True)
    return server
