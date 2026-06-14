"""HFT Arena orchestrator.

Control plane: accepts submissions, sandboxes them, launches load runs, and
relays the live leaderboard to the dashboard over a WebSocket.
"""
import asyncio
import json
import os
import uuid

from fastapi import Depends, FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import redis.asyncio as aioredis

# Sandbox backend: "docker" (build+run via socket) or "k8s" (Pod via the K8s API).
if os.environ.get("SANDBOX_BACKEND") == "k8s":
    import sandbox_k8s as sandbox
else:
    import sandbox

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")
STATIC = os.path.join(os.path.dirname(__file__), "static")
# Mutating endpoints (/submissions, /runs, /stop) require this key when set; if
# unset, auth is disabled (open) for local dev. Read-only views stay public.
ARENA_API_KEY = os.environ.get("ARENA_API_KEY")


async def require_key(x_api_key: str = Header(default=None)):
    if ARENA_API_KEY and x_api_key != ARENA_API_KEY:
        raise HTTPException(status_code=401, detail="invalid or missing API key")

CH_CONTROL = "arena:control"   # orchestrator -> bot_fleet (redis mode) + telemetry metadata
CH_EVENTS = "arena:events"     # telemetry/orchestrator -> dashboard
CH_RUNEVENTS = "arena:runevents"  # bot_fleet -> orchestrator (run finished)
KEY_SNAPSHOT = "arena:snapshot"

# Control transport for orchestrator->fleet commands: "redis" pub/sub or "grpc".
CONTROL = os.environ.get("CONTROL", "redis")

app = FastAPI(title="HFT Arena Orchestrator")
app.mount("/static", StaticFiles(directory=STATIC), name="static")

r: aioredis.Redis = None
_grpc = None          # ControlServicer when CONTROL == "grpc"
_grpc_server = None   # keep a reference so the gRPC server isn't garbage-collected


@app.on_event("startup")
async def _startup():
    global r, _grpc, _grpc_server
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    if CONTROL == "grpc":
        import control_grpc
        _grpc = control_grpc.ControlServicer(on_done=_on_run_done)
        _grpc_server = await control_grpc.serve(_grpc)
    else:
        asyncio.create_task(_runevents_listener())


async def _on_run_done(run_id, submission_id):
    """gRPC ReportDone handler: stop the sandbox and let telemetry persist."""
    if submission_id:
        await asyncio.to_thread(sandbox.stop, submission_id)
    # telemetry persists off CH_RUNEVENTS regardless of control transport
    await r.publish(CH_RUNEVENTS, json.dumps(
        {"run_id": run_id, "submission_id": submission_id, "done": True}))
    await publish_event({"status": "Run complete.", "running": False})


async def _runevents_listener():
    """Stop a submission's sandbox once the bot fleet reports the run is done."""
    pubsub = r.pubsub()
    await pubsub.subscribe(CH_RUNEVENTS)
    async for m in pubsub.listen():
        if m.get("type") != "message":
            continue
        ev = json.loads(m["data"])
        if ev.get("done") and ev.get("submission_id"):
            await asyncio.to_thread(sandbox.stop, ev["submission_id"])
            await publish_event({"status": "Run complete.", "running": False})


async def publish_event(payload: dict):
    msg = json.dumps(payload)
    await r.set(KEY_SNAPSHOT, msg)
    await r.publish(CH_EVENTS, msg)


@app.get("/")
async def index():
    return FileResponse(os.path.join(STATIC, "index.html"))


@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/history")
async def history():
    """Recent runs persisted to TimescaleDB (maintained by telemetry)."""
    raw = await r.get("arena:history")
    return json.loads(raw) if raw else {"type": "history", "runs": []}


@app.get("/trends")
async def trends():
    """Per-submission percentile + latency-jitter trend over time, from the
    TimescaleDB `runs_rollup` continuous aggregate (maintained by telemetry)."""
    raw = await r.get("arena:trends")
    return json.loads(raw) if raw else {"type": "trends", "rows": []}


class Submission(BaseModel):
    language: str = "python"
    code: str
    name: str = "submission"


@app.post("/submissions", dependencies=[Depends(require_key)])
async def create_submission(sub: Submission):
    submission_id = uuid.uuid4().hex[:8]
    await publish_event({"status": f"Building {sub.name} ({sub.language})…"})
    try:
        # Docker build is blocking — keep the event loop responsive.
        await asyncio.to_thread(sandbox.build_image, submission_id, sub.language, sub.code)
    except Exception as exc:
        await publish_event({"status": f"Build failed: {exc}"})
        return {"error": str(exc)}, 400
    await r.hset(f"arena:submission:{submission_id}",
                 mapping={"name": sub.name, "language": sub.language})
    await publish_event({"status": f"Built {sub.name}. Ready to run."})
    return {"submission_id": submission_id, "name": sub.name}


class RunReq(BaseModel):
    submission_id: str
    bots: int = 250
    duration: int = 30


@app.post("/runs", dependencies=[Depends(require_key)])
async def start_run(req: RunReq):
    meta = await r.hgetall(f"arena:submission:{req.submission_id}")
    if not meta:
        return {"error": "unknown submission_id"}, 404
    name = meta.get("name", req.submission_id)
    run_id = uuid.uuid4().hex[:8]

    lang = meta.get("language", "python")
    await publish_event({"status": f"Deploying sandbox for {name}…"})
    await asyncio.to_thread(sandbox.launch, req.submission_id, lang)
    healthy = await asyncio.to_thread(sandbox.wait_healthy, req.submission_id)
    if not healthy:
        tail = await asyncio.to_thread(sandbox.logs, req.submission_id)
        await publish_event({"status": f"Sandbox failed health check. logs: {tail[:200]}"})
        return {"error": "sandbox unhealthy"}, 500

    target = f"ws://{sandbox.target_host(req.submission_id)}:9000"
    # Tell telemetry which run/submission is live, then start the bot fleet.
    await publish_event({"status": f"Attacking {name}…", "running": True,
                         "submission": name})
    cmd = {"cmd": "start", "run_id": run_id, "submission": name,
           "submission_id": req.submission_id, "target": target,
           "bots": req.bots, "duration": req.duration}
    # CH_CONTROL always carries the start so telemetry can label run_id->submission;
    # in redis mode the fleet also consumes it. In grpc mode the fleet gets it over
    # the gRPC stream instead.
    await r.publish(CH_CONTROL, json.dumps(cmd))
    if CONTROL == "grpc":
        import arena_pb2
        await _grpc.broadcast(arena_pb2.Command(
            cmd="start", run_id=run_id, submission=name,
            submission_id=req.submission_id, target=target,
            bots=req.bots, duration=req.duration))
    # Primary stop is the fleet's run-done event; this is a generous safety net
    # in case a worker dies mid-run (the open-loop sweep makes run length dynamic).
    asyncio.create_task(_auto_stop(req.submission_id, req.duration + 180, name))
    return {"run_id": run_id, "target": target, "bots": req.bots,
            "duration": req.duration}


async def _auto_stop(submission_id: str, after: int, name: str):
    await asyncio.sleep(after)
    await asyncio.to_thread(sandbox.stop, submission_id)


@app.post("/stop", dependencies=[Depends(require_key)])
async def stop_all():
    if CONTROL == "grpc":
        import arena_pb2
        await _grpc.broadcast(arena_pb2.Command(cmd="stop"))
    else:
        await r.publish(CH_CONTROL, json.dumps({"cmd": "stop"}))
    return {"stopped": True}


@app.websocket("/ws/leaderboard")
async def ws_leaderboard(ws: WebSocket):
    await ws.accept()
    # Replay the latest snapshot so a fresh dashboard isn't blank.
    snap = await r.get(KEY_SNAPSHOT)
    if snap:
        await ws.send_text(snap)
    pubsub = r.pubsub()
    await pubsub.subscribe(CH_EVENTS)
    try:
        async for m in pubsub.listen():
            if m.get("type") == "message":
                await ws.send_text(m["data"])
    except WebSocketDisconnect:
        pass
    finally:
        await pubsub.unsubscribe(CH_EVENTS)
        await pubsub.aclose()
