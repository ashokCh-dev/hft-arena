"""HFT Arena orchestrator.

Control plane: accepts submissions, sandboxes them, launches load runs, and
relays the live leaderboard to the dashboard over a WebSocket.
"""
import asyncio
import json
import os
import uuid

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
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

CH_CONTROL = "arena:control"   # orchestrator -> bot_fleet
CH_EVENTS = "arena:events"     # telemetry/orchestrator -> dashboard
CH_RUNEVENTS = "arena:runevents"  # bot_fleet -> orchestrator (run finished)
KEY_SNAPSHOT = "arena:snapshot"

app = FastAPI(title="HFT Arena Orchestrator")
app.mount("/static", StaticFiles(directory=STATIC), name="static")

r: aioredis.Redis = None


@app.on_event("startup")
async def _startup():
    global r
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    asyncio.create_task(_runevents_listener())


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


class Submission(BaseModel):
    language: str = "python"
    code: str
    name: str = "submission"


@app.post("/submissions")
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


@app.post("/runs")
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
    await r.publish(CH_CONTROL, json.dumps({
        "cmd": "start", "run_id": run_id, "submission": name,
        "submission_id": req.submission_id, "target": target,
        "bots": req.bots, "duration": req.duration,
    }))
    # Primary stop is the fleet's run-done event; this is a generous safety net
    # in case a worker dies mid-run (the open-loop sweep makes run length dynamic).
    asyncio.create_task(_auto_stop(req.submission_id, req.duration + 180, name))
    return {"run_id": run_id, "target": target, "bots": req.bots,
            "duration": req.duration}


async def _auto_stop(submission_id: str, after: int, name: str):
    await asyncio.sleep(after)
    await asyncio.to_thread(sandbox.stop, submission_id)


@app.post("/stop")
async def stop_all():
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
