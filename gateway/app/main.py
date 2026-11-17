"""Edge gateway: proxies requests to the currently routed upstream and
exposes the operations API + web console for on-duty target switching.

Sessions opened from the web console are sticky: the first admitted request
binds the session to the target that is effective at that moment, and every
later request of that session is served by the same target even if the route
has been switched meanwhile.  Requests without a session follow the effective
route.  A bound target that has become unreachable fails the session request
with an error instead of silently falling back to the new route.
"""
from __future__ import annotations

import asyncio
import os
import socket
import time
import uuid
from contextlib import asynccontextmanager, suppress
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from . import db

STATIC_DIR = Path(__file__).parent / "static"


def parse_upstreams() -> dict[str, str]:
    raw = os.environ.get(
        "UPSTREAMS", "alpha=http://127.0.0.1:9001;beta=http://127.0.0.1:9002"
    )
    out: dict[str, str] = {}
    for part in raw.split(";"):
        part = part.strip()
        if not part:
            continue
        name, _, url = part.partition("=")
        out[name.strip()] = url.strip().rstrip("/")
    return out


UPSTREAMS = parse_upstreams()
INITIAL_TARGET = os.environ.get("INITIAL_TARGET") or next(iter(UPSTREAMS))
# Stable per gateway instance.  Compose sets GATEWAY_ID explicitly for each
# gateway sharing the SQLite file.  When it is absent we derive the identity
# from the database path, so a restarted process recognises its own previous
# in-flight rows and clears them at boot.  Two gateways sharing one database
# MUST set distinct GATEWAY_IDs.
GATEWAY_ID = os.environ.get("GATEWAY_ID") or (
    f"{socket.gethostname()}:{os.environ.get('GATEWAY_DB', '/data/gateway.db')}"
)
RUN_ID = f"{GATEWAY_ID}-{uuid.uuid4().hex[:12]}"


async def heartbeat_loop(app: FastAPI, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            info = await asyncio.to_thread(db.heartbeat)
            app.state.heartbeat = info
        except Exception as exc:  # noqa: BLE001 - keep the gateway serving
            app.state.last_heartbeat_error = str(exc)
        try:
            await asyncio.wait_for(
                stop.wait(), timeout=db.GATEWAY_HEARTBEAT_SECONDS
            )
        except asyncio.TimeoutError:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.boot_info = db.init_db(UPSTREAMS, INITIAL_TARGET, GATEWAY_ID, RUN_ID)
    app.state.http = httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=3.0))
    app.state.heartbeat = {}
    app.state.last_heartbeat_error = None
    stop = asyncio.Event()
    task = asyncio.create_task(heartbeat_loop(app, stop))
    try:
        yield
    finally:
        stop.set()
        with suppress(asyncio.CancelledError):
            await task
        await app.state.http.aclose()


app = FastAPI(title="edge-gateway", lifespan=lifespan)


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
def health():
    return {"ok": True, "gateway_id": GATEWAY_ID, "run_id": RUN_ID}


@app.get("/api/state")
async def state():
    s = await asyncio.to_thread(db.get_state)
    s["boot"] = app.state.boot_info
    s["gateway_id"] = GATEWAY_ID
    s["heartbeat"] = app.state.heartbeat
    return s


class CallRequest(BaseModel):
    delay_ms: int | None = Field(default=None, ge=0, le=60_000)
    path: str = "/work"
    session_id: str | None = None


@app.post("/api/call")
async def call(req: CallRequest):
    """Admit a request and proxy it to the target decided at admission time.

    For a session request that target is the session's bound target (bound on
    the first call of the session), so the full multi-step business stays on
    the original service across route switches.  If that target is down the
    request fails; it is never re-routed to the new target."""
    path = req.path if req.path.startswith("/") else "/work"
    if "://" in path or ".." in path:
        return JSONResponse(status_code=400, content={"ok": False, "error": "bad_path"})
    session_id = (req.session_id or "").strip() or None
    # SQLite writes are short but can block on the shared file, so keep them
    # off the event loop.
    admission = await asyncio.to_thread(db.admit, path, session_id)
    if "error" in admission:
        code = 404 if admission["error"] == "session_not_found" else 409
        return JSONResponse(
            status_code=code,
            content={
                "ok": False,
                "error": admission["error"],
                "session_id": session_id,
                "target": admission.get("target"),
            },
        )
    target_url = await asyncio.to_thread(db.upstream_url, admission["target"])
    started = time.monotonic()
    try:
        if target_url is None:
            raise httpx.ConnectError(f"unknown target {admission['target']!r}")
        params = {} if req.delay_ms is None else {"delay_ms": req.delay_ms}
        # Pass the business session through so the demo upstream can show its
        # per-session call sequence.
        if admission["session_id"]:
            params["session_id"] = admission["session_id"]
        resp = await app.state.http.get(f"{target_url}{path}", params=params)
        try:
            body = resp.json()
        except ValueError:
            body = {"text": resp.text[:500]}
        return {
            "ok": resp.status_code == 200,
            "status": resp.status_code,
            "request_id": admission["id"],
            "target": admission["target"],
            "route_version": admission["version"],
            "session_id": admission["session_id"],
            "session_bound_now": admission["bound_new"],
            "duration_ms": int((time.monotonic() - started) * 1000),
            "upstream": body,
        }
    except httpx.HTTPError as exc:
        # In particular: a bound session whose original target is unreachable
        # must surface an error, not be served by the newly routed target.
        return JSONResponse(
            status_code=502,
            content={
                "ok": False,
                "error": f"upstream_error: {exc}",
                "request_id": admission["id"],
                "target": admission["target"],
                "route_version": admission["version"],
                "session_id": admission["session_id"],
            },
        )
    finally:
        await asyncio.to_thread(db.finish, admission["id"])


class OpenSessionRequest(BaseModel):
    session_id: str | None = None


@app.post("/api/session/open")
async def open_session(req: OpenSessionRequest):
    res = await asyncio.to_thread(db.open_session, req.session_id, GATEWAY_ID)
    if not res["ok"]:
        return JSONResponse(status_code=409, content=res)
    return res


class SessionRef(BaseModel):
    session_id: str


@app.post("/api/session/close")
async def close_session(req: SessionRef):
    res = await asyncio.to_thread(db.close_session, req.session_id)
    if not res["ok"]:
        return JSONResponse(status_code=404, content=res)
    return res


class ProbeRequest(BaseModel):
    target: str
    base_version: int


@app.post("/api/switch/probe")
async def probe(req: ProbeRequest):
    """Probe a candidate target. Never mutates the route: while probing, new
    requests keep flowing to the old target, and a failed probe leaves the
    original route untouched."""
    route = await asyncio.to_thread(db.get_route)
    url = await asyncio.to_thread(db.upstream_url, req.target)
    if url is None:
        return JSONResponse(
            status_code=404, content={"ok": False, "error": "unknown_target"}
        )
    if req.target == route["current_target"]:
        return {"ok": False, "error": "target_is_current", "current_version": route["version"]}
    ok, detail = False, {}
    try:
        health = await app.state.http.get(f"{url}/health", timeout=2.5)
        work = await app.state.http.get(
            f"{url}/work", params={"delay_ms": 0}, timeout=5.0
        )
        ok = health.status_code == 200 and work.status_code == 200
        detail = {"health_status": health.status_code, "work_status": work.status_code}
    except httpx.HTTPError as exc:
        detail = {"error": str(exc)}
    rec = await asyncio.to_thread(
        db.record_probe, req.target, req.base_version, ok, detail
    )
    return {
        "ok": ok,
        "probe_id": rec["id"],
        "target": req.target,
        "base_version": req.base_version,
        "current_version": route["version"],
        "version_matches": route["version"] == req.base_version,
        "detail": detail,
    }


class CommitRequest(BaseModel):
    target: str
    base_version: int
    probe_id: str


@app.post("/api/switch/commit")
async def commit(req: CommitRequest):
    """Confirm a switch. Version-guarded: once this commits, newly admitted
    requests without a session immediately go to the new target without
    waiting for old in-flight requests; a stale commit is rejected with 409."""
    res = await asyncio.to_thread(
        db.commit_switch, req.target, req.base_version, req.probe_id
    )
    if not res["ok"]:
        code = 409 if res["error"] == "version_conflict" else 400
        return JSONResponse(status_code=code, content=res)
    return res
