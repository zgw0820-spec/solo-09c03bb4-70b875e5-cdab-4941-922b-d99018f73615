"""Demo upstream with configurable response delay, a health endpoint used by
the gateway's pre-switch probe, and a per-session monotonically increasing
call counter.

The counter is the observable proof of session stickiness: a multi-step
business session served by the same upstream sees 1, 2, 3, ... even if the
gateway route was switched to another upstream in between.  Switching to the
other upstream would restart at 1, which the web console shows explicitly.
"""
from __future__ import annotations

import asyncio
import os
import threading
import time

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

NAME = os.environ.get("UPSTREAM_NAME", "upstream")
DEFAULT_DELAY_MS = int(os.environ.get("DEFAULT_DELAY_MS", "0"))
MAX_DELAY_MS = 60_000

app = FastAPI(title=f"upstream-{NAME}")
_state = {"health_fail": os.environ.get("HEALTH_FAIL", "0") == "1"}
_seq_lock = threading.Lock()
_seq: dict[str, int] = {}


def next_seq(key: str) -> int:
    with _seq_lock:
        _seq[key] = _seq.get(key, 0) + 1
        return _seq[key]


@app.get("/health")
def health():
    if _state["health_fail"]:
        return JSONResponse(
            status_code=503, content={"status": "failing", "upstream": NAME}
        )
    return {"status": "ok", "upstream": NAME}


class HealthMode(BaseModel):
    fail: bool


@app.post("/admin/health_mode")
def set_health_mode(mode: HealthMode):
    """Demo hook: make /health fail or recover at runtime."""
    _state["health_fail"] = mode.fail
    return {"upstream": NAME, "health_fail": _state["health_fail"]}


@app.api_route("/work", methods=["GET", "POST"])
async def work(
    delay_ms: int | None = Query(default=None),
    session_id: str | None = Query(default=None),
):
    delay = DEFAULT_DELAY_MS if delay_ms is None else max(0, min(delay_ms, MAX_DELAY_MS))
    # The sequence is allocated before sleeping so concurrent calls of one
    # session get well-defined, increasing numbers.
    seq = next_seq(session_id) if session_id else None
    await asyncio.sleep(delay / 1000)
    body = {"upstream": NAME, "delay_ms": delay, "ts": time.time()}
    if session_id:
        body["session_id"] = session_id
        body["seq"] = seq
    return body


@app.get("/")
def root():
    return {"upstream": NAME, "default_delay_ms": DEFAULT_DELAY_MS}
