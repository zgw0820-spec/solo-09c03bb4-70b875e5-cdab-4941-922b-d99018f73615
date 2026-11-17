"""SQLite persistence for the edge gateway.

The database is shared by more than one gateway process (the compose demo runs
two).  Every mutation therefore uses an explicit ``BEGIN IMMEDIATE``
transaction so that "first admission binds a session", "close session" and
"commit route switch" are serialised across processes by SQLite itself.

Tables:
  upstreams  - registered upstream targets (seeded from env, upserted on boot)
  routes     - single-row table holding the effective route and its version
  sessions   - a web-opened session.  ``target`` stays NULL until the first
               admitted request binds it to the route that is effective at
               that moment.  A session is sticky: later calls keep using the
               bound target even after the route switches.  Closed sessions
               only reject *new* admissions; already admitted requests finish.
  inflight   - one row per admitted proxied request, tagged with the owning
               gateway instance and run.  A gateway restart clears only its
               own previous-run rows, so it never wipes a peer's live requests.
  gateways   - heartbeat/lease per gateway instance.  In-flight rows of a
               gateway whose heartbeat expired are treated as stale (and
               reaped), so a crashed peer cannot block "can be taken offline"
               forever.
  probes     - audit log of candidate probes; a commit requires a fresh,
               successful probe recorded against the same route version
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

DB_PATH = Path(os.environ.get("GATEWAY_DB", "/data/gateway.db"))
PROBE_TTL_SECONDS = float(os.environ.get("PROBE_TTL_SECONDS", "60"))
GATEWAY_LEASE_SECONDS = float(os.environ.get("GATEWAY_LEASE_SECONDS", "6"))
GATEWAY_HEARTBEAT_SECONDS = float(os.environ.get("GATEWAY_HEARTBEAT_SECONDS", "1.5"))

_lock = threading.RLock()
_conn: sqlite3.Connection | None = None
_gateway_id: str | None = None
_run_id: str | None = None


def _is_busy(exc: BaseException) -> bool:
    text = str(exc).lower()
    return "locked" in text or "busy" in text


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    # isolation_level=None -> autocommit; transactions are opened explicitly so
    # that both processes always take a write lock up-front (BEGIN IMMEDIATE)
    # instead of failing on a deferred read -> write lock upgrade.
    conn = sqlite3.connect(
        str(DB_PATH), timeout=30.0, isolation_level=None, check_same_thread=False
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=15000")
    for attempt in range(20):
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            break
        except sqlite3.OperationalError as exc:
            if not _is_busy(exc) or attempt == 19:
                raise
            time.sleep(0.05 * (attempt + 1))
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _begin(immediate: bool) -> None:
    assert _conn is not None
    sql = "BEGIN IMMEDIATE" if immediate else "BEGIN"
    for attempt in range(12):
        try:
            _conn.execute(sql)
            return
        except sqlite3.OperationalError as exc:
            if not _is_busy(exc) or attempt == 11:
                raise
            time.sleep(0.02 * (attempt + 1))


@contextmanager
def _tx(immediate: bool = True):
    """Run statements in one SQLite transaction.

    With BEGIN IMMEDIATE the write lock is taken before any read, which is what
    makes admission/binding, close and switch serialisation deterministic
    across gateway processes.
    """
    assert _conn is not None
    _begin(immediate)
    try:
        yield _conn
    except BaseException:
        try:
            _conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    else:
        _conn.execute("COMMIT")


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}


def _reap_stale(conn: sqlite3.Connection, now: float) -> int:
    """Delete in-flight rows that no live gateway owns.

    A gateway instance is live while its heartbeat is within the lease.  Rows
    with no gateway tag are legacy/interrupted rows and are never owned by a
    running process, so they are stale too.  The current gateway always has a
    fresh heartbeat (set on boot and by the heartbeat task).
    """
    stale_gateways = [
        row["gateway_id"]
        for row in conn.execute(
            "SELECT gateway_id FROM gateways WHERE heartbeat_at < ?",
            (now - GATEWAY_LEASE_SECONDS,),
        )
    ]
    dead = 0
    if stale_gateways:
        marks = ",".join("?" for _ in stale_gateways)
        dead += conn.execute(
            f"DELETE FROM inflight WHERE gateway_id IN ({marks})", stale_gateways
        ).rowcount
    dead += conn.execute("DELETE FROM inflight WHERE gateway_id IS NULL").rowcount
    return dead


def init_db(
    upstreams: dict[str, str], initial_target: str, gateway_id: str, run_id: str
) -> dict:
    """Create/migrate tables, upsert the registry, seed the route once, and
    drop in-flight rows left behind by previous runs of *this* gateway or by
    crashed peers.  Unclosed sessions are deliberately preserved."""
    global _conn, _gateway_id, _run_id
    if not upstreams:
        raise RuntimeError("no upstreams configured")
    if initial_target not in upstreams:
        raise RuntimeError(f"initial target {initial_target!r} not in upstreams")
    _gateway_id, _run_id = gateway_id, run_id
    _conn = _connect()
    now = time.time()
    with _lock, _tx():
        _conn.execute(
            """
            CREATE TABLE IF NOT EXISTS upstreams (
                name       TEXT PRIMARY KEY,
                url        TEXT NOT NULL,
                retired_at REAL
            )
            """
        )
        _conn.execute(
            """
            CREATE TABLE IF NOT EXISTS routes (
                id             INTEGER PRIMARY KEY CHECK (id = 1),
                current_target TEXT NOT NULL,
                version        INTEGER NOT NULL,
                updated_at     REAL NOT NULL
            )
            """
        )
        _conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                id         TEXT PRIMARY KEY,
                target     TEXT,
                status     TEXT NOT NULL CHECK (status IN ('open', 'closed')),
                opened_by  TEXT,
                created_at REAL NOT NULL,
                bound_at   REAL,
                closed_at  REAL
            )
            """
        )
        _conn.execute(
            """
            CREATE TABLE IF NOT EXISTS inflight (
                id         TEXT PRIMARY KEY,
                target     TEXT NOT NULL,
                path       TEXT NOT NULL,
                started_at REAL NOT NULL,
                session_id TEXT,
                gateway_id TEXT,
                run_id     TEXT
            )
            """
        )
        _conn.execute(
            """
            CREATE TABLE IF NOT EXISTS gateways (
                gateway_id   TEXT PRIMARY KEY,
                run_id       TEXT NOT NULL,
                started_at   REAL NOT NULL,
                heartbeat_at REAL NOT NULL
            )
            """
        )
        _conn.execute(
            """
            CREATE TABLE IF NOT EXISTS probes (
                id           TEXT PRIMARY KEY,
                target       TEXT NOT NULL,
                base_version INTEGER NOT NULL,
                ok           INTEGER NOT NULL,
                detail       TEXT,
                created_at   REAL NOT NULL
            )
            """
        )
        # Migrate a v1 (single-gateway) database in place.
        inflight_cols = _columns(_conn, "inflight")
        if "session_id" not in inflight_cols:
            _conn.execute("ALTER TABLE inflight ADD COLUMN session_id TEXT")
        if "gateway_id" not in inflight_cols:
            _conn.execute("ALTER TABLE inflight ADD COLUMN gateway_id TEXT")
        if "run_id" not in inflight_cols:
            _conn.execute("ALTER TABLE inflight ADD COLUMN run_id TEXT")
        upstream_cols = _columns(_conn, "upstreams")
        if "retired_at" not in upstream_cols:
            # A v1 database has no history of who ever carried traffic, so the
            # migration deliberately leaves retired_at NULL: the can-offline
            # hint stays off (fail-closed) until the next switch marks the
            # then-current target as an old target.
            _conn.execute("ALTER TABLE upstreams ADD COLUMN retired_at REAL")
        session_cols = _columns(_conn, "sessions")
        if "opened_by" not in session_cols:
            _conn.execute("ALTER TABLE sessions ADD COLUMN opened_by TEXT")
        _conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_inflight_target ON inflight(target)"
        )
        _conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_inflight_gateway ON inflight(gateway_id)"
        )
        _conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sessions_status_target "
            "ON sessions(status, target)"
        )

        for name, url in upstreams.items():
            _conn.execute(
                "INSERT INTO upstreams (name, url) VALUES (?, ?) "
                "ON CONFLICT(name) DO UPDATE SET url = excluded.url",
                (name, url),
            )
        seeded = False
        if _conn.execute("SELECT COUNT(*) AS c FROM routes").fetchone()["c"] == 0:
            _conn.execute(
                "INSERT INTO routes (id, current_target, version, updated_at) "
                "VALUES (1, ?, 0, ?)",
                (initial_target, now),
            )
            seeded = True
        # Register this run before counting live gateways, then drop rows from
        # our own previous runs.  Sibling gateway rows are left alone here; the
        # lease logic below decides whether a peer is still alive.
        _conn.execute(
            "INSERT INTO gateways (gateway_id, run_id, started_at, heartbeat_at) "
            "VALUES (?, ?, ?, ?) ON CONFLICT(gateway_id) DO UPDATE SET "
            "run_id = excluded.run_id, started_at = excluded.started_at, "
            "heartbeat_at = excluded.heartbeat_at",
            (gateway_id, run_id, now, now),
        )
        cleared_own = _conn.execute(
            "DELETE FROM inflight WHERE gateway_id = ? AND "
            "(run_id IS NULL OR run_id != ?)",
            (gateway_id, run_id),
        ).rowcount
        reaped = _reap_stale(_conn, now)
    return {
        "seeded_route": seeded,
        # Total stale rows removed at boot: our own interrupted ones plus rows
        # of crashed peers whose lease already expired.
        "cleared_inflight": cleared_own + reaped,
        "cleared_own_inflight": cleared_own,
        "reaped_peer_inflight": reaped,
        "gateway_id": gateway_id,
        "run_id": run_id,
    }


def _db() -> sqlite3.Connection:
    assert _conn is not None, "db not initialised"
    return _conn


def heartbeat() -> dict:
    """Refresh this gateway's lease and reap rows of gateways that stopped
    heartbeating (crashed peers).  Called periodically from the event loop."""
    now = time.time()
    with _lock, _tx():
        _db().execute(
            "UPDATE gateways SET heartbeat_at = ? WHERE gateway_id = ?",
            (now, _gateway_id),
        )
        reaped = _reap_stale(_db(), now)
        live = [
            row["gateway_id"]
            for row in _db().execute(
                "SELECT gateway_id FROM gateways WHERE heartbeat_at >= ?",
                (now - GATEWAY_LEASE_SECONDS,),
            )
        ]
    return {"reaped_inflight": reaped, "live_gateways": live}


def get_route() -> dict:
    with _lock, _tx(immediate=False):
        row = _db().execute(
            "SELECT current_target, version, updated_at FROM routes WHERE id = 1"
        ).fetchone()
    return dict(row)


def upstream_url(name: str) -> str | None:
    with _lock:
        row = _db().execute(
            "SELECT url FROM upstreams WHERE name = ?", (name,)
        ).fetchone()
    return row["url"] if row else None


def admit(path: str, session_id: str | None) -> dict:
    """Atomically decide the target for one request, bind a session on its
    first admission, and register the request as in-flight.

    Because the whole decision runs under BEGIN IMMEDIATE, a route switch and
    a session close (both writers) cannot interleave halfway through it:
      * a first admission sees exactly one effective route and binds it,
      * an admission either precedes a close (and is already admitted, so it
        still runs) or observes the close and is rejected.
    """
    rid = uuid.uuid4().hex
    with _lock, _tx():
        route = _db().execute(
            "SELECT current_target, version FROM routes WHERE id = 1"
        ).fetchone()
        bound_new = False
        if session_id is not None:
            session = _db().execute(
                "SELECT id, target, status FROM sessions WHERE id = ?", (session_id,)
            ).fetchone()
            if session is None:
                return {"error": "session_not_found", "id": rid}
            if session["status"] != "open":
                return {
                    "error": "session_closed",
                    "id": rid,
                    "session_id": session_id,
                    "target": session["target"],
                }
            target = session["target"]
            if target is None:
                target = route["current_target"]
                cur = _db().execute(
                    "UPDATE sessions SET target = ?, bound_at = ? "
                    "WHERE id = ? AND target IS NULL AND status = 'open'",
                    (target, time.time(), session_id),
                )
                if cur.rowcount != 1:  # cannot happen under BEGIN IMMEDIATE
                    return {"error": "session_bind_conflict", "id": rid}
                bound_new = True
        else:
            target = route["current_target"]
        _db().execute(
            "INSERT INTO inflight "
            "(id, target, path, started_at, session_id, gateway_id, run_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (rid, target, path, time.time(), session_id, _gateway_id, _run_id),
        )
    return {
        "id": rid,
        "target": target,
        "version": route["version"],
        "session_id": session_id,
        "bound_new": bound_new,
    }


def finish(request_id: str) -> None:
    with _lock, _tx():
        _db().execute("DELETE FROM inflight WHERE id = ?", (request_id,))


def open_session(session_id: str | None = None, opened_by: str | None = None) -> dict:
    """Open a session, or return the existing one.  Opening an already closed
    session is rejected: closed sessions must never be resurrected, otherwise
    a late call could silently be accepted again."""
    sid = (session_id or "").strip() or uuid.uuid4().hex
    now = time.time()
    with _lock, _tx():
        row = _db().execute(
            "SELECT id, target, status, opened_by, created_at, bound_at, closed_at "
            "FROM sessions WHERE id = ?",
            (sid,),
        ).fetchone()
        if row is not None:
            if row["status"] != "open":
                return {
                    "ok": False,
                    "error": "session_closed",
                    "session": dict(row),
                }
            return {"ok": True, "created": False, "session": dict(row)}
        _db().execute(
            "INSERT INTO sessions (id, target, status, opened_by, created_at) "
            "VALUES (?, NULL, 'open', ?, ?)",
            (sid, opened_by or _gateway_id, now),
        )
    return {
        "ok": True,
        "created": True,
        "session": {
            "id": sid,
            "target": None,
            "status": "open",
            "opened_by": opened_by or _gateway_id,
            "created_at": now,
            "bound_at": None,
            "closed_at": None,
        },
    }


def close_session(session_id: str) -> dict:
    """Stop admitting new requests for the session.  In-flight requests are
    untouched and still return their responses.  Idempotent so two gateway
    processes closing concurrently agree on the same final state."""
    with _lock, _tx():
        row = _db().execute(
            "SELECT id, target, status, opened_by, created_at, bound_at, closed_at "
            "FROM sessions WHERE id = ?",
            (session_id,),
        ).fetchone()
        if row is None:
            return {"ok": False, "error": "session_not_found"}
        already = row["status"] == "closed"
        if not already:
            _db().execute(
                "UPDATE sessions SET status = 'closed', closed_at = ? WHERE id = ?",
                (time.time(), session_id),
            )
            row = _db().execute(
                "SELECT id, target, status, opened_by, created_at, bound_at, closed_at "
                "FROM sessions WHERE id = ?",
                (session_id,),
            ).fetchone()
        return {"ok": True, "already_closed": already, "session": dict(row)}


def record_probe(target: str, base_version: int, ok: bool, detail: dict) -> dict:
    """A probe never mutates the route; it only records an auditable result."""
    pid = uuid.uuid4().hex
    with _lock, _tx():
        _db().execute(
            "INSERT INTO probes (id, target, base_version, ok, detail, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (pid, target, base_version, 1 if ok else 0, json.dumps(detail), time.time()),
        )
    return {"id": pid}


def commit_switch(target: str, base_version: int, probe_id: str) -> dict:
    """Compare-and-swap the route on its version. A commit carrying a stale
    version (e.g. a late probe from an interleaved switch attempt) loses and
    cannot overwrite the already-effective route.

    Session bindings are intentionally unaffected: bound sessions keep their
    original target, which is the whole point of session stickiness.
    """
    with _lock, _tx():
        route = _db().execute(
            "SELECT current_target, version FROM routes WHERE id = 1"
        ).fetchone()
        if route["version"] != base_version:
            return {
                "ok": False,
                "error": "version_conflict",
                "current_version": route["version"],
                "current_target": route["current_target"],
            }
        if route["current_target"] == target:
            return {
                "ok": False,
                "error": "target_is_current",
                "current_version": route["version"],
                "current_target": route["current_target"],
            }
        if upstream_url(target) is None:
            return {"ok": False, "error": "unknown_target"}
        probe = _db().execute(
            "SELECT * FROM probes WHERE id = ?", (probe_id,)
        ).fetchone()
        if (
            probe is None
            or not probe["ok"]
            or probe["target"] != target
            or probe["base_version"] != base_version
        ):
            return {"ok": False, "error": "probe_invalid"}
        if time.time() - probe["created_at"] > PROBE_TTL_SECONDS:
            return {"ok": False, "error": "probe_stale"}
        switched_at = time.time()
        cur = _db().execute(
            "UPDATE routes SET current_target = ?, version = version + 1, updated_at = ? "
            "WHERE id = 1 AND version = ?",
            (target, switched_at, base_version),
        )
        if cur.rowcount != 1:  # guarded by BEGIN IMMEDIATE; belt and braces
            return {"ok": False, "error": "version_conflict"}
        # The target we just left becomes an "old target" from now on; the new
        # current target is no longer retired.
        _db().execute(
            "UPDATE upstreams SET retired_at = ? WHERE name = ?",
            (switched_at, route["current_target"]),
        )
        _db().execute(
            "UPDATE upstreams SET retired_at = NULL WHERE name = ?", (target,)
        )
        return {"ok": True, "current_target": target, "version": base_version + 1}


def get_state() -> dict:
    """Read one consistent snapshot: route, in-flight rows, sessions and the
    derived can-offline decision all come from the same transaction, so two
    gateways sharing the file never report contradictory conclusions.

    Only in-flight rows owned by a live gateway are counted.  A gateway that
    was killed stops heartbeating and its rows age out of the lease, so they
    cannot pin a target as "cannot be taken offline" forever."""
    now = time.time()
    with _lock, _tx(immediate=False):
        route = dict(
            _db().execute("SELECT * FROM routes WHERE id = 1").fetchone()
        )
        upstreams = [
            dict(r)
            for r in _db().execute(
                "SELECT name, url, retired_at FROM upstreams ORDER BY name"
            )
        ]
        gateways = [
            dict(r)
            for r in _db().execute("SELECT * FROM gateways ORDER BY gateway_id")
        ]
        all_inflight = [
            dict(r)
            for r in _db().execute("SELECT * FROM inflight ORDER BY started_at")
        ]
        sessions = [
            dict(r)
            for r in _db().execute(
                """
                SELECT s.id, s.target, s.status, s.opened_by, s.created_at,
                       s.bound_at, s.closed_at,
                       (SELECT COUNT(*) FROM inflight i
                         WHERE i.session_id = s.id) AS inflight
                FROM sessions s
                ORDER BY CASE s.status WHEN 'open' THEN 0 ELSE 1 END,
                         s.created_at DESC
                LIMIT 200
                """
            )
        ]
        probes = [
            dict(r)
            for r in _db().execute(
                "SELECT * FROM probes ORDER BY created_at DESC LIMIT 10"
            )
        ]
    live = {
        g["gateway_id"]
        for g in gateways
        if g["heartbeat_at"] >= now - GATEWAY_LEASE_SECONDS
    }
    for g in gateways:
        g["alive"] = g["gateway_id"] in live
        g["is_self"] = g["gateway_id"] == _gateway_id
    inflight = [row for row in all_inflight if row["gateway_id"] in live]
    # Session in-flight counts must use the same live set as the upstream
    # counts, otherwise the two conclusions could disagree.
    live_ids = {row["id"] for row in inflight}
    for s in sessions:
        s["inflight"] = sum(
            1 for row in all_inflight if row["session_id"] == s["id"] and row["id"] in live_ids
        )
    open_session_counts: dict[str, int] = {}
    for s in sessions:
        if s["status"] == "open" and s["target"] is not None:
            open_session_counts[s["target"]] = (
                open_session_counts.get(s["target"], 0) + 1
            )
    counts: dict[str, int] = {}
    for row in inflight:
        counts[row["target"]] = counts.get(row["target"], 0) + 1
    for u in upstreams:
        u["inflight"] = counts.get(u["name"], 0)
        u["open_sessions"] = open_session_counts.get(u["name"], 0)
        u["is_current"] = u["name"] == route["current_target"]
        blockers: list[str] = []
        if u["inflight"]:
            blockers.append(f"{u['inflight']} 条在途请求")
        if u["open_sessions"]:
            blockers.append(f"{u['open_sessions']} 个未关闭的绑定会话")
        u["offline_blockers"] = blockers
        # Only an upstream that actually carried traffic and has since been
        # switched away from is an "old target".  An upstream that was never
        # current is merely a standby candidate, never "ready to be taken
        # offline".  An old target qualifies once it has no in-flight requests
        # AND no unclosed session still bound to it.
        u["is_old_target"] = (not u["is_current"]) and u["retired_at"] is not None
        u["can_offline"] = (
            u["is_old_target"] and u["inflight"] == 0 and u["open_sessions"] == 0
        )
    for row in inflight:
        row["age_ms"] = int((now - row["started_at"]) * 1000)
    for p in probes:
        p["detail"] = json.loads(p["detail"]) if p["detail"] else {}
    return {
        "now": now,
        "route": route,
        "upstreams": upstreams,
        "sessions": sessions,
        "inflight": inflight,
        "gateways": gateways,
        "recent_probes": probes,
    }
