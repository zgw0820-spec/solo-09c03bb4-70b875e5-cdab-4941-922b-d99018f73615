#!/usr/bin/env bash
# Two gateways sharing one SQLite file, plus two upstreams.
# Runs the two-gateway consistency test, restarts gateway A, and checks that
# an unclosed bound session survives. Mirrors the docker-compose topology.
#
#   ./scripts/run_two_gateways.sh
#
# Env overrides: PYTHON, LOCAL_PYTHONPATH, GW_A_PORT, GW_B_PORT,
#                UP_A_PORT, UP_B_PORT, WORK_DIR
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="${WORK_DIR:-/tmp/edge-gw-two}"
DB="$WORK/shared.db"
PY="${PYTHON:-python3}"
export PYTHONPATH="${LOCAL_PYTHONPATH:-${PYTHONPATH:-}}"
GW_A_PORT="${GW_A_PORT:-8101}"
GW_B_PORT="${GW_B_PORT:-8102}"
UP_A_PORT="${UP_A_PORT:-9101}"
UP_B_PORT="${UP_B_PORT:-9102}"

rm -rf "$WORK"
mkdir -p "$WORK"

PIDS=()
cleanup() { kill "${PIDS[@]}" 2>/dev/null || true; }
trap cleanup EXIT

start_upstream() { # name port delay
  UPSTREAM_NAME="$1" DEFAULT_DELAY_MS="$3" \
    "$PY" -m uvicorn app.main:app --app-dir "$ROOT/upstream" \
      --host 127.0.0.1 --port "$2" --log-level warning \
      >"$WORK/upstream-$1.log" 2>&1 &
  PIDS+=($!)
}

start_gateway() { # gateway_id port
  GATEWAY_ID="$1" GATEWAY_DB="$DB" \
  UPSTREAMS="alpha=http://127.0.0.1:$UP_A_PORT;beta=http://127.0.0.1:$UP_B_PORT" \
  INITIAL_TARGET=alpha \
    "$PY" -m uvicorn app.main:app --app-dir "$ROOT/gateway" \
      --host 127.0.0.1 --port "$2" --log-level warning \
      >>"$WORK/gateway-$1.log" 2>&1 &
  PIDS+=($!)
}

wait_ready() { # url
  for _ in $(seq 1 150); do
    if curl -fsS "$1" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  echo "timeout waiting for $1" >&2
  return 1
}

echo "== 上游 alpha(:$UP_A_PORT) / beta(:$UP_B_PORT) =="
start_upstream alpha "$UP_A_PORT" 2500
start_upstream beta  "$UP_B_PORT" 300
wait_ready "http://127.0.0.1:$UP_A_PORT/health"
wait_ready "http://127.0.0.1:$UP_B_PORT/health"

echo "== 网关 gw-a(:$GW_A_PORT) 与 gw-b(:$GW_B_PORT) 共享 $DB =="
start_gateway gw-a "$GW_A_PORT"
wait_ready "http://127.0.0.1:$GW_A_PORT/api/health"
start_gateway gw-b "$GW_B_PORT"
wait_ready "http://127.0.0.1:$GW_B_PORT/api/health"

echo "== 双网关一致性与交错测试 =="
OUT="$WORK/two_gateway.out"
"$PY" "$ROOT/scripts/two_gateway_test.py" \
  "http://127.0.0.1:$GW_A_PORT" "http://127.0.0.1:$GW_B_PORT" | tee "$OUT"

read -r _ PERSIST_SID PERSIST_TARGET <<<"$(grep '^PERSIST_SESSION' "$OUT" | tail -1)"

echo "== kill -9 gw-a，重启后核对未关闭会话 $PERSIST_SID 仍绑定 $PERSIST_TARGET =="
GW_A_KILL="$(ps -ef | grep "port $GW_A_PORT" | grep -v grep | awk '{print $2}' | head -1)"
kill -9 "$GW_A_KILL" 2>/dev/null || true
sleep 0.5
start_gateway gw-a "$GW_A_PORT"
wait_ready "http://127.0.0.1:$GW_A_PORT/api/health"

"$PY" - "$GW_A_PORT" "$PERSIST_SID" "$PERSIST_TARGET" <<'PY'
import json, sys, urllib.request, urllib.error
port, sid, target = sys.argv[1], sys.argv[2], sys.argv[3]
base = f"http://127.0.0.1:{port}"
def api(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, method=method, data=data,
                                 headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())
fails = []
def check(name, cond, extra=""):
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  ({extra})" if extra and not cond else ""))
    if not cond:
        fails.append(name)
_, st = api("GET", "/api/state")
sess = next((s for s in st["sessions"] if s["id"] == sid), None)
check("重启后未关闭会话仍存在且绑定不变",
      sess is not None and sess["status"] == "open" and sess["target"] == target, sess)
check("重启后自身中断在途已清理",
      all(i["gateway_id"] != "gw-a" for i in st["inflight"]), st["inflight"])
s, r = api("POST", "/api/call", {"session_id": sid, "delay_ms": 20})
check("重启后该会话仍由原目标服务", r.get("ok") is True and r.get("target") == target, r)
s, c = api("POST", "/api/session/close", {"session_id": sid})
check("重启后可关闭该会话", s == 200 and c.get("ok") is True, c)
if fails:
    sys.exit(1)
PY

echo
echo "== 全部通过。数据库: $DB  日志: $WORK =="
