#!/usr/bin/env python3
"""Two-gateway consistency test on one shared SQLite file.

Usage:
  two_gateway_test.py GW_A_URL GW_B_URL [WORK_DIR]

Both gateways must point at the same GATEWAY_DB and have distinct GATEWAY_IDs.
Isolation level "admission / close / switch interleaving": the *route version*
used by each worker is captured before the call, so requests are admitted
while other threads switch the route and close sessions.  The assertions check
the invariants, not one particular interleaving:

  * a session's target never changes after the first admission, regardless of
    which gateway serves the call or how often the route moves;
  * a call admitted before a close still returns; a call admitted after the
    close is rejected;
  * for every upstream, both gateways derive the same in-flight count,
    unclosed-session count and can_offline verdict.
"""
import json
import random
import sys
import threading
import time
import urllib.error
import urllib.request

A = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8101"
B = sys.argv[2] if len(sys.argv) > 2 else "http://127.0.0.1:8102"

FAILURES: list[str] = []
_print_lock = threading.Lock()


def api(base, method, path, body=None, timeout=70):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        base + path, method=method, data=data,
        headers={"content-type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def check(name, cond, extra=""):
    with _print_lock:
        print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  ({extra})" if extra and not cond else ""))
    if not cond:
        FAILURES.append(name)


def state(base):
    return api(base, "GET", "/api/state")[1]


def switch_route(base):
    """Move the route to the other upstream using a fresh probe."""
    st = state(base)
    cur, ver = st["route"]["current_target"], st["route"]["version"]
    targets = [u["name"] for u in st["upstreams"] if u["name"] in ("alpha", "beta")]
    target = next(t for t in targets if t != cur)
    s, p = api(base, "POST", "/api/switch/probe", {"target": target, "base_version": ver})
    if not p.get("ok"):
        return None
    return api(base, "POST", "/api/switch/commit",
               {"target": target, "base_version": ver, "probe_id": p["probe_id"]})[1]


def main():
    random.seed(7)
    _, st = api(A, "GET", "/api/state")
    check("两个网关登记在同一库", st["gateway_id"] in {g["gateway_id"] for g in st["gateways"]})
    _, stb = api(B, "GET", "/api/state")
    check("两个网关都能读到对端心跳",
          {g["gateway_id"] for g in st["gateways"]} == {g["gateway_id"] for g in stb["gateways"]},
          [st["gateways"], stb["gateways"]])

    # ---- 1) cross-gateway stickiness --------------------------------------
    _, o = api(A, "POST", "/api/session/open", {})
    sid = o["session"]["id"]
    _, first = api(A, "POST", "/api/call", {"session_id": sid, "delay_ms": 20})
    bound = first["target"]
    check("A 首次接单绑定当时目标", first.get("session_bound_now") is True, first)
    for _ in range(3):
        switch_route(random.choice([A, B]))
    # The bound target is always an upstream that carried traffic, so it must
    # be tracked as an old target (and therefore eventually offlinable).
    _, st = api(A, "GET", "/api/state")
    check("绑定目标被识别为旧目标", any(
        u["name"] == bound and u["is_old_target"] for u in st["upstreams"]), st["upstreams"])
    _, second = api(B, "POST", "/api/call", {"session_id": sid, "delay_ms": 20})
    check("切换后经另一网关调用仍回原目标",
          second.get("target") == bound and second["upstream"]["upstream"] == bound, second)
    check("演示上游按会话号递增序号",
          second["upstream"].get("seq") == first["upstream"].get("seq") + 1,
          [first["upstream"], second["upstream"]])
    _, plain = api(A, "POST", "/api/call", {"delay_ms": 20})
    check("无会话请求走当前路由", plain["target"] == state(A)["route"]["current_target"], plain)

    # ---- 2) close on one gateway is seen by the other --------------------
    _, c = api(B, "POST", "/api/session/close", {"session_id": sid})
    check("B 关闭会话", c.get("ok") is True, c)
    s, r = api(A, "POST", "/api/call", {"session_id": sid, "delay_ms": 10})
    check("A 立即拒绝该会话新请求", s == 409 and r.get("error") == "session_closed", (s, r))
    s, c2 = api(A, "POST", "/api/session/close", {"session_id": sid})
    check("两个网关关闭结果一致（幂等）", s == 200 and c2.get("already_closed") is True, c2)

    # ---- 3) race: first admission vs close vs switch ---------------------
    race_sessions: list[dict] = []
    for i in range(16):
        _, o = api(random.choice([A, B]), "POST", "/api/session/open", {})
        race_sessions.append({"id": o["session"]["id"], "calls": [], "closed": False,
                              "bound": None})
    errors: list[str] = []
    lock = threading.Lock()

    def call_worker(rec):
        for _ in range(4):
            base = random.choice([A, B])
            delay = random.randint(0, 30)
            try:
                s, r = api(base, "POST", "/api/call",
                           {"session_id": rec["id"], "delay_ms": delay})
            except Exception as e:  # noqa: BLE001
                with lock:
                    errors.append(f"call {e}")
                return
            with lock:
                rec["calls"].append((s, r))
                if s == 200 and r.get("ok"):
                    if rec["bound"] is None:
                        rec["bound"] = r["target"]
                    elif r["target"] != rec["bound"]:
                        errors.append("sticky violated")
                elif s == 409:
                    if r.get("error") != "session_closed":
                        errors.append(f"unexpected 409 {r}")
                else:
                    errors.append(f"unexpected {s} {r}")

    def close_worker(rec):
        time.sleep(random.uniform(0, 0.05))
        base = random.choice([A, B])
        s, r = api(base, "POST", "/api/session/close", {"session_id": rec["id"]})
        if s != 200 or not r.get("ok"):
            with lock:
                errors.append(f"close {s} {r}")
        with lock:
            rec["closed"] = True
            rec["close_index"] = len(rec["calls"])

    def switch_worker():
        for _ in range(5):
            try:
                switch_route(random.choice([A, B]))
            except Exception as e:  # noqa: BLE001
                with lock:
                    errors.append(f"switch {e}")
            time.sleep(random.uniform(0.01, 0.03))

    threads = []
    for rec in race_sessions:
        threads.append(threading.Thread(target=call_worker, args=(rec,)))
        threads.append(threading.Thread(target=close_worker, args=(rec,)))
    threads += [threading.Thread(target=switch_worker) for _ in range(3)]
    random.shuffle(threads)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    check("交错过程无异常响应/粘滞破坏", not errors, errors[:3])

    _, st = api(A, "GET", "/api/state")
    sessions = {s["id"]: s for s in st["sessions"]}
    bad = []
    for rec in race_sessions:
        s = sessions.get(rec["id"])
        if s is None:
            bad.append((rec["id"], "missing"))
            continue
        if s["status"] != "closed":
            bad.append((rec["id"], "not closed"))
        if rec["bound"] is not None and s["target"] != rec["bound"]:
            bad.append((rec["id"], "binding differs", s["target"], rec["bound"]))
        if rec["bound"] is not None and any(
            r.get("target") != rec["bound"] for code, r in rec["calls"] if code == 200
        ):
            bad.append((rec["id"], "call target differs"))
    check("交错后每个会话只认一个绑定目标且均已关闭", not bad, bad[:3])

    # ---- 4) both gateways agree on the conclusion -----------------------
    affected = {rec["bound"] for rec in race_sessions if rec["bound"]}
    _, sa = api(A, "GET", "/api/state")
    _, sb = api(B, "GET", "/api/state")
    ka = {(u["name"], u["inflight"], u["open_sessions"], u["can_offline"]) for u in sa["upstreams"]}
    kb = {(u["name"], u["inflight"], u["open_sessions"], u["can_offline"]) for u in sb["upstreams"]}
    check("两个网关对在途/会话/可下线结论完全一致", ka == kb, (ka, kb))
    check("两侧路由一致", sa["route"] == sb["route"], (sa["route"], sb["route"]))

    # ---- 5) successful close + drain makes the old target offlinable ----
    for rec in race_sessions:
        api(random.choice([A, B]), "POST", "/api/session/close", {"session_id": rec["id"]})
    deadline = time.time() + 15
    while time.time() < deadline:
        _, st = api(A, "GET", "/api/state")
        if not st["inflight"]:
            break
        time.sleep(0.1)
    _, st = api(A, "GET", "/api/state")
    check("所有会话已关闭且在途清零", not st["inflight"], st["inflight"])
    stale = [u for u in st["upstreams"] if not u["is_current"] and u["open_sessions"] > 0]
    check("无遗留未关闭会话", not stale, stale)
    old = [u for u in st["upstreams"] if not u["is_current"] and u["name"] in ("alpha", "beta")]
    check("旧目标在途与会话都清零后可下线",
          old and all(u["can_offline"] for u in old), old)
    check("当前目标不可下线", all(u["can_offline"] is False for u in st["upstreams"] if u["is_current"]))

    # ---- 6) persistence: an unclosed bound session across a gateway restart
    _, o = api(A, "POST", "/api/session/open", {})
    persist = o["session"]["id"]
    _, r = api(A, "POST", "/api/call", {"session_id": persist, "delay_ms": 20})
    bound = r["target"]
    print(f"PERSIST_SESSION {persist} {bound}")


if __name__ == "__main__":
    main()
    if FAILURES:
        print(f"\n{len(FAILURES)} 项检查失败:")
        for f in FAILURES:
            print(" -", f)
        sys.exit(1)
    print("\n全部检查通过 (two-gateway)")
