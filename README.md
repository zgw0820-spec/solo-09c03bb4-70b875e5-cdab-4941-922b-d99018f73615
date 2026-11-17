# 边缘网关值班切换 Demo（会话保持）

FastAPI + SQLite 实现的边缘网关与操作网页。值班员切换上游时：

- **无会话请求**：接纳时遵循当时的有效路由。切换确认后，新请求立即走新目标。
- **已接纳请求**：`/work` 在接纳时快照目标，切换后仍从原目标取回完整响应。
- **网页会话**：会话首次 `/work` 接单时绑定当时的目标；之后即使路由切走，
  该会话后续请求（可落到任意一个网关）仍访问原目标。原目标不可达时**报错，
  不转投**新目标。
- **关闭会话**：立即停止接纳该会话的新请求；已经接单的请求仍正常返回。
- **可下线判断**：旧目标“没有在途请求 **且** 没有未关闭的绑定会话”时才提示可下线；
  从未接管过流量的备用目标不会被提示可下线。

## 组成

| 组件 | 说明 |
| --- | --- |
| `gateway/` | FastAPI 网关：会话、转发、操作 API、值班台网页（`/`）、SQLite |
| `gateway/app/db.py` | SQLite 持久化；跨进程 `BEGIN IMMEDIATE` 事务串行化“首次接单绑定 / 关闭 / 切换” |
| `upstream/` | 演示上游；按 `session_id` 递增 `seq`，`/health` 供探测，`/admin/health_mode` 可让健康检查失败 |
| `docker-compose.yml` | 双网关 `gateway-a`/`gateway-b` 共享一个 SQLite 卷 + 两个上游 |
| `scripts/run_local.sh` | 单网关端到端（切换语义 + 会话 + `kill -9` 重启恢复） |
| `scripts/run_two_gateways.sh` | 双网关共享 SQLite 的粘滞、关闭、交错切换一致性 + 重启保留 |
| `scripts/two_gateway_test.py` | 双网关并发/交错断言 |

## 运行

Docker Compose（双网关共享 SQLite）：

```bash
docker compose up --build
# 网关 A 值班台: http://localhost:8080/
# 网关 B 值班台: http://localhost:8081/    （两个网页操作同一份 SQLite）
```

本地直接跑（无需 Docker）：

```bash
pip install -r gateway/requirements.txt -r upstream/requirements.txt
./scripts/run_local.sh          # 单网关：切换语义 + 会话 + 重启
./scripts/run_two_gateways.sh   # 双网关：共享 SQLite 一致性
```

若依赖装在自定义路径，可用 `LOCAL_PYTHONPATH=/path/to/site-packages ./scripts/run_local.sh`
（或 `PYTHON=/path/to/python`）。

## 网页操作

1. **开启新会话**：会话此时未绑定。
2. **会话连续调用**：按设定延迟/次数依次发 `/work`。第一次返回会标出
   “首次接单，已绑定该目标”，每条结果显示 `目标` 与 `调用序号 #n`。
3. 另一值班员（或另一个网关网页）**探测 → 确认切换**。
4. 再次 **会话连续调用**：目标仍是原目标，序号继续递增；同页 **无会话请求**
   则显示新目标、序号为 `—`。
5. **关闭会话**：之后该会话的新调用被拒（`session_closed`）；关闭瞬间仍在途的
   请求仍返回。旧目标在“在途 0 且未关闭绑定会话 0”后显示“可下线 ✓”。

网页底部展示会话列表与网关实例心跳（双网关时可见两个实例）。

## 语义与实现要点

- **会话绑定（持久化）**：`sessions.target` 初始为 `NULL`。首次接纳在同一个
  `BEGIN IMMEDIATE` 事务里读有效路由并写入绑定；`BEGIN IMMEDIATE` 让两个网关
  进程的“首次接单 / 关闭 / 切换”严格串行，不会出现半绑定或先关闭后仍接纳。
- **粘滞不转投**：`/api/call` 用会话绑定目标查找上游；查找失败/连接失败返回
  `502 upstream_error` 并带原目标名，绝不改投新目标。
- **关闭语义**：`sessions.status` 置 `closed`，`/api/call` 在接纳事务里看到
  `closed` 即返回 `409 session_closed`；已插入 `inflight` 的请求不受影响。
  关闭幂等，两个网关并发关闭得到同一结论。已关闭会话不可重新开启（防止延迟
  请求把会话“复活”）。
- **下线判断**：`get_state()` 在一个只读事务里同时取路由、在途、会话，派生
  `is_old_target`（非当前目标且曾接管过流量，由切换时写入 `upstreams.retired_at`）
  与 `can_offline = is_old_target && 在途 == 0 && 未关闭会话 == 0`，并给出
  `offline_blockers` 文字原因；两个网关读同一快照，结论一致。
- **切换不等待**：确认切换只做一次按版号 CAS 的单行 `UPDATE`，不等待旧请求；
  绑定会话不受切换影响（这正是会话保持的意义）。
- **重启**：路由、上游、会话（含绑定与关闭状态）持久化。网关以稳定 `GATEWAY_ID`
  标识，启动时只清理**自己上一轮**遗留的 `inflight`（`run_id` 区分轮次），不会
  误删另一个网关正在服务的请求。会话保留。网页顶部展示清理条数（本网关/失联
  网关分列）。
- **失联网关**：每个网关周期性写心跳（`gateways.heartbeat_at`）。心跳超过租约
  （默认 6s）即视为失联，其残留 `inflight` 不再计入在途/下线判断并被清理，
  避免崩溃网关永久阻止下线；绑定会话本身仍保留（重启后继续生效）。
- **SQLite 并发**：`isolation_level=None` + 显式 `BEGIN IMMEDIATE`，避免延迟事务
  读升级写时的 `SQLITE_BUSY`；WAL、`busy_timeout=15s`；阻塞的库操作放在线程池，
  不占用事件循环。

> 注意：多个网关共享同一 SQLite 文件时**必须**为每个网关设置不同的 `GATEWAY_ID`。
> 未设置时默认按库路径派生（适合单网关重启识别自身），同库双网关会互相误判。

## 操作 API

| 方法/路径 | 说明 |
| --- | --- |
| `GET /api/state` | 路由、上游（在途/未关闭会话/`can_offline`/`offline_blockers`）、会话、在途明细、网关心跳、最近探测 |
| `POST /api/session/open` | 开启会话 `{session_id?}`；已关闭会话返回 409 |
| `POST /api/session/close` | 关闭会话 `{session_id}`；幂等；未知会话 404 |
| `POST /api/call` | 转发 `{delay_ms?, path?, session_id?}`；会话已关闭 409、未知会话 404、原目标不可达 502 |
| `POST /api/switch/probe` | 探测候选 `{target, base_version}`，不改路由 |
| `POST /api/switch/commit` | 确认切换 `{target, base_version, probe_id}`，版号冲突 409 |

## 验证

已在本机实际执行（无 Docker 环境，Compose 仅做静态校验）：

```bash
./scripts/run_local.sh        # 单网关 53 项断言，全部通过
./scripts/run_two_gateways.sh # 双网关 22 项 + 重启 4 项断言，全部通过
```

覆盖点：路由切换与版号 CAS、探测失败维持路由、迟到确认被拒、慢请求从原上游
取回完整响应、会话首次绑定与切换后粘滞、跨网关粘滞与序号递增、关闭后拒绝新
请求但已在途者返回、关闭幂等、未知会话/已关闭会话拒绝、原目标不可达 502 不转投、
下线需在途与会话均清零、双网关状态结论一致、并发交错无异常、`kill -9` 后重启
路由/会话保留且自身在途被清理。

## 未完成 / 限制

- 本环境无 Docker 引擎，`docker compose up` 未实际执行；Compose 文件仅经 YAML
  解析与字段校验，容器内网络与卷行为未实测。
- 演示上游的会话序号保存在进程内存，上游自身重启会从 1 重新计数（网关侧会话
  绑定不受影响）。
- 会话无 TTL/自动回收：长期未关闭的会话会一直阻止其绑定目标下线，需要值班员
  主动关闭（这是有意的下线保护语义）。
- 鉴权、限流、真实业务多步 API 未包含；`/work` 为演示端点。
