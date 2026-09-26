# sandbox-control-plane

沙箱控制面独立服务（`docs/sandbox-standalone-deployment.md` 第 1 步）。纯 Node HTTP，自包含。

## 起

```bash
pnpm install
pnpm build
SANDBOX_CP_INBOUND_SECRET=... DATABASE_URL=postgresql://... PORT=8080 pnpm start
```

启动时 ensure 全部表（`sandbox_activity` / `sandbox_pool` / `sandbox_pod_logs` / `sandbox_pod_events` /
`sandbox_pod_meta`，DDL 与主服务逐字一致），再起 reaper / observer / pool / retention，最后 listen。
没有 K8s 凭据也能起：执行面 503，观测面照常。

测试：`TEST_DB_PORT=55700 pnpm test -- --maxWorkers=3`（每个 worker 一份内存 PGlite，端口 = 基准 + worker 号）。

镜像：`docker build -t sandbox-control-plane:<tag> .`；集群清单在 `k8s/`。

## env

| 变量 | 必填 | 说明 |
|---|---|---|
| `DATABASE_URL` | 是 | Postgres 连接串 |
| `SANDBOX_CP_INBOUND_SECRET` | 是 | 入站 Bearer 密钥；缺失拒绝启动 |
| `PORT` / `HOST` | | 监听端口 / 地址，默认 `8080` / `0.0.0.0` |
| `PGPOOL_MAX` | | 连接池上限，默认 4 |
| `SANDBOX_K8S_SERVER_URL` / `SANDBOX_K8S_TOKEN` / `SANDBOX_K8S_NAMESPACE` | | 显式 K8s 凭据，三项齐才生效；否则回退集群内 ServiceAccount |
| `SANDBOX_K8S_CA` | | API server CA（PEM） |
| `SANDBOX_K8S_SA_DIR` | | ServiceAccount 挂载目录，默认 `/var/run/secrets/kubernetes.io/serviceaccount` |
| `KUBERNETES_SERVICE_HOST` / `KUBERNETES_SERVICE_PORT` | | 集群内由 kubelet 注入 |
| `SANDBOX_AIO_PORT` | | 沙箱 pod 内 AIO 端口，默认 8080 |
| `SANDBOX_IDLE_TTL_S` | | 空闲回收阈值；不配 = 回收器关、`/reap` 503 |
| `SANDBOX_MAX_AGE_S` | | 绝对寿命；同时决定 acquire/hold 回的 `expires_at` |
| `SANDBOX_REAPER_INTERVAL_S` | | 后台回收周期；不配不起 |
| `SANDBOX_POOL_DESIRED` | | 预热池目标数；不配不开池 |
| `SANDBOX_POOL_INTERVAL_S` | | 池子维护周期；不配不起 |
| `SANDBOX_POOL_MAX_AGE_S` | | 池子 pod 空置寿命，默认 1800 |
| `SANDBOX_OBSERVER_ENABLED` | | `0` 关 observer（默认开，需 K8s 凭据） |
| `SANDBOX_OBSERVER_MAX_LOG_LINES` | | 单 pod 日志行上限 |
| `OBSERVABILITY_RETENTION_DAYS` | | 观测表保留天数，默认 90 |
| `OBSERVABILITY_RETENTION_INTERVAL_S` | | 保留期清理周期，默认 21600（6h） |
| `OBSERVABILITY_RETENTION_ENABLED` | | `0` 关保留期清理 |
| `PIP_INDEX_URL` / `PIP_TRUSTED_HOST` | | 注入沙箱 pod 的内网 PyPI 源（URL 里的凭据会被剥掉） |

## HTTP 面

除 `GET /healthz` 外都要 `Authorization: Bearer <SANDBOX_CP_INBOUND_SECRET>`，否则 401 `{"detail":"unauthorized"}`。
JSON、蛇形。

| 方法 | 路径 | 请求 | 200 响应 |
|---|---|---|---|
| POST | `/internal/sandboxes/acquire` | `{tenant, session_id}` | `{pod_name, base_url, via, expires_at}` |
| POST | `/internal/sandboxes/execute` | `{tenant, session_id, command, timeout_ms?}` | `{exit_code, output, success, error}` |
| POST | `/internal/sandboxes/mount-files` | `{tenant, session_id, dest_root?, files:[{path, content_base64}]}` | `{mounted}` |
| POST | `/internal/sandboxes/hold` | `{tenant, session_id, ttl_ms, reason}` | `{held_until, ttl_s, expires_at}` |
| POST | `/internal/sandboxes/release` | `{tenant, session_id}` | `{released}` |
| POST | `/internal/sandboxes/reap` | `{}` | `{checked, reaped, skipped, held}` |
| GET | `/internal/sandboxes/logs?pod=&limit=` | | `{lines:[{id, pod_name, line, observed_at, tenant_id, session_id}]}` |
| GET | `/internal/sandboxes/events?pod=&limit=` | | `{events:[{id, pod_name, source, type, reason, message, payload, observed_at, tenant_id, session_id}]}` |
| GET | `/internal/sandboxes/session-logs?tenant=&session_id=&lines=&markers=` | | `{pods, lines, markers}` |
| GET | `/internal/control-plane/health` | | `{ok:true, health}` 或 `{ok:false, kind, detail?}` |
| GET | `/healthz` | | `{ok:true}`（不鉴权） |

- `mount-files`：`path` 以 `/` 开头按绝对路径写，否则拼在 `dest_root`（默认 `/home/gem/skills`）下。
- `hold`：`ttl_ms` 截断到 2h，缺失或非法取 30min。
- `limit`：logs 默认 500 / 上限 2000；events 默认 200 / 上限 1000；session-logs `lines` 500 / 2000、`markers` 20 / 1000。
- `observed_at` 出线为 ISO 字符串。

错误：

| 情况 | 状态 | body |
|---|---|---|
| `SandboxNotFoundError` | 410 | `{detail, error:"SandboxNotFoundError"}` |
| `SandboxUnreachableError` | 502 | `{detail, error:"SandboxUnreachableError"}` |
| 没配 K8s（执行面） | 503 | `{detail:"sandbox control plane not configured", error:"SandboxNotConfiguredError"}` |
| 回收器没配（`/reap`） | 503 | `{detail:"sandbox reaper not configured (SANDBOX_IDLE_TTL_S unset or invalid)"}` |
| 其它异常 | 500 | `{detail, error:<err.name>}` |
| JSON 解析失败 | 400 | `{detail:"invalid json"}` |
| 缺 `tenant` / `session_id` / `command` / `files` | 400 | `{detail:"missing <字段>"}` |
| 未知路径 | 404 | `{detail:"not found"}` |

## 在 devbox 上跑（发版走 devbox 快照）

启动命令都在 `entrypoint.sh`：平台启动命令填 `/bin/bash -c "/home/devbox/project/entrypoint.sh prod"`；
人用 `./entrypoint.sh restart`（构建 → 停 → 脱离终端拉起 → 等 `/healthz`），`status` / `stop` / `run <脚本>` / `dev` 见脚本头部。
运行时配置放 gitignored 的 `deploy/control-plane.env`（进程环境优先于它）。
