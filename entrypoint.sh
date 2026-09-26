#!/bin/bash
# Devbox entrypoint — 沙箱控制面（sandbox-control-plane，纯 Node 服务）。
#
# 与主仓 agent-hub-backend 的 entrypoint.sh 同一套约定：平台启动命令只填一条，人用 restart。
#
#   ./entrypoint.sh prod             前台起（缺 node_modules / dist 比 src 旧就先装依赖 + 构建）——平台启动命令用它
#   ./entrypoint.sh stop             停
#   ./entrypoint.sh restart          先构建 → 停 → 脱离终端拉起 → 等 /healthz。日志 deploy/logs/control-plane-<port>.log
#   ./entrypoint.sh restart --no-build
#   ./entrypoint.sh status           PID / 启动时间 / healthz
#   ./entrypoint.sh run <脚本> [参数...]   带生产 env 跑 scripts/<脚本>（只收文件名；写库的先干跑再 --confirm）
#   ./entrypoint.sh dev              tsx watch 前台起（改代码自动重启）
#
# 平台启动命令（引号不能省，见主仓 entrypoint.sh 的说明）：
#   /bin/bash -c "/home/devbox/project/entrypoint.sh prod"
#
# 环境变量优先级（后面的不覆盖前面的）：进程环境 → ${ENV_FILE:-deploy/control-plane.env} → 脚本默认值。
# deploy/ 整个目录 gitignored，env 文件缺席只告警。必填：DATABASE_URL、SANDBOX_CP_INBOUND_SECRET；
# 其余见 README「环境变量」。

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

ENV_FILE="${ENV_FILE:-$ROOT/deploy/control-plane.env}"
LOG_DIR="$ROOT/deploy/logs"
RUN_DIR="$ROOT/deploy/run"

log() { echo ">> $*" >&2; }

load_env() {
  if [[ -f "$ENV_FILE" ]]; then
    log "env: loading $ENV_FILE"
    local overridden=()
    while IFS= read -r line || [[ -n "$line" ]]; do
      [[ "$line" =~ ^[[:space:]]*# ]] && continue
      [[ "$line" =~ ^[[:space:]]*$ ]] && continue
      local key="${line%%=*}"
      key="${key#export }"
      key="${key//[[:space:]]/}"
      [[ -z "$key" ]] && continue
      if [[ -n "${!key+x}" ]]; then
        overridden+=("$key")
        continue
      fi
      local val="${line#*=}"
      # 去掉成对的引号
      if [[ "$val" =~ ^\"(.*)\"$ ]] || [[ "$val" =~ ^\'(.*)\'$ ]]; then val="${BASH_REMATCH[1]}"; fi
      export "$key=$val"
    done < "$ENV_FILE"
    [[ ${#overridden[@]} -gt 0 ]] && log "env: overridden by process env: ${overridden[*]}"
  else
    log "env: $ENV_FILE not found (deploy/ is gitignored) — relying on process env only"
  fi
  export PORT="${PORT:-8080}"
  export NODE_ENV="${NODE_ENV:-production}"
  # 集群 CA 一般是个文件（与主仓 deploy/cluster-ca.pem 同一份）；k8s.ts 只认 SANDBOX_K8S_CA 的 PEM 串
  if [[ -z "${SANDBOX_K8S_CA:-}" && -n "${SANDBOX_K8S_CA_FILE:-}" && -f "${SANDBOX_K8S_CA_FILE}" ]]; then
    SANDBOX_K8S_CA="$(cat "${SANDBOX_K8S_CA_FILE}")"; export SANDBOX_K8S_CA
  fi
}

require_env() {
  local missing=()
  for k in DATABASE_URL SANDBOX_CP_INBOUND_SECRET; do
    [[ -z "${!k:-}" ]] && missing+=("$k")
  done
  if [[ ${#missing[@]} -gt 0 ]]; then
    log "missing required env: ${missing[*]}"
    exit 1
  fi
}

ensure_deps() {
  if [[ ! -d node_modules ]]; then
    log "pnpm install (node_modules missing)"
    pnpm install --frozen-lockfile
  fi
}

needs_build() {
  [[ ! -f dist/main.js ]] && return 0
  local newest
  newest="$(find src package.json tsconfig.build.json -type f -newer dist/main.js -print -quit 2>/dev/null || true)"
  [[ -n "$newest" ]]
}

build() {
  ensure_deps
  if needs_build; then
    log "pnpm build (dist older than src)"
    pnpm build
  else
    log "build: dist is current"
  fi
}

pid_file() { echo "$RUN_DIR/control-plane-${PORT}.pid"; }

running_pid() {
  local f; f="$(pid_file)"
  [[ -f "$f" ]] || return 1
  local pid; pid="$(cat "$f")"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null && echo "$pid" && return 0
  return 1
}

do_stop() {
  local pid
  if pid="$(running_pid)"; then
    log "stopping pid $pid"
    kill -TERM "$pid" 2>/dev/null || true
    for _ in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
    kill -0 "$pid" 2>/dev/null && { log "still alive, SIGKILL"; kill -KILL "$pid" 2>/dev/null || true; }
  else
    log "not running"
  fi
  rm -f "$(pid_file)"
}

wait_healthy() {
  local url="http://127.0.0.1:${PORT}/healthz"
  for _ in $(seq 1 60); do
    if curl -fsS --noproxy '*' "$url" >/dev/null 2>&1; then log "healthy: $url"; return 0; fi
    sleep 1
  done
  log "healthz not answering after 60s: $url (see $LOG_DIR/control-plane-${PORT}.log)"
  return 1
}

do_start_detached() {
  mkdir -p "$LOG_DIR" "$RUN_DIR"
  local logf="$LOG_DIR/control-plane-${PORT}.log"
  log "starting detached → $logf"
  setsid nohup node dist/main.js >> "$logf" 2>&1 < /dev/null &
  echo $! > "$(pid_file)"
  disown || true
  wait_healthy
}

cmd="${1:-}"; shift || true
case "$cmd" in
  prod|production)
    load_env; require_env; build
    log "starting foreground on :$PORT"
    exec node dist/main.js
    ;;
  stop)
    load_env; do_stop
    ;;
  restart)
    load_env; require_env
    if [[ "${1:-}" != "--no-build" ]]; then build; else ensure_deps; fi
    do_stop; do_start_detached
    ;;
  status)
    load_env
    if pid="$(running_pid)"; then
      echo "pid $pid  started $(ps -o lstart= -p "$pid")"
    else
      echo "not running"
    fi
    curl -sS --noproxy '*' -o /dev/null -w "healthz: %{http_code}\n" "http://127.0.0.1:${PORT}/healthz" || true
    ;;
  run)
    load_env; ensure_deps
    script="${1:-}"; shift || true
    [[ -z "$script" || "$script" == */* ]] && { log "usage: entrypoint.sh run <script-file-name> [args]"; exit 2; }
    [[ -f "scripts/$script" ]] || { log "scripts/$script not found"; exit 2; }
    exec pnpm exec tsx "scripts/$script" "$@"
    ;;
  dev)
    load_env; ensure_deps
    export NODE_ENV=development
    exec pnpm exec tsx watch src/main.ts
    ;;
  ""|-h|--help|help)
    sed -n '2,20p' "$0"
    ;;
  *)
    log "unknown command: $cmd"; sed -n '2,20p' "$0"; exit 2
    ;;
esac
