#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$ROOT_DIR/backend"
FRONTEND_DIR="$ROOT_DIR/frontend"
VENV_DIR="$BACKEND_DIR/.venv"
RUN_DIR="$ROOT_DIR/.run"
LOG_DIR="$RUN_DIR/logs"
PID_DIR="$RUN_DIR/pids"

BACKEND_HOST="${BACKEND_HOST:-0.0.0.0}"
BACKEND_PORT="${BACKEND_PORT:-8000}"
FRONTEND_HOST="${FRONTEND_HOST:-0.0.0.0}"
FRONTEND_PORT="${FRONTEND_PORT:-5173}"
REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379/0}"
CELERY_CONCURRENCY="${CELERY_CONCURRENCY:-2}"

# 可手动覆盖：SERVER_IP=1.2.3.4 ./run_debian_no_nginx.sh install
SERVER_IP="${SERVER_IP:-$(hostname -I 2>/dev/null | awk '{print $1}')}"

usage() {
  cat <<'EOF'
Usage:
  ./run_debian_no_nginx.sh install   # 安装系统/项目依赖并生成环境配置
  ./run_debian_no_nginx.sh start     # 启动 Redis + FastAPI + Celery + Vite
  ./run_debian_no_nginx.sh stop      # 停止 FastAPI + Celery + Vite
  ./run_debian_no_nginx.sh restart   # 重启 FastAPI + Celery + Vite
  ./run_debian_no_nginx.sh status    # 查看运行状态与访问地址
  ./run_debian_no_nginx.sh all       # install + start

Optional env vars:
  SERVER_IP=192.168.1.10
  BACKEND_PORT=8000
  FRONTEND_PORT=5173
  CELERY_CONCURRENCY=2
  REDIS_URL=redis://127.0.0.1:6379/0
EOF
}

log() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*"
}

run_as_root() {
  if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
    "$@"
  else
    if command -v sudo >/dev/null 2>&1; then
      sudo "$@"
    else
      log "未检测到 sudo，请使用 root 账户运行。"
      exit 1
    fi
  fi
}

ensure_paths() {
  mkdir -p "$LOG_DIR" "$PID_DIR"
}

ensure_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    log "缺少命令: $cmd"
    exit 1
  fi
}

write_backend_env_if_missing() {
  local env_file="$BACKEND_DIR/.env"
  if [[ ! -f "$env_file" ]]; then
    cat >"$env_file" <<EOF
DATABASE_URL=sqlite:///./refund_audit.db
REDIS_URL=$REDIS_URL
DASHSCOPE_API_KEY=
EOF
    log "已创建 $env_file（请补充 DASHSCOPE_API_KEY）"
  else
    log "$env_file 已存在，保留原配置"
  fi
}

write_frontend_env() {
  local env_file="$FRONTEND_DIR/.env.development.local"
  if [[ -z "${SERVER_IP}" ]]; then
    log "自动探测 SERVER_IP 失败，请手动指定：SERVER_IP=你的服务器IP"
    exit 1
  fi
  cat >"$env_file" <<EOF
VITE_API_BASE=http://${SERVER_IP}:${BACKEND_PORT}/api/v1
VITE_BASE_URL=http://${SERVER_IP}:${BACKEND_PORT}
EOF
  log "已写入 $env_file"
}

pid_file() {
  local name="$1"
  echo "$PID_DIR/${name}.pid"
}

log_file() {
  local name="$1"
  echo "$LOG_DIR/${name}.log"
}

is_running() {
  local name="$1"
  local pidf
  pidf="$(pid_file "$name")"
  if [[ ! -f "$pidf" ]]; then
    return 1
  fi
  local pid
  pid="$(cat "$pidf" 2>/dev/null || true)"
  if [[ -z "$pid" ]]; then
    return 1
  fi
  kill -0 "$pid" >/dev/null 2>&1
}

start_proc() {
  local name="$1"
  local workdir="$2"
  local cmd="$3"
  local pidf
  pidf="$(pid_file "$name")"
  local logf
  logf="$(log_file "$name")"

  if is_running "$name"; then
    log "$name 已在运行 (PID $(cat "$pidf"))"
    return 0
  fi

  log "启动 $name ..."
  (
    cd "$workdir"
    nohup bash -lc "$cmd" >>"$logf" 2>&1 &
    echo $! >"$pidf"
  )

  sleep 1
  if is_running "$name"; then
    log "$name 启动成功 (PID $(cat "$pidf"))"
  else
    log "$name 启动失败，最近日志："
    tail -n 40 "$logf" || true
    exit 1
  fi
}

stop_proc() {
  local name="$1"
  local pidf
  pidf="$(pid_file "$name")"

  if [[ ! -f "$pidf" ]]; then
    log "$name 未运行"
    return 0
  fi

  local pid
  pid="$(cat "$pidf" 2>/dev/null || true)"
  if [[ -z "$pid" ]]; then
    rm -f "$pidf"
    log "$name pid 文件无效，已清理"
    return 0
  fi

  if kill -0 "$pid" >/dev/null 2>&1; then
    log "停止 $name (PID $pid) ..."
    kill "$pid" >/dev/null 2>&1 || true
    for _ in {1..15}; do
      if ! kill -0 "$pid" >/dev/null 2>&1; then
        break
      fi
      sleep 1
    done
    if kill -0 "$pid" >/dev/null 2>&1; then
      log "$name 未在超时时间内退出，发送 SIGKILL"
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
  fi

  rm -f "$pidf"
  log "$name 已停止"
}

install_system_deps() {
  log "安装系统依赖 ..."
  run_as_root apt-get update
  run_as_root apt-get install -y \
    python3 python3-venv python3-pip \
    redis-server \
    nodejs npm \
    build-essential curl ca-certificates
  run_as_root systemctl enable --now redis-server
}

install_backend_deps() {
  log "安装后端依赖 ..."
  ensure_cmd python3
  cd "$BACKEND_DIR"

  if [[ ! -d "$VENV_DIR" ]]; then
    python3 -m venv "$VENV_DIR"
  fi

  "$VENV_DIR/bin/pip" install -U pip wheel
  "$VENV_DIR/bin/pip" install -r "$BACKEND_DIR/requirements.txt"
  write_backend_env_if_missing
}

install_frontend_deps() {
  log "安装前端依赖 ..."
  ensure_cmd npm
  cd "$FRONTEND_DIR"
  npm install
  write_frontend_env
}

install_all() {
  ensure_paths
  install_system_deps
  install_backend_deps
  install_frontend_deps
  log "安装完成"
}

start_all() {
  ensure_paths
  ensure_cmd bash
  ensure_cmd npm
  ensure_cmd python3

  if [[ ! -x "$VENV_DIR/bin/uvicorn" ]]; then
    log "未检测到后端虚拟环境，请先执行: ./run_debian_no_nginx.sh install"
    exit 1
  fi

  run_as_root systemctl start redis-server
  write_frontend_env

  start_proc "backend" "$BACKEND_DIR" \
    "$VENV_DIR/bin/uvicorn app.main:app --host ${BACKEND_HOST} --port ${BACKEND_PORT}"

  start_proc "celery" "$BACKEND_DIR" \
    "$VENV_DIR/bin/celery -A app.tasks.celery_app.celery_app worker --loglevel=info --concurrency=${CELERY_CONCURRENCY}"

  start_proc "frontend" "$FRONTEND_DIR" \
    "npm run dev -- --host ${FRONTEND_HOST} --port ${FRONTEND_PORT}"

  status_all
}

stop_all() {
  ensure_paths
  stop_proc "frontend"
  stop_proc "celery"
  stop_proc "backend"
}

status_one() {
  local name="$1"
  if is_running "$name"; then
    log "$name 运行中 (PID $(cat "$(pid_file "$name")"))"
  else
    log "$name 未运行"
  fi
}

status_all() {
  ensure_paths
  status_one "backend"
  status_one "celery"
  status_one "frontend"

  echo
  log "前端访问: http://${SERVER_IP}:${FRONTEND_PORT}"
  log "后端文档: http://${SERVER_IP}:${BACKEND_PORT}/docs"
  log "后端健康: http://${SERVER_IP}:${BACKEND_PORT}/"
  log "日志目录: $LOG_DIR"
}

main() {
  local action="${1:-}"
  case "$action" in
    install) install_all ;;
    start) start_all ;;
    stop) stop_all ;;
    restart) stop_all; start_all ;;
    status) status_all ;;
    all) install_all; start_all ;;
    ""|-h|--help|help) usage ;;
    *)
      log "未知命令: $action"
      usage
      exit 1
      ;;
  esac
}

main "$@"
