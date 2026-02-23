#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ACTION="${1:-all}"
shift || true

ASSUME_YES=0
PURGE_DEPS=0
STOP_REDIS_ONLY=0

usage() {
  cat <<'EOF'
Usage:
  ./uninstall_debian.sh stop
  ./uninstall_debian.sh remove --yes
  ./uninstall_debian.sh all --yes [--purge-deps]
  ./uninstall_debian.sh purge-deps

Actions:
  stop        Stop backend/celery/frontend processes started for this project.
  remove      Delete the whole project directory.
  all         stop + remove (most common).
  purge-deps  Remove optional system packages (redis/nodejs/npm).

Flags:
  --yes         Skip confirmation prompts.
  --purge-deps  Used with "all": also purge redis/nodejs/npm packages.
EOF
}

log() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*"
}

run_as_root() {
  if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
    "$@"
  elif command -v sudo >/dev/null 2>&1; then
    sudo "$@"
  else
    log "This step requires root privileges. Install sudo or run as root."
    exit 1
  fi
}

confirm_or_exit() {
  local message="$1"
  if [[ "$ASSUME_YES" -eq 1 ]]; then
    return 0
  fi

  read -r -p "$message [y/N]: " ans
  if [[ ! "$ans" =~ ^[Yy]$ ]]; then
    log "Cancelled."
    exit 1
  fi
}

stop_project_processes() {
  log "Stopping project processes..."

  if [[ -x "$PROJECT_DIR/run_debian_no_nginx.sh" ]]; then
    "$PROJECT_DIR/run_debian_no_nginx.sh" stop || true
  fi

  # Fallback process stop in case pid files are stale.
  pkill -f "uvicorn app.main:app" || true
  pkill -f "celery -A app.tasks.celery_app.celery_app worker" || true
  pkill -f "vite" || true

  log "Project processes stop command completed."
}

purge_optional_packages() {
  log "Purging optional packages: redis-server redis-tools nodejs npm"
  run_as_root systemctl stop redis-server || true
  run_as_root systemctl disable redis-server || true
  run_as_root apt-get purge -y redis-server redis-tools nodejs npm || true
  run_as_root apt-get autoremove -y --purge || true
  log "Optional package purge completed."
}

remove_project_directory() {
  confirm_or_exit "This will permanently delete: $PROJECT_DIR"
  log "Removing project directory..."
  cd /
  rm -rf "$PROJECT_DIR"
  log "Project directory removed: $PROJECT_DIR"
}

parse_flags() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --yes) ASSUME_YES=1 ;;
      --purge-deps) PURGE_DEPS=1 ;;
      --stop-redis) STOP_REDIS_ONLY=1 ;;
      -h|--help|help) usage; exit 0 ;;
      *)
        log "Unknown option: $1"
        usage
        exit 1
        ;;
    esac
    shift
  done
}

main() {
  parse_flags "$@"

  case "$ACTION" in
    stop)
      stop_project_processes
      if [[ "$STOP_REDIS_ONLY" -eq 1 ]]; then
        run_as_root systemctl stop redis-server || true
        log "Redis stop command completed."
      fi
      ;;
    remove)
      remove_project_directory
      ;;
    all)
      confirm_or_exit "This will stop services and delete the project directory. Continue?"
      ASSUME_YES=1
      stop_project_processes
      remove_project_directory
      if [[ "$PURGE_DEPS" -eq 1 ]]; then
        purge_optional_packages
      fi
      ;;
    purge-deps)
      confirm_or_exit "This will purge redis/nodejs/npm packages from the system. Continue?"
      purge_optional_packages
      ;;
    ""|-h|--help|help)
      usage
      ;;
    *)
      log "Unknown action: $ACTION"
      usage
      exit 1
      ;;
  esac
}

main "$@"
