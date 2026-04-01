#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
RUNTIME_DIR="$ROOT_DIR/app/config/three_machine/runtime"
LOG_ROOT_DIR="$ROOT_DIR/app/experiments/logs/three_machine"

require_env() {
  local key="$1"
  if [[ -z "${!key:-}" ]]; then
    echo "[ERROR] missing env: $key" >&2
    exit 1
  fi
}

escape_sed() {
  printf '%s' "$1" | sed -e 's/[\/&]/\\&/g'
}

render_template() {
  local template_path="$1"
  local output_path="$2"

  mkdir -p "$(dirname "$output_path")"

  local a b c
  a=$(escape_sed "${A_MASTER_IP}")
  b=$(escape_sed "${B_ACQ_IP}")
  c=$(escape_sed "${C_SENDER_IP}")

  sed \
    -e "s|__A_MASTER_IP__|$a|g" \
    -e "s|__B_ACQ_IP__|$b|g" \
    -e "s|__C_SENDER_IP__|$c|g" \
    "$template_path" > "$output_path"
}

prepare_role_env() {
  require_env A_MASTER_IP
  require_env B_ACQ_IP
  require_env C_SENDER_IP

  mkdir -p "$RUNTIME_DIR" "$LOG_ROOT_DIR"

  unset GLOG_log_dir
  unset GLOG_alsologtostderr
  export GLOG_logtostderr=1
  export GLOG_stderrthreshold=0
}

new_log_dir() {
  local scenario="$1"
  local role="$2"
  local dir="$LOG_ROOT_DIR/${scenario}_${role}_$(date +%Y%m%d_%H%M%S)"
  mkdir -p "$dir"
  printf '%s' "$dir"
}
