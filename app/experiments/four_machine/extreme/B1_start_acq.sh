#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../common/common.sh"

prepare_role_env
LOG_DIR=$(new_log_dir "extreme" "B1")

ACQ_CFG="$RUNTIME_DIR/acq_node_b1_extreme.json"
render_template "$ROOT_DIR/app/config/four_machine/templates/acq_node_b1.template.json" "$ACQ_CFG"

echo "[B1] config: $ACQ_CFG"
echo "[B1] logs  : $LOG_DIR/acq_node.log"

cd "$ROOT_DIR"
make app-acq-r2s-node
./bin/app_acq_r2s_node --config "$ACQ_CFG" > "$LOG_DIR/acq_node.log" 2>&1
