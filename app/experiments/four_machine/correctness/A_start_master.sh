#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../common/common.sh"

prepare_role_env
LOG_DIR=$(new_log_dir "correctness" "A")

MASTER_CFG="$RUNTIME_DIR/coin_master_correctness.json"
render_template "$ROOT_DIR/app/config/four_machine/templates/coin_master_correctness.template.json" "$MASTER_CFG"

echo "[A] config: $MASTER_CFG"
echo "[A] logs  : $LOG_DIR/coin_master.log"

cd "$ROOT_DIR"
make app-coin-master
./bin/app_coin_master --config "$MASTER_CFG" > "$LOG_DIR/coin_master.log" 2>&1
