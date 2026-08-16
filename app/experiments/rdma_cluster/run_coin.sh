#!/usr/bin/env bash
# Start coincidence-only coin master. Edit JSON IPs before multi-host runs.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT_DIR"

CONFIG="${1:-app/config/rdma_cluster/coin.json}"
BIN="${COIN_BIN:-bin/app/app_coin_master}"
if [[ ! -x "$BIN" ]]; then
  BIN="build/apps/basic/bin/app/app_coin_master"
fi
if [[ ! -x "$BIN" ]]; then
  echo "[run_coin] app_coin_master not found. Build preset linux-release-apps-basic." >&2
  exit 1
fi

unset GLOG_log_dir
export GLOG_logtostderr=1
export GLOG_stderrthreshold=0

echo "[run_coin] config=$CONFIG bin=$BIN"
exec "$BIN" --config "$CONFIG"
