#!/usr/bin/env bash
# Start worker (synthetic / lsingle_replay). Handshake first, then send.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT_DIR"

CONFIG="${1:-app/config/rdma_cluster/worker0.json}"
BIN="${WORKER_BIN:-bin/app/app_acq_r2s_node}"
if [[ ! -x "$BIN" ]]; then
  BIN="build/apps/cuda/bin/app/app_acq_r2s_node"
fi
if [[ ! -x "$BIN" ]]; then
  echo "[run_worker] app_acq_r2s_node not found. Build preset linux-release-apps-cuda." >&2
  exit 1
fi

unset GLOG_log_dir
export GLOG_logtostderr=1
export GLOG_stderrthreshold=0

echo "[run_worker] config=$CONFIG bin=$BIN"
exec "$BIN" --config "$CONFIG"
