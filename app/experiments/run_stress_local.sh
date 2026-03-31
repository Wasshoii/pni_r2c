#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

LOG_DIR="app/experiments/logs/stress_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOG_DIR"

# glog compatibility: keep traditional redirected logs and also emit glog files.
GLOG_DIR="$LOG_DIR/glog"
mkdir -p "$GLOG_DIR"
export GLOG_log_dir="$GLOG_DIR"
export GLOG_alsologtostderr=1
export GLOG_stderrthreshold=0

wait_for_log_pattern() {
  local log_file="$1"
  local pattern="$2"
  local timeout_sec="$3"

  local start_ts
  start_ts=$(date +%s)
  while true; do
    if [[ -f "$log_file" ]] && grep -q "$pattern" "$log_file"; then
      return 0
    fi

    local now_ts
    now_ts=$(date +%s)
    if (( now_ts - start_ts >= timeout_sec )); then
      echo "Timeout waiting for pattern '$pattern' in $log_file" >&2
      if [[ -f "$log_file" ]]; then
        tail -n 80 "$log_file" >&2 || true
      fi
      return 1
    fi
    sleep 1
  done
}

cleanup() {
  set +e
  pkill -f "bin/app_acq_r2s_node --config app/config/experiments/acq_node_0.json" >/dev/null 2>&1 || true
  pkill -f "bin/app_acq_r2s_node --config app/config/experiments/acq_node_1.json" >/dev/null 2>&1 || true
  pkill -f "bin/app_acq_r2s_node --config app/config/experiments/acq_node_2.json" >/dev/null 2>&1 || true
  pkill -f "bin/app_coin_master --config app/config/experiments/coin_master_stress.json" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

make app-coin-master app-acq-r2s-node app-udp-replayer

./bin/app_coin_master --config app/config/experiments/coin_master_stress.json > "$LOG_DIR/coin_master.log" 2>&1 &
sleep 2

./bin/app_acq_r2s_node --config app/config/experiments/acq_node_0.json > "$LOG_DIR/acq_node_0.log" 2>&1 &
./bin/app_acq_r2s_node --config app/config/experiments/acq_node_1.json > "$LOG_DIR/acq_node_1.log" 2>&1 &
./bin/app_acq_r2s_node --config app/config/experiments/acq_node_2.json > "$LOG_DIR/acq_node_2.log" 2>&1 &

wait_for_log_pattern "$LOG_DIR/acq_node_0.log" "Aquisition started." 60
wait_for_log_pattern "$LOG_DIR/acq_node_1.log" "Aquisition started." 60
wait_for_log_pattern "$LOG_DIR/acq_node_2.log" "Aquisition started." 60

RPIDS=()
./bin/app_udp_raw_replayer \
  --raw-path "Data/bdm2/split_Data/2_PET_2Bed pet 600s-bed0_ch0_ch1_ch2_ch3.raw" \
  --source-ip 127.0.0.1 --destination-ip 127.0.0.1 \
  --source-port-base 17100 --destination-port-base 18100 \
  --channel-count 4 --channel-offset 0 --max-segments 180 --inter-packet-us 0 --repeat 4 > "$LOG_DIR/replayer_0.log" 2>&1 &
RPIDS+=("$!")

./bin/app_udp_raw_replayer \
  --raw-path "Data/bdm2/split_Data/2_PET_2Bed pet 600s-bed0_ch0_ch1_ch2_ch3.raw" \
  --source-ip 127.0.0.1 --destination-ip 127.0.0.1 \
  --source-port-base 17104 --destination-port-base 18104 \
  --channel-count 4 --channel-offset 0 --max-segments 180 --inter-packet-us 0 --repeat 4 > "$LOG_DIR/replayer_1.log" 2>&1 &
RPIDS+=("$!")

./bin/app_udp_raw_replayer \
  --raw-path "Data/bdm2/split_Data/2_PET_2Bed pet 600s-bed0_ch0_ch1_ch2_ch3.raw" \
  --source-ip 127.0.0.1 --destination-ip 127.0.0.1 \
  --source-port-base 17108 --destination-port-base 18108 \
  --channel-count 4 --channel-offset 0 --max-segments 180 --inter-packet-us 0 --repeat 4 > "$LOG_DIR/replayer_2.log" 2>&1 &
RPIDS+=("$!")

wait "${RPIDS[@]}"
sleep 25
cleanup

echo "Stress run finished. Logs: $LOG_DIR"
