#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

LOG_DIR="app/experiments/logs/correctness_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOG_DIR"

cleanup() {
  set +e
  pkill -f "bin/app_acq_r2s_node --config app/config/experiments/acq_node_0.json" >/dev/null 2>&1 || true
  pkill -f "bin/app_acq_r2s_node --config app/config/experiments/acq_node_1.json" >/dev/null 2>&1 || true
  pkill -f "bin/app_acq_r2s_node --config app/config/experiments/acq_node_2.json" >/dev/null 2>&1 || true
  pkill -f "bin/app_coin_master --config app/config/experiments/coin_master_correctness.json" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

make app-coin-master app-acq-r2s-node app-udp-replayer

./bin/app_coin_master --config app/config/experiments/coin_master_correctness.json > "$LOG_DIR/coin_master.log" 2>&1 &
sleep 2

./bin/app_acq_r2s_node --config app/config/experiments/acq_node_0.json > "$LOG_DIR/acq_node_0.log" 2>&1 &
./bin/app_acq_r2s_node --config app/config/experiments/acq_node_1.json > "$LOG_DIR/acq_node_1.log" 2>&1 &
./bin/app_acq_r2s_node --config app/config/experiments/acq_node_2.json > "$LOG_DIR/acq_node_2.log" 2>&1 &

sleep 8

./bin/app_udp_raw_replayer \
  --raw-path "Data/bdm2/split_Data/2_PET_2Bed pet 600s-bed0_ch0_ch1_ch2_ch3.raw" \
  --source-ip 127.0.0.1 --destination-ip 127.0.0.1 \
  --source-port-base 17100 --destination-port-base 18100 \
  --channel-count 4 --max-segments 80 --inter-packet-us 2 --repeat 1 > "$LOG_DIR/replayer_0.log" 2>&1 &
RP0=$!

./bin/app_udp_raw_replayer \
  --raw-path "Data/bdm2/split_Data/2_PET_2Bed pet 600s-bed0_ch4_ch5_ch6_ch7.raw" \
  --source-ip 127.0.0.1 --destination-ip 127.0.0.1 \
  --source-port-base 17104 --destination-port-base 18104 \
  --channel-count 4 --max-segments 80 --inter-packet-us 2 --repeat 1 > "$LOG_DIR/replayer_1.log" 2>&1 &
RP1=$!

./bin/app_udp_raw_replayer \
  --raw-path "Data/bdm2/split_Data/2_PET_2Bed pet 600s-bed0_ch8_ch9_ch10_ch11.raw" \
  --source-ip 127.0.0.1 --destination-ip 127.0.0.1 \
  --source-port-base 17108 --destination-port-base 18108 \
  --channel-count 4 --max-segments 80 --inter-packet-us 2 --repeat 1 > "$LOG_DIR/replayer_2.log" 2>&1 &
RP2=$!

wait "$RP0" "$RP1" "$RP2"

sleep 10
cleanup

echo "Correctness run finished. Logs: $LOG_DIR"
