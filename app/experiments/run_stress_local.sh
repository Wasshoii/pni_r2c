#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

LOG_DIR="app/experiments/logs/stress_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOG_DIR"

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

sleep 8

RPIDS=()
for i in 0 1 2 3 4 5; do
  ./bin/app_udp_raw_replayer \
    --raw-path "Data/bdm2/split_Data/2_PET_2Bed pet 600s-bed0_ch0_ch1_ch2_ch3.raw" \
    --source-ip 127.0.0.1 --destination-ip 127.0.0.1 \
    --source-port-base 17100 --destination-port-base 18100 \
    --channel-count 4 --max-segments 180 --inter-packet-us 0 --repeat 2 > "$LOG_DIR/replayer_${i}.log" 2>&1 &
  RPIDS+=("$!")
done

wait "${RPIDS[@]}"
sleep 15
cleanup

echo "Stress run finished. Logs: $LOG_DIR"
