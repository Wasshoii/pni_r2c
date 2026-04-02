#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../common/common.sh"

prepare_role_env
LOG_DIR=$(new_log_dir "stability" "C")

RAW_PATH="${RAW_PATH:-Data/bdm2/split_Data/2_PET_2Bed pet 600s-bed0_ch0_ch1_ch2_ch3.raw}"
MAX_SEGMENTS="${MAX_SEGMENTS:-300}"
INTER_PACKET_US="${INTER_PACKET_US:-5}"
REPEAT="${REPEAT:-120}"

cd "$ROOT_DIR"
make app-udp-replayer

RPIDS=()
./bin/app_udp_raw_replayer \
  --raw-path "$RAW_PATH" \
  --source-ip "$C_SENDER_IP" --destination-ip "$B1_ACQ_IP" \
  --source-port-base 17100 --destination-port-base 18100 \
  --channel-count 4 --channel-offset 0 \
  --max-segments "$MAX_SEGMENTS" --inter-packet-us "$INTER_PACKET_US" --repeat "$REPEAT" \
  > "$LOG_DIR/replayer_b1.log" 2>&1 &
RPIDS+=("$!")

./bin/app_udp_raw_replayer \
  --raw-path "$RAW_PATH" \
  --source-ip "$C_SENDER_IP" --destination-ip "$B2_ACQ_IP" \
  --source-port-base 17104 --destination-port-base 18104 \
  --channel-count 4 --channel-offset 0 \
  --max-segments "$MAX_SEGMENTS" --inter-packet-us "$INTER_PACKET_US" --repeat "$REPEAT" \
  > "$LOG_DIR/replayer_b2.log" 2>&1 &
RPIDS+=("$!")

wait "${RPIDS[@]}"

echo "[C] stability replay finished, logs: $LOG_DIR"
