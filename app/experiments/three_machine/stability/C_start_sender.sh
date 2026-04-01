#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../common/common.sh"

prepare_role_env
LOG_DIR=$(new_log_dir "stability" "C")

RAW_PATH="${RAW_PATH:-Data/bdm2/split_Data/2_PET_2Bed pet 600s-bed0_ch0_ch1_ch2_ch3.raw}"
MAX_SEGMENTS="${MAX_SEGMENTS:-120}"
INTER_PACKET_US="${INTER_PACKET_US:-10}"
DURATION_SEC="${DURATION_SEC:-1800}"

cd "$ROOT_DIR"
make app-udp-replayer

end_ts=$(( $(date +%s) + DURATION_SEC ))
round=0
while (( $(date +%s) < end_ts )); do
  round=$((round + 1))
  ./bin/app_udp_raw_replayer \
    --raw-path "$RAW_PATH" \
    --source-ip "$C_SENDER_IP" --destination-ip "$B_ACQ_IP" \
    --source-port-base 17100 --destination-port-base 18100 \
    --channel-count 4 --channel-offset 0 \
    --max-segments "$MAX_SEGMENTS" --inter-packet-us "$INTER_PACKET_US" --repeat 1 \
    >> "$LOG_DIR/replayer.log" 2>&1
  echo "[C] stability round=$round done" >> "$LOG_DIR/replayer.log"
done

echo "[C] stability sender finished, log: $LOG_DIR/replayer.log"
