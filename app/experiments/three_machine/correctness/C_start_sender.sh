#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../common/common.sh"

prepare_role_env
LOG_DIR=$(new_log_dir "correctness" "C")

RAW_PATH="${RAW_PATH:-Data/bdm2/split_Data/2_PET_2Bed pet 600s-bed0_ch0_ch1_ch2_ch3.raw}"
MAX_SEGMENTS="${MAX_SEGMENTS:-120}"
INTER_PACKET_US="${INTER_PACKET_US:-2}"
REPEAT="${REPEAT:-1}"

cd "$ROOT_DIR"
make app-udp-replayer

./bin/app_udp_raw_replayer \
  --raw-path "$RAW_PATH" \
  --source-ip "$C_SENDER_IP" --destination-ip "$B_ACQ_IP" \
  --source-port-base 17100 --destination-port-base 18100 \
  --channel-count 4 --channel-offset 0 \
  --max-segments "$MAX_SEGMENTS" --inter-packet-us "$INTER_PACKET_US" --repeat "$REPEAT" \
  > "$LOG_DIR/replayer.log" 2>&1

echo "[C] replay finished, log: $LOG_DIR/replayer.log"

if [[ -n "${PROMPT_REF_MD5:-}" && -f "${PROMPT_FILE:-Data/result/Bdm2/pniCoin/three_machine_correctness/prompt.lmf}" ]]; then
  ACTUAL_PROMPT_MD5=$(md5sum "${PROMPT_FILE:-Data/result/Bdm2/pniCoin/three_machine_correctness/prompt.lmf}" | awk '{print $1}')
  echo "[C] prompt md5 expected=$PROMPT_REF_MD5 actual=$ACTUAL_PROMPT_MD5"
fi

if [[ -n "${DELAY_REF_MD5:-}" && -f "${DELAY_FILE:-Data/result/Bdm2/pniCoin/three_machine_correctness/delay.lmf}" ]]; then
  ACTUAL_DELAY_MD5=$(md5sum "${DELAY_FILE:-Data/result/Bdm2/pniCoin/three_machine_correctness/delay.lmf}" | awk '{print $1}')
  echo "[C] delay md5 expected=$DELAY_REF_MD5 actual=$ACTUAL_DELAY_MD5"
fi
