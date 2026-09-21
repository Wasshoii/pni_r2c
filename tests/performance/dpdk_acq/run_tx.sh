#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
LOG_DIR="${ROOT_DIR}/tests/performance/dpdk_acq/logs"

PROFILE="930"
DST_MAC=""
SOURCE_IP="10.10.1.10"
DEST_IP=""
PORT_ID="0"
PPS="0"
DURATION_SEC="10"
PAYLOAD_SIZE="512"
TX_BIN=""
EAL_ARGS="--file-prefix=dpdk_tx_replayer_acq"

usage() {
  cat <<'EOF'
Usage:
  run_tx.sh --dst-mac <mac> --destination-ip <rx-ip> [options]

Options:
  --dst-mac <mac>            RX NIC MAC (required)
  --destination-ip <ipv4>    RX DPDK logical IP (required)
  --source-ip <ipv4>         UDP source IP in packets (default: 10.10.1.10)
  --profile 930|9120_2ring   channel count (144 or 288)
  --port-id <n>              TX DPDK port id (default: 0)
  --pps <n>                  0 = max (default)
  --duration-sec N           send duration (default: 10)
  --payload-size N           UDP payload bytes (default: 512)
  --tx-bin <path>            override tool_dpdk_tx_replayer
  --eal-args "..."           extra EAL args
  --help

TX and RX cannot share one vfio-bound NIC. Run this on the send machine.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dst-mac) DST_MAC="$2"; shift 2 ;;
    --destination-ip) DEST_IP="$2"; shift 2 ;;
    --source-ip) SOURCE_IP="$2"; shift 2 ;;
    --profile) PROFILE="$2"; shift 2 ;;
    --port-id) PORT_ID="$2"; shift 2 ;;
    --pps) PPS="$2"; shift 2 ;;
    --duration-sec) DURATION_SEC="$2"; shift 2 ;;
    --payload-size) PAYLOAD_SIZE="$2"; shift 2 ;;
    --tx-bin) TX_BIN="$2"; shift 2 ;;
    --eal-args) EAL_ARGS="$2"; shift 2 ;;
    --help) usage; exit 0 ;;
    *) echo "Unknown argument: $1"; usage; exit 1 ;;
  esac
done

if [[ -z "${DST_MAC}" || -z "${DEST_IP}" ]]; then
  echo "[ERROR] --dst-mac and --destination-ip are required"
  usage
  exit 1
fi

CHANNEL_COUNT=144
case "${PROFILE}" in
  930) CHANNEL_COUNT=144 ;;
  9120_2ring) CHANNEL_COUNT=288 ;;
  *) echo "[ERROR] unknown --profile ${PROFILE}"; exit 1 ;;
esac

resolve_bin() {
  local candidate
  for candidate in "$@"; do
    if [[ -x "${candidate}" ]]; then
      printf '%s' "${candidate}"
      return 0
    fi
  done
  printf '%s' "$1"
}

if [[ -z "${TX_BIN}" ]]; then
  TX_BIN="$(resolve_bin \
    "${ROOT_DIR}/bin/tools/tool_dpdk_tx_replayer" \
    "${ROOT_DIR}/build/tools/bin/tools/tool_dpdk_tx_replayer")"
fi
if [[ ! -x "${TX_BIN}" ]]; then
  echo "[ERROR] missing ${TX_BIN}"
  echo "[HINT] cmake --preset linux-release-tools && cmake --build --preset build-tools --target tool_dpdk_tx_replayer"
  exit 1
fi

mkdir -p "${LOG_DIR}"
TX_LOG="${LOG_DIR}/tx_${PROFILE}.log"
: > "${TX_LOG}"

echo "[INFO] profile=${PROFILE} channels=${CHANNEL_COUNT} dst=${DEST_IP} mac=${DST_MAC} pps=${PPS}"
cd "${ROOT_DIR}"
"${TX_BIN}" \
  --dst-mac "${DST_MAC}" \
  --port-id "${PORT_ID}" \
  --source-ip "${SOURCE_IP}" \
  --destination-ip "${DEST_IP}" \
  --source-port-base 17100 \
  --destination-port-base 18100 \
  --channel-count "${CHANNEL_COUNT}" \
  --payload-size "${PAYLOAD_SIZE}" \
  --pps "${PPS}" \
  --duration-sec "${DURATION_SEC}" \
  --eal-args "${EAL_ARGS}" \
  | tee "${TX_LOG}"
