#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${SCRIPT_DIR}/logs"

TX_LOG=""
MASTER_LOG=""

usage() {
  cat <<'EOF'
Usage:
  summarize_logs.sh [--tx-log PATH] [--master-log PATH]

Defaults: tests/performance/dpdk_acq/logs/tx_*.log and master_*.log (newest).

Prints TX sent, RX total_rx_packets / unknown / imissed, and
  loss = (tx_sent - rx_packets) / tx_sent
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tx-log) TX_LOG="$2"; shift 2 ;;
    --master-log) MASTER_LOG="$2"; shift 2 ;;
    --help) usage; exit 0 ;;
    *) echo "Unknown argument: $1"; usage; exit 1 ;;
  esac
done

latest() {
  local pattern="$1"
  ls -1t ${pattern} 2>/dev/null | head -n 1 || true
}

if [[ -z "${TX_LOG}" ]]; then
  TX_LOG="$(latest "${LOG_DIR}/tx_*.log")"
fi
if [[ -z "${MASTER_LOG}" ]]; then
  MASTER_LOG="$(latest "${LOG_DIR}/master_*.log")"
fi

if [[ -z "${TX_LOG}" || ! -f "${TX_LOG}" ]]; then
  echo "[ERROR] TX log not found"
  exit 1
fi
if [[ -z "${MASTER_LOG}" || ! -f "${MASTER_LOG}" ]]; then
  echo "[ERROR] master log not found"
  exit 1
fi

tx_sent="$(grep -E '\[DPDK-TX\] done, totalSent=' "${TX_LOG}" | tail -n 1 | sed -E 's/.*totalSent=([0-9]+).*/\1/' || true)"
if [[ -z "${tx_sent}" ]]; then
  tx_sent="$(grep -E '\[DPDK-TX\] sent=' "${TX_LOG}" | tail -n 1 | sed -E 's/.*sent=([0-9]+).*/\1/' || true)"
fi

sum_line="$(grep -E '\[DPDK-ACQ-MASTER\] sum ' "${MASTER_LOG}" | tail -n 1 || true)"
stopped_line="$(grep -E '\[DPDK-ACQ-MASTER\] stopped total_rx_packets=' "${MASTER_LOG}" | tail -n 1 || true)"

rx_packets="$(printf '%s\n' "${stopped_line}" | sed -E 's/.*total_rx_packets=([0-9]+).*/\1/' || true)"
if [[ -z "${rx_packets}" || "${rx_packets}" == "${stopped_line}" ]]; then
  rx_packets="$(printf '%s\n' "${sum_line}" | sed -E 's/.*total_rx_packets=([0-9]+).*/\1/' || true)"
fi
unknown="$(printf '%s\n' "${sum_line}" | sed -E 's/.*unknown=([0-9]+).*/\1/' || true)"
imissed="$(printf '%s\n' "${sum_line}" | sed -E 's/.*imissed=([0-9]+).*/\1/' || true)"
ierrors="$(printf '%s\n' "${sum_line}" | sed -E 's/.*ierrors=([0-9]+).*/\1/' || true)"

echo "tx_log=${TX_LOG}"
echo "master_log=${MASTER_LOG}"
echo "tx_sent=${tx_sent:-?}"
echo "rx_packets=${rx_packets:-?}"
echo "unknown=${unknown:-?}"
echo "imissed=${imissed:-?}"
echo "ierrors=${ierrors:-?}"

if [[ -n "${tx_sent}" && -n "${rx_packets}" && "${tx_sent}" =~ ^[0-9]+$ && "${rx_packets}" =~ ^[0-9]+$ ]]; then
  if [[ "${tx_sent}" -eq 0 ]]; then
    echo "loss=n/a (tx_sent=0)"
  else
    awk -v tx="${tx_sent}" -v rx="${rx_packets}" 'BEGIN {
      lost = tx - rx
      printf "lost_packets=%d\nloss=%.6f\n", lost, lost / tx
    }'
  fi
else
  echo "loss=n/a (could not parse counters)"
  exit 2
fi
