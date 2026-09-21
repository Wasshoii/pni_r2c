#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
LOG_DIR="${ROOT_DIR}/tests/performance/dpdk_acq/logs"

PROFILE="930"
BIND_IP=""
SOURCE_IP=""
DURATION_SEC=20
WRITE_RAW=0
MASTER_BIN=""
NODE_BIN=""

usage() {
  cat <<'EOF'
Usage:
  run_rx.sh --bind-ip <rx-data-ip> [options]

Options:
  --bind-ip <ipv4>       RX DPDK logical IP (required; also destinationIp / dpdkBindIps)
  --source-ip <ipv4>     UDP source IP in the mapping (default: same as --bind-ip for loopback lab;
                         dual-machine: TX packet source IP)
  --profile 930|9120_2ring   channel preset (default: 930 = 144 ch; 9120_2ring = 288 ch)
  --duration-sec N       master run time after START (default: 20)
  --write-raw            tell the node to roll raw files
  --master-bin <path>    override test_dpdk_acq_master
  --node-bin <path>      override test_dpdk_acq_node
  --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bind-ip) BIND_IP="$2"; shift 2 ;;
    --source-ip) SOURCE_IP="$2"; shift 2 ;;
    --profile) PROFILE="$2"; shift 2 ;;
    --duration-sec) DURATION_SEC="$2"; shift 2 ;;
    --write-raw) WRITE_RAW=1; shift ;;
    --master-bin) MASTER_BIN="$2"; shift 2 ;;
    --node-bin) NODE_BIN="$2"; shift 2 ;;
    --help) usage; exit 0 ;;
    *) echo "Unknown argument: $1"; usage; exit 1 ;;
  esac
done

if [[ -z "${BIND_IP}" ]]; then
  echo "[ERROR] --bind-ip is required"
  usage
  exit 1
fi
if [[ -z "${SOURCE_IP}" ]]; then
  SOURCE_IP="${BIND_IP}"
fi

case "${PROFILE}" in
  930) TEMPLATE="${SCRIPT_DIR}/configs/acq_930.json" ;;
  9120_2ring) TEMPLATE="${SCRIPT_DIR}/configs/acq_9120_2ring.json" ;;
  *) echo "[ERROR] unknown --profile ${PROFILE} (use 930 or 9120_2ring)"; exit 1 ;;
esac

resolve_bin() {
  local name="$1"
  shift
  local candidate
  for candidate in "$@"; do
    if [[ -x "${candidate}" ]]; then
      printf '%s' "${candidate}"
      return 0
    fi
  done
  printf '%s' "$1"
}

if [[ -z "${MASTER_BIN}" ]]; then
  MASTER_BIN="$(resolve_bin test_dpdk_acq_master \
    "${ROOT_DIR}/bin/test/test_dpdk_acq_master" \
    "${ROOT_DIR}/build/tests/pni/bin/test/test_dpdk_acq_master")"
fi
if [[ -z "${NODE_BIN}" ]]; then
  NODE_BIN="$(resolve_bin test_dpdk_acq_node \
    "${ROOT_DIR}/bin/test/test_dpdk_acq_node" \
    "${ROOT_DIR}/build/tests/pni/bin/test/test_dpdk_acq_node")"
fi

if [[ ! -x "${MASTER_BIN}" ]]; then
  echo "[ERROR] missing ${MASTER_BIN}"
  echo "[HINT] cmake --preset linux-release-tests-pni && cmake --build --preset build-tests-pni --target test_dpdk_acq_master"
  exit 1
fi
if [[ ! -x "${NODE_BIN}" ]]; then
  echo "[ERROR] missing ${NODE_BIN}"
  echo "[HINT] cmake --preset linux-release-tests-pni && cmake --build --preset build-tests-pni --target test_dpdk_acq_node"
  exit 1
fi

mkdir -p "${LOG_DIR}"
RENDERED="${LOG_DIR}/acq_${PROFILE}.auto.json"
sed -e "s/__BIND_IP__/${BIND_IP}/g" -e "s/__SOURCE_IP__/${SOURCE_IP}/g" "${TEMPLATE}" > "${RENDERED}"

MASTER_LOG="${LOG_DIR}/master_${PROFILE}.log"
NODE_LOG="${LOG_DIR}/node_${PROFILE}.log"
: > "${MASTER_LOG}"
: > "${NODE_LOG}"

MASTER_PID=""
NODE_PID=""
cleanup() {
  set +e
  if [[ -n "${NODE_PID}" ]] && kill -0 "${NODE_PID}" 2>/dev/null; then
    kill -TERM "${NODE_PID}" 2>/dev/null || true
  fi
  if [[ -n "${MASTER_PID}" ]] && kill -0 "${MASTER_PID}" 2>/dev/null; then
    kill -TERM "${MASTER_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

echo "[INFO] profile=${PROFILE} bind_ip=${BIND_IP} source_ip=${SOURCE_IP} duration=${DURATION_SEC}s write_raw=${WRITE_RAW}"
echo "[INFO] config=${RENDERED}"

(
  cd "${ROOT_DIR}"
  GLOG_logtostderr=1 "${MASTER_BIN}" --config "${RENDERED}" --duration-sec "${DURATION_SEC}" >>"${MASTER_LOG}" 2>&1
) &
MASTER_PID=$!
sleep 1

NODE_ARGS=(--config "${RENDERED}")
if [[ "${WRITE_RAW}" -eq 1 ]]; then
  NODE_ARGS+=(--write-raw)
fi
(
  cd "${ROOT_DIR}"
  GLOG_logtostderr=1 "${NODE_BIN}" "${NODE_ARGS[@]}" >>"${NODE_LOG}" 2>&1
) &
NODE_PID=$!

echo "[INFO] master pid=${MASTER_PID} log=${MASTER_LOG}"
echo "[INFO] node pid=${NODE_PID} log=${NODE_LOG}"
echo "[INFO] waiting for master to finish (duration-sec=${DURATION_SEC})..."

set +e
wait "${MASTER_PID}"
MASTER_RC=$?
wait "${NODE_PID}"
NODE_RC=$?
set -e
MASTER_PID=""
NODE_PID=""

echo "[INFO] master_rc=${MASTER_RC} node_rc=${NODE_RC}"
echo "[INFO] last master stats:"
grep '\[DPDK-ACQ-MASTER\]' "${MASTER_LOG}" | tail -n 5 || true
exit "${MASTER_RC}"
