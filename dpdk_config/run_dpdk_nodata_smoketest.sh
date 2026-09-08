#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_APPS_DIR="${ROOT_DIR}/build/apps"
BUILD_BASIC_DIR="${BUILD_APPS_DIR}/basic"
BUILD_CUDA_DIR="${BUILD_APPS_DIR}/cuda"
CFG_DIR="${ROOT_DIR}/app/config/experiments/no_data_auto"
LOG_DIR="${ROOT_DIR}/dpdk_config/logs"

APP_COIN_MASTER_BIN="${BUILD_BASIC_DIR}/app_coin_master"
APP_ACQ_R2S_NODE_BIN="${BUILD_CUDA_DIR}/app_acq_r2s_node"

MASTER_CFG="${CFG_DIR}/coin_master_dpdk_nodata.auto.json"
NODE_CFG="${CFG_DIR}/acq_r2s_node_dpdk_nodata.auto.json"
MASTER_LOG="${LOG_DIR}/coin_master_nodata.log"
NODE_LOG="${LOG_DIR}/acq_node_nodata.log"

MASTER_PID=""
NODE_PID=""

usage() {
  cat <<'EOF'
Usage:
  run_dpdk_nodata_smoketest.sh [--skip-build] [--skip-precheck] [--duration-sec N] [--stop-only]

What this script does:
  1) Auto-detect NIC/IP/NUMA and CPU cores, then regenerate minimal no-data configs.
  2) (Optional) run dpdk_precheck.sh.
  3) Build app binaries if missing.
  4) Start app_coin_master and app_acq_r2s_node.
  5) Inject synthetic UDP packets (no business raw data required).
  6) Evaluate logs and print PASS/WARN/FAIL summary.

Notes:
  - This is a smoke test for DPDK acquisition path readiness and config wiring.
  - If host has no DPDK-capable NIC bound to vfio/uio, DPDK init may still fail.
  - Use --stop-only to stop lingering app_coin_master/app_acq_r2s_node processes.
EOF
}

SKIP_BUILD=0
SKIP_PRECHECK=0
DURATION_SEC=20
STOP_ONLY=0

terminate_pid_gracefully() {
  set +e
  local pid="$1"
  local name="$2"
  local i

  if [[ -z "${pid}" ]] || ! kill -0 "${pid}" 2>/dev/null; then
    return 0
  fi

  echo "[INFO] stopping ${name} pid=${pid}"
  pkill -TERM -P "${pid}" >/dev/null 2>&1 || true
  kill -TERM "${pid}" >/dev/null 2>&1 || true

  for ((i=0; i<20; ++i)); do
    if ! kill -0 "${pid}" 2>/dev/null; then
      return 0
    fi
    sleep 0.2
  done

  echo "[WARN] force killing ${name} pid=${pid}"
  pkill -KILL -P "${pid}" >/dev/null 2>&1 || true
  kill -KILL "${pid}" >/dev/null 2>&1 || true
}

stop_targets() {
  set +e
  local pids
  pids="$(pgrep -f 'build/apps/basic/app_coin_master|build/apps/cuda/app_acq_r2s_node|app_coin_master|app_acq_r2s_node' || true)"
  if [[ -z "${pids}" ]]; then
    echo "[INFO] no target process to stop"
    return 0
  fi

  echo "[INFO] stopping target processes: ${pids}"
  kill -TERM ${pids} >/dev/null 2>&1 || true
  sleep 1

  pids="$(pgrep -f 'build/apps/basic/app_coin_master|build/apps/cuda/app_acq_r2s_node|app_coin_master|app_acq_r2s_node' || true)"
  if [[ -n "${pids}" ]]; then
    echo "[INFO] force killing remaining processes: ${pids}"
    kill -KILL ${pids} >/dev/null 2>&1 || true
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-build)
      SKIP_BUILD=1
      shift
      ;;
    --skip-precheck)
      SKIP_PRECHECK=1
      shift
      ;;
    --duration-sec)
      DURATION_SEC="$2"
      shift 2
      ;;
    --stop-only)
      STOP_ONLY=1
      shift
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] unknown arg: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ "${STOP_ONLY}" -eq 1 ]]; then
  stop_targets
  exit 0
fi

cleanup() {
  set +e
  terminate_pid_gracefully "${NODE_PID}" "app_acq_r2s_node"
  terminate_pid_gracefully "${MASTER_PID}" "app_coin_master"

  # Final guard in case PIDs changed or detached unexpectedly.
  stop_targets >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

# Prevent leftover background processes from previous interrupted runs.
stop_targets

mkdir -p "${CFG_DIR}" "${LOG_DIR}"

if ! command -v ip >/dev/null 2>&1; then
  echo "[ERROR] missing 'ip' command"
  exit 1
fi

mapfile -t ip_lines < <(ip -4 -o addr show up scope global | awk '!/ lo / {print $2" "$4}')
if [[ "${#ip_lines[@]}" -eq 0 ]]; then
  echo "[ERROR] no active global IPv4 interface found"
  exit 1
fi

NIC_IFACE="$(awk '{print $1}' <<<"${ip_lines[0]}")"
NIC_CIDR="$(awk '{print $2}' <<<"${ip_lines[0]}")"
BIND_IP="${NIC_CIDR%%/*}"

NIC_NUMA=-1
if [[ -f "/sys/class/net/${NIC_IFACE}/device/numa_node" ]]; then
  NIC_NUMA="$(cat "/sys/class/net/${NIC_IFACE}/device/numa_node" 2>/dev/null || echo -1)"
fi

mapfile -t all_cpus < <(lscpu -p=CPU 2>/dev/null | sed '/^#/d' | awk -F, '{print $1}')
if [[ "${#all_cpus[@]}" -eq 0 ]]; then
  cpu_count="$(nproc --all)"
  for ((i=0; i<cpu_count; ++i)); do all_cpus+=("$i"); done
fi

selected_cpus=()
if [[ "${NIC_NUMA}" =~ ^[0-9]+$ ]]; then
  mapfile -t numa_cpus < <(lscpu -p=CPU,NODE 2>/dev/null | sed '/^#/d' | awk -F, -v n="${NIC_NUMA}" '$2==n {print $1}')
  if [[ "${#numa_cpus[@]}" -gt 0 ]]; then
    all_cpus=("${numa_cpus[@]}")
  fi
fi

limit=4
if [[ "${#all_cpus[@]}" -lt 4 ]]; then
  limit="${#all_cpus[@]}"
fi
for ((i=0; i<limit; ++i)); do
  selected_cpus+=("${all_cpus[$i]}")
done
if [[ "${#selected_cpus[@]}" -eq 0 ]]; then
  selected_cpus=(0)
fi

CPU_JSON=""
for c in "${selected_cpus[@]}"; do
  if [[ -n "${CPU_JSON}" ]]; then
    CPU_JSON+=" ,"
  fi
  CPU_JSON+=" ${c}"
done

STRICT_NUMA=false
REQ_CPU_NUMA=false
EXPECTED_NUMA=-1
if [[ "${NIC_NUMA}" =~ ^[0-9]+$ ]]; then
  STRICT_NUMA=true
  REQ_CPU_NUMA=true
  EXPECTED_NUMA="${NIC_NUMA}"
fi

cat > "${MASTER_CFG}" <<EOF
{
  "coinMaster": {
    "listenAddress": "0.0.0.0:50061",
    "expectedNodeCount": 1,
    "autoStartWhenAllRegistered": true,
    "startLeadTimeMs": 800,
    "waitForStartDefaultTimeoutMs": 30000,
    "rejectStreamBeforeStart": true,
    "statusPrintIntervalMs": 1000,
    "runSeconds": 0
  },
  "aligner": {
    "outputDir": "Data/result/Bdm2/pniCoin/exp_nodata_dpdk",
    "channelNum": 48,
    "crystalsPerChannel": 676,
    "networkLatencyMarginPico": 0,
    "processingIntervalMs": 200,
    "maxChunksPerNode": 100,
    "maxTotalMemoryBytes": 2147483648,
    "useMemoryPool": true,
    "savePrompt": false,
    "saveDelay": false,
    "coinProtocol": {
      "timeWindowPs": 2000,
      "delayTimePs": 2000000,
      "energyLowerEV": 350000.0,
      "energyUpperEV": 650000.0
    }
  },
  "acquisitionControl": {
    "enabled": true,
    "masterAddress": "127.0.0.1:50093",
    "autoDistributeWhenAllConnected": true,
    "autoStartOnCoinStartSignal": false,
    "startDurationMs": 0,
    "acquisitionAlgorithm": "dpdk",
    "sourcePortBase": 17100,
    "destinationPortBase": 18100,
    "channelCount": 1,
    "sourceIp": "${BIND_IP}",
    "detectorSources": [
      {"detectorId": "d0", "sourceIp": "${BIND_IP}", "sourcePort": 17100}
    ],
    "destinationIp": "${BIND_IP}",
    "sessionName": "nodata_dpdk_session",
    "storageUnitSize": 2048,
    "minPacketSize": 1,
    "maxBufferSize": 4294967296,
    "timeSwitchBufferMs": 200,
    "reservedStorageGiB": 20,
    "maxFileSizeMb": 256,
    "dpdkCopyThreadNum": 4,
    "dpdkRxRingsPerPort": 1,
    "dpdkMbufDoublePointerSizeMultiply": 32,
    "dpdkMbufDoublePointerNumMultiply": 2,
    "dpdkBindIps": ["${BIND_IP}"],
    "nodeOverrides": [
      {
        "nodeId": "acq-r2s-node-0",
        "acquisitionAlgorithm": "dpdk",
        "dpdkBindIps": ["${BIND_IP}"]
      }
    ]
  }
}
EOF

cat > "${NODE_CFG}" <<EOF
{
  "acqNode": {
    "masterAddress": "127.0.0.1:50093",
    "nodeId": "acq-r2s-node-0",
    "nodeAddress": "${BIND_IP}",
    "outputRoot": "Data/raw_data",
    "sessionNamePrefix": "nodata_dpdk_node",
    "statusIntervalMs": 500,
    "enableRawFileWrite": false
  },
  "r2s": {
    "calibrationDir": "Data/bdm2/calibration",
    "resultDir": "Data/result/Bdm2/split",
    "channelIndices": [0],
    "sortDataByTime": true,
    "saveData2SingleFile": false,
    "asyncFileWrite": false
  },
  "bridge": {
    "enabled": false,
    "queueCapacity": 64,
    "reservePacketsPerSlot": 512,
    "reserveBytesPerSlot": 1048576,
    "blockWhenQueueFull": true,
    "queueFullWarnEvery": 5000,
    "inputChannelCount": 1
  },
  "coinClient": {
    "enabled": true,
    "serverAddress": "127.0.0.1:50061",
    "nodeId": 0,
    "nodeAddress": "${BIND_IP}",
    "channelCount": 1,
    "detectorType": "BDM2",
    "remapLocalToGlobalChannels": false,
    "globalChannelOffset": 0,
    "crystalsPerChannel": 676,
    "maxPendingChunks": 32,
    "batchSize": 1,
    "heartbeatIntervalMs": 5000,
    "waitForStartSignal": false,
    "waitForStartTimeoutMs": 0,
    "waitForStartRpcTimeoutMs": 15000,
    "waitForStartRetryIntervalMs": 1000
  },
  "runtime": {
    "shutdownGraceMs": 1000,
    "enableCpuAffinity": true,
    "cpuAffinityCores": [${CPU_JSON}],
    "strictBindIpsOwnershipCheck": true,
    "strictNumaTopologyCheck": ${STRICT_NUMA},
    "requireBindIpsSingleNuma": true,
    "requireCpuAffinityOnNuma": ${REQ_CPU_NUMA},
    "expectedNumaNode": ${EXPECTED_NUMA}
  }
}
EOF

echo "[INFO] generated configs:"
echo "  ${MASTER_CFG}"
echo "  ${NODE_CFG}"
echo "[INFO] selected iface=${NIC_IFACE} bind_ip=${BIND_IP} iface_numa=${NIC_NUMA} cpus=[${CPU_JSON}]"

if [[ "${SKIP_PRECHECK}" -eq 0 && -x "${SCRIPT_DIR}/dpdk_precheck.sh" ]]; then
  echo "[INFO] running dpdk precheck..."
  "${SCRIPT_DIR}/dpdk_precheck.sh" || true
fi

if [[ "${SKIP_BUILD}" -eq 0 ]]; then
  if [[ ! -x "${APP_COIN_MASTER_BIN}" ]]; then
    echo "[INFO] building app_coin_master..."
    cmake --build "${BUILD_BASIC_DIR}" --target app_coin_master
  fi
  if [[ ! -x "${APP_ACQ_R2S_NODE_BIN}" ]]; then
    echo "[INFO] building app_acq_r2s_node..."
    cmake --build "${BUILD_CUDA_DIR}" --target app_acq_r2s_node
  fi
fi

if [[ ! -x "${APP_COIN_MASTER_BIN}" ]]; then
  echo "[ERROR] missing binary: ${APP_COIN_MASTER_BIN}"
  echo "[HINT] build it by: cmake --build ${BUILD_BASIC_DIR} --target app_coin_master"
  exit 1
fi
if [[ ! -x "${APP_ACQ_R2S_NODE_BIN}" ]]; then
  echo "[ERROR] missing binary: ${APP_ACQ_R2S_NODE_BIN}"
  echo "[HINT] build it by: cmake --build ${BUILD_CUDA_DIR} --target app_acq_r2s_node"
  exit 1
fi

: > "${MASTER_LOG}"
: > "${NODE_LOG}"

echo "[INFO] starting coin master..."
(
  cd "${ROOT_DIR}"
  GLOG_logtostderr=1 "${APP_COIN_MASTER_BIN}" --config "${MASTER_CFG}" >>"${MASTER_LOG}" 2>&1
) &
MASTER_PID=$!

sleep 1
echo "[INFO] starting acq_r2s node..."
(
  cd "${ROOT_DIR}"
  GLOG_logtostderr=1 "${APP_ACQ_R2S_NODE_BIN}" --config "${NODE_CFG}" >>"${NODE_LOG}" 2>&1
) &
NODE_PID=$!

echo "[INFO] waiting ${DURATION_SEC}s for configuration/distribution..."
sleep "${DURATION_SEC}"

echo "[INFO] injecting synthetic UDP packets to ${BIND_IP}:18100 ..."
python3 - <<PY
import socket
import time
payload = b'\xAB' * 512
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("${BIND_IP}", 17100))
for _ in range(3000):
    sock.sendto(payload, ("${BIND_IP}", 18100))
    time.sleep(0.0002)
sock.close()
print("udp_injection_done")
PY

sleep 2

PASS=0
WARN=0
FAIL=0

if grep -q "AcquisitionMaster started" "${MASTER_LOG}"; then
  echo "[PASS] coin master started"
  PASS=$((PASS+1))
else
  echo "[FAIL] coin master did not report start"
  FAIL=$((FAIL+1))
fi

if grep -q "acqControl.algorithm     : dpdk" "${MASTER_LOG}"; then
  echo "[PASS] master config uses DPDK"
  PASS=$((PASS+1))
else
  echo "[WARN] did not find explicit DPDK config print in master log"
  WARN=$((WARN+1))
fi

if grep -Eq "connected=1/1|all nodes registered|Start signal issued" "${MASTER_LOG}"; then
  echo "[PASS] coin master and node connected / start signal observed"
  PASS=$((PASS+1))
else
  echo "[WARN] no coin-node connection/start signal marker found"
  WARN=$((WARN+1))
fi

if grep -Eiq "DPDK initialization failed|STATE_ERROR|bind_ips not found on local NICs|runtime requested but this build does not enable DPDK|does not enable PNI_STANDARD_CONFIG_ENABLE_DPDK" "${NODE_LOG}"; then
  echo "[FAIL] node log contains DPDK/runtime errors"
  FAIL=$((FAIL+1))
else
  echo "[PASS] no obvious DPDK/runtime fatal errors in node log"
  PASS=$((PASS+1))
fi

if grep -q "CPU affinity enabled" "${NODE_LOG}"; then
  echo "[PASS] CPU affinity applied"
  PASS=$((PASS+1))
else
  echo "[WARN] CPU affinity marker not found in node log"
  WARN=$((WARN+1))
fi

if grep -q "algorithm=DPDK" "${NODE_LOG}"; then
  echo "[PASS] node received DPDK configure task"
  PASS=$((PASS+1))
else
  echo "[WARN] no explicit 'algorithm=DPDK' marker found; check full log"
  WARN=$((WARN+1))
fi

echo "[INFO] summary: PASS=${PASS} WARN=${WARN} FAIL=${FAIL}"
echo "[INFO] logs:"
echo "  ${MASTER_LOG}"
echo "  ${NODE_LOG}"

if [[ "${FAIL}" -gt 0 ]]; then
  exit 2
fi
