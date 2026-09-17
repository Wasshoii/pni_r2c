#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_BASIC_DIR="${ROOT_DIR}/build/apps/basic"
BUILD_CUDA_DIR="${ROOT_DIR}/build/apps/cuda"
BUILD_TOOLS_DIR="${ROOT_DIR}/build/tools"
CFG_DIR="${ROOT_DIR}/app/config/experiments/no_data_auto"
LOG_DIR="${ROOT_DIR}/dpdk_config/logs"

MASTER_CFG="${CFG_DIR}/coin_master_dpdk_nodata.auto.json"
NODE_CFG="${CFG_DIR}/acq_r2s_node_dpdk_nodata.auto.json"
MASTER_LOG="${LOG_DIR}/coin_master_nodata.log"
NODE_LOG="${LOG_DIR}/acq_node_nodata.log"
TX_LOG="${LOG_DIR}/dpdk_tx_replayer_nodata.log"

MASTER_PID=""
NODE_PID=""
TX_PID=""

BIND_IP_ARG=""
DST_MAC=""
TX_PORT_ID="0"
TX_DURATION_SEC="8"
SKIP_BUILD=0
SKIP_PRECHECK=0
SKIP_INJECT=0
DURATION_SEC=20
STOP_ONLY=0

usage() {
  cat <<'EOF'
Usage:
  run_dpdk_nodata_smoketest.sh [options]

Options:
  --bind-ip <ipv4>         DPDK bind / destination IP (default: first global IPv4)
  --dst-mac <mac>          if set, inject with tool_dpdk_tx_replayer (needs a TX DPDK port)
  --tx-port-id <n>         DPDK port id for the replayer (default: 0)
  --tx-duration-sec <n>    replayer duration (default: 8)
  --skip-inject            do not send packets; only check process/config/DPDK init
  --skip-build             do not cmake --build missing binaries
  --skip-precheck          do not run dpdk_precheck.sh
  --duration-sec N         wait after node start before inject (default: 20)
  --stop-only              stop leftover app_coin_master / app_acq_r2s_node / tool_dpdk_tx_replayer
  --help

This is a wiring smoke for DPDKNew (InitDPDKNew, task distribute, InProcess coin).
It is not a 200 Gib/s data-plane test. Kernel Python UDP is not used: it cannot
reach a vfio-bound NIC. Real RX needs tool_dpdk_tx_replayer on another DPDK port
or another machine. See docs/app以及实验配置/DPDK采集配置与使用.md.
EOF
}

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

APP_COIN_MASTER_BIN="$(resolve_bin app_coin_master \
  "${ROOT_DIR}/bin/app/app_coin_master" \
  "${BUILD_BASIC_DIR}/bin/app/app_coin_master" \
  "${BUILD_BASIC_DIR}/app_coin_master")"
APP_ACQ_R2S_NODE_BIN="$(resolve_bin app_acq_r2s_node \
  "${ROOT_DIR}/bin/app/app_acq_r2s_node" \
  "${BUILD_CUDA_DIR}/bin/app/app_acq_r2s_node" \
  "${BUILD_CUDA_DIR}/app_acq_r2s_node")"
TX_REPLAYER_BIN="$(resolve_bin tool_dpdk_tx_replayer \
  "${ROOT_DIR}/bin/tools/tool_dpdk_tx_replayer" \
  "${BUILD_TOOLS_DIR}/bin/tools/tool_dpdk_tx_replayer" \
  "${BUILD_BASIC_DIR}/bin/tools/tool_dpdk_tx_replayer")"

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

  for ((i = 0; i < 20; ++i)); do
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
  pids="$(pgrep -f 'app_coin_master|app_acq_r2s_node|tool_dpdk_tx_replayer' || true)"
  if [[ -z "${pids}" ]]; then
    echo "[INFO] no target process to stop"
    return 0
  fi

  echo "[INFO] stopping target processes: ${pids}"
  kill -TERM ${pids} >/dev/null 2>&1 || true
  sleep 1

  pids="$(pgrep -f 'app_coin_master|app_acq_r2s_node|tool_dpdk_tx_replayer' || true)"
  if [[ -n "${pids}" ]]; then
    echo "[INFO] force killing remaining processes: ${pids}"
    kill -KILL ${pids} >/dev/null 2>&1 || true
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bind-ip)
      BIND_IP_ARG="$2"
      shift 2
      ;;
    --dst-mac)
      DST_MAC="$2"
      shift 2
      ;;
    --tx-port-id)
      TX_PORT_ID="$2"
      shift 2
      ;;
    --tx-duration-sec)
      TX_DURATION_SEC="$2"
      shift 2
      ;;
    --skip-inject)
      SKIP_INJECT=1
      shift
      ;;
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
  terminate_pid_gracefully "${TX_PID}" "tool_dpdk_tx_replayer"
  terminate_pid_gracefully "${NODE_PID}" "app_acq_r2s_node"
  terminate_pid_gracefully "${MASTER_PID}" "app_coin_master"
  stop_targets >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

stop_targets
mkdir -p "${CFG_DIR}" "${LOG_DIR}"

if ! command -v ip >/dev/null 2>&1; then
  echo "[ERROR] missing 'ip' command"
  exit 1
fi

MGMT_IFACE="$(ip -o -4 route show to default 2>/dev/null | awk '{print $5}' | head -n 1 || true)"

if [[ -n "${BIND_IP_ARG}" ]]; then
  BIND_IP="${BIND_IP_ARG}"
  NIC_IFACE="(from --bind-ip)"
  NIC_NUMA=-1
else
  mapfile -t ip_lines < <(ip -4 -o addr show up scope global | awk '!/ lo / {print $2" "$4}')
  if [[ "${#ip_lines[@]}" -eq 0 ]]; then
    echo "[ERROR] no active global IPv4 interface found; pass --bind-ip after vfio bind"
    exit 1
  fi
  NIC_IFACE="$(awk '{print $1}' <<<"${ip_lines[0]}")"
  NIC_CIDR="$(awk '{print $2}' <<<"${ip_lines[0]}")"
  BIND_IP="${NIC_CIDR%%/*}"
  NIC_NUMA=-1
  if [[ -f "/sys/class/net/${NIC_IFACE}/device/numa_node" ]]; then
    NIC_NUMA="$(cat "/sys/class/net/${NIC_IFACE}/device/numa_node" 2>/dev/null || echo -1)"
  fi
  if [[ -n "${MGMT_IFACE}" && "${NIC_IFACE}" == "${MGMT_IFACE}" ]]; then
    echo "[WARN] auto bind_ip ${BIND_IP} is on default-route iface ${MGMT_IFACE}; after vfio this is usually the management NIC. Pass --bind-ip <data-plane-ip>."
  fi
fi

mapfile -t all_cpus < <(lscpu -p=CPU 2>/dev/null | sed '/^#/d' | awk -F, '{print $1}')
if [[ "${#all_cpus[@]}" -eq 0 ]]; then
  cpu_count="$(nproc --all)"
  for ((i = 0; i < cpu_count; ++i)); do all_cpus+=("$i"); done
fi

selected_cpus=()
if [[ "${NIC_NUMA}" =~ ^[0-9]+$ ]]; then
  mapfile -t numa_cpus < <(lscpu -p=CPU,NODE 2>/dev/null | sed '/^#/d' | awk -F, -v n="${NIC_NUMA}" '$2==n {print $1}')
  if [[ "${#numa_cpus[@]}" -gt 0 ]]; then
    all_cpus=("${numa_cpus[@]}")
  fi
fi

# DPDKNew needs main lcore + 1 RX + 1 Copy per queue. Smoke uses 1 queue → ≥3 cores.
limit=4
if [[ "${#all_cpus[@]}" -lt 4 ]]; then
  limit="${#all_cpus[@]}"
fi
if [[ "${limit}" -lt 3 ]]; then
  echo "[WARN] fewer than 3 CPUs selected; DPDKNew 1-queue needs main+RX+Copy"
fi
for ((i = 0; i < limit; ++i)); do
  selected_cpus+=("${all_cpus[$i]}")
done
if [[ "${#selected_cpus[@]}" -eq 0 ]]; then
  selected_cpus=(0)
fi

CPU_JSON=""
EAL_LCORE=""
for c in "${selected_cpus[@]}"; do
  if [[ -n "${CPU_JSON}" ]]; then
    CPU_JSON+=", "
    EAL_LCORE+=","
  fi
  CPU_JSON+="${c}"
  EAL_LCORE+="${c}"
done

STRICT_NUMA=false
REQ_CPU_NUMA=false
EXPECTED_NUMA=-1
if [[ "${NIC_NUMA}" =~ ^[0-9]+$ ]]; then
  STRICT_NUMA=true
  REQ_CPU_NUMA=true
  EXPECTED_NUMA="${NIC_NUMA}"
fi

# vfio-bound NICs drop kernel IPv4; ownership check would fail InitDPDKNew.
STRICT_BIND_IPS=false

cat > "${MASTER_CFG}" <<EOF
{
  "dataplane": {
    "requireRoce": false,
    "forceInProcess": true
  },
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
    "timeSwitchBufferMs": 50,
    "reservedStorageGiB": 20,
    "maxFileSizeMb": 256,
    "dpdkRxRingsPerPort": 1,
    "dpdkMbufPoolSize": 65535,
    "dpdkMbufCacheSize": 250,
    "dpdkExtraEalArgs": ["-l", "${EAL_LCORE}", "--file-prefix", "acq_r2s_nodata"],
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
  "dataplane": {
    "requireRoce": false,
    "forceInProcess": true
  },
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
    "leaseQueueCapacity": 2,
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
    "strictBindIpsOwnershipCheck": ${STRICT_BIND_IPS},
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
echo "[INFO] bind_ip=${BIND_IP} iface=${NIC_IFACE} iface_numa=${NIC_NUMA} cpus=[${CPU_JSON}] eal=-l ${EAL_LCORE}"
echo "[INFO] smoke: InProcess coin, mbufPool=65535, timeSwitch=50ms, rings=1, bind-ip ownership check off"

if [[ "${SKIP_PRECHECK}" -eq 0 && -x "${SCRIPT_DIR}/dpdk_precheck.sh" ]]; then
  echo "[INFO] running dpdk precheck..."
  "${SCRIPT_DIR}/dpdk_precheck.sh" || true
fi

if [[ "${SKIP_BUILD}" -eq 0 ]]; then
  if [[ ! -x "${APP_COIN_MASTER_BIN}" ]]; then
    echo "[INFO] building app_coin_master..."
    cmake --build "${BUILD_BASIC_DIR}" --target app_coin_master
    APP_COIN_MASTER_BIN="$(resolve_bin app_coin_master \
      "${ROOT_DIR}/bin/app/app_coin_master" \
      "${BUILD_BASIC_DIR}/bin/app/app_coin_master" \
      "${BUILD_BASIC_DIR}/app_coin_master")"
  fi
  if [[ ! -x "${APP_ACQ_R2S_NODE_BIN}" ]]; then
    echo "[INFO] building app_acq_r2s_node..."
    cmake --build "${BUILD_CUDA_DIR}" --target app_acq_r2s_node
    APP_ACQ_R2S_NODE_BIN="$(resolve_bin app_acq_r2s_node \
      "${ROOT_DIR}/bin/app/app_acq_r2s_node" \
      "${BUILD_CUDA_DIR}/bin/app/app_acq_r2s_node" \
      "${BUILD_CUDA_DIR}/app_acq_r2s_node")"
  fi
  if [[ -n "${DST_MAC}" && ! -x "${TX_REPLAYER_BIN}" ]]; then
    echo "[INFO] building tool_dpdk_tx_replayer..."
    cmake --build "${BUILD_TOOLS_DIR}" --target tool_dpdk_tx_replayer
    TX_REPLAYER_BIN="$(resolve_bin tool_dpdk_tx_replayer \
      "${ROOT_DIR}/bin/tools/tool_dpdk_tx_replayer" \
      "${BUILD_TOOLS_DIR}/bin/tools/tool_dpdk_tx_replayer" \
      "${BUILD_BASIC_DIR}/bin/tools/tool_dpdk_tx_replayer")"
  fi
fi

if [[ ! -x "${APP_COIN_MASTER_BIN}" ]]; then
  echo "[ERROR] missing binary: ${APP_COIN_MASTER_BIN}"
  echo "[HINT] cmake --preset linux-release-apps-basic && cmake --build --preset build-apps-basic --target app_coin_master"
  exit 1
fi
if [[ ! -x "${APP_ACQ_R2S_NODE_BIN}" ]]; then
  echo "[ERROR] missing binary: ${APP_ACQ_R2S_NODE_BIN}"
  echo "[HINT] cmake --preset linux-release-apps-cuda && cmake --build --preset build-apps-cuda --target app_acq_r2s_node"
  exit 1
fi

: > "${MASTER_LOG}"
: > "${NODE_LOG}"
: > "${TX_LOG}"

echo "[INFO] starting coin master: ${APP_COIN_MASTER_BIN}"
(
  cd "${ROOT_DIR}"
  GLOG_logtostderr=1 "${APP_COIN_MASTER_BIN}" --config "${MASTER_CFG}" >>"${MASTER_LOG}" 2>&1
) &
MASTER_PID=$!

sleep 1
echo "[INFO] starting acq_r2s node: ${APP_ACQ_R2S_NODE_BIN}"
(
  cd "${ROOT_DIR}"
  GLOG_logtostderr=1 "${APP_ACQ_R2S_NODE_BIN}" --config "${NODE_CFG}" >>"${NODE_LOG}" 2>&1
) &
NODE_PID=$!

echo "[INFO] waiting ${DURATION_SEC}s for configuration/distribution..."
sleep "${DURATION_SEC}"

INJECTED=0
if [[ "${SKIP_INJECT}" -eq 1 ]]; then
  echo "[INFO] skip packet inject (--skip-inject)"
elif [[ -n "${DST_MAC}" ]]; then
  if [[ ! -x "${TX_REPLAYER_BIN}" ]]; then
    echo "[WARN] --dst-mac set but tool_dpdk_tx_replayer missing; skip inject"
  else
    echo "[INFO] injecting with ${TX_REPLAYER_BIN} dst-mac=${DST_MAC} dest=${BIND_IP}:18100 port-id=${TX_PORT_ID}"
    echo "[WARN] TX and RX cannot share one vfio NIC; use a second port or another machine"
    (
      cd "${ROOT_DIR}"
      "${TX_REPLAYER_BIN}" \
        --dst-mac "${DST_MAC}" \
        --port-id "${TX_PORT_ID}" \
        --source-ip "${BIND_IP}" \
        --destination-ip "${BIND_IP}" \
        --source-port-base 17100 \
        --destination-port-base 18100 \
        --channel-count 1 \
        --payload-size 512 \
        --pps 50000 \
        --duration-sec "${TX_DURATION_SEC}" \
        --eal-args "--file-prefix=dpdk_tx_replayer_nodata" \
        >>"${TX_LOG}" 2>&1
    ) &
    TX_PID=$!
    INJECTED=1
    wait "${TX_PID}" || true
    TX_PID=""
  fi
else
  echo "[INFO] no --dst-mac: skip inject. Kernel UDP cannot reach vfio. For RX proof use tool_dpdk_tx_replayer on another port/host."
fi

if [[ "${INJECTED}" -eq 1 ]]; then
  sleep 2
fi

PASS=0
WARN=0
FAIL=0

if grep -q "AcquisitionMaster started" "${MASTER_LOG}"; then
  echo "[PASS] coin master started AcquisitionMaster"
  PASS=$((PASS + 1))
else
  echo "[FAIL] AcquisitionMaster did not start (need acquisitionControl.enabled=true on app_coin_master)"
  FAIL=$((FAIL + 1))
fi

if grep -q "acqControl.algorithm     : dpdk" "${MASTER_LOG}"; then
  echo "[PASS] master config uses DPDK"
  PASS=$((PASS + 1))
else
  echo "[WARN] did not find explicit DPDK config print in master log"
  WARN=$((WARN + 1))
fi

if grep -Eq "connected=1/1|all nodes registered|Start signal issued" "${MASTER_LOG}"; then
  echo "[PASS] coin master and node connected / start signal observed"
  PASS=$((PASS + 1))
else
  echo "[WARN] no coin-node connection/start signal marker found"
  WARN=$((WARN + 1))
fi

if grep -Eiq "failed to start CoincidenceClient|requireRoce but" "${NODE_LOG}"; then
  echo "[FAIL] CoincidenceClient/RoCE handshake failed (smoke expects InProcess)"
  FAIL=$((FAIL + 1))
fi

if grep -Eiq "DPDK initialization failed|STATE_ERROR|bind_ips not found on local NICs|runtime requested but this build does not enable DPDK|does not enable PNI_STANDARD_CONFIG_ENABLE_DPDK" "${NODE_LOG}"; then
  echo "[FAIL] node log contains DPDK/runtime errors"
  FAIL=$((FAIL + 1))
else
  echo "[PASS] no obvious DPDK/runtime fatal errors in node log"
  PASS=$((PASS + 1))
fi

if grep -q "DPDKNew initialized" "${NODE_LOG}"; then
  echo "[PASS] DPDKNew initialized"
  PASS=$((PASS + 1))
else
  echo "[WARN] no 'DPDKNew initialized' marker; InitDPDKNew may not have run"
  WARN=$((WARN + 1))
fi

if grep -q "CPU affinity enabled" "${NODE_LOG}"; then
  echo "[PASS] CPU affinity applied"
  PASS=$((PASS + 1))
else
  echo "[WARN] CPU affinity marker not found in node log"
  WARN=$((WARN + 1))
fi

if grep -q "algorithm=DPDKNew" "${NODE_LOG}"; then
  echo "[PASS] node received DPDKNew configure task"
  PASS=$((PASS + 1))
else
  echo "[WARN] no explicit 'algorithm=DPDKNew' marker found; check full log"
  WARN=$((WARN + 1))
fi

if [[ "${INJECTED}" -eq 1 ]]; then
  if grep -Eiq "udp_injection_done|pps|Tx packets|sent=" "${TX_LOG}"; then
    echo "[PASS] tx replayer produced send stats (does not prove RX)"
    PASS=$((PASS + 1))
  else
    echo "[WARN] tx replayer log has no send stats; see ${TX_LOG}"
    WARN=$((WARN + 1))
  fi
fi

echo "[INFO] summary: PASS=${PASS} WARN=${WARN} FAIL=${FAIL}"
echo "[INFO] logs:"
echo "  ${MASTER_LOG}"
echo "  ${NODE_LOG}"
echo "  ${TX_LOG}"
echo "[INFO] this smoke does not measure 200 Gib/s; OS bind/hugepage: dpdk_apply.sh, dual-machine RX: tool_dpdk_tx_replayer"

if [[ "${FAIL}" -gt 0 ]]; then
  exit 2
fi
