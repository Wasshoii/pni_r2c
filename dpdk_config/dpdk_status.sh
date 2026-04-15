#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_DIR="${SCRIPT_DIR}/state"

say() {
  echo "[INFO] $*"
}

section() {
  echo
  echo "==== $* ===="
}

show_cmd() {
  local cmd="$1"
  if command -v "$cmd" >/dev/null 2>&1; then
    command -v "$cmd"
  else
    echo "missing"
  fi
}

latest_state_file() {
  ls -1t "${STATE_DIR}"/*.env 2>/dev/null | head -n 1 || true
}

section "Toolchain"
echo "dpdk-devbind.py: $(show_cmd dpdk-devbind.py)"
echo "dpdk-hugepages.py: $(show_cmd dpdk-hugepages.py)"
echo "dpdk-testpmd: $(show_cmd dpdk-testpmd)"

section "NIC Binding"
if command -v dpdk-devbind.py >/dev/null 2>&1; then
  dpdk-devbind.py --status || true
else
  echo "dpdk-devbind.py not found"
fi

section "HugePages"
grep -E 'HugePages_Total|HugePages_Free|Hugepagesize' /proc/meminfo || true
if command -v dpdk-hugepages.py >/dev/null 2>&1; then
  echo "---"
  dpdk-hugepages.py -s || true
fi

section "VFIO"
lsmod | grep -E '^vfio|^vfio_pci' || true
if [[ -e /dev/vfio/vfio ]]; then
  echo "/dev/vfio/vfio: exists"
else
  echo "/dev/vfio/vfio: missing"
fi
if [[ -r /sys/module/vfio/parameters/enable_unsafe_noiommu_mode ]]; then
  echo -n "enable_unsafe_noiommu_mode: "
  cat /sys/module/vfio/parameters/enable_unsafe_noiommu_mode
fi

section "State Snapshot"
LATEST_STATE="$(latest_state_file)"
if [[ -n "${LATEST_STATE}" ]]; then
  say "latest state file: ${LATEST_STATE}"
  sed -n '1,120p' "${LATEST_STATE}"
else
  echo "no state file found under ${STATE_DIR}"
fi
