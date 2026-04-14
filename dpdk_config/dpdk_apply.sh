#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_DIR="${SCRIPT_DIR}/state"

PCI_ADDR=""
TARGET_DRIVER="vfio-pci"
HUGEPAGES_COUNT=1024
HUGEPAGES_SIZE="2M"
HUGEPAGES_MOUNT="/dev/hugepages"
CHOWN_USER="${SUDO_USER:-$USER}"
ASSUME_YES=0
DRY_RUN=0
ENABLE_UNSAFE_NOIOMMU=0

usage() {
  cat <<'EOF'
Usage:
  dpdk_apply.sh --pci <domain:bus:slot.func> [options]

Required:
  --pci <addr>                     PCI address to bind, e.g. 0000:04:00.0

Options:
  --driver <name>                  target DPDK driver (default: vfio-pci)
  --hugepages-count <n>            hugepages count (default: 1024)
  --hugepages-size <2M|1G>         hugepage size (default: 2M)
  --hugepages-mount <path>         hugetlbfs mountpoint (default: /dev/hugepages)
  --chown-user <user>              owner for /dev/vfio and hugetlbfs mount (default: current user)
  --enable-unsafe-noiommu          load vfio with unsafe noiommu mode
  --yes                            do not ask for confirmation
  --dry-run                        print actions only
  --help                           print this help

Notes:
  1. This script assumes DPDK is already installed.
  2. Binding a NIC can break current network connectivity.
EOF
}

log() {
  echo "[INFO] $*"
}

run_cmd() {
  if [[ "${DRY_RUN}" -eq 1 ]]; then
    echo "[DRY-RUN] $*"
  else
    eval "$*"
  fi
}

need_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "[ERROR] missing command: $cmd"
    exit 1
  fi
}

as_root() {
  if [[ "${EUID}" -eq 0 ]]; then
    run_cmd "$*"
  else
    run_cmd "sudo $*"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pci)
      PCI_ADDR="$2"
      shift 2
      ;;
    --driver)
      TARGET_DRIVER="$2"
      shift 2
      ;;
    --hugepages-count)
      HUGEPAGES_COUNT="$2"
      shift 2
      ;;
    --hugepages-size)
      HUGEPAGES_SIZE="$2"
      shift 2
      ;;
    --hugepages-mount)
      HUGEPAGES_MOUNT="$2"
      shift 2
      ;;
    --chown-user)
      CHOWN_USER="$2"
      shift 2
      ;;
    --enable-unsafe-noiommu)
      ENABLE_UNSAFE_NOIOMMU=1
      shift
      ;;
    --yes)
      ASSUME_YES=1
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] unknown argument: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ -z "${PCI_ADDR}" ]]; then
  echo "[ERROR] --pci is required"
  usage
  exit 1
fi

if [[ "${HUGEPAGES_SIZE}" != "2M" && "${HUGEPAGES_SIZE}" != "1G" ]]; then
  echo "[ERROR] --hugepages-size must be 2M or 1G"
  exit 1
fi

need_cmd dpdk-devbind.py

if [[ "${ASSUME_YES}" -ne 1 ]]; then
  echo "About to apply DPDK config with:"
  echo "  PCI_ADDR=${PCI_ADDR}"
  echo "  TARGET_DRIVER=${TARGET_DRIVER}"
  echo "  HUGEPAGES_COUNT=${HUGEPAGES_COUNT}"
  echo "  HUGEPAGES_SIZE=${HUGEPAGES_SIZE}"
  echo "  HUGEPAGES_MOUNT=${HUGEPAGES_MOUNT}"
  echo "  CHOWN_USER=${CHOWN_USER}"
  echo
  read -r -p "Continue? [y/N]: " answer
  if [[ "${answer}" != "y" && "${answer}" != "Y" ]]; then
    echo "Canceled."
    exit 1
  fi
fi

mkdir -p "${STATE_DIR}"

if [[ ! -e "/sys/bus/pci/devices/${PCI_ADDR}" ]]; then
  echo "[ERROR] PCI device not found: ${PCI_ADDR}"
  exit 1
fi

ORIG_DRIVER="none"
if [[ -L "/sys/bus/pci/devices/${PCI_ADDR}/driver" ]]; then
  ORIG_DRIVER="$(basename "$(readlink "/sys/bus/pci/devices/${PCI_ADDR}/driver")")"
fi

ORIG_IFACE="none"
if [[ -d "/sys/bus/pci/devices/${PCI_ADDR}/net" ]]; then
  ORIG_IFACE="$(ls "/sys/bus/pci/devices/${PCI_ADDR}/net" | head -n 1 || true)"
  ORIG_IFACE="${ORIG_IFACE:-none}"
fi

STATE_FILE="${STATE_DIR}/$(date +%Y%m%d_%H%M%S)_${PCI_ADDR//[:.]/_}.env"
cat > "${STATE_FILE}" <<EOF
PCI_ADDR=${PCI_ADDR}
ORIG_DRIVER=${ORIG_DRIVER}
ORIG_IFACE=${ORIG_IFACE}
TARGET_DRIVER=${TARGET_DRIVER}
HUGEPAGES_COUNT=${HUGEPAGES_COUNT}
HUGEPAGES_SIZE=${HUGEPAGES_SIZE}
HUGEPAGES_MOUNT=${HUGEPAGES_MOUNT}
APPLIED_AT=$(date -Iseconds)
EOF
log "Saved state: ${STATE_FILE}"

if [[ "${TARGET_DRIVER}" == "vfio-pci" ]]; then
  as_root "modprobe vfio"
  as_root "modprobe vfio-pci"
  if [[ "${ENABLE_UNSAFE_NOIOMMU}" -eq 1 ]]; then
    as_root "modprobe vfio enable_unsafe_noiommu_mode=1"
  fi
fi

HUGEPAGE_SYSFS="/sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages"
if [[ "${HUGEPAGES_SIZE}" == "1G" ]]; then
  HUGEPAGE_SYSFS="/sys/kernel/mm/hugepages/hugepages-1048576kB/nr_hugepages"
fi

if [[ -e "${HUGEPAGE_SYSFS}" ]]; then
  as_root "sh -c 'echo ${HUGEPAGES_COUNT} > ${HUGEPAGE_SYSFS}'"
else
  echo "[WARN] hugepage sysfs not found for size ${HUGEPAGES_SIZE}: ${HUGEPAGE_SYSFS}"
fi

as_root "mkdir -p ${HUGEPAGES_MOUNT}"
if ! mount | grep -q "on ${HUGEPAGES_MOUNT} type hugetlbfs"; then
  if [[ "${HUGEPAGES_SIZE}" == "1G" ]]; then
    as_root "mount -t hugetlbfs -o pagesize=1G none ${HUGEPAGES_MOUNT}"
  else
    as_root "mount -t hugetlbfs none ${HUGEPAGES_MOUNT}"
  fi
fi

as_root "dpdk-devbind.py --bind=${TARGET_DRIVER} ${PCI_ADDR}"

if [[ -d /dev/vfio ]]; then
  as_root "chown -R ${CHOWN_USER}:${CHOWN_USER} /dev/vfio || true"
fi
as_root "chown -R ${CHOWN_USER}:${CHOWN_USER} ${HUGEPAGES_MOUNT} || true"

log "Final status"
dpdk-devbind.py --status || true
if command -v dpdk-hugepages.py >/dev/null 2>&1; then
  dpdk-hugepages.py -s || true
fi

log "DPDK apply completed. Use dpdk_rollback.sh to revert."
