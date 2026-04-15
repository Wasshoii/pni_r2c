#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_DIR="${SCRIPT_DIR}/state"

PCI_ADDRS=()
TARGET_DRIVER="vfio-pci"
HUGEPAGES_COUNT=1024
HUGEPAGES_SIZE="2M"
HUGEPAGES_MOUNT="/dev/hugepages"
CHOWN_USER="${SUDO_USER:-$USER}"
ASSUME_YES=0
DRY_RUN=0
ENABLE_UNSAFE_NOIOMMU=0
FORCE_MANAGEMENT_NIC=0
PARTIAL_ROLLBACK_DONE=0
PARTIAL_APPLIED_PCIS=()

declare -A ORIG_DRIVER_MAP
declare -A ORIG_IFACE_MAP

usage() {
  cat <<'EOF'
Usage:
  dpdk_apply.sh --pci <domain:bus:slot.func> [--pci <domain:bus:slot.func> ...] [options]

Required:
  --pci <addr>                     PCI address to bind, e.g. 0000:04:00.0 (repeatable)

Options:
  --driver <name>                  target DPDK driver (default: vfio-pci)
  --hugepages-count <n>            hugepages count (default: 1024)
  --hugepages-size <2M|1G>         hugepage size (default: 2M)
  --hugepages-mount <path>         hugetlbfs mountpoint (default: /dev/hugepages)
  --chown-user <user>              owner for /dev/vfio and hugetlbfs mount (default: current user)
  --enable-unsafe-noiommu          load vfio with unsafe noiommu mode
  --force-management-nic           allow binding NIC that carries default route
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
  if [[ "$#" -eq 0 ]]; then
    return 0
  fi
  if [[ "${DRY_RUN}" -eq 1 ]]; then
    printf "[DRY-RUN]"
    for arg in "$@"; do
      printf " %q" "${arg}"
    done
    printf "\n"
    return 0
  fi
  "$@"
}

need_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "[ERROR] missing command: $cmd"
    exit 1
  fi
}

as_root() {
  if [[ "$#" -eq 0 ]]; then
    return 0
  fi
  if [[ "${EUID}" -eq 0 ]]; then
    run_cmd "$@"
  else
    run_cmd sudo "$@"
  fi
}

get_default_route_iface() {
  ip -o -4 route show to default 2>/dev/null | awk '{print $5}' | head -n 1
}

rollback_partial() {
  if [[ "${PARTIAL_ROLLBACK_DONE}" -eq 1 ]]; then
    return
  fi
  PARTIAL_ROLLBACK_DONE=1

  if [[ "${#PARTIAL_APPLIED_PCIS[@]}" -eq 0 ]]; then
    return
  fi

  echo "[WARN] apply failed, starting partial rollback for already-bound NICs..."
  for pci in "${PARTIAL_APPLIED_PCIS[@]}"; do
    local orig_driver="${ORIG_DRIVER_MAP[${pci}]:-none}"
    if [[ -n "${orig_driver}" && "${orig_driver}" != "none" ]]; then
      if as_root dpdk-devbind.py "--bind=${orig_driver}" "${pci}"; then
        echo "[INFO] rolled back ${pci} -> ${orig_driver}"
      else
        echo "[WARN] failed rollback for ${pci} -> ${orig_driver}"
      fi
    else
      echo "[WARN] skip rollback for ${pci}: original driver unknown"
    fi
  done
}

on_exit() {
  local rc=$?
  if [[ "${rc}" -ne 0 ]]; then
    rollback_partial
  fi
  return "${rc}"
}

trap on_exit EXIT

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pci)
      PCI_ADDRS+=("$2")
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
    --force-management-nic)
      FORCE_MANAGEMENT_NIC=1
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

if [[ "${#PCI_ADDRS[@]}" -eq 0 ]]; then
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
  echo "  PCI_ADDRS=${PCI_ADDRS[*]}"
  echo "  TARGET_DRIVER=${TARGET_DRIVER}"
  echo "  HUGEPAGES_COUNT=${HUGEPAGES_COUNT}"
  echo "  HUGEPAGES_SIZE=${HUGEPAGES_SIZE}"
  echo "  HUGEPAGES_MOUNT=${HUGEPAGES_MOUNT}"
  echo "  CHOWN_USER=${CHOWN_USER}"
  echo "  FORCE_MANAGEMENT_NIC=${FORCE_MANAGEMENT_NIC}"
  echo
  read -r -p "Continue? [y/N]: " answer
  if [[ "${answer}" != "y" && "${answer}" != "Y" ]]; then
    echo "Canceled."
    exit 1
  fi
fi

mkdir -p "${STATE_DIR}"

HP_2M_BEFORE="$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages 2>/dev/null || echo unknown)"
HP_1G_BEFORE="$(cat /sys/kernel/mm/hugepages/hugepages-1048576kB/nr_hugepages 2>/dev/null || echo unknown)"
if mount | grep -q "on ${HUGEPAGES_MOUNT} type hugetlbfs"; then
  HUGEPAGE_MOUNT_EXISTED=1
else
  HUGEPAGE_MOUNT_EXISTED=0
fi
VFIO_NOIOMMU_BEFORE="unknown"
if [[ -r /sys/module/vfio/parameters/enable_unsafe_noiommu_mode ]]; then
  VFIO_NOIOMMU_BEFORE="$(cat /sys/module/vfio/parameters/enable_unsafe_noiommu_mode 2>/dev/null || echo unknown)"
fi

MGMT_IFACE="$(get_default_route_iface || true)"

for PCI_ADDR in "${PCI_ADDRS[@]}"; do
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

  if [[ -n "${MGMT_IFACE}" && "${ORIG_IFACE}" == "${MGMT_IFACE}" && "${FORCE_MANAGEMENT_NIC}" -ne 1 ]]; then
    echo "[ERROR] ${PCI_ADDR} maps to default-route iface ${MGMT_IFACE}; refusing bind. Use --force-management-nic to override."
    exit 1
  fi

  ORIG_DRIVER_MAP["${PCI_ADDR}"]="${ORIG_DRIVER}"
  ORIG_IFACE_MAP["${PCI_ADDR}"]="${ORIG_IFACE}"

  STATE_FILE="${STATE_DIR}/$(date +%Y%m%d_%H%M%S)_${PCI_ADDR//[:.]/_}.env"
  cat > "${STATE_FILE}" <<EOF
PCI_ADDR=${PCI_ADDR}
ORIG_DRIVER=${ORIG_DRIVER}
ORIG_IFACE=${ORIG_IFACE}
TARGET_DRIVER=${TARGET_DRIVER}
HUGEPAGES_COUNT=${HUGEPAGES_COUNT}
HUGEPAGES_SIZE=${HUGEPAGES_SIZE}
HUGEPAGES_MOUNT=${HUGEPAGES_MOUNT}
HP_2M_BEFORE=${HP_2M_BEFORE}
HP_1G_BEFORE=${HP_1G_BEFORE}
HUGEPAGE_MOUNT_EXISTED=${HUGEPAGE_MOUNT_EXISTED}
VFIO_NOIOMMU_BEFORE=${VFIO_NOIOMMU_BEFORE}
MGMT_IFACE_AT_APPLY=${MGMT_IFACE:-none}
APPLIED_AT=$(date -Iseconds)
EOF
  log "Saved state: ${STATE_FILE}"
done

if [[ "${TARGET_DRIVER}" == "vfio-pci" ]]; then
  as_root modprobe vfio
  as_root modprobe vfio-pci
  if [[ "${ENABLE_UNSAFE_NOIOMMU}" -eq 1 ]]; then
    as_root modprobe vfio enable_unsafe_noiommu_mode=1
  fi
fi

HUGEPAGE_SYSFS="/sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages"
if [[ "${HUGEPAGES_SIZE}" == "1G" ]]; then
  HUGEPAGE_SYSFS="/sys/kernel/mm/hugepages/hugepages-1048576kB/nr_hugepages"
fi

if [[ -e "${HUGEPAGE_SYSFS}" ]]; then
  as_root sh -c "echo ${HUGEPAGES_COUNT} > ${HUGEPAGE_SYSFS}"
else
  echo "[WARN] hugepage sysfs not found for size ${HUGEPAGES_SIZE}: ${HUGEPAGE_SYSFS}"
fi

as_root mkdir -p "${HUGEPAGES_MOUNT}"
if ! mount | grep -q "on ${HUGEPAGES_MOUNT} type hugetlbfs"; then
  if [[ "${HUGEPAGES_SIZE}" == "1G" ]]; then
    as_root mount -t hugetlbfs -o pagesize=1G none "${HUGEPAGES_MOUNT}"
  else
    as_root mount -t hugetlbfs none "${HUGEPAGES_MOUNT}"
  fi
fi

for PCI_ADDR in "${PCI_ADDRS[@]}"; do
  as_root dpdk-devbind.py "--bind=${TARGET_DRIVER}" "${PCI_ADDR}"
  PARTIAL_APPLIED_PCIS+=("${PCI_ADDR}")
done

if [[ -d /dev/vfio ]]; then
  as_root chown -R "${CHOWN_USER}:${CHOWN_USER}" /dev/vfio || true
fi
as_root chown -R "${CHOWN_USER}:${CHOWN_USER}" "${HUGEPAGES_MOUNT}" || true

log "Final status"
dpdk-devbind.py --status || true
if command -v dpdk-hugepages.py >/dev/null 2>&1; then
  dpdk-hugepages.py -s || true
fi

PARTIAL_ROLLBACK_DONE=1
log "DPDK apply completed. Use dpdk_rollback.sh to revert."
