#!/usr/bin/env bash
set -euo pipefail

PASS_COUNT=0
WARN_COUNT=0
FAIL_COUNT=0

pass() {
  echo "[PASS] $*"
  PASS_COUNT=$((PASS_COUNT + 1))
}

warn() {
  echo "[WARN] $*"
  WARN_COUNT=$((WARN_COUNT + 1))
}

fail() {
  echo "[FAIL] $*"
  FAIL_COUNT=$((FAIL_COUNT + 1))
}

section() {
  echo
  echo "==== $* ===="
}

section "System"
uname -a || true
if [[ -f /etc/os-release ]]; then
  sed -n '1,8p' /etc/os-release || true
fi

section "DPDK tools"
for cmd in dpdk-testpmd dpdk-devbind.py dpdk-hugepages.py pkg-config; do
  if command -v "$cmd" >/dev/null 2>&1; then
    pass "$cmd found at $(command -v "$cmd")"
  else
    fail "$cmd not found in PATH"
  fi
done

if pkg-config --modversion libdpdk >/dev/null 2>&1; then
  pass "libdpdk version: $(pkg-config --modversion libdpdk)"
else
  warn "pkg-config cannot resolve libdpdk"
fi

section "EAL smoke check"
if command -v dpdk-testpmd >/dev/null 2>&1; then
  TESTPMD_LOG="/tmp/dpdk_precheck_testpmd.log"
  TESTPMD_RC=0
  TESTPMD_PREFIX="dpdk_precheck_$$"
  if command -v timeout >/dev/null 2>&1; then
    timeout -k 2s 8s dpdk-testpmd --file-prefix="${TESTPMD_PREFIX}" --no-pci --no-huge -- --total-num-mbufs=2048 --nb-cores=1 >"${TESTPMD_LOG}" 2>&1 || TESTPMD_RC=$?
  else
    dpdk-testpmd --file-prefix="${TESTPMD_PREFIX}" --no-pci --no-huge -- --total-num-mbufs=2048 --nb-cores=1 >"${TESTPMD_LOG}" 2>&1 || TESTPMD_RC=$?
  fi

  if [[ "${TESTPMD_RC}" -eq 0 ]]; then
    pass "dpdk-testpmd smoke run succeeded"
  elif [[ "${TESTPMD_RC}" -eq 124 ]]; then
    if grep -q "EAL: Detected" "${TESTPMD_LOG}"; then
      pass "EAL initialized (testpmd timed out by design in precheck)"
    else
      warn "testpmd timed out without clear EAL init logs, check ${TESTPMD_LOG}"
    fi
  elif grep -q "No probed ethernet devices" "${TESTPMD_LOG}"; then
    pass "EAL initialized (no DPDK NIC probed yet)"
  else
    warn "dpdk-testpmd smoke run returned rc=${TESTPMD_RC}, check ${TESTPMD_LOG}"
  fi
fi

section "HugePages"
grep -E 'HugePages_Total|HugePages_Free|Hugepagesize' /proc/meminfo || true
HP_TOTAL=$(awk '/HugePages_Total/ {print $2}' /proc/meminfo)
if [[ "${HP_TOTAL:-0}" -gt 0 ]]; then
  pass "HugePages_Total=${HP_TOTAL}"
else
  warn "HugePages_Total is 0"
fi

if command -v dpdk-hugepages.py >/dev/null 2>&1; then
  dpdk-hugepages.py -s || true
fi

if mount | grep -q hugetlbfs; then
  pass "hugetlbfs mount exists"
else
  warn "hugetlbfs mount not found"
fi

section "VFIO / IOMMU"
if [[ -e /dev/vfio/vfio ]]; then
  pass "/dev/vfio/vfio exists"
else
  warn "/dev/vfio/vfio missing"
fi

if lsmod | grep -Eq '^vfio|^vfio_pci'; then
  pass "vfio module loaded"
else
  warn "vfio module is not loaded"
fi

if [[ -d /sys/kernel/iommu_groups ]]; then
  IOMMU_GROUPS=$(find /sys/kernel/iommu_groups -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')
  if [[ "${IOMMU_GROUPS}" -gt 0 ]]; then
    pass "iommu_groups detected: ${IOMMU_GROUPS}"
  else
    warn "iommu_groups directory exists but no groups found"
  fi
else
  warn "iommu_groups directory not found"
fi

echo "Kernel cmdline: $(cat /proc/cmdline)"

section "NIC status"
if command -v lspci >/dev/null 2>&1; then
  lspci -nn | grep -Ei 'Ethernet|Network' || true
else
  warn "lspci not found"
fi

if command -v dpdk-devbind.py >/dev/null 2>&1; then
  dpdk-devbind.py --status || true
fi

section "Summary"
echo "PASS=${PASS_COUNT} WARN=${WARN_COUNT} FAIL=${FAIL_COUNT}"
if [[ "${FAIL_COUNT}" -gt 0 ]]; then
  exit 1
fi
