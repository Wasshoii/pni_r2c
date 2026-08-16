#!/usr/bin/env bash
# RoCE / verbs preflight for RDMA cluster experiments.
# Does not start apps. Exit 0 if the host looks ready.
set -euo pipefail

fail() {
  echo "[rdma_preflight] ERROR: $*" >&2
  exit 1
}

echo "[rdma_preflight] checking verbs devices"
if ! command -v ibv_devinfo >/dev/null 2>&1; then
  fail "ibv_devinfo not found (install libibverbs / rdma-core tools)"
fi

if ! ibv_devinfo >/tmp/rdma_preflight_ibv.txt 2>/tmp/rdma_preflight_ibv.err; then
  fail "ibv_devinfo failed: $(tr '\n' ' ' </tmp/rdma_preflight_ibv.err)"
fi

if ! grep -q "hca_id:" /tmp/rdma_preflight_ibv.txt; then
  fail "no RNIC reported by ibv_devinfo"
fi

echo "[rdma_preflight] devices:"
grep -E 'hca_id:|transport:|port:|state:|gid' /tmp/rdma_preflight_ibv.txt | head -n 80

if ! grep -Eq 'RoCE|InfiniBand' /tmp/rdma_preflight_ibv.txt; then
  echo "[rdma_preflight] WARN: transport line did not mention RoCE/InfiniBand; inspect ibv_devinfo"
fi

echo "[rdma_preflight] hugepages"
if [[ -r /proc/meminfo ]]; then
  grep -E 'HugePages_|Hugepagesize' /proc/meminfo || true
fi
hp=$(awk '/HugePages_Total/ {print $2}' /proc/meminfo 2>/dev/null || echo 0)
if [[ "${hp:-0}" -lt 1 ]]; then
  echo "[rdma_preflight] WARN: HugePages_Total=0 (RDMA MR may still work; some setups need hugepages)"
fi

if command -v ibv_devices >/dev/null 2>&1; then
  echo "[rdma_preflight] ibv_devices:"
  ibv_devices || true
fi

echo "[rdma_preflight] OK"
exit 0
