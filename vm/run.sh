#!/usr/bin/env bash
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
storage="${VM_STORAGE:-$here}"
port="${VM_SSH_PORT:-2222}"
exec qemu-system-aarch64 \
  -machine virt -cpu cortex-a72 -smp 4 -m 2048 -nographic \
  -drive if=pflash,format=raw,readonly=on,file=/usr/share/AAVMF/AAVMF_CODE.fd \
  -drive if=pflash,format=raw,file="$storage/vars.fd" \
  -drive if=virtio,format=qcow2,file="$storage/device.qcow2" \
  -drive if=virtio,format=raw,file="$storage/seed.img" \
  -netdev user,id=net0,hostfwd=tcp:127.0.0.1:"$port"-:22 \
  -device virtio-net-pci,netdev=net0
