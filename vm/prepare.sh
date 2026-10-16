#!/usr/bin/env bash
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
storage="${VM_STORAGE:-$here}"
mkdir -p "$storage"
base="$storage/debian-13-genericcloud-arm64.qcow2"
url="https://cloud.debian.org/images/cloud/trixie/latest"

if [[ ! -f "$base" ]]; then
  curl -L --fail --retry 3 -o "$base" "$url/$(basename "$base")"
fi
if [[ ! -f "$storage/SHA512SUMS" ]]; then
  curl -L --fail -o "$storage/SHA512SUMS" "$url/SHA512SUMS"
fi
(cd "$storage" && grep "debian-13-genericcloud-arm64.qcow2" SHA512SUMS | sha512sum -c -)

if [[ ! -f "$storage/id_ed25519" ]]; then
  ssh-keygen -q -t ed25519 -N '' -f "$storage/id_ed25519"
fi
pub="$(cat "$storage/id_ed25519.pub")"
cat > "$storage/user-data" <<EOF
#cloud-config
users:
  - name: heartbeatdev
    groups: [sudo]
    sudo: ALL=(ALL) NOPASSWD:ALL
    shell: /bin/bash
    lock_passwd: true
    ssh_authorized_keys:
      - $pub
ssh_pwauth: false
package_update: false
EOF
cat > "$storage/meta-data" <<EOF
instance-id: heartbeat-arm64-v1
local-hostname: heartbeat-arm64
EOF
cloud-localds "$storage/seed.img" "$storage/user-data" "$storage/meta-data"

if [[ ! -f "$storage/device.qcow2" ]]; then
  qemu-img create -f qcow2 -F qcow2 -b "$base" "$storage/device.qcow2"
  qemu-img resize "$storage/device.qcow2" 8G
fi
if [[ ! -f "$storage/vars.fd" ]]; then
  cp /usr/share/AAVMF/AAVMF_VARS.fd "$storage/vars.fd"
fi
echo "ARM64 VM prepared: 4 vCPUs, 2048 MiB, 8 GiB virtual disk"
