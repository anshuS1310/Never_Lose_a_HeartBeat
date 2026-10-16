#!/usr/bin/env bash
# Run inside the ARM64 guest. The host can then hard-stop QEMU to simulate power loss.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
state="${1:-$HOME/heartbeat-power-test}"
mkdir -p "$state"
cd "$root"
export PIPELINE_DB="$state/device.db"
unset PIPELINE_CLOUD_URL PIPELINE_TOKEN
nohup python3 -u tools/dev_mock.py --mode ramp --duration 120 --port 7100 \
  > "$state/mock.log" 2>&1 < /dev/null &
echo "$!" > "$state/mock.pid"
sleep 1
nohup python3 -u pipeline.py run --mock-port 7100 \
  > "$state/pipeline.log" 2>&1 < /dev/null &
echo "$!" > "$state/pipeline.pid"
printf 'mock_pid=%s pipeline_pid=%s db=%s\n' \
  "$(cat "$state/mock.pid")" "$(cat "$state/pipeline.pid")" "$PIPELINE_DB"
