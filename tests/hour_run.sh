#!/usr/bin/env bash
# One-hour ARM64 resource run using the development mock only.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
state="${1:-$HOME/heartbeat-hour-$(date +%Y%m%d%H%M%S)}"
if [[ -e "$state" ]]; then
  echo "Run directory already exists: $state" >&2
  exit 1
fi
umask 077
mkdir -p "$state"
cd "$root"
set -a
. ./device.env
set +a
export PIPELINE_DB="$state/device.db"
export PYTHONUNBUFFERED=1
mock_pid=''
pipeline_pid=''
cleanup() {
  [[ -z "$pipeline_pid" ]] || kill -TERM "$pipeline_pid" 2>/dev/null || true
  [[ -z "$mock_pid" ]] || kill -TERM "$mock_pid" 2>/dev/null || true
}
trap cleanup EXIT
python3 -u pipeline.py run --mock-port 7100 > "$state/pipeline.log" 2>&1 &
pipeline_pid=$!
online=false
for _ in $(seq 1 80); do
  if python3 pipeline.py status 2>/dev/null | grep -q '"mode":"online"'; then
    online=true
    break
  fi
  sleep 0.25
done
if [[ "$online" != true ]]; then
  echo "Pipeline did not reach online mode before the mock started." >&2
  exit 1
fi
python3 -u tools/dev_mock.py --mode signal --seed 7 --duration 3600 --port 7100 \
  > "$state/mock.log" 2>&1 &
mock_pid=$!
for _ in $(seq 1 40); do
  [[ -s "$state/mock.log" ]] && break
  sleep 0.25
done
session="$(head -n 1 "$state/mock.log")"
if [[ ! "$session" =~ ^[a-f0-9]{8}$ ]]; then
  echo "Mock did not start: $(cat "$state/mock.log")" >&2
  exit 1
fi
echo "run_dir=$state session=$session mock_pid=$mock_pid pipeline_pid=$pipeline_pid"
python3 tools/resource_monitor.py --pid "$mock_pid" --pid "$pipeline_pid" \
  --interface enp0s1 --seconds 3600 > "$state/resources.json"
python3 tests/hour_report.py "$session" "$PIPELINE_DB" > "$state/report.json"
cat "$state/resources.json"
cat "$state/report.json"
