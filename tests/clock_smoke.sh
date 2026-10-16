#!/usr/bin/env bash
# Isolated development-mock test; changes the ARM64 guest clock and restores it.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
run="$HOME/heartbeat-clock-$(date +%Y%m%d%H%M%S)"
mkdir -p "$run"
real_start="$(date -u +%s)"
uptime_start="$(cut -d' ' -f1 /proc/uptime)"
ntp_was_enabled="$(timedatectl show -p NTP --value)"
mock_pid=''
pipeline_pid=''
restore() {
  [[ -z "$pipeline_pid" ]] || kill -TERM "$pipeline_pid" 2>/dev/null || true
  [[ -z "$mock_pid" ]] || kill -TERM "$mock_pid" 2>/dev/null || true
  [[ -z "$pipeline_pid" ]] || wait "$pipeline_pid" 2>/dev/null || true
  [[ -z "$mock_pid" ]] || wait "$mock_pid" 2>/dev/null || true
  restored="$(python3 - "$real_start" "$uptime_start" <<'PY'
import sys
from pathlib import Path
current = float(Path('/proc/uptime').read_text().split()[0])
print(round(int(sys.argv[1]) + current - float(sys.argv[2])))
PY
)"
  sudo date -u -s "@$restored" > /dev/null
  if [[ "$ntp_was_enabled" == yes ]]; then
    sudo timedatectl set-ntp true
  fi
}
trap restore EXIT

sudo timedatectl set-ntp false
cd "$root"
python3 -u tools/dev_mock.py --mode ramp --duration 30 --port 7111 > "$run/mock.log" 2>&1 &
mock_pid=$!
for _ in $(seq 1 40); do
  [[ -s "$run/mock.log" ]] && break
  sleep 0.25
done
session="$(head -n 1 "$run/mock.log")"
[[ "$session" =~ ^[a-f0-9]{8}$ ]]
export PIPELINE_DB="$run/device.db" PIPELINE_CLOUD_URL='' PIPELINE_TOKEN=''
python3 -u pipeline.py run --mock-port 7111 > "$run/pipeline.log" 2>&1 &
pipeline_pid=$!
sleep 5
sudo date -u -s "@$((real_start + 7200))" > /dev/null
sleep 5
sudo date -u -s "@$((real_start - 7200))" > /dev/null
sleep 25

python3 - "$session" "$PIPELINE_DB" <<'PY' | tee "$run/report.json"
import json
import sqlite3
import sys

session, path = sys.argv[1:]
db = sqlite3.connect(path)
counts = [db.execute("SELECT COUNT(*) FROM samples WHERE session=? AND sensor=?",
                     (session, sensor)).fetchone()[0] for sensor in (1, 2)]
frames = db.execute("SELECT COUNT(*) FROM frames WHERE session=?", (session,)).fetchone()[0]
results = db.execute("SELECT COUNT(*) FROM results WHERE session=? AND mode='offline'",
                     (session,)).fetchone()[0]
losses = db.execute("SELECT COUNT(*) FROM losses WHERE session=?", (session,)).fetchone()[0]
report = {"session": session, "samples": counts, "frames": frames,
          "offline_results": results, "losses": losses}
print(json.dumps(report))
assert counts == [12000, 12000] and frames == 30 and results == 30 and losses == 0
db.close()
PY
echo "run_dir=$run"
