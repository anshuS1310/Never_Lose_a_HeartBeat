#!/usr/bin/env bash
# Isolated ARM64 development-mock disk-full test. Requires sudo for a loop mount.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
run="$HOME/heartbeat-disk-full-$(date +%Y%m%d%H%M%S)"
volume="$run/volume"
mkdir -p "$volume"
image="$run/volume.ext4"
truncate -s 64M "$image"
/usr/sbin/mkfs.ext4 -F -q "$image"
sudo mount -t ext4 -o loop "$image" "$volume"
sudo chown "$(id -u):$(id -g)" "$volume"
db="$volume/device.db"
mock_pid=''
pipeline_pid=''
cleanup() {
  [[ -z "$pipeline_pid" ]] || kill -TERM "$pipeline_pid" 2>/dev/null || true
  [[ -z "$mock_pid" ]] || kill -TERM "$mock_pid" 2>/dev/null || true
  [[ -z "$pipeline_pid" ]] || wait "$pipeline_pid" 2>/dev/null || true
  [[ -z "$mock_pid" ]] || wait "$mock_pid" 2>/dev/null || true
  sudo umount "$volume"
  rmdir "$volume"
}
trap cleanup EXIT

cd "$root"
python3 -u tools/dev_mock.py --mode ramp --duration 45 --port 7110 > "$run/mock.log" 2>&1 &
mock_pid=$!
for _ in $(seq 1 40); do
  [[ -s "$run/mock.log" ]] && break
  sleep 0.25
done
session="$(head -n 1 "$run/mock.log")"
[[ "$session" =~ ^[a-f0-9]{8}$ ]]
export PIPELINE_DB="$db" PIPELINE_CLOUD_URL='' PIPELINE_TOKEN=''
python3 -u pipeline.py run --mock-port 7110 > "$run/pipeline.log" 2>&1 &
pipeline_pid=$!
sleep 3

# Fill the ordinary space, wait for the preallocated reserve to be released,
# then fill the released space too. The guest root disk is not filled.
dd if=/dev/zero of="$volume/fill1" bs=1M count=64 status=none 2>> "$run/fill.log" || true
for _ in $(seq 1 50); do
  [[ ! -e "$db.reserve" ]] && break
  sleep 0.2
done
dd if=/dev/zero of="$volume/fill2" bs=1M count=64 status=none 2>> "$run/fill.log" || true
sleep 13
sudo rm -f -- "$volume/fill1" "$volume/fill2"
restarted=0
if ! kill -0 "$pipeline_pid" 2>/dev/null; then
  restarted=1
  wait "$pipeline_pid" 2>/dev/null || true
  python3 -u pipeline.py run --mock-port 7110 >> "$run/pipeline.log" 2>&1 &
  pipeline_pid=$!
fi
sleep 50

kill -TERM "$pipeline_pid" 2>/dev/null || true
wait "$pipeline_pid" 2>/dev/null || true
pipeline_pid=''
python3 - "$session" "$db" "$run" "$restarted" <<'PY' | tee "$run/report.json"
import json
import sqlite3
import sys
from pathlib import Path

session, db_path, run_path, restarted = sys.argv[1:]
db = sqlite3.connect(db_path)
db.row_factory = sqlite3.Row
integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
assert integrity == "ok", integrity
expected = 45 * 400
report = {"session": session, "storage": "64 MiB ext4 loop filesystem",
          "integrity": integrity,
          "expected_per_sensor": expected,
          "pipeline_restarted": bool(int(restarted)),
          "reserve_released": '"event":"disk_reserve_released"' in
              (Path(run_path) / "pipeline.log").read_text(),
          "sensors": {}}
for sensor in (1, 2):
    seen = bytearray(expected)
    count = 0
    for row in db.execute("SELECT idx,v1,v2,v3 FROM samples WHERE session=? AND sensor=?",
                          (session, sensor)):
        index = row["idx"]
        assert 0 <= index < expected and not seen[index], (sensor, index)
        actual = [row["v1"], row["v2"]]
        if sensor == 1:
            actual.append(row["v3"])
        factor = 3 if sensor == 1 else 2
        assert actual == [(factor * index + channel) % 8388608
                          for channel in range(1, len(actual) + 1)]
        seen[index] = 1
        count += 1
    losses = []
    for row in db.execute("SELECT first_idx,last_idx,reason FROM losses "
                          "WHERE session=? AND sensor=? ORDER BY first_idx", (session, sensor)):
        first, last, reason = tuple(row)
        losses.append({"from_index": first, "to_index": last, "reason": reason})
        if last is not None:
            for index in range(first, min(last + 1, expected)):
                assert not seen[index], (sensor, index, "overlap")
                seen[index] = 1
    report["sensors"][sensor] = {"saved": count, "losses": losses,
                                 "unaccounted": seen.count(0)}
db.backup(sqlite3.connect(str(Path(run_path) / "device-backup.db")))
db.close()
print(json.dumps(report))
assert all(item["unaccounted"] == 0 for item in report["sensors"].values()), report
assert not report["pipeline_restarted"] and report["reserve_released"], report
PY
echo "run_dir=$run"
