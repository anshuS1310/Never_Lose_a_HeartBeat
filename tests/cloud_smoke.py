"""Exercise a running local or deployed Cloudflare Worker without writing secrets."""
import argparse
import json
import os
import subprocess
import sys
import tempfile
import urllib.error
import uuid

from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from heartbeat.agent import cloud_request
from heartbeat.processing import compute_window


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=os.environ.get("PIPELINE_CLOUD_URL", "http://127.0.0.1:8787"))
    parser.add_argument("--token", default=os.environ.get("PIPELINE_TOKEN", "local-test-token"))
    args = parser.parse_args()
    session = "cloudsmoke"
    one = [[0, 0, 0] for _ in range(2801)]
    for index in range(0, 2800, 400):
        one[index] = [0, 1000, 0]
    two = [[3, 4] for _ in range(400)]
    frame = {"session": session, "window": 6, "mode": "online",
             "sensor1": one[2400:2800], "sensor2": two, "losses": [],
             "context_start": 0, "context1": one, "previous_peak": None}
    expected = compute_window(6, frame["sensor1"], two, one, 0, None, "online")
    try:
        cloud_request(args.url, args.token, "POST", "/frame", frame)
    except urllib.error.HTTPError as error:
        print(f"ingest HTTP {error.code}: {error.read(1000)!r}", file=sys.stderr)
        raise
    cloud_request(args.url, args.token, "POST", "/frame", frame)
    page = cloud_request(args.url, args.token, path=f"/results?session={session}")["results"]
    actual = next(row for row in page if row["window"] == 6)
    actual.pop("processed_at", None)
    if actual != expected:
        raise AssertionError(f"result mismatch: {actual} != {expected}")
    frames = cloud_request(args.url, args.token, path=f"/frames?session={session}&after=-1")["frames"]
    if len(frames) != 1 or frames[0]["sensor1"] != frame["sensor1"]:
        raise AssertionError("cloud frame mismatch or duplicate")
    changed = {**frame, "sensor1": [x[:] for x in frame["sensor1"]]}
    changed["sensor1"][0][0] = 99
    try:
        cloud_request(args.url, args.token, "POST", "/frame", changed)
    except urllib.error.HTTPError as error:
        if error.code != 409:
            raise
    else:
        raise AssertionError("conflicting frame was accepted")
    compact_session = "compactsmoke" + uuid.uuid4().hex[:8]
    compact = {**frame, "session": compact_session,
               "context1": [row[1] if row is not None else None for row in one]}
    cloud_request(args.url, args.token, "POST", "/frame", compact)
    compact_result = cloud_request(args.url, args.token,
        path=f"/results?session={compact_session}")["results"][0]
    compact_result.pop("processed_at", None)
    if compact_result != expected:
        raise AssertionError("compact ECG context changed result")
    batch_session = "batchsmoke" + uuid.uuid4().hex[:8]
    batch = []
    for window in (0, 1):
        batch.append({"session": batch_session, "window": window, "mode": "offline",
                      "sensor1": [[window, 1, 2] for _ in range(400)],
                      "sensor2": [[3, 4] for _ in range(400)], "losses": [],
                      "offline_result": compute_window(window,
                          [[window, 1, 2] for _ in range(400)], two,
                          None, None, None, "offline")})
    cloud_request(args.url, args.token, "POST", "/frame-batch", batch)
    cloud_request(args.url, args.token, "POST", "/frame-batch", batch)
    batch_results = cloud_request(args.url, args.token,
        path=f"/results?session={batch_session}")["results"]
    if len(batch_results) != 2 or {row["window"] for row in batch_results} != {0, 1}:
        raise AssertionError("batch results missing or duplicated")
    conflicting = [dict(item) for item in batch]
    conflicting[0]["sensor1"] = [[99, 1, 2] for _ in range(400)]
    try:
        cloud_request(args.url, args.token, "POST", "/frame-batch", conflicting)
    except urllib.error.HTTPError as error:
        if error.code != 409:
            raise
    else:
        raise AssertionError("conflicting batch was accepted")
    terminal_session = "terminalsmoke" + uuid.uuid4().hex[:8]
    terminal = {"session": terminal_session, "sensor": 1,
                "from_index": 400, "reason": "mock_restarted"}
    cloud_request(args.url, args.token, "POST", "/terminal-loss", terminal)
    cloud_request(args.url, args.token, "POST", "/terminal-loss", terminal)
    terminal_losses = cloud_request(args.url, args.token,
        path=f"/terminal-loss?session={terminal_session}")["losses"]
    if terminal_losses != [{"sensor": 1, "from_index": 400,
                            "to_index": None, "reason": "mock_restarted"}]:
        raise AssertionError(f"terminal loss mismatch: {terminal_losses}")
    env = {**os.environ, "PIPELINE_DB": str(Path(tempfile.gettempdir()) /
                                             f"missing-{terminal_session}.db"),
           "PIPELINE_CLOUD_URL": args.url, "PIPELINE_TOKEN": args.token}
    export = subprocess.run([sys.executable, "pipeline.py", "export", "--session",
                             terminal_session, "--from", "cloud"], cwd=Path(__file__).resolve().parents[1],
                            env=env, capture_output=True, text=True, check=True)
    if [json.loads(line) for line in export.stdout.splitlines()] != terminal_losses:
        raise AssertionError("cloud CLI export differs from terminal loss query")
    print(json.dumps({"status": "ok", "heart_rate": actual["heart_rate"],
                      "frames": len(frames), "batch_results": len(batch_results),
                      "terminal_losses": len(terminal_losses)}))


if __name__ == "__main__":
    main()
