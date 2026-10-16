"""Measure cloud result latency against a real-time development mock session."""
import datetime as dt
import json
import os
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from heartbeat.agent import cloud_request
from heartbeat.store import connect


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()


def main():
    url, token = os.environ["PIPELINE_CLOUD_URL"], os.environ["PIPELINE_TOKEN"]
    duration = int(os.environ.get("LATENCY_DURATION", "25"))
    port = free_port()
    with tempfile.TemporaryDirectory() as temp:
        db_path = Path(temp) / "device.db"
        start_file = Path(temp) / "mock_started_ns"
        env = {**os.environ, "PIPELINE_DB": str(db_path), "PYTHONUNBUFFERED": "1",
               "PIPELINE_TRACE": "1"}
        before = dt.datetime.now(dt.timezone.utc)
        mock = subprocess.Popen([sys.executable, "-u", "tools/dev_mock.py", "--mode", "signal",
            "--duration", str(duration), "--port", str(port),
            "--start-file", str(start_file)], cwd=ROOT,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        session = mock.stdout.readline().strip()
        after = dt.datetime.now(dt.timezone.utc)
        mock_started = dt.datetime.fromtimestamp(int(start_file.read_text()) / 1e9,
                                                   tz=dt.timezone.utc)
        if not session:
            raise RuntimeError(mock.stderr.read())
        log_path = Path(temp) / "pipeline.log"
        with log_path.open("w", encoding="utf-8") as log:
            pipeline = subprocess.Popen([sys.executable, "-u", "pipeline.py", "run",
                "--mock-port", str(port)], cwd=ROOT, env=env,
                stdout=subprocess.DEVNULL, stderr=log)
            try:
                deadline = time.monotonic() + duration + 45
                while time.monotonic() < deadline:
                    time.sleep(1)
                    db = connect(db_path)
                    count = db.execute("SELECT COUNT(*) FROM frames WHERE session=?", (session,)).fetchone()[0]
                    uploaded = db.execute("SELECT COUNT(*) FROM frames WHERE session=? AND uploaded=1",
                                          (session,)).fetchone()[0]
                    db.close()
                    if count >= duration and uploaded == count:
                        break
            finally:
                stop(pipeline)
                stop(mock)
        results = cloud_request(url, token, path=f"/results?session={session}", timeout=10)["results"]
        online = [r for r in results if r["mode"] == "online" and r["window"] >= 5]
        latencies = []
        for result in online:
            processed = dt.datetime.strptime(result["processed_at"], "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=dt.timezone.utc)
            expected_end = mock_started + dt.timedelta(seconds=result["window"] + 1)
            latencies.append((processed - expected_end).total_seconds())
        events = []
        for line in log_path.read_text(encoding="utf-8").splitlines():
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                pass
        def event_delays(name):
            return [item["epoch_seconds"] - mock_started.timestamp() - item["window"] - 1
                    for item in events if item.get("event") == name and item.get("window", -1) >= 5]
        ready_delays = event_delays("frame_ready")
        ack_delays = event_delays("upload_ack")
        ready_events = [item for item in events if item.get("event") == "frame_ready"]
        stage_max = lambda key: (max(item[key] for item in ready_events)
                                 if ready_events else None)
        stage_median = lambda key: (round(statistics.median(item[key] for item in ready_events), 3)
                                    if ready_events else None)
        preparations = [item["prepare_seconds"] for item in events
                        if item.get("event") == "upload_ack"]
        requests = [item["request_seconds"] for item in events
                    if item.get("event") == "upload_ack"]
        report = {"session": session, "mock_started_utc": mock_started.isoformat(),
                  "duration_seconds": duration, "online_windows_after_w4": len(online),
                  "max_latency_seconds": max(latencies) if latencies else None,
                  "latencies_seconds": latencies,
                  "max_frame_ready_delay_seconds": round(max(ready_delays), 3) if ready_delays else None,
                  "max_upload_ack_delay_seconds": round(max(ack_delays), 3) if ack_delays else None,
                  "max_scan_seconds": stage_max("scan_seconds"),
                  "median_scan_seconds": stage_median("scan_seconds"),
                  "max_frame_payload_seconds": stage_max("payload_seconds"),
                  "median_frame_payload_seconds": stage_median("payload_seconds"),
                  "max_cursor_lead_windows": max((min(item["cursor1"], item["cursor2"]) // 400 -
                                                  item["window"] - 1 for item in ready_events),
                                                 default=None),
                  "max_prepare_seconds": max(preparations) if preparations else None,
                  "max_request_seconds": max(requests) if requests else None,
                  "cloud_results": len(results)}
        print(json.dumps(report))
        if not latencies or max(latencies) > 5:
            raise AssertionError("online cloud latency target was not met")


if __name__ == "__main__":
    main()
