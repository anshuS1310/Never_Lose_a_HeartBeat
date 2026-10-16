"""Run the development mock, kill and restart the pipeline, then verify ramp data."""
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from heartbeat.store import connect
from heartbeat.agent import cloud_request


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
            process.wait(timeout=3)


def main():
    port = free_port()
    duration = int(os.environ.get("INTEGRATION_DURATION", "10"))
    first_seconds = int(os.environ.get("INTEGRATION_FIRST_SECONDS", "4"))
    second_seconds = int(os.environ.get("INTEGRATION_SECOND_SECONDS", "6"))
    with tempfile.TemporaryDirectory() as temp:
        db_path = Path(temp) / "heartbeat.db"
        env = {**os.environ, "PIPELINE_DB": str(db_path), "PYTHONUNBUFFERED": "1"}
        mock = subprocess.Popen([sys.executable, "-u", "tools/dev_mock.py", "--mode", "ramp",
                                 "--duration", str(duration), "--port", str(port)], cwd=ROOT,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        session = mock.stdout.readline().strip()
        if not session:
            raise RuntimeError(mock.stderr.read())
        pipeline_cmd = [sys.executable, "-u", "pipeline.py", "run", "--mock-port", str(port)]
        log_path = Path(temp) / "pipeline.log"
        log = log_path.open("w", encoding="utf-8")
        first = subprocess.Popen(pipeline_cmd, cwd=ROOT, env=env,
                                 stdout=subprocess.DEVNULL, stderr=log)
        time.sleep(first_seconds)
        stop(first)
        second = subprocess.Popen(pipeline_cmd, cwd=ROOT, env=env,
                                  stdout=subprocess.DEVNULL, stderr=log)
        time.sleep(second_seconds)
        deadline = time.monotonic() + 45
        cloud_enabled = bool(env.get("PIPELINE_CLOUD_URL") and env.get("PIPELINE_TOKEN"))
        while time.monotonic() < deadline:
            probe = connect(db_path)
            counts_now = [probe.execute("SELECT COUNT(*) FROM samples WHERE session=? AND sensor=?",
                                        (session, sensor)).fetchone()[0] for sensor in (1, 2)]
            ready = probe.execute("SELECT COUNT(*) FROM frames WHERE session=?", (session,)).fetchone()[0]
            done = probe.execute("SELECT COUNT(*) FROM frames WHERE session=? AND uploaded=1",
                                 (session,)).fetchone()[0]
            local_results = probe.execute("SELECT COUNT(*) FROM results WHERE session=? AND mode='offline'",
                                          (session,)).fetchone()[0]
            probe.close()
            if (counts_now == [duration * 400] * 2 and ready >= duration and
                    ((cloud_enabled and done == ready) or
                     (not cloud_enabled and local_results >= ready))):
                break
            time.sleep(0.5)
        stop(second)
        stop(mock)
        log.close()
        db = connect(db_path)
        try:
            counts = [db.execute("SELECT COUNT(*) FROM samples WHERE session=? AND sensor=?",
                                 (session, sensor)).fetchone()[0] for sensor in (1, 2)]
            if counts != [duration * 400] * 2:
                raise AssertionError(f"too few samples: {counts}\n{log_path.read_text()}")
            for row in db.execute("SELECT sensor,idx,v1,v2,v3 FROM samples WHERE session=?",
                                  (session,)):
                i = row["idx"]
                expected = ([(3 * i + c) % 8388608 for c in (1, 2, 3)] if row["sensor"] == 1
                            else [(2 * i + c) % 8388608 for c in (1, 2)])
                actual = [row["v1"], row["v2"]]
                if row["sensor"] == 1:
                    actual.append(row["v3"])
                if actual != expected:
                    raise AssertionError(f"wrong sample {row['sensor']}:{i}")
            losses = db.execute("SELECT sensor,first_idx,last_idx,reason FROM losses WHERE session=?",
                                (session,)).fetchall()
            frames = db.execute("SELECT COUNT(*) FROM frames WHERE session=?", (session,)).fetchone()[0]
            results = db.execute("SELECT COUNT(*) FROM results WHERE session=?", (session,)).fetchone()[0]
            uploaded = db.execute("SELECT COUNT(*) FROM frames WHERE session=? AND uploaded=1",
                                  (session,)).fetchone()[0]
            print({"session": session, "samples": counts, "losses": [tuple(x) for x in losses],
                   "frames": frames, "offline_results": results, "uploaded_frames": uploaded})
            cloud_url, token = env.get("PIPELINE_CLOUD_URL"), env.get("PIPELINE_TOKEN")
            if cloud_url and token:
                cloud = cloud_request(cloud_url, token,
                    path=f"/results?session={session}")["results"]
                if uploaded != frames or len(cloud) < frames:
                    raise AssertionError(f"cloud incomplete: {uploaded}, {len(cloud)}\n{log_path.read_text()[-5000:]}")
            elif results != frames:
                raise AssertionError("offline results incomplete")
            if losses or frames != duration:
                raise AssertionError("integration run incomplete or unexpected loss")
        finally:
            db.close()


if __name__ == "__main__":
    main()
