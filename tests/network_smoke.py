"""Simulate cloud unreachability, then verify mode recovery and catch-up."""
import http.server
import json
import os
import sqlite3
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from heartbeat.agent import cloud_request
from heartbeat.store import read_only

TARGET = os.environ.get("PIPELINE_CLOUD_URL", "http://127.0.0.1:8787")
TOKEN = os.environ.get("PIPELINE_TOKEN", "local-test-token")


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Proxy(http.server.BaseHTTPRequestHandler):
    online = True

    def do_GET(self):
        self.forward()

    def do_POST(self):
        self.forward()

    def forward(self):
        if not Proxy.online:
            self.send_error(503, "simulated outage")
            return
        size = int(self.headers.get("Content-Length", 0))
        data = self.rfile.read(size) if size else None
        request = urllib.request.Request(TARGET + self.path, data=data, method=self.command,
            headers={"Authorization": self.headers.get("Authorization", ""),
                     "Content-Type": "application/json",
                     "User-Agent": "heartbeat-pipeline/0.1"})
        try:
            response = urllib.request.urlopen(request, timeout=4)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            body = response.read()
            self.send_response(response.status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def log_message(self, *_args):
        pass


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()


def await_mode(db_path, wanted, timeout):
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        if db_path.exists():
            db = read_only(db_path)
            try:
                row = db.execute("SELECT value FROM meta WHERE key='mode'").fetchone()
            except sqlite3.OperationalError as error:
                if "no such table" not in str(error):
                    raise
                row = None
            finally:
                db.close()
            if row and row[0] == wanted:
                return time.monotonic() - started
        time.sleep(0.2)
    raise AssertionError(f"mode did not become {wanted} within {timeout}s")


def main():
    with tempfile.TemporaryDirectory() as temp:
        mock_port, proxy_port = free_port(), free_port()
        proxy = http.server.ThreadingHTTPServer(("127.0.0.1", proxy_port), Proxy)
        threading.Thread(target=proxy.serve_forever, daemon=True).start()
        db_path = Path(temp) / "device.db"
        env = {**os.environ, "PIPELINE_DB": str(db_path),
               "PIPELINE_CLOUD_URL": f"http://127.0.0.1:{proxy_port}",
               "PIPELINE_TOKEN": TOKEN, "PYTHONUNBUFFERED": "1"}
        mock = subprocess.Popen([sys.executable, "-u", "tools/dev_mock.py", "--mode", "ramp",
            "--duration", "18", "--port", str(mock_port)], cwd=ROOT,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        session = mock.stdout.readline().strip()
        log_path = Path(temp) / "pipeline.log"
        with log_path.open("w", encoding="utf-8") as log:
            pipeline = subprocess.Popen([sys.executable, "-u", "pipeline.py", "run",
                "--mock-port", str(mock_port)], cwd=ROOT, env=env,
                stdout=subprocess.DEVNULL, stderr=log)
            try:
                try:
                    # Cold startup on an emulated ARM64 VM can take longer than
                    # a mode switch; only the outage/recovery waits test 10 s.
                    await_mode(db_path, "online", 30)
                except AssertionError as error:
                    raise AssertionError(f"{error}\n{log_path.read_text()}") from error
                time.sleep(3)
                Proxy.online = False
                offline_seconds = await_mode(db_path, "offline", 9)
                time.sleep(2)
                Proxy.online = True
                online_seconds = await_mode(db_path, "online", 9)
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    probe = read_only(db_path)
                    ready = probe.execute("SELECT COUNT(*) FROM frames WHERE session=?",
                                          (session,)).fetchone()[0]
                    done = probe.execute("SELECT COUNT(*) FROM frames WHERE session=? AND uploaded=1",
                                         (session,)).fetchone()[0]
                    probe.close()
                    if ready >= 18 and done == ready:
                        break
                    time.sleep(0.5)
            finally:
                stop(pipeline)
                stop(mock)
                proxy.shutdown()
        db = read_only(db_path)
        counts = [db.execute("SELECT COUNT(*) FROM samples WHERE session=? AND sensor=?",
            (session, sensor)).fetchone()[0] for sensor in (1, 2)]
        offline_results = db.execute("SELECT COUNT(*) FROM results WHERE session=? AND mode='offline'",
            (session,)).fetchone()[0]
        frames = db.execute("SELECT COUNT(*) FROM frames WHERE session=?", (session,)).fetchone()[0]
        uploaded = db.execute("SELECT COUNT(*) FROM frames WHERE session=? AND uploaded=1",
            (session,)).fetchone()[0]
        db.close()
        cloud_frames = 0
        after = -1
        while True:
            page = cloud_request(TARGET, TOKEN,
                path=f"/frames?session={session}&after={after}")["frames"]
            if not page:
                break
            cloud_frames += len(page)
            after = page[-1]["window"]
        report = {"session": session, "offline_switch_s": round(offline_seconds, 2),
                  "online_switch_s": round(online_seconds, 2), "samples": counts,
                  "offline_results": offline_results, "frames": frames,
                  "uploaded_frames": uploaded, "cloud_frames": cloud_frames}
        print(json.dumps(report))
        if min(counts) < 6000 or offline_results < 2 or frames < 18 or uploaded != frames or cloud_frames != frames:
            raise AssertionError(f"network test failed:\n{log_path.read_text()[-5000:]}")


if __name__ == "__main__":
    main()
