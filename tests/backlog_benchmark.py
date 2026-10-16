"""Prepare exact ramp data and offline results, then time only cloud catch-up."""
import json
import os
import subprocess
import sys
import tempfile
import time
import uuid
from contextlib import nullcontext
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from heartbeat.processing import compute_window
from heartbeat.agent import cloud_request
from heartbeat.store import connect, read_only, save_result


def main():
    windows = int(os.environ.get("BACKLOG_WINDOWS", "120"))
    resumed_db = os.environ.get("BACKLOG_RESUME_DB")
    context = nullcontext(str(Path(resumed_db).parent)) if resumed_db else tempfile.TemporaryDirectory()
    with context as temp:
        db_path = Path(resumed_db) if resumed_db else Path(temp) / "device.db"
        db = connect(db_path)
        if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise RuntimeError("backlog database failed integrity check")
        if resumed_db:
            sessions = db.execute("SELECT id,next1,next2 FROM sessions WHERE id LIKE 'backlog%'").fetchall()
            if (len(sessions) != 1 or sessions[0]["next1"] != windows * 400 or
                    sessions[0]["next2"] != windows * 400):
                raise RuntimeError("resume database has the wrong session or sample count")
            session = sessions[0]["id"]
            counts = [db.execute("SELECT COUNT(*) FROM samples WHERE session=? AND sensor=?",
                                 (session, sensor)).fetchone()[0] for sensor in (1, 2)]
            if counts != [windows * 400, windows * 400]:
                raise RuntimeError(f"resume database samples incomplete: {counts}")
            row = db.execute("SELECT MAX(window) FROM results WHERE session=? AND mode='offline'",
                             (session,)).fetchone()
            first_window = 0 if row[0] is None else row[0] + 1
            result_count = db.execute("SELECT COUNT(*) FROM results WHERE session=? AND mode='offline'",
                                      (session,)).fetchone()[0]
            if result_count != first_window:
                raise RuntimeError("resume database has a gap in offline results")
        else:
            session = "backlog" + uuid.uuid4().hex[:10]
            db.execute("INSERT INTO sessions(id,next1,next2,ended1,ended2) VALUES(?,?,?,?,?)",
                       (session, windows * 400, windows * 400, 1, 1))
            db.execute("BEGIN IMMEDIATE")
            for sensor in (1, 2):
                for window in range(windows):
                    first = window * 400
                    rows = []
                    for index in range(first, first + 400):
                        values = ([(3 * index + c) % 8388608 for c in (1, 2, 3)] if sensor == 1
                                  else [(2 * index + c) % 8388608 for c in (1, 2)])
                        rows.append((session, sensor, index, values[0], values[1],
                                     values[2] if sensor == 1 else None))
                    db.executemany("INSERT INTO samples VALUES(?,?,?,?,?,?)", rows)
            db.execute("COMMIT")
            first_window = 0
        preparation_started = time.monotonic()
        for window in range(first_window, windows):
            first = window * 400
            one = [[(3 * index + channel) % 8388608 for channel in (1, 2, 3)]
                   for index in range(first, first + 400)]
            two = [[(2 * index + channel) % 8388608 for channel in (1, 2)]
                   for index in range(first, first + 400)]
            context_start = (window - 6) * 400
            context1 = ([(3 * index + 2) % 8388608 if index >= 0 else None
                         for index in range(context_start, first + 401)]
                        if window >= 4 else None)
            db.execute("INSERT OR IGNORE INTO frames(session,window,mode,count1,count2) VALUES(?,?,?,?,?)",
                       (session, window, "offline", 400, 400))
            result = compute_window(window, one, two, context1,
                                    context_start if context1 is not None else None,
                                    None, "offline")
            save_result(db, session, result)
            db.execute("INSERT OR IGNORE INTO frame_wire VALUES(?,?,?)",
                       (session, window, json.dumps({
                           "session": session, "window": window, "mode": "offline",
                           "sensor1": one, "sensor2": two, "losses": []
                       }, separators=(",", ":"))))
        if resumed_db:
            for window in range(first_window):
                if db.execute("SELECT 1 FROM frame_wire WHERE session=? AND window=?",
                              (session, window)).fetchone():
                    continue
                first = window * 400
                one = [[(3 * index + channel) % 8388608 for channel in (1, 2, 3)]
                       for index in range(first, first + 400)]
                two = [[(2 * index + channel) % 8388608 for channel in (1, 2)]
                       for index in range(first, first + 400)]
                db.execute("INSERT INTO frame_wire VALUES(?,?,?)",
                           (session, window, json.dumps({
                               "session": session, "window": window, "mode": "offline",
                               "sensor1": one, "sensor2": two, "losses": []
                           }, separators=(",", ":"))))
        preparation_seconds = time.monotonic() - preparation_started
        wire_count = db.execute("SELECT COUNT(*) FROM frame_wire WHERE session=?",
                                (session,)).fetchone()[0]
        if wire_count != windows:
            raise RuntimeError(f"offline frame cache incomplete: {wire_count} / {windows}")
        db.close()
        env = {**os.environ, "PIPELINE_DB": str(db_path), "PYTHONUNBUFFERED": "1"}
        if not env.get("PIPELINE_CLOUD_URL") or not env.get("PIPELINE_TOKEN"):
            raise RuntimeError("set PIPELINE_CLOUD_URL and PIPELINE_TOKEN")
        log_path = Path(temp) / "pipeline.log"
        with log_path.open("w", encoding="utf-8") as log:
            started = time.monotonic()
            process = subprocess.Popen([sys.executable, "-u", "pipeline.py", "run",
                "--mock-port", "7199"], cwd=ROOT, env=env,
                stdout=subprocess.DEVNULL, stderr=log)
            try:
                while time.monotonic() - started < max(75, windows / 8 + 60):
                    time.sleep(0.5)
                    probe = read_only(db_path)
                    uploaded = probe.execute("SELECT COUNT(*) FROM frames WHERE session=? AND uploaded=1",
                                             (session,)).fetchone()[0]
                    probe.close()
                    if uploaded == windows:
                        break
            finally:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
        elapsed = time.monotonic() - started
        verified = read_only(db_path)
        synced = verified.execute("SELECT COUNT(*) FROM results WHERE session=? AND mode='offline' "
                                  "AND synced=1", (session,)).fetchone()[0]
        verified.close()
        cloud_results = []
        while True:
            page = cloud_request(env["PIPELINE_CLOUD_URL"], env["PIPELINE_TOKEN"],
                path=f"/results?session={session}&offset={len(cloud_results)}",
                timeout=12)["results"]
            cloud_results.extend(page)
            if len(page) < 100:
                break
        report = {"session": session, "windows": windows, "uploaded": uploaded,
                  "preparation_seconds": round(preparation_seconds, 2),
                  "resumed_precommitted_database": bool(resumed_db),
                  "cached_frame_payloads": wire_count,
                  "elapsed_seconds": round(elapsed, 2),
                  "frames_per_second": round(uploaded / elapsed, 2),
                  "local_results_synced": synced, "cloud_results": len(cloud_results)}
        print(json.dumps(report))
        if (uploaded != windows or synced != windows or len(cloud_results) != windows or
                {row["window"] for row in cloud_results} != set(range(windows)) or
                uploaded / elapsed < 8):
            raise AssertionError(f"catch-up rate below 8 frames/s\n{log_path.read_text()[-5000:]}")


if __name__ == "__main__":
    main()
