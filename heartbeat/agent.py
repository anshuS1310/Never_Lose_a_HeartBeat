import json
import logging
import os
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor

from .collector import Collector
from .processing import compute_window, scan_peaks
from .reserve import ensure as ensure_reserve, release as release_reserve
from .store import (active_session, connect, frame_payload, get_ecg_values, get_rows,
                    insert_loss, save_result)

LOG = logging.getLogger("pipeline")


def cloud_request(url, token, method="GET", path="/health", body=None, timeout=2.0):
    headers = {"Authorization": f"Bearer {token}",
               "User-Agent": "heartbeat-pipeline/0.1"}
    data = None
    if body is not None:
        data = (body if isinstance(body, bytes) else
                body.encode("utf-8") if isinstance(body, str) else
                json.dumps(body, separators=(",", ":")).encode("utf-8"))
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url.rstrip("/") + path, data=data,
                                     headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


class Agent:
    def __init__(self, db_path, mock_host, mock_port, cloud_url, token):
        self.db_path, self.mock_host, self.mock_port = db_path, mock_host, mock_port
        self.cloud_url, self.token = cloud_url, token
        self.stop = threading.Event()
        self.mode_lock = threading.Lock()
        self.mode = "offline"
        self.mode_dirty = False
        self.last_upload_ok = 0.0
        self.trace = os.environ.get("PIPELINE_TRACE") == "1"
        self.ecg_contexts = OrderedDict()
        self.upload_contexts = OrderedDict()
        self.context_lock = threading.Lock()

    def log(self, event, **fields):
        LOG.info(json.dumps({"event": event, **fields}, separators=(",", ":")))

    def set_mode(self, mode):
        with self.mode_lock:
            changed = self.mode != mode
            if not changed and not self.mode_dirty:
                return
            self.mode = mode
            try:
                db = connect(self.db_path)
                try:
                    db.execute("INSERT INTO meta(key,value) VALUES('mode',?) "
                               "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (mode,))
                finally:
                    db.close()
                self.mode_dirty = False
            except sqlite3.OperationalError as error:
                if "full" not in str(error).lower():
                    raise
                self.mode_dirty = True
                if release_reserve(self.db_path):
                    self.log("disk_reserve_released", error=str(error))
                else:
                    self.log("mode_disk_full_retry", error=str(error))
            if changed:
                self.log("mode_change", mode=mode)

    def get_mode(self):
        with self.mode_lock:
            return self.mode

    def health_loop(self):
        failures = 0
        while not self.stop.is_set():
            if not self.cloud_url or not self.token:
                self.set_mode("offline")
                self.stop.wait(2)
                continue
            with self.mode_lock:
                recent_upload = (self.mode == "online" and
                                 time.monotonic() - self.last_upload_ok < 2.5)
            if recent_upload:
                self.stop.wait(2)
                continue
            try:
                cloud_request(self.cloud_url, self.token, timeout=4)
                failures = 0
                self.set_mode("online")
            except Exception as error:
                failures += 1
                self.set_mode("offline")
                self.log("health_failure", error=str(error))
            self.stop.wait(2)

    def ecg_context(self, db, session, window):
        start, end = (window - 6) * 400, (window + 1) * 400
        old = self.ecg_contexts.get(session)
        if old and old[0] + 400 == start:
            next_first = old[0] + len(old[1])
            values = old[1][400:] + get_ecg_values(db, session, next_first, end)
        else:
            values = get_ecg_values(db, session, start, end)
        self.ecg_contexts[session] = (start, values)
        self.ecg_contexts.move_to_end(session)
        if len(self.ecg_contexts) > 8:
            self.ecg_contexts.popitem(last=False)
        return start, values

    def peak_scan_to(self, db, session, target, context=None, context_start=None):
        state = db.execute("SELECT scanned_through,previous_peak FROM peak_state WHERE session=?",
                           (session,)).fetchone()
        scanned = state["scanned_through"] if state else 799
        previous = state["previous_peak"] if state else None
        while scanned < target:
            first = max(800, scanned + 1)
            last = min(target, first + 399)
            if last < first:
                break
            if (context is not None and context_start <= first - 800 and
                    context_start + len(context) - 1 >= last + 1):
                source, source_start = context, context_start
            else:
                source_start = first - 800
                source = get_ecg_values(db, session, source_start, last + 1)
            peaks = scan_peaks(source, source_start, first, last, previous)
            if peaks:
                previous = peaks[-1]
            db.execute("BEGIN IMMEDIATE")
            try:
                db.executemany("INSERT OR IGNORE INTO peaks(session,idx) VALUES(?,?)",
                               ((session, index) for index in peaks))
                db.execute("INSERT INTO peak_state(session,scanned_through,previous_peak) "
                           "VALUES(?,?,?) ON CONFLICT(session) DO UPDATE SET "
                           "scanned_through=excluded.scanned_through, "
                           "previous_peak=excluded.previous_peak", (session, last, previous))
                db.execute("COMMIT")
            except BaseException:
                db.execute("ROLLBACK")
                raise
            scanned = last

    def frame_loop(self):
        db = connect(self.db_path)
        try:
            self._frame_loop(db)
        finally:
            db.close()

    def _frame_loop(self, db):
        while not self.stop.is_set():
            made = False
            for session in db.execute("SELECT * FROM sessions ORDER BY created_at,id").fetchall():
                sid = session["id"]
                row = db.execute("SELECT MAX(window) FROM frames WHERE session=?", (sid,)).fetchone()
                window = (row[0] + 1) if row[0] is not None else 0
                first, end = window * 400, (window + 1) * 400
                # A full window needs the following ECG sample to decide its final peak.
                ended = bool(session["ended1"] and session["ended2"])
                if ended:
                    maximum = max(session["next1"], session["next2"])
                    ready = first < maximum
                    if ready:
                        for sensor in (1, 2):
                            cursor = session[f"next{sensor}"]
                            if cursor < min(end, maximum):
                                terminal = db.execute(
                                    "SELECT 1 FROM losses WHERE session=? AND sensor=? "
                                    "AND last_idx IS NULL LIMIT 1", (sid, sensor)).fetchone()
                                if terminal:
                                    continue
                                db.execute("BEGIN IMMEDIATE")
                                insert_loss(db, sid, sensor, cursor, min(end, maximum) - 1,
                                            "sensor_ended")
                                db.execute(f"UPDATE sessions SET next{sensor}=? WHERE id=?",
                                           (min(end, maximum), sid))
                                db.execute("COMMIT")
                else:
                    ready = (session["next1"] > end and session["next2"] >= end)
                if not ready:
                    continue
                stage_start = time.monotonic()
                context_start, context = self.ecg_context(db, sid, window)
                self.peak_scan_to(db, sid, end - 1, context, context_start)
                scan_done = time.monotonic()
                mode = self.get_mode()
                payload = frame_payload(db, sid, window, mode,
                                        include_context=False)
                payload_done = time.monotonic()
                previous_peak = (db.execute("SELECT MAX(idx) FROM peaks WHERE session=? AND idx<?",
                                            (sid, (window - 4) * 400)).fetchone()[0]
                                 if window >= 4 else None)
                inserted = db.execute("INSERT OR IGNORE INTO frames(session,window,mode,count1,count2) "
                           "VALUES(?,?,?,?,?)", (sid, window, mode,
                           sum(x is not None for x in payload["sensor1"]),
                           sum(x is not None for x in payload["sensor2"])))
                if self.trace and inserted.rowcount:
                    self.log("frame_ready", session=sid, window=window,
                             epoch_seconds=time.time(), cursor1=session["next1"],
                             cursor2=session["next2"],
                             scan_seconds=round(scan_done - stage_start, 3),
                             payload_seconds=round(payload_done - scan_done, 3))
                if mode == "offline":
                    result = compute_window(window, payload["sensor1"], payload["sensor2"],
                                            context, context_start, previous_peak, "offline")
                    save_result(db, sid, result)
                    db.execute("INSERT OR IGNORE INTO frame_wire VALUES(?,?,?)",
                               (sid, window, json.dumps(payload, separators=(",", ":"))))
                elif window >= 4:
                    with self.context_lock:
                        self.upload_contexts[(sid, window)] = (
                            context_start, context[:], previous_peak)
                        if len(self.upload_contexts) > 64:
                            self.upload_contexts.popitem(last=False)
                made = True
            if self.get_mode() == "offline":
                pending = db.execute("SELECT f.session,f.window,f.mode FROM frames f "
                    "LEFT JOIN results r ON r.session=f.session AND r.window=f.window "
                    "AND r.mode='offline' WHERE f.uploaded=0 AND r.session IS NULL "
                    "ORDER BY f.window LIMIT 1").fetchone()
                if pending:
                    payload = frame_payload(db, pending["session"], pending["window"], "offline")
                    result = compute_window(pending["window"], payload["sensor1"], payload["sensor2"],
                                            payload.get("context1"), payload.get("context_start"),
                                            payload.get("previous_peak"), "offline")
                    save_result(db, pending["session"], result)
                    made = True
            if not made:
                self.stop.wait(0.12)
    def resilient_frame_loop(self):
        while not self.stop.is_set():
            try:
                self.frame_loop()
                return
            except sqlite3.OperationalError as error:
                if "full" not in str(error).lower():
                    raise
                if release_reserve(self.db_path):
                    self.log("disk_reserve_released", error=str(error))
                else:
                    self.log("frame_disk_full_retry", error=str(error))
                self.stop.wait(1)

    def _upload_task(self, sid, window, mode):
        # Each pool thread owns its SQLite connection; connections are never shared.
        local = getattr(threading.current_thread(), "heartbeat_db", None)
        if local is None:
            local = connect(self.db_path)
            threading.current_thread().heartbeat_db = local
        try:
            started = time.monotonic()
            payload = frame_payload(local, sid, window, mode, include_context=False)
            if mode == "online" and window >= 4:
                with self.context_lock:
                    cached = self.upload_contexts.get((sid, window))
                if cached is None:
                    payload = frame_payload(local, sid, window, mode, include_context=True)
                else:
                    payload["context_start"], payload["context1"], payload["previous_peak"] = cached
            result = local.execute("SELECT body FROM results WHERE session=? AND window=? AND mode='offline'",
                                   (sid, window)).fetchone()
            if result:
                payload["offline_result"] = json.loads(result[0])
            prepared = time.monotonic()
            cloud_request(self.cloud_url, self.token, "POST", "/frame", payload, timeout=8)
            acknowledged = time.monotonic()
            if self.trace:
                self.log("upload_ack", session=sid, window=window,
                         epoch_seconds=time.time(),
                         prepare_seconds=round(prepared - started, 3),
                         request_seconds=round(acknowledged - prepared, 3))
            local.execute("UPDATE frames SET uploaded=1 WHERE session=? AND window=?", (sid, window))
            if mode == "online" and window >= 4:
                with self.context_lock:
                    self.upload_contexts.pop((sid, window), None)
            if result:
                local.execute("UPDATE results SET synced=1 WHERE session=? AND window=? AND mode='offline'",
                              (sid, window))
            with self.mode_lock:
                self.last_upload_ok = time.monotonic()
        except Exception as error:
            self.log("upload_failure", session=sid, window=window, error=str(error))
            self.set_mode("offline")

    def _upload_batch_task(self, rows):
        local = getattr(threading.current_thread(), "heartbeat_db", None)
        if local is None:
            local = connect(self.db_path)
            threading.current_thread().heartbeat_db = local
        frames = []
        with_results = []
        try:
            for sid, window, _ in rows:
                cached = local.execute("SELECT body FROM frame_wire WHERE session=? AND window=?",
                                       (sid, window)).fetchone()
                wire = (cached[0] if cached else json.dumps(
                    frame_payload(local, sid, window, "offline", include_context=False),
                    separators=(",", ":")))
                result = local.execute("SELECT body FROM results WHERE session=? AND window=? "
                                       "AND mode='offline'", (sid, window)).fetchone()
                if result:
                    wire = wire[:-1] + ',"offline_result":' + result[0] + "}"
                    with_results.append((sid, window))
                frames.append(wire.encode("utf-8"))
            cloud_request(self.cloud_url, self.token, "POST", "/frame-batch",
                          b"[" + b",".join(frames) + b"]", timeout=12)
            local.execute("BEGIN IMMEDIATE")
            try:
                local.executemany("UPDATE frames SET uploaded=1 WHERE session=? AND window=?",
                                  ((sid, window) for sid, window, _ in rows))
                local.executemany("UPDATE results SET synced=1 WHERE session=? AND window=? "
                                  "AND mode='offline'", with_results)
                local.execute("COMMIT")
            except BaseException:
                local.execute("ROLLBACK")
                raise
            with self.mode_lock:
                self.last_upload_ok = time.monotonic()
        except Exception as error:
            if isinstance(error, urllib.error.HTTPError):
                detail = error.read(500).decode("utf-8", "replace")
            else:
                detail = str(error)
            self.log("batch_upload_failure", first=rows[0][1], count=len(rows), error=detail)
            self.set_mode("offline")

    def _upload_terminal(self, db):
        terminal = db.execute("SELECT l.session,l.sensor,l.first_idx,l.reason FROM losses l "
            "LEFT JOIN terminal_sync t ON t.session=l.session AND t.sensor=l.sensor "
            "AND t.first_idx=l.first_idx AND t.reason=l.reason "
            "WHERE l.last_idx IS NULL AND t.session IS NULL LIMIT 1").fetchone()
        if not terminal:
            return False
        try:
            cloud_request(self.cloud_url, self.token, "POST", "/terminal-loss",
                          {"session": terminal["session"], "sensor": terminal["sensor"],
                           "from_index": terminal["first_idx"], "reason": terminal["reason"]},
                          timeout=8)
            db.execute("INSERT OR IGNORE INTO terminal_sync VALUES(?,?,?,?)", tuple(terminal))
        except Exception as error:
            self.log("terminal_loss_upload_failure", error=str(error))
            self.set_mode("offline")
        return True

    def _sync_offline_result(self, db):
        row = db.execute("SELECT r.session,r.window,r.body FROM results r "
            "JOIN frames f ON f.session=r.session AND f.window=r.window "
            "WHERE r.mode='offline' AND r.synced=0 AND f.uploaded=1 LIMIT 1").fetchone()
        if not row:
            return False
        try:
            cloud_request(self.cloud_url, self.token, "POST", "/offline-result",
                          {"session": row["session"], "result": json.loads(row["body"])}, timeout=8)
            db.execute("UPDATE results SET synced=1 WHERE session=? AND window=? AND mode='offline'",
                       (row["session"], row["window"]))
        except Exception as error:
            self.log("offline_result_sync_failure", error=str(error))
            self.set_mode("offline")
        return True

    def upload_loop(self, mode, workers):
        db = connect(self.db_path)
        inflight = {}
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix=f"upload-{mode}") as pool:
            while not self.stop.is_set():
                for key, future in list(inflight.items()):
                    if future.done():
                        future.result()
                        del inflight[key]
                if self.get_mode() != "online":
                    self.stop.wait(0.2)
                    continue
                if mode == "online" and self._upload_terminal(db):
                    continue
                if mode == "online" and self._sync_offline_result(db):
                    continue
                capacity = workers - len(inflight)
                if capacity > 0:
                    rows = db.execute("SELECT session,window,mode FROM frames "
                        "WHERE uploaded=0 AND mode=? ORDER BY session,window LIMIT 128",
                        (mode,)).fetchall()
                    if mode == "offline":
                        busy = {key for batch in inflight for key in batch}
                        available = [(row["session"], row["window"], row["mode"])
                                     for row in rows if (row["session"], row["window"]) not in busy]
                        while available and capacity:
                            sid = available[0][0]
                            group = [row for row in available if row[0] == sid][:8]
                            key = tuple((row[0], row[1]) for row in group)
                            inflight[key] = pool.submit(self._upload_batch_task, group)
                            used = set(key)
                            available = [row for row in available if (row[0], row[1]) not in used]
                            capacity -= 1
                    else:
                        for row in rows:
                            key = (row["session"], row["window"])
                            if key not in inflight:
                                inflight[key] = pool.submit(self._upload_task, *row)
                                capacity -= 1
                                if capacity == 0:
                                    break
                self.stop.wait(0.08 if inflight else 0.2)
        db.close()

    def run(self):
        logging.basicConfig(level=logging.INFO, format="%(message)s")
        db = connect(self.db_path)
        check = db.execute("PRAGMA integrity_check").fetchone()[0]
        if check != "ok":
            raise RuntimeError(f"SQLite integrity check failed: {check}")
        ensure_reserve(self.db_path)
        db.execute("INSERT INTO meta(key,value) VALUES('mode','offline') "
                   "ON CONFLICT(key) DO UPDATE SET value='offline'")
        threads = [
            threading.Thread(target=Collector(self.db_path, self.mock_host, self.mock_port,
                                              self.stop, self.log).run, daemon=True),
            threading.Thread(target=self.health_loop, daemon=True),
            threading.Thread(target=self.resilient_frame_loop, daemon=True),
            threading.Thread(target=self.upload_loop, args=("online", 4), daemon=True),
            threading.Thread(target=self.upload_loop, args=("offline", 8), daemon=True),
        ]
        for thread in threads:
            thread.start()
        try:
            while all(thread.is_alive() for thread in threads):
                time.sleep(0.3)
        except KeyboardInterrupt:
            pass
        finally:
            self.stop.set()
            for thread in threads:
                thread.join(timeout=5)
            db.close()
