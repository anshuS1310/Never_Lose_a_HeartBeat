import json
import re
import socket
import sqlite3
import time
import zlib

from .reserve import release as release_reserve
from .store import active_session, connect, expected, insert_loss, save_reply, set_active_session

DATA_FIELD = re.compile(r'"data"\s*:\s*')


class CorruptReply(ValueError):
    pass


def parse_reply(raw):
    """Validate the CRC against the literal data array bytes in the TCP reply."""
    match = DATA_FIELD.search(raw)
    if not match:
        raise CorruptReply("missing data array")
    data, end = json.JSONDecoder().raw_decode(raw, match.end())
    if not isinstance(data, list):
        raise CorruptReply("data is not an array")
    try:
        reply = json.loads(raw)
        actual = f"{zlib.crc32(raw[match.end():end].encode('utf-8')) & 0xffffffff:08x}"
        if str(reply["crc32"]).lower() != actual:
            raise CorruptReply("CRC mismatch")
        sensor = int(reply["sensor"])
        if sensor not in (1, 2) or len(data) != int(reply["count"]):
            raise CorruptReply("sensor/count mismatch")
        for values in data:
            if (not isinstance(values, list) or len(values) != (3 if sensor == 1 else 2)
                    or any(type(v) is not int or v < -8388608 or v > 8388607 for v in values)):
                raise CorruptReply("invalid signed 24-bit sample")
        if int(reply["first_index"]) < 0 or not str(reply["session_id"]):
            raise CorruptReply("invalid index/session")
        return reply
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise CorruptReply(str(error)) from error


def accept_reply(db, reply, allow_unreported_gap=False):
    session = str(reply["session_id"])
    sensor = int(reply["sensor"])
    old = active_session(db)
    if old != session:
        db.execute("BEGIN IMMEDIATE")
        try:
            if old:
                previous = db.execute("SELECT next1,next2,ended1,ended2 FROM sessions WHERE id=?", (old,)).fetchone()
                if previous:
                    for s in (1, 2):
                        if not previous[f"ended{s}"]:
                            insert_loss(db, old, s, previous[f"next{s}"], None, "mock_restarted")
                    db.execute("UPDATE sessions SET ended1=1,ended2=1 WHERE id=?", (old,))
            db.execute("INSERT OR IGNORE INTO sessions(id) VALUES(?)", (session,))
            set_active_session(db, session)
            db.execute("COMMIT")
        except BaseException:
            if db.in_transaction:
                db.execute("ROLLBACK")
            raise
    first = int(reply["first_index"])
    count = int(reply["count"])
    cursor = expected(db, session, sensor)
    reported = sorted(reply.get("lost", []), key=lambda item: item["from_index"])
    covers_gap = any(int(item["from_index"]) <= cursor and
                     int(item["to_index"]) >= first - 1 for item in reported)
    if first > cursor and not covers_gap and not allow_unreported_gap:
        return False  # caller re-reads once before recording the gap
    if first + count <= cursor and not reported and not reply.get("session_end"):
        return True
    save_reply(db, session, sensor, first, reply["data"], reported,
               bool(reply.get("session_end")),
               "gap_not_reported_by_mock" if first > cursor and not covers_gap else None)
    return True


class Collector:
    def __init__(self, db_path, host, port, stop_event, logger):
        self.db_path, self.host, self.port = db_path, host, port
        self.stop_event, self.log = stop_event, logger

    def run(self):
        self.db = connect(self.db_path)
        try:
            while not self.stop_event.is_set():
                try:
                    with socket.create_connection((self.host, self.port), timeout=1.2) as sock:
                        sock.settimeout(1.2)
                        reader = sock.makefile("rb")
                        self._read_loop(sock, reader)
                except (OSError, EOFError, ValueError, UnicodeError) as error:
                    self.log("collector_retry", error=str(error))
                    self.stop_event.wait(0.3)
                except sqlite3.OperationalError as error:
                    if "full" in str(error).lower():
                        if release_reserve(self.db_path):
                            self.log("disk_reserve_released", error=str(error))
                        else:
                            self.log("collector_disk_full_retry", error=str(error))
                        self.stop_event.wait(1)
                    else:
                        self.log("collector_storage_fatal", error=str(error))
                        return
        finally:
            self.db.close()

    def _request(self, sock, reader, sensor, from_index=None):
        command = {"cmd": "read", "sensor": sensor}
        if from_index is not None:
            command["from_index"] = from_index
        sock.sendall((json.dumps(command, separators=(",", ":")) + "\n").encode())
        raw = reader.readline(4_000_000)
        if not raw or not raw.endswith(b"\n"):
            raise EOFError("mock closed or oversized reply")
        return parse_reply(raw.decode("utf-8"))

    def _read_loop(self, sock, reader):
        while not self.stop_event.is_set():
            got_data = False
            for sensor in (1, 2):
                session = active_session(self.db)
                cursor = expected(self.db, session, sensor) if session else None
                try:
                    reply = self._request(sock, reader, sensor, cursor)
                except CorruptReply as error:
                    self.log("corrupt_reply", sensor=sensor, error=str(error))
                    reply = self._request(sock, reader, sensor, cursor)
                if reply["sensor"] != sensor:
                    raise CorruptReply("reply sensor differs from request")
                if not accept_reply(self.db, reply):
                    session = active_session(self.db)
                    retry_cursor = expected(self.db, session, sensor)
                    retry = self._request(sock, reader, sensor, retry_cursor)
                    if retry["sensor"] != sensor:
                        raise CorruptReply("re-read sensor differs")
                    accept_reply(self.db, retry, allow_unreported_gap=True)
                    reply = retry
                got_data |= bool(reply["count"] or reply.get("lost"))
            if not got_data:
                self.stop_event.wait(0.15)
