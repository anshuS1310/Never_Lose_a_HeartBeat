"""Exercise CRC re-read, a duplicated reply, and a reported one-sensor loss."""
import json
import sqlite3
import socketserver
import sys
import tempfile
import threading
import time
import zlib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from heartbeat.collector import Collector
from heartbeat.store import connect, read_only


def values(sensor, index):
    factors = (3, (1, 2, 3)) if sensor == 1 else (2, (1, 2))
    return [(factors[0] * index + channel) % 8388608 for channel in factors[1]]


def reply(sensor, first, count, losses=(), ended=False, corrupt=False):
    data = [values(sensor, index) for index in range(first, first + count)]
    literal = json.dumps(data, separators=(",", ":"))
    crc = zlib.crc32(literal.encode()) & 0xffffffff
    if corrupt:
        crc ^= 1
    return ("{" + f'"session_id":"fault123","sensor":{sensor},'
            f'"first_index":{first},"count":{count},'
            f'"lost":{json.dumps(list(losses))},"session_end":{str(ended).lower()},'
            f'"crc32":"{crc:08x}","data":{literal}' + "}\n").encode()


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        while raw := self.rfile.readline():
            request = json.loads(raw)
            sensor = request["sensor"]
            n = self.server.calls[sensor]
            self.server.calls[sensor] += 1
            if sensor == 1:
                if n <= 2:
                    response = reply(1, 0, 400, corrupt=(n == 0))
                elif n == 3:
                    response = reply(1, 400, 400, ended=True)
                else:
                    response = reply(1, 800, 0, ended=True)
            elif n == 0:
                response = reply(2, 0, 400)
            elif n == 1:
                response = reply(2, 500, 300,
                    losses=({"from_index": 400, "to_index": 499,
                             "reason": "sensor_paused"},), ended=True)
            else:
                response = reply(2, 800, 0, ended=True)
            self.wfile.write(response)


class Server(socketserver.TCPServer):
    allow_reuse_address = True


def main():
    with tempfile.TemporaryDirectory() as temp:
        path = Path(temp) / "device.db"
        with Server(("127.0.0.1", 0), Handler) as server:
            server.calls = {1: 0, 2: 0}
            serving = threading.Thread(target=server.serve_forever, daemon=True)
            serving.start()
            stop = threading.Event()
            events = []
            collector = Collector(path, "127.0.0.1", server.server_address[1],
                                  stop, lambda event, **fields: events.append(event))
            thread = threading.Thread(target=collector.run)
            thread.start()
            try:
                deadline = time.monotonic() + 10
                counts = [0, 0]
                while time.monotonic() < deadline:
                    if not path.exists():
                        time.sleep(0.1)
                        continue
                    db = read_only(path)
                    try:
                        counts = [db.execute("SELECT COUNT(*) FROM samples WHERE sensor=?",
                                             (sensor,)).fetchone()[0] for sensor in (1, 2)]
                    except sqlite3.OperationalError as error:
                        if "no such table" not in str(error):
                            raise
                    finally:
                        db.close()
                    if counts == [800, 700]:
                        break
                    time.sleep(0.1)
                assert counts == [800, 700], counts
            finally:
                stop.set()
                thread.join(timeout=3)
                server.shutdown()
                serving.join(timeout=3)
        db = connect(path)
        losses = [tuple(row) for row in db.execute(
            "SELECT sensor,first_idx,last_idx,reason FROM losses")]
        assert losses == [(2, 400, 499, "sensor_paused")], losses
        for sensor, expected in ((1, 800), (2, 700)):
            for row in db.execute("SELECT idx,v1,v2,v3 FROM samples WHERE sensor=?",
                                  (sensor,)):
                actual = [row["v1"], row["v2"]]
                if sensor == 1:
                    actual.append(row["v3"])
                assert actual == values(sensor, row["idx"])
        db.close()
        assert "corrupt_reply" in events, events
        print(json.dumps({"samples": [800, 700], "losses": losses,
                          "corrupt_retried": True, "duplicate_dropped": True}))


if __name__ == "__main__":
    main()
