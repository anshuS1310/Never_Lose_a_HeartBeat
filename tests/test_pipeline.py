import json
import shutil
import os
import subprocess
import tempfile
import threading
import time
import unittest
import zlib
from pathlib import Path

from heartbeat.collector import CorruptReply, accept_reply, parse_reply
from heartbeat.agent import Agent
from heartbeat.processing import compute_window, scan_peaks
from heartbeat.store import connect, frame_payload, get_rows, read_only, save_reply

ROOT = Path(__file__).resolve().parents[1]


class CollectorTests(unittest.TestCase):
    def test_crc_uses_literal_data_text(self):
        data = "[[1, 2, 3], [4, 5, 6]]"
        reply = (f'{{"session_id":"abc","sensor":1,"first_index":0,"count":2,'
                 f'"lost":[],"session_end":false,"crc32":"{zlib.crc32(data.encode()):08x}",'
                 f'"data":{data}}}')
        self.assertEqual(parse_reply(reply)["data"][1], [4, 5, 6])
        with self.assertRaises(CorruptReply):
            parse_reply(reply.replace("[4, 5, 6]", "[4, 5, 7]"))

    def test_durable_gap_and_duplicate(self):
        with tempfile.TemporaryDirectory() as temp:
            db = connect(Path(temp) / "device.db")
            base = {"session_id": "s", "sensor": 1, "first_index": 0,
                    "count": 2, "data": [[1, 2, 3], [4, 5, 6]],
                    "lost": [], "session_end": False}
            self.assertTrue(accept_reply(db, base))
            self.assertTrue(accept_reply(db, base))
            self.assertEqual(db.execute("SELECT COUNT(*) FROM samples").fetchone()[0], 2)
            next_reply = {**base, "first_index": 4, "count": 1,
                          "data": [[7, 8, 9]], "lost": [{"from_index": 2,
                          "to_index": 3, "reason": "buffer_overflow"}]}
            self.assertTrue(accept_reply(db, next_reply))
            self.assertEqual(db.execute("SELECT first_idx,last_idx,reason FROM losses").fetchone()[:],
                             (2, 3, "buffer_overflow"))
            self.assertEqual(get_rows(db, "s", 1, 0, 4),
                             [[1, 2, 3], [4, 5, 6], None, None, [7, 8, 9]])
            db.close()
            reopened = connect(Path(temp) / "device.db")
            self.assertEqual(reopened.execute("SELECT COUNT(*) FROM samples").fetchone()[0], 3)
            reopened.close()
            reader = read_only(Path(temp) / "device.db")
            self.assertEqual(reader.execute("SELECT COUNT(*) FROM samples").fetchone()[0], 3)
            with self.assertRaises(Exception):
                reader.execute("INSERT INTO sessions(id) VALUES('forbidden')")
            reader.close()

    def test_mock_restart_finishes_prior_session_window(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "device.db"
            db = connect(path)
            for sensor in (1, 2):
                values = ([1, 2, 3] if sensor == 1 else [4, 5])
                count = 400 if sensor == 1 else 300
                self.assertTrue(accept_reply(db, {
                    "session_id": "old", "sensor": sensor, "first_index": 0,
                    "count": count, "data": [values] * count,
                    "lost": [], "session_end": False}))
            self.assertTrue(accept_reply(db, {
                "session_id": "new", "sensor": 1, "first_index": 0,
                "count": 1, "data": [[7, 8, 9]],
                "lost": [], "session_end": False}))
            ended = db.execute("SELECT ended1,ended2 FROM sessions WHERE id='old'").fetchone()
            self.assertEqual(tuple(ended), (1, 1))
            db.close()

            agent = Agent(path, "127.0.0.1", 7100, "", "")
            thread = threading.Thread(target=agent.frame_loop)
            thread.start()
            deadline = time.monotonic() + 5
            try:
                while time.monotonic() < deadline:
                    db = connect(path)
                    count = db.execute("SELECT COUNT(*) FROM frames WHERE session='old'").fetchone()[0]
                    result = db.execute("SELECT COUNT(*) FROM results WHERE session='old'").fetchone()[0]
                    db.close()
                    if count == 1 and result == 1:
                        break
                    time.sleep(0.05)
                self.assertEqual((count, result), (1, 1))
            finally:
                agent.stop.set()
                thread.join(timeout=5)
            db = connect(path)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM losses WHERE session='old' "
                                        "AND last_idx IS NULL").fetchone()[0], 2)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM losses WHERE session='old' "
                                        "AND reason='sensor_ended'").fetchone()[0], 0)
            wire = db.execute("SELECT body FROM frame_wire WHERE session='old' AND window=0").fetchone()
            self.assertIsNotNone(wire)
            payload = json.loads(wire[0])
            self.assertEqual(payload["sensor1"][0], [1, 2, 3])
            self.assertEqual(payload["sensor2"][299], [4, 5])
            self.assertEqual(payload["sensor2"][300:], [None] * 100)
            db.close()


class ProcessingTests(unittest.TestCase):
    def fixture(self):
        one = [[0, 0, 0] for _ in range(2801)]
        for index in range(0, 2800, 400):
            one[index] = [0, 1000, 0]
        two = [[3, 4] for _ in range(400)]
        return one, two

    def test_peaks_rate_and_missing(self):
        one, two = self.fixture()
        result = compute_window(6, one[2400:2800], two, one, 0, None, "offline")
        self.assertEqual(result["heart_rate"], 60.0)
        self.assertEqual(result["magnitude"], 5.0)
        self.assertTrue(result["complete"])
        scalar = [row[1] if row is not None else None for row in one]
        self.assertEqual(compute_window(6, one[2400:2800], two, scalar,
                                        0, None, "offline"), result)
        one[1800] = None
        result = compute_window(6, one[2400:2800], two, one, 0, None, "offline")
        self.assertIsNone(result["heart_rate"])

    def test_heart_rate_90_and_72(self):
        two = [[3, 4] for _ in range(400)]
        for expected_rate in (90, 72):
            with self.subTest(rate=expected_rate):
                one = [[0, 0, 0] for _ in range(2801)]
                gap = 400 * 60 / expected_rate
                for beat in range(12):
                    index = round(800 + beat * gap)
                    if 800 <= index <= 3600:
                        one[index - 800] = [0, 1000, 0]
                result = compute_window(8, one[2400:2800], two, one, 800,
                                        None, "offline")
                self.assertLess(abs(result["heart_rate"] - expected_rate), 0.2)

    @unittest.skipUnless(shutil.which("node"), "Node.js is only needed for Worker parity")
    def test_cloud_python_parity(self):
        one, two = self.fixture()
        fixture = {"window": 6, "sensor1": one[2400:2800], "sensor2": two,
                   "context1": one, "context_start": 0, "previous_peak": None}
        expected = compute_window(6, fixture["sensor1"], two, one, 0, None, "online")
        script = ("import {computeWindow} from './cloud/src/processing.js';"
                  "let s='';for await (const c of process.stdin) s+=c;"
                  "const f=JSON.parse(s);console.log(JSON.stringify(computeWindow("
                  "f.window,f.sensor1,f.sensor2,f.context1,f.context_start,f.previous_peak,'online')));")
        completed = subprocess.run(["node", "--input-type=module", "-e", script],
                                   cwd=ROOT, input=json.dumps(fixture), text=True,
                                   capture_output=True, check=True)
        self.assertEqual(json.loads(completed.stdout), expected)
        fixture["context1"] = [row[1] if row is not None else None for row in one]
        compact = subprocess.run(["node", "--input-type=module", "-e", script],
                                 cwd=ROOT, input=json.dumps(fixture), text=True,
                                 capture_output=True, check=True)
        self.assertEqual(json.loads(compact.stdout), expected)


if __name__ == "__main__":
    unittest.main()
