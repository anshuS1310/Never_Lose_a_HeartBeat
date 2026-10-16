#!/usr/bin/env python3
"""Synthetic TCP sensor simulator for local development and repeatable tests."""
import argparse
import json
import math
from pathlib import Path
import socketserver
import time
import uuid
import zlib


def sample(sensor, index, mode):
    if mode == "ramp":
        if sensor == 1:
            return [(3 * index + c) % 8388608 for c in (1, 2, 3)]
        return [(2 * index + c) % 8388608 for c in (1, 2)]
    minute = (index // (400 * 60)) % 6
    bpm = 60 if minute < 2 else (90 if minute < 4 else 72)
    beat = round(400 * 60 / bpm)
    phase = index % beat
    ecg = 100 + max(0, 1800 - min(phase, beat - phase) * 380)
    if sensor == 1:
        return [round(ecg * 0.6), ecg, round(ecg * 1.3)]
    return [round(400000 + 10000 * math.sin(index / 20000)),
            round(150000 + 5000 * math.cos(index / 13000))]


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        state = self.server.state
        while raw := self.rfile.readline():
            try:
                request = json.loads(raw)
                if request.get("cmd") != "read" or request.get("sensor") not in (1, 2):
                    return
                sensor = request["sensor"]
                now = min(state["duration"] * 400,
                          int((time.monotonic() - state["started"]) * 400))
                earliest = max(0, now - 4000)
                cursor = request.get("from_index", state["cursor"][sensor])
                losses = []
                if cursor < earliest:
                    losses = [{"from_index": cursor, "to_index": earliest - 1,
                               "reason": "buffer_overflow"}]
                    cursor = earliest
                first = cursor
                last = min(now, first + 400)
                data = [sample(sensor, i, state["mode"]) for i in range(first, last)]
                state["cursor"][sensor] = last
                data_text = json.dumps(data, separators=(",", ":"))
                crc = f"{zlib.crc32(data_text.encode()) & 0xffffffff:08x}"
                response = ("{" + f'"session_id":"{state["session"]}",'
                            f'"sensor":{sensor},"first_index":{first},'
                            f'"count":{len(data)},"lost":{json.dumps(losses)},'
                            f'"session_end":{str(last >= state["duration"] * 400).lower()},'
                            f'"crc32":"{crc}","data":{data_text}' + "}\n")
                self.wfile.write(response.encode())
            except (ValueError, BrokenPipeError, ConnectionResetError):
                return


class ReusableTCPServer(socketserver.TCPServer):
    allow_reuse_address = True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("ramp", "signal"), default="ramp")
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--port", type=int, default=7100)
    parser.add_argument("--start-file", help="write startup UNIX nanoseconds for latency tests")
    args = parser.parse_args()
    with ReusableTCPServer(("127.0.0.1", args.port), Handler) as server:
        server.state = {"session": uuid.uuid4().hex[:8], "cursor": {1: 0, 2: 0},
                        "duration": args.duration, "mode": args.mode,
                        "started": time.monotonic()}
        if args.start_file:
            Path(args.start_file).write_text(str(time.time_ns()), encoding="ascii")
        print(server.state["session"], flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()
