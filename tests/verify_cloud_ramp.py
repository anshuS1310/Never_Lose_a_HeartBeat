"""Verify every cloud frame against the development ramp formula."""
import argparse
import json
import os
import sys
import urllib.parse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from heartbeat.agent import cloud_request


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--session", required=True)
    parser.add_argument("--windows", type=int, required=True)
    args = parser.parse_args()
    url = os.environ["PIPELINE_CLOUD_URL"]
    token = os.environ["PIPELINE_TOKEN"]
    session = urllib.parse.quote(args.session, safe="")
    next_window = 0
    after = -1
    while True:
        page = cloud_request(url, token,
                             path=f"/frames?session={session}&after={after}",
                             timeout=30)["frames"]
        if not page:
            break
        for frame in page:
            window = frame["window"]
            if window != next_window or frame["losses"]:
                raise AssertionError(f"unexpected frame or loss at window {next_window}")
            for sensor in (1, 2):
                rows = frame[f"sensor{sensor}"]
                if len(rows) != 400:
                    raise AssertionError(f"wrong sample count in window {window}")
                factor, channels = ((3, 3) if sensor == 1 else (2, 2))
                for offset, actual in enumerate(rows):
                    index = window * 400 + offset
                    expected = [(factor * index + channel) % 8388608
                                for channel in range(1, channels + 1)]
                    if actual != expected:
                        raise AssertionError(f"wrong value at sensor {sensor}, index {index}")
            next_window += 1
        after = page[-1]["window"]
    terminal = cloud_request(url, token,
                             path=f"/terminal-loss?session={session}", timeout=30)["losses"]
    if next_window != args.windows or terminal:
        raise AssertionError(f"incomplete cloud copy: {next_window} windows, {terminal} losses")
    print(json.dumps({"session": args.session, "windows": next_window,
                      "samples_per_sensor": next_window * 400,
                      "loss_records": 0, "status": "ok"}))


if __name__ == "__main__":
    main()
