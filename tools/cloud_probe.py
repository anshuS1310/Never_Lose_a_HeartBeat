"""Measure authenticated Worker round-trip time without printing credentials."""
import json
import os
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from heartbeat.agent import cloud_request


def main():
    url, token = os.environ["PIPELINE_CLOUD_URL"], os.environ["PIPELINE_TOKEN"]
    durations = []
    for _ in range(8):
        started = time.monotonic()
        cloud_request(url, token, timeout=10)
        durations.append(time.monotonic() - started)
    print(json.dumps({"requests": len(durations),
                      "min_seconds": round(min(durations), 3),
                      "median_seconds": round(statistics.median(durations), 3),
                      "max_seconds": round(max(durations), 3)}))


if __name__ == "__main__":
    main()
