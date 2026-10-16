import os
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from heartbeat.agent import cloud_request

for i in range(5):
    start = time.monotonic()
    try:
        value = cloud_request(os.environ["PIPELINE_CLOUD_URL"], os.environ["PIPELINE_TOKEN"],
                              timeout=10)
        print(i, round(time.monotonic() - start, 3), value)
    except Exception as error:
        print(i, round(time.monotonic() - start, 3), type(error).__name__, error)
