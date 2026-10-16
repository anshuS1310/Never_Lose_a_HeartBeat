"""Check a development-mock one-hour run after the resource monitor finishes."""
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from heartbeat.agent import cloud_request
from heartbeat.store import read_only


def main():
    session, db_path = sys.argv[1:3]
    expected = 3600 * 400
    deadline = time.monotonic() + 180
    while True:
        db = read_only(db_path)
        counts = [db.execute("SELECT COUNT(*) FROM samples WHERE session=? AND sensor=?",
                             (session, sensor)).fetchone()[0] for sensor in (1, 2)]
        frames = db.execute("SELECT COUNT(*) FROM frames WHERE session=?", (session,)).fetchone()[0]
        uploaded = db.execute("SELECT COUNT(*) FROM frames WHERE session=? AND uploaded=1",
                              (session,)).fetchone()[0]
        losses = db.execute("SELECT COUNT(*) FROM losses WHERE session=?", (session,)).fetchone()[0]
        offline_windows = [row[0] for row in db.execute(
            "SELECT window FROM frames WHERE session=? AND mode='offline' ORDER BY window",
            (session,))]
        db.close()
        if counts == [expected, expected] and frames == 3600 and uploaded == 3600:
            break
        if time.monotonic() >= deadline:
            raise AssertionError(f"hour incomplete: {counts=}, {frames=}, {uploaded=}, {losses=}")
        time.sleep(2)
    results = []
    while True:
        page = cloud_request(os.environ["PIPELINE_CLOUD_URL"], os.environ["PIPELINE_TOKEN"],
            path=f"/results?session={session}&offset={len(results)}", timeout=15)["results"]
        results.extend(page)
        if len(page) < 100:
            break
    online = [item for item in results if item["mode"] == "online"]
    cloud_windows = {item["window"] for item in results}
    rates = {str(window): next((item["heart_rate"] for item in online
                                if item["window"] == window), None)
             for window in (30, 150, 270)}
    mock_start = Path(db_path).parent.joinpath("mock.log").stat().st_mtime
    delays = sorted(datetime.fromisoformat(item["processed_at"]).replace(
        tzinfo=timezone.utc).timestamp() - mock_start - item["window"] - 1
        for item in online)
    report = {"session": session, "samples": counts, "frames": frames,
              "uploaded": uploaded, "loss_records": losses,
              "offline_startup_windows": offline_windows,
              "cloud_result_modes": dict(Counter(item["mode"] for item in results)),
              "cloud_window_coverage": len(cloud_windows),
              "representative_heart_rates": rates,
              "estimated_cloud_delay_seconds": {
                  "min": round(delays[0], 3) if delays else None,
                  "p95": round(delays[int(0.95 * (len(delays) - 1))], 3) if delays else None,
                  "max": round(delays[-1], 3) if delays else None,
                  "over_5_seconds": sum(value > 5 for value in delays)},
              "latency_note": "Guest wall clock drifted under QEMU; cloud timestamp delays are estimates."}
    print(json.dumps(report))
    if losses or cloud_windows != set(range(3600)) or len(results) != 3600:
        raise AssertionError("loss or incomplete cloud window coverage in hour run")
    for window, expected_rate in ((30, 60), (150, 90), (270, 72)):
        actual = rates[str(window)]
        if actual is None or abs(actual - expected_rate) > 0.2:
            raise AssertionError(f"heart rate for window {window}: {actual}, expected {expected_rate}")


if __name__ == "__main__":
    main()
