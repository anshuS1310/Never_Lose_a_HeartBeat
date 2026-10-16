"""Verify saved ramp samples and SQLite integrity after an abrupt VM stop."""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from heartbeat.store import connect


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("db")
    parser.add_argument("--old-session")
    args = parser.parse_args()
    db = connect(args.db)
    integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
    if integrity != "ok":
        raise AssertionError(f"SQLite integrity: {integrity}")
    db.execute("BEGIN")  # one consistent snapshot while a live pipeline may be writing
    sessions = []
    for session in db.execute("SELECT id,next1,next2 FROM sessions ORDER BY created_at,id"):
        sid = session["id"]
        counts = []
        for sensor in (1, 2):
            seen = set()
            for row in db.execute("SELECT idx,v1,v2,v3 FROM samples WHERE session=? AND sensor=?",
                                  (sid, sensor)):
                index = row["idx"]
                if index in seen:
                    raise AssertionError(f"duplicate index: {sid}/{sensor}/{index}")
                seen.add(index)
                expected = ([(3 * index + c) % 8388608 for c in (1, 2, 3)]
                            if sensor == 1 else
                            [(2 * index + c) % 8388608 for c in (1, 2)])
                actual = [row["v1"], row["v2"]]
                if sensor == 1:
                    actual.append(row["v3"])
                if actual != expected:
                    raise AssertionError(f"wrong ramp data: {sid}/{sensor}/{index}")
            counts.append(len(seen))
        losses = [dict(row) for row in db.execute(
            "SELECT sensor,first_idx AS from_index,last_idx AS to_index,reason "
            "FROM losses WHERE session=? ORDER BY sensor,first_idx", (sid,))]
        sessions.append({"session": sid, "samples": counts,
                         "cursors": [session["next1"], session["next2"]],
                         "losses": losses})
    db.execute("COMMIT")
    db.close()
    if args.old_session:
        old = next((item for item in sessions if item["session"] == args.old_session), None)
        if old is None or not all(any(loss["sensor"] == sensor and
                                   loss["reason"] == "mock_restarted" and
                                   loss["to_index"] is None
                                   for loss in old["losses"]) for sensor in (1, 2)):
            raise AssertionError("old session lacks open-ended restart loss on both sensors")
    print(json.dumps({"integrity": integrity, "sessions": sessions}))


if __name__ == "__main__":
    main()
