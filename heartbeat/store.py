import json
import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY, next1 INTEGER NOT NULL DEFAULT 0, next2 INTEGER NOT NULL DEFAULT 0,
  ended1 INTEGER NOT NULL DEFAULT 0, ended2 INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS samples (
  session TEXT NOT NULL, sensor INTEGER NOT NULL, idx INTEGER NOT NULL,
  v1 INTEGER NOT NULL, v2 INTEGER NOT NULL, v3 INTEGER,
  PRIMARY KEY(session,sensor,idx)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS losses (
  session TEXT NOT NULL, sensor INTEGER NOT NULL, first_idx INTEGER NOT NULL,
  last_idx INTEGER, reason TEXT NOT NULL,
  PRIMARY KEY(session,sensor,first_idx,reason)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS frames (
  session TEXT NOT NULL, window INTEGER NOT NULL, mode TEXT NOT NULL,
  count1 INTEGER NOT NULL, count2 INTEGER NOT NULL,
  uploaded INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(session,window)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS frame_wire (
  session TEXT NOT NULL, window INTEGER NOT NULL, body TEXT NOT NULL,
  PRIMARY KEY(session,window)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS results (
  session TEXT NOT NULL, window INTEGER NOT NULL, mode TEXT NOT NULL,
  body TEXT NOT NULL, synced INTEGER NOT NULL DEFAULT 0,
  processed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY(session,window,mode)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS peak_state (
  session TEXT PRIMARY KEY, scanned_through INTEGER NOT NULL DEFAULT 799,
  previous_peak INTEGER
);
CREATE TABLE IF NOT EXISTS peaks (
  session TEXT NOT NULL, idx INTEGER NOT NULL, PRIMARY KEY(session,idx)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS terminal_sync (
  session TEXT NOT NULL, sensor INTEGER NOT NULL, first_idx INTEGER NOT NULL,
  reason TEXT NOT NULL, PRIMARY KEY(session,sensor,first_idx,reason)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""


def connect(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=10, isolation_level=None)
    db.row_factory = sqlite3.Row
    try:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        db.execute("PRAGMA busy_timeout=10000")
        db.executescript(SCHEMA)
    except BaseException:
        db.close()
        raise
    return db


def read_only(path):
    """Open an existing database for CLI queries without requiring write access."""
    uri = Path(path).resolve().as_uri() + "?mode=ro"
    db = sqlite3.connect(uri, uri=True, timeout=10, isolation_level=None)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=10000")
    db.execute("PRAGMA query_only=ON")
    return db


def active_session(db):
    row = db.execute("SELECT value FROM meta WHERE key='active_session'").fetchone()
    return row[0] if row else None


def set_active_session(db, session):
    db.execute("INSERT INTO meta(key,value) VALUES('active_session',?) "
               "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (session,))


def expected(db, session, sensor):
    row = db.execute(f"SELECT next{sensor} FROM sessions WHERE id=?", (session,)).fetchone()
    return row[0] if row else 0


def insert_loss(db, session, sensor, first, last, reason):
    if last is not None and last < first:
        return
    db.execute("INSERT OR IGNORE INTO losses VALUES(?,?,?,?,?)",
               (session, sensor, first, last, reason))


def save_reply(db, session, sensor, first, data, losses, session_end=False,
               gap_reason=None):
    """The entire verified reply and cursor are one durable transaction."""
    cursor = expected(db, session, sensor)
    db.execute("BEGIN IMMEDIATE")
    try:
        db.execute("INSERT OR IGNORE INTO sessions(id) VALUES(?)", (session,))
        events = [(int(loss["from_index"]), "loss", loss) for loss in losses]
        events.extend((first + offset, "sample", values) for offset, values in enumerate(data))
        events.sort(key=lambda item: (item[0], item[1] != "sample"))
        rows = []
        for index, kind, item in events:
            if kind == "loss":
                hi = int(item["to_index"])
                if hi < cursor:
                    continue
                if index > cursor:
                    insert_loss(db, session, sensor, cursor, index - 1,
                                gap_reason or "gap_not_reported_by_mock")
                lo = max(index, cursor)
                insert_loss(db, session, sensor, lo, hi,
                            str(item.get("reason", "mock_reported_loss")))
                cursor = hi + 1
            else:
                if index < cursor:
                    continue
                if index > cursor:
                    insert_loss(db, session, sensor, cursor, index - 1,
                                gap_reason or "gap_not_reported_by_mock")
                values = item
                rows.append((session, sensor, index, values[0], values[1],
                             values[2] if sensor == 1 else None))
                cursor = index + 1
        if not events and first > cursor and gap_reason:
            insert_loss(db, session, sensor, cursor, first - 1, gap_reason)
            cursor = first
        db.executemany("INSERT OR IGNORE INTO samples VALUES(?,?,?,?,?,?)", rows)
        db.execute(f"UPDATE sessions SET next{sensor}=?, ended{sensor}=MAX(ended{sensor},?) WHERE id=?",
                   (cursor, int(session_end), session))
        db.execute("COMMIT")
    except BaseException:
        if db.in_transaction:
            db.execute("ROLLBACK")
        raise


def get_rows(db, session, sensor, first, last):
    result = [None] * (last - first + 1)
    for row in db.execute("SELECT idx,v1,v2,v3 FROM samples WHERE session=? AND sensor=? "
                          "AND idx BETWEEN ? AND ? ORDER BY idx",
                          (session, sensor, first, last)):
        result[row["idx"] - first] = ([row["v1"], row["v2"], row["v3"]]
                                       if sensor == 1 else [row["v1"], row["v2"]])
    return result


def get_ecg_values(db, session, first, last):
    result = [None] * (last - first + 1)
    for row in db.execute("SELECT idx,v2 FROM samples WHERE session=? AND sensor=1 "
                          "AND idx BETWEEN ? AND ? ORDER BY idx", (session, first, last)):
        result[row["idx"] - first] = row["v2"]
    return result


def frame_payload(db, session, window, mode, include_context=True):
    first, last = window * 400, window * 400 + 399
    one = get_rows(db, session, 1, first, last)
    two = get_rows(db, session, 2, first, last)
    losses = []
    for row in db.execute("SELECT sensor,first_idx,last_idx,reason FROM losses WHERE session=? "
                          "AND first_idx<=? AND (last_idx IS NULL OR last_idx>=?) "
                          "ORDER BY sensor,first_idx", (session, last, first)):
        if row["last_idx"] is None:
            continue  # open-ended terminal loss is sent separately
        losses.append({"sensor": row["sensor"], "from_index": max(first, row["first_idx"]),
                       "to_index": min(last, row["last_idx"]), "reason": row["reason"]})
    payload = {"session": session, "window": window, "mode": mode,
               "sensor1": one, "sensor2": two, "losses": losses}
    if window >= 4 and include_context:
        context_start = (window - 6) * 400
        payload["context_start"] = context_start
        payload["context1"] = (get_ecg_values(db, session, context_start, last + 1)
                               if mode == "online" else
                               get_rows(db, session, 1, context_start, last + 1))
        previous = db.execute("SELECT MAX(idx) FROM peaks WHERE session=? AND idx<?",
                              (session, (window - 4) * 400)).fetchone()[0]
        payload["previous_peak"] = previous
    return payload


def save_result(db, session, result):
    db.execute("INSERT OR REPLACE INTO results(session,window,mode,body) VALUES(?,?,?,?)",
               (session, result["window"], result["mode"], json.dumps(result, separators=(",", ":"))))
