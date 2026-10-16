import argparse
import json
import os
import sys
import urllib.parse

from .agent import Agent, cloud_request
from .store import read_only


def output(value):
    print(json.dumps(value, separators=(",", ":"), ensure_ascii=False))


def config():
    return (os.environ.get("PIPELINE_DB", "./heartbeat.db"),
            os.environ.get("PIPELINE_CLOUD_URL", ""),
            os.environ.get("PIPELINE_TOKEN", ""))


def device_export(db, session):
    for row in db.execute("SELECT sensor,idx,v1,v2,v3 FROM samples WHERE session=? "
                          "ORDER BY sensor,idx", (session,)):
        values = [row["v1"], row["v2"]]
        if row["sensor"] == 1:
            values.append(row["v3"])
        output({"sensor": row["sensor"], "index": row["idx"], "values": values})
    for row in db.execute("SELECT sensor,first_idx,last_idx,reason FROM losses WHERE session=? "
                          "ORDER BY sensor,first_idx", (session,)):
        output({"sensor": row["sensor"], "from_index": row["first_idx"],
                "to_index": row["last_idx"], "reason": row["reason"]})


def cloud_frames(url, token, session):
    after = -1
    encoded = urllib.parse.quote(session, safe="")
    while True:
        page = cloud_request(url, token, path=f"/frames?session={encoded}&after={after}",
                             timeout=10)["frames"]
        if not page:
            return
        for frame in page:
            yield frame
        after = page[-1]["window"]


def cloud_export(url, token, session):
    losses = []
    for frame in cloud_frames(url, token, session):
        for sensor in (1, 2):
            key = f"sensor{sensor}"
            for offset, values in enumerate(frame[key]):
                if values is not None:
                    output({"sensor": sensor, "index": frame["window"] * 400 + offset,
                            "values": values})
        losses.extend(frame["losses"])
    for loss in losses:
        output(loss)
    encoded = urllib.parse.quote(session, safe="")
    for loss in cloud_request(url, token, path=f"/terminal-loss?session={encoded}",
                              timeout=10)["losses"]:
        output(loss)


def results(db, url, token, session):
    found = {}
    for row in db.execute("SELECT body FROM results WHERE session=?", (session,)):
        result = json.loads(row[0])
        found[(result["window"], result["mode"])] = result
    if url and token:
        try:
            offset = 0
            encoded = urllib.parse.quote(session, safe="")
            while True:
                page = cloud_request(url, token,
                    path=f"/results?session={encoded}&offset={offset}", timeout=10)["results"]
                if not page:
                    break
                for result in page:
                    found[(result["window"], result["mode"])] = result
                offset += len(page)
        except Exception as error:
            print(f"cloud results unavailable: {error}", file=sys.stderr)
    for key in sorted(found):
        output(found[key])


def status(db):
    row = db.execute("SELECT value FROM meta WHERE key='mode'").fetchone()
    mode = row[0] if row else "offline"
    saved = db.execute("SELECT COUNT(*) FROM samples").fetchone()[0]
    cloud = db.execute("SELECT COALESCE(SUM(count1+count2),0) FROM frames WHERE uploaded=1").fetchone()[0]
    output({"mode": mode, "samples_saved": saved, "samples_in_cloud": cloud,
            "backlog": saved - cloud})


def main(argv=None):
    parser = argparse.ArgumentParser(prog="pipeline")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run")
    run.add_argument("--mock-host", default=os.environ.get("PIPELINE_MOCK_HOST", "127.0.0.1"))
    run.add_argument("--mock-port", type=int, default=int(os.environ.get("PIPELINE_MOCK_PORT", 7100)))
    sub.add_parser("status")
    export = sub.add_parser("export")
    export.add_argument("--session", required=True)
    export.add_argument("--from", dest="source", choices=("device", "cloud"), required=True)
    view = sub.add_parser("results")
    view.add_argument("--session", required=True)
    args = parser.parse_args(argv)
    path, url, token = config()
    if args.command == "run":
        Agent(path, args.mock_host, args.mock_port, url, token).run()
        return 0
    if args.command == "export" and args.source == "cloud":
        if not url or not token:
            parser.error("PIPELINE_CLOUD_URL and PIPELINE_TOKEN are required")
        cloud_export(url, token, args.session)
        return 0
    db = read_only(path)
    try:
        if args.command == "status":
            status(db)
        elif args.command == "export":
            device_export(db, args.session)
        else:
            results(db, url, token, args.session)
    finally:
        db.close()
    return 0
