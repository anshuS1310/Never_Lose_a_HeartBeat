"""Check a synthetic ramp export streamed on stdin."""
import argparse
import json
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected", type=int, required=True,
                        help="expected samples per sensor")
    args = parser.parse_args()
    counts = {1: 0, 2: 0}
    losses = []
    for line in sys.stdin:
        row = json.loads(line)
        sensor = row["sensor"]
        if sensor not in counts:
            raise AssertionError(f"invalid sensor: {sensor}")
        if "index" not in row:
            losses.append(row)
            continue
        index = row["index"]
        if index != counts[sensor]:
            raise AssertionError(f"missing, duplicate or out-of-order {sensor}:{index}; "
                                 f"expected {counts[sensor]}")
        expected = ([(3 * index + c) % 8388608 for c in (1, 2, 3)]
                    if sensor == 1 else
                    [(2 * index + c) % 8388608 for c in (1, 2)])
        if row["values"] != expected:
            raise AssertionError(f"wrong value: {sensor}:{index}")
        counts[sensor] += 1
    if counts != {1: args.expected, 2: args.expected} or losses:
        raise AssertionError(f"incomplete export: {counts=}, {losses=}")
    print(json.dumps({"sensor_1": counts[1], "sensor_2": counts[2],
                      "loss_records": len(losses), "status": "ok"}))


if __name__ == "__main__":
    main()
