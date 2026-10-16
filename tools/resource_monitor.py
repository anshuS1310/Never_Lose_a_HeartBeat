#!/usr/bin/env python3
"""Measure CPU, memory, disk writes, and network transfer on Linux."""
import argparse
import json
import os
import time


def process(pid):
    stat = open(f"/proc/{pid}/stat", encoding="utf-8").read()
    fields = stat[stat.rfind(")") + 2:].split()
    cpu_ticks = int(fields[11]) + int(fields[12])
    status = open(f"/proc/{pid}/status", encoding="utf-8").read().splitlines()
    rss_kib = next(int(line.split()[1]) for line in status if line.startswith("VmRSS:"))
    io = open(f"/proc/{pid}/io", encoding="utf-8").read().splitlines()
    written = next(int(line.split()[1]) for line in io if line.startswith("write_bytes:"))
    return cpu_ticks, rss_kib, written


def sent_bytes(interface):
    for line in open("/proc/net/dev", encoding="utf-8"):
        if line.strip().startswith(interface + ":"):
            return int(line.split(":", 1)[1].split()[8])
    raise ValueError(f"interface not found: {interface}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pid", type=int, action="append", required=True,
                        help="repeat for pipeline and mock PIDs")
    parser.add_argument("--interface", required=True)
    parser.add_argument("--seconds", type=int, default=3600)
    parser.add_argument("--interval", type=float, default=2.0)
    args = parser.parse_args()
    ticks_per_second = os.sysconf("SC_CLK_TCK")
    memory_kib = next(int(line.split()[1]) for line in
                      open("/proc/meminfo", encoding="utf-8")
                      if line.startswith("MemTotal:"))
    started = time.monotonic()
    initial = [process(pid) for pid in args.pid]
    network_initial = sent_bytes(args.interface)
    peak_rss = sum(item[1] for item in initial)
    while time.monotonic() - started < args.seconds:
        time.sleep(max(0, min(args.interval, args.seconds - (time.monotonic() - started))))
        current = [process(pid) for pid in args.pid]
        peak_rss = max(peak_rss, sum(item[1] for item in current))
    elapsed = time.monotonic() - started
    network_final = sent_bytes(args.interface)
    cpu_ticks = sum(end[0] - begin[0] for begin, end in zip(initial, current))
    written = sum(end[2] - begin[2] for begin, end in zip(initial, current))
    print(json.dumps({"environment": {"architecture": os.uname().machine,
        "cpu_count": os.cpu_count(), "memory_limit_bytes": memory_kib * 1024},
        "duration_seconds": round(elapsed, 2), "pids": args.pid,
        "average_cpu_percent_one_core": round(100 * cpu_ticks / ticks_per_second / elapsed, 2),
        "average_cpu_percent_device": round(100 * cpu_ticks / ticks_per_second /
                                            elapsed / os.cpu_count(), 2),
        "peak_memory_mb": round(peak_rss / 1024, 2),
        "disk_written_mb": round(written / 1_000_000, 2),
        "network_sent_mb": round((network_final - network_initial) / 1_000_000, 2),
        "network_interface": args.interface}, separators=(",", ":")))


if __name__ == "__main__":
    main()
