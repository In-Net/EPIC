#!/usr/bin/env python3
import argparse
import csv
import math
import sys


HOSTS = 4


def read_source(path):
    rows = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            primitive = row["primitive"]
            if primitive not in {"reduce", "broadcast"}:
                continue
            try:
                message_bytes = int(row["message_bytes"])
                mode = int(row["mode"])
                mbps = float(row.get("best_mbps") or row.get("app_mbps"))
            except (KeyError, TypeError, ValueError):
                continue
            if not math.isfinite(mbps) or mbps <= 0.0:
                continue
            rows[(primitive, mode, message_bytes)] = row
    return rows


def size_label(n):
    units = [("GB", 1024 ** 3), ("MB", 1024 ** 2), ("KB", 1024)]
    for suffix, scale in units:
        if n >= scale and n % scale == 0:
            return f"{n // scale}{suffix}"
    return f"{n}B"


def emit_derived(source_rows, writer):
    mapping = {
        "reducescatter": "reduce",
        "allgather": "broadcast",
    }

    for derived, source_primitive in mapping.items():
        for mode in (2, 3):
            candidates = [
                (sub_bytes, row)
                for (primitive, row_mode, sub_bytes), row in source_rows.items()
                if primitive == source_primitive and row_mode == mode
            ]
            for sub_bytes, row in sorted(candidates):
                source_mbps = float(row.get("best_mbps") or row.get("app_mbps"))
                source_latency_us = sub_bytes * 8.0 / source_mbps
                logical_bytes = sub_bytes * HOSTS
                total_latency_us = source_latency_us * HOSTS
                derived_mbps = logical_bytes * 8.0 / total_latency_us
                writer.writerow({
                    "logical_message_bytes": logical_bytes,
                    "size": size_label(logical_bytes),
                    "primitive": derived,
                    "mode": mode,
                    "derived_mbps": f"{derived_mbps:.1f}",
                    "rounds": HOSTS,
                    "derived_from": source_primitive,
                    "sub_message_bytes": sub_bytes,
                    "sub_size": size_label(sub_bytes),
                    "source_mbps": f"{source_mbps:.1f}",
                    "source_latency_us": f"{source_latency_us:.3f}",
                    "total_latency_us": f"{total_latency_us:.3f}",
                    "segment_window": row.get("segment_window", ""),
                    "ok_runs": row.get("ok_runs", ""),
                    "total_runs": row.get("total_runs", ""),
                    "recv_window": row.get("recv_window", ""),
                    "payload_bytes": row.get("payload_bytes", ""),
                    "data_mtu": row.get("data_mtu", ""),
                    "best_log_dir": row.get("best_log_dir") or row.get("log_dir", ""),
                    "status": "derived",
                })


def emit_missing(source_rows, writer, requested_sizes):
    for logical_bytes in requested_sizes:
        if logical_bytes % HOSTS != 0:
            continue
        sub_bytes = logical_bytes // HOSTS
        for derived, source_primitive in (
            ("reducescatter", "reduce"),
            ("allgather", "broadcast"),
        ):
            for mode in (2, 3):
                if (source_primitive, mode, sub_bytes) in source_rows:
                    continue
                writer.writerow({
                    "logical_message_bytes": logical_bytes,
                    "size": size_label(logical_bytes),
                    "primitive": derived,
                    "mode": mode,
                    "derived_mbps": "nan",
                    "rounds": HOSTS,
                    "derived_from": source_primitive,
                    "sub_message_bytes": sub_bytes,
                    "sub_size": size_label(sub_bytes),
                    "source_mbps": "nan",
                    "source_latency_us": "nan",
                    "total_latency_us": "nan",
                    "segment_window": "",
                    "ok_runs": "",
                    "total_runs": "",
                    "recv_window": "",
                    "payload_bytes": "",
                    "data_mtu": "",
                    "best_log_dir": "",
                    "status": "missing_subcall",
                })


def parse_sizes(text):
    if not text:
        return []
    return [int(x) for x in text.replace(",", " ").split()]


def main():
    ap = argparse.ArgumentParser(
        description="Derive ReduceScatter and AllGather from Reduce/Broadcast rows."
    )
    ap.add_argument("source_csv", help="EPIC best CSV with reduce/broadcast rows")
    ap.add_argument(
        "--requested-sizes",
        default="4096 16384 65536 262144 1048576 4194304 16777216 67108864 268435456 1073741824",
        help="logical message sizes to mark as missing when the subcall row is absent",
    )
    args = ap.parse_args()

    fields = [
        "logical_message_bytes",
        "size",
        "primitive",
        "mode",
        "derived_mbps",
        "rounds",
        "derived_from",
        "sub_message_bytes",
        "sub_size",
        "source_mbps",
        "source_latency_us",
        "total_latency_us",
        "segment_window",
        "ok_runs",
        "total_runs",
        "recv_window",
        "payload_bytes",
        "data_mtu",
        "best_log_dir",
        "status",
    ]
    writer = csv.DictWriter(sys.stdout, fields)
    writer.writeheader()

    source_rows = read_source(args.source_csv)
    emit_missing(source_rows, writer, parse_sizes(args.requested_sizes))
    emit_derived(source_rows, writer)


if __name__ == "__main__":
    main()
