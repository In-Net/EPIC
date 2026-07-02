#!/usr/bin/env python3
import argparse
import csv
import sys


def parse_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def is_ok(row):
    status = row.get("status", "")
    return status in ("", "ok")


def main():
    ap = argparse.ArgumentParser(
        description="Normalize collective bandwidth by the pure transport baseline."
    )
    ap.add_argument("combined_csv", help="combined_bandwidth_best.csv")
    ap.add_argument(
        "--include-mpi-ucx",
        action="store_true",
        help="also include MPI UCX normalized by pure RoCE",
    )
    args = ap.parse_args()

    with open(args.combined_csv, newline="") as f:
        rows = list(csv.DictReader(f))

    pure = {}
    for row in rows:
        if row.get("kind") != "pure" or row.get("primitive") != "pingpong":
            continue
        if not is_ok(row):
            continue
        transport = row.get("mode_or_transport", "")
        mbps = parse_float(row.get("mbps"))
        if mbps is None or mbps <= 0:
            continue
        key = (row["message_bytes"], transport)
        if key not in pure or mbps > pure[key]:
            pure[key] = mbps

    fields = [
        "message_bytes",
        "primitive",
        "system",
        "mode_or_transport",
        "mbps",
        "pure_transport",
        "pure_mbps",
        "utilization_pct",
        "log_dir",
    ]
    writer = csv.DictWriter(sys.stdout, fields)
    writer.writeheader()

    out = []
    for row in rows:
        if not is_ok(row):
            continue
        kind = row.get("kind", "")
        mode = row.get("mode_or_transport", "")
        if kind == "epic":
            system = "epic"
            pure_transport = "roce"
        elif kind == "mpi" and mode == "tcp":
            system = "mpi_tcp"
            pure_transport = "tcp"
        elif kind == "mpi" and mode == "ucx" and args.include_mpi_ucx:
            system = "mpi_ucx"
            pure_transport = "roce"
        else:
            continue

        mbps = parse_float(row.get("mbps"))
        pure_mbps = pure.get((row["message_bytes"], pure_transport))
        if mbps is None or pure_mbps is None or pure_mbps <= 0:
            continue

        out.append({
            "message_bytes": row["message_bytes"],
            "primitive": row["primitive"],
            "system": system,
            "mode_or_transport": mode,
            "mbps": f"{mbps:.1f}",
            "pure_transport": pure_transport,
            "pure_mbps": f"{pure_mbps:.1f}",
            "utilization_pct": f"{mbps * 100.0 / pure_mbps:.1f}",
            "log_dir": row.get("log_dir", ""),
        })

    out.sort(key=lambda r: (
        int(r["message_bytes"]),
        r["primitive"],
        r["system"],
        r["mode_or_transport"],
    ))
    writer.writerows(out)


if __name__ == "__main__":
    main()
