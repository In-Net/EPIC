#!/usr/bin/env python3
import argparse
import csv
import sys


def main():
    ap = argparse.ArgumentParser(description="Summarize MPI bandwidth only.")
    ap.add_argument("csv", nargs="+", help="mpi_matrix.csv files")
    args = ap.parse_args()

    fields = [
        "data_mtu", "message_bytes", "primitive", "transport", "rate",
        "chunk_count", "pipeline_depth", "chunk_gap_us", "app_mbps", "requests_per_sec",
        "unit", "status", "log_dir",
    ]
    writer = csv.DictWriter(sys.stdout, fields)
    writer.writeheader()

    for csv_path in args.csv:
        with open(csv_path, newline="") as f:
            for row in csv.DictReader(f):
                status = row.get("status", "")
                primitive = row["primitive"]
                message_bytes = int(row["count"]) * 4
                app_mbps = 0.0
                requests_per_sec = 0.0
                unit = "Mbps"
                if status == "ok":
                    avg_us = float(row["avg_rank_max_us"])
                    if primitive == "barrier":
                        requests_per_sec = 1000000.0 / avg_us if avg_us else 0.0
                        unit = "requests/second"
                    else:
                        app_mbps = message_bytes * 8.0 / avg_us if avg_us else 0.0
                writer.writerow({
                    "data_mtu": row.get("data_mtu", ""),
                    "message_bytes": str(message_bytes),
                    "primitive": primitive,
                    "transport": row["transport"],
                    "rate": row["rate"],
                    "chunk_count": row.get("chunk_count", ""),
                    "pipeline_depth": row.get("pipeline_depth", ""),
                    "chunk_gap_us": row.get("chunk_gap_us", ""),
                    "app_mbps": f"{app_mbps:.1f}" if status == "ok" and primitive != "barrier" else "nan",
                    "requests_per_sec": f"{requests_per_sec:.2f}" if status == "ok" and primitive == "barrier" else "nan",
                    "unit": unit,
                    "status": status,
                    "log_dir": row["log_dir"],
                })


if __name__ == "__main__":
    main()
