#!/usr/bin/env python3
import argparse
import csv
import sys
from pathlib import Path


def parse_kv(line):
    out = {}
    for token in line.split():
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        out[key] = value
    return out


def switch_profile(log_dir):
    path = Path(log_dir) / "switch.log"
    profile = {}
    result = {}
    if not path.exists():
        return result, profile
    for line in path.read_text(errors="replace").splitlines():
        if line.startswith("SWITCH_RESULT "):
            result = parse_kv(line)
        elif line.startswith("SWITCH_PROFILE "):
            profile = parse_kv(line)
    return result, profile


def packets_per_segment(primitive):
    if primitive == "allreduce":
        return 8
    if primitive in {"reduce", "reducescatter"}:
        return 5
    if primitive in {"broadcast", "allgather"}:
        return 4
    raise ValueError(primitive)


def main():
    ap = argparse.ArgumentParser(description="Summarize primitive bandwidth only.")
    ap.add_argument("csv", nargs="+", help="primitive_matrix.csv files")
    args = ap.parse_args()

    fields = [
        "data_mtu", "payload_bytes", "segment_window", "recv_window",
        "qp_timeout", "message_bytes", "primitive", "mode", "rate", "app_mbps",
        "requests_per_sec", "unit", "switch_ceiling_mbps", "status", "log_dir",
    ]
    writer = csv.DictWriter(sys.stdout, fields)
    writer.writeheader()

    for csv_path in args.csv:
        with open(csv_path, newline="") as f:
            for row in csv.DictReader(f):
                status = row.get("status", "")
                primitive = row["primitive"]
                message_bytes = int(row["count"]) * 4
                rankmax_us = float(row["rankmax_avg_us"]) if status == "ok" else 0.0
                if primitive == "barrier":
                    app_mbps = 0.0
                    requests_per_sec = 1000000.0 / rankmax_us if rankmax_us else 0.0
                    unit = "requests/second"
                else:
                    app_mbps = message_bytes * 8.0 / rankmax_us if rankmax_us else 0.0
                    requests_per_sec = 0.0
                    unit = "Mbps"

                _, profile = switch_profile(row["log_dir"])
                switch_mbps = 0.0
                if profile and status == "ok" and primitive != "barrier":
                    parsed_per_segment = packets_per_segment(primitive)
                    avg_parse_ns = float(profile.get("avg_parse_ns", "0"))
                    avg_handle_ns = float(profile.get("avg_handle_ns", "0"))
                    service_us = parsed_per_segment * (avg_parse_ns + avg_handle_ns) / 1000.0
                    payload_bytes = int(row["payload_bytes"])
                    switch_mbps = payload_bytes * 8.0 / service_us if service_us else 0.0

                writer.writerow({
                    "data_mtu": row.get("data_mtu", ""),
                    "payload_bytes": row["payload_bytes"],
                    "segment_window": row.get("segment_window", ""),
                    "recv_window": row.get("recv_window", ""),
                    "qp_timeout": row.get("qp_timeout", ""),
                    "message_bytes": str(message_bytes),
                    "primitive": primitive,
                    "mode": row["mode"],
                    "rate": row["rate"],
                    "app_mbps": f"{app_mbps:.1f}" if status == "ok" and primitive != "barrier" else "nan",
                    "requests_per_sec": f"{requests_per_sec:.2f}" if status == "ok" and primitive == "barrier" else "nan",
                    "unit": unit,
                    "switch_ceiling_mbps": f"{switch_mbps:.1f}" if switch_mbps else "nan",
                    "status": status,
                    "log_dir": row["log_dir"],
                })


if __name__ == "__main__":
    main()
