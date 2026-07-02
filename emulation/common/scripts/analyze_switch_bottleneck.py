#!/usr/bin/env python3
import argparse
import csv
import math
from pathlib import Path


def parse_kv(line):
    out = {}
    for tok in line.split():
        if "=" not in tok:
            continue
        key, value = tok.split("=", 1)
        out[key] = value
    return out


def parse_rate_bps(rate):
    if rate in ("none", "off", "0", ""):
        return None
    units = {
        "kbit": 1_000,
        "mbit": 1_000_000,
        "gbit": 1_000_000_000,
    }
    text = rate.lower()
    for suffix, mul in units.items():
        if text.endswith(suffix):
            return float(text[:-len(suffix)]) * mul
    return None


def switch_profile(log_dir):
    path = Path(log_dir) / "switch.log"
    result = {}
    profile = {}
    if not path.exists():
        return result, profile
    for line in path.read_text(errors="replace").splitlines():
        if line.startswith("SWITCH_RESULT "):
            result = parse_kv(line)
        elif line.startswith("SWITCH_PROFILE "):
            profile = parse_kv(line)
    return result, profile


def per_segment_counts(primitive):
    if primitive == "allreduce":
        return 8, 8
    if primitive in {"reduce", "reducescatter"}:
        return 5, 5
    if primitive in {"broadcast", "allgather"}:
        return 4, 4
    raise ValueError(primitive)


def main():
    ap = argparse.ArgumentParser(description="Estimate switch/link bottlenecks from primitive matrix logs.")
    ap.add_argument("csv", help="primitive_matrix.csv")
    ap.add_argument("--wire-bytes", type=int, default=1120,
                    help="estimated on-wire bytes per 1024B RoCE payload packet")
    args = ap.parse_args()

    fields = [
        "rate", "primitive", "mode", "count", "rankmax_avg_us",
        "e2e_payload_mbps", "switch_service_us_per_segment",
        "switch_payload_mbps", "switch_parsed_per_segment",
        "switch_tx_per_segment", "avg_parse_ns", "avg_handle_ns",
        "avg_build_ns", "avg_send_ns", "link_packet_us",
        "switch_vs_link",
    ]
    print(",".join(fields))
    with open(args.csv, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("status") != "ok":
                continue
            primitive = row["primitive"]
            count = int(row["count"])
            payload_bytes = int(row["payload_bytes"])
            rankmax_us = float(row["rankmax_avg_us"])
            segments = (count * 4) // payload_bytes
            parsed_per_seg, tx_per_seg = per_segment_counts(primitive)
            result, profile = switch_profile(row["log_dir"])
            if not profile:
                continue

            avg_parse_ns = float(profile.get("avg_parse_ns", "nan"))
            avg_handle_ns = float(profile.get("avg_handle_ns", "nan"))
            avg_build_ns = float(profile.get("avg_build_ns", "nan"))
            avg_send_ns = float(profile.get("avg_send_ns", "nan"))

            service_ns_per_seg = parsed_per_seg * (avg_parse_ns + avg_handle_ns)
            service_us_per_seg = service_ns_per_seg / 1000.0
            msg_bytes = count * 4
            e2e_payload_mbps = msg_bytes * 8.0 / rankmax_us
            switch_payload_mbps = payload_bytes * 8.0 / service_us_per_seg

            rate_bps = parse_rate_bps(row["rate"])
            link_packet_us = ""
            switch_vs_link = ""
            if rate_bps:
                link_packet_us_f = args.wire_bytes * 8.0 * 1_000_000.0 / rate_bps
                link_packet_us = f"{link_packet_us_f:.3f}"
                switch_vs_link = "switch" if service_us_per_seg > link_packet_us_f else "link"
            else:
                switch_vs_link = "unknown_link"

            out = {
                "rate": row["rate"],
                "primitive": primitive,
                "mode": row["mode"],
                "count": str(count),
                "rankmax_avg_us": f"{rankmax_us:.3f}",
                "e2e_payload_mbps": f"{e2e_payload_mbps:.1f}",
                "switch_service_us_per_segment": f"{service_us_per_seg:.3f}",
                "switch_payload_mbps": f"{switch_payload_mbps:.1f}",
                "switch_parsed_per_segment": str(parsed_per_seg),
                "switch_tx_per_segment": str(tx_per_seg),
                "avg_parse_ns": f"{avg_parse_ns:.1f}",
                "avg_handle_ns": f"{avg_handle_ns:.1f}",
                "avg_build_ns": f"{avg_build_ns:.1f}",
                "avg_send_ns": f"{avg_send_ns:.1f}",
                "link_packet_us": link_packet_us,
                "switch_vs_link": switch_vs_link,
            }
            print(",".join(out[field] for field in fields))


if __name__ == "__main__":
    main()
