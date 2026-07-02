#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path


def parse_kv_line(line):
    out = {}
    for token in line.split():
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        out[key] = value
    return out


def avg(values):
    return sum(values) / len(values) if values else 0.0


def us(delta_ns):
    return delta_ns / 1000.0


def main():
    ap = argparse.ArgumentParser(description="Summarize reduce switch trace.")
    ap.add_argument("log_dir", help="primitive run log directory")
    ap.add_argument("--csv", default=None, help="write per-PSN CSV")
    ap.add_argument("--skip-first", type=int, default=0,
                    help="skip the first N PSNs, useful for dropping warmup")
    args = ap.parse_args()

    switch_log = Path(args.log_dir) / "switch.log"
    if not switch_log.exists():
        raise SystemExit(f"missing {switch_log}")

    root = 0
    mode = ""
    psns = {}
    for line in switch_log.read_text(errors="replace").splitlines():
        if line.startswith("SWITCH "):
            kv = parse_kv_line(line)
            root = int(kv.get("root", root))
            mode = kv.get("mode", mode)
            continue
        if not line.startswith("SWITCH_TRACE "):
            continue
        kv = parse_kv_line(line)
        event = kv.get("event")
        rank = int(kv.get("rank", "-1"))
        psn = int(kv.get("psn", "-1"))
        t_ns = int(kv.get("t_ns", "0"))
        slot = psns.setdefault(psn, {
            "rx_data": [],
            "tx_root_data": [],
            "rx_root_ack": [],
            "tx_ack": [],
        })
        if event == "rx_data":
            slot["rx_data"].append((rank, t_ns))
        elif event == "tx_data" and rank == root:
            slot["tx_root_data"].append(t_ns)
        elif event == "rx_ack" and rank == root:
            slot["rx_root_ack"].append(t_ns)
        elif event == "tx_ack":
            slot["tx_ack"].append((rank, t_ns))

    rows = []
    for psn in sorted(psns):
        if psn < args.skip_first:
            continue
        slot = psns[psn]
        rx_data_times = [t for _, t in slot["rx_data"]]
        rx_root_data_times = [t for rank, t in slot["rx_data"] if rank == root]
        rx_nonroot_data_times = [t for rank, t in slot["rx_data"] if rank != root]
        tx_root_data_times = slot["tx_root_data"]
        rx_root_ack_times = slot["rx_root_ack"]
        tx_ack_times = [t for _, t in slot["tx_ack"]]
        if not rx_data_times or not tx_root_data_times:
            continue

        first_rx_data = min(rx_data_times)
        last_rx_data = max(rx_data_times)
        first_rx_root_data = min(rx_root_data_times) if rx_root_data_times else None
        last_rx_nonroot_data = max(rx_nonroot_data_times) if rx_nonroot_data_times else None
        first_tx_result = min(tx_root_data_times)
        first_rx_root_ack = min(rx_root_ack_times) if rx_root_ack_times else None
        first_tx_ack = min(tx_ack_times) if tx_ack_times else None
        last_tx_ack = max(tx_ack_times) if tx_ack_times else None

        row = {
            "psn": psn,
            "mode": mode,
            "rx_data_count": len(rx_data_times),
            "tx_result_count": len(tx_root_data_times),
            "rx_root_ack_count": len(rx_root_ack_times),
            "tx_ack_count": len(tx_ack_times),
            "first_to_last_rx_data_us": us(last_rx_data - first_rx_data),
            "root_rx_data_to_tx_result_us": us(first_tx_result - first_rx_root_data)
            if first_rx_root_data else "",
            "last_nonroot_rx_data_to_tx_result_us": us(first_tx_result - last_rx_nonroot_data)
            if last_rx_nonroot_data else "",
            "last_rx_data_to_tx_result_us": us(first_tx_result - last_rx_data),
            "first_rx_data_to_tx_result_us": us(first_tx_result - first_rx_data),
            "tx_result_to_rx_root_ack_us": us(first_rx_root_ack - first_tx_result)
            if first_rx_root_ack else "",
            "rx_root_ack_to_first_tx_ack_us": us(first_tx_ack - first_rx_root_ack)
            if first_rx_root_ack and first_tx_ack else "",
            "rx_root_ack_to_last_tx_ack_us": us(last_tx_ack - first_rx_root_ack)
            if first_rx_root_ack and last_tx_ack else "",
            "tx_ack_span_us": us(last_tx_ack - first_tx_ack)
            if first_tx_ack and last_tx_ack else "",
            "first_rx_data_to_last_tx_ack_us": us(last_tx_ack - first_rx_data)
            if last_tx_ack else "",
        }
        rows.append(row)

    fields = [
        "psn", "mode", "rx_data_count", "tx_result_count", "rx_root_ack_count",
        "tx_ack_count", "first_to_last_rx_data_us", "last_rx_data_to_tx_result_us",
        "root_rx_data_to_tx_result_us", "last_nonroot_rx_data_to_tx_result_us",
        "first_rx_data_to_tx_result_us", "tx_result_to_rx_root_ack_us",
        "rx_root_ack_to_first_tx_ack_us", "rx_root_ack_to_last_tx_ack_us",
        "tx_ack_span_us", "first_rx_data_to_last_tx_ack_us",
    ]
    if args.csv:
        with open(args.csv, "w", newline="") as f:
            writer = csv.DictWriter(f, fields)
            writer.writeheader()
            writer.writerows(rows)

    numeric = {field: [] for field in fields if field not in ("psn", "mode")}
    for row in rows:
        for field in numeric:
            value = row.get(field)
            if value != "":
                numeric[field].append(float(value))

    print(f"trace={switch_log}")
    print(f"mode={mode} root={root} psns={len(rows)}")
    for field in fields:
        if field in ("psn", "mode"):
            continue
        values = numeric[field]
        if values:
            print(f"{field}: avg={avg(values):.3f} max={max(values):.3f}")


if __name__ == "__main__":
    main()
