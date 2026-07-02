#!/usr/bin/env python3
import argparse
import csv
import re
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
    ap = argparse.ArgumentParser(description="Summarize broadcast switch trace.")
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
            "rx_root_data": None,
            "tx_data": [],
            "rx_ack": [],
            "ack_agg_complete": None,
            "tx_root_ack": None,
        })
        if event == "rx_data" and rank == root:
            slot["rx_root_data"] = t_ns
        elif event == "tx_data" and rank != root:
            slot["tx_data"].append((rank, t_ns))
        elif event == "rx_ack" and rank != root:
            slot["rx_ack"].append((rank, t_ns))
        elif event == "bcast_ack_agg_complete":
            slot["ack_agg_complete"] = t_ns
        elif event == "tx_ack" and rank == root:
            slot["tx_root_ack"] = t_ns

    rows = []
    for psn in sorted(psns):
        if psn < args.skip_first:
            continue
        slot = psns[psn]
        rx_root = slot["rx_root_data"]
        tx_data_times = [t for _, t in slot["tx_data"]]
        rx_ack_times = [t for _, t in slot["rx_ack"]]
        tx_root_ack = slot["tx_root_ack"]
        if rx_root is None or not tx_data_times:
            continue
        first_tx = min(tx_data_times)
        last_tx = max(tx_data_times)
        first_ack = min(rx_ack_times) if rx_ack_times else None
        last_ack = max(rx_ack_times) if rx_ack_times else None
        row = {
            "psn": psn,
            "mode": mode,
            "rx_root_to_first_tx_data_us": us(first_tx - rx_root),
            "rx_root_to_last_tx_data_us": us(last_tx - rx_root),
            "tx_data_span_us": us(last_tx - first_tx),
            "rx_ack_count": len(rx_ack_times),
            "tx_data_count": len(tx_data_times),
            "last_tx_data_to_first_rx_ack_us": us(first_ack - last_tx) if first_ack else "",
            "last_tx_data_to_last_rx_ack_us": us(last_ack - last_tx) if last_ack else "",
            "rx_ack_span_us": us(last_ack - first_ack) if first_ack and last_ack else "",
            "last_rx_ack_to_root_ack_us": us(tx_root_ack - last_ack)
            if last_ack and tx_root_ack else "",
            "rx_root_to_root_ack_us": us(tx_root_ack - rx_root) if tx_root_ack else "",
        }
        rows.append(row)

    fields = [
        "psn", "mode", "rx_root_to_first_tx_data_us",
        "rx_root_to_last_tx_data_us", "tx_data_span_us", "tx_data_count",
        "rx_ack_count", "last_tx_data_to_first_rx_ack_us",
        "last_tx_data_to_last_rx_ack_us", "rx_ack_span_us",
        "last_rx_ack_to_root_ack_us", "rx_root_to_root_ack_us",
    ]
    if args.csv:
        with open(args.csv, "w", newline="") as f:
            writer = csv.DictWriter(f, fields)
            writer.writeheader()
            writer.writerows(rows)

    numeric = {field: [] for field in fields if field not in ("psn", "mode")}
    rank_ack_rtt = {}
    for row in rows:
        for field in numeric:
            value = row.get(field)
            if value != "":
                numeric[field].append(float(value))

    for psn in sorted(psns):
        if psn < args.skip_first:
            continue
        slot = psns[psn]
        tx_by_rank = {}
        ack_by_rank = {}
        for rank, t_ns in slot["tx_data"]:
            if rank == root:
                continue
            tx_by_rank.setdefault(rank, t_ns)
        for rank, t_ns in slot["rx_ack"]:
            if rank == root:
                continue
            ack_by_rank.setdefault(rank, t_ns)
        for rank, tx_ns in tx_by_rank.items():
            ack_ns = ack_by_rank.get(rank)
            if ack_ns is not None:
                rank_ack_rtt.setdefault(rank, []).append(us(ack_ns - tx_ns))

    print(f"trace={switch_log}")
    print(f"mode={mode} root={root} psns={len(rows)}")
    for field in fields:
        if field in ("psn", "mode"):
            continue
        values = numeric[field]
        if values:
            print(f"{field}: avg={avg(values):.3f} max={max(values):.3f}")
    for rank in sorted(rank_ack_rtt):
        values = rank_ack_rtt[rank]
        print(f"rank{rank}_tx_data_to_rx_ack_us: avg={avg(values):.3f} max={max(values):.3f}")


if __name__ == "__main__":
    main()
