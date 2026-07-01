#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import sys
import math
import os
from collections import defaultdict

# 匹配示例：
# scalar FlowNet.h[554] JCT0 124.85373692187
PATTERN = re.compile(
    r'^\s*scalar\s+\S+\[(\d+)\]\s+(JCT[0-4])\s+([+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)\s*$'
)

def percentile(sorted_vals, p):
    """线性插值百分位，p ∈ [0,100]"""
    if not sorted_vals:
        return float("nan")
    if p <= 0:
        return sorted_vals[0]
    if p >= 100:
        return sorted_vals[-1]
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    return sorted_vals[f] * (c - k) + sorted_vals[c] * (k - f)

def write_outputs(out_dir, base, metric, host_to_vals):
    values_out = os.path.join(out_dir, f"{base}.{metric}.txt")
    stats_out  = os.path.join(out_dir, f"{base}.{metric}.stats.txt")

    if not host_to_vals:
        with open(stats_out, "w", encoding="utf-8") as fs:
            fs.write("\n".join([
                f"Metric      : {metric}",
                f"Output file : {values_out}",
                f"Total jobs  : 0",
                "No entries found."
            ]) + "\n")
        with open(values_out, "w", encoding="utf-8") as fo:
            fo.write(f"# host_id\t{metric}\n")
        return

    host_ids = sorted(host_to_vals.keys())
    rows = []
    vals = []

    for hid in host_ids:
        v = host_to_vals[hid][-1]   # 取最后一次
        rows.append((hid, v, len(host_to_vals[hid])))
        vals.append(v)

    # values 文件
    with open(values_out, "w", encoding="utf-8") as fo:
        fo.write(f"# host_id\t{metric}\n")
        for hid, v, _ in rows:
            fo.write(f"{hid}\t{v}\n")

    # 统计
    n = len(vals)
    vals_sorted = sorted(vals)
    vmin = vals_sorted[0]
    vmax = vals_sorted[-1]
    mean = sum(vals_sorted) / n
    var = sum((x - mean) ** 2 for x in vals_sorted) / n
    std = math.sqrt(var)

    dup_hosts = sum(1 for hid in host_ids if len(host_to_vals[hid]) > 1)

    stats_lines = [
        f"Metric      : {metric}",
        f"Output file : {values_out}",
        f"Total jobs  : {n}",
        f"Min         : {vmin}",
        f"Max         : {vmax}",
        f"Mean        : {mean}",
        f"Std         : {std}",
        f"P50         : {percentile(vals_sorted, 50)}",
        f"P90         : {percentile(vals_sorted, 90)}",
        f"P95         : {percentile(vals_sorted, 95)}",
        f"P99         : {percentile(vals_sorted, 99)}",
    ]
    if dup_hosts > 0:
        stats_lines.append(
            f"Hosts with multiple {metric} records: {dup_hosts} (kept last)"
        )

    with open(stats_out, "w", encoding="utf-8") as fs:
        fs.write("\n".join(stats_lines) + "\n")

def main():
    if len(sys.argv) != 2:
        print("Usage: python extract_jct_by_type.py <file.sca>", file=sys.stderr)
        sys.exit(1)

    sca_path = sys.argv[1]
    if not sca_path.endswith(".sca"):
        print("Error: input file must end with .sca", file=sys.stderr)
        sys.exit(1)

    # sca 文件名（不含路径、不含扩展名）
    base = os.path.splitext(os.path.basename(sca_path))[0]

    # 输出目录：data/<sca文件名>/
    out_dir = os.path.join("data", base)
    os.makedirs(out_dir, exist_ok=True)

    # metric -> host_id -> [vals...]
    data = {f"JCT{k}": defaultdict(list) for k in range(5)}

    with open(sca_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            m = PATTERN.match(line)
            if not m:
                continue
            host_id = int(m.group(1))
            metric = m.group(2)
            val = float(m.group(3))
            data[metric][host_id].append(val)

    # 输出每类 JCT
    for k in range(5):
        metric = f"JCT{k}"
        write_outputs(out_dir, base, metric, data[metric])

    # 终端摘要
    print(f"Output directory: {out_dir}/")
    for k in range(5):
        metric = f"JCT{k}"
        cnt_hosts = len(data[metric])
        cnt_vals = sum(len(vs) for vs in data[metric].values())
        print(
            f"{metric}: hosts={cnt_hosts}, records={cnt_vals}, "
            f"files={base}.{metric}.txt / {base}.{metric}.stats.txt"
        )

if __name__ == "__main__":
    main()
