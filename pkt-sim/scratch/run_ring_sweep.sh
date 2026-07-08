#!/usr/bin/env bash
# Ring 四原语（AllReduce / Reduce / ReduceScatter / AllGather）扫消息量，
# 生成两张“转置”大表，各写到一个 CSV（放在 packet-loss-result/ring-test/）：
#   ring-completion-us.csv       完成时间(us)   = 完成时刻 - START_US
#   ring-throughput-gibitps.csv  吞吐(Gibit/s)  = 通信量*8 / 时间 / 2^30 （二进制换算）
#
# 表格形状（两张表一致）：
#   第一行： 原语,4KB,16KB,...,1GB
#   之后每行： <原语>,<各尺寸的值...>
#
# 吞吐口径（方法一 algbw = 整个 dataSize / 时间，所有原语因子=1，与 run_linknum_sweep.sh 一致）：
#   吞吐 = dataSize*8 / 时间 / 2^30
#   注意：RS/AG 的 algbw = k/(k-1) × 线速，会略超线速（~104 Gibit/s），这是 algbw 定义使然，非 bug。
#
# 用法： bash scratch/run_ring_sweep.sh
set -u

cd "$(dirname "$0")/.." || exit 1   # 切到 ns-3-dev/

NUMRANKS=8                     # 环内 rank 数（与各 .cc 默认一致）；改这里请同时确认 Reduce 的 rootRank
START_US=1000000               # app 启动时刻(=Seconds(1.0))，与 run_linknum_sweep.sh 对齐
GIB=1073741824                 # 2^30
OUTDIR="packet-loss-result/ring-test"
mkdir -p "$OUTDIR"
TIME_CSV="$OUTDIR/ring-completion-us.csv"
THRO_CSV="$OUTDIR/ring-throughput-gibitps.csv"

# 尺寸标签 + 字节数（二进制：KB=1024, MB=1024^2, GB=1024^3）
SIZE_LABELS=(4KB 16KB 64KB 256KB 1MB 4MB 16MB 64MB 256MB 1GB)
SIZE_BYTES=(4096 16384 65536 262144 1048576 4194304 16777216 67108864 268435456 1073741824)

# 原语：显示名 | 可执行文件 stem | 完成日志匹配串 | 通信量因子(分子) | 通信量因子(分母)
PRIM_NAMES=(AllReduce Reduce ReduceScatter AllGather)
PRIM_STEMS=(RingAllReduce RingReduce RingReduceScatter RingAllGather)
PRIM_PATS=("AllReduce complete!" "all data received" "ReduceScatter complete!" "AllGather complete!")
PRIM_NUM=(1 1 1 1)   # 方法一 algbw：所有原语通信量 = dataSize（因子 1/1）
PRIM_DEN=(1 1 1 1)

echo ">> 编译四个 Ring 目标 ..."
./ns3 build "${PRIM_STEMS[@]}" >/dev/null 2>&1 || { echo "编译失败" >&2; exit 1; }

# 跑一次，返回“最后一个 rank 的完成时刻(us)”
getcomp(){
    local bin="$1" ds="$2" pat="$3"
    "$bin" --dataSize="$ds" 2>&1 \
        | grep -F "$pat" \
        | grep -oE '\[[0-9]+us\]' | grep -oE '[0-9]+' \
        | sort -n | tail -1
}

# 表头（两张表相同）
hdr="原语"
for l in "${SIZE_LABELS[@]}"; do hdr="$hdr,$l"; done
echo "$hdr" > "$TIME_CSV"
echo "$hdr" > "$THRO_CSV"

printf "%-13s" "原语\尺寸"; for l in "${SIZE_LABELS[@]}"; do printf " %10s" "$l"; done; printf "\n"

for i in "${!PRIM_NAMES[@]}"; do
    name="${PRIM_NAMES[$i]}"; stem="${PRIM_STEMS[$i]}"; pat="${PRIM_PATS[$i]}"
    num="${PRIM_NUM[$i]}"; den="${PRIM_DEN[$i]}"

    bin="$(ls build/scratch/ns3*-"${stem}"-default 2>/dev/null | head -1)"
    if [[ -z "$bin" ]]; then echo "找不到 $stem 可执行文件（先 ./ns3 build）" >&2; exit 1; fi

    trow="$name"; hrow="$name"
    printf "%-13s" "$name"
    for j in "${!SIZE_BYTES[@]}"; do
        ds="${SIZE_BYTES[$j]}"
        comp="$(getcomp "$bin" "$ds" "$pat")"; comp="${comp:-0}"
        dur=$((comp - START_US))
        thr="$(awk -v b="$ds" -v d="$dur" -v n="$num" -v m="$den" -v g="$GIB" 'BEGIN{
            if (d <= 0) { print "0.00"; exit }
            vol = b * n / m;
            printf "%.2f", vol * 8.0 / (d * 1e-6) / g }')"
        trow="$trow,$dur"; hrow="$hrow,$thr"
        printf " %10s" "$dur"
    done
    echo "$trow" >> "$TIME_CSV"
    echo "$hrow" >> "$THRO_CSV"
    printf "\n"
done

echo
echo "完成时间表 -> $TIME_CSV"
echo "吞吐量表   -> $THRO_CSV"
