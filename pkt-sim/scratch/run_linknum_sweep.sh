#!/usr/bin/env bash
# 实验2：固定丢包率 0.1%，扫丢包链路数 1→8。
# 丢包率(0.1%)与链路数范围(1..8)固定不变；吞吐用二进制 Gibit/s（÷2^30）。
# 结果写到 ns-3-dev/packet-loss-result/，文件名编码 策略标签/数据量/RTO，便于区分多次实验。
#
# 用法：bash scratch/run_linknum_sweep.sh <策略标签> [dataBytes] [retxUs]
#   <策略标签>  必填，给本次丢包策略起个名（如 globalcounter / evenoffset / dropall）
#   [dataBytes] 选填，总数据量(字节)，默认 64MB = 1400*47936 = 67110400（与 run_loss_sweep.sh 对齐）
#   [retxUs]    选填，重传超时(微秒)，默认 1000 (1ms)（与 run_loss_sweep.sh 对齐）
set -u

cd "$(dirname "$0")/.." || exit 1   # 切到 ns-3-dev/

LABEL="${1:-}"
if [[ -z "$LABEL" ]]; then
    echo "错误：缺少策略标签。用法: bash scratch/run_linknum_sweep.sh <策略标签> [dataBytes] [retxUs]" >&2
    exit 1
fi
DATA_BYTES="${2:-67110400}"   # 64MB 默认 = 1400*47936（与 run_loss_sweep.sh 对齐）
RTO="${3:-1000}"              # 1ms 默认（与 run_loss_sweep.sh 对齐）

RATE=0.001                     # 固定 0.1% 丢包率（默认丢包率）
LINKNUMS=(1 2 3 4 5 6 7 8)     # 固定链路数范围
START_US=1000000
GIB=1073741824                  # 2^30

OUTDIR="packet-loss-result"
mkdir -p "$OUTDIR"
DATA_MB="$(awk -v b="$DATA_BYTES" 'BEGIN{printf "%.0f", b/1048576}')"
CSV="$OUTDIR/linknum__${LABEL}__r0.1pct__${DATA_MB}MB__rto${RTO}us.csv"

getstats(){
    local out comp retx
    out="$(./ns3 run "$1" 2>&1)"
    comp="$(printf '%s\n' "$out" | awk '
        /AllReduce完成/ {
            if (match($0, /t=[0-9]+us/)) {
                v = substr($0, RSTART + 2, RLENGTH - 4);
                print v;
            } else if (match($0, /\[[0-9]+us\]/)) {
                v = substr($0, RSTART + 1, RLENGTH - 4);
                print v;
            }
        }' | sort -n | tail -1)"
    retx="$(printf '%s\n' "$out" | grep -oE 'event=RETX_SUMMARY total=[0-9]+|\[RETX_SUMMARY\] total=[0-9]+' | grep -oE '[0-9]+' | tail -1)"
    printf '%s %s\n' "${comp:-0}" "${retx:-0}"
}
echo "丢包链路数,EPIC2 完成(us),EPIC2 重传包数,EPIC2 (Gibit/s),EPIC3 完成(us),EPIC3 重传包数,EPIC3 (Gibit/s),EPIC3 优势" > "$CSV"
printf "实验2 固定0.1%%扫链路数 | 标签=%s | 数据量=%sMB | RTO=%sus\n" "$LABEL" "$DATA_MB" "$RTO"
printf "%-8s %-12s %-10s %-14s %-12s %-10s %-14s %-8s\n" "链路数" "E2_us" "E2_RETX" "E2_Gibit/s" "E3_us" "E3_RETX" "E3_Gibit/s" "E3优势"

for n in "${LINKNUMS[@]}"; do
    read -r c2 retx2 < <(getstats "EPIC2AllReduce_PacketLoss --lossRate=$RATE --lossLinkNum=$n --retxTimeoutUs=$RTO --dataBytes=$DATA_BYTES")
    read -r c3 retx3 < <(getstats "EPIC3AllReduce_PacketLoss --lossRate=$RATE --lossLinkNum=$n --retxTimeoutUs=$RTO --dataBytes=$DATA_BYTES")
    d2=$((c2-START_US)); d3=$((c3-START_US))
    read -r t2 t3 adv < <(awk -v d2="$d2" -v d3="$d3" -v b="$DATA_BYTES" -v g="$GIB" 'BEGIN{
        t2=b*8.0/(d2*1e-6)/g; t3=b*8.0/(d3*1e-6)/g;
        printf "%.2f %.2f %+.1f%%", t2, t3, (t3/t2-1)*100 }')
    echo "$n,$d2,$retx2,$t2,$d3,$retx3,$t3,$adv" >> "$CSV"
    printf "%-8s %-12s %-10s %-14s %-12s %-10s %-14s %-8s\n" "$n" "$d2" "$retx2" "$t2" "$d3" "$retx3" "$t3" "$adv"
done

echo; echo "结果已写入: $CSV"
