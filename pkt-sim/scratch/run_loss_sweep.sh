#!/usr/bin/env bash
# 实验1：单链路丢包，扫丢包率。
# 丢包率档位固定不变；吞吐用二进制 Gibit/s（÷2^30）。
# 结果写到 ns-3-dev/packet-loss-result/，文件名编码 策略标签/数据量/RTO，便于区分多次实验。
#
# 用法：bash scratch/run_loss_sweep.sh <策略标签> [dataBytes] [retxUs]
#   <策略标签>  必填，给本次丢包策略起个名（如 globalcounter / evenoffset / dropall）
#   [dataBytes] 选填，总数据量(字节)，默认 64MB = 1400*47936 = 67110400
#   [retxUs]    选填，重传超时(微秒)，默认 1000 (1ms)
set -u

cd "$(dirname "$0")/.." || exit 1   # 切到 ns-3-dev/

LABEL="${1:-}"
if [[ -z "$LABEL" ]]; then
    echo "错误：缺少策略标签。用法: bash scratch/run_loss_sweep.sh <策略标签> [dataBytes] [retxUs]" >&2
    exit 1
fi
DATA_BYTES="${2:-67110400}"   # 64MB 默认 = 1400*47936
RTO="${3:-1000}"              # 1ms 默认

RATES=(0 0.00001 0.00003 0.0001 0.0003 0.001 0.003 0.01)   # 固定档位（含 baseline 0）
START_US=1000000                                # AllReduce 从 1s 开始
GIB=1073741824                                   # 2^30

OUTDIR="packet-loss-result"
mkdir -p "$OUTDIR"
DATA_MB="$(awk -v b="$DATA_BYTES" 'BEGIN{printf "%.0f", b/1048576}')"
CSV="$OUTDIR/lossrate__${LABEL}__${DATA_MB}MB__rto${RTO}us.csv"

# 取某次仿真的最后完成时刻(us)和程序输出的重传包总数
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
echo "丢包率,EPIC2 完成(us),EPIC2 重传包数,EPIC2 (Gibit/s),EPIC3 完成(us),EPIC3 重传包数,EPIC3 (Gibit/s),EPIC3 优势" > "$CSV"
printf "实验1 单链路扫丢包率 | 标签=%s | 数据量=%sMB | RTO=%sus\n" "$LABEL" "$DATA_MB" "$RTO"
printf "%-8s %-12s %-10s %-14s %-12s %-10s %-14s %-8s\n" "丢包率" "E2_us" "E2_RETX" "E2_Gibit/s" "E3_us" "E3_RETX" "E3_Gibit/s" "E3优势"

for r in "${RATES[@]}"; do
    read -r c2 retx2 < <(getstats "EPIC2AllReduce_PacketLoss --lossRate=$r --lossLinkNum=1 --retxTimeoutUs=$RTO --dataBytes=$DATA_BYTES")
    read -r c3 retx3 < <(getstats "EPIC3AllReduce_PacketLoss --lossRate=$r --lossLinkNum=1 --retxTimeoutUs=$RTO --dataBytes=$DATA_BYTES")
    d2=$((c2-START_US)); d3=$((c3-START_US))
    read -r rlbl t2 t3 adv < <(awk -v r="$r" -v d2="$d2" -v d3="$d3" -v b="$DATA_BYTES" -v g="$GIB" 'BEGIN{
        if(r==0) rlbl="0"; else rlbl=sprintf("%g%%", r*100);
        t2=b*8.0/(d2*1e-6)/g; t3=b*8.0/(d3*1e-6)/g;
        printf "%s %.2f %.2f %+.1f%%", rlbl, t2, t3, (t3/t2-1)*100 }')
    echo "$rlbl,$d2,$retx2,$t2,$d3,$retx3,$t3,$adv" >> "$CSV"
    printf "%-8s %-12s %-10s %-14s %-12s %-10s %-14s %-8s\n" "$rlbl" "$d2" "$retx2" "$t2" "$d3" "$retx3" "$t3" "$adv"
done

echo; echo "结果已写入: $CSV"
