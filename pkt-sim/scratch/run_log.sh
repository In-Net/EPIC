#!/usr/bin/env bash
# 单次仿真的「丢包/重传」精简日志，写入 ns-3-dev/packet-loss-result/log/。
# 只记录两类事件（按时间顺序）：
#   [DROP] t=.. wid=.. seq=.. type=.. link=..   哪个worker的哪个包、何时、在哪条链路被丢
#   [RETX] t=.. wid=.. seq=..                    何时、由哪个worker重传（端侧重传）
# 文件末尾给出总丢包数、总重传数、各 rank 重传数。
#
# 用法: bash scratch/run_log.sh <策略标签> <scheme> [ns3参数...]
# 例:   bash scratch/run_log.sh globalcounter EPIC2AllReduce_PacketLoss --lossRate=0.001 --lossLinkNum=1 --retxTimeoutUs=100 --dataBytes=16777600
set -u
cd "$(dirname "$0")/.." || exit 1

LABEL="${1:-}"; SCHEME="${2:-}"
if [[ -z "$LABEL" || -z "$SCHEME" ]]; then
    echo "用法: bash scratch/run_log.sh <策略标签> <EPIC2AllReduce_PacketLoss|EPIC3AllReduce_PacketLoss> [ns3参数...]" >&2
    exit 1
fi
shift 2

# 解析参数（缺省值与 .cc 默认一致），用于命名
rate=0.01; link=1; rto=1000; db=16777600
for a in "$@"; do
    case "$a" in
        --lossRate=*)      rate="${a#*=}";;
        --lossLinkNum=*)   link="${a#*=}";;
        --retxTimeoutUs=*) rto="${a#*=}";;
        --dataBytes=*)     db="${a#*=}";;
    esac
done
case "$SCHEME" in EPIC2*) S=EPIC2;; EPIC3*) S=EPIC3;; *) S="$SCHEME";; esac
MB="$(awk -v b="$db" 'BEGIN{printf "%.0f", b/1048576}')"
RPCT="$(awk -v r="$rate" 'BEGIN{printf "%g", r*100}')"

OUTDIR="packet-loss-result/log"
mkdir -p "$OUTDIR"
LOG="$OUTDIR/${LABEL}__${S}__r${RPCT}pct__link${link}__${MB}MB__rto${rto}us.log"

OUT="$(./ns3 run "$SCHEME $*" 2>&1)"

{
    echo "# 方案=$SCHEME | 标签=$LABEL | 丢包率=${RPCT}% | 链路数=$link | 数据量=${MB}MB | RTO=${rto}us"
    echo "# [DROP] 哪个worker的哪个包何时在哪条链路被丢；[RETX] 何时由哪个worker重传（端侧重传）"
    echo "# ---- 事件（按时间顺序）----"
    printf '%s\n' "$OUT" | grep -E '^\[(DROP|RETX)\]'
    echo
    echo "# ---- 统计 ----"
    echo "总丢包数: $(printf '%s\n' "$OUT" | grep -c '^\[DROP\]')"
    echo "总重传数: $(printf '%s\n' "$OUT" | grep -c '^\[RETX\]')"
    echo "各 rank 完成时上报的重传数:"
    printf '%s\n' "$OUT" | grep "AllReduce完成"
} > "$LOG"

echo "日志已写入: $LOG"
echo "总丢包数: $(grep -c '^\[DROP\]' "$LOG")   总重传数: $(grep -c '^\[RETX\]' "$LOG")"
