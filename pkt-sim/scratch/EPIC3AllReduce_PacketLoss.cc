#ifndef INC_APPLICATION_H
#define INC_APPLICATION_H

#include "ns3/core-module.h"
#include "ns3/network-module.h"
#include "ns3/internet-module.h"
#include "ns3/point-to-point-module.h"
#include "ns3/application.h"
#include "ns3/socket.h"
#include "ns3/address.h"
#include "ns3/event-id.h"
#include "ns3/ptr.h"
#include "ns3/traced-callback.h"
#include "ns3/udp-socket-factory.h"
#include "ns3/udp-socket.h"
#include "ns3/tag.h"
#include "ns3/inet-socket-address.h"
#include "ns3/log.h"
#include "ns3/error-model.h"
#include "ns3/double.h"
#include "ns3/enum.h"
#include "ns3/pointer.h"

#include <algorithm>
#include <vector>
#include <cmath>
#include <map>
#include <set>
#include <cstdint>
#include <fstream>
#include <sstream>

using namespace ns3;

NS_LOG_COMPONENT_DEFINE("INCSwitchMLAllReduceTest");

static uint64_t g_totalRetxPktNum = 0;

// 对应zzz_savefiles/scratch/INC2AllReduce_PacketLoss_stochastic2.cc

enum PacketType : uint8_t {
    DATA = 0,       // 原始数据包（worker → switch）
    AGGREGATED = 1, // 聚合后的包（switch → worker）
    NAK = 2,        // 否定确认（switch → worker）：逐跳 gap 检测触发的快速重传
    ACK = 3         // 正向确认（switch → worker）：交换机收到 DATA 即回，与广播解耦。
                    //   仅用于取消该 seq 的 RTO（防止无损 rank 误超时重传），
                    //   不推进发送窗口（窗口仍由 AGGREGATED 驱动，保持聚合池深度安全）。
};

class INCTag : public Tag{
    public:

        INCTag() : m_seq(0), m_idx(0), m_wid(0), m_ver(0), m_type(DATA), m_isRetransmit(0) {}

        static TypeId GetTypeId() {
            static TypeId tid = TypeId("ns3::INCTag")
                .SetParent<Tag>()
                .AddConstructor<INCTag>();
            return tid;
        }

        TypeId GetInstanceTypeId() const override{ 
            return GetTypeId(); 
        }

        void SetType(PacketType type) { m_type = type; }
        PacketType GetType() const { return m_type; }

        bool IsData() const { return m_type == DATA; }
        bool IsAggregated() const { return m_type == AGGREGATED; }
    
        void SetIsRetransmit(uint8_t isRetransmit) { m_isRetransmit = isRetransmit; }
        uint8_t GetIsRetransmit() const { return m_isRetransmit; }

        void SetSeq(uint32_t seq) { m_seq = seq; }
        uint32_t GetSeq() const { return m_seq; }

        void SetIdx(uint32_t idx) { m_idx = idx; }
        uint32_t GetIdx() const { return m_idx; }

        // 新增：worker ID
        void SetWid(uint32_t wid) { m_wid = wid; }
        uint32_t GetWid() const { return m_wid; }

        // 新增：version (0 或 1)
        void SetVer(uint8_t ver) { m_ver = ver; }
        uint8_t GetVer() const { return m_ver; }
        
        uint32_t GetSerializedSize() const override {
            // seq(4) + idx(4) + wid(4) + ver(1) + type(1) = 14 bytes
            return 3 * sizeof(uint32_t) + 3 * sizeof(uint8_t);
        }

        void Serialize(TagBuffer i) const override{
            i.WriteU32(m_seq);
            i.WriteU32(m_idx);
            i.WriteU32(m_wid);
            i.WriteU8(m_ver);
            i.WriteU8(m_type);
            i.WriteU8(m_isRetransmit);
        }

        void Deserialize(TagBuffer i) override{
            m_seq = i.ReadU32();
            m_idx = i.ReadU32();
            m_wid = i.ReadU32();
            m_ver = i.ReadU8();
            m_type = static_cast<PacketType>(i.ReadU8());
            m_isRetransmit = i.ReadU8();
        }

        void Print(std::ostream& os) const override {
            os << "INCTag: seq=" << m_seq 
               << ", idx=" << m_idx 
               << ", wid=" << m_wid
               << ", ver=" << (uint32_t)m_ver
               << ", type=" << (m_type == DATA ? "DATA" : "AGGREGATED")
               << ", isRetransmit=" << (int)m_isRetransmit;
        }

    private:
        uint32_t m_seq;   // 标识具体的包
        uint32_t m_idx;   // 用于查找聚合器上的slot
        uint32_t m_wid;   // worker ID，标识来自哪个rank
        uint8_t m_ver;    // 版本号，0或1交替
        PacketType m_type;
        uint8_t m_isRetransmit;
};

// ========== 全局丢包日志管理器 ==========
// 按 rank (wid) 组织丢包记录，每个 rank 一行
class PacketDropLogger {
public:
    static PacketDropLogger& Instance() {
        static PacketDropLogger instance;
        return instance;
    }
    
    void SetRankNum(uint32_t rankNum) {
        m_rankDrops.resize(rankNum);
    }
    
    // 记录某个 rank 丢包的 seq
    void AddDrop(uint32_t wid, uint32_t seq) {
        if (wid < m_rankDrops.size()) {
            m_rankDrops[wid].push_back(seq);
        }
    }
    
    // 写入日志文件：每行是一个 rank 的丢包 seq 列表
    void WriteToFile(const std::string& filename = "INC_packet_drop.log") {
        std::ofstream logFile(filename);
        if (logFile.is_open()) {
            // 每行格式: rank,seq1,seq2,seq3,...
            for (uint32_t wid = 0; wid < m_rankDrops.size(); wid++) {
                logFile << wid;
                for (uint32_t seq : m_rankDrops[wid]) {
                    logFile << "," << seq;
                }
                logFile << "\n";
            }
            logFile.close();
        }
    }
    
private:
    std::vector<std::vector<uint32_t>> m_rankDrops;  // m_rankDrops[wid] = {seq1, seq2, ...}
};

// ========== 确定性丢包模型 ==========
// 每 N 个包丢 1 个，完全可重复，支持偏移量，只丢 DATA 包
// 使用 Tag 判断包类型，记录丢包的 seq 和 wid
class DeterministicErrorModel : public ErrorModel {
public:
    static TypeId GetTypeId() {
        static TypeId tid = TypeId("ns3::DeterministicErrorModel")
            .SetParent<ErrorModel>()
            .AddConstructor<DeterministicErrorModel>();
        return tid;
    }

    DeterministicErrorModel() : m_counter(0), m_dropInterval(10), m_dropOffset(0) {}

    void SetDropInterval(uint32_t interval) { m_dropInterval = interval; }
    void SetDropOffset(uint32_t offset) { m_dropOffset = offset; }
    void SetLinkId(uint32_t linkId) { m_linkId = linkId; }
    void SetLossRate(double p) { m_lossRate = p; }           // 每包丢包概率（累积器步进）
    void SetLossAccumInit(double a) { m_lossAccum = a; }     // 累积器初值（多链路错相位）

    uint32_t m_dropCount=150;

private:
    bool DoCorrupt(Ptr<Packet> p) override {
        // 使用 Tag 判断包类型
        INCTag tag;
        if (!p->PeekPacketTag(tag)) {
            return false;  // 没有 Tag，不丢
        }

        // 只丢 DATA 包（不丢 AGGREGATED 等其它类型）
        if (tag.GetType() != DATA) {
            return false;
        }

        // 重传包也参与丢包（不再免丢）
        // if (tag.GetIsRetransmit() == 1) {
        //     return false;
        // }

        //uint32_t seq = tag.GetSeq();
        //uint32_t wid = tag.GetWid();

        // 概率累积丢包：每个包给累积器加 m_lossRate，累积到 >=1 就丢 1 个并减 1。
        // 例如 m_lossRate=0.01 → 每 100 个包丢 1 个 = 1% 丢包率。
        // m_lossRate==0 表示不丢包（baseline）；多链路用 m_lossAccum 初值错相位。
        bool shouldDrop = false;
        if (m_lossRate > 0.0) {
            m_lossAccum += m_lossRate;
            if (m_lossAccum >= 1.0) {
                m_lossAccum -= 1.0;
                shouldDrop = true;
            }
        }
        
        if (shouldDrop) {
            // 记录丢包的 wid 和 seq
            uint32_t wid = tag.GetWid();
            uint32_t seq = tag.GetSeq();
            PacketDropLogger::Instance().AddDrop(wid, seq);
            std::clog << "[LossModel] t=" << Simulator::Now().GetMicroSeconds() << "us"
                      << " event=DROP wid=" << wid << " seq=" << seq
                      << " type=" << (tag.GetType()==DATA?"DATA":"AGG")
                      << " rtx=" << (int)tag.GetIsRetransmit()
                      << " link=" << m_linkId << "\n";
        }
        
        return shouldDrop;
    }

    void DoReset() override { m_counter = 0; }

    uint32_t m_counter;
    uint32_t m_dropInterval;
    uint32_t m_dropOffset;
    uint32_t m_linkId;
    double m_lossRate = 0.0;    // 每包丢包概率
    double m_lossAccum = 0.0;   // 概率累积器
};

class AggSlot {
    public:

        void SetRankNum(uint32_t n) { m_num = n; }
        
        // 使用取模方式，count 归零表示完成
        bool AggPkt(Ptr<Packet> pkt) {
            m_count = (m_count + 1) % m_num;
            
            if (m_count == 1) {
                // 第一个包到达，初始化/覆盖之前的缓存
                m_pkt = pkt;
            } else {
                // 后续包，累加（简化处理，实际应做向量聚合）
                m_pkt = pkt;
            }
            
            // count 归零表示全部到齐
            return (m_count == 0);
        }

        Ptr<Packet> GetAggPkt() {
            return m_pkt;
        }

        // 判断该轮是否已完成（用于重传逻辑）
        bool IsCompleted() const { return m_count == 0; }

        uint32_t m_count = 0;
        uint32_t m_num = 0;
        Ptr<Packet> m_pkt = nullptr;
};

// 用于保存待确认包的信息（重传时需要）
struct PendingPktInfo {
    uint32_t seq;
    uint32_t idx;
    uint8_t ver;
};

class INCSenderApp : public Application {
    public:
        INCSenderApp() : m_rankId(0), m_sentPktNum(0), m_recvPktNum(0), 
                         m_retxTimeout(MicroSeconds(100)) {};  // 200 微秒超时
        ~INCSenderApp(){};

        void StartApplication() override;
        void StopApplication() override;

        void InitComm(uint64_t dataSize, uint32_t pktPayloadSize, uint32_t windowSize, uint32_t retxTimeout);
        void StartAllReduce();
        void RecvAggPacket(Ptr<Socket> socket);
        void FillWindow();  // 单一滑动窗口：从右沿(m_nextSeq)补发新包
        
        // 重传相关
        void RetransmitPacket(uint32_t idx);
        void FastRetransmit(uint32_t seq);  // M1: NAK 触发的快速重传
        void SendPacketWithTimer(uint32_t seq, uint32_t idx, uint8_t ver);
        void CancelTimer(uint32_t idx);
        void CancelTimerOnly(uint32_t seq);  // ACK: 只取消 RTO 定时器，保留 pending（不推进窗口）

        // INC相关
        uint32_t m_rankId;             // rank编号
        uint64_t m_dataSize;
        uint32_t m_pktPayloadSize;
        uint32_t m_pktNum;
        uint32_t m_sentPktNum;         // 已发送包数
        uint32_t m_recvPktNum;         // 已接收聚合包数
        uint32_t m_windowSize;
        uint32_t m_windowBase;         // 单一滑动窗口左沿：最低未确认 seq
        uint32_t m_nextSeq;            // 单一滑动窗口右沿：下一个待发送 seq

        // 通信相关
        Ptr<Socket> m_sendSocket;
        Ptr<Socket> m_recvSocket;
        Address m_peerAddr;
        uint16_t m_port;

        // 重传机制相关
        Time m_retxTimeout;                              // 重传超时时间
        std::map<uint32_t, EventId> m_retxTimers;        // seq -> 定时器事件
        std::map<uint32_t, PendingPktInfo> m_pendingPkts; // seq -> 待确认包信息
        uint32_t m_retxPktNum;                             // 重传包数
};

void INCSenderApp::StartApplication() {
    //1.创建接收缓冲区
    m_recvSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_recvSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024)); // 1GB
    m_recvSocket->Bind(InetSocketAddress(Ipv4Address::GetAny(), m_port));
    m_recvSocket->SetRecvCallback(MakeCallback(&INCSenderApp::RecvAggPacket, this));

    //2.创建发送缓冲区
    m_sendSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_sendSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_sendSocket->Bind();
    m_sendSocket->Connect(m_peerAddr);

    Simulator::Schedule(MicroSeconds(1), &INCSenderApp::StartAllReduce, this);
}

void INCSenderApp::StopApplication() {
    NS_LOG_INFO("[Rank " << m_rankId << "] t=" << Simulator::Now().GetSeconds() << "s"
                << " event=STOP sent=" << m_sentPktNum
                << " recv_aggregated=" << m_recvPktNum
                << " total_pkts=" << m_pktNum);
    
    // 取消所有重传定时器
    for (auto& pair : m_retxTimers) {
        Simulator::Cancel(pair.second);
    }
    m_retxTimers.clear();
    m_pendingPkts.clear();
    
    if(m_sendSocket) m_sendSocket->Close();
    if(m_recvSocket) m_recvSocket->Close();
}

void INCSenderApp::InitComm(uint64_t dataSize, uint32_t pktPayloadSize, uint32_t windowSize, uint32_t retxTimeout) {
    m_dataSize = dataSize;
    m_pktPayloadSize = pktPayloadSize;
    m_pktNum = dataSize / pktPayloadSize;
    m_windowSize = windowSize;
    m_retxTimeout = MicroSeconds(retxTimeout);
    m_retxPktNum = 0;
    m_windowBase = 0;
    m_nextSeq = 0;
}

void INCSenderApp::StartAllReduce() {
    NS_LOG_INFO("[Rank " << m_rankId << "] t=" << Simulator::Now().GetSeconds() << "s"
                << " event=START_ALLREDUCE window=" << m_windowSize
                << " total_pkts=" << m_pktNum);
    
    // 单一滑动窗口：左沿=0，从右沿一次性填满 [0, windowSize)
    m_windowBase = 0;
    m_nextSeq = 0;
    FillWindow();
}

void INCSenderApp::RecvAggPacket(Ptr<Socket> socket) {
    Ptr<Packet> pkt;
    while ((pkt = socket->Recv())) {
        INCTag tag;
        pkt->PeekPacketTag(tag);
    
        if (tag.GetType() == ACK) {
            // 交换机已收到该 DATA：取消其 RTO 定时器，避免无损 rank 在等待广播期间
            // 误超时重传（消除“木桶效应”式的冗余重传）。窗口推进仍等 AGGREGATED。
            CancelTimerOnly(tag.GetSeq());
            continue;
        }
        else if (tag.GetType() == NAK) {
            // M1: 交换机检测到上行空洞，立即对该 seq 快速重传（不等 RTO）
            uint32_t nseq = tag.GetSeq();
            if (m_pendingPkts.find(nseq) != m_pendingPkts.end()) {
                FastRetransmit(nseq);
            }
            continue;
        }
        else if (tag.GetType() != AGGREGATED) {
            NS_LOG_INFO("[Rank " << m_rankId << "] t=" << Simulator::Now().GetMicroSeconds() << "us event=ERROR_UNKNOWN_PACKET");
        }
        else {
            uint32_t seq = tag.GetSeq();
            uint32_t idx = tag.GetIdx();
            uint8_t ver = tag.GetVer();

            // ========== 新增：检查版本号是否匹配 ==========
            auto it = m_pendingPkts.find(seq);
            if (it == m_pendingPkts.end()) {
                // 没有待确认的包，可能是重复的 AGGREGATED，忽略
                NS_LOG_INFO("[Rank " << m_rankId << "] t=" << Simulator::Now().GetMicroSeconds() << "us"
                            << " event=RECV_DUP_AGGREGATED seq=" << seq
                            << " idx=" << idx << " ver=" << (uint32_t)ver);
                continue;
            }

            NS_LOG_INFO("[Rank " << m_rankId << "] t=" << Simulator::Now().GetMicroSeconds() << "us"
                        << " event=RECV_AGGREGATED seq=" << seq
                        << " idx=" << idx << " ver=" << (uint32_t)ver);
            
            // 取消该 idx 的重传定时器
            CancelTimer(seq);
            
            m_recvPktNum++;

            // 单一滑动窗口：左沿越过连续已确认(已从 m_pendingPkts 移除)的 seq
            while (m_windowBase < m_nextSeq &&
                   m_pendingPkts.find(m_windowBase) == m_pendingPkts.end()) {
                m_windowBase++;
            }
            // 左沿前移后，从右沿补发新包，保持在途量 <= windowSize
            FillWindow();

            // 检查是否完成：接收到所有包
            if (m_recvPktNum == m_pktNum) {
                NS_LOG_INFO("[Rank " << m_rankId << "] t=" << Simulator::Now().GetMicroSeconds() << "us"
                            << " event=ALLREDUCE_DONE AllReduce完成! sent=" << m_sentPktNum
                            << " recv_aggregated=" << m_recvPktNum << " retx=" << m_retxPktNum);
            }
        }
    }
}

// 单一滑动窗口：在 [m_windowBase, m_windowBase+m_windowSize) 范围内，
// 从右沿(m_nextSeq)补发尚未发送的新包，使在途包数维持到窗口上限。
//   idx = seq % windowSize （窗口宽度恰为 windowSize，窗口内 idx 唯一）
//   ver = (seq / windowSize) % 2 （与原逐代翻转一致，交换机聚合逻辑不变）
void INCSenderApp::FillWindow() {
    while (m_nextSeq < m_windowBase + m_windowSize && m_nextSeq < m_pktNum) {
        uint32_t seq = m_nextSeq;
        uint32_t idx = seq % m_windowSize;
        uint8_t ver = (seq / m_windowSize) % 2;
        SendPacketWithTimer(seq, idx, ver);
        m_nextSeq++;
    }
}

// 发送数据包并启动重传定时器
void INCSenderApp::SendPacketWithTimer(uint32_t seq, uint32_t idx, uint8_t ver) {
    Ptr<Packet> packet = Create<Packet>(m_pktPayloadSize);

    INCTag tag;
    tag.SetSeq(seq);
    tag.SetIdx(idx);
    tag.SetWid(m_rankId);
    tag.SetVer(ver);
    tag.SetType(DATA);

    packet->AddPacketTag(tag);

    std::clog << "[Rank " << m_rankId << "] t=" << Simulator::Now().GetMicroSeconds() << "us"
              << " event=DATA_SEND seq=" << seq
              << " idx=" << idx << " ver=" << (uint32_t)ver
              << " rtx=0\n";
    m_sendSocket->Send(packet);
    m_sentPktNum++;
    
    // 保存待确认包信息
    PendingPktInfo info;
    info.seq = seq;
    info.idx = idx;
    info.ver = ver;
    m_pendingPkts[seq] = info;
    
    // 启动重传定时器
    EventId timerId = Simulator::Schedule(m_retxTimeout, 
                                          &INCSenderApp::RetransmitPacket, 
                                          this, seq);
    m_retxTimers[seq] = timerId;
}

// 取消指定 idx 的定时器
void INCSenderApp::CancelTimer(uint32_t seq) {
    auto it = m_retxTimers.find(seq);
    if (it != m_retxTimers.end()) {
        Simulator::Cancel(it->second);
        m_retxTimers.erase(it);
    }
    m_pendingPkts.erase(seq);
}

// ACK 专用：只取消该 seq 的 RTO 定时器，不删除 m_pendingPkts。
// 这样发送窗口(m_windowBase)仍由 AGGREGATED 推进，聚合池深度不变（安全），
// 但无损 rank 不会在等待广播期间误超时重传。
void INCSenderApp::CancelTimerOnly(uint32_t seq) {
    auto it = m_retxTimers.find(seq);
    if (it != m_retxTimers.end()) {
        Simulator::Cancel(it->second);
        m_retxTimers.erase(it);
    }
}

// 重传超时回调
void INCSenderApp::RetransmitPacket(uint32_t seq) {
    auto it = m_pendingPkts.find(seq);
    if (it == m_pendingPkts.end()) {
        return;  // 已经被取消
    }
    
    PendingPktInfo info = it->second;
    
    m_retxPktNum++;
    g_totalRetxPktNum++;
    std::clog << "[Rank " << m_rankId << "] t=" << Simulator::Now().GetMicroSeconds() << "us"
              << " event=RETX seq=" << info.seq << " type=DATA\n";
    
    // 重发包（ver 不变）
    Ptr<Packet> packet = Create<Packet>(m_pktPayloadSize);
    INCTag tag;
    tag.SetSeq(info.seq);
    tag.SetIdx(info.idx);
    tag.SetWid(m_rankId);
    tag.SetVer(info.ver);
    tag.SetType(DATA);
    tag.SetIsRetransmit(1);
    packet->AddPacketTag(tag);
    
    std::clog << "[Rank " << m_rankId << "] t=" << Simulator::Now().GetMicroSeconds() << "us"
              << " event=DATA_SEND seq=" << info.seq
              << " idx=" << info.idx << " ver=" << (uint32_t)info.ver
              << " rtx=1\n";
    m_sendSocket->Send(packet);
    // 注意：重传不增加 m_sentPktNum，因为是重发
    
    // 重新启动定时器
    EventId timerId = Simulator::Schedule(m_retxTimeout,
                                          &INCSenderApp::RetransmitPacket,
                                          this, seq);
    m_retxTimers[seq] = timerId;
}

// M1: NAK 触发的快速重传——取消旧 RTO 定时器并立即重发（复用 RetransmitPacket 逻辑）
void INCSenderApp::FastRetransmit(uint32_t seq) {
    auto it = m_retxTimers.find(seq);
    if (it != m_retxTimers.end()) {
        Simulator::Cancel(it->second);
        m_retxTimers.erase(it);
    }
    RetransmitPacket(seq);  // 重发 + 重新调度 RTO 定时器
}

class INCReceiverApp : public Application {
    public:
        INCReceiverApp() : m_rankNum(0) {}
        ~INCReceiverApp() {}

        void StartApplication() override;
        void StopApplication() override;

        void InitComm(std::vector<Address> peerAddrList, uint32_t switchPoolSize);
        void RecvAggPacket(Ptr<Socket> socket);
        void SendAggPacket(uint8_t ver, uint32_t idx);           // multicast 给所有 worker
        void SendAggPacketToOne(uint8_t ver, uint32_t idx, uint32_t wid);  // 单播给指定 worker
        void DetectGapAndNak(uint32_t wid, uint32_t seq);        // M1: 逐跳 gap 检测
        void SendNak(uint32_t wid, uint32_t missingSeq);         // M1: 发送 NAK 给 worker
        void SendAck(uint32_t wid, uint32_t seq);                // 收到 DATA 即回 ACK（与广播解耦）

        // 二维聚合池: m_aggPool[ver][idx]，ver ∈ {0, 1}
        std::vector<std::vector<AggSlot>> m_aggPool;
        
        // 三维 seen 数组: m_seen[ver][idx][wid]
        std::vector<std::vector<std::vector<bool>>> m_seen;
        
        uint32_t m_switchPoolSize;
        uint32_t m_rankNum;  // worker 数量

        Ptr<Socket> m_sendSocket;
        Ptr<Socket> m_recvSocket;
        uint16_t m_port;

        std::vector<Address> m_peerAddrList;

        // === M1: 逐跳 gap 检测 + NAK 所需的每-worker 状态 ===
        std::vector<std::set<uint32_t>> m_receivedPsn;  // 每 worker 已到达 switch 的 DATA seq
        std::vector<std::set<uint32_t>> m_missingPsn;   // 每 worker 已暴露但尚未到达的 DATA seq
        std::vector<std::map<uint32_t, uint32_t>> m_nakCount; // 每 worker/seq 的 NAK 发送次数
        std::vector<uint32_t> m_maxSeenPsn;             // 每 worker 已观察到的最大 DATA seq
        std::vector<bool> m_hasMaxSeenPsn;              // maxSeenPsn 是否已初始化
};

void INCReceiverApp::StartApplication() {
    //1. 设置recviver socket
    m_recvSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_recvSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024)); // 1GB
    m_recvSocket->Bind(InetSocketAddress(Ipv4Address::GetAny(), m_port));
    m_recvSocket->SetRecvCallback(MakeCallback(&INCReceiverApp::RecvAggPacket, this));

    m_sendSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_sendSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_sendSocket->Bind();
}

void INCReceiverApp::StopApplication() {
    if(m_sendSocket) m_sendSocket->Close();
    if(m_recvSocket) m_recvSocket->Close();
}

void INCReceiverApp::InitComm(std::vector<Address> peerAddrList, uint32_t switchPoolSize) {
    m_peerAddrList = peerAddrList;
    m_switchPoolSize = switchPoolSize;
    m_rankNum = m_peerAddrList.size();

    // 初始化二维聚合池: m_aggPool[ver][idx]
    m_aggPool.resize(2);  // 两个版本
    for (uint32_t ver = 0; ver < 2; ver++) {
        m_aggPool[ver].resize(m_switchPoolSize);
        for (uint32_t idx = 0; idx < m_switchPoolSize; idx++) {
            m_aggPool[ver][idx].SetRankNum(m_rankNum);
        }
    }
    
    // 初始化三维 seen 数组: m_seen[ver][idx][wid]
    m_seen.resize(2);
    for (uint32_t ver = 0; ver < 2; ver++) {
        m_seen[ver].resize(m_switchPoolSize);
        for (uint32_t idx = 0; idx < m_switchPoolSize; idx++) {
            m_seen[ver][idx].resize(m_rankNum, false);
        }
    }

    // === M1: 每-worker 的 gap 检测状态 ===
    m_receivedPsn.assign(m_rankNum, std::set<uint32_t>());
    m_missingPsn.assign(m_rankNum, std::set<uint32_t>());
    m_nakCount.assign(m_rankNum, std::map<uint32_t, uint32_t>());
    m_maxSeenPsn.assign(m_rankNum, 0);
    m_hasMaxSeenPsn.assign(m_rankNum, false);
}

void INCReceiverApp::RecvAggPacket(Ptr<Socket> socket) {
    Ptr<Packet> packet;
    while ((packet = socket->Recv())) {

        INCTag tag;
        packet->PeekPacketTag(tag);
        
        uint8_t ver = tag.GetVer();
        uint32_t idx = tag.GetIdx();
        uint32_t wid = tag.GetWid();
        uint32_t seq = tag.GetSeq();

        // === M1: 逐跳 gap 检测 + NAK（switch 只收 DATA）===
        DetectGapAndNak(wid, seq);

        // 论文 Mode-III：交换机一收到 DATA 就立即回 ACK（与聚合/广播解耦）。
        // 让无损 rank 立刻确认该包、取消其 RTO，不必等广播 → 消除冗余重传。
        SendAck(wid, seq);

        if (!m_seen[ver][idx][wid]) {
            // ===== 新包，正常聚合 =====
            m_seen[ver][idx][wid] = true;
            m_seen[(ver + 1) % 2][idx][wid] = false;  // 清除对方版本的标记
            
            bool isCompleted = m_aggPool[ver][idx].AggPkt(packet);
            
            if (isCompleted) {
                // 全部到齐，multicast 给所有 worker
                /*NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << wid 
                            << " 收到所有包，完成聚合");*/
                SendAggPacket(ver, idx);
            }
            // 否则丢弃，等待其他 worker 的包
        } else {
            // ===== 重传包 =====
            if (m_aggPool[ver][idx].IsCompleted()) {
                // 该轮已完成，返回缓存结果给这个 worker
                SendAggPacketToOne(ver, idx, wid);
            }
            // 否则丢弃（还在聚合中，忽略重复）
        }
    }
}

// multicast 聚合结果给所有 worker
void INCReceiverApp::SendAggPacket(uint8_t ver, uint32_t idx) {
    Ptr<Packet> packet = m_aggPool[ver][idx].GetAggPkt();

    INCTag tag;
    packet->RemovePacketTag(tag);
    tag.SetType(AGGREGATED);
    packet->AddPacketTag(tag);

    // 循环发送回给所有 rank
    for (const Address& addr : m_peerAddrList) {
        Ptr<Packet> copy = packet->Copy();
        m_sendSocket->SendTo(copy, 0, addr);
    }
    // 注意：不再调用 ReSet()，因为需要保留缓存供重传查询
}

// 单播聚合结果给指定 worker（用于重传响应）
void INCReceiverApp::SendAggPacketToOne(uint8_t ver, uint32_t idx, uint32_t wid) {
    Ptr<Packet> packet = m_aggPool[ver][idx].GetAggPkt();

    INCTag tag;
    packet->RemovePacketTag(tag);
    tag.SetType(AGGREGATED);
    packet->AddPacketTag(tag);

    // 只发送给指定的 worker
    Ptr<Packet> copy = packet->Copy();
    m_sendSocket->SendTo(copy, 0, m_peerAddrList[wid]);
    /*
    NS_LOG_INFO("[" << Simulator::Now().GetSeconds() << "s] Switch 响应重传: "
                << "ver=" << (uint32_t)ver << ", idx=" << idx << ", wid=" << wid);
    */
}

// M1: 逐跳 gap 检测。switch 为每个缺失 psn 维护 NAK 计数器。
// 每收到一个 DATA seq，就对所有 psn < seq、尚未收到、且 NAK 计数器 <= 当前 seq 的 NAK 计数器的包发送 NAK，
// 并将对应 psn 的 NAK 计数器加 1；端侧收到 NAK 后重传对应 DATA。
void INCReceiverApp::DetectGapAndNak(uint32_t wid, uint32_t seq) {
    auto& received = m_receivedPsn[wid];
    auto& missing = m_missingPsn[wid];
    auto& nakCount = m_nakCount[wid];

    received.insert(seq);
    missing.erase(seq);

    if (!m_hasMaxSeenPsn[wid]) {
        for (uint32_t missingSeq = 0; missingSeq < seq; ++missingSeq) {
            if (received.find(missingSeq) == received.end()) {
                missing.insert(missingSeq);
            }
        }
        m_maxSeenPsn[wid] = seq;
        m_hasMaxSeenPsn[wid] = true;
    } else if (seq > m_maxSeenPsn[wid]) {
        for (uint32_t missingSeq = m_maxSeenPsn[wid] + 1; missingSeq < seq; ++missingSeq) {
            if (received.find(missingSeq) == received.end()) {
                missing.insert(missingSeq);
            }
        }
        m_maxSeenPsn[wid] = seq;
    }

    uint32_t currentNakCount = nakCount[seq];
    auto it = missing.begin();
    while (it != missing.end() && *it < seq) {
        uint32_t missingSeq = *it;
        uint32_t& count = nakCount[missingSeq];
        if (count <= currentNakCount) {
            SendNak(wid, missingSeq);
            count++;
        }
        ++it;
    }
}

// M1: 向指定 worker 发送 NAK（携带缺失的 seq）。NAK 不参与丢包模型（只丢 DATA）。
void INCReceiverApp::SendNak(uint32_t wid, uint32_t missingSeq) {
    Ptr<Packet> packet = Create<Packet>(0);
    INCTag tag;
    tag.SetSeq(missingSeq);
    tag.SetWid(wid);
    tag.SetType(NAK);
    packet->AddPacketTag(tag);
    NS_LOG_INFO("[Switch] t=" << Simulator::Now().GetMicroSeconds() << "us"
                << " event=NAK_SEND dst=Rank" << wid
                << " seq=" << missingSeq);
    m_sendSocket->SendTo(packet, 0, m_peerAddrList[wid]);
}

// 向指定 worker 发送 ACK（携带已收到的 seq）。ACK 不参与丢包模型（只丢 DATA）。
// 语义：交换机已收到该 seq → worker 取消其 RTO 定时器（防误超时重传）。
void INCReceiverApp::SendAck(uint32_t wid, uint32_t seq) {
    Ptr<Packet> packet = Create<Packet>(0);
    INCTag tag;
    tag.SetSeq(seq);
    tag.SetWid(wid);
    tag.SetType(ACK);
    packet->AddPacketTag(tag);
    m_sendSocket->SendTo(packet, 0, m_peerAddrList[wid]);
}

int main(int argc, char *argv[]) {

 

    LogComponentEnable("INCSwitchMLAllReduceTest", LOG_LEVEL_INFO);

    // ========== 配置参数 ==========
    uint32_t numWorkers = 8;           // 8个worker
    uint16_t port = 9999;
    
    // 初始化丢包日志管理器
    PacketDropLogger::Instance().SetRankNum(numWorkers);
    uint64_t dataSize = 0;             // [必填] 总数据量(字节)，无默认
    uint32_t payloadSize = 1400;       // 每包 payload 大小 (bytes)
    uint32_t windowSize = 512;          // switch pool size / window size
    uint32_t retxTimeout = 0;          // [必填] 重传超时(微秒)，无默认
    uint32_t packetLossLinkNum = 0;    // [必填] 丢包链路数量，无默认
    // double packetLossRate = 0.1;    // 改用确定性丢包模型

    // ========== 命令行参数：全部必填，无默认值 ==========
    double lossRate = -1.0;            // [必填] 丢包率 (0~1，0=不丢包)；-1=未提供
    CommandLine cmd(__FILE__);
    cmd.AddValue("lossRate", "[必填] 每条丢包链路的丢包率 (0~1)，0=不丢包", lossRate);
    cmd.AddValue("lossLinkNum", "[必填] 丢包链路数量 (1~numWorkers)", packetLossLinkNum);
    cmd.AddValue("retxTimeoutUs", "[必填] 重传超时时间(微秒)", retxTimeout);
    cmd.AddValue("dataBytes", "[必填] 总数据量(字节)", dataSize);
    cmd.Parse(argc, argv);

    // 必填校验：任一未显式提供则报错退出
    if (lossRate < 0.0 || packetLossLinkNum == 0 || retxTimeout == 0 || dataSize == 0) {
        std::cerr << "错误：以下参数必填且无默认值：\n"
                  << "  --lossRate=<0~1>     (0=不丢包)\n"
                  << "  --lossLinkNum=<>=1>\n"
                  << "  --retxTimeoutUs=<>0>\n"
                  << "  --dataBytes=<>0>\n"
                  << "示例：--lossRate=0.001 --lossLinkNum=1 --retxTimeoutUs=1000 --dataBytes=67110400\n";
        return 1;
    }
    uint32_t lossInterval = (lossRate > 0.0) ? (uint32_t)std::lround(1.0 / lossRate) : 0;

    // 创建日志文件
    // std::ofstream logFile("INC1_simulation_" + std::to_string(packetLossLinkNum) + "linkloss.log");

    // 重定向 std::clog 到文件
    // std::clog.rdbuf(logFile.rdbuf());

    // ========== 创建节点 ==========
    // node 0: switch (INCReceiverApp)
    // node 1-4: workers (INCSenderApp)
    NodeContainer allNodes;
    allNodes.Create(numWorkers + 1);  // 5个节点

    Ptr<Node> switchNode = allNodes.Get(0);
    NodeContainer workerNodes;
    for (uint32_t i = 1; i <= numWorkers; i++) {
        workerNodes.Add(allNodes.Get(i));
    }

    // ========== 安装网络协议栈 ==========
    InternetStackHelper internet;
    internet.Install(allNodes);

    // ========== 创建星型拓扑：每个worker连接到switch ==========
    PointToPointHelper p2p;
    p2p.SetDeviceAttribute("DataRate", StringValue("100Gbps"));
    p2p.SetChannelAttribute("Delay", StringValue("1us"));
    p2p.SetQueue("ns3::DropTailQueue<Packet>", "MaxSize", StringValue("1000000p"));

    Ipv4AddressHelper address;

    // 存储switch侧的地址（worker发送的目标）
    Ipv4Address switchAddr;
    // 存储每个worker的地址（switch回复的目标）
    std::vector<Ipv4Address> workerAddrs(numWorkers);
    // 存储所有网络设备，用于设置丢包模型
    std::vector<NetDeviceContainer> allDevices;

    // 创建 worker -> switch 的链路
    for (uint32_t i = 0; i < numWorkers; i++) {
        // 创建 worker[i] -> switch 的链路
        NetDeviceContainer devices = p2p.Install(workerNodes.Get(i), switchNode);
        allDevices.push_back(devices);

        // 为这条链路分配 IP 地址 (10.0.i.0/24)
        std::ostringstream subnet;
        subnet << "10.0." << i << ".0";
        address.SetBase(subnet.str().c_str(), "255.255.255.0");
        Ipv4InterfaceContainer interfaces = address.Assign(devices);

        // worker地址是 interfaces.GetAddress(0)
        // switch地址是 interfaces.GetAddress(1)
        workerAddrs[i] = interfaces.GetAddress(0);
        
        // 所有worker都发往同一个switch，这里用最后一条链路的switch地址
        // 实际上switch在每条链路上都有不同的IP，但都能接收
        if (i == 0) {
            switchAddr = interfaces.GetAddress(1);
        }

        /*NS_LOG_INFO("Link Worker" << i << " <-> Switch: " 
                    << interfaces.GetAddress(0) << " <-> " << interfaces.GetAddress(1));
                    */
    }
    // ========== 设置丢包模型（概率累积，仅 DATA 包） ==========
    NS_LOG_INFO("[Simulator] t=" << Simulator::Now().GetMicroSeconds() << "us"
                << " event=LOSS_CONFIG lossRate=" << lossRate
                << " model=accumulator_DATA_only");
    (void)lossInterval;
    for (uint32_t i = 0; i < packetLossLinkNum; i++) {
        // Worker 端接收丢包（worker -> switch 上行方向）
        Ptr<DeterministicErrorModel> workerErrorModel = CreateObject<DeterministicErrorModel>();
        workerErrorModel->SetLossRate(lossRate);
        workerErrorModel->SetLossAccumInit((double)i / packetLossLinkNum);  // 多链路错相位
        workerErrorModel->SetLinkId(i);

        allDevices[i].Get(1)->SetAttribute("ReceiveErrorModel", PointerValue(workerErrorModel));
    }

    // 启用全局路由
    Ipv4GlobalRoutingHelper::PopulateRoutingTables();

    // ========== 为switch创建 INCReceiverApp ==========
    // 收集所有worker的地址
    std::vector<Address> peerAddrList;
    for (uint32_t i = 0; i < numWorkers; i++) {
        peerAddrList.push_back(InetSocketAddress(workerAddrs[i], port));
    }

    Ptr<INCReceiverApp> receiverApp = CreateObject<INCReceiverApp>();
    receiverApp->m_port = port;
    receiverApp->InitComm(peerAddrList, windowSize);
    
    switchNode->AddApplication(receiverApp);
    receiverApp->SetStartTime(Seconds(1.0));
    receiverApp->SetStopTime(Seconds(30.0));
    
    NS_LOG_INFO("[Switch] t=" << Simulator::Now().GetMicroSeconds() << "us"
                << " event=CONFIG_DONE poolSize=" << windowSize
                << " workerNum=" << numWorkers);

    // ========== 为每个worker创建 INCSenderApp ==========
    for (uint32_t i = 0; i < numWorkers; i++) {
        Ptr<INCSenderApp> senderApp = CreateObject<INCSenderApp>();
        
        // 设置通信参数
        senderApp->m_rankId = i;
        senderApp->m_port = port;
        senderApp->m_peerAddr = InetSocketAddress(switchAddr, port);
        senderApp->InitComm(dataSize, payloadSize, windowSize, retxTimeout);

        workerNodes.Get(i)->AddApplication(senderApp);
        senderApp->SetStartTime(Seconds(1.0));
        senderApp->SetStopTime(Seconds(2.0));

        NS_LOG_INFO("[Rank " << i << "] t=" << Simulator::Now().GetMicroSeconds() << "us"
                    << " event=CONFIG_DONE switchAddr=" << switchAddr);
    }

    NS_LOG_INFO("[Simulator] t=" << Simulator::Now().GetMicroSeconds() << "us event=SIM_START name=INC_AllReduce");

    // 运行仿真
    Simulator::Run();
    std::clog << "[Simulator] t=" << Simulator::Now().GetMicroSeconds() << "us"
              << " event=RETX_SUMMARY total=" << g_totalRetxPktNum << "\n";
    
    // 打印仿真结束时间
    NS_LOG_INFO("[Simulator] t=" << Simulator::Now().GetMicroSeconds() << "us event=SIM_END name=INC_AllReduce");
    NS_LOG_INFO("[Simulator] t=" << Simulator::Now().GetSeconds() << "s event=SIM_END_SECONDS");
    
    Simulator::Destroy();
    
    // 写入丢包日志文件
    // PacketDropLogger::Instance().WriteToFile("INC1_packet_drop.log");

    return 0;
}

#endif