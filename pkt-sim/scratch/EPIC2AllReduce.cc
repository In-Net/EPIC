#ifndef EPIC2_ALLREDUCE_H
#define EPIC2_ALLREDUCE_H

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
#include <map>
#include <fstream>
#include <sstream>

using namespace ns3;

NS_LOG_COMPONENT_DEFINE("EPIC2AllReduceTest");

enum PacketType : uint8_t {
    DATA = 0,
    AGGREGATED = 1,
    ACK = 2,
};

class EPIC2AllReduceTag : public Tag{
    public:

        EPIC2AllReduceTag() : m_seq(0), m_idx(0), m_wid(0), m_ver(0), m_type(DATA), m_isRetransmit(0) {}

        static TypeId GetTypeId() {
            static TypeId tid = TypeId("ns3::EPIC2AllReduceTag")
                .SetParent<Tag>()
                .AddConstructor<EPIC2AllReduceTag>();
            return tid;
        }

        TypeId GetInstanceTypeId() const override{ 
            return GetTypeId(); 
        }

        void SetType(PacketType type) { m_type = type; }
        PacketType GetType() const { return m_type; }

        bool IsData() const { return m_type == DATA; }
        bool IsAggregated() const { return m_type == AGGREGATED; }
        bool IsAck() const { return m_type == ACK; }
    
        void SetIsRetransmit(uint8_t isRetransmit) { m_isRetransmit = isRetransmit; }
        uint8_t GetIsRetransmit() const { return m_isRetransmit; }

        void SetSeq(uint32_t seq) { m_seq = seq; }
        uint32_t GetSeq() const { return m_seq; }

        void SetIdx(uint32_t idx) { m_idx = idx; }
        uint32_t GetIdx() const { return m_idx; }

        void SetWid(uint32_t wid) { m_wid = wid; }
        uint32_t GetWid() const { return m_wid; }

        void SetVer(uint8_t ver) { m_ver = ver; }
        uint8_t GetVer() const { return m_ver; }
        
        uint32_t GetSerializedSize() const override {
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
            os << "EPIC2AllReduceTag: seq=" << m_seq 
               << ", idx=" << m_idx 
               << ", wid=" << m_wid
               << ", ver=" << (uint32_t)m_ver
               << ", type=" << (m_type == DATA ? "DATA" : (m_type == AGGREGATED ? "AGGREGATED" : "ACK"))
               << ", isRetransmit=" << (int)m_isRetransmit;
        }

    private:
        uint32_t m_seq;
        uint32_t m_idx;
        uint32_t m_wid;
        uint8_t m_ver;
        PacketType m_type;
        uint8_t m_isRetransmit;
};

class AggSlot {
    public:

        void SetRankNum(uint32_t n) { m_num = n; }
        
        bool AggPkt(Ptr<Packet> pkt) {
            m_count = (m_count + 1) % m_num;
            
            if (m_count == 1) {
                m_pkt = pkt;
            } else {
                m_pkt = pkt;
            }
            
            return (m_count == 0);
        }

        Ptr<Packet> GetAggPkt() {
            return m_pkt;
        }

        bool IsCompleted() const { return m_count == 0; }

        uint32_t m_count = 0;
        uint32_t m_num = 0;
        Ptr<Packet> m_pkt = nullptr;
};

struct PendingPktInfo {
    uint32_t seq;
    uint32_t idx;
    uint8_t ver;
};

class EPIC2AllReduceSenderApp : public Application {
    public:
        EPIC2AllReduceSenderApp() : m_rankId(0), m_sentPktNum(0), m_recvPktNum(0), m_recvAckNum(0),
                         m_retxTimeout(MicroSeconds(100)) {};
        ~EPIC2AllReduceSenderApp(){};

        void StartApplication() override;
        void StopApplication() override;

        void InitComm(uint64_t dataSize, uint32_t pktPayloadSize, uint32_t windowSize, uint32_t retxTimeout);
        void StartAllReduce();
        void RecvAggPacket(Ptr<Socket> socket);
        void SendAckToSwitch(uint32_t seq, uint32_t idx, uint8_t ver);
        void SendNextOffSetPacket(uint32_t seq, uint32_t idx, uint8_t recvVer);
        
        void RetransmitPacket(uint32_t idx);
        void SendPacketWithTimer(uint32_t seq, uint32_t idx, uint8_t ver);
        void CancelTimer(uint32_t idx);

        uint32_t m_rankId;
        uint64_t m_dataSize;
        uint32_t m_pktPayloadSize;
        uint32_t m_pktNum;
        uint32_t m_sentPktNum;
        uint32_t m_recvPktNum;
        uint32_t m_recvAckNum;
        uint32_t m_windowSize;

        Ptr<Socket> m_sendSocket;
        Ptr<Socket> m_recvSocket;
        Address m_peerAddr;
        uint16_t m_port;

        Time m_retxTimeout;
        std::map<uint32_t, EventId> m_retxTimers;
        std::map<uint32_t, PendingPktInfo> m_pendingPkts;
        uint32_t m_retxPktNum;
};

void EPIC2AllReduceSenderApp::StartApplication() {
    m_recvSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_recvSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024)); // 1GB
    m_recvSocket->Bind(InetSocketAddress(Ipv4Address::GetAny(), m_port));
    m_recvSocket->SetRecvCallback(MakeCallback(&EPIC2AllReduceSenderApp::RecvAggPacket, this));

    m_sendSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_sendSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_sendSocket->Bind();
    m_sendSocket->Connect(m_peerAddr);

    Simulator::Schedule(MicroSeconds(1), &EPIC2AllReduceSenderApp::StartAllReduce, this);
}

void EPIC2AllReduceSenderApp::StopApplication() {
    
    for (auto& pair : m_retxTimers) {
        Simulator::Cancel(pair.second);
    }
    m_retxTimers.clear();
    m_pendingPkts.clear();
    
    if(m_sendSocket) m_sendSocket->Close();
    if(m_recvSocket) m_recvSocket->Close();
}

void EPIC2AllReduceSenderApp::InitComm(uint64_t dataSize, uint32_t pktPayloadSize, uint32_t windowSize, uint32_t retxTimeout) {
    m_dataSize = dataSize;
    m_pktPayloadSize = pktPayloadSize;
    m_pktNum = dataSize / pktPayloadSize;
    m_windowSize = windowSize;
    m_retxTimeout = MicroSeconds(retxTimeout);
}

void EPIC2AllReduceSenderApp::StartAllReduce() {
    NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_rankId 
                << " start AllReduce");
    
    for (uint32_t i = 0; i < m_windowSize && i < m_pktNum; i++) {
        uint32_t seq = i;
        uint32_t idx = i % m_windowSize;
        uint8_t ver = 0;
        
        SendPacketWithTimer(seq, idx, ver);
    }
}

void EPIC2AllReduceSenderApp::RecvAggPacket(Ptr<Socket> socket) {
    Ptr<Packet> pkt;
    while ((pkt = socket->Recv())) {
        EPIC2AllReduceTag tag;
        pkt->PeekPacketTag(tag);
    
        if (tag.GetType() == AGGREGATED) {
            uint32_t seq = tag.GetSeq();
            uint32_t idx = tag.GetIdx();
            uint8_t ver = tag.GetVer();

            auto it = m_pendingPkts.find(seq);
            if (it == m_pendingPkts.end()) {
                continue;
            }

            /*NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_rankId 
                        << " Receive AGGREGATED packet: seq=" << seq << ", idx=" << idx << ", ver=" << (uint32_t)ver);*/
            
            m_recvPktNum++;
            
            SendAckToSwitch(seq, idx, ver);
        }
        else if (tag.GetType() == ACK) {
            uint32_t seq = tag.GetSeq();
            uint32_t idx = tag.GetIdx();
            uint8_t ver = tag.GetVer();

            auto it = m_pendingPkts.find(seq);
            if (it == m_pendingPkts.end()) {
                continue;
            }

            CancelTimer(seq);
            
            m_recvAckNum++;
            
            if (seq + m_windowSize < m_pktNum) {
                SendNextOffSetPacket(seq, idx, ver);
            }
            
            if (m_recvAckNum == m_pktNum) {
                NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_rankId 
                            << " AllReduce completed!");
            }
        }
        else {
            NS_LOG_INFO("Rank " << m_rankId << " Error: Receive Unknown Type Packet!");
        }
    }
}

void EPIC2AllReduceSenderApp::SendAckToSwitch(uint32_t seq, uint32_t idx, uint8_t ver) {
    Ptr<Packet> packet = Create<Packet>(0);

    EPIC2AllReduceTag tag;
    tag.SetSeq(seq);
    tag.SetIdx(idx);
    tag.SetWid(m_rankId);
    tag.SetVer(ver);
    tag.SetType(ACK);

    packet->AddPacketTag(tag);

    m_sendSocket->Send(packet);
}

void EPIC2AllReduceSenderApp::SendNextOffSetPacket(uint32_t seq, uint32_t idx, uint8_t recvVer) {
    uint32_t newSeq = seq + m_windowSize;
    uint8_t newVer = (recvVer + 1) % 2;
    
    SendPacketWithTimer(newSeq, idx, newVer);
}

void EPIC2AllReduceSenderApp::SendPacketWithTimer(uint32_t seq, uint32_t idx, uint8_t ver) {
    Ptr<Packet> packet = Create<Packet>(m_pktPayloadSize);

    EPIC2AllReduceTag tag;
    tag.SetSeq(seq);
    tag.SetIdx(idx);
    tag.SetWid(m_rankId);
    tag.SetVer(ver);
    tag.SetType(DATA);

    packet->AddPacketTag(tag);

    m_sendSocket->Send(packet);
    m_sentPktNum++;
    
    PendingPktInfo info;
    info.seq = seq;
    info.idx = idx;
    info.ver = ver;
    m_pendingPkts[seq] = info;
    
    EventId timerId = Simulator::Schedule(m_retxTimeout, 
                                          &EPIC2AllReduceSenderApp::RetransmitPacket, 
                                          this, seq);
    m_retxTimers[seq] = timerId;
}

void EPIC2AllReduceSenderApp::CancelTimer(uint32_t seq) {
    auto it = m_retxTimers.find(seq);
    if (it != m_retxTimers.end()) {
        Simulator::Cancel(it->second);
        m_retxTimers.erase(it);
    }
    m_pendingPkts.erase(seq);
}

void EPIC2AllReduceSenderApp::RetransmitPacket(uint32_t seq) {
    auto it = m_pendingPkts.find(seq);
    if (it == m_pendingPkts.end()) {
        return;
    }
    
    PendingPktInfo info = it->second;
    
    m_retxPktNum++;
    
    Ptr<Packet> packet = Create<Packet>(m_pktPayloadSize);
    EPIC2AllReduceTag tag;
    tag.SetSeq(info.seq);
    tag.SetIdx(info.idx);
    tag.SetWid(m_rankId);
    tag.SetVer(info.ver);
    tag.SetType(DATA);
    tag.SetIsRetransmit(1);
    packet->AddPacketTag(tag);
    
    m_sendSocket->Send(packet);
    
    EventId timerId = Simulator::Schedule(m_retxTimeout, 
                                          &EPIC2AllReduceSenderApp::RetransmitPacket, 
                                          this, seq);
    m_retxTimers[seq] = timerId;
}

class EPIC2AllReduceReceiverApp : public Application {
    public:
        EPIC2AllReduceReceiverApp() : m_rankNum(0) {}
        ~EPIC2AllReduceReceiverApp() {}

        void StartApplication() override;
        void StopApplication() override;

        void InitComm(std::vector<Address> peerAddrList, uint32_t switchPoolSize);
        void RecvAggPacket(Ptr<Socket> socket);
        void SendAggPacket(uint8_t ver, uint32_t idx);
        void SendAggPacketToOne(uint8_t ver, uint32_t idx, uint32_t wid);
        void SendAckToWorker(uint32_t seq, uint32_t idx, uint8_t ver, uint32_t wid);

        std::vector<std::vector<AggSlot>> m_aggPool;
        
        std::vector<std::vector<std::vector<bool>>> m_seen;
        
        uint32_t m_switchPoolSize;
        uint32_t m_rankNum;

        Ptr<Socket> m_sendSocket;
        Ptr<Socket> m_recvSocket;
        uint16_t m_port;

        std::vector<Address> m_peerAddrList;
};

void EPIC2AllReduceReceiverApp::StartApplication() {
    m_recvSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_recvSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024)); // 1GB
    m_recvSocket->Bind(InetSocketAddress(Ipv4Address::GetAny(), m_port));
    m_recvSocket->SetRecvCallback(MakeCallback(&EPIC2AllReduceReceiverApp::RecvAggPacket, this));

    m_sendSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_sendSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_sendSocket->Bind();
}

void EPIC2AllReduceReceiverApp::StopApplication() {
    if(m_sendSocket) m_sendSocket->Close();
    if(m_recvSocket) m_recvSocket->Close();
}

void EPIC2AllReduceReceiverApp::InitComm(std::vector<Address> peerAddrList, uint32_t switchPoolSize) {
    m_peerAddrList = peerAddrList;
    m_switchPoolSize = switchPoolSize;
    m_rankNum = m_peerAddrList.size();

    m_aggPool.resize(2);
    for (uint32_t ver = 0; ver < 2; ver++) {
        m_aggPool[ver].resize(m_switchPoolSize);
        for (uint32_t idx = 0; idx < m_switchPoolSize; idx++) {
            m_aggPool[ver][idx].SetRankNum(m_rankNum);
        }
    }
    
    m_seen.resize(2);
    for (uint32_t ver = 0; ver < 2; ver++) {
        m_seen[ver].resize(m_switchPoolSize);
        for (uint32_t idx = 0; idx < m_switchPoolSize; idx++) {
            m_seen[ver][idx].resize(m_rankNum, false);
        }
    }
}

void EPIC2AllReduceReceiverApp::RecvAggPacket(Ptr<Socket> socket) {
    Ptr<Packet> packet;
    while ((packet = socket->Recv())) {

        EPIC2AllReduceTag tag;
        packet->PeekPacketTag(tag);
        
        uint8_t ver = tag.GetVer();
        uint32_t idx = tag.GetIdx();
        uint32_t wid = tag.GetWid();
        uint32_t seq = tag.GetSeq();
        
        if (tag.GetType() == ACK) {
            SendAckToWorker(seq, idx, ver, wid);
            continue;
        }
        
        if (!m_seen[ver][idx][wid]) {
            m_seen[ver][idx][wid] = true;
            m_seen[(ver + 1) % 2][idx][wid] = false;
            
            bool isCompleted = m_aggPool[ver][idx].AggPkt(packet);
            
            if (isCompleted) {
                SendAggPacket(ver, idx);
            }
        } else {
            if (m_aggPool[ver][idx].IsCompleted()) {
                SendAggPacketToOne(ver, idx, wid);
            }
        }
    }
}

void EPIC2AllReduceReceiverApp::SendAggPacket(uint8_t ver, uint32_t idx) {
    Ptr<Packet> packet = m_aggPool[ver][idx].GetAggPkt();

    EPIC2AllReduceTag tag;
    packet->RemovePacketTag(tag);
    tag.SetType(AGGREGATED);
    packet->AddPacketTag(tag);

    for (const Address& addr : m_peerAddrList) {
        Ptr<Packet> copy = packet->Copy();
        m_sendSocket->SendTo(copy, 0, addr);
    }
}

void EPIC2AllReduceReceiverApp::SendAggPacketToOne(uint8_t ver, uint32_t idx, uint32_t wid) {
    Ptr<Packet> packet = m_aggPool[ver][idx].GetAggPkt();

    EPIC2AllReduceTag tag;
    packet->RemovePacketTag(tag);
    tag.SetType(AGGREGATED);
    packet->AddPacketTag(tag);

    Ptr<Packet> copy = packet->Copy();
    m_sendSocket->SendTo(copy, 0, m_peerAddrList[wid]);
}

void EPIC2AllReduceReceiverApp::SendAckToWorker(uint32_t seq, uint32_t idx, uint8_t ver, uint32_t wid) {
    Ptr<Packet> packet = Create<Packet>(0);

    EPIC2AllReduceTag tag;
    tag.SetSeq(seq);
    tag.SetIdx(idx);
    tag.SetWid(wid);
    tag.SetVer(ver);
    tag.SetType(ACK);

    packet->AddPacketTag(tag);

    m_sendSocket->SendTo(packet, 0, m_peerAddrList[wid]);
}

int main(int argc, char *argv[]) {
    LogComponentEnable("EPIC2AllReduceTest", LOG_LEVEL_INFO);

    uint32_t numWorkers = 8;
    uint16_t port = 9999;
    uint64_t dataSize = 1024 * 1024;
    uint32_t payloadSize = 1400;
    uint32_t windowSize = 512;
    uint32_t retxTimeout = 100;

    NodeContainer allNodes;
    allNodes.Create(numWorkers + 1);

    Ptr<Node> switchNode = allNodes.Get(0);
    NodeContainer workerNodes;
    for (uint32_t i = 1; i <= numWorkers; i++) {
        workerNodes.Add(allNodes.Get(i));
    }

    InternetStackHelper internet;
    internet.Install(allNodes);

    PointToPointHelper p2p;
    p2p.SetDeviceAttribute("DataRate", StringValue("100Gbps"));
    p2p.SetChannelAttribute("Delay", StringValue("1us"));
    p2p.SetQueue("ns3::DropTailQueue<Packet>", "MaxSize", StringValue("1000000p"));

    Ipv4AddressHelper address;

    Ipv4Address switchAddr;
    std::vector<Ipv4Address> workerAddrs(numWorkers);
    std::vector<NetDeviceContainer> allDevices;

    for (uint32_t i = 0; i < numWorkers; i++) {
        NetDeviceContainer devices = p2p.Install(workerNodes.Get(i), switchNode);
        allDevices.push_back(devices);

        std::ostringstream subnet;
        subnet << "10.0." << i << ".0";
        address.SetBase(subnet.str().c_str(), "255.255.255.0");
        Ipv4InterfaceContainer interfaces = address.Assign(devices);

        workerAddrs[i] = interfaces.GetAddress(0);
        
        if (i == 0) {
            switchAddr = interfaces.GetAddress(1);
        }

    }

    Ipv4GlobalRoutingHelper::PopulateRoutingTables();

    std::vector<Address> peerAddrList;
    for (uint32_t i = 0; i < numWorkers; i++) {
        peerAddrList.push_back(InetSocketAddress(workerAddrs[i], port));
    }

    Ptr<EPIC2AllReduceReceiverApp> receiverApp = CreateObject<EPIC2AllReduceReceiverApp>();
    receiverApp->m_port = port;
    receiverApp->InitComm(peerAddrList, windowSize);
    
    switchNode->AddApplication(receiverApp);
    receiverApp->SetStartTime(Seconds(1.0));
    receiverApp->SetStopTime(Seconds(30.0));

    for (uint32_t i = 0; i < numWorkers; i++) {
        Ptr<EPIC2AllReduceSenderApp> senderApp = CreateObject<EPIC2AllReduceSenderApp>();
        
        senderApp->m_rankId = i;
        senderApp->m_port = port;
        senderApp->m_peerAddr = InetSocketAddress(switchAddr, port);
        senderApp->InitComm(dataSize, payloadSize, windowSize, retxTimeout);

        workerNodes.Get(i)->AddApplication(senderApp);
        senderApp->SetStartTime(Seconds(1.0));
        senderApp->SetStopTime(Seconds(2.0));
    }

    NS_LOG_INFO("========== EPIC2 AllReduce Start ==========");

    Simulator::Run();
    
    NS_LOG_INFO("========== EPIC2 AllReduce End ==========");
    
    Simulator::Destroy();

    return 0;
}

#endif