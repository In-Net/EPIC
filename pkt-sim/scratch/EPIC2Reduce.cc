#ifndef EPIC2_REDUCE_H
#define EPIC2_REDUCE_H

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

#include <algorithm>
#include <vector>
#include <map>

using namespace ns3;

NS_LOG_COMPONENT_DEFINE("EPIC2ReduceTest");

enum PacketType : uint8_t {
    DATA = 0,
    AGGREGATED = 1,
    ACK = 2,
};

class EPIC2ReduceTag : public Tag{
    public:

        EPIC2ReduceTag() {m_seq = 0; m_idx = 0; m_type = DATA;}

        static TypeId GetTypeId() {
            static TypeId tid = TypeId("ns3::EPIC2ReduceTag")
                .SetParent<Tag>()
                .AddConstructor<EPIC2ReduceTag>();
            return tid;
        }

        TypeId GetInstanceTypeId() const override{ 
            return GetTypeId(); 
        }

        void SetType(PacketType type) { m_type = type; }
        PacketType GetType() const { return m_type; }

        bool IsData() const { return m_type == DATA; }
        bool IsAggregated() const { return m_type == AGGREGATED; }
    

        void SetSeq(uint32_t seq) { m_seq = seq; }
        uint32_t GetSeq() const { return m_seq; }

        void SetIdx(uint32_t idx) { m_idx = idx; }
        uint32_t GetIdx() const { return m_idx; }
        
        uint32_t GetSerializedSize() const override {
            return 2 * sizeof(uint32_t) + sizeof(uint8_t);
        }

        void Serialize(TagBuffer i) const override{
            i.WriteU32(m_seq);
            i.WriteU32(m_idx);
            i.WriteU8(m_type);
        }

        void Deserialize(TagBuffer i) override{
            m_seq = i.ReadU32();
            m_idx = i.ReadU32();
            m_type = static_cast<PacketType>(i.ReadU8());
        }

        void Print(std::ostream& os) const override {
            os << "EPIC2ReduceTag: seq=" << m_seq 
               << ", idx=" << m_idx 
               << ", type=" << (m_type == DATA ? "DATA" : "AGGREGATED");
        }

    private:
        uint32_t m_seq;
        uint32_t m_idx;
        PacketType m_type;
};

class AggSlot {
    public:

        void SetRankNum(uint32_t n) {m_num = n;}
        
        bool AggPkt(Ptr<Packet> pkt) {
            m_count++;
            if (m_count == m_num) {
                m_pkt = pkt;
                return true;
            }
            return false;
        }

        Ptr<Packet> GetAggPkt() {
            return m_pkt;
        }

        void ReSet() {m_idx = 0; m_count = 0; m_pkt = nullptr;}

        uint32_t m_idx = 0;
        uint32_t m_count = 0;
        uint32_t m_num = 0;

        Ptr<Packet> m_pkt = nullptr;
};

class EPIC2ReduceSenderApp : public Application {
    public:
        EPIC2ReduceSenderApp() : m_rankId(0), m_isRoot(false), m_sentPktNum(0), m_recvPktNum(0) {};
        ~EPIC2ReduceSenderApp(){};

        void StartApplication() override;
        void StopApplication() override;

        void InitComm(uint64_t dataSize, uint32_t pktPayloadSize, uint32_t m_windowSize, 
            std::map<uint32_t, Address> rankAddrMap);
        void StartReduce();
        void RecvPacket(Ptr<Socket> socket);
        void RecvAckPacket(Ptr<Socket> socket);
        void RecvAggPacket(Ptr<Socket> socket);
        void SendAckPacket(uint32_t seq, uint32_t idx);
        void SendNextOffSetPacket(uint32_t seq, uint32_t idx);

        uint32_t m_rankId;
        uint32_t rootRankId;
        bool m_isRoot;
        uint64_t m_dataSize;
        uint32_t m_pktPayloadSize;
        uint32_t m_pktNum;
        uint32_t m_sentPktNum;
        uint32_t m_recvPktNum;
        uint32_t m_windowSize;

        Ptr<Socket> m_sendSocket;
        Ptr<Socket> m_recvSocket;
        Address m_peerAddr;
        uint16_t m_port;

        std::map<uint32_t, Address> m_rankAddrMap;
};

void EPIC2ReduceSenderApp::StartApplication() {
    m_recvSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_recvSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024)); // 1GB
    m_recvSocket->Bind(InetSocketAddress(Ipv4Address::GetAny(), m_port));
    m_recvSocket->SetRecvCallback(MakeCallback(&EPIC2ReduceSenderApp::RecvPacket, this));

    m_sendSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_sendSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_sendSocket->Bind();
    m_sendSocket->Connect(m_peerAddr);

    Simulator::Schedule(MicroSeconds(1), &EPIC2ReduceSenderApp::StartReduce, this);
}

void EPIC2ReduceSenderApp::StopApplication() {
    if(m_sendSocket) m_sendSocket->Close();
    if(m_recvSocket) m_recvSocket->Close();
}

void EPIC2ReduceSenderApp::InitComm(uint64_t dataSize, uint32_t pktPayloadSize, uint32_t windowSize, 
    std::map<uint32_t, Address> rankAddrMap) {
    m_dataSize = dataSize;
    m_pktPayloadSize = pktPayloadSize;
    m_pktNum = dataSize / pktPayloadSize;

    m_windowSize = windowSize;
    m_rankAddrMap = rankAddrMap;
}

void EPIC2ReduceSenderApp::StartReduce() {
    if (m_isRoot) {
        NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_rankId 
                    << " (Root) start Reduce");
    } else {
        NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_rankId 
                    << " start Reduce");
    }
    
    if (!m_isRoot) {
        for (uint32_t i = 0; i < m_windowSize && i < m_pktNum; i++) {
            Ptr<Packet> packet = Create<Packet>(m_pktPayloadSize);
            EPIC2ReduceTag tag;
            tag.SetSeq(i);
            tag.SetIdx( i % m_windowSize );
            tag.SetType(DATA);

            packet->AddPacketTag(tag);

            m_sendSocket->Send(packet);
            m_sentPktNum++;
        }
    }
}

void EPIC2ReduceSenderApp::RecvPacket(Ptr<Socket> socket) {
    if (m_isRoot) {
        RecvAggPacket(socket);
    }
    else{
        RecvAckPacket(socket);
    }
}

void EPIC2ReduceSenderApp::RecvAggPacket(Ptr<Socket> socket) {
    Ptr<Packet> pkt;
    while ((pkt = socket->Recv())) {
        EPIC2ReduceTag tag;
        pkt->PeekPacketTag(tag);
    
        if (tag.GetType() != AGGREGATED) {
            NS_LOG_INFO("Rank " << m_rankId << " receive non-AGGREGATED packet!");
        }
        else {
            m_recvPktNum++;
            uint32_t seq = tag.GetSeq();
            uint32_t idx = tag.GetIdx();
            
            SendAckPacket(seq, idx);
            
            if (m_recvPktNum == m_pktNum) {
                NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_rankId 
                            << " (Root) Reduce completed!");
            
            }
        }
    }
}

void EPIC2ReduceSenderApp::SendAckPacket(uint32_t seq, uint32_t idx) {

    for (auto pair : m_rankAddrMap) {
        uint32_t rankId = pair.first;
        Address addr = pair.second;
        if (rankId != rootRankId) {
            EPIC2ReduceTag tag;
            tag.SetSeq(seq);
            tag.SetIdx(idx);
            tag.SetType(ACK);

            Ptr<Packet> ackPacket = Create<Packet>(0);
            ackPacket->AddPacketTag(tag);
            m_sendSocket->SendTo(ackPacket, 0, addr);
        }
    }
}

void EPIC2ReduceSenderApp::RecvAckPacket(Ptr<Socket> socket) {
    Ptr<Packet> pkt;
    while ((pkt = socket->Recv())) {
        EPIC2ReduceTag tag;
        pkt->PeekPacketTag(tag);
    
        if (tag.GetType() != ACK) {
            NS_LOG_INFO("Rank " << m_rankId << " receive non-ACK packet!");
        }
        else {
            m_recvPktNum++;
            uint32_t seq = tag.GetSeq();
            uint32_t idx = tag.GetIdx();
            
            if (seq + m_windowSize < m_pktNum) {
                SendNextOffSetPacket(seq, idx);
            }
        }
    }
}

void EPIC2ReduceSenderApp::SendNextOffSetPacket(uint32_t seq, uint32_t idx) {
    Ptr<Packet> packet = Create<Packet>(m_pktPayloadSize);

    EPIC2ReduceTag tag;
    tag.SetSeq(seq + m_windowSize);
    tag.SetIdx(idx);
    tag.SetType(DATA);

    packet->AddPacketTag(tag);

    m_sendSocket->Send(packet);
    m_sentPktNum++;
}

class EPIC2ReduceReceiverApp : public Application {
    public:
        EPIC2ReduceReceiverApp() {}
        ~EPIC2ReduceReceiverApp() {}

        void StartApplication() override;
        void StopApplication() override;

        void InitComm(std::vector<Address> peerAddrList, Address rootAddr, uint32_t switchPoolSize);
        void RecvPacket(Ptr<Socket> socket);
        bool AggregatePacket(uint32_t idx, Ptr<Packet> packet);
        void SendAggPacket(uint32_t idx);

        std::vector<AggSlot> m_aggPool;
        uint32_t m_switchPoolSize;

        Ptr<Socket> m_sendSocket;
        Ptr<Socket> m_recvSocket;
        uint16_t m_port;

        std::vector<Address> m_peerAddrList;
        Address m_rootAddr;
};

void EPIC2ReduceReceiverApp::StartApplication() {
    m_recvSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_recvSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024)); // 1GB
    m_recvSocket->Bind(InetSocketAddress(Ipv4Address::GetAny(), m_port));
    m_recvSocket->SetRecvCallback(MakeCallback(&EPIC2ReduceReceiverApp::RecvPacket, this));

    m_sendSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_sendSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_sendSocket->Bind();
}

void EPIC2ReduceReceiverApp::StopApplication() {
    if(m_sendSocket) m_sendSocket->Close();
    if(m_recvSocket) m_recvSocket->Close();
}

void EPIC2ReduceReceiverApp::InitComm(std::vector<Address> peerAddrList, Address rootAddr, uint32_t switchPoolSize) {
    m_peerAddrList = peerAddrList;
    m_rootAddr = rootAddr;
    m_switchPoolSize = switchPoolSize;
    uint32_t rankNum = m_peerAddrList.size();

    for (uint32_t i = 0; i < m_switchPoolSize; i++) {
        AggSlot slot;
        slot.SetRankNum(rankNum);
        m_aggPool.push_back(slot);
    }
}

void EPIC2ReduceReceiverApp::RecvPacket(Ptr<Socket> socket) {
    Ptr<Packet> packet;
    while ((packet = socket->Recv())) {

        EPIC2ReduceTag tag;
        packet->PeekPacketTag(tag);
        uint32_t idx = tag.GetIdx();
        bool isCompleted = AggregatePacket(idx, packet);
        if (isCompleted) {
            SendAggPacket(idx);
        }
    }
}

bool EPIC2ReduceReceiverApp::AggregatePacket(uint32_t idx, Ptr<Packet> packet) {

    bool isCompleted = m_aggPool[idx].AggPkt(packet);

    return isCompleted;
}

void EPIC2ReduceReceiverApp::SendAggPacket(uint32_t idx) {
    Ptr<Packet> packet = m_aggPool[idx].GetAggPkt();

    EPIC2ReduceTag tag;
    packet->RemovePacketTag(tag);
    tag.SetType(AGGREGATED);
    packet->AddPacketTag(tag);

    Ptr<Packet> copy = packet->Copy();

    m_sendSocket->SendTo(copy, 0, m_rootAddr);

    m_aggPool[idx].ReSet();
}

int main(int argc, char *argv[]) {
    LogComponentEnable("EPIC2ReduceTest", LOG_LEVEL_INFO);

    uint32_t numWorkers = 8;
    uint16_t port = 9999;
    uint64_t dataSize = 1024 * 1024;
    uint32_t payloadSize = 1400;
    uint32_t windowSize = 512;
    uint32_t rootRank = 0;

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

    for (uint32_t i = 0; i < numWorkers; i++) {
        NetDeviceContainer devices = p2p.Install(workerNodes.Get(i), switchNode);

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

    std::vector<Address> nonRootWorkerAddrList;
    for (uint32_t i = 0; i < numWorkers; i++) {
        if (i != rootRank) {
            nonRootWorkerAddrList.push_back(InetSocketAddress(workerAddrs[i], port));
        }
    }

    Address rootAddr = InetSocketAddress(workerAddrs[rootRank], port);

    Ptr<EPIC2ReduceReceiverApp> receiverApp = CreateObject<EPIC2ReduceReceiverApp>();
    receiverApp->m_port = port;
    receiverApp->InitComm(nonRootWorkerAddrList, rootAddr, windowSize);
    
    switchNode->AddApplication(receiverApp);
    receiverApp->SetStartTime(Seconds(1.0));
    receiverApp->SetStopTime(Seconds(30.0));
    
    std::map<uint32_t, Address> rankAddrMap;
    for (uint32_t i = 0; i < numWorkers; i++) {
        rankAddrMap[i] = InetSocketAddress(workerAddrs[i], port);
    }
    
    for (uint32_t i = 0; i < numWorkers; i++) {
        Ptr<EPIC2ReduceSenderApp> senderApp = CreateObject<EPIC2ReduceSenderApp>();
        
        senderApp->m_rankId = i;
        senderApp->rootRankId = rootRank;
        senderApp->m_isRoot = (i == rootRank);
        senderApp->m_port = port;
        
        if (i == rootRank) {
            senderApp->m_peerAddr = InetSocketAddress(switchAddr, port);
        } else {
            senderApp->m_peerAddr = InetSocketAddress(switchAddr, port);
        }
        
        senderApp->InitComm(dataSize, payloadSize, windowSize, rankAddrMap);

        workerNodes.Get(i)->AddApplication(senderApp);
        senderApp->SetStartTime(Seconds(1.0));
        senderApp->SetStopTime(Seconds(10.0));
    }

    NS_LOG_INFO("========== EPIC2 Reduce Start ==========");

    Simulator::Run();
    Simulator::Destroy();
    
    NS_LOG_INFO("========== EPIC2 Reduce End ==========");
    return 0;
}

#endif
