#ifndef EPIC2_ALLGATHER_H
#define EPIC2_ALLGATHER_H

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
#include <set>

using namespace ns3;

NS_LOG_COMPONENT_DEFINE("EPIC2AllGatherTest");

enum PacketType : uint8_t {
    DATA = 0,
    ACK = 1,
    START = 2
};

class EPIC2AllGatherTag : public Tag {
public:
    EPIC2AllGatherTag() : m_seq(0), m_srcRank(0), m_senderRank(0), m_type(DATA) {}

    static TypeId GetTypeId() {
        static TypeId tid = TypeId("ns3::EPIC2AllGatherTag")
            .SetParent<Tag>()
            .AddConstructor<EPIC2AllGatherTag>();
        return tid;
    }

    TypeId GetInstanceTypeId() const override { 
        return GetTypeId(); 
    }

    void SetSeq(uint32_t seq) { m_seq = seq; }
    uint32_t GetSeq() const { return m_seq; }
    
    void SetSrcRank(uint32_t srcRank) { m_srcRank = srcRank; }
    uint32_t GetSrcRank() const { return m_srcRank; }
    
    void SetSenderRank(uint32_t senderRank) { m_senderRank = senderRank; }
    uint32_t GetSenderRank() const { return m_senderRank; }
    
    void SetType(PacketType type) { m_type = type; }
    PacketType GetType() const { return m_type; }
    
    uint32_t GetSerializedSize() const override {
        return 3 * sizeof(uint32_t) + sizeof(uint8_t);
    }

    void Serialize(TagBuffer i) const override {
        i.WriteU32(m_seq);
        i.WriteU32(m_srcRank);
        i.WriteU32(m_senderRank);
        i.WriteU8(m_type);
    }

    void Deserialize(TagBuffer i) override {
        m_seq = i.ReadU32();
        m_srcRank = i.ReadU32();
        m_senderRank = i.ReadU32();
        m_type = static_cast<PacketType>(i.ReadU8());
    }

    void Print(std::ostream& os) const override {
        os << "EPIC2AllGatherTag: seq=" << m_seq << ", srcRank=" << m_srcRank 
           << ", senderRank=" << m_senderRank;
    }

private:
    uint32_t m_seq;
    uint32_t m_srcRank;
    uint32_t m_senderRank;
    PacketType m_type;
};

class EPIC2AllGatherSenderApp : public Application {
public:
    EPIC2AllGatherSenderApp() : m_rankId(0), m_rankNum(0), m_recvPktNum(0), 
                                m_windowSize(0), m_recvAckNum(0), 
                                m_currentRoot(0), m_myBroadcastDone(false)
                                {}
    ~EPIC2AllGatherSenderApp() {}

    void StartApplication() override;
    void StopApplication() override;

    void InitComm(uint64_t dataSize, uint32_t pktPayloadSize, uint32_t rankNum, uint32_t windowSize);
    void StartAllGather();
    void StartBroadcastAsRoot();
    void SendDataPacket(uint32_t seq);
    void SendAckPacket(uint32_t seq, uint32_t srcRank);
    void RecvPacket(Ptr<Socket> socket);

    uint32_t m_rankId;
    uint32_t m_rankNum;
    uint64_t m_dataSize;
    uint32_t m_pktPayloadSize;
    uint32_t m_pktNum;
    uint32_t m_recvPktNum;

    uint32_t m_windowSize;
    uint32_t m_recvAckNum;
    std::set<uint32_t> m_processedAckSeq;
    
    uint32_t m_currentRoot;
    bool m_myBroadcastDone;

    Ptr<Socket> m_sendSocket;
    Ptr<Socket> m_recvSocket;
    Address m_peerAddr;
    uint16_t m_port;
};

void EPIC2AllGatherSenderApp::StartApplication() {
    m_recvSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_recvSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_recvSocket->Bind(InetSocketAddress(Ipv4Address::GetAny(), m_port));
    m_recvSocket->SetRecvCallback(MakeCallback(&EPIC2AllGatherSenderApp::RecvPacket, this));

    m_sendSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_sendSocket->Bind();
    m_sendSocket->Connect(m_peerAddr);

    Simulator::Schedule(MicroSeconds(1), &EPIC2AllGatherSenderApp::StartAllGather, this);
}

void EPIC2AllGatherSenderApp::StopApplication() {
    if (m_sendSocket) m_sendSocket->Close();
    if (m_recvSocket) m_recvSocket->Close();
}

void EPIC2AllGatherSenderApp::InitComm(uint64_t dataSize, uint32_t pktPayloadSize, 
                                      uint32_t rankNum, uint32_t windowSize) {
    m_dataSize = dataSize;
    m_pktPayloadSize = pktPayloadSize;
    m_pktNum = dataSize / pktPayloadSize;
    m_rankNum = rankNum;
    m_windowSize = windowSize;
}

void EPIC2AllGatherSenderApp::StartAllGather() {
    NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_rankId 
                << " Start AllGather");
    
    if (m_rankId == 0) {
        StartBroadcastAsRoot();
    }
}

void EPIC2AllGatherSenderApp::StartBroadcastAsRoot() {
    
    m_recvAckNum = 0;
    m_processedAckSeq.clear();
    
    for (uint32_t i = 0; i < m_windowSize && i < m_pktNum; i++) {
        SendDataPacket(i);
    }
}

void EPIC2AllGatherSenderApp::SendDataPacket(uint32_t seq) {
    Ptr<Packet> packet = Create<Packet>(m_pktPayloadSize);
    EPIC2AllGatherTag tag;
    tag.SetSeq(seq);
    tag.SetSrcRank(m_rankId);
    tag.SetType(DATA);
    packet->AddPacketTag(tag);
    m_sendSocket->Send(packet);
}

void EPIC2AllGatherSenderApp::SendAckPacket(uint32_t seq, uint32_t srcRank) {
    Ptr<Packet> packet = Create<Packet>(0);
    EPIC2AllGatherTag tag;
    tag.SetSeq(seq);
    tag.SetSrcRank(srcRank);
    tag.SetSenderRank(m_rankId);
    tag.SetType(ACK);
    packet->AddPacketTag(tag);
    m_sendSocket->Send(packet);
}

void EPIC2AllGatherSenderApp::RecvPacket(Ptr<Socket> socket) {
    Ptr<Packet> pkt;
    while ((pkt = socket->Recv())) {
        EPIC2AllGatherTag tag;
        pkt->PeekPacketTag(tag);
        
        PacketType type = tag.GetType();
        uint32_t seq = tag.GetSeq();
        uint32_t srcRank = tag.GetSrcRank();
        
        if (type == START) {
            m_currentRoot = srcRank;
            if (m_rankId == m_currentRoot) {
                StartBroadcastAsRoot();
            }
        }
        else if (type == DATA && srcRank != m_rankId) {
            m_recvPktNum++;
            
            SendAckPacket(seq, srcRank);
            
            uint32_t totalExpected = m_pktNum * (m_rankNum - 1);
            if (m_recvPktNum == totalExpected && m_myBroadcastDone) {
                NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_rankId 
                            << " AllGather completed!");
            }
        }
        else if (type == ACK && srcRank == m_rankId) {
            if (m_processedAckSeq.count(seq) > 0) {
                continue;
            }
            m_processedAckSeq.insert(seq);
            m_recvAckNum++;
            
            uint32_t nextSeq = seq + m_windowSize;
            if (nextSeq < m_pktNum) {
                SendDataPacket(nextSeq);
            }
            
            if (m_recvAckNum == m_pktNum && !m_myBroadcastDone) {
                m_myBroadcastDone = true;
                /*NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_rankId 
                            << " Broadcast " << m_currentRoot << " completed!");*/
                
                uint32_t totalExpected = m_pktNum * (m_rankNum - 1);
                if (m_recvPktNum == totalExpected) {
                    /*NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_rankId 
                                << " AllGather completed!");*/
                }
            }
        }
    }
}

class EPIC2AllGatherReceiverApp : public Application {
public:
    EPIC2AllGatherReceiverApp() : m_rankNum(0), m_currentRoot(0), m_pktNum(0) {}
    ~EPIC2AllGatherReceiverApp() {}

    void StartApplication() override;
    void StopApplication() override;

    void InitComm(std::vector<Address> workerAddrList, uint32_t pktNum);
    void RecvPacket(Ptr<Socket> socket);
    void BroadcastToOthers(Ptr<Packet> packet, uint32_t srcRank);
    void SendAckToRoot(uint32_t seq);
    void OnRoundComplete();
    void SendStartToNextRoot();

    Ptr<Socket> m_sendSocket;
    Ptr<Socket> m_recvSocket;
    uint16_t m_port;

    uint32_t m_rankNum;
    uint32_t m_currentRoot;
    uint32_t m_pktNum;
    uint32_t m_completedPkts;
    std::vector<Address> m_workerAddrList;
    
    std::map<uint32_t, std::set<uint32_t>> m_ackCollector;
};

void EPIC2AllGatherReceiverApp::StartApplication() {
    m_recvSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_recvSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_recvSocket->Bind(InetSocketAddress(Ipv4Address::GetAny(), m_port));
    m_recvSocket->SetRecvCallback(MakeCallback(&EPIC2AllGatherReceiverApp::RecvPacket, this));

    m_sendSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_sendSocket->Bind();
}

void EPIC2AllGatherReceiverApp::StopApplication() {
    if (m_sendSocket) m_sendSocket->Close();
    if (m_recvSocket) m_recvSocket->Close();
}

void EPIC2AllGatherReceiverApp::InitComm(std::vector<Address> workerAddrList, uint32_t pktNum) {
    m_workerAddrList = workerAddrList;
    m_rankNum = workerAddrList.size();
    m_pktNum = pktNum;
    m_currentRoot = 0;
    m_completedPkts = 0;
}

void EPIC2AllGatherReceiverApp::RecvPacket(Ptr<Socket> socket) {
    Ptr<Packet> packet;
    while ((packet = socket->Recv())) {
        EPIC2AllGatherTag tag;
        packet->PeekPacketTag(tag);
        
        PacketType type = tag.GetType();
        uint32_t seq = tag.GetSeq();
        uint32_t srcRank = tag.GetSrcRank();
        
        if (type == DATA && srcRank == m_currentRoot) {
            BroadcastToOthers(packet, srcRank);
        }
        else if (type == ACK && srcRank == m_currentRoot) {
            uint32_t senderRank = tag.GetSenderRank();
            
            m_ackCollector[seq].insert(senderRank);
            
            if (m_ackCollector[seq].size() == m_rankNum - 1) {
                SendAckToRoot(seq);
                m_ackCollector.erase(seq);
                
                m_completedPkts++;
                
                if (m_completedPkts == m_pktNum) {
                    OnRoundComplete();
                }
            }
        }
    }
}

void EPIC2AllGatherReceiverApp::BroadcastToOthers(Ptr<Packet> packet, uint32_t srcRank) {
    for (uint32_t i = 0; i < m_rankNum; i++) {
        if (i != srcRank) {
            Ptr<Packet> copy = packet->Copy();
            m_sendSocket->SendTo(copy, 0, m_workerAddrList[i]);
        }
    }
}

void EPIC2AllGatherReceiverApp::SendAckToRoot(uint32_t seq) {
    Ptr<Packet> packet = Create<Packet>(0);
    
    EPIC2AllGatherTag tag;
    tag.SetSeq(seq);
    tag.SetSrcRank(m_currentRoot);
    tag.SetType(ACK);
    packet->AddPacketTag(tag);

    m_sendSocket->SendTo(packet, 0, m_workerAddrList[m_currentRoot]);
}

void EPIC2AllGatherReceiverApp::OnRoundComplete() {
    /*NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Switch: 第 " 
                << m_currentRoot << " 轮广播完成");*/
    
    m_completedPkts = 0;
    m_ackCollector.clear();
    m_currentRoot++;
    
    if (m_currentRoot < m_rankNum) {
        /*NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Switch: 通知 Rank " 
                    << m_currentRoot << " 开始广播");*/
        SendStartToNextRoot();
    } else {
        /*NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Switch: AllGather 全部完成!");*/
    }
}

void EPIC2AllGatherReceiverApp::SendStartToNextRoot() {
    Ptr<Packet> packet = Create<Packet>(0);
    
    EPIC2AllGatherTag tag;
    tag.SetSeq(0);
    tag.SetSrcRank(m_currentRoot);
    tag.SetType(START);
    packet->AddPacketTag(tag);
    
    for (uint32_t i = 0; i < m_rankNum; i++) {
        Ptr<Packet> copy = packet->Copy();
        m_sendSocket->SendTo(copy, 0, m_workerAddrList[i]);
    }
}

int main(int argc, char *argv[]) {
    LogComponentEnable("EPIC2AllGatherTest", LOG_LEVEL_INFO);

    uint32_t numWorkers = 8;
    uint16_t port = 9999;
    uint64_t dataSize = 1024 * 1024;
    uint32_t payloadSize = 1400;
    uint32_t windowSize = 512;

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
    p2p.SetQueue("ns3::DropTailQueue<Packet>", "MaxSize", StringValue("10000000p"));

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

    std::vector<Address> workerAddrList;
    for (uint32_t i = 0; i < numWorkers; i++) {
        workerAddrList.push_back(InetSocketAddress(workerAddrs[i], port));
    }

    uint32_t pktNum = dataSize / payloadSize;

    Ptr<EPIC2AllGatherReceiverApp> receiverApp = CreateObject<EPIC2AllGatherReceiverApp>();
    receiverApp->m_port = port;
    receiverApp->InitComm(workerAddrList, pktNum);
    
    switchNode->AddApplication(receiverApp);
    receiverApp->SetStartTime(Seconds(1.0));
    receiverApp->SetStopTime(Seconds(30.0));
    
    for (uint32_t i = 0; i < numWorkers; i++) {
        Ptr<EPIC2AllGatherSenderApp> senderApp = CreateObject<EPIC2AllGatherSenderApp>();
        
        senderApp->m_rankId = i;
        senderApp->m_port = port;
        senderApp->m_peerAddr = InetSocketAddress(switchAddr, port);
        senderApp->InitComm(dataSize, payloadSize, numWorkers, windowSize);

        workerNodes.Get(i)->AddApplication(senderApp);
        senderApp->SetStartTime(Seconds(1.0));
        senderApp->SetStopTime(Seconds(30.0));
    }

    NS_LOG_INFO("========== EPIC2 AllGather Start ==========");

    Simulator::Run();
    
    NS_LOG_INFO("========== EPIC2 AllGather End ==========");
    
    Simulator::Destroy();

    return 0;
}

#endif
