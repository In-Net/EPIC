#ifndef EPIC2_BROADCAST_H
#define EPIC2_BROADCAST_H

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

NS_LOG_COMPONENT_DEFINE("EPIC2BroadcastTest");

class EPIC2BroadcastTag : public Tag {
public:
    EPIC2BroadcastTag() : m_seq(0) {}

    static TypeId GetTypeId() {
        static TypeId tid = TypeId("ns3::EPIC2BroadcastTag")
            .SetParent<Tag>()
            .AddConstructor<EPIC2BroadcastTag>();
        return tid;
    }

    TypeId GetInstanceTypeId() const override { 
        return GetTypeId(); 
    }

    void SetSeq(uint32_t seq) { m_seq = seq; }
    uint32_t GetSeq() const { return m_seq; }
    
    uint32_t GetSerializedSize() const override {
        return sizeof(uint32_t);
    }

    void Serialize(TagBuffer i) const override {
        i.WriteU32(m_seq);
    }

    void Deserialize(TagBuffer i) override {
        m_seq = i.ReadU32();
    }

    void Print(std::ostream& os) const override {
        os << "EPIC2BroadcastTag: seq=" << m_seq;
    }

private:
    uint32_t m_seq;
};

class EPIC2BroadcastSenderApp : public Application {
public:
    EPIC2BroadcastSenderApp() : m_rankId(0), m_isRoot(false), m_sentPktNum(0), m_recvPktNum(0) {}
    ~EPIC2BroadcastSenderApp() {}

    void StartApplication() override;
    void StopApplication() override;

    void InitComm(uint64_t dataSize, uint32_t pktPayloadSize);
    void StartBroadcast();
    
    void RecvDataFromRoot(Ptr<Socket> socket);

    uint32_t m_rankId;
    uint32_t m_rootRankId;
    bool m_isRoot;
    uint64_t m_dataSize;
    uint32_t m_pktPayloadSize;
    uint32_t m_pktNum;
    uint32_t m_sentPktNum;
    uint32_t m_recvPktNum;

    Ptr<Socket> m_sendSocket;
    Ptr<Socket> m_recvSocket;
    Address m_peerAddr;
    uint16_t m_port;
};

void EPIC2BroadcastSenderApp::StartApplication() {
    if (!m_isRoot) {
        m_recvSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
        m_recvSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
        m_recvSocket->Bind(InetSocketAddress(Ipv4Address::GetAny(), m_port));
        m_recvSocket->SetRecvCallback(MakeCallback(&EPIC2BroadcastSenderApp::RecvDataFromRoot, this));
    }

    if (m_isRoot) {
        m_sendSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
        m_sendSocket->Bind();
        m_sendSocket->Connect(m_peerAddr);
    }

    Simulator::Schedule(MicroSeconds(1), &EPIC2BroadcastSenderApp::StartBroadcast, this);
}

void EPIC2BroadcastSenderApp::StopApplication() {
    if (m_sendSocket) m_sendSocket->Close();
    if (m_recvSocket) m_recvSocket->Close();
}

void EPIC2BroadcastSenderApp::InitComm(uint64_t dataSize, uint32_t pktPayloadSize) {
    m_dataSize = dataSize;
    m_pktPayloadSize = pktPayloadSize;
    m_pktNum = dataSize / pktPayloadSize;
}

void EPIC2BroadcastSenderApp::StartBroadcast() {
    if (m_isRoot) {
        NS_LOG_INFO("[" << Simulator::Now().GetSeconds() << "s] Rank " << m_rankId 
                    << " (Root) Start Broadcast");
        
        for (uint32_t i = 0; i < m_pktNum; i++) {
            Ptr<Packet> packet = Create<Packet>(m_pktPayloadSize);
            EPIC2BroadcastTag tag;
            tag.SetSeq(i);
            packet->AddPacketTag(tag);
            m_sendSocket->Send(packet);
            m_sentPktNum++;
        }
    } else {
        NS_LOG_INFO("[" << Simulator::Now().GetSeconds() << "s] Rank " << m_rankId 
                    << " Waiting for Broadcast data");
    }
}

void EPIC2BroadcastSenderApp::RecvDataFromRoot(Ptr<Socket> socket) {
    Ptr<Packet> pkt;
    while ((pkt = socket->Recv())) {
        m_recvPktNum++;
        
        if (m_recvPktNum == m_pktNum) {
            NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_rankId 
                        << " Broadcast completed!");
        }
    }
}

class EPIC2BroadcastReceiverApp : public Application {
public:
    EPIC2BroadcastReceiverApp() {}
    ~EPIC2BroadcastReceiverApp() {}

    void StartApplication() override;
    void StopApplication() override;

    void InitComm(std::vector<Address> nonRootWorkerAddrList);
    void RecvPacket(Ptr<Socket> socket);
    void BroadcastToWorkers(Ptr<Packet> packet);

    Ptr<Socket> m_sendSocket;
    Ptr<Socket> m_recvSocket;
    uint16_t m_port;

    std::vector<Address> m_nonRootWorkerAddrList;
};

void EPIC2BroadcastReceiverApp::StartApplication() {
    m_recvSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_recvSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_recvSocket->Bind(InetSocketAddress(Ipv4Address::GetAny(), m_port));
    m_recvSocket->SetRecvCallback(MakeCallback(&EPIC2BroadcastReceiverApp::RecvPacket, this));

    m_sendSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_sendSocket->Bind();
}

void EPIC2BroadcastReceiverApp::StopApplication() {
    if (m_sendSocket) m_sendSocket->Close();
    if (m_recvSocket) m_recvSocket->Close();
}

void EPIC2BroadcastReceiverApp::InitComm(std::vector<Address> nonRootWorkerAddrList) {
    m_nonRootWorkerAddrList = nonRootWorkerAddrList;
}

void EPIC2BroadcastReceiverApp::RecvPacket(Ptr<Socket> socket) {
    Ptr<Packet> packet;
    while ((packet = socket->Recv())) {
        BroadcastToWorkers(packet);
    }
}

void EPIC2BroadcastReceiverApp::BroadcastToWorkers(Ptr<Packet> packet) {
    for (const auto& addr : m_nonRootWorkerAddrList) {
        Ptr<Packet> copy = packet->Copy();
        m_sendSocket->SendTo(copy, 0, addr);
    }
}

int main(int argc, char *argv[]) {
    LogComponentEnable("EPIC2BroadcastTest", LOG_LEVEL_INFO);

    uint32_t numWorkers = 8;
    uint16_t port = 9999;
    uint64_t dataSize = 1024 * 1024;
    uint32_t payloadSize = 1400;
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

    std::vector<Address> nonRootWorkerAddrList;
    for (uint32_t i = 0; i < numWorkers; i++) {
        if (i != rootRank) {
            nonRootWorkerAddrList.push_back(InetSocketAddress(workerAddrs[i], port));
        }
    }

    Ptr<EPIC2BroadcastReceiverApp> receiverApp = CreateObject<EPIC2BroadcastReceiverApp>();
    receiverApp->m_port = port;
    receiverApp->InitComm(nonRootWorkerAddrList);
    
    switchNode->AddApplication(receiverApp);
    receiverApp->SetStartTime(Seconds(1.0));
    receiverApp->SetStopTime(Seconds(30.0));
    
    for (uint32_t i = 0; i < numWorkers; i++) {
        Ptr<EPIC2BroadcastSenderApp> senderApp = CreateObject<EPIC2BroadcastSenderApp>();
        
        senderApp->m_rankId = i;
        senderApp->m_rootRankId = rootRank;
        senderApp->m_isRoot = (i == rootRank);
        senderApp->m_port = port;
        senderApp->m_peerAddr = InetSocketAddress(switchAddr, port);
        
        senderApp->InitComm(dataSize, payloadSize);

        workerNodes.Get(i)->AddApplication(senderApp);
        senderApp->SetStartTime(Seconds(1.0));
        senderApp->SetStopTime(Seconds(10.0));
    }

    NS_LOG_INFO("========== EPIC2 Broadcast Start ==========");

    Simulator::Run();
    Simulator::Destroy();
    
    NS_LOG_INFO("========== EPIC2 Broadcast End ==========");

    return 0;
}

#endif