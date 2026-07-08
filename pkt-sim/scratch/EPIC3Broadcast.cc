#ifndef EPIC3_BROADCAST_APPLICATION_H
#define EPIC3_BROADCAST_APPLICATION_H

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
#include <set>

using namespace ns3;

NS_LOG_COMPONENT_DEFINE("EPIC3BroadcastTest");

enum PacketType : uint8_t {
    DATA = 0,
    ACK = 1,
    NAK = 2
};

class EPIC3BroadcastTag : public Tag {
public:
    EPIC3BroadcastTag() : m_psn(0), m_idx(0), m_wid(0), m_type(DATA) {}

    static TypeId GetTypeId() {
        static TypeId tid = TypeId("ns3::EPIC3BroadcastTag")
            .SetParent<Tag>()
            .AddConstructor<EPIC3BroadcastTag>();
        return tid;
    }

    TypeId GetInstanceTypeId() const override { 
        return GetTypeId(); 
    }

    void SetType(PacketType type) { m_type = type; }
    PacketType GetType() const { return m_type; }

    void SetPsn(uint32_t psn) { m_psn = psn; }
    uint32_t GetPsn() const { return m_psn; }

    void SetIdx(uint32_t idx) { m_idx = idx; }
    uint32_t GetIdx() const { return m_idx; }

    void SetWid(uint32_t wid) { m_wid = wid; }
    uint32_t GetWid() const { return m_wid; }

    uint32_t GetSerializedSize() const override {
        return 3 * sizeof(uint32_t) + sizeof(uint8_t);
    }

    void Serialize(TagBuffer i) const override {
        i.WriteU32(m_psn);
        i.WriteU32(m_idx);
        i.WriteU32(m_wid);
        i.WriteU8(m_type);
    }

    void Deserialize(TagBuffer i) override {
        m_psn = i.ReadU32();
        m_idx = i.ReadU32();
        m_wid = i.ReadU32();
        m_type = static_cast<PacketType>(i.ReadU8());
    }

    void Print(std::ostream& os) const override {
        os << "EPIC3BroadcastTag: psn=" << m_psn 
           << ", idx=" << m_idx 
           << ", wid=" << m_wid
           << ", type=" << (int)m_type;
    }

private:
    uint32_t m_psn;
    uint32_t m_idx;
    uint32_t m_wid;
    PacketType m_type;
};

class BroadcastSlot {
public:
    void SetChildNum(uint32_t n) { 
        m_childNum = n; 
        m_ackState.resize(n, false);
    }

    bool HasData() const { return m_hasData; }
    
    bool IsCompleted() const { return m_ackCount == m_childNum; }

    void SetData(uint32_t psn, Ptr<Packet> pkt) {
        m_psn = psn;
        m_pkt = pkt;
        m_hasData = true;
        m_ackCount = 0;
        std::fill(m_ackState.begin(), m_ackState.end(), false);
    }

    bool ReceiveAck(uint32_t childIdx) {
        if (m_ackState[childIdx]) {
            return IsCompleted();
        }
        m_ackState[childIdx] = true;
        m_ackCount++;
        return IsCompleted();
    }

    bool HasAcked(uint32_t childIdx) const { return m_ackState[childIdx]; }

    std::vector<uint32_t> GetUnackedChildren() const {
        std::vector<uint32_t> unacked;
        for (uint32_t i = 0; i < m_childNum; i++) {
            if (!m_ackState[i]) {
                unacked.push_back(i);
            }
        }
        return unacked;
    }

    Ptr<Packet> GetPkt() { return m_pkt; }
    uint32_t GetPsn() const { return m_psn; }

    void Reset() {
        m_pkt = nullptr;
        m_hasData = false;
        m_ackCount = 0;
        std::fill(m_ackState.begin(), m_ackState.end(), false);
    }

    uint32_t m_psn = 0;
    uint32_t m_ackCount = 0;
    uint32_t m_childNum = 0;
    bool m_hasData = false;
    Ptr<Packet> m_pkt = nullptr;
    std::vector<bool> m_ackState;
};

class EPIC3BroadcastSenderApp : public Application {
public:
    EPIC3BroadcastSenderApp() : m_rankId(0), m_rootRank(0), m_sentPktNum(0), m_recvPktNum(0), 
                      m_retxTimeout(MicroSeconds(100)) {}
    ~EPIC3BroadcastSenderApp() {}

    void StartApplication() override;
    void StopApplication() override;

    void InitComm(uint64_t dataSize, uint32_t pktPayloadSize, uint32_t windowSize);
    void StartBroadcast();
    void RecvPacket(Ptr<Socket> socket);
    
    bool IsRootRank() const { return m_rankId == m_rootRank; }
    
    void SendDataPacket(uint32_t psn, uint32_t idx);
    void RetransmitData(uint32_t idx);
    void CancelDataTimer(uint32_t idx);
    
    void SendAckPacket(uint32_t psn, uint32_t idx);

    uint32_t m_rankId;
    uint32_t m_rootRank;
    uint64_t m_dataSize;
    uint32_t m_pktPayloadSize;
    uint32_t m_pktNum;
    uint32_t m_sentPktNum;
    uint32_t m_recvPktNum;
    uint32_t m_windowSize;
    uint32_t m_poolSize;

    Ptr<Socket> m_sendSocket;
    Ptr<Socket> m_recvSocket;
    Address m_peerAddr;
    uint16_t m_port;

    Time m_retxTimeout;
    std::map<uint32_t, EventId> m_dataTimers;
    std::map<uint32_t, uint32_t> m_pendingPsn;
    std::set<uint32_t> m_processedPsn;
};

void EPIC3BroadcastSenderApp::StartApplication() {
    m_recvSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_recvSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_recvSocket->Bind(InetSocketAddress(Ipv4Address::GetAny(), m_port));
    m_recvSocket->SetRecvCallback(MakeCallback(&EPIC3BroadcastSenderApp::RecvPacket, this));

    m_sendSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_sendSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_sendSocket->Bind();
    m_sendSocket->Connect(m_peerAddr);

    Simulator::Schedule(MicroSeconds(1), &EPIC3BroadcastSenderApp::StartBroadcast, this);
}

void EPIC3BroadcastSenderApp::StopApplication() {
    for (auto& pair : m_dataTimers) {
        Simulator::Cancel(pair.second);
    }
    m_dataTimers.clear();
    m_pendingPsn.clear();
    m_processedPsn.clear();
    
    if (m_sendSocket) m_sendSocket->Close();
    if (m_recvSocket) m_recvSocket->Close();
}

void EPIC3BroadcastSenderApp::InitComm(uint64_t dataSize, uint32_t pktPayloadSize, uint32_t windowSize) {
    m_dataSize = dataSize;
    m_pktPayloadSize = pktPayloadSize;
    m_pktNum = dataSize / pktPayloadSize;
    m_windowSize = windowSize;
    m_poolSize = windowSize;  // N = windowSize
}

void EPIC3BroadcastSenderApp::StartBroadcast() {
    if (IsRootRank()) {
        NS_LOG_INFO("[" << Simulator::Now().GetSeconds() << "s] Rank " << m_rankId 
                    << " start Broadcast");
        
        for (uint32_t i = 0; i < m_windowSize && i < m_pktNum; i++) {
            SendDataPacket(i, i % m_poolSize);
        }
    } else {
        NS_LOG_INFO("[" << Simulator::Now().GetSeconds() << "s] Rank " << m_rankId 
                    << " waiting for data");
    }
}

void EPIC3BroadcastSenderApp::RecvPacket(Ptr<Socket> socket) {
    Ptr<Packet> pkt;
    while ((pkt = socket->Recv())) {
        EPIC3BroadcastTag tag;
        pkt->PeekPacketTag(tag);
        
        PacketType type = tag.GetType();
        uint32_t psn = tag.GetPsn();
        uint32_t idx = tag.GetIdx();

        if (IsRootRank()) {
            if (type == ACK) {
                if (m_processedPsn.count(psn) > 0) {
                    continue;
                }
                m_processedPsn.insert(psn);
                
                CancelDataTimer(idx);
                
                m_recvPktNum++;
                
                uint32_t nextPsn = psn + m_windowSize;
                if (nextPsn < m_pktNum) {
                    SendDataPacket(nextPsn, nextPsn % m_poolSize);
                }
                
                if (m_recvPktNum == m_pktNum) {
                    NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_rankId 
                                << " Broadcast completed!");
                }
            }
        } else {
            if (type == DATA) {
                SendAckPacket(psn, idx);
                
                if (m_processedPsn.count(psn) > 0) {
                    continue;
                }
                m_processedPsn.insert(psn);
                
                m_recvPktNum++;
                
                if (m_recvPktNum == m_pktNum) {
                    NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_rankId 
                                << " Broadcast completed!");
                }
            }
        }
    }
}

void EPIC3BroadcastSenderApp::SendDataPacket(uint32_t psn, uint32_t idx) {
    Ptr<Packet> packet = Create<Packet>(m_pktPayloadSize);
    
    EPIC3BroadcastTag tag;
    tag.SetPsn(psn);
    tag.SetIdx(idx);
    tag.SetWid(m_rankId);
    tag.SetType(DATA);
    packet->AddPacketTag(tag);

    m_sendSocket->Send(packet);
    m_sentPktNum++;
    
    m_pendingPsn[idx] = psn;
    EventId timerId = Simulator::Schedule(m_retxTimeout, 
                                          &EPIC3BroadcastSenderApp::RetransmitData, 
                                          this, idx);
    m_dataTimers[idx] = timerId;
}

void EPIC3BroadcastSenderApp::SendAckPacket(uint32_t psn, uint32_t idx) {
    Ptr<Packet> packet = Create<Packet>(0);
    
    EPIC3BroadcastTag tag;
    tag.SetPsn(psn);
    tag.SetIdx(idx);
    tag.SetWid(m_rankId);
    tag.SetType(ACK);
    packet->AddPacketTag(tag);

    m_sendSocket->Send(packet);
}

void EPIC3BroadcastSenderApp::RetransmitData(uint32_t idx) {
    if (m_pendingPsn.count(idx) == 0) {
        return;
    }
    
    uint32_t psn = m_pendingPsn[idx];
    
    Ptr<Packet> packet = Create<Packet>(m_pktPayloadSize);
    EPIC3BroadcastTag tag;
    tag.SetPsn(psn);
    tag.SetIdx(idx);
    tag.SetWid(m_rankId);
    tag.SetType(DATA);
    packet->AddPacketTag(tag);
    
    m_sendSocket->Send(packet);
    
    EventId timerId = Simulator::Schedule(m_retxTimeout, 
                                          &EPIC3BroadcastSenderApp::RetransmitData, 
                                          this, idx);
    m_dataTimers[idx] = timerId;
}

void EPIC3BroadcastSenderApp::CancelDataTimer(uint32_t idx) {
    auto it = m_dataTimers.find(idx);
    if (it != m_dataTimers.end()) {
        Simulator::Cancel(it->second);
        m_dataTimers.erase(it);
    }
    m_pendingPsn.erase(idx);
}

class EPIC3BroadcastReceiverApp : public Application {
public:
    EPIC3BroadcastReceiverApp() : m_rankNum(0), m_rootRank(0), m_bcastTimeout(MicroSeconds(100)) {}
    ~EPIC3BroadcastReceiverApp() {}

    void StartApplication() override;
    void StopApplication() override;

    void InitComm(std::vector<Address> peerAddrList, uint32_t poolSize, uint32_t rootRank);
    void RecvPacket(Ptr<Socket> socket);
    
    void HandleDataPacket(uint32_t psn, uint32_t idx, uint32_t wid, Ptr<Packet> pkt);
    void HandleAckPacket(uint32_t psn, uint32_t idx, uint32_t wid);
    void HandleNakPacket(uint32_t psn, uint32_t idx, uint32_t wid);
    
    void SendAckToRoot(uint32_t psn, uint32_t idx);
    void SendNakToRoot(uint32_t psn, uint32_t idx);
    void SendDataToAllChildren(uint32_t idx);
    void SendDataToChild(uint32_t idx, uint32_t childWid);
    
    void OnBroadcastComplete(uint32_t idx);
    
    void RetransmitToChildren(uint32_t idx);
    void CancelBroadcastTimer(uint32_t idx);

    std::vector<BroadcastSlot> m_bcastPool;
    std::map<uint32_t, EventId> m_bcastTimers;
    
    uint32_t m_poolSize;
    uint32_t m_rankNum;
    uint32_t m_rootRank;
    Time m_bcastTimeout;

    Ptr<Socket> m_sendSocket;
    Ptr<Socket> m_recvSocket;
    uint16_t m_port;
    std::vector<Address> m_peerAddrList;
    
    std::vector<uint32_t> m_childWids;
};

void EPIC3BroadcastReceiverApp::StartApplication() {
    m_recvSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_recvSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_recvSocket->Bind(InetSocketAddress(Ipv4Address::GetAny(), m_port));
    m_recvSocket->SetRecvCallback(MakeCallback(&EPIC3BroadcastReceiverApp::RecvPacket, this));

    m_sendSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_sendSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_sendSocket->Bind();
}

void EPIC3BroadcastReceiverApp::StopApplication() {
    for (auto& pair : m_bcastTimers) {
        Simulator::Cancel(pair.second);
    }
    m_bcastTimers.clear();
    
    if (m_sendSocket) m_sendSocket->Close();
    if (m_recvSocket) m_recvSocket->Close();
}

void EPIC3BroadcastReceiverApp::InitComm(std::vector<Address> peerAddrList, uint32_t poolSize, uint32_t rootRank) {
    m_peerAddrList = peerAddrList;
    m_poolSize = poolSize;
    m_rankNum = m_peerAddrList.size();
    m_rootRank = rootRank;

    m_childWids.clear();
    for (uint32_t i = 0; i < m_rankNum; i++) {
        if (i != m_rootRank) {
            m_childWids.push_back(i);
        }
    }

    m_bcastPool.resize(m_poolSize);
    
    for (uint32_t i = 0; i < m_poolSize; i++) {
        m_bcastPool[i].SetChildNum(m_childWids.size());
        m_bcastPool[i].m_psn = i;
    }
}

void EPIC3BroadcastReceiverApp::RecvPacket(Ptr<Socket> socket) {
    Ptr<Packet> pkt;
    while ((pkt = socket->Recv())) {
        EPIC3BroadcastTag tag;
        pkt->PeekPacketTag(tag);
        
        PacketType type = tag.GetType();
        uint32_t psn = tag.GetPsn();
        uint32_t idx = tag.GetIdx();
        uint32_t wid = tag.GetWid();

        if (type == DATA) {
            HandleDataPacket(psn, idx, wid, pkt);
        }
        else if (type == ACK) {
            HandleAckPacket(psn, idx, wid);
        }
        else if (type == NAK) {
            HandleNakPacket(psn, idx, wid);
        }
    }
}

void EPIC3BroadcastReceiverApp::HandleDataPacket(uint32_t psn, uint32_t idx, uint32_t wid, Ptr<Packet> pkt) {
    if (wid != m_rootRank) {
        return;
    }
    
    uint32_t bpsn = m_bcastPool[idx].m_psn;
    
    if (psn < bpsn) {
        SendAckToRoot(psn, idx);
    }
    else if (psn > bpsn) {
        if (!m_bcastPool[idx].HasData()) {
            SendNakToRoot(bpsn, idx);
        } else {
            auto unacked = m_bcastPool[idx].GetUnackedChildren();
            for (uint32_t childIdx : unacked) {
                SendDataToChild(idx, m_childWids[childIdx]);
            }
            CancelBroadcastTimer(idx);
            EventId timerId = Simulator::Schedule(m_bcastTimeout,
                                                  &EPIC3BroadcastReceiverApp::RetransmitToChildren,
                                                  this, idx);
            m_bcastTimers[idx] = timerId;
        }
    }
    else {
        if (m_bcastPool[idx].HasData()) {
            SendAckToRoot(psn, idx);
        } else {
            m_bcastPool[idx].SetData(psn, pkt);
            
            SendAckToRoot(psn, idx);
            
            SendDataToAllChildren(idx);
            
            EventId timerId = Simulator::Schedule(m_bcastTimeout,
                                                  &EPIC3BroadcastReceiverApp::RetransmitToChildren,
                                                  this, idx);
            m_bcastTimers[idx] = timerId;
        }
    }
}

void EPIC3BroadcastReceiverApp::HandleAckPacket(uint32_t psn, uint32_t idx, uint32_t wid) {
    if (wid == m_rootRank) {
        return;
    }
    
    uint32_t bpsn = m_bcastPool[idx].m_psn;
    
    if (psn != bpsn) {
        return;
    }
    
    uint32_t childIdx = UINT32_MAX;
    for (uint32_t i = 0; i < m_childWids.size(); i++) {
        if (m_childWids[i] == wid) {
            childIdx = i;
            break;
        }
    }
    
    if (childIdx == UINT32_MAX) {
        return;
    }
    
    if (m_bcastPool[idx].HasAcked(childIdx)) {
        return;
    }
    
    bool allAcked = m_bcastPool[idx].ReceiveAck(childIdx);
    
    if (allAcked) {
        OnBroadcastComplete(idx);
    }
}

void EPIC3BroadcastReceiverApp::HandleNakPacket(uint32_t psn, uint32_t idx, uint32_t wid) {
}

void EPIC3BroadcastReceiverApp::SendAckToRoot(uint32_t psn, uint32_t idx) {
    Ptr<Packet> packet = Create<Packet>(0);
    
    EPIC3BroadcastTag tag;
    tag.SetPsn(psn);
    tag.SetIdx(idx);
    tag.SetWid(m_rootRank);
    tag.SetType(ACK);
    packet->AddPacketTag(tag);

    m_sendSocket->SendTo(packet, 0, m_peerAddrList[m_rootRank]);
}

void EPIC3BroadcastReceiverApp::SendNakToRoot(uint32_t psn, uint32_t idx) {
    Ptr<Packet> packet = Create<Packet>(0);
    
    EPIC3BroadcastTag tag;
    tag.SetPsn(psn);
    tag.SetIdx(idx);
    tag.SetWid(m_rootRank);
    tag.SetType(NAK);
    packet->AddPacketTag(tag);

    m_sendSocket->SendTo(packet, 0, m_peerAddrList[m_rootRank]);
}

void EPIC3BroadcastReceiverApp::SendDataToAllChildren(uint32_t idx) {
    Ptr<Packet> pkt = m_bcastPool[idx].GetPkt();
    uint32_t psn = m_bcastPool[idx].GetPsn();
    
    for (uint32_t childWid : m_childWids) {
        Ptr<Packet> copy = pkt->Copy();
        
        EPIC3BroadcastTag tag;
        copy->RemovePacketTag(tag);
        tag.SetPsn(psn);
        tag.SetIdx(idx);
        tag.SetWid(childWid);
        tag.SetType(DATA);
        copy->AddPacketTag(tag);

        m_sendSocket->SendTo(copy, 0, m_peerAddrList[childWid]);
    }
}

void EPIC3BroadcastReceiverApp::SendDataToChild(uint32_t idx, uint32_t childWid) {
    Ptr<Packet> pkt = m_bcastPool[idx].GetPkt();
    uint32_t psn = m_bcastPool[idx].GetPsn();
    
    Ptr<Packet> copy = pkt->Copy();
    
    EPIC3BroadcastTag tag;
    copy->RemovePacketTag(tag);
    tag.SetPsn(psn);
    tag.SetIdx(idx);
    tag.SetWid(childWid);
    tag.SetType(DATA);
    copy->AddPacketTag(tag);

    m_sendSocket->SendTo(copy, 0, m_peerAddrList[childWid]);
}

void EPIC3BroadcastReceiverApp::OnBroadcastComplete(uint32_t idx) {
    CancelBroadcastTimer(idx);
    
    m_bcastPool[idx].m_psn += m_poolSize;
    
    m_bcastPool[idx].Reset();
}

void EPIC3BroadcastReceiverApp::RetransmitToChildren(uint32_t idx) {
    if (!m_bcastPool[idx].HasData()) {
        return;
    }
    
    auto unacked = m_bcastPool[idx].GetUnackedChildren();
    
    for (uint32_t childIdx : unacked) {
        SendDataToChild(idx, m_childWids[childIdx]);
    }
    
    EventId timerId = Simulator::Schedule(m_bcastTimeout,
                                          &EPIC3BroadcastReceiverApp::RetransmitToChildren,
                                          this, idx);
    m_bcastTimers[idx] = timerId;
}

void EPIC3BroadcastReceiverApp::CancelBroadcastTimer(uint32_t idx) {
    auto it = m_bcastTimers.find(idx);
    if (it != m_bcastTimers.end()) {
        Simulator::Cancel(it->second);
        m_bcastTimers.erase(it);
    }
}

int main(int argc, char *argv[]) {
    LogComponentEnable("EPIC3BroadcastTest", LOG_LEVEL_INFO);

    uint32_t numWorkers = 8;
    uint16_t port = 9999;
    uint64_t dataSize = 1 * 1024 * 1024;  
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

    Ptr<EPIC3BroadcastReceiverApp> receiverApp = CreateObject<EPIC3BroadcastReceiverApp>();
    receiverApp->m_port = port;
    receiverApp->InitComm(peerAddrList, windowSize, rootRank);

    switchNode->AddApplication(receiverApp);
    receiverApp->SetStartTime(Seconds(1.0));
    receiverApp->SetStopTime(Seconds(10.0));

    for (uint32_t i = 0; i < numWorkers; i++) {
        Ptr<EPIC3BroadcastSenderApp> senderApp = CreateObject<EPIC3BroadcastSenderApp>();

        senderApp->m_rankId = i;
        senderApp->m_rootRank = rootRank;
        senderApp->m_port = port;
        senderApp->m_peerAddr = InetSocketAddress(switchAddr, port);
        senderApp->InitComm(dataSize, payloadSize, windowSize);

        workerNodes.Get(i)->AddApplication(senderApp);
        senderApp->SetStartTime(Seconds(1.0));
        senderApp->SetStopTime(Seconds(30.0));
    }

    NS_LOG_INFO("========== EPIC3 Broadcast Start ==========");

    Simulator::Run();

    NS_LOG_INFO("========== EPIC3 Broadcast End ==========");

    Simulator::Destroy();

    return 0;
}

#endif