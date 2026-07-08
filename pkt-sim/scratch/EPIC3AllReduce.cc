#ifndef EPIC3_ALLREDUCE_H
#define EPIC3_ALLREDUCE_H

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
#include <fstream>
#include <sstream>

using namespace ns3;

NS_LOG_COMPONENT_DEFINE("EPIC3AllReduceTest");

enum PacketType : uint8_t {
    DATA = 0,
    AGGREGATED = 1,
    ACK = 2,
    NAK = 3
};

class EPIC3AllReduceTag : public Tag {
public:
    EPIC3AllReduceTag() : m_psn(0), m_idx(0), m_wid(0), m_type(DATA), m_isRetransmit(0) {}

    static TypeId GetTypeId() {
        static TypeId tid = TypeId("ns3::EPIC3AllReduceTag")
            .SetParent<Tag>()
            .AddConstructor<EPIC3AllReduceTag>();
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

    void SetIsRetransmit(uint8_t isRetransmit) { m_isRetransmit = isRetransmit; }
    uint8_t GetIsRetransmit() const { return m_isRetransmit; }

    uint32_t GetSerializedSize() const override {
        return 3 * sizeof(uint32_t) + 2 * sizeof(uint8_t);
    }

    void Serialize(TagBuffer i) const override {
        i.WriteU32(m_psn);
        i.WriteU32(m_idx);
        i.WriteU32(m_wid);
        i.WriteU8(m_type);
        i.WriteU8(m_isRetransmit);
    }

    void Deserialize(TagBuffer i) override {
        m_psn = i.ReadU32();
        m_idx = i.ReadU32();
        m_wid = i.ReadU32();
        m_type = static_cast<PacketType>(i.ReadU8());
        m_isRetransmit = i.ReadU8();
    }

    void Print(std::ostream& os) const override {
        os << "EPIC3AllReduceTag: psn=" << m_psn 
           << ", idx=" << m_idx 
           << ", wid=" << m_wid
           << ", type=" << (int)m_type
           << ", isRetransmit=" << (int)m_isRetransmit;
    }

private:
    uint32_t m_psn;
    uint32_t m_idx;
    uint32_t m_wid;
    PacketType m_type;
    uint8_t m_isRetransmit;
};

class AggSlot {
public:
    void SetRankNum(uint32_t n) { 
        m_num = n; 
        m_arrState.resize(n, false);
    }

    bool AggPkt(uint32_t wid, Ptr<Packet> pkt) {
        if (m_arrState[wid]) {
            return IsCompleted();
        }
        
        m_arrState[wid] = true;
        m_count++;
        m_pkt = pkt;
        
        return IsCompleted();
    }

    bool IsCompleted() const { return m_count == m_num; }
    
    bool HasReceived(uint32_t wid) const { return m_arrState[wid]; }

    Ptr<Packet> GetAggPkt() { return m_pkt; }

    std::vector<uint32_t> GetMissingWorkers() const {
        std::vector<uint32_t> missing;
        for (uint32_t i = 0; i < m_num; i++) {
            if (!m_arrState[i]) {
                missing.push_back(i);
            }
        }
        return missing;
    }

    void Reset() {
        m_count = 0;
        m_pkt = nullptr;
        std::fill(m_arrState.begin(), m_arrState.end(), false);
    }

    uint32_t m_psn = 0;
    uint32_t m_count = 0;
    uint32_t m_num = 0;
    Ptr<Packet> m_pkt = nullptr;
    std::vector<bool> m_arrState;
};

class BroadcastSlot {
public:
    void SetRankNum(uint32_t n) { 
        m_num = n; 
        m_ackState.resize(n, false);
    }

    bool IsEmpty() const { return !m_hasResult; }
    
    bool IsCompleted() const { return m_ackCount == m_num; }

    void SetResult(uint32_t psn, Ptr<Packet> pkt) {
        m_psn = psn;
        m_pkt = pkt;
        m_hasResult = true;
        m_ackCount = 0;
        std::fill(m_ackState.begin(), m_ackState.end(), false);
    }

    bool ReceiveAck(uint32_t wid) {
        if (m_ackState[wid]) {
            return IsCompleted();
        }
        m_ackState[wid] = true;
        m_ackCount++;
        return IsCompleted();
    }

    bool HasAcked(uint32_t wid) const { return m_ackState[wid]; }

    std::vector<uint32_t> GetUnackedWorkers() const {
        std::vector<uint32_t> unacked;
        for (uint32_t i = 0; i < m_num; i++) {
            if (!m_ackState[i]) {
                unacked.push_back(i);
            }
        }
        return unacked;
    }

    Ptr<Packet> GetPkt() { return m_pkt; }
    uint32_t GetPsn() const { return m_psn; }

    void Reset() {
        m_psn = 0;
        m_pkt = nullptr;
        m_hasResult = false;
        m_ackCount = 0;
        std::fill(m_ackState.begin(), m_ackState.end(), false);
    }

    uint32_t m_psn = 0;
    uint32_t m_ackCount = 0;
    uint32_t m_num = 0;
    bool m_hasResult = false;
    Ptr<Packet> m_pkt = nullptr;
    std::vector<bool> m_ackState;
};

class EPIC3AllReduceSenderApp : public Application {
public:
    EPIC3AllReduceSenderApp() : m_rankId(0), m_sentPktNum(0), m_recvPktNum(0), 
                      m_retxTimeout(MicroSeconds(100)) {}
    ~EPIC3AllReduceSenderApp() {}

    void StartApplication() override;
    void StopApplication() override;

    void InitComm(uint64_t dataSize, uint32_t pktPayloadSize, uint32_t windowSize, uint32_t retxTimeout);
    void StartAllReduce();
    void RecvPacket(Ptr<Socket> socket);
    
    void SendDataPacket(uint32_t psn, uint32_t idx);
    void SendAckPacket(uint32_t psn, uint32_t idx);
    
    void RetransmitData(uint32_t idx);
    void CancelDataTimer(uint32_t idx);

    uint32_t m_rankId;
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

    Time m_retxTimeout;
    std::map<uint32_t, EventId> m_dataTimers;
    std::map<uint32_t, uint32_t> m_pendingPsn;
    std::set<uint32_t> m_processedAggPsn;

    uint32_t m_retxPktNum = 0;
};

void EPIC3AllReduceSenderApp::StartApplication() {
    m_recvSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_recvSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_recvSocket->Bind(InetSocketAddress(Ipv4Address::GetAny(), m_port));
    m_recvSocket->SetRecvCallback(MakeCallback(&EPIC3AllReduceSenderApp::RecvPacket, this));

    m_sendSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_sendSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_sendSocket->Bind();
    m_sendSocket->Connect(m_peerAddr);

    Simulator::Schedule(MicroSeconds(1), &EPIC3AllReduceSenderApp::StartAllReduce, this);
}

void EPIC3AllReduceSenderApp::StopApplication() {    
    for (auto& pair : m_dataTimers) {
        Simulator::Cancel(pair.second);
    }
    m_dataTimers.clear();
    m_pendingPsn.clear();
    m_processedAggPsn.clear();
    
    if (m_sendSocket) m_sendSocket->Close();
    if (m_recvSocket) m_recvSocket->Close();
}

void EPIC3AllReduceSenderApp::InitComm(uint64_t dataSize, uint32_t pktPayloadSize, uint32_t windowSize, uint32_t retxTimeout) {
    m_dataSize = dataSize;
    m_pktPayloadSize = pktPayloadSize;
    m_pktNum = dataSize / pktPayloadSize;
    m_windowSize = windowSize;
    m_retxTimeout = MicroSeconds(retxTimeout);
}

void EPIC3AllReduceSenderApp::StartAllReduce() {
    NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_rankId 
                << " start AllReduce!");
    
    for (uint32_t i = 0; i < m_windowSize && i < m_pktNum; i++) {
        SendDataPacket(i, i % m_windowSize);
    }
}

void EPIC3AllReduceSenderApp::RecvPacket(Ptr<Socket> socket) {
    Ptr<Packet> pkt;
    while ((pkt = socket->Recv())) {
        EPIC3AllReduceTag tag;
        pkt->PeekPacketTag(tag);
        
        PacketType type = tag.GetType();
        uint32_t psn = tag.GetPsn();
        uint32_t idx = tag.GetIdx();

        if (type == AGGREGATED) {
            SendAckPacket(psn, idx);
            
            if (m_processedAggPsn.count(psn) > 0) {
                NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_rankId 
                            << " receive duplicate AGGREGATED: psn=" << psn << ", idx=" << idx);
                continue;
            }
            m_processedAggPsn.insert(psn);
            
            CancelDataTimer(idx);
            
            m_recvPktNum++;
            
            uint32_t nextPsn = psn + m_windowSize;
            if (nextPsn < m_pktNum) {
                SendDataPacket(nextPsn, idx);
            }
            
            if (m_recvPktNum == m_pktNum) {
                NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_rankId 
                            << " AllReduce completed!");
            }
        }
        else if (type == NAK) {
            if (m_pendingPsn.count(idx) && m_pendingPsn[idx] == psn) {
                CancelDataTimer(idx);
                SendDataPacket(psn, idx);
            }
        }
        else if (type == ACK) {
            CancelDataTimer(idx);
        }
    }
}

void EPIC3AllReduceSenderApp::SendDataPacket(uint32_t psn, uint32_t idx) {
    Ptr<Packet> packet = Create<Packet>(m_pktPayloadSize);
    
    EPIC3AllReduceTag tag;
    tag.SetPsn(psn);
    tag.SetIdx(idx);
    tag.SetWid(m_rankId);
    tag.SetType(DATA);
    packet->AddPacketTag(tag);

    m_sendSocket->Send(packet);
    m_sentPktNum++;
    
    m_pendingPsn[idx] = psn;
    EventId timerId = Simulator::Schedule(m_retxTimeout, 
                                          &EPIC3AllReduceSenderApp::RetransmitData, 
                                          this, idx);
    m_dataTimers[idx] = timerId;
}

void EPIC3AllReduceSenderApp::SendAckPacket(uint32_t psn, uint32_t idx) {
    Ptr<Packet> packet = Create<Packet>(0);
    
    EPIC3AllReduceTag tag;
    tag.SetPsn(psn);
    tag.SetIdx(idx);
    tag.SetWid(m_rankId);
    tag.SetType(ACK);
    packet->AddPacketTag(tag);

    m_sendSocket->Send(packet);
}

void EPIC3AllReduceSenderApp::RetransmitData(uint32_t idx) {
    if (m_pendingPsn.count(idx) == 0) {
        return;
    }
    
    uint32_t psn = m_pendingPsn[idx];
    
    NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_rankId 
                << " retransmit DATA: psn=" << psn << ", idx=" << idx);
    
    m_retxPktNum++;
                
    
    Ptr<Packet> packet = Create<Packet>(m_pktPayloadSize);
    EPIC3AllReduceTag tag;
    tag.SetPsn(psn);
    tag.SetIdx(idx);
    tag.SetWid(m_rankId);
    tag.SetType(DATA);
    tag.SetIsRetransmit(1);
    packet->AddPacketTag(tag);
    
    m_sendSocket->Send(packet);
    
    EventId timerId = Simulator::Schedule(m_retxTimeout, 
                                          &EPIC3AllReduceSenderApp::RetransmitData, 
                                          this, idx);
    m_dataTimers[idx] = timerId;
}

void EPIC3AllReduceSenderApp::CancelDataTimer(uint32_t idx) {
    auto it = m_dataTimers.find(idx);
    if (it != m_dataTimers.end()) {
        Simulator::Cancel(it->second);
        m_dataTimers.erase(it);
    }
    m_pendingPsn.erase(idx);
}

class EPIC3AllReduceReceiverApp : public Application {
public:
    EPIC3AllReduceReceiverApp() : m_rankNum(0), m_bcastTimeout(MicroSeconds(100)) {}
    ~EPIC3AllReduceReceiverApp() {}

    void StartApplication() override;
    void StopApplication() override;

    void InitComm(std::vector<Address> peerAddrList, uint32_t poolSize, uint32_t bcastTimeout);
    void RecvPacket(Ptr<Socket> socket);
    
    void HandleDataPacket(uint32_t psn, uint32_t idx, uint32_t wid, Ptr<Packet> pkt);
    void HandleAckPacket(uint32_t psn, uint32_t idx, uint32_t wid);
    
    void SendAckToWorker(uint32_t psn, uint32_t idx, uint32_t wid);
    void SendNakToWorkers(uint32_t psn, uint32_t idx, const std::vector<uint32_t>& workers);
    void SendAggregatedToAll(uint32_t idx);
    void SendAggregatedToOne(uint32_t idx, uint32_t wid);
    
    void TryMoveToBroadcast(uint32_t idx);
    
    void RetransmitBroadcast(uint32_t idx);
    void CancelBroadcastTimer(uint32_t idx);

    std::vector<AggSlot> m_aggPool;
    
    std::vector<BroadcastSlot> m_bcastPool;
    std::map<uint32_t, EventId> m_bcastTimers;
    
    uint32_t m_poolSize;
    uint32_t m_rankNum;
    Time m_bcastTimeout;

    Ptr<Socket> m_sendSocket;
    Ptr<Socket> m_recvSocket;
    uint16_t m_port;
    std::vector<Address> m_peerAddrList;
};

void EPIC3AllReduceReceiverApp::StartApplication() {
    m_recvSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_recvSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_recvSocket->Bind(InetSocketAddress(Ipv4Address::GetAny(), m_port));
    m_recvSocket->SetRecvCallback(MakeCallback(&EPIC3AllReduceReceiverApp::RecvPacket, this));

    m_sendSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_sendSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_sendSocket->Bind();
}

void EPIC3AllReduceReceiverApp::StopApplication() {
    for (auto& pair : m_bcastTimers) {
        Simulator::Cancel(pair.second);
    }
    m_bcastTimers.clear();
    
    if (m_sendSocket) m_sendSocket->Close();
    if (m_recvSocket) m_recvSocket->Close();
}

void EPIC3AllReduceReceiverApp::InitComm(std::vector<Address> peerAddrList, uint32_t poolSize, uint32_t bcastTimeout) {
    m_peerAddrList = peerAddrList;
    m_poolSize = poolSize;
    m_rankNum = m_peerAddrList.size();
    m_bcastTimeout = MicroSeconds(bcastTimeout);
    m_aggPool.resize(m_poolSize);
    m_bcastPool.resize(m_poolSize);
    
    for (uint32_t i = 0; i < m_poolSize; i++) {
        m_aggPool[i].SetRankNum(m_rankNum);
        m_aggPool[i].m_psn = i;
        m_bcastPool[i].SetRankNum(m_rankNum);
    }
}

void EPIC3AllReduceReceiverApp::RecvPacket(Ptr<Socket> socket) {
    Ptr<Packet> pkt;
    while ((pkt = socket->Recv())) {
        EPIC3AllReduceTag tag;
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
    }
}

void EPIC3AllReduceReceiverApp::HandleDataPacket(uint32_t psn, uint32_t idx, uint32_t wid, Ptr<Packet> pkt) {
    uint32_t aggPsn = m_aggPool[idx].m_psn;
    
    if (psn < aggPsn) {
        if (!m_bcastPool[idx].IsEmpty() && m_bcastPool[idx].GetPsn() == psn) {
            SendAggregatedToOne(idx, wid);
        }
    }
    else if (psn > aggPsn) {
        if (m_aggPool[idx].IsCompleted()) {
            TryMoveToBroadcast(idx);
            
            if (psn == m_aggPool[idx].m_psn) {
                bool completed = m_aggPool[idx].AggPkt(wid, pkt);
                if (completed) {
                    TryMoveToBroadcast(idx);
                }
            }
        } else {
            auto missing = m_aggPool[idx].GetMissingWorkers();
            SendNakToWorkers(aggPsn, idx, missing);
        }
    }
    else {
        if (m_aggPool[idx].HasReceived(wid)) {
        } else {
            bool completed = m_aggPool[idx].AggPkt(wid, pkt);
            
            SendAckToWorker(psn, idx, wid);
            if (completed) {
                TryMoveToBroadcast(idx);
            }
        }
    }
}

void EPIC3AllReduceReceiverApp::HandleAckPacket(uint32_t psn, uint32_t idx, uint32_t wid) {
    if (m_bcastPool[idx].IsEmpty()) {
        return;
    }
    
    if (psn != m_bcastPool[idx].GetPsn()) {
        return;
    }
    
    bool allAcked = m_bcastPool[idx].ReceiveAck(wid);
    
    if (allAcked) {
        CancelBroadcastTimer(idx);
        m_bcastPool[idx].Reset();
        
        if (m_aggPool[idx].IsCompleted()) {
            TryMoveToBroadcast(idx);
        }
    }
}

void EPIC3AllReduceReceiverApp::SendAckToWorker(uint32_t psn, uint32_t idx, uint32_t wid) {

    Ptr<Packet> packet = Create<Packet>(0);
    
    EPIC3AllReduceTag tag;
    tag.SetPsn(psn);
    tag.SetIdx(idx);
    tag.SetWid(wid);
    tag.SetType(ACK);
    packet->AddPacketTag(tag);

    m_sendSocket->SendTo(packet, 0, m_peerAddrList[wid]);
}

void EPIC3AllReduceReceiverApp::SendNakToWorkers(uint32_t psn, uint32_t idx, const std::vector<uint32_t>& workers) {
    for (uint32_t wid : workers) {
        Ptr<Packet> packet = Create<Packet>(0);
        
        EPIC3AllReduceTag tag;
        tag.SetPsn(psn);
        tag.SetIdx(idx);
        tag.SetWid(wid);
        tag.SetType(NAK);
        packet->AddPacketTag(tag);

        m_sendSocket->SendTo(packet, 0, m_peerAddrList[wid]);
    }
}

void EPIC3AllReduceReceiverApp::TryMoveToBroadcast(uint32_t idx) {
    if (!m_bcastPool[idx].IsEmpty()) {
        return;
    }
    
    if (!m_aggPool[idx].IsCompleted()) {
        return;
    }
    
    uint32_t psn = m_aggPool[idx].m_psn;
    Ptr<Packet> pkt = m_aggPool[idx].GetAggPkt();
    m_bcastPool[idx].SetResult(psn, pkt);
    
    SendAggregatedToAll(idx);
    
    EventId timerId = Simulator::Schedule(m_bcastTimeout,
                                          &EPIC3AllReduceReceiverApp::RetransmitBroadcast,
                                          this, idx);
    m_bcastTimers[idx] = timerId;
    
    m_aggPool[idx].m_psn += m_poolSize;
    m_aggPool[idx].Reset();
}

void EPIC3AllReduceReceiverApp::SendAggregatedToAll(uint32_t idx) {
    Ptr<Packet> pkt = m_bcastPool[idx].GetPkt();
    uint32_t psn = m_bcastPool[idx].GetPsn();
    
    for (uint32_t wid = 0; wid < m_rankNum; wid++) {
        Ptr<Packet> copy = pkt->Copy();
        
        EPIC3AllReduceTag tag;
        copy->RemovePacketTag(tag);
        tag.SetPsn(psn);
        tag.SetIdx(idx);
        tag.SetWid(wid);
        tag.SetType(AGGREGATED);
        copy->AddPacketTag(tag);

        m_sendSocket->SendTo(copy, 0, m_peerAddrList[wid]);
    }
}

void EPIC3AllReduceReceiverApp::SendAggregatedToOne(uint32_t idx, uint32_t wid) {
    Ptr<Packet> pkt = m_bcastPool[idx].GetPkt();
    uint32_t psn = m_bcastPool[idx].GetPsn();
    
    Ptr<Packet> copy = pkt->Copy();
    
    EPIC3AllReduceTag tag;
    copy->RemovePacketTag(tag);
    tag.SetPsn(psn);
    tag.SetIdx(idx);
    tag.SetWid(wid);
    tag.SetType(AGGREGATED);
    copy->AddPacketTag(tag);

    m_sendSocket->SendTo(copy, 0, m_peerAddrList[wid]);
}

void EPIC3AllReduceReceiverApp::RetransmitBroadcast(uint32_t idx) {
    if (m_bcastPool[idx].IsEmpty()) {
        return;
    }

    auto unacked = m_bcastPool[idx].GetUnackedWorkers();
    
    for (uint32_t wid : unacked) {
        SendAggregatedToOne(idx, wid);
    }
    
    EventId timerId = Simulator::Schedule(m_bcastTimeout,
                                          &EPIC3AllReduceReceiverApp::RetransmitBroadcast,
                                          this, idx);
    m_bcastTimers[idx] = timerId;
}

void EPIC3AllReduceReceiverApp::CancelBroadcastTimer(uint32_t idx) {
    auto it = m_bcastTimers.find(idx);
    if (it != m_bcastTimers.end()) {
        Simulator::Cancel(it->second);
        m_bcastTimers.erase(it);
    }
}

int main(int argc, char *argv[]) {
    LogComponentEnable("EPIC3AllReduceTest", LOG_LEVEL_INFO);

    uint32_t numWorkers = 8;
    uint16_t port = 9999;
        
    uint64_t dataSize = 1024 * 1024;
    uint32_t payloadSize = 1400;
    uint32_t windowSize = 512;
    uint32_t retxTimeout = 80;
    uint32_t bcastTimeout = 80;

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

    uint32_t poolSize = windowSize;

    Ptr<EPIC3AllReduceReceiverApp> receiverApp = CreateObject<EPIC3AllReduceReceiverApp>();
    receiverApp->m_port = port;
    receiverApp->InitComm(peerAddrList, poolSize, bcastTimeout);

    switchNode->AddApplication(receiverApp);
    receiverApp->SetStartTime(Seconds(1.0));
    receiverApp->SetStopTime(Seconds(1.3));

    for (uint32_t i = 0; i < numWorkers; i++) {
        Ptr<EPIC3AllReduceSenderApp> senderApp = CreateObject<EPIC3AllReduceSenderApp>();

        senderApp->m_rankId = i;
        senderApp->m_port = port;
        senderApp->m_peerAddr = InetSocketAddress(switchAddr, port);
        senderApp->InitComm(dataSize, payloadSize, windowSize, retxTimeout);

        workerNodes.Get(i)->AddApplication(senderApp);
        senderApp->SetStartTime(Seconds(1.0));
        senderApp->SetStopTime(Seconds(10.0));
    }

    NS_LOG_INFO("========== EPIC3 AllReduce Start ==========");

    Simulator::Run();

    NS_LOG_INFO("========== EPIC3 AllReduce End ==========");

    Simulator::Destroy();

    return 0;
}

#endif
