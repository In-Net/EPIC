#ifndef EPIC3_REDUCE_APPLICATION_H
#define EPIC3_REDUCE_APPLICATION_H

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

NS_LOG_COMPONENT_DEFINE("EPIC3ReduceTest");

enum PacketType : uint8_t {
    DATA = 0,
    AGGREGATED = 1,
    ACK = 2,
    NAK = 3
};

class EPIC3ReduceTag : public Tag {
public:
    EPIC3ReduceTag() : m_psn(0), m_idx(0), m_wid(0), m_type(DATA) {}

    static TypeId GetTypeId() {
        static TypeId tid = TypeId("ns3::EPIC3ReduceTag")
            .SetParent<Tag>()
            .AddConstructor<EPIC3ReduceTag>();
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
        os << "EPIC3ReduceTag: psn=" << m_psn 
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
        m_sentToRoot = false;
        std::fill(m_arrState.begin(), m_arrState.end(), false);
    }

    uint32_t m_psn = 0;
    uint32_t m_count = 0;
    uint32_t m_num = 0;
    Ptr<Packet> m_pkt = nullptr;
    std::vector<bool> m_arrState;
    bool m_sentToRoot = false;
};

class EPIC3ReduceSenderApp : public Application {
public:
    EPIC3ReduceSenderApp() : m_rankId(0), m_rootRank(0), m_sentPktNum(0), m_recvPktNum(0), 
                      m_retxTimeout(MicroSeconds(100)) {}
    ~EPIC3ReduceSenderApp() {}

    void StartApplication() override;
    void StopApplication() override;

    void InitComm(uint64_t dataSize, uint32_t pktPayloadSize, uint32_t windowSize);
    void StartReduce();
    void RecvPacket(Ptr<Socket> socket);
    
    bool IsRootRank() const { return m_rankId == m_rootRank; }
    
    void SendDataPacket(uint32_t psn, uint32_t idx);
    
    void SendAckPacket(uint32_t psn, uint32_t idx);
    
    void RetransmitData(uint32_t idx);
    void CancelDataTimer(uint32_t idx);

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

void EPIC3ReduceSenderApp::StartApplication() {
    m_recvSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_recvSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_recvSocket->Bind(InetSocketAddress(Ipv4Address::GetAny(), m_port));
    m_recvSocket->SetRecvCallback(MakeCallback(&EPIC3ReduceSenderApp::RecvPacket, this));

    m_sendSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_sendSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_sendSocket->Bind();
    m_sendSocket->Connect(m_peerAddr);

    Simulator::Schedule(MicroSeconds(1), &EPIC3ReduceSenderApp::StartReduce, this);
}

void EPIC3ReduceSenderApp::StopApplication() {
    
    for (auto& pair : m_dataTimers) {
        Simulator::Cancel(pair.second);
    }
    m_dataTimers.clear();
    m_pendingPsn.clear();
    m_processedPsn.clear();
    
    if (m_sendSocket) m_sendSocket->Close();
    if (m_recvSocket) m_recvSocket->Close();
}

void EPIC3ReduceSenderApp::InitComm(uint64_t dataSize, uint32_t pktPayloadSize, uint32_t windowSize) {
    m_dataSize = dataSize;
    m_pktPayloadSize = pktPayloadSize;
    m_pktNum = dataSize / pktPayloadSize;
    m_windowSize = windowSize;
    m_poolSize = 2 * windowSize;
}

void EPIC3ReduceSenderApp::StartReduce() {
    if (IsRootRank()) {
        NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_rankId 
                    << " (Root) Start Reduce!");
    } else {
        NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_rankId 
                    << " Start Reduce!");
        
        for (uint32_t i = 0; i < m_windowSize && i < m_pktNum; i++) {
            SendDataPacket(i, i % m_poolSize);
        }
    }
}

void EPIC3ReduceSenderApp::RecvPacket(Ptr<Socket> socket) {
    Ptr<Packet> pkt;
    while ((pkt = socket->Recv())) {
        EPIC3ReduceTag tag;
        pkt->PeekPacketTag(tag);
        
        PacketType type = tag.GetType();
        uint32_t psn = tag.GetPsn();
        uint32_t idx = tag.GetIdx();

        if (IsRootRank()) {
            if (type == AGGREGATED) {
                SendAckPacket(psn, idx);
                
                if (m_processedPsn.count(psn) > 0) {
                    continue;
                }
                m_processedPsn.insert(psn);
                
                m_recvPktNum++;
                
                if (m_recvPktNum == m_pktNum) {
                    NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_rankId 
                                << " (Root) Reduce completed!");
                }
            }
        } else {
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
                                << " Reduce completed!");
                }
            }
            else if (type == NAK) {
                if (m_pendingPsn.count(idx) && m_pendingPsn[idx] == psn) {
                    CancelDataTimer(idx);
                    SendDataPacket(psn, idx);
                }
            }
        }
    }
}

void EPIC3ReduceSenderApp::SendDataPacket(uint32_t psn, uint32_t idx) {
    Ptr<Packet> packet = Create<Packet>(m_pktPayloadSize);
    
    EPIC3ReduceTag tag;
    tag.SetPsn(psn);
    tag.SetIdx(idx);
    tag.SetWid(m_rankId);
    tag.SetType(DATA);
    packet->AddPacketTag(tag);

    m_sendSocket->Send(packet);
    m_sentPktNum++;
    
    m_pendingPsn[idx] = psn;
    EventId timerId = Simulator::Schedule(m_retxTimeout, 
                                          &EPIC3ReduceSenderApp::RetransmitData, 
                                          this, idx);
    m_dataTimers[idx] = timerId;
}

void EPIC3ReduceSenderApp::SendAckPacket(uint32_t psn, uint32_t idx) {
    Ptr<Packet> packet = Create<Packet>(0);
    
    EPIC3ReduceTag tag;
    tag.SetPsn(psn);
    tag.SetIdx(idx);
    tag.SetWid(m_rankId);
    tag.SetType(ACK);
    packet->AddPacketTag(tag);

    m_sendSocket->Send(packet);
}

void EPIC3ReduceSenderApp::RetransmitData(uint32_t idx) {
    if (m_pendingPsn.count(idx) == 0) {
        return;
    }
    
    uint32_t psn = m_pendingPsn[idx];
    
    Ptr<Packet> packet = Create<Packet>(m_pktPayloadSize);
    EPIC3ReduceTag tag;
    tag.SetPsn(psn);
    tag.SetIdx(idx);
    tag.SetWid(m_rankId);
    tag.SetType(DATA);
    packet->AddPacketTag(tag);
    
    m_sendSocket->Send(packet);
    
    EventId timerId = Simulator::Schedule(m_retxTimeout, 
                                          &EPIC3ReduceSenderApp::RetransmitData, 
                                          this, idx);
    m_dataTimers[idx] = timerId;
}

void EPIC3ReduceSenderApp::CancelDataTimer(uint32_t idx) {
    auto it = m_dataTimers.find(idx);
    if (it != m_dataTimers.end()) {
        Simulator::Cancel(it->second);
        m_dataTimers.erase(it);
    }
    m_pendingPsn.erase(idx);
}

class EPIC3ReduceReceiverApp : public Application {
public:
    EPIC3ReduceReceiverApp() : m_rankNum(0), m_rootRank(0), m_aggTimeout(MicroSeconds(100)) {}
    ~EPIC3ReduceReceiverApp() {}

    void StartApplication() override;
    void StopApplication() override;

    void InitComm(std::vector<Address> peerAddrList, uint32_t poolSize, uint32_t rootRank);
    void RecvPacket(Ptr<Socket> socket);
    
    void HandleDataPacket(uint32_t psn, uint32_t idx, uint32_t wid, Ptr<Packet> pkt);
    void HandleAckPacket(uint32_t psn, uint32_t idx, uint32_t wid);
    
    void SendAckToWorker(uint32_t psn, uint32_t idx, uint32_t wid);
    void SendNakToWorkers(uint32_t psn, uint32_t idx, const std::vector<uint32_t>& workers);
    void SendAggregatedToRoot(uint32_t idx);
    
    void OnAggregationComplete(uint32_t idx);
    
    void RetransmitToRoot(uint32_t idx);
    void CancelAggTimer(uint32_t idx);

    std::vector<AggSlot> m_aggPool;
    std::map<uint32_t, EventId> m_aggTimers;
    
    uint32_t m_poolSize;
    uint32_t m_rankNum;
    uint32_t m_rootRank;
    Time m_aggTimeout;

    Ptr<Socket> m_sendSocket;
    Ptr<Socket> m_recvSocket;
    uint16_t m_port;
    std::vector<Address> m_peerAddrList;
};

void EPIC3ReduceReceiverApp::StartApplication() {
    m_recvSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_recvSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_recvSocket->Bind(InetSocketAddress(Ipv4Address::GetAny(), m_port));
    m_recvSocket->SetRecvCallback(MakeCallback(&EPIC3ReduceReceiverApp::RecvPacket, this));

    m_sendSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_sendSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_sendSocket->Bind();
}

void EPIC3ReduceReceiverApp::StopApplication() {
    for (auto& pair : m_aggTimers) {
        Simulator::Cancel(pair.second);
    }
    m_aggTimers.clear();
    
    if (m_sendSocket) m_sendSocket->Close();
    if (m_recvSocket) m_recvSocket->Close();
}

void EPIC3ReduceReceiverApp::InitComm(std::vector<Address> peerAddrList, uint32_t poolSize, uint32_t rootRank) {
    m_peerAddrList = peerAddrList;
    m_poolSize = poolSize;
    m_rankNum = m_peerAddrList.size();
    m_rootRank = rootRank;

    m_aggPool.resize(m_poolSize);
    
    for (uint32_t i = 0; i < m_poolSize; i++) {
        m_aggPool[i].SetRankNum(m_rankNum - 1);
        m_aggPool[i].m_psn = i;
    }
}

void EPIC3ReduceReceiverApp::RecvPacket(Ptr<Socket> socket) {
    Ptr<Packet> pkt;
    while ((pkt = socket->Recv())) {
        EPIC3ReduceTag tag;
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

void EPIC3ReduceReceiverApp::HandleDataPacket(uint32_t psn, uint32_t idx, uint32_t wid, Ptr<Packet> pkt) {
    uint32_t arrIdx = wid - 1;
    
    uint32_t aggPsn = m_aggPool[idx].m_psn;
    
    if (psn < aggPsn) {
        SendAckToWorker(psn, idx, wid);
    }
    else if (psn > aggPsn) {
        if (m_aggPool[idx].IsCompleted()) {
            if (!m_aggPool[idx].m_sentToRoot) {
                OnAggregationComplete(idx);
            }
        } else {
            auto missing = m_aggPool[idx].GetMissingWorkers();
            std::vector<uint32_t> missingWids;
            for (uint32_t arrI : missing) {
                missingWids.push_back(arrI + 1);
            }
            SendNakToWorkers(aggPsn, idx, missingWids);
        }
    }
    else {
        if (m_aggPool[idx].HasReceived(arrIdx)) {
            SendAckToWorker(psn, idx, wid);
        } else {
            SendAckToWorker(psn, idx, wid);
            
            bool completed = m_aggPool[idx].AggPkt(arrIdx, pkt);
            
            if (completed) {
                OnAggregationComplete(idx);
            }
        }
    }
}

void EPIC3ReduceReceiverApp::HandleAckPacket(uint32_t psn, uint32_t idx, uint32_t wid) {
    if (wid != m_rootRank) {
        return;
    }
    
    uint32_t aggPsn = m_aggPool[idx].m_psn;
    
    if (psn != aggPsn) {
        return;
    }
    
    CancelAggTimer(idx);
    
    m_aggPool[idx].m_psn += m_poolSize;
    
    m_aggPool[idx].Reset();
}

void EPIC3ReduceReceiverApp::SendAckToWorker(uint32_t psn, uint32_t idx, uint32_t wid) {
    Ptr<Packet> packet = Create<Packet>(0);
    
    EPIC3ReduceTag tag;
    tag.SetPsn(psn);
    tag.SetIdx(idx);
    tag.SetWid(wid);
    tag.SetType(ACK);
    packet->AddPacketTag(tag);

    m_sendSocket->SendTo(packet, 0, m_peerAddrList[wid]);
}

void EPIC3ReduceReceiverApp::SendNakToWorkers(uint32_t psn, uint32_t idx, const std::vector<uint32_t>& workers) {
    for (uint32_t wid : workers) {
        Ptr<Packet> packet = Create<Packet>(0);
        
        EPIC3ReduceTag tag;
        tag.SetPsn(psn);
        tag.SetIdx(idx);
        tag.SetWid(wid);
        tag.SetType(NAK);
        packet->AddPacketTag(tag);

        m_sendSocket->SendTo(packet, 0, m_peerAddrList[wid]);
    }
}

void EPIC3ReduceReceiverApp::OnAggregationComplete(uint32_t idx) {
    SendAggregatedToRoot(idx);
    m_aggPool[idx].m_sentToRoot = true;
    
    EventId timerId = Simulator::Schedule(m_aggTimeout,
                                          &EPIC3ReduceReceiverApp::RetransmitToRoot,
                                          this, idx);
    m_aggTimers[idx] = timerId;
}

void EPIC3ReduceReceiverApp::SendAggregatedToRoot(uint32_t idx) {
    Ptr<Packet> pkt = m_aggPool[idx].GetAggPkt();
    uint32_t psn = m_aggPool[idx].m_psn;
    
    Ptr<Packet> copy = pkt->Copy();
    
    EPIC3ReduceTag tag;
    copy->RemovePacketTag(tag);
    tag.SetPsn(psn);
    tag.SetIdx(idx);
    tag.SetWid(m_rootRank);
    tag.SetType(AGGREGATED);
    copy->AddPacketTag(tag);

    m_sendSocket->SendTo(copy, 0, m_peerAddrList[m_rootRank]);
}

void EPIC3ReduceReceiverApp::RetransmitToRoot(uint32_t idx) {
    if (!m_aggPool[idx].IsCompleted() || !m_aggPool[idx].m_sentToRoot) {
        return;
    }
    
    SendAggregatedToRoot(idx);
    
    EventId timerId = Simulator::Schedule(m_aggTimeout,
                                          &EPIC3ReduceReceiverApp::RetransmitToRoot,
                                          this, idx);
    m_aggTimers[idx] = timerId;
}

void EPIC3ReduceReceiverApp::CancelAggTimer(uint32_t idx) {
    auto it = m_aggTimers.find(idx);
    if (it != m_aggTimers.end()) {
        Simulator::Cancel(it->second);
        m_aggTimers.erase(it);
    }
}

int main(int argc, char *argv[]) {
    LogComponentEnable("EPIC3ReduceTest", LOG_LEVEL_INFO);

    uint32_t numWorkers = 8;
    uint16_t port = 9999;
    uint64_t dataSize = 1024 * 1024;  
    uint32_t payloadSize = 1400;
    uint32_t windowSize = 512;
    uint32_t poolSize = 2 * windowSize;
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

    Ptr<EPIC3ReduceReceiverApp> receiverApp = CreateObject<EPIC3ReduceReceiverApp>();
    receiverApp->m_port = port;
    receiverApp->InitComm(peerAddrList, poolSize, rootRank);

    switchNode->AddApplication(receiverApp);
    receiverApp->SetStartTime(Seconds(1.0));
    receiverApp->SetStopTime(Seconds(30.0));

    for (uint32_t i = 0; i < numWorkers; i++) {
        Ptr<EPIC3ReduceSenderApp> senderApp = CreateObject<EPIC3ReduceSenderApp>();

        senderApp->m_rankId = i;
        senderApp->m_rootRank = rootRank;
        senderApp->m_port = port;
        senderApp->m_peerAddr = InetSocketAddress(switchAddr, port);
        senderApp->InitComm(dataSize, payloadSize, windowSize);

        workerNodes.Get(i)->AddApplication(senderApp);
        senderApp->SetStartTime(Seconds(1.0));
        senderApp->SetStopTime(Seconds(10.0));
    }

    NS_LOG_INFO("========== EPIC3 Reduce Start ==========");

    Simulator::Run();

    NS_LOG_INFO("========== EPIC3 Reduce End ==========");

    Simulator::Destroy();

    return 0;
}

#endif
