#ifndef EPIC3_ALLGATHER_APPLICATION_H
#define EPIC3_ALLGATHER_APPLICATION_H

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

NS_LOG_COMPONENT_DEFINE("EPIC3AllGatherTest");

enum PacketType : uint8_t {
    DATA = 0,
    ACK = 1,
    NAK = 2,
    START = 3
};

class EPIC3AllGatherTag : public Tag {
public:
    EPIC3AllGatherTag() : m_psn(0), m_idx(0), m_wid(0), m_srcRank(0), m_type(DATA) {}

    static TypeId GetTypeId() {
        static TypeId tid = TypeId("ns3::EPIC3AllGatherTag")
            .SetParent<Tag>()
            .AddConstructor<EPIC3AllGatherTag>();
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

    void SetSrcRank(uint32_t srcRank) { m_srcRank = srcRank; }
    uint32_t GetSrcRank() const { return m_srcRank; }

    uint32_t GetSerializedSize() const override {
        return 4 * sizeof(uint32_t) + sizeof(uint8_t);
    }

    void Serialize(TagBuffer i) const override {
        i.WriteU32(m_psn);
        i.WriteU32(m_idx);
        i.WriteU32(m_wid);
        i.WriteU32(m_srcRank);
        i.WriteU8(m_type);
    }

    void Deserialize(TagBuffer i) override {
        m_psn = i.ReadU32();
        m_idx = i.ReadU32();
        m_wid = i.ReadU32();
        m_srcRank = i.ReadU32();
        m_type = static_cast<PacketType>(i.ReadU8());
    }

    void Print(std::ostream& os) const override {
        os << "EPIC3AllGatherTag: psn=" << m_psn 
           << ", idx=" << m_idx 
           << ", wid=" << m_wid
           << ", srcRank=" << m_srcRank
           << ", type=" << (int)m_type;
    }

private:
    uint32_t m_psn;
    uint32_t m_idx;
    uint32_t m_wid;
    uint32_t m_srcRank;
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

class EPIC3AllGatherSenderApp : public Application {
public:
    EPIC3AllGatherSenderApp() : m_rankId(0), m_rankNum(0), m_sentPktNum(0), m_recvPktNum(0), 
                      m_recvAckNum(0), m_retxTimeout(MicroSeconds(100)) {}
    ~EPIC3AllGatherSenderApp() {}

    void StartApplication() override;
    void StopApplication() override;

    void InitComm(uint64_t dataSize, uint32_t pktPayloadSize, uint32_t windowSize, uint32_t rankNum);
    void StartAllGather();
    void RecvPacket(Ptr<Socket> socket);
    
    void StartBroadcastAsRoot();
    void SendDataPacket(uint32_t psn, uint32_t idx);
    void RetransmitData(uint32_t idx);
    void CancelDataTimer(uint32_t idx);
    
    void SendAckPacket(uint32_t psn, uint32_t idx, uint32_t srcRank);

    uint32_t m_rankId;
    uint32_t m_rankNum;
    uint64_t m_dataSize;
    uint32_t m_pktPayloadSize;
    uint32_t m_pktNum;
    uint32_t m_sentPktNum;
    uint32_t m_recvPktNum;
    uint32_t m_recvAckNum;
    uint32_t m_windowSize;
    uint32_t m_poolSize;

    Ptr<Socket> m_sendSocket;
    Ptr<Socket> m_recvSocket;
    Address m_peerAddr;
    uint16_t m_port;

    Time m_retxTimeout;
    std::map<uint32_t, EventId> m_dataTimers;
    std::map<uint32_t, uint32_t> m_pendingPsn;
    std::set<uint32_t> m_processedAckPsn;
    
    std::map<uint32_t, std::set<uint32_t>> m_processedDataPsn;
    
    uint32_t m_currentRoot = 0;
    bool m_myBroadcastDone = false;
};

void EPIC3AllGatherSenderApp::StartApplication() {
    m_recvSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_recvSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_recvSocket->Bind(InetSocketAddress(Ipv4Address::GetAny(), m_port));
    m_recvSocket->SetRecvCallback(MakeCallback(&EPIC3AllGatherSenderApp::RecvPacket, this));

    m_sendSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_sendSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_sendSocket->Bind();
    m_sendSocket->Connect(m_peerAddr);

    Simulator::Schedule(MicroSeconds(1), &EPIC3AllGatherSenderApp::StartAllGather, this);
}

void EPIC3AllGatherSenderApp::StopApplication() {
    for (auto& pair : m_dataTimers) {
        Simulator::Cancel(pair.second);
    }
    m_dataTimers.clear();
    m_pendingPsn.clear();
    m_processedAckPsn.clear();
    m_processedDataPsn.clear();
    
    if (m_sendSocket) m_sendSocket->Close();
    if (m_recvSocket) m_recvSocket->Close();
}

void EPIC3AllGatherSenderApp::InitComm(uint64_t dataSize, uint32_t pktPayloadSize, uint32_t windowSize, uint32_t rankNum) {
    m_dataSize = dataSize;
    m_pktPayloadSize = pktPayloadSize;
    m_pktNum = dataSize / pktPayloadSize;
    m_windowSize = windowSize;
    m_poolSize = windowSize;
    m_rankNum = rankNum;
}

void EPIC3AllGatherSenderApp::StartAllGather() {
    NS_LOG_INFO("[" << Simulator::Now().GetSeconds() << "s] Rank " << m_rankId 
                << " start AllGather!");
    
    if (m_rankId == 0) {
        StartBroadcastAsRoot();
    }
}

void EPIC3AllGatherSenderApp::StartBroadcastAsRoot() {
    /*NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_rankId 
                << " start broadcast as root!");*/
    
    m_recvAckNum = 0;
    m_processedAckPsn.clear();
    for (auto& pair : m_dataTimers) {
        Simulator::Cancel(pair.second);
    }
    m_dataTimers.clear();
    m_pendingPsn.clear();
    
    for (uint32_t i = 0; i < m_windowSize && i < m_pktNum; i++) {
        SendDataPacket(i, i % m_poolSize);
    }
}

void EPIC3AllGatherSenderApp::RecvPacket(Ptr<Socket> socket) {
    Ptr<Packet> pkt;
    while ((pkt = socket->Recv())) {
        EPIC3AllGatherTag tag;
        pkt->PeekPacketTag(tag);
        
        PacketType type = tag.GetType();
        uint32_t psn = tag.GetPsn();
        uint32_t idx = tag.GetIdx();
        uint32_t srcRank = tag.GetSrcRank();

        if (type == START) {
            m_currentRoot = srcRank;
            if (m_rankId == m_currentRoot) {
                StartBroadcastAsRoot();
            }
        }
        else if (type == ACK && srcRank == m_rankId) {
            if (m_processedAckPsn.count(psn) > 0) {
                continue;
            }
            m_processedAckPsn.insert(psn);
            
            CancelDataTimer(idx);
            m_recvAckNum++;
            
            uint32_t nextPsn = psn + m_windowSize;
            if (nextPsn < m_pktNum) {
                SendDataPacket(nextPsn, nextPsn % m_poolSize);
            }
            
            if (m_recvAckNum == m_pktNum && !m_myBroadcastDone) {
                m_myBroadcastDone = true;
                /*NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_rankId 
                            << " broadcast round " << m_currentRoot << " completed!");*/
                
                uint32_t totalExpected = m_pktNum * (m_rankNum - 1);
                if (m_recvPktNum == totalExpected) {
                    NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_rankId 
                                << " AllGather completed!");
                }
            }
        }
        else if (type == DATA && srcRank != m_rankId) {
            SendAckPacket(psn, idx, srcRank);
            
            if (m_processedDataPsn[srcRank].count(psn) > 0) {
                continue;
            }
            m_processedDataPsn[srcRank].insert(psn);
            
            m_recvPktNum++;
            
            uint32_t totalExpected = m_pktNum * (m_rankNum - 1);
            if (m_recvPktNum == totalExpected && m_myBroadcastDone) {
                NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_rankId 
                            << " AllGather completed!");
            }
        }
    }
}

void EPIC3AllGatherSenderApp::SendDataPacket(uint32_t psn, uint32_t idx) {
    Ptr<Packet> packet = Create<Packet>(m_pktPayloadSize);
    
    EPIC3AllGatherTag tag;
    tag.SetPsn(psn);
    tag.SetIdx(idx);
    tag.SetWid(m_rankId);
    tag.SetSrcRank(m_rankId);
    tag.SetType(DATA);
    packet->AddPacketTag(tag);

    m_sendSocket->Send(packet);
    m_sentPktNum++;
    
    m_pendingPsn[idx] = psn;
    EventId timerId = Simulator::Schedule(m_retxTimeout, 
                                          &EPIC3AllGatherSenderApp::RetransmitData, 
                                          this, idx);
    m_dataTimers[idx] = timerId;
}

void EPIC3AllGatherSenderApp::SendAckPacket(uint32_t psn, uint32_t idx, uint32_t srcRank) {
    Ptr<Packet> packet = Create<Packet>(0);
    
    EPIC3AllGatherTag tag;
    tag.SetPsn(psn);
    tag.SetIdx(idx);
    tag.SetWid(m_rankId);
    tag.SetSrcRank(srcRank);
    tag.SetType(ACK);
    packet->AddPacketTag(tag);

    m_sendSocket->Send(packet);
}

void EPIC3AllGatherSenderApp::RetransmitData(uint32_t idx) {
    if (m_pendingPsn.count(idx) == 0) {
        return;
    }
    
    uint32_t psn = m_pendingPsn[idx];
    
    Ptr<Packet> packet = Create<Packet>(m_pktPayloadSize);
    EPIC3AllGatherTag tag;
    tag.SetPsn(psn);
    tag.SetIdx(idx);
    tag.SetWid(m_rankId);
    tag.SetSrcRank(m_rankId);
    tag.SetType(DATA);
    packet->AddPacketTag(tag);
    
    m_sendSocket->Send(packet);
    
    EventId timerId = Simulator::Schedule(m_retxTimeout, 
                                          &EPIC3AllGatherSenderApp::RetransmitData, 
                                          this, idx);
    m_dataTimers[idx] = timerId;
}

void EPIC3AllGatherSenderApp::CancelDataTimer(uint32_t idx) {
    auto it = m_dataTimers.find(idx);
    if (it != m_dataTimers.end()) {
        Simulator::Cancel(it->second);
        m_dataTimers.erase(it);
    }
    m_pendingPsn.erase(idx);
}

class EPIC3AllGatherReceiverApp : public Application {
public:
    EPIC3AllGatherReceiverApp() : m_rankNum(0), m_currentRoot(0), m_bcastTimeout(MicroSeconds(100)) {}
    ~EPIC3AllGatherReceiverApp() {}

    void StartApplication() override;
    void StopApplication() override;

    void InitComm(std::vector<Address> peerAddrList, uint32_t poolSize, uint32_t pktNum);
    void RecvPacket(Ptr<Socket> socket);
    
    void HandleDataPacket(uint32_t psn, uint32_t idx, uint32_t wid, uint32_t srcRank, Ptr<Packet> pkt);
    void HandleAckPacket(uint32_t psn, uint32_t idx, uint32_t wid, uint32_t srcRank);
    
    void SendAckToRoot(uint32_t psn, uint32_t idx);
    void SendNakToRoot(uint32_t psn, uint32_t idx);
    void SendDataToAllChildren(uint32_t idx);
    void SendDataToChild(uint32_t idx, uint32_t childWid);
    
    void OnBroadcastComplete(uint32_t idx);
    void OnRoundComplete();
    void SendStartToNextRoot();
    
    void RetransmitToChildren(uint32_t idx);
    void CancelBroadcastTimer(uint32_t idx);
    
    std::vector<uint32_t> GetChildWids() const;
    uint32_t GetChildIdx(uint32_t wid) const;

    std::vector<BroadcastSlot> m_bcastPool;
    std::map<uint32_t, EventId> m_bcastTimers;
    
    uint32_t m_poolSize;
    uint32_t m_rankNum;
    uint32_t m_pktNum;
    uint32_t m_currentRoot;
    uint32_t m_completedPkts;
    Time m_bcastTimeout;

    Ptr<Socket> m_sendSocket;
    Ptr<Socket> m_recvSocket;
    uint16_t m_port;
    std::vector<Address> m_peerAddrList;
};

void EPIC3AllGatherReceiverApp::StartApplication() {
    m_recvSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_recvSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_recvSocket->Bind(InetSocketAddress(Ipv4Address::GetAny(), m_port));
    m_recvSocket->SetRecvCallback(MakeCallback(&EPIC3AllGatherReceiverApp::RecvPacket, this));

    m_sendSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_sendSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_sendSocket->Bind();
}

void EPIC3AllGatherReceiverApp::StopApplication() {
    for (auto& pair : m_bcastTimers) {
        Simulator::Cancel(pair.second);
    }
    m_bcastTimers.clear();
    
    if (m_sendSocket) m_sendSocket->Close();
    if (m_recvSocket) m_recvSocket->Close();
}

void EPIC3AllGatherReceiverApp::InitComm(std::vector<Address> peerAddrList, uint32_t poolSize, uint32_t pktNum) {
    m_peerAddrList = peerAddrList;
    m_poolSize = poolSize;
    m_rankNum = m_peerAddrList.size();
    m_pktNum = pktNum;
    m_currentRoot = 0;
    m_completedPkts = 0;

    m_bcastPool.resize(m_poolSize);
    for (uint32_t i = 0; i < m_poolSize; i++) {
        m_bcastPool[i].SetChildNum(m_rankNum - 1);
        m_bcastPool[i].m_psn = i;
    }
}

std::vector<uint32_t> EPIC3AllGatherReceiverApp::GetChildWids() const {
    std::vector<uint32_t> children;
    for (uint32_t i = 0; i < m_rankNum; i++) {
        if (i != m_currentRoot) {
            children.push_back(i);
        }
    }
    return children;
}

uint32_t EPIC3AllGatherReceiverApp::GetChildIdx(uint32_t wid) const {
    if (wid < m_currentRoot) {
        return wid;
    } else {
        return wid - 1;
    }
}

void EPIC3AllGatherReceiverApp::RecvPacket(Ptr<Socket> socket) {
    Ptr<Packet> pkt;
    while ((pkt = socket->Recv())) {
        EPIC3AllGatherTag tag;
        pkt->PeekPacketTag(tag);
        
        PacketType type = tag.GetType();
        uint32_t psn = tag.GetPsn();
        uint32_t idx = tag.GetIdx();
        uint32_t wid = tag.GetWid();
        uint32_t srcRank = tag.GetSrcRank();

        if (type == DATA) {
            HandleDataPacket(psn, idx, wid, srcRank, pkt);
        }
        else if (type == ACK) {
            HandleAckPacket(psn, idx, wid, srcRank);
        }
    }
}

void EPIC3AllGatherReceiverApp::HandleDataPacket(uint32_t psn, uint32_t idx, uint32_t wid, uint32_t srcRank, Ptr<Packet> pkt) {
    if (srcRank != m_currentRoot || wid != srcRank) {
        return;
    }
    
    BroadcastSlot& slot = m_bcastPool[idx];
    uint32_t bpsn = slot.m_psn;
    
    if (psn < bpsn) {
        SendAckToRoot(psn, idx);
    }
    else if (psn > bpsn) {
        if (!slot.HasData()) {
            SendNakToRoot(bpsn, idx);
        } else {
            auto unacked = slot.GetUnackedChildren();
            auto children = GetChildWids();
            for (uint32_t childIdx : unacked) {
                SendDataToChild(idx, children[childIdx]);
            }
            CancelBroadcastTimer(idx);
            EventId timerId = Simulator::Schedule(m_bcastTimeout,
                                                  &EPIC3AllGatherReceiverApp::RetransmitToChildren,
                                                  this, idx);
            m_bcastTimers[idx] = timerId;
        }
    }
    else {
        if (slot.HasData()) {
            SendAckToRoot(psn, idx);
        } else {
            slot.SetData(psn, pkt);
            SendAckToRoot(psn, idx);
            SendDataToAllChildren(idx);
            
            EventId timerId = Simulator::Schedule(m_bcastTimeout,
                                                  &EPIC3AllGatherReceiverApp::RetransmitToChildren,
                                                  this, idx);
            m_bcastTimers[idx] = timerId;
        }
    }
}

void EPIC3AllGatherReceiverApp::HandleAckPacket(uint32_t psn, uint32_t idx, uint32_t wid, uint32_t srcRank) {
    if (srcRank != m_currentRoot || wid == m_currentRoot) {
        return;
    }
    
    BroadcastSlot& slot = m_bcastPool[idx];
    uint32_t bpsn = slot.m_psn;
    
    if (psn != bpsn) {
        return;
    }
    
    uint32_t childIdx = GetChildIdx(wid);
    
    if (slot.HasAcked(childIdx)) {
        return;
    }
    
    bool allAcked = slot.ReceiveAck(childIdx);
    
    if (allAcked) {
        OnBroadcastComplete(idx);
    }
}

void EPIC3AllGatherReceiverApp::SendAckToRoot(uint32_t psn, uint32_t idx) {
    Ptr<Packet> packet = Create<Packet>(0);
    
    EPIC3AllGatherTag tag;
    tag.SetPsn(psn);
    tag.SetIdx(idx);
    tag.SetWid(m_currentRoot);
    tag.SetSrcRank(m_currentRoot);
    tag.SetType(ACK);
    packet->AddPacketTag(tag);

    m_sendSocket->SendTo(packet, 0, m_peerAddrList[m_currentRoot]);
}

void EPIC3AllGatherReceiverApp::SendNakToRoot(uint32_t psn, uint32_t idx) {
    Ptr<Packet> packet = Create<Packet>(0);
    
    EPIC3AllGatherTag tag;
    tag.SetPsn(psn);
    tag.SetIdx(idx);
    tag.SetWid(m_currentRoot);
    tag.SetSrcRank(m_currentRoot);
    tag.SetType(NAK);
    packet->AddPacketTag(tag);

    m_sendSocket->SendTo(packet, 0, m_peerAddrList[m_currentRoot]);
}

void EPIC3AllGatherReceiverApp::SendDataToAllChildren(uint32_t idx) {
    BroadcastSlot& slot = m_bcastPool[idx];
    Ptr<Packet> pkt = slot.GetPkt();
    uint32_t psn = slot.GetPsn();
    
    auto children = GetChildWids();
    for (uint32_t childWid : children) {
        Ptr<Packet> copy = pkt->Copy();
        
        EPIC3AllGatherTag tag;
        copy->RemovePacketTag(tag);
        tag.SetPsn(psn);
        tag.SetIdx(idx);
        tag.SetWid(childWid);
        tag.SetSrcRank(m_currentRoot);
        tag.SetType(DATA);
        copy->AddPacketTag(tag);

        m_sendSocket->SendTo(copy, 0, m_peerAddrList[childWid]);
    }
}

void EPIC3AllGatherReceiverApp::SendDataToChild(uint32_t idx, uint32_t childWid) {
    BroadcastSlot& slot = m_bcastPool[idx];
    Ptr<Packet> pkt = slot.GetPkt();
    uint32_t psn = slot.GetPsn();
    
    Ptr<Packet> copy = pkt->Copy();
    
    EPIC3AllGatherTag tag;
    copy->RemovePacketTag(tag);
    tag.SetPsn(psn);
    tag.SetIdx(idx);
    tag.SetWid(childWid);
    tag.SetSrcRank(m_currentRoot);
    tag.SetType(DATA);
    copy->AddPacketTag(tag);

    m_sendSocket->SendTo(copy, 0, m_peerAddrList[childWid]);
}

void EPIC3AllGatherReceiverApp::OnBroadcastComplete(uint32_t idx) {
    CancelBroadcastTimer(idx);
    
    BroadcastSlot& slot = m_bcastPool[idx];
    slot.m_psn += m_poolSize;
    slot.Reset();
    
    m_completedPkts++;
    
    if (m_completedPkts == m_pktNum) {
        OnRoundComplete();
    }
}

void EPIC3AllGatherReceiverApp::OnRoundComplete() {
    /*NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Switch: broadcast round " 
                << m_currentRoot << " completed!");*/
    
    m_completedPkts = 0;
    for (uint32_t i = 0; i < m_poolSize; i++) {
        m_bcastPool[i].m_psn = i;
        m_bcastPool[i].Reset();
    }
    
    m_currentRoot++;
    
    if (m_currentRoot < m_rankNum) {
        /*NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Switch: 通知 Rank " 
                    << m_currentRoot << " start broadcast!");*/
        SendStartToNextRoot();
    } else {
        /*NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Switch: AllGather completed!");*/
    }
}

void EPIC3AllGatherReceiverApp::SendStartToNextRoot() {
    Ptr<Packet> packet = Create<Packet>(0);
    
    EPIC3AllGatherTag tag;
    tag.SetPsn(0);
    tag.SetIdx(0);
    tag.SetWid(m_currentRoot);
    tag.SetSrcRank(m_currentRoot);
    tag.SetType(START);
    packet->AddPacketTag(tag);
    
    for (uint32_t i = 0; i < m_rankNum; i++) {
        Ptr<Packet> copy = packet->Copy();
        m_sendSocket->SendTo(copy, 0, m_peerAddrList[i]);
    }
    
    /*NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Switch: 通知 Rank " 
                << m_currentRoot << " 开始广播");*/
}

void EPIC3AllGatherReceiverApp::RetransmitToChildren(uint32_t idx) {
    BroadcastSlot& slot = m_bcastPool[idx];
    if (!slot.HasData()) {
        return;
    }
    
    auto unacked = slot.GetUnackedChildren();
    auto children = GetChildWids();
    
    for (uint32_t childIdx : unacked) {
        SendDataToChild(idx, children[childIdx]);
    }
    
    EventId timerId = Simulator::Schedule(m_bcastTimeout,
                                          &EPIC3AllGatherReceiverApp::RetransmitToChildren,
                                          this, idx);
    m_bcastTimers[idx] = timerId;
}

void EPIC3AllGatherReceiverApp::CancelBroadcastTimer(uint32_t idx) {
    auto it = m_bcastTimers.find(idx);
    if (it != m_bcastTimers.end()) {
        Simulator::Cancel(it->second);
        m_bcastTimers.erase(it);
    }
}

// ==================== Main ====================
int main(int argc, char *argv[]) {
    LogComponentEnable("EPIC3AllGatherTest", LOG_LEVEL_INFO);

    uint32_t numWorkers = 8;
    uint16_t port = 9999;
    uint64_t dataSize = 1024 * 1024;
    uint32_t payloadSize = 1400;
    uint32_t windowSize = 512;
    uint32_t pktNum = dataSize / payloadSize;

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
    p2p.SetQueue("ns3::DropTailQueue<Packet>", "MaxSize", StringValue("5000000p"));

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

    Ptr<EPIC3AllGatherReceiverApp> receiverApp = CreateObject<EPIC3AllGatherReceiverApp>();
    receiverApp->m_port = port;
    receiverApp->InitComm(peerAddrList, windowSize, pktNum);

    switchNode->AddApplication(receiverApp);
    receiverApp->SetStartTime(Seconds(1.0));
    receiverApp->SetStopTime(Seconds(30.0));

    for (uint32_t i = 0; i < numWorkers; i++) {
        Ptr<EPIC3AllGatherSenderApp> senderApp = CreateObject<EPIC3AllGatherSenderApp>();

        senderApp->m_rankId = i;
        senderApp->m_port = port;
        senderApp->m_peerAddr = InetSocketAddress(switchAddr, port);
        senderApp->InitComm(dataSize, payloadSize, windowSize, numWorkers);

        workerNodes.Get(i)->AddApplication(senderApp);
        senderApp->SetStartTime(Seconds(1.0));
        senderApp->SetStopTime(Seconds(30.0));
    }

    NS_LOG_INFO("========== EPIC3 AllGather Start ==========");

    Simulator::Run();

    NS_LOG_INFO("========== EPIC3 AllGather End ==========");

    Simulator::Destroy();

    return 0;
}

#endif
