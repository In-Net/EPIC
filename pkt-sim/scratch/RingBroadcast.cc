#ifndef RING_BROADCAST_H
#define RING_BROADCAST_H

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

NS_LOG_COMPONENT_DEFINE("RingBroadcastTest");

class RingChunk{
    public:
        uint32_t chunkId;
        uint64_t chunkSize;
        uint64_t chunkRankIdList;
};

class RingTag : public Tag {
    public:
        RingTag() : m_chunkId(0), m_chunkRankIdList(0) {}
        
        void SetChunkId(uint64_t chunkId) { m_chunkId = chunkId; }
        uint32_t GetChunkId() const { return m_chunkId; }

        void SetChunkRankIdList(uint32_t rankIdList) { m_chunkRankIdList = rankIdList; }
        uint32_t GetChunkRankIdList() const { return m_chunkRankIdList; }
        
        static TypeId GetTypeId() {
            static TypeId tid = TypeId("ns3::RingTag")
                .SetParent<Tag>()
                .AddConstructor<RingTag>();
            return tid;
        }

        TypeId GetInstanceTypeId() const override { 
            return GetTypeId(); 
        }
    
        uint32_t GetSerializedSize() const override { 
            return sizeof(uint32_t) + sizeof(uint64_t); 
        }
        
        void Serialize(TagBuffer i) const override {
            i.WriteU32(m_chunkId);
            i.WriteU64(m_chunkRankIdList);
        }
        
        void Deserialize(TagBuffer i) override {
            m_chunkId = i.ReadU32();
            m_chunkRankIdList = i.ReadU64();
        }
        
        void Print(std::ostream& os) const override {
            os << "chunk id=" << m_chunkId << ", collected rank id list" 
            << m_chunkRankIdList << ")";
        }

    private:
        uint32_t m_chunkId;
        uint64_t m_chunkRankIdList;
};
        

class RingBroadcastApp : public Application {

public:
    RingBroadcastApp();
    ~RingBroadcastApp(){};

    void StartApplication() override;
    void StopApplication() override;

    void InitComm(std::vector<uint32_t> nodeIdList, uint32_t myRankId, 
                  uint32_t nextRankId, Address nextRankAddr, uint16_t port, 
                  uint64_t dataSize, uint32_t payloadSize, uint32_t rootRankId);

    void SendChunk(uint32_t chunkId);
    void RecvChunk(Ptr<Socket> socket);
    void AddChunk(RingChunk chunk);

    void StartBroadcast();
    void PrintChunkList();

    std::vector<uint32_t> m_nodeIdList = {};
    std::vector<uint32_t> m_rankIdList = {};
    uint32_t m_dataSize = 0;
    uint32_t k = 0;
    uint32_t m_rankNum = 0;
    uint32_t m_chunkNum = 0;      
    uint64_t m_chunkDefaultSize = 128 * 1024;
    std::map<uint32_t, RingChunk> m_chunkList = {};
    uint32_t m_nodeId = 0;
    uint32_t m_myRankId = 0;
    uint32_t m_nextRankId = 0;
    uint32_t m_rootRankId = 0;    
    uint32_t m_pktPayLoadSize = 0;
    std::map<uint32_t, uint32_t> m_chunkRecvPktSize;
    std::map<uint32_t, uint32_t> m_chunkSize;
    uint32_t m_completedChunkCount = 0;
    
    Ptr<Socket> m_sendSocket;
    Ptr<Socket> m_recvSocket;
    Address m_nextRankAddr;
    uint16_t m_port;
};

RingBroadcastApp::RingBroadcastApp()
{
    NS_LOG_FUNCTION(this);
}

void RingBroadcastApp::StartApplication()
{
    NS_LOG_FUNCTION(this);
    
    m_recvSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_recvSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_recvSocket->Bind(InetSocketAddress(Ipv4Address::GetAny(), m_port));
    m_recvSocket->SetRecvCallback(MakeCallback(&RingBroadcastApp::RecvChunk, this));
    
    m_sendSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_sendSocket->Bind();
    m_sendSocket->Connect(m_nextRankAddr);
    
    Simulator::Schedule(MicroSeconds(1), &RingBroadcastApp::StartBroadcast, this);
}

void RingBroadcastApp::StartBroadcast()
{
    if (m_myRankId == m_rootRankId) {
        NS_LOG_INFO("[" << Simulator::Now().GetSeconds() << "s] Rank " << m_myRankId 
                    << " is root rank, start Broadcast!");
        
        for (uint32_t i = 0; i < m_chunkNum; i++) {
            Simulator::Schedule(MicroSeconds(i+1), &RingBroadcastApp::SendChunk, this, i);
        }
    }
    else if (m_nextRankId == m_rootRankId) {
        NS_LOG_INFO("[" << Simulator::Now().GetSeconds() << "s] Rank " << m_myRankId 
                    << " wait for receiving data");
    }
    else {
        NS_LOG_INFO("[" << Simulator::Now().GetSeconds() << "s] Rank " << m_myRankId 
                    << " wait for receiving data!");
    }
}

void RingBroadcastApp::StopApplication()
{
    NS_LOG_FUNCTION(this);
    
    //PrintChunkList();
    
    if (m_sendSocket) m_sendSocket->Close();
    if (m_recvSocket) m_recvSocket->Close();
}

void RingBroadcastApp::PrintChunkList()
{
    NS_LOG_INFO("========== Rank " << m_myRankId << " ChunkList ==========");
    for (uint32_t i = 0; i < m_chunkList.size(); i++) {
        const RingChunk& chunk = m_chunkList[i];
        std::string rankBits = "";
        for (uint32_t r = 0; r < k; r++) {
            if (chunk.chunkRankIdList & (1ULL << r)) {
                rankBits += std::to_string(r) + " ";
            }
        }
        NS_LOG_INFO("  Chunk " << chunk.chunkId 
                    << ": size=" << chunk.chunkSize 
                    << ", collected ranks=[" << rankBits << "]"
                    << ", bitmap=0x" << std::hex << chunk.chunkRankIdList << std::dec);
    }
    NS_LOG_INFO("==========================================");
}

void RingBroadcastApp::InitComm(std::vector<uint32_t> nodeIdList, uint32_t myRankId,
                             uint32_t nextRankId, Address nextRankAddr, uint16_t port, 
                             uint64_t dataSize, uint32_t pktPayLoadSize, uint32_t rootRankId){
    m_nodeIdList = nodeIdList;
    m_myRankId = myRankId;
    m_nodeId = m_nodeIdList[m_myRankId];
    m_dataSize = dataSize;

    k = m_nodeIdList.size();
    m_rankNum = k;

    m_nextRankId = nextRankId;
    m_rootRankId = rootRankId;
    m_pktPayLoadSize = pktPayLoadSize;

    m_nextRankAddr = nextRankAddr;
    m_port = port;

    uint32_t totSize = 0;
    uint32_t id = 0;

    
    while (totSize < dataSize) {
        RingChunk c;
        c.chunkId = id;
        c.chunkRankIdList = 1ULL << m_myRankId;
        c.chunkSize = std::min(m_chunkDefaultSize, dataSize - totSize);
        m_chunkNum++;
        if (m_myRankId == m_rootRankId) {
            m_chunkList[id] = c;
        }
        m_chunkRecvPktSize[id] = 0;
        m_chunkSize[id] = c.chunkSize;
        id++;
        totSize += c.chunkSize;
    }
}

void RingBroadcastApp::SendChunk(uint32_t chunkId)
{
    NS_LOG_FUNCTION(this);
    RingChunk chunk = m_chunkList[chunkId];
    uint64_t maxBytes = chunk.chunkSize;
    uint64_t totBytes = 0;

    while (totBytes < maxBytes)
    {
        uint64_t toSend = m_pktPayLoadSize;
        if (toSend > (maxBytes - totBytes)) 
            toSend = maxBytes - totBytes;

        Ptr<Packet> packet = Create<Packet>(toSend);
        RingTag tag;
        tag.SetChunkId(chunk.chunkId);
        tag.SetChunkRankIdList(chunk.chunkRankIdList);
        packet->AddPacketTag(tag);

        int actual = m_sendSocket->Send(packet);
        if (actual > 0)
        {
            totBytes += toSend;
        }
        else
        {
            NS_LOG_DEBUG("Rank=" << m_myRankId << " UDP send failed, ret=" << actual);
            break;
        }
    }
}

void RingBroadcastApp::RecvChunk(Ptr<Socket> socket)
{
    NS_LOG_FUNCTION(this);

    Ptr<Packet> packet;
    while ((packet = socket->Recv()))
    {
        if (packet->GetSize() == 0)
        {
            break;
        }
        
        RingTag tag;
        packet->PeekPacketTag(tag);
        uint32_t chunkId = tag.GetChunkId();
        m_chunkRecvPktSize[chunkId] += packet->GetSize();
        
        if (m_chunkRecvPktSize[chunkId] >= m_chunkSize[chunkId]) {
            
            uint64_t chunkRankIdList = tag.GetChunkRankIdList();

            RingChunk c;
            c.chunkId = chunkId;
            c.chunkRankIdList = chunkRankIdList;
            c.chunkSize = m_chunkRecvPktSize[chunkId];
            
            if (m_nextRankId == m_rootRankId) {
                AddChunk(c);
                m_completedChunkCount++;
                if (m_completedChunkCount == m_chunkNum) {
                    NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us]" << " Broadcast complete!");
                }

                m_chunkRecvPktSize[chunkId] = 0;
                break;
            }
            else {
                AddChunk(c);
                Simulator::ScheduleNow(&RingBroadcastApp::SendChunk, this, chunkId);
                m_chunkRecvPktSize[chunkId] = 0;
                break;
            }
            break;
        }
    }
}

void RingBroadcastApp::AddChunk(RingChunk chunk) {
    m_chunkList[chunk.chunkId] = chunk;
}

int main(int argc, char *argv[])
{
    LogComponentEnable("RingBroadcastTest", LOG_LEVEL_INFO);
    
    uint32_t numRanks = 8;
    uint16_t port = 9999;
    uint64_t dataSize = 1024 * 1024;
    uint32_t payloadSize = 1400;
    uint32_t rootRank = 0;    
    
    NodeContainer nodes;
    nodes.Create(numRanks);
    
    InternetStackHelper internet;
    internet.Install(nodes);
    
    // Ring: 0->1, 1->2, 2->3, 3->0
    PointToPointHelper p2p;
    p2p.SetDeviceAttribute("DataRate", StringValue("100Gbps"));
    p2p.SetChannelAttribute("Delay", StringValue("1us"));
    p2p.SetQueue("ns3::DropTailQueue<Packet>", "MaxSize", StringValue("5000000p"));
    
    Ipv4AddressHelper address;
    
    std::vector<Ipv4Address> nextRankAddrs(numRanks);
    
    for (uint32_t i = 0; i < numRanks; i++)
    {
        uint32_t nextRank = (i + 1) % numRanks;
        
        NetDeviceContainer devices = p2p.Install(nodes.Get(i), nodes.Get(nextRank));
        
        std::ostringstream subnet;
        subnet << "10.0." << i << ".0";
        address.SetBase(subnet.str().c_str(), "255.255.255.0");
        Ipv4InterfaceContainer interfaces = address.Assign(devices);
        
        nextRankAddrs[i] = interfaces.GetAddress(1);
    }
    
    Ipv4GlobalRoutingHelper::PopulateRoutingTables();
    
    std::vector<uint32_t> nodeIdList = {0, 1, 2, 3, 4, 5, 6, 7};

    for (uint32_t i = 0; i < numRanks; i++)
    {
        uint32_t myRankId = i;
        uint32_t nextRankId = (i + 1) % numRanks;
        
        Ptr<RingBroadcastApp> app = CreateObject<RingBroadcastApp>();
        
        app->InitComm(nodeIdList, myRankId, nextRankId, 
                      InetSocketAddress(nextRankAddrs[i], port), port,
                      dataSize, payloadSize, rootRank);
 
        nodes.Get(i)->AddApplication(app);
        app->SetStartTime(Seconds(1.0));
        app->SetStopTime(Seconds(10.0));
    }
    
    NS_LOG_INFO("========== simulation start ==========");
    
    Simulator::Run();
    Simulator::Destroy();
    
    NS_LOG_INFO("========== simulation end ==========");
    
    return 0;
}

#endif // RING_BROADCAST_H
