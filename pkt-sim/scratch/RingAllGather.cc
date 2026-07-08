#ifndef RING_ALL_GATHER_H
#define RING_ALL_GATHER_H

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

NS_LOG_COMPONENT_DEFINE("RingAllGatherTest");

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
        uint64_t GetChunkRankIdList() const { return m_chunkRankIdList; }
        
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
            os << "chunk id=" << m_chunkId << ", collected rank id list=" 
            << m_chunkRankIdList << ")";
        }

    private:
        uint32_t m_chunkId;
        uint64_t m_chunkRankIdList;
};
        

class RingAllGatherApp : public Application {

public:
    RingAllGatherApp();
    ~RingAllGatherApp(){};

    void StartApplication() override;
    void StopApplication() override;

    void InitComm(std::vector<uint32_t> nodeIdList, uint32_t myRankId, 
                                        uint32_t nextRankId, Address nextRankAddr, uint16_t port, 
                                        uint64_t dataSize, uint32_t payloadSize);

    void SendChunk(uint32_t chunkId);
    void RecvChunk(Ptr<Socket> socket);
    void GatherChunk(uint32_t chunkId);

    void StartAllGather();
    void PrintChunkList();

    std::vector<uint32_t> m_nodeIdList = {};
    std::vector<uint32_t> m_rankIdList = {};
    uint32_t m_dataSize = 0;
    uint32_t k = 0;
    uint32_t m_rankNum = 0;
    uint32_t m_chunkNum = 0;
    uint64_t m_chunkSize = 0;
    std::map<uint32_t, RingChunk> m_chunkList = {};
    uint32_t m_nodeId = 0;
    uint32_t m_myRankId = 0;
    uint32_t m_nextRankId = 0;
    uint32_t m_pktPayLoadSize = 0;


    uint32_t m_step = 0;
    uint64_t m_curChunkRecvBytes = 0;
    uint64_t m_curChunkRecvPktNum = 0;
    
    Ptr<Socket> m_sendSocket;
    Ptr<Socket> m_recvSocket;
    Address m_nextRankAddr;
    uint16_t m_port;
};

RingAllGatherApp::RingAllGatherApp()
{
    NS_LOG_FUNCTION(this);
}

void RingAllGatherApp::StartApplication()
{
    NS_LOG_FUNCTION(this);
    
    m_recvSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_recvSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_recvSocket->Bind(InetSocketAddress(Ipv4Address::GetAny(), m_port));
    m_recvSocket->SetRecvCallback(MakeCallback(&RingAllGatherApp::RecvChunk, this));
    
    m_sendSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_sendSocket->Bind();
    m_sendSocket->Connect(m_nextRankAddr);
    
    Simulator::Schedule(MicroSeconds(1), &RingAllGatherApp::StartAllGather, this);
}

void RingAllGatherApp::StartAllGather()
{
    NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_myRankId << ": start AllGather!");
    m_step = 1;
    SendChunk(m_myRankId);
}

void RingAllGatherApp::StopApplication()
{
    NS_LOG_FUNCTION(this);
    
    //PrintChunkList();
    
    if (m_sendSocket) m_sendSocket->Close();
    if (m_recvSocket) m_recvSocket->Close();
}

void RingAllGatherApp::PrintChunkList()
{
    NS_LOG_INFO("========== Rank " << m_myRankId << " ChunkList (size=" << m_chunkList.size() << ") ==========");
    for (const auto& pair : m_chunkList) {
        const RingChunk& chunk = pair.second;
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

void RingAllGatherApp::InitComm(std::vector<uint32_t> nodeIdList, uint32_t myRankId,
                                uint32_t nextRankId, Address nextRankAddr, uint16_t port, 
                                uint64_t dataSize, uint32_t pktPayLoadSize){
    m_nodeIdList = nodeIdList;
    m_myRankId = myRankId;
    m_nodeId = m_nodeIdList[m_myRankId];
    m_dataSize = dataSize;

    k = m_nodeIdList.size();
    m_rankNum = k;
    m_chunkNum = k;
    m_chunkSize = m_dataSize / k;

    m_nextRankId = nextRankId;
    m_pktPayLoadSize = pktPayLoadSize;

    m_nextRankAddr = nextRankAddr;
    m_port = port;

    RingChunk chunk;
    chunk.chunkId = m_myRankId;
    chunk.chunkSize = m_chunkSize;
    m_chunkList[m_myRankId] = chunk;
}

void RingAllGatherApp::SendChunk(uint32_t chunkId)
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

void RingAllGatherApp::RecvChunk(Ptr<Socket> socket)
{
    NS_LOG_FUNCTION(this);

    Ptr<Packet> packet;
    while ((packet = socket->Recv()))
    {
        if (packet->GetSize() == 0)
        {
            break;
        }
        
        m_curChunkRecvBytes += packet->GetSize();
        
        RingTag tag;
        packet->PeekPacketTag(tag);

        uint32_t chunkId = tag.GetChunkId();
        m_curChunkRecvPktNum++;
        
        if (m_curChunkRecvBytes >= m_chunkSize) {
            m_step++;
            if (m_step >= 2 && m_step <= k - 1) {
                GatherChunk(chunkId);
                Simulator::Schedule(MicroSeconds(1), &RingAllGatherApp::SendChunk, this, chunkId);
                m_curChunkRecvBytes = 0;
                m_curChunkRecvPktNum = 0;
                break;
            }
            else if (m_step == k) {
                GatherChunk(chunkId);
                NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_myRankId 
                            << ": AllGather complete!");
                m_curChunkRecvBytes = 0;
                m_curChunkRecvPktNum = 0;
                break;
            }
            break;
        }
    }
}

void RingAllGatherApp::GatherChunk(uint32_t chunkId)
{
    NS_LOG_FUNCTION(this);
    RingChunk c;
    c.chunkId = chunkId;
    c.chunkSize = m_chunkSize;
    m_chunkList[chunkId] = c;
}

int main(int argc, char *argv[])
{
    LogComponentEnable("RingAllGatherTest", LOG_LEVEL_INFO);

    uint32_t numRanks = 8;
    uint16_t port = 9999;
    uint64_t dataSize = 1024 * 1024;
    uint32_t payloadSize = 1400;
    
    CommandLine cmd(__FILE__);
    cmd.AddValue("dataSize", "total data size in bytes", dataSize);
    cmd.AddValue("numRanks", "number of ranks in the ring", numRanks);
    cmd.AddValue("payloadSize", "UDP payload size in bytes", payloadSize);
    cmd.Parse(argc, argv);

    NodeContainer nodes;
    nodes.Create(numRanks);
    
    InternetStackHelper internet;
    internet.Install(nodes);
    
    PointToPointHelper p2p;
    p2p.SetDeviceAttribute("DataRate", StringValue("100Gbps"));
    p2p.SetChannelAttribute("Delay", StringValue("1us"));
    p2p.SetQueue("ns3::DropTailQueue<Packet>", "MaxSize", StringValue("80000000p"));
    
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
    
    std::vector<uint32_t> nodeIdList(numRanks);
    for (uint32_t i = 0; i < numRanks; i++) nodeIdList[i] = i;
    
    for (uint32_t i = 0; i < numRanks; i++)
    {
        uint32_t myRankId = i;
        uint32_t nextRankId = (i + 1) % numRanks;
        
        Ptr<RingAllGatherApp> app = CreateObject<RingAllGatherApp>();
        
        app->InitComm(nodeIdList, myRankId, nextRankId, 
                      InetSocketAddress(nextRankAddrs[i], port), port,
                      dataSize, payloadSize);
 
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

#endif // RING_ALL_GATHER_H
