#ifndef RING_REDUCE_H
#define RING_REDUCE_H

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

NS_LOG_COMPONENT_DEFINE("RingReduceTest");

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
        

class RingReduceApp : public Application {

public:
    RingReduceApp();
    ~RingReduceApp(){};

    void StartApplication() override;
    void StopApplication() override;

    void InitComm(std::vector<uint32_t> nodeIdList, uint32_t myRankId, 
                  uint32_t nextRankId, Address nextRankAddr, uint16_t port, 
                  uint64_t dataSize, uint32_t payloadSize, uint32_t rootRankId);

    void SendChunk(uint32_t chunkId);
    void RecvChunk(Ptr<Socket> socket);
    void ReduceChunk(uint32_t chunkId, uint64_t chunkRankIdList);

    void ReduceSendChunk(uint32_t chunkId, uint64_t chunkRankIdList);

    void StartReduce();
    void PrintChunkList();

    std::vector<uint32_t> m_nodeIdList = {};
    std::vector<uint32_t> m_rankIdList = {};
    uint32_t m_dataSize = 0;
    uint32_t k = 0;
    uint32_t m_rankNum = 0;
    uint32_t m_chunkNum = 0;
    uint64_t m_chunkDefaultSize = 128 * 1024;
    std::vector<RingChunk> m_chunkList = {};
    uint32_t m_nodeId = 0;
    uint32_t m_myRankId = 0;
    uint32_t m_nextRankId = 0;
    uint32_t m_rootRankId = 0;
    uint32_t m_pktPayLoadSize = 0;
    std::map<uint32_t, uint32_t> m_chunkRecvPktSize;
    uint32_t m_completedChunkCount = 0;

    uint32_t m_step = 0;
    
    Ptr<Socket> m_sendSocket;
    Ptr<Socket> m_recvSocket;
    Address m_nextRankAddr;
    uint16_t m_port;
};

RingReduceApp::RingReduceApp()
{
    NS_LOG_FUNCTION(this);
}

void RingReduceApp::StartApplication()
{
    NS_LOG_FUNCTION(this);
    
    m_recvSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_recvSocket->SetAttribute("RcvBufSize", UintegerValue(1024 * 1024 * 1024));
    m_recvSocket->Bind(InetSocketAddress(Ipv4Address::GetAny(), m_port));
    m_recvSocket->SetRecvCallback(MakeCallback(&RingReduceApp::RecvChunk, this));
    
    m_sendSocket = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_sendSocket->Bind();
    m_sendSocket->Connect(m_nextRankAddr);
    
    Simulator::Schedule(MicroSeconds(1), &RingReduceApp::StartReduce, this);
}

void RingReduceApp::StartReduce()
{
    m_step = 1;
    
    uint32_t startRank = (m_rootRankId + 1) % k;
    
    if (m_myRankId == startRank) {
        NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_myRankId 
                    << " start Reduce!");
        for (uint32_t i = 0; i < m_chunkNum; i++) {
            Simulator::Schedule(MicroSeconds(i+1), &RingReduceApp::SendChunk, this, i);
        }
    }
    else if (m_myRankId == m_rootRankId) {
        NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_myRankId 
                    << " is root rank, waiting for receiving reduce result");
        
    }
    else { 
        NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_myRankId 
                    << " start Reduce!");
    }
}

void RingReduceApp::StopApplication()
{
    NS_LOG_FUNCTION(this);
    
    if (m_sendSocket) m_sendSocket->Close();
    if (m_recvSocket) m_recvSocket->Close();
}

void RingReduceApp::PrintChunkList()
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

void RingReduceApp::InitComm(std::vector<uint32_t> nodeIdList, uint32_t myRankId,
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
        m_chunkList.push_back(c);
        m_chunkRecvPktSize[id] = 0;
        id++;
        totSize += c.chunkSize;
        m_chunkNum++;
    }
}

void RingReduceApp::SendChunk(uint32_t chunkId)
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

void RingReduceApp::RecvChunk(Ptr<Socket> socket)
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

        if (m_chunkRecvPktSize[chunkId] == m_chunkList[chunkId].chunkSize) {
            
            uint64_t chunkRankIdList = tag.GetChunkRankIdList();
            
            m_step++;
            
            if (m_myRankId == m_rootRankId) {
                ReduceChunk(chunkId, chunkRankIdList);

                m_completedChunkCount++;

                if (m_completedChunkCount >= m_chunkNum) {
                    NS_LOG_INFO("[" << Simulator::Now().GetMicroSeconds() << "us] Rank " << m_myRankId 
                                << " is root rank, all data received, total " << m_chunkNum << " chunks");
                }

                m_chunkRecvPktSize[chunkId] = 0;
                break;
            }
            else {
                Simulator::Schedule(MicroSeconds(1), &RingReduceApp::ReduceSendChunk, this, chunkId, chunkRankIdList);
                m_chunkRecvPktSize[chunkId] = 0;
                break;
            }
            break;
        }
    }
}

void RingReduceApp::ReduceChunk(uint32_t chunkId, uint64_t chunkRankIdList)
{
    NS_LOG_FUNCTION(this);
    m_chunkList[chunkId].chunkRankIdList |= chunkRankIdList;
}

void RingReduceApp::ReduceSendChunk(uint32_t chunkId, uint64_t chunkRankIdList)
{
    NS_LOG_FUNCTION(this);
    ReduceChunk(chunkId, chunkRankIdList);
    SendChunk(chunkId);
}

int main(int argc, char *argv[])
{
    LogComponentEnable("RingReduceTest", LOG_LEVEL_INFO);
    
    // ========== 配置参数 ==========
    uint32_t numRanks = 8;
    uint16_t port = 9999;
    uint64_t dataSize = 1024 * 1024;
    uint32_t payloadSize = 1400;
    uint32_t rootRank = 7;
    
    CommandLine cmd(__FILE__);
    cmd.AddValue("dataSize", "total data size in bytes", dataSize);
    cmd.AddValue("numRanks", "number of ranks in the ring", numRanks);
    cmd.AddValue("payloadSize", "UDP payload size in bytes", payloadSize);
    cmd.AddValue("rootRank", "root rank id for Reduce", rootRank);
    cmd.Parse(argc, argv);

    NodeContainer nodes;
    nodes.Create(numRanks);
    
    InternetStackHelper internet;
    internet.Install(nodes);
    
    PointToPointHelper p2p;
    p2p.SetDeviceAttribute("DataRate", StringValue("100Gbps"));
    p2p.SetChannelAttribute("Delay", StringValue("1us"));
    p2p.SetQueue("ns3::DropTailQueue<Packet>", "MaxSize", StringValue("10000000p"));
    
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
    
    NS_LOG_INFO("Root Rank: " << rootRank);
    
    for (uint32_t i = 0; i < numRanks; i++)
    {
        uint32_t myRankId = i;
        uint32_t nextRankId = (i + 1) % numRanks;
        
        Ptr<RingReduceApp> app = CreateObject<RingReduceApp>();
        
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

#endif // RING_REDUCE_H
