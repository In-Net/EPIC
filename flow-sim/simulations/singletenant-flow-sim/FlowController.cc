#include <omnetpp.h>
#include <vector>
#include <limits>
#include <algorithm>
#include <cmath>
#include "FlowMsg.h"
using namespace omnetpp;
using std::vector;

// 每条活跃流的状态
struct FlowEntry {//all reduce
    int        flowId;
    int        groupId;
    int        srcHost;      // 来自哪个任务
    double     remBits;
    double     rate;         // 分配带宽
    vector<int> edges;        // 链路集合
    vector<int> nodes;        // 节点集合
    cMessage*  endEvt;       // scheduled end event
};

class FlowController : public cSimpleModule {
  private:
    int Mode;
    int job_cnt;
    //Mode=1:EDT
    //Mode=2:Spatial Multiplexing
    //Mode=3:Temporal Multiplexing

    double linkCapacity; // 每条链路的容量，假设全网相同
    int TP;
    int DP;
    int PP;
    simtime_t      lastTime;
    vector<FlowEntry> flows;
    int cnt=0;//for debug
    int switch_limit;//Mode=2/3时有效，每台交换机可以容纳的聚合组上限
    
    //决定胖树的拓扑结构
    int leaf_server_degree;
    int spine_leaf_degree;
    int spine_core_degree;
    int pod_cnt;
    
    // 每层节点的数量
    int server_cnt;
    int leaf_cnt;
    int spine_cnt;
    int core_cnt;

    // 每层边的数量
    int edge_server_leaf_cnt; // server-leaf
    int edge_leaf_spine_cnt; // leaf-spine
    int edge_spine_core_cnt; // spine-core
    int edge_cnt;

    //空间复用模式下，由交换机处理能力和通信组大小决定
    vector<int> switch_used; // Mode=2/3时，标记占用交换机的数量
    vector<bool> edge_used; // EDT模式时，每条链路只能位于一棵INC树
    vector<int> useINC;//Mode=1/2时，静态模式，记录每个group是分配INC资源，-1表示未分配

    void init() {
        lastTime = simTime();
        Mode = getParentModule()->par("Mode").intValue();
        if(Mode!=0 && Mode!=1 && Mode!=2 && Mode!=3) error("Invalid Mode");
        job_cnt = getParentModule()->par("job_cnt").intValue();
        leaf_server_degree = getParentModule()->par("leaf_server_degree").intValue();
        spine_leaf_degree = getParentModule()->par("spine_leaf_degree").intValue();
        spine_core_degree = getParentModule()->par("spine_core_degree").intValue();
        pod_cnt = getParentModule()->par("pod_cnt").intValue();
        switch_limit = getParentModule()->par("switch_limit").intValue();
        TP = getParentModule()->par("TP").intValue();
        DP = getParentModule()->par("DP").intValue();   
        PP = getParentModule()->par("PP").intValue();
        linkCapacity = getParentModule()->par("linkCapacity").doubleValue();

        leaf_cnt  = pod_cnt * spine_leaf_degree;
        spine_cnt = pod_cnt * spine_leaf_degree;
        server_cnt = leaf_cnt * leaf_server_degree;
        core_cnt = spine_leaf_degree * spine_core_degree;

        edge_server_leaf_cnt = server_cnt;
        edge_leaf_spine_cnt = pod_cnt * spine_leaf_degree * spine_leaf_degree;
        edge_spine_core_cnt = pod_cnt * spine_leaf_degree * spine_core_degree; // == pod_cnt * core_cnt

        edge_cnt = edge_server_leaf_cnt + edge_leaf_spine_cnt + edge_spine_core_cnt;

        if(job_cnt*TP*PP*DP > server_cnt){
            EV << "Not enough servers: need " << job_cnt*TP*PP*DP << ", have " << server_cnt << "\n";
            //error("Not enough servers for all jobs");
        }

        if(Mode==0);//不考虑交换机资源限制
        else if(Mode==1){//EDT模式下，每条链路只能用于一个INC通信
            edge_used.resize(edge_cnt,false);
            useINC.resize(job_cnt*(TP*PP+DP*PP),-1);
        }
        else if(Mode==2){
            //空间复用模式下，交换机处理能力决定limit
            switch_used.resize(leaf_cnt + spine_cnt + core_cnt, 0);
            useINC.resize(job_cnt*(TP*PP+DP*PP),-1);
        }
        else if(Mode==3){
            //时间复用模式下，交换机处理能力在灌水时考虑
            switch_used.resize(leaf_cnt + spine_cnt + core_cnt, 0);
            useINC.resize(job_cnt*(TP*PP+DP*PP),-1);
        }
        else error("Invalid Mode");
    }

    // ---------- 节点/辅助函数 ----------

    int servers_per_pod() const {
        return spine_leaf_degree * leaf_server_degree;
    }

    // decode server → (pod, leaf_idx, slot)
    void decode_server(int server_id, int &pod, int &leaf_idx, int &slot) const {
        int spp = servers_per_pod();
        pod = server_id / spp;
        int off = server_id % spp;
        leaf_idx = off / leaf_server_degree;
        slot = off % leaf_server_degree;
    }

    //server node id needn't convert

    int leaf_node(int pod, int leaf_idx) const {
        return server_cnt + pod * spine_leaf_degree + leaf_idx;
    }

    int spine_node(int pod, int spine_idx) const {
        return server_cnt + leaf_cnt + pod * spine_leaf_degree + spine_idx;
    }

    int core_node(int core_idx) const {
        return server_cnt + leaf_cnt + spine_cnt + core_idx;
    }

    // ---------- 边编号工具 ----------

    int edge_id_server_leaf(int server_id) const {
        // 每个 server 一个下行端口，只连一个 leaf
        return server_id; // 0..server_cnt-1
    }

    int edge_id_leaf_spine(int pod, int leaf_idx, int spine_idx) const {
        int E_sl = edge_server_leaf_cnt;
        int per_pod = spine_leaf_degree * spine_leaf_degree;
        return E_sl + pod * per_pod + leaf_idx * spine_leaf_degree + spine_idx;
    }

    int edge_id_spine_core(int pod, int spine_idx, int core_idx) const {
        int E_sl = edge_server_leaf_cnt;
        int E_ls = edge_leaf_spine_cnt;
        int j = core_idx % spine_core_degree;
        return E_sl + E_ls + (pod * spine_leaf_degree + spine_idx) * spine_core_degree + j;
    }

    int get_root_core_idx(int groupId) const {//不管什么模式，root_core_idx都只由groupId决定
        int spine_idx = groupId % spine_leaf_degree;
        int core_idx = (groupId / spine_leaf_degree) % spine_core_degree;
        int root_core_idx = spine_idx * spine_core_degree + core_idx; 
        return root_core_idx;
    }

    

    // ---------- 路由函数 ----------
    // 输入: 两个 server 的编号 (0..server_cnt-1)
    // 输出: 经过的节点序列 nodes（不包括server，因为不需要考虑server的内存限制）、边编号序列 edges（按顺序）
    // 有向： u -> v
    // root_core_idx: 用于跨 pod 路由时选择的 core，范围 0..core_cnt-1
    // 把边当成无向边，因为上行和下行流量都是需要的
    void route(int server_u, int server_v,int root_core_idx,
               std::vector<int> &nodes,
               std::vector<int> &edges) const
    {
        nodes.clear();
        edges.clear();

        if (server_u == server_v) {
            error("route: server_u and server_v are the same");
        }

        int pu, lu, su_slot;
        int pv, lv, sv_slot;
        decode_server(server_u, pu, lu, su_slot);
        decode_server(server_v, pv, lv, sv_slot);

        int leaf_u = leaf_node(pu, lu);
        int leaf_v = leaf_node(pv, lv);

        // case 1: 同 leaf
        if (pu == pv && lu == lv) {
            nodes = { leaf_u };

            edges.push_back(edge_id_server_leaf(server_u));
            edges.push_back(edge_id_server_leaf(server_v));
            return;
        }

        // case 2: 同 pod，不同 leaf
        if (pu == pv) {
            int pod = pu;
            // 选择一个 spine，为了便于调试，暂时采用简单的 hash
            int spine_idx = root_core_idx / spine_core_degree; // 0..spine_leaf_degree-1
            int spine = spine_node(pod, spine_idx);

            nodes = {
                leaf_u,
                spine,
                leaf_v
            };

            // S_u - L_u
            edges.push_back(edge_id_server_leaf(server_u));
            // L_u - Spine
            edges.push_back(edge_id_leaf_spine(pod, lu, spine_idx));
            // Spine - L_v
            edges.push_back(edge_id_leaf_spine(pod, lv, spine_idx));
            // L_v - S_v
            edges.push_back(edge_id_server_leaf(server_v));

            return;
        }

        // case 3: 跨 pod，需要经过 Spine & Core
        
        int core_idx = root_core_idx;

        int spine_idx = core_idx / spine_core_degree; // 0..spine_leaf_degree-1

        int spine_u = spine_node(pu, spine_idx);
        int spine_v = spine_node(pv, spine_idx);
        int core    = core_node(core_idx);

        nodes = {
            leaf_u,
            spine_u,
            core,
            spine_v,
            leaf_v
        };

        // S_u - L_u
        edges.push_back(edge_id_server_leaf(server_u));
        // L_u - Spine_u
        edges.push_back(edge_id_leaf_spine(pu, lu, spine_idx));
        // Spine_u - Core
        edges.push_back(edge_id_spine_core(pu, spine_idx, core_idx));
        // Core - Spine_v
        edges.push_back(edge_id_spine_core(pv, spine_idx, core_idx));
        // Spine_v - L_v
        edges.push_back(edge_id_leaf_spine(pv, lv, spine_idx));
        // L_v - S_v
        edges.push_back(edge_id_server_leaf(server_v));

        return;
    }

    //给定all reduce的服务器列表，计算经过的节点和边
    void All_reduce_route(std::vector<int> servers,std::vector<int> &nodes,
               std::vector<int> &edges,int root_core_idx=-1) const
    {
        nodes.clear();
        edges.clear();
        if(root_core_idx==-1){//如果不指定根，哈希随便取一个
            root_core_idx = 0;
            for(auto n:servers){
                root_core_idx += n;
            }
            root_core_idx = root_core_idx % core_cnt;
        }
        
        for(int i=1;i<(int)servers.size();i++){
            std::vector<int> tmp_nodes;
            std::vector<int> tmp_edges;
            route(servers[0],servers[i],root_core_idx,tmp_nodes,tmp_edges);
            
            for(auto n:tmp_nodes){
                nodes.push_back(n);
            }
            for(auto e:tmp_edges){
                edges.push_back(e);
            }
        }
        sort(edges.begin(),edges.end());
        edges.erase(unique(edges.begin(),edges.end()),edges.end());
        sort(nodes.begin(),nodes.end());
        nodes.erase(unique(nodes.begin(),nodes.end()),nodes.end());
        return;
    }

  protected:
    virtual void initialize() override {
        //EV << "FlowController initialized\n";
        init();
    }

    // 最初版灌水，不管什么模式都不考虑交换机处理能力限制
    void allocateRates() {
        //EV << "\n=== Allocating rates, event #" << cnt << " ===\n";
        
        simtime_t now = simTime();
        double dt = (now - lastTime).dbl();
        lastTime = now;
        int M = edge_cnt; // allreduce直接当无向边，不需要两倍
        
        
        
        // 1) 扣掉已服务 bits，cancel 旧的 endEvt
        for (auto &e : flows) {
            e.remBits = std::max(0.0, e.remBits - e.rate*dt);
            if (e.endEvt->isScheduled())
                cancelEvent(e.endEvt);
        }

        // 2) water‐filling
        vector<double> cap(M, linkCapacity); 
        
        vector<int> U; U.reserve(flows.size());
        for (int i=0; i<(int)flows.size(); i++) U.push_back(i);
        
        //int iteration = 0;
        while (!U.empty()) {
            vector<int> p(M,0);
            for (int idx:U)
                for (int l:flows[idx].edges)
                    p[l]++;
            double alpha = std::numeric_limits<double>::infinity();
            int b=-1;
            for (int l=0; l<M; l++) if (p[l]>0) {
                double a = cap[l]/p[l];
                if (a<alpha) { alpha=a; b=l; }
            }
            vector<int> assigned;
            for (int idx:U) {
                if (std::find(flows[idx].edges.begin(),
                              flows[idx].edges.end(), b)
                    != flows[idx].edges.end())
                {
                    flows[idx].rate = alpha;
                    for (int l:flows[idx].edges)
                        cap[l] -= alpha;
                    assigned.push_back(idx);
                }
            }
            // remove from U
            vector<int> U2;
            for (int idx:U)
                if (std::find(assigned.begin(),assigned.end(),idx)
                    ==assigned.end())
                    U2.push_back(idx);
            U.swap(U2);
        }

        // 3) schedule new end events
        for (auto &e:flows) {
            simtime_t dtfin = (e.rate>0 ? e.remBits/e.rate : 0);
            scheduleAt(now + dtfin, e.endEvt);
        }
        
        /*
        EV << "After allocation at time " << now << ":\n";
        for (auto &e:flows) {
            EV << "  Flow " << e.flowId << " from host " << e.srcHost
               << ": rate=" << e.rate << " bps, remBits=" << e.remBits 
               << " bits, useINC=" << useINC[e.groupId] << "\n";
            for(auto ed:e.edges){
                EV << "    edge " << ed << "\n";
            }
            EV << "\n";
        }
        EV << "-------\n";
        */
        //EV << "=== Rate allocation complete ===\n";
    }


    virtual void handleMessage(cMessage *msg) override {
        ++cnt;//for debug
        //EV << "FC receive masssage cnt= " << cnt << "\n";
        if (auto *fs = dynamic_cast<FlowStart*>(msg)) {
            // new flow
            for(int server:fs->servers){
                if(server<0 || server>=server_cnt) error("Invalid server id in FlowStart");
            }
            FlowEntry e;
            e.flowId   = fs->flowId;
            e.groupId  = fs->groupId;
            e.srcHost  = fs->srcHost;
            e.remBits  = fs->bits;
            e.rate     = 0;
            
            //EV << "receive flow from " << fs->srcHost << "\n";
            if(Mode==0){
                int gid = fs->groupId;
                int root_core_idx = get_root_core_idx(gid);
                All_reduce_route(fs->servers,e.nodes,e.edges,root_core_idx);
                e.remBits = e.remBits*2;//不使用INC资源，近似等价于通信量翻倍
            }
            else if(Mode==1){//EDT模式
                int gid = fs->groupId;
                int root_core_idx = get_root_core_idx(gid);
                if(fs->bits<0){//预告知，尝试规划资源
                    if(useINC[gid]!=-1){//重复分配
                        error("Duplicate groupId in FlowStart with bits<0");
                    }
                    All_reduce_route(fs->servers,e.nodes,e.edges,root_core_idx);
                    bool flag=1;
                    for(auto e:e.edges){
                        if(edge_used[e]){
                            flag=0;
                            break;
                        }
                    }
                    if(flag){
                        for(auto e:e.edges){
                            edge_used[e]=true;
                        }
                        useINC[gid]=1;
                    }
                    else{
                        useINC[gid]=0;
                    }
                    
                    std::string scalarName = "useINC_gid_" + std::to_string(gid);
                    recordScalar(scalarName.c_str(), useINC[gid] == 1 ? 1 : 0);
                    delete fs;
                    return;
                }
                if(useINC[gid]==-1){
                    error("Unknown groupId in FlowStart with bits>0");
                }
                else if(useINC[gid]==0){
                    e.remBits = e.remBits * 2;//不使用INC资源，近似等价于通信量翻倍
                }
                All_reduce_route(fs->servers,e.nodes,e.edges,root_core_idx);
            }
            else if(Mode==2){
                int gid = fs->groupId;
                int root_core_idx = get_root_core_idx(gid);
                if(fs->bits<0){//预先告知通信组信息
                    if(useINC[gid]!=-1){//重复分配
                        error("Duplicate groupId in FlowStart with bits<0");
                    }
                    All_reduce_route(fs->servers,e.nodes,e.edges,root_core_idx);
                    bool flag=1;
                    for(auto n:e.nodes){
                        if(n>=server_cnt){
                            int offset = n - server_cnt;
                            if(switch_used[offset] > switch_limit){//应该不可能
                                error("Switch node in flow entry exceeds limit in Mode 2");
                            }
                            if(switch_used[offset] == switch_limit){//服务器资源已满
                                flag=0;
                                break;
                            }
                        }
                        else error("Server node in flow entry in Mode 2");
                    }
                    if(flag){
                        for(auto n:e.nodes){
                            int offset = n - server_cnt;
                            switch_used[offset]++;
                        }
                        useINC[gid]=1;
                    }
                    else{
                        useINC[gid]=0;
                    }
                    std::string scalarName = "useINC_gid_" + std::to_string(gid);
                    recordScalar(scalarName.c_str(), useINC[gid] == 1 ? 1 : 0);
                    delete fs;
                    return;
                }
                if(useINC[gid]==-1){
                    error("Unknown groupId in FlowStart with bits>0");
                }
                else if(useINC[gid]==0){
                    e.remBits = e.remBits * 2;//不使用INC资源，近似等价于通信量翻倍
                }
                All_reduce_route(fs->servers,e.nodes,e.edges,root_core_idx);
            }
            else if(Mode==3){//动态实时分配
                if(fs->bits<0){
                    error("Invalid bits<0 in FlowStart in Mode 3");
                }
                int gid = fs->groupId;
                int root_core_idx = get_root_core_idx(gid);
                All_reduce_route(fs->servers,e.nodes,e.edges,root_core_idx);
                bool flag=1;
                for(auto n:e.nodes){
                    if(n>=server_cnt){
                        int offset = n - server_cnt;
                        if(switch_used[offset] > switch_limit){//应该不可能
                            error("Switch node in flow entry exceeds limit in Mode 3");
                        }
                        if(switch_used[offset] == switch_limit){//服务器资源已满
                            flag=0;
                            break;
                        }
                    }
                    else error("Server node in flow entry in Mode 3");
                }
                if(flag){
                    for(auto n:e.nodes){
                        int offset = n - server_cnt;
                        switch_used[offset]++;
                    }
                    useINC[gid]=1;
                }
                else{
                    useINC[gid]=0;
                    e.remBits = e.remBits * 2;//不使用INC资源，近似等价于通信量翻倍
                }
                std::string scalarName = "useINC_gid_" + std::to_string(gid);
                recordScalar(scalarName.c_str(), useINC[gid] == 1 ? 1 : 0);
            }
            else error("Invalid Mode in FlowStart");

            
            e.endEvt   = new cMessage("FlowEndEvt");
            flows.push_back(e);
            delete fs;
            allocateRates();
            if(Mode!=0 && 0){
                recordScalar("---------------------active_flows:", simTime().dbl());
                for(auto &fe:flows){
                    std::string scalarName = "flow_rate_id_" + std::to_string(fe.flowId);
                    recordScalar(scalarName.c_str(), fe.rate);
                    recordScalar("  remBits:", fe.remBits);
                    recordScalar("  useINC:", useINC[fe.groupId]);
                    recordScalar("  root_core_idx:", get_root_core_idx(fe.groupId));
                    /*for(auto sw:fe.nodes){
                        if(sw>=server_cnt){
                            recordScalar("  server:", sw);
                        }
                    }*/
                    for(auto ed:fe.edges){
                        recordScalar("    edge:", ed);
                    }
                    recordScalar("-------", 0);
                }
                /*
                for(int i=0;i<(int)switch_used.size();i++){
                    std::string scalarName = "switch_used_id_" + std::to_string(i);
                    recordScalar(scalarName.c_str(), switch_used[i]);
                }*/
                recordScalar("+++++++++++++++++++++", simTime().dbl());
            }
        }
        else if (msg->isName("FlowEndEvt")) {
            // find which flow
            int idx=-1;
            for (int i=0; i<(int)flows.size(); i++)
                if (flows[i].endEvt == msg) { idx=i; break; }
            if (idx<0) error("Unknown end evt");
            
            FlowEntry endingFlow = flows[idx];

            
            //EV << "end flow from " << endingFlow.srcHost << "\n";
            
            if(Mode==3 && useINC[endingFlow.groupId]){//EDT模式，释放交换机资源
                if(useINC[endingFlow.groupId]==-1){
                    error("not dup useINC");
                }
                for(auto n:endingFlow.nodes){
                    if(n>=server_cnt){
                        int offset = n - server_cnt;
                        if(switch_used[offset]<=0) error("switch_used underflow in Mode 3");
                        switch_used[offset]--;
                    }
                    else error("Server node in flow entry in Mode 3 FlowEnd");
                }
            }
            
            //remove first
            //delete flows[idx].endEvt;
            flows.erase(flows.begin()+idx);
            
            allocateRates();//allocate after removal

            // send FlowEnd to dstHost
            FlowEnd *fe = new FlowEnd();
            fe->flowId = endingFlow.flowId;
            sendDirect(fe,
                       getParentModule()->getSubmodule(
                         "h", endingFlow.srcHost),
                       "in");
            // cleanup
            delete msg;
        }
        else {
            delete msg;
        }
    }
};

Define_Module(FlowController);