#include <omnetpp.h>
#include <vector>
#include <cassert>
#include "FlowMsg.h"
using namespace omnetpp;

class Host : public cSimpleModule {// 一个host代表一个租户任务
  private:
    int Mode;//通信模式
    //Mode=1:EDT
    //Mode=2:Spatial Multiplexing
    //Mode=3:Temporal Multiplexing

    int hostId;//任务编号

    // 任务基础信息
    int TP;//TP 组大小
    int DP;//DP 组大小
    int PP;//PP 组大小，不进行通信，但会影响放置
    double num_tokens_per_seq;
    double batch_size;
    double num_parameters;
    double dtype_size;
    double num_layers;
    double hidden_size;
    double gpu_flops;
    double TP_bits;// TP 通信组单次传输的比特数
    double DP_bits;// DP 通信组单次传输的比特数
    simtime_t calculation_time = 1.0; // 计算时间
    simtime_t start_time;

    cMessage *sendEvent = nullptr;
    int op;//0：计算，1：TP通信，2：DP通信
    int left_iteration;//剩余迭代次数
    int nextFlowId = 0;//给通讯组分配编号
    int left_group;//当前还剩多少个通讯组没有完成
    int serverID_base;//该任务的第一个server ID
    
    int serverID(int DPidx,int PPidx,int TPidx){
        return DPidx * TP * PP + PPidx * TP + TPidx;
    }

    void sendTP(int DPidx,int PPidx,int flag=0){
        std::vector<int> servers;
        for(int i=0; i<TP; i++) servers.push_back(serverID(DPidx,PPidx,i)+serverID_base);
        FlowStart *fs = new FlowStart();
        fs->flowId   = nextFlowId++;
        fs->srcHost  = hostId;
        fs->servers = servers;
        fs->groupId = hostId*(DP*PP+TP*PP) + DPidx*PP+PPidx; //标记 TP 通信组编号
        if(flag) fs->bits = -1;//Mode=2时，告知FlowController 通信组信息
        else fs->bits     = TP_bits;
        send(fs, "out");
    }

    void sendDP(int TPidx,int PPidx,int flag=0){
        std::vector<int> servers;
        for(int i=0; i<DP; i++) servers.push_back(serverID(i,PPidx,TPidx)+serverID_base);
        FlowStart *fs = new FlowStart();
        fs->flowId   = nextFlowId++;
        fs->srcHost  = hostId;
        fs->servers = servers;
        fs->groupId = hostId*(DP*PP+TP*PP) + DP*PP + TPidx*PP+PPidx; //标记 DP 通信组编号
        if(flag) fs->bits = -1;//Mode=2时，告知FlowController 通信组信息
        else fs->bits     = DP_bits;
        send(fs, "out");
    }

  protected:
    virtual void initialize() override {
        hostId = getIndex();
        Mode = getParentModule()->par("Mode").intValue();
        sendEvent = new cMessage("sendEvent");
        left_iteration = getParentModule()->par("iterations").intValue();
        TP = getParentModule()->par("TP").intValue();
        DP = getParentModule()->par("DP").intValue();
        PP = getParentModule()->par("PP").intValue();
        num_tokens_per_seq = getParentModule()->par("num_tokens_per_seq").doubleValue();
        batch_size = getParentModule()->par("batch_size").doubleValue();
        num_parameters = getParentModule()->par("num_parameters").doubleValue();
        dtype_size = getParentModule()->par("dtype_size").doubleValue();
        num_layers = getParentModule()->par("num_layers").doubleValue();
        hidden_size = getParentModule()->par("hidden_size").doubleValue();
        gpu_flops = getParentModule()->par("gpu_flops").doubleValue();
        double total_calculation = 2*num_tokens_per_seq*batch_size*num_parameters*(1+2);
        calculation_time = total_calculation / (TP*DP*PP) / gpu_flops;
        TP_bits = dtype_size*2*2*2*num_layers*hidden_size*num_tokens_per_seq*batch_size/(PP*DP); // 每个 TP 组通信量
        DP_bits = dtype_size*2*num_parameters/(PP*TP); // 每个 DP 组通信量
        serverID_base = TP * DP * PP * hostId;//顺序放置
        op=0;//初始为计算状态

        recordScalar("TPbits",TP_bits);
        recordScalar("DPbits",DP_bits);
        recordScalar("calc_time",calculation_time);

        if(Mode!=0 && Mode!=1 && Mode!=2 && Mode!=3) error("Invalid Mode");
        if(Mode==1||Mode==2){//静态模式，提前通知Controller 通信组信息
            for(int dp=0; dp<DP; dp++){
                for(int pp=0; pp<PP; pp++)
                    sendTP(dp,pp,1);
            }
            //先发送 TP 组信息，再发送 DP 组信息，保证TP组优先分配资源
            op=-1;
        }
        
        if(Mode==3||Mode==0) start_time = simTime();
        scheduleAt(simTime()+calculation_time, sendEvent);
        //对于mode3：进入第一轮的计算阶段
        
        
        EV << "Host " << hostId << " initialized." << " serverID_base=" << serverID_base
           << " iterations=" << left_iteration
           << " TP=" << TP << " DP=" << DP
           << " TP_bits=" << TP_bits << " DP_bits=" << DP_bits
           << " calculation_time=" << calculation_time << "\n";
        
    }
    virtual void handleMessage(cMessage *msg) override {
        
        
        if(msg == sendEvent) {
            if(op!=0&&op!=-1) error("Invalid state in sendEvent");
            if(op==-1){
                if(Mode!=2&&Mode!=1) error("Invalid Mode in sendEvent");
                op=0;
                for(int tp=0; tp<TP; tp++){
                    for(int pp=0; pp<PP; pp++)
                        sendDP(tp,pp,1);
                }
                start_time = simTime();
                scheduleAt(simTime()+calculation_time, sendEvent);//进入第一轮的计算阶段
            }
            else{
                op=1;//开始TP传输
                recordScalar("finish_compute", simTime()-start_time);
                for(int dp=0; dp<DP; dp++){
                    for(int pp=0; pp<PP; pp++)
                        sendTP(dp,pp);
                }
                left_group = DP*PP;
            }
            
            
        }
        else if (auto *fe = dynamic_cast<FlowEnd*>(msg)) {
            left_group--;
            if(left_group == 0){
                if(op==0) error("Invalid state in FlowEnd");
                if(op==1){//TP传输完成，开始DP传输
                    op=2;
                    recordScalar("finish_TP_trans", simTime()-start_time);
                    left_group = TP*PP;
                    for(int tp=0; tp<TP; tp++){
                        for(int pp=0; pp<PP; pp++)
                            sendDP(tp,pp);
                    }
                    
                }
                else if(op==2){//DP传输完成，开始下一轮计算或结束
                    left_iteration--;
                    recordScalar("finish_DP_trans", simTime()-start_time);
                    if(left_iteration == 0){//全部完成，结束
                        cancelAndDelete(sendEvent);
                        delete msg;
                        simtime_t JCT = simTime()-start_time;//该Host完成时间
                        recordScalar("JCT", JCT);
                        return;
                    }
                    op=0;
                    scheduleAt(simTime()+calculation_time, sendEvent);
                }
            }
            delete msg;
        }
        else {
            delete msg;
        }
        
    }
};

Define_Module(Host);
