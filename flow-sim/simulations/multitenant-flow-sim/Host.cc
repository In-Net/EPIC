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
    int traceIdx;//任务类型
    int DP;//DP 组大小
    double DP_bits;// DP 通信组单次传输的比特数
    simtime_t calculation_time; // 计算时间
    simtime_t start_time;

    struct ModelTrace {
        const char* name;
        double compute_ms; // 每个 iteration 的计算时间（ms）
        double comm_gb;    // 每个 iteration 的 DP 通信量（GB）
        int DP;            // DP 组大小
    };

    static constexpr ModelTrace kTraces[5] = {
        {"XL", 24000.000, 60.0000, 512},
        {"L", 3000.000, 20.0000, 128},
        {"M", 540.000, 6.0000, 32},
        {"S", 40.000, 0.2000, 8},
        {"XS", 10.000, 0.0200, 2},
    };


    cMessage *sendEvent = nullptr;
    int op;//0：计算，1：TP通信，2：DP通信
    int left_iteration;//剩余迭代次数
    int nextFlowId = 0;//给通讯组分配编号
    int left_group;//当前还剩多少个通讯组没有完成
    std::vector<int> serverIds;//该任务使用的所有server ID
    
    int serverID(int DPidx,int /*PPidx*/,int /*TPidx*/){
        return serverIds[DPidx];
    }


    void sendDP(int flag=0){
        FlowStart *fs = new FlowStart();
        fs->flowId   = nextFlowId++;
        fs->srcHost  = hostId;
        fs->servers = serverIds;
        fs->groupId = hostId; //标记 DP 通信组编号，全局唯一
        if(flag) fs->bits = -1;//Mode=2时，告知FlowController 通信组信息
        else fs->bits     = DP_bits;
        send(fs, "out");
    }

  protected:
    virtual void initialize() override {
        hostId = getIndex();
        Mode = getParentModule()->par("Mode").intValue();
        left_iteration = getParentModule()->par("iterations").intValue();


        sendEvent = new cMessage("sendEvent");
        
        
    }
    virtual void handleMessage(cMessage *msg) override {
        
        if(auto *ss = dynamic_cast<SigStart*>(msg)){
            //收到FlowController的开始信号
            traceIdx = ss->traceIdx;
            serverIds = ss->serverIds;
            const auto &tr = kTraces[traceIdx];
            DP = tr.DP;
            calculation_time = tr.compute_ms / 1000.0; // 计算时间随DP组大小线性缩短
            DP_bits = tr.comm_gb * 1e9 * 8.0;
            delete msg;

            op=0;//初始为计算状态
            

            if(Mode!=1 && Mode!=2 && Mode!=3 && Mode!=0) error("Invalid Mode");
            if(Mode==1||Mode==2){//静态模式，提前通知Controller 通信组信息
                sendDP(1);
            }
            
            start_time = simTime();
            //recordScalar("phase_start_compute", simTime());
            scheduleAt(simTime()+calculation_time, sendEvent);
            //进入第一轮的计算阶段
            
            
            EV << "Host " << hostId
            << " initialized. trace=" << tr.name
            << " traceIdx=" << traceIdx
            << " iterations=" << left_iteration
            << " DP=" << DP
            << " DP_bits=" << DP_bits
            << " calculation_time=" << calculation_time << "\n";
        }
        else if(msg == sendEvent) {
            if (op != 0) error("Invalid state in sendEvent");

            op = 2;//保持阶段编号一致，op=1为多租户的Tp预留，
            //recordScalar("phase_finish_compute", simTime()-start_time);

            sendDP(/*flag=*/0);
            left_group = 1;
            return;
            
            
        }
        else if (auto *fe = dynamic_cast<FlowEnd*>(msg)) {
            left_group--;
            if(left_group == 0){
                if (op != 2) error("Invalid state in FlowEnd");
                
                //DP传输完成，开始下一轮计算或结束
                left_iteration--;
                //recordScalar("finish_DP_trans", simTime()-start_time);
                if(left_iteration == 0){//全部完成，结束
                    cancelAndDelete(sendEvent);
                    delete msg;
                    simtime_t JCT = simTime()-start_time;//该Host完成时间
                    if(traceIdx==0){
                        recordScalar("JCT0", JCT);
                    }
                    else if(traceIdx==1){
                        recordScalar("JCT1", JCT);
                    }
                    else if(traceIdx==2){
                        recordScalar("JCT2", JCT);
                    }
                    else if(traceIdx==3){
                        recordScalar("JCT3", JCT);
                    }
                    else if(traceIdx==4){
                        recordScalar("JCT4", JCT);
                    }
                    else error("Invalid traceIdx in JCT recording");
                    recordScalar("traceIdx", traceIdx);
                    recordScalar("DPbits", DP_bits);
                    recordScalar("calc_time", calculation_time);
                    recordScalar("iterations", left_iteration);
                    recordScalar("DP", DP);
                    auto *se = new SigEnd();
                    se->srcHost = hostId;
                    se->serverIds = serverIds;
                    send(se, "out");
                    return;
                }
                op=0;
                scheduleAt(simTime()+calculation_time, sendEvent);
                //std::string phase_name = "phase_start_compute "+std::to_string(hostId);
                //recordScalar(phase_name.c_str(), simTime());
            }
            delete msg;
        }
        else {
            delete msg;
        }
        
    }
};

Define_Module(Host);
