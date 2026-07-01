#ifndef __FLOWMSG_H__
#define __FLOWMSG_H__
#include <omnetpp.h>
#include <vector>
using namespace omnetpp;

// Host 发给 FlowController 的通讯组启动消息
class FlowStart : public cMessage {
  public:
    int flowId;
    int groupId;           // 通信组的全局编号
    int srcHost;           // 来自哪个任务
    std::vector<int> servers;//allreduce 的参与节点列表
    double bits;               // 流总大小（比特）
    FlowStart() : cMessage("FlowStart") {}
};

// FlowController 发回给主机的结束消息
class FlowEnd : public cMessage {
  public:
    int flowId;
    FlowEnd() : cMessage("FlowEnd") {}
};

#endif
