//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program.  If not, see http://www.gnu.org/licenses/.
//
simple Host
{
    gates:
        input in;    // 接收 FlowEnd
        output out;   // 发出 FlowStart
}

simple FlowController
{
    gates:
        input in[];      // 接收所有 FlowStart
        // 不需要输出 gate，sendDirect() 直发到 Host.in
}

network FlowNet
{
    parameters:
        int Mode;
        //Mode=1:EDT
        //Mode=2:Spatial Multiplexing
        //Mode=3:Temporal Multiplexing
        int job_cnt;
        
        //fat tree info
        int leaf_server_degree;
        int spine_leaf_degree;
        int spine_core_degree;
        int pod_cnt;
        int switch_limit;//一台交换机最多可以分配的内存资源份数，只在mode=2/3时有效。
        double linkCapacity;

        int gpusPerServer;
        double scaleupCapacity;
        
        //job info
        int iterations;
        
        //double TP_bits @unit(b) = default(1e9b);
        //double DP_bits @unit(b) = default(2e8b);
        //double calculation_time @unit(s) = default(60ms);

    submodules:
        h[job_cnt]: Host {
            @display("p=393,229");
        }
        fc: FlowController {
            @display("p=300,200");
        }
    connections allowunconnected:
        for i=0..job_cnt-1 {
            h[i].out --> fc.in++; // 所有 FlowStart 都到 FC
        }
}
