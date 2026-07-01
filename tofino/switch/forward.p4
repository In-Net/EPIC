#ifndef __FORWARD_P4__
#define __FORWARD_P4__
#include "std_header.p4"
#include "custom_header.p4"


control Forward(
        inout headers hdr,
        inout ingress_metadata md,
        inout ingress_intrinsic_metadata_for_deparser_t ig_dps_md,
        inout ingress_intrinsic_metadata_for_tm_t ig_tm_md) {

    action drop() {
        ig_dps_md.drop_ctl = 0x1;
    }

    action forward(PortId_t port) {
        ig_dps_md.drop_ctl = 0;
        ig_tm_md.ucast_egress_port = port;
    }

    action multicast(MulticastGroupId_t group) {// 16 bit
        ig_dps_md.drop_ctl = 0;
        ig_tm_md.mcast_grp_a = group;
    }

    table recirculate_table{
        key = {
            hdr.netccl.group_id : exact;
        }
        actions = {
            forward;
            drop;
        }
        size = MAX_GROUP_NUM;
        default_action = drop();
    }

    table l2_forward_table{
        key = {
            hdr.eth.dmac : exact;
        }
        actions = {
            forward;
            multicast;
            drop;
        }
        size = MAX_SERVER_NUM;
        default_action = drop();//forward(148);
    }

    apply {
        // using "||" and "&&" may cause BUG
        if(md.bitmap_old == md.bitmap_flip) md.full = 1; 
        else if(md.bitmap_old == md.bitmap_mask) md.full = 1;
        else md.full = 0;

        // configure multicast
        if(hdr.internal.pass_cnt == 2 && hdr.internal.enable_multicast == 1) {// 
            // Multicast but exclude this packet,
            // this packet should be unicasted.
            // Another solution is to drop this packet without setting exclusion. 
            ig_tm_md.mcast_grp_a = (bit<16>)hdr.netccl.group_id; 
            ig_dps_md.drop_ctl = 0;
            // invalidate(ig_tm_md.ucast_egress_port);//forward(9w0x1ff); // drop without set drop_ctl=0x1
        }
        else if(hdr.internal.pass_cnt == 1) {// configure unicast
            // drop on md.full == 0
            if(md.full == 0) {
                drop();
            }
            else {
                recirculate_table.apply(); // TODO: make multicast and unicast packet forward to different recirculate ports
            }
        }
        else { // otherpass || (secondpass && is_multicast==0)
            l2_forward_table.apply();
        }

        // change internal header
        if(hdr.internal.pass_cnt == 1 && md.full == 1) {
// #define ALWAYS_MULTICAST 
// it seems that enable this has better performance???
// 
#ifdef ALWAYS_MULTICAST
            hdr.internal.enable_multicast = 1; 
#else 
            hdr.internal.enable_multicast = md.new_packet; 
#endif
        }
    }
}

#endif