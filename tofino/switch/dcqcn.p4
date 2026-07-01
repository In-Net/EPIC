#ifndef __DCQCN_P4__
#define __DCQCN_P4__

#include "std_header.p4"
#include "custom_header.p4"

control dcqcn(
    inout headers hdr,
    in egress_intrinsic_metadata_t eg_intr_md) {

    Wred<bit<19>, bit<32>>(32w1, 8w1, 8w0) wred;
    apply {
        if(hdr.ip.isValid()) {
            if(hdr.ip.dscp_ecn[1:0] == 0) { // Using "!=" and "&&" sometimes causes BUG
            }
            else {
                bit<8> drop_flag = wred.execute(eg_intr_md.deq_qdepth, 0);
                if(drop_flag == 1) hdr.ip.dscp_ecn[1:0] = 3;
            }
        }
    }
}

#endif