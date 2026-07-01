#include "std_header.p4"
#include "custom_header.p4"
#include "dcqcn.p4"
#include "random_drop.p4"
// #include "byteswap.p4"

parser EgressParser(packet_in packet,
               out headers hdr,
               out egress_metadata md,
               out egress_intrinsic_metadata_t eg_intr_md) {

    // only allreduce flows come in
    state start {
        packet.extract(eg_intr_md);
        packet.extract(hdr.internal);
        transition select(hdr.internal.pass_cnt) {
            INVALID_PASSCNT : parse_eth;
            _ : parse_netccl;
        }
    }

    state parse_eth {
        packet.extract(hdr.eth);
        transition select(hdr.eth.protocol) {
            IP_PROTOCOL : parse_ip;
            default     : accept;
        }
    }

    state parse_ip {// for dcqcn
        packet.extract(hdr.ip);
        transition accept;
    }

    state parse_netccl{
        packet.extract(hdr.eth);
        packet.extract(hdr.ip);
        packet.extract(hdr.netccl);
        packet.extract(hdr.payload);
        transition accept;
    }
}

control Egress(
        inout headers hdr,
        inout egress_metadata md,
        in egress_intrinsic_metadata_t eg_intr_md,
        in egress_intrinsic_metadata_from_parser_t eg_ps_md,
        inout egress_intrinsic_metadata_for_deparser_t eg_dps_md,
        inout egress_intrinsic_metadata_for_output_port_t eg_op_md) {

    action restore_fields(ip_addr_t sip, ip_addr_t dip) {
        eg_dps_md.drop_ctl = 0;
        hdr.ip.sip = sip;
        hdr.ip.dip = dip;
    }

    table restore_table{
        key = {
            hdr.netccl.group_id : exact;
            hdr.netccl.rank : exact;
        }
        actions = {
            restore_fields;
            NoAction;
        }
        size = MAX_WORKER_NUM;
        default_action = NoAction();
    }

    action retore_smac(mac_addr_t smac) {
        hdr.eth.smac = smac;
    }

    table restore_smac_table{
        key = {
            hdr.ip.sip : exact;
        }
        actions = {
            retore_smac;
            NoAction;
        }
        size = MAX_SERVER_NUM;
        default_action = NoAction();
    }

    action retore_dmac(mac_addr_t dmac) {
        hdr.eth.dmac = dmac;
    }

    table restore_dmac_table{
        key = {
            hdr.ip.dip : exact;
        }
        actions = {
            retore_dmac;
            NoAction;
        }
        size = MAX_SERVER_NUM;
        default_action = NoAction();
    }

    Register<bit<32>, bit<8>>(8) reg_retrans_cnt;

    RegisterAction<bit<32>, bit<8>, bit<32>>(reg_retrans_cnt) reg_retrans_cnt_update = {
        void apply(inout bit<32> cnt, out bit<32> ret) {
            cnt = cnt + 1;
            ret = cnt;
        }
    };

    action retrans_cnt_update() {
        reg_retrans_cnt_update.execute(hdr.netccl.rank);
    }

    action invalidate_payload() {
        hdr.payload.setInvalid();
        hdr.ip.length = (bit<16>)(sizeInBytes(hdr.ip) + sizeInBytes(hdr.netccl));
    }

    apply { 
#ifdef TEST
        if(eg_intr_md.egress_rid == 123) {
            hdr.internal.setInvalid();
            return;
        }
#endif

        if(hdr.internal.pass_cnt == 2) {
// #define DEBUG_CNT_RETRANS
#ifdef DEBUG_CNT_RETRANS
            if(hdr.internal.enable_multicast == 0) {
                retrans_cnt_update();
            }
#endif
            if(hdr.internal.enable_multicast != 0) {
                hdr.netccl.rank = (bit<8>)eg_intr_md.egress_rid;
            }
            restore_table.apply();
            restore_smac_table.apply();
            restore_dmac_table.apply();
            // ByteSwapAll.apply(hdr);
        }

        // remove unnecessary payloads for reduce and broadcast
        if(hdr.internal.pass_cnt == 2) {
            if(hdr.netccl.coll_type == COLL_TYPE_REDUCE) {
                if(hdr.netccl.rank == hdr.netccl.root) ;
                else invalidate_payload();
            }
            else if(hdr.netccl.coll_type == COLL_TYPE_BROADCAST) {
                if(hdr.netccl.rank == hdr.netccl.root) invalidate_payload();
            }
        }

        // remove unnecessary internal header
        if(!hdr.netccl.isValid()) hdr.internal.setInvalid();
        else if(hdr.internal.pass_cnt == 2) hdr.internal.setInvalid();
        else hdr.internal.pass_cnt = hdr.internal.pass_cnt + 1;
            
        dcqcn.apply(hdr, eg_intr_md);
        random_drop.apply(eg_dps_md);
    }
}

control EgressChecksum(inout headers hdr) {
    Checksum() csum;
    apply{
        hdr.ip.checksum = csum.update({
            hdr.ip.ver_hl,
            hdr.ip.dscp_ecn,
            hdr.ip.length,
            hdr.ip.id,
            hdr.ip.flag_offset,
            hdr.ip.ttl,
            hdr.ip.protocol,
            hdr.ip.sip,
            hdr.ip.dip
        });
    }
}

control EgressDeparser(packet_out packet,
                  inout headers hdr,
                  in egress_metadata md,
                  in egress_intrinsic_metadata_for_deparser_t eg_dps_md) {
            
    apply { 
        EgressChecksum.apply(hdr);
        packet.emit(hdr);
    }
}