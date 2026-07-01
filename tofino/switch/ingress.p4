#include "std_header.p4"
#include "custom_header.p4"
#include "aggregator.p4"
#include "forward.p4"
// #include "byteswap.p4"

parser IngressParser(packet_in packet,
               out headers hdr,
               out ingress_metadata md,
               out ingress_intrinsic_metadata_t ig_intr_md) {

    state start {
        packet.extract(ig_intr_md);
        transition select(ig_intr_md.resubmit_flag) {
            0 : parse_port_metadata;
        }
    }

    state parse_port_metadata {
        md.port_metadata = port_metadata_unpack<port_metadata_t>(packet);
        transition select(md.port_metadata.is_recirculate_port) {
            0 : parse_ethernet;
            1 : parse_recirculate;
        }
    }

    state parse_ethernet {// only for non-recirculated packets
        packet.extract(hdr.eth);
        transition select(hdr.eth.protocol) {
            IP_PROTOCOL : parse_ip;
            default     : parse_other;
        }
    }

    state parse_ip {
        packet.extract(hdr.ip);
        transition select(hdr.ip.protocol ++ hdr.ip.length) {
            (bit<8>)NETCCL_PROTOCOL ++ (bit<16>)(20 + 10 + 256): alloc_internal_parse_payload;
            (bit<8>)NETCCL_PROTOCOL ++ (bit<16>)(20 + 10): alloc_internal_alloc_payload;
            default         : parse_other;
        }
    }

    state alloc_internal_parse_payload {// PATH 1: an INC packet from an external port
        hdr.internal.setValid();
        hdr.internal.pass_cnt = 1;
        hdr.internal.enable_multicast = 0;
        packet.extract(hdr.netccl);
        transition parse_payload;
    }

    state alloc_internal_alloc_payload {// PATH 1: an INC packet from an external port
        hdr.internal.setValid();
        hdr.internal.pass_cnt = 1;
        hdr.internal.enable_multicast = 0;
        packet.extract(hdr.netccl);
        transition alloc_payload;
    }

    state parse_other {// PATH 2: an non-INC packet from an external port
        hdr.internal.setValid();
        hdr.internal.pass_cnt = INVALID_PASSCNT;
        hdr.internal.enable_multicast = 0;
        transition accept;
    }
    
    state parse_recirculate {// PATH 3: an INC packet from an recirculation port
        packet.extract(hdr.internal);
        packet.extract(hdr.eth);
        packet.extract(hdr.ip);
        packet.extract(hdr.netccl);
        transition select(hdr.ip.length) {
            20 + 10 + 256: parse_payload;
            20 + 10: alloc_payload;
        }
    }

    // state parse_netccl {
    //     packet.extract(hdr.netccl);
    //     transition select(hdr.ip.length) {
    //         20 + 10 + 256: parse_payload;
    //         20 + 10: alloc_payload;
    //     }
    // }

    state parse_payload {
        packet.extract(hdr.payload);
        transition accept;
    }

    state alloc_payload {
        // advance 16 Bytes (8 * 16 bits)
        packet.advance(8 * (64 - 4 - sizeInBytes(hdr.eth) - sizeInBytes(hdr.ip) - sizeInBytes(hdr.netccl)));
        hdr.payload.setValid();
        hdr.payload.data00 = 0;
        hdr.payload.data01 = 0;
        hdr.payload.data02 = 0;
        hdr.payload.data03 = 0;
        hdr.payload.data04 = 0;
        hdr.payload.data05 = 0;
        hdr.payload.data06 = 0;
        hdr.payload.data07 = 0;
        hdr.payload.data08 = 0;
        hdr.payload.data09 = 0;
        hdr.payload.data0a = 0;
        hdr.payload.data0b = 0;
        hdr.payload.data0c = 0;
        hdr.payload.data0d = 0;
        hdr.payload.data0e = 0;
        hdr.payload.data0f = 0;
        hdr.payload.data10 = 0;
        hdr.payload.data11 = 0;
        hdr.payload.data12 = 0;
        hdr.payload.data13 = 0;
        hdr.payload.data14 = 0;
        hdr.payload.data15 = 0;
        hdr.payload.data16 = 0;
        hdr.payload.data17 = 0;
        hdr.payload.data18 = 0;
        hdr.payload.data19 = 0;
        hdr.payload.data1a = 0;
        hdr.payload.data1b = 0;
        hdr.payload.data1c = 0;
        hdr.payload.data1d = 0;
        hdr.payload.data1e = 0;
        hdr.payload.data1f = 0;
        hdr.payload.data20 = 0;
        hdr.payload.data21 = 0;
        hdr.payload.data22 = 0;
        hdr.payload.data23 = 0;
        hdr.payload.data24 = 0;
        hdr.payload.data25 = 0;
        hdr.payload.data26 = 0;
        hdr.payload.data27 = 0;
        hdr.payload.data28 = 0;
        hdr.payload.data29 = 0;
        hdr.payload.data2a = 0;
        hdr.payload.data2b = 0;
        hdr.payload.data2c = 0;
        hdr.payload.data2d = 0;
        hdr.payload.data2e = 0;
        hdr.payload.data2f = 0;
        hdr.payload.data30 = 0;
        hdr.payload.data31 = 0;
        hdr.payload.data32 = 0;
        hdr.payload.data33 = 0;
        hdr.payload.data34 = 0;
        hdr.payload.data35 = 0;
        hdr.payload.data36 = 0;
        hdr.payload.data37 = 0;
        hdr.payload.data38 = 0;
        hdr.payload.data39 = 0;
        hdr.payload.data3a = 0;
        hdr.payload.data3b = 0;
        hdr.payload.data3c = 0;
        hdr.payload.data3d = 0;
        hdr.payload.data3e = 0;
        hdr.payload.data3f = 0;
        transition accept;
    }
}

struct bitmap_pair {
    psn_t version;
    bitmap_t bitmap; // these two must have the same length
}

control swap_addr(inout headers hdr) {
    mac_addr_t mac_tmp;
    ip_addr_t ip_tmp;
    action swap_mac() {
        hdr.eth.dmac = hdr.eth.smac;
        hdr.eth.smac = mac_tmp;
    }
    action swap_ip() {
        hdr.ip.dip = hdr.ip.sip;
        hdr.ip.sip = ip_tmp;
    }
    apply {
        mac_tmp = hdr.eth.dmac;
        swap_mac();
        ip_tmp = hdr.ip.dip;
        swap_ip();
    }
}

control Ingress(
        inout headers hdr,
        inout ingress_metadata md,
        in ingress_intrinsic_metadata_t ig_intr_md,
        in ingress_intrinsic_metadata_from_parser_t ig_ps_md,
        inout ingress_intrinsic_metadata_for_deparser_t ig_dps_md,
        inout ingress_intrinsic_metadata_for_tm_t ig_tm_md) {

    action set_dmac() {
        hdr.eth.dmac = hdr.eth.smac;
    }

    action set_version() {
        // Since switch memory may be shared by multiple groups, include group_id in version is necessary.
        // Make sure the usage of psn (psn[23:0]) contains psn[14], because bit 14 flips once for each window of packets received
        md.version = hdr.netccl.group_id ++ hdr.netccl.psn[23:0]; 
        // The code below is also logically correct
        // md.version = hdr.netccl.group_id ++ hdr.netccl.psn[14] ++ 23w0; 
    }

    action get_allreduce_metadata(bitmap_t bitmap_mask) {
        md.bitmap_mask = bitmap_mask;
        // agg_addr is packeted in header
    }

    table metadata_table {
        key = {
            hdr.netccl.group_id: exact;
        }
        actions = {
            get_allreduce_metadata;
            NoAction;
        }
        size = MAX_GROUP_NUM; 
        default_action = NoAction();
    }

    action set_bitmap(out bitmap_t ret, bitmap_t bitmap) {
        ret = bitmap;
    }

    table bitmap_table {
        key = {
            hdr.netccl.rank: exact;
        }
        actions = {
            set_bitmap(md.bitmap);
            NoAction;
        }
        size = 32;
        const entries = {
#define ENTRY(k) k: set_bitmap(md.bitmap, 1<<k)
            ENTRY(0);
            ENTRY(1);
            ENTRY(2);
            ENTRY(3);
            ENTRY(4);
            ENTRY(5);
            ENTRY(6);
            ENTRY(7);
            ENTRY(8);
            ENTRY(9);
            ENTRY(10);
            ENTRY(11);
            ENTRY(12);
            ENTRY(13);
            ENTRY(14);
            ENTRY(15);
            ENTRY(16);
            ENTRY(17);
            ENTRY(18);
            ENTRY(19);
            ENTRY(20);
            ENTRY(21);
            ENTRY(22);
            ENTRY(23);
            ENTRY(24);
            ENTRY(25);
            ENTRY(26);
            ENTRY(27);
            ENTRY(28);
            ENTRY(29);
            ENTRY(30);
            ENTRY(31);
#undef ENTRY
        }
    }

    Register<bitmap_pair, agg_addr_t>(REGISTER_LEN, {0,0}) reg_bitmap;

    RegisterAction<bitmap_pair, agg_addr_t, bitmap_t>(reg_bitmap) reg_bitmap_update = {
        void apply(inout bitmap_pair reg, out bitmap_t ret) {
            // ret = 0;
            // if(reg.version == md.version) {
            //     ret = reg.bitmap;
            // }

            // if(reg.version == md.version) {
            //     reg.bitmap = reg.bitmap | md.bitmap;
            // }
            // else {
            //     reg.version = md.version;
            //     reg.bitmap = md.bitmap;
            // }

            // This version has no BUG in SDE 9.13, but cannot pass compilation in SDE 9.7.0.
            // Add pa_atomic to md.version to pass compilation.
            if(reg.version == md.version) {// TODO: make version be "(bit<8>)group_id ++ (bit<24>)psn"
                ret = reg.bitmap;
                reg.bitmap = reg.bitmap | md.bitmap;
            }
            else {
                reg.version = md.version;
                ret = 0;
                reg.bitmap = md.bitmap;
            }
            // The code below will cause a BUG (needs >= 2 workers to trigger)
            // In this version, ret is set to be alu_hi instead of mem_hi 
            // PS: "alu_a" equals to "assign with argument A" in bfa file
            // if(reg.version != md.version) {
            //     reg.version = md.version;
            //     ret = 0;
            //     reg.bitmap = md.bitmap;
            // }
            // else {
            //     ret = reg.bitmap;
            //     reg.bitmap = reg.bitmap | md.bitmap;
            // }
        }
    };

    action bitmap_update() {
        md.bitmap_old = reg_bitmap_update.execute(hdr.netccl.agg_addr);
    }
    
    action set_bitmap_flip() {
        md.bitmap_flip = md.bitmap ^ md.bitmap_mask;
    }

    action set_packet_state(bit<1> new_packet) {
        md.new_packet = new_packet;
    }

    table check_new_packet {
        key = {
            md.bitmap_old : ternary;
            hdr.netccl.rank : exact;
        }
        actions = {
            set_packet_state;
        }
        const entries = {
#define ENTRY(k) (0 &&& 1<<k, k) : set_packet_state(1)
            ENTRY(0);
            ENTRY(1);
            ENTRY(2);
            ENTRY(3);
            ENTRY(4);
            ENTRY(5);
            ENTRY(6);
            ENTRY(7);
            ENTRY(8);
            ENTRY(9);
            ENTRY(10);
            ENTRY(11);
            ENTRY(12);
            ENTRY(13);
            ENTRY(14);
            ENTRY(15);
            ENTRY(16);
            ENTRY(17);
            ENTRY(18);
            ENTRY(19);
            ENTRY(20);
            ENTRY(21);
            ENTRY(22);
            ENTRY(23);
            ENTRY(24);
            ENTRY(25);
            ENTRY(26);
            ENTRY(27);
            ENTRY(28);
            ENTRY(29);
            ENTRY(30);
            ENTRY(31);
#undef ENTRY
        }
        const default_action = set_packet_state(0);
        size = 65;
    }

    action adjust_ip_length(){
        hdr.ip.length = (bit<16>)(sizeInBytes(hdr.ip) + sizeInBytes(hdr.netccl) + sizeInBytes(hdr.payload));
    }

    apply {
        // ByteSwapAll.apply(hdr);

        // 0 
        if(hdr.netccl.isValid()) {
            set_dmac();
            adjust_ip_length();
        }

        // 0
        metadata_table.apply();// get bitmap_mask from group_id
        bitmap_table.apply();// get bitmap from rank
        set_version();

        // 1
        if(hdr.internal.pass_cnt == 1) bitmap_update();// update register bitmap and get bitmap_old
        // 1 
        set_bitmap_flip();

        // 2
        // One operand of &, | and ^ must be constant in "if",
        // so we implement "if((a & b) == 0) c = 1; else c = 0;" by a table to reduce stages consumed.
        // Otherwise, we should use "t = a & b; if(t == 0) ...", which consumes two stages.
        // Note, "c = (a & b) == 0;" is also not allowed even if c is in type of bool.
        // Note, in action() and control() (without if), one operand of &, | and ^ must be the same as destination, like "a = a & b".

        // Indicate which payload has been aggregated
        // The original version is: "md.payload_bitmap_old = md.bitmap_old & md.payload_bitmap_mask;"
        // However, "a = b & c" is not allowed on Tofino, thus we merged md.payload_bitmap_old and md.payload_bitmap_mask

        check_new_packet.apply();// get md.new_packet and md.new_payload from bitmap_old, rank and has_payload

        // 3
        if(md.bitmap_old == 0) md.first_packet = 1;
        else md.first_packet = 0;
    
        // 4-11
        if(hdr.netccl.isValid()) {
            AllAggregatorAccess.apply(hdr, md);
        }

        Forward.apply(hdr, md, ig_dps_md, ig_tm_md);
        
#ifdef TEST
        if(hdr.netccl.isValid()) {
            ig_tm_md.mcast_grp_b = 2;
        }
#endif   
    }
}

control IngressDeparser(
        packet_out packet,
        inout headers hdr,
        in ingress_metadata md,
        in ingress_intrinsic_metadata_for_deparser_t ig_dps_md) {

    apply{
        packet.emit(hdr);
    }
}