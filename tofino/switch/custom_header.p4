#ifndef __CUSTOM_HEADER_STRUCT_P4__
#define __CUSTOM_HEADER_STRUCT_P4__

#include "std_header.p4"

typedef bit<32> bitmap_t;
typedef bit<16> agg_addr_t; 
typedef int<32> agg_t;
typedef bit<32> psn_t;

#define COLL_TYPE_ALLREDUCE 1
#define COLL_TYPE_REDUCE 2
#define COLL_TYPE_BROADCAST 3

header internal_t {// shared among ingress, egress and recirculation passes
    bit<8> pass_cnt;
    bit<1> enable_multicast;
    @padding bit<7> _pad;
}

header netccl_t {
    bit<8> coll_type;// 1 bit is_root + 7 bit collective_type
    bit<8> group_id;
    bit<8> rank;
    bit<8> root;// for reduce and broadcast
    // bit<8> queue_id;// use TOS to select queue
    // @padding bit<8> _pad;
    agg_addr_t agg_addr;
    psn_t psn;
}

header payload_t {
    agg_t data00;
    agg_t data01;
    agg_t data02;
    agg_t data03;
    agg_t data04;
    agg_t data05;
    agg_t data06;
    agg_t data07;
    agg_t data08;
    agg_t data09;
    agg_t data0a;
    agg_t data0b;
    agg_t data0c;
    agg_t data0d;
    agg_t data0e;
    agg_t data0f;
    agg_t data10;
    agg_t data11;
    agg_t data12;
    agg_t data13;
    agg_t data14;
    agg_t data15;
    agg_t data16;
    agg_t data17;
    agg_t data18;
    agg_t data19;
    agg_t data1a;
    agg_t data1b;
    agg_t data1c;
    agg_t data1d;
    agg_t data1e;
    agg_t data1f;
    agg_t data20;
    agg_t data21;
    agg_t data22;
    agg_t data23;
    agg_t data24;
    agg_t data25;
    agg_t data26;
    agg_t data27;
    agg_t data28;
    agg_t data29;
    agg_t data2a;
    agg_t data2b;
    agg_t data2c;
    agg_t data2d;
    agg_t data2e;
    agg_t data2f;
    agg_t data30;
    agg_t data31;
    agg_t data32;
    agg_t data33;
    agg_t data34;
    agg_t data35;
    agg_t data36;
    agg_t data37;
    agg_t data38;
    agg_t data39;
    agg_t data3a;
    agg_t data3b;
    agg_t data3c;
    agg_t data3d;
    agg_t data3e;
    agg_t data3f;

    // agg_t data40;
    // agg_t data41;
    // agg_t data42;
    // agg_t data43;
    // agg_t data44;
    // agg_t data45;
    // agg_t data46;
    // agg_t data47;
}

struct agg_pair_t{
    agg_t agg0;
    agg_t agg1;
}

struct headers {// Make sure you have a right order in parser, or BUGs may occur.
    internal_t internal;// 2B
    eth_t eth;// 14B
    ip_t ip;// 20B
    netccl_t netccl;// 10B
    payload_t payload;// 256B
}

struct port_metadata_t {
    bit<1> is_recirculate_port;
    @padding bit<15> _pad; 
}

#define NETCCL_PROTOCOL 0xfe

#define INVALID_PASSCNT ((bit<8>)0xff)

@pa_atomic("ingress", "md.version")
struct ingress_metadata {
    // allreduce
    port_metadata_t port_metadata;
    
    bit<1> first_packet;
    bit<1> new_packet;// not all packets have payload
    bit<1> full;

    bitmap_t bitmap;
    bitmap_t bitmap_mask;
    bitmap_t bitmap_old;
    bitmap_t bitmap_flip;

    psn_t version;
    // bitmap_t payload_bitmap;
}

struct egress_metadata {

}

#endif