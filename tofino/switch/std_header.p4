#ifndef __HEADER_P4__
#define __HEADER_P4__

#define IP_PROTOCOL 0x0800

#define TCP_PROTOCOL 0x06
#define UDP_PROTOCOL 0x11

#define RDMA_DPORT 0x12b7 // 4791

#define RDMA_OP_SEND_FIRST 8w0x00
#define RDMA_OP_SEND_MIDDLE 8w0x01
#define RDMA_OP_SEND_LAST 8w0x02
#define RDMA_OP_SEND_LAST_WITH_IMM 8w0x03
#define RDMA_OP_SEND_ONLY 8w0x04
#define RDMA_OP_SEND_ONLY_WITH_IMM 8w0x05
#define RDMA_OP_WRITE_FIRST 8w0x06
#define RDMA_OP_WRITE_MIDDLE 8w0x07
#define RDMA_OP_WRITE_LAST 8w0x08
#define RDMA_OP_WRITE_LAST_WITH_IMM 8w0x09
#define RDMA_OP_WRITE_ONLY 8w0x0a // WRITE_ONLY occurs when the message is shorter than RDMA_MTU
#define RDMA_OP_WRITE_ONLY_WITH_IMM 8w0x0b
#define RDMA_OP_ACK 8w0x11
#define RDMA_OP_CNP 8w0x81

//bit<9> PortId_t 
typedef bit<48> mac_addr_t;
typedef bit<32> ip_addr_t;
typedef bit<16> port_t;
typedef bit<16> checksum_t;

header eth_t {
    mac_addr_t dmac;
    mac_addr_t smac;
    bit<16> protocol;
}

// tofino is not friendly with bit<X> where X is not a multiple of 8
header ip_t {
    bit<8> ver_hl; // 4 + 4
    bit<8> dscp_ecn; // tos, 6 + 2
    bit<16> length;
    bit<16> id; // id is unique even during retransmission
    bit<16> flag_offset; // flag and offset of fragment, 3 + 13
    bit<8> ttl;
    bit<8> protocol;
    checksum_t checksum;
    ip_addr_t sip;
    ip_addr_t dip;
}

header udp_t {
    port_t sport;
    port_t dport;
    bit<16> length;
    checksum_t checksum; // can be 0 if not needed
}

header bth_t {
    bit<8> opcode;
    bit<8> se_migreq_pad_ver; // se = Solicited Event, ver = Transport Header Version, 1 + 1 + 2 + 4
    bit<16> pkey; // Partition Key, like VLAN id, 1 bit permission + 15 bit key 
    bit<8> f_b_rsv; // FECN, BECN, reserved(0), 1 + 1 + 6, FECN may be useless in ROCEv2?
    bit<24> dqpn; // dest QPN
    // bit<8> ackreq_rsv; // the ACK for this packet should be scheduled by the responder, 1 + 7 
    // bit<24> seq_num;// responder is able to send ACK and can decide whether to send ACK.
    bit<32> ackreq_rsv_seqnum; // 1 + 7 + 24
}

header reth_t {
    bit<64> mem_addr; // virtual address
    bit<32> rkey;
    bit<32> mem_length; // DMA length, padding bytes is not included
}

header aeth_t {
    bit<32> syndrome_msn; 
    // 1 bit 0 + 2 bit flag (ACK, RNR NAK, reserved, NAK) + 5 bit number (credit cnt, RNR timer, N/A, NAK code)
    // + 24 bit message sequence number, start from 0
}

header cnp_t {
    bit<128> reserved;
}

header imm_t {
    bit<32> imm;
}

header icrc_t {
    bit<32> crc;
}

#endif