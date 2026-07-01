#ifndef __NETCCL_BACKEND_HEADER_HPP__
#define __NETCCL_BACKEND_HEADER_HPP__

#include "../common/common.hpp"

#include <rte_ether.h>
#include <rte_ip.h>
#include <rte_udp.h>

// copied from DPDK headers

// struct __rte_aligned(2) rte_ether_addr {
//     uint8_t addr_bytes[RTE_ETHER_ADDR_LEN]; 
// };

// struct __rte_aligned(2) rte_ether_hdr {
//     struct rte_ether_addr dst_addr; 
//     struct rte_ether_addr src_addr; 
//     rte_be16_t ether_type; 
// };

// rte_ether_addr_copy()
// rte_ether_format_addr()
// rte_ether_unformat_addr()

// struct rte_ipv4_hdr {
//     __extension__
//     union {
//         uint8_t version_ihl;    
//         struct {
// #if RTE_BYTE_ORDER == RTE_LITTLE_ENDIAN
//             uint8_t ihl:4;     
//             uint8_t version:4; 
// #elif RTE_BYTE_ORDER == RTE_BIG_ENDIAN
//             uint8_t version:4; 
//             uint8_t ihl:4;     
// #endif
//         };
//     };
//     uint8_t  type_of_service;   
//     rte_be16_t total_length;    
//     rte_be16_t packet_id;       
//     rte_be16_t fragment_offset; 
//     uint8_t  time_to_live;      
//     uint8_t  next_proto_id;     
//     rte_be16_t hdr_checksum;    
//     rte_be32_t src_addr;        
//     rte_be32_t dst_addr;        
// } __rte_packed;

// struct rte_udp_hdr {
//     rte_be16_t src_port;    
//     rte_be16_t dst_port;    
//     rte_be16_t dgram_len;   
//     rte_be16_t dgram_cksum; 
// } __rte_packed;

#define PAYLOAD_LEN 64
#define PAYLOAD_SIZE (PAYLOAD_LEN * sizeof(int))

// struct ether_addr {
//     uint8_t addr_bytes[RTE_ETHER_ADDR_LEN]; 
// } PACKED;

// struct ether_hdr {
//     struct ether_addr dst_addr; 
//     struct ether_addr src_addr; 
//     uint16_t ether_type; 
// } PACKED;

// struct ip_addr_hdr {// skip other fields in IPv4 header
//     uint32_t src_addr;
//     uint32_t dst_addr;
// } PACKED;

// static_assert(sizeof(ip_addr_hdr) % 2 == 0);

using psn_t = uint32_t;

static_assert(RTE_BYTE_ORDER == RTE_LITTLE_ENDIAN);

struct netccl_inc_hdr {
    uint8_t coll_type;
    // uint8_t queue_id;// A backup of ip.type_of_service, for DEBUG
    group_id_t group_id;
    rank_t rank;
    rank_t root;
    agg_size_t agg_addr;// this can access about 16MB switch memory
    psn_t psn;// packet sequence number
} PACKED;

struct netccl_payload {
    int32_t data[PAYLOAD_LEN];
} PACKED;

static_assert(sizeof(netccl_payload) % 2 == 0);

struct netccl_pkt {
    rte_ether_hdr eth;
    rte_ipv4_hdr ip;
    // rte_udp_hdr udp;
    netccl_inc_hdr inc;
    netccl_payload payload;
};

struct netccl_prefill_pkt {
    rte_ether_hdr eth;
    rte_ipv4_hdr ip;
};

#endif