#ifndef EPIC_CLEAN_H
#define EPIC_CLEAN_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <netinet/in.h>

#define EPIC_STAR4_HOSTS 4
#define EPIC_ROCE_UDP_PORT 4791
#define EPIC_DEFAULT_PAYLOAD_BYTES 1024
#define EPIC_MAX_PAYLOAD_BYTES 4096
#define EPIC_DEFAULT_COUNT 4096
#define EPIC_DEFAULT_ITERS 1
#define EPIC_DEFAULT_GID_INDEX 1
#define EPIC_DEFAULT_IB_PORT 1
#define EPIC_DEFAULT_SEGMENT_WINDOW 16
#define EPIC_DEFAULT_RECV_WINDOW 1024
#define EPIC_DEFAULT_QP_TIMEOUT 0x12
#define EPIC_FAKE_SWITCH_QPN_BASE 0x00ee0000u
#define EPIC_PSN_WINDOW 65536

typedef enum {
    EPIC_PRIM_ALLREDUCE = 0,
    EPIC_PRIM_REDUCE = 1,
    EPIC_PRIM_BCAST = 2,
    EPIC_PRIM_BARRIER = 3,
    EPIC_PRIM_ALLGATHER = 4,
    EPIC_PRIM_REDUCESCATTER = 5,
} epic_primitive_t;

typedef enum {
    EPIC_MODE_NON_TERMINATION = 2,
    EPIC_MODE_TERMINATION = 3,
} epic_mode_t;

typedef struct __attribute__((packed)) {
    uint8_t dst_mac[6];
    uint8_t src_mac[6];
    uint16_t ether_type;
} epic_eth_t;

typedef struct __attribute__((packed)) {
    uint8_t version_ihl;
    uint8_t tos;
    uint16_t total_length;
    uint16_t id;
    uint16_t flags_frag_off;
    uint8_t ttl;
    uint8_t protocol;
    uint16_t checksum;
    uint32_t src_ip;
    uint32_t dst_ip;
} epic_ipv4_t;

typedef struct __attribute__((packed)) {
    uint16_t src_port;
    uint16_t dst_port;
    uint16_t length;
    uint16_t checksum;
} epic_udp_t;

typedef struct __attribute__((packed)) {
    uint8_t opcode;
    uint8_t se_m_pad;
    uint16_t pkey;
    uint32_t qpn;
    uint32_t apsn;
} epic_bth_t;

typedef struct __attribute__((packed)) {
    uint32_t syn_msn;
} epic_aeth_t;

typedef struct {
    bool valid;
    bool is_data;
    bool is_ack;
    uint8_t opcode;
    uint32_t dst_qpn;
    uint32_t psn;
    uint32_t msn;
    uint32_t src_ip;
    uint32_t dst_ip;
    uint16_t src_port;
    uint16_t dst_port;
    const uint8_t *payload;
    size_t payload_len;
} epic_roce_packet_t;

typedef struct {
    int rank;
    char dev[16];
    uint8_t host_mac[6];
    uint8_t switch_mac[6];
    uint32_t host_ip;
    uint32_t switch_ip;
    uint32_t host_qpn;
    uint32_t switch_qpn;
} epic_port_t;

typedef struct {
    epic_primitive_t primitive;
    epic_mode_t mode;
    int root_rank;
    int count;
    int payload_bytes;
    epic_port_t ports[EPIC_STAR4_HOSTS];
} epic_switch_config_t;

const char *epic_primitive_name(epic_primitive_t primitive);
bool epic_parse_primitive(const char *text, epic_primitive_t *primitive);
bool epic_parse_mode(const char *text, epic_mode_t *mode);
bool epic_parse_ipv4(const char *text, uint32_t *addr);
bool epic_parse_mac(const char *text, uint8_t mac[6]);
const char *epic_ipv4_to_str(uint32_t addr, char *buf, size_t len);
void epic_mac_to_str(const uint8_t mac[6], char *buf, size_t len);

void epic_crc32_init(void);
uint16_t epic_ipv4_checksum(const void *data, size_t len);
uint32_t epic_compute_icrc(const uint8_t *frame, size_t frame_len);

bool epic_parse_roce(const uint8_t *frame, size_t frame_len, epic_roce_packet_t *out);

size_t epic_build_roce_data(uint8_t *frame, size_t frame_cap,
                            const uint8_t src_mac[6], const uint8_t dst_mac[6],
                            uint32_t src_ip, uint32_t dst_ip,
                            uint16_t src_port, uint16_t dst_port,
                            uint32_t dst_qpn, uint32_t psn,
                            const int32_t *payload_host_order,
                            size_t payload_bytes);

size_t epic_build_roce_data_raw(uint8_t *frame, size_t frame_cap,
                                const uint8_t src_mac[6], const uint8_t dst_mac[6],
                                uint32_t src_ip, uint32_t dst_ip,
                                uint16_t src_port, uint16_t dst_port,
                                uint32_t dst_qpn, uint32_t psn,
                                const uint8_t *payload,
                                size_t payload_bytes);

size_t epic_build_roce_ack(uint8_t *frame, size_t frame_cap,
                           const uint8_t src_mac[6], const uint8_t dst_mac[6],
                           uint32_t src_ip, uint32_t dst_ip,
                           uint16_t src_port, uint16_t dst_port,
                           uint32_t dst_qpn, uint32_t psn);

#endif
