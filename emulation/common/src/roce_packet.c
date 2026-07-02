#include "epic_clean.h"

#include <arpa/inet.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define ETHERTYPE_IPV4 0x0800
#define ROCE_OPCODE_SEND_ONLY 0x04
#define ROCE_OPCODE_ACK 0x11

static uint32_t crc32_table[8][256];
static bool crc32_ready;

const char *epic_primitive_name(epic_primitive_t primitive) {
    switch (primitive) {
    case EPIC_PRIM_ALLREDUCE:
        return "allreduce";
    case EPIC_PRIM_REDUCE:
        return "reduce";
    case EPIC_PRIM_BCAST:
        return "broadcast";
    case EPIC_PRIM_BARRIER:
        return "barrier";
    case EPIC_PRIM_ALLGATHER:
        return "allgather";
    case EPIC_PRIM_REDUCESCATTER:
        return "reducescatter";
    }
    return "unknown";
}

bool epic_parse_primitive(const char *text, epic_primitive_t *primitive) {
    if (!text || !primitive) {
        return false;
    }
    if (strcmp(text, "allreduce") == 0) {
        *primitive = EPIC_PRIM_ALLREDUCE;
        return true;
    }
    if (strcmp(text, "reduce") == 0) {
        *primitive = EPIC_PRIM_REDUCE;
        return true;
    }
    if (strcmp(text, "broadcast") == 0 || strcmp(text, "bcast") == 0) {
        *primitive = EPIC_PRIM_BCAST;
        return true;
    }
    if (strcmp(text, "barrier") == 0) {
        *primitive = EPIC_PRIM_BARRIER;
        return true;
    }
    if (strcmp(text, "allgather") == 0) {
        *primitive = EPIC_PRIM_ALLGATHER;
        return true;
    }
    if (strcmp(text, "reducescatter") == 0 ||
        strcmp(text, "reduce_scatter") == 0 ||
        strcmp(text, "reduce-scatter") == 0) {
        *primitive = EPIC_PRIM_REDUCESCATTER;
        return true;
    }
    return false;
}

bool epic_parse_mode(const char *text, epic_mode_t *mode) {
    if (!text || !mode) {
        return false;
    }
    if (strcmp(text, "2") == 0 || strcmp(text, "nontermination") == 0 ||
        strcmp(text, "non-termination") == 0 || strcmp(text, "mode2") == 0) {
        *mode = EPIC_MODE_NON_TERMINATION;
        return true;
    }
    if (strcmp(text, "3") == 0 || strcmp(text, "termination") == 0 ||
        strcmp(text, "mode3") == 0) {
        *mode = EPIC_MODE_TERMINATION;
        return true;
    }
    return false;
}

bool epic_parse_ipv4(const char *text, uint32_t *addr) {
    struct in_addr tmp;
    if (!text || !addr || inet_pton(AF_INET, text, &tmp) != 1) {
        return false;
    }
    *addr = tmp.s_addr;
    return true;
}

bool epic_parse_mac(const char *text, uint8_t mac[6]) {
    unsigned int b[6];
    if (!text || !mac) {
        return false;
    }
    if (sscanf(text, "%x:%x:%x:%x:%x:%x", &b[0], &b[1], &b[2], &b[3], &b[4],
               &b[5]) != 6) {
        return false;
    }
    for (int i = 0; i < 6; i++) {
        if (b[i] > 0xff) {
            return false;
        }
        mac[i] = (uint8_t)b[i];
    }
    return true;
}

const char *epic_ipv4_to_str(uint32_t addr, char *buf, size_t len) {
    struct in_addr tmp = {.s_addr = addr};
    return inet_ntop(AF_INET, &tmp, buf, len);
}

void epic_mac_to_str(const uint8_t mac[6], char *buf, size_t len) {
    snprintf(buf, len, "%02x:%02x:%02x:%02x:%02x:%02x", mac[0], mac[1],
             mac[2], mac[3], mac[4], mac[5]);
}

void epic_crc32_init(void) {
    if (crc32_ready) {
        return;
    }

    const uint32_t poly = 0xedb88320u;
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t crc = i;
        for (int j = 0; j < 8; j++) {
            crc = (crc >> 1) ^ ((crc & 1u) ? poly : 0u);
        }
        crc32_table[0][i] = crc;
    }

    for (int t = 1; t < 8; t++) {
        for (int i = 0; i < 256; i++) {
            crc32_table[t][i] =
                (crc32_table[t - 1][i] >> 8) ^
                crc32_table[0][crc32_table[t - 1][i] & 0xffu];
        }
    }
    crc32_ready = true;
}

static uint32_t epic_crc32(const void *data, size_t len) {
    epic_crc32_init();
    const uint8_t *buf = data;
    uint32_t crc = 0xffffffffu;

    while (len >= 8) {
        uint8_t b0 = buf[0] ^ (crc & 0xffu);
        uint8_t b1 = buf[1] ^ ((crc >> 8) & 0xffu);
        uint8_t b2 = buf[2] ^ ((crc >> 16) & 0xffu);
        uint8_t b3 = buf[3] ^ ((crc >> 24) & 0xffu);
        uint8_t b4 = buf[4];
        uint8_t b5 = buf[5];
        uint8_t b6 = buf[6];
        uint8_t b7 = buf[7];

        crc = crc32_table[0][b7] ^ crc32_table[1][b6] ^
              crc32_table[2][b5] ^ crc32_table[3][b4] ^
              crc32_table[4][b3] ^ crc32_table[5][b2] ^
              crc32_table[6][b1] ^ crc32_table[7][b0];

        buf += 8;
        len -= 8;
    }

    while (len--) {
        crc = (crc >> 8) ^ crc32_table[0][(crc ^ *buf++) & 0xffu];
    }
    return crc ^ 0xffffffffu;
}

uint16_t epic_ipv4_checksum(const void *data, size_t len) {
    const uint8_t *bytes = data;
    uint32_t sum = 0;

    while (len > 1) {
        sum += (uint16_t)((bytes[0] << 8) | bytes[1]);
        bytes += 2;
        len -= 2;
    }
    if (len) {
        sum += (uint16_t)(bytes[0] << 8);
    }
    while (sum >> 16) {
        sum = (sum & 0xffffu) + (sum >> 16);
    }
    return htons((uint16_t)~sum);
}

uint32_t epic_compute_icrc(const uint8_t *frame, size_t frame_len) {
    if (frame_len < sizeof(epic_eth_t) + sizeof(epic_ipv4_t) + 4) {
        return 0;
    }

    const epic_ipv4_t *ip = (const epic_ipv4_t *)(frame + sizeof(epic_eth_t));
    size_t ip_len = ntohs(ip->total_length);
    if (sizeof(epic_eth_t) + ip_len > frame_len || ip_len < 4) {
        return 0;
    }

    size_t crc_len = 8 + ip_len - 4;
    uint8_t buf[8 + sizeof(epic_ipv4_t) + sizeof(epic_udp_t) +
                sizeof(epic_bth_t) + EPIC_MAX_PAYLOAD_BYTES];
    if (crc_len > sizeof(buf)) {
        return 0;
    }

    memset(buf, 0xff, 8);
    memcpy(buf + 8, frame + sizeof(epic_eth_t), ip_len - 4);

    epic_ipv4_t *cip = (epic_ipv4_t *)(buf + 8);
    size_t ihl = (cip->version_ihl & 0x0f) * 4u;
    if (ihl < sizeof(epic_ipv4_t) || ip_len < ihl + sizeof(epic_udp_t) + sizeof(epic_bth_t)) {
        return 0;
    }
    epic_udp_t *udp = (epic_udp_t *)(buf + 8 + ihl);
    epic_bth_t *bth = (epic_bth_t *)((uint8_t *)udp + sizeof(*udp));

    cip->tos = 0xff;
    cip->ttl = 0xff;
    cip->checksum = 0xffff;
    udp->checksum = 0xffff;
    ((uint8_t *)&bth->qpn)[0] = 0xff;

    uint32_t crc = epic_crc32(buf, crc_len);
    return crc;
}

bool epic_parse_roce(const uint8_t *frame, size_t frame_len, epic_roce_packet_t *out) {
    if (!frame || !out) {
        return false;
    }
    memset(out, 0, sizeof(*out));

    if (frame_len < sizeof(epic_eth_t) + sizeof(epic_ipv4_t) + sizeof(epic_udp_t) +
                        sizeof(epic_bth_t) + 4) {
        return false;
    }

    const epic_eth_t *eth = (const epic_eth_t *)frame;
    if (ntohs(eth->ether_type) != ETHERTYPE_IPV4) {
        return false;
    }

    const epic_ipv4_t *ip = (const epic_ipv4_t *)(frame + sizeof(*eth));
    size_t ihl = (ip->version_ihl & 0x0f) * 4u;
    if ((ip->version_ihl >> 4) != 4 || ihl < sizeof(epic_ipv4_t) ||
        ip->protocol != IPPROTO_UDP) {
        return false;
    }

    size_t ip_total = ntohs(ip->total_length);
    if (sizeof(*eth) + ip_total > frame_len ||
        ip_total < ihl + sizeof(epic_udp_t) + sizeof(epic_bth_t) + 4) {
        return false;
    }

    const epic_udp_t *udp = (const epic_udp_t *)(frame + sizeof(*eth) + ihl);
    uint16_t udp_len = ntohs(udp->length);
    if (udp_len < sizeof(*udp) + sizeof(epic_bth_t) + 4 ||
        sizeof(*eth) + ihl + udp_len > frame_len ||
        (ntohs(udp->dst_port) != EPIC_ROCE_UDP_PORT &&
         ntohs(udp->src_port) != EPIC_ROCE_UDP_PORT)) {
        return false;
    }

    const epic_bth_t *bth = (const epic_bth_t *)((const uint8_t *)udp + sizeof(*udp));
    size_t bth_off = sizeof(*eth) + ihl + sizeof(*udp);
    size_t after_bth = bth_off + sizeof(*bth);
    size_t roce_tail = sizeof(*udp) + sizeof(*bth) + 4;

    out->valid = true;
    out->opcode = bth->opcode;
    out->dst_qpn = ntohl(bth->qpn) & 0x00ffffffu;
    out->psn = ntohl(bth->apsn) & 0x00ffffffu;
    out->src_ip = ip->src_ip;
    out->dst_ip = ip->dst_ip;
    out->src_port = ntohs(udp->src_port);
    out->dst_port = ntohs(udp->dst_port);

    if (bth->opcode == ROCE_OPCODE_ACK) {
        if (udp_len < sizeof(*udp) + sizeof(*bth) + sizeof(epic_aeth_t) + 4) {
            return false;
        }
        const epic_aeth_t *aeth = (const epic_aeth_t *)(frame + after_bth);
        out->is_ack = true;
        out->msn = ntohl(aeth->syn_msn) & 0x00ffffffu;
        return true;
    }

    if (bth->opcode == ROCE_OPCODE_SEND_ONLY || bth->opcode == 0x00 ||
        bth->opcode == 0x01 || bth->opcode == 0x02 || bth->opcode == 0x03 ||
        bth->opcode == 0x05) {
        out->is_data = true;
        if (udp_len < roce_tail) {
            return false;
        }
        out->payload = frame + after_bth;
        out->payload_len = udp_len - roce_tail;
        return true;
    }

    return true;
}

static size_t build_roce_base(uint8_t *frame, size_t frame_cap,
                              const uint8_t src_mac[6], const uint8_t dst_mac[6],
                              uint32_t src_ip, uint32_t dst_ip,
                              uint16_t src_port, uint16_t dst_port,
                              uint32_t dst_qpn, uint32_t psn,
                              uint8_t opcode, size_t extra_len,
                              bool ack_request) {
    size_t total_len = sizeof(epic_eth_t) + sizeof(epic_ipv4_t) +
                       sizeof(epic_udp_t) + sizeof(epic_bth_t) + extra_len + 4;
    if (frame_cap < total_len) {
        return 0;
    }

    memset(frame, 0, total_len);

    epic_eth_t *eth = (epic_eth_t *)frame;
    memcpy(eth->src_mac, src_mac, 6);
    memcpy(eth->dst_mac, dst_mac, 6);
    eth->ether_type = htons(ETHERTYPE_IPV4);

    epic_ipv4_t *ip = (epic_ipv4_t *)(frame + sizeof(*eth));
    ip->version_ihl = 0x45;
    ip->tos = 0;
    ip->total_length = htons((uint16_t)(total_len - sizeof(*eth)));
    ip->id = htons(0x1111);
    ip->flags_frag_off = htons(0x4000);
    ip->ttl = 64;
    ip->protocol = IPPROTO_UDP;
    ip->src_ip = src_ip;
    ip->dst_ip = dst_ip;
    ip->checksum = 0;
    ip->checksum = epic_ipv4_checksum(ip, sizeof(*ip));

    epic_udp_t *udp = (epic_udp_t *)((uint8_t *)ip + sizeof(*ip));
    udp->src_port = htons(src_port);
    udp->dst_port = htons(dst_port);
    udp->length = htons((uint16_t)(total_len - sizeof(*eth) - sizeof(*ip)));
    udp->checksum = 0;

    epic_bth_t *bth = (epic_bth_t *)((uint8_t *)udp + sizeof(*udp));
    bth->opcode = opcode;
    bth->se_m_pad = 0;
    bth->pkey = htons(0xffff);
    bth->qpn = htonl(dst_qpn & 0x00ffffffu);
    bth->apsn = htonl((psn & 0x00ffffffu) | (ack_request ? 0x80000000u : 0));

    return total_len;
}

size_t epic_build_roce_data(uint8_t *frame, size_t frame_cap,
                            const uint8_t src_mac[6], const uint8_t dst_mac[6],
                            uint32_t src_ip, uint32_t dst_ip,
                            uint16_t src_port, uint16_t dst_port,
                            uint32_t dst_qpn, uint32_t psn,
                            const int32_t *payload_host_order,
                            size_t payload_bytes) {
    if ((payload_bytes > 0 && !payload_host_order) ||
        payload_bytes % sizeof(uint32_t) != 0 ||
        payload_bytes > EPIC_MAX_PAYLOAD_BYTES) {
        return 0;
    }

    size_t total_len = build_roce_base(frame, frame_cap, src_mac, dst_mac, src_ip,
                                      dst_ip, src_port, dst_port, dst_qpn, psn,
                                      ROCE_OPCODE_SEND_ONLY, payload_bytes, true);
    if (!total_len) {
        return 0;
    }

    size_t payload_off = sizeof(epic_eth_t) + sizeof(epic_ipv4_t) +
                         sizeof(epic_udp_t) + sizeof(epic_bth_t);
    uint32_t *dst = (uint32_t *)(frame + payload_off);
    for (size_t i = 0; i < payload_bytes / sizeof(uint32_t); i++) {
        dst[i] = htonl((uint32_t)payload_host_order[i]);
    }

    uint32_t icrc = epic_compute_icrc(frame, total_len);
    memcpy(frame + total_len - 4, &icrc, sizeof(icrc));
    return total_len;
}

size_t epic_build_roce_data_raw(uint8_t *frame, size_t frame_cap,
                                const uint8_t src_mac[6], const uint8_t dst_mac[6],
                                uint32_t src_ip, uint32_t dst_ip,
                                uint16_t src_port, uint16_t dst_port,
                                uint32_t dst_qpn, uint32_t psn,
                                const uint8_t *payload,
                                size_t payload_bytes) {
    if ((payload_bytes > 0 && !payload) ||
        payload_bytes > EPIC_MAX_PAYLOAD_BYTES) {
        return 0;
    }

    size_t total_len = build_roce_base(frame, frame_cap, src_mac, dst_mac, src_ip,
                                      dst_ip, src_port, dst_port, dst_qpn, psn,
                                      ROCE_OPCODE_SEND_ONLY, payload_bytes, true);
    if (!total_len) {
        return 0;
    }

    size_t payload_off = sizeof(epic_eth_t) + sizeof(epic_ipv4_t) +
                         sizeof(epic_udp_t) + sizeof(epic_bth_t);
    if (payload_bytes > 0) {
        memcpy(frame + payload_off, payload, payload_bytes);
    }

    uint32_t icrc = epic_compute_icrc(frame, total_len);
    memcpy(frame + total_len - 4, &icrc, sizeof(icrc));
    return total_len;
}

size_t epic_build_roce_ack(uint8_t *frame, size_t frame_cap,
                           const uint8_t src_mac[6], const uint8_t dst_mac[6],
                           uint32_t src_ip, uint32_t dst_ip,
                           uint16_t src_port, uint16_t dst_port,
                           uint32_t dst_qpn, uint32_t psn) {
    size_t total_len = build_roce_base(frame, frame_cap, src_mac, dst_mac, src_ip,
                                      dst_ip, src_port, dst_port, dst_qpn, psn,
                                      ROCE_OPCODE_ACK, sizeof(epic_aeth_t), false);
    if (!total_len) {
        return 0;
    }

    size_t aeth_off = sizeof(epic_eth_t) + sizeof(epic_ipv4_t) +
                      sizeof(epic_udp_t) + sizeof(epic_bth_t);
    epic_aeth_t *aeth = (epic_aeth_t *)(frame + aeth_off);
    aeth->syn_msn = htonl(((psn + 1) & 0x00ffffffu) | 0x1f000000u);

    uint32_t icrc = epic_compute_icrc(frame, total_len);
    memcpy(frame + total_len - 4, &icrc, sizeof(icrc));
    return total_len;
}
