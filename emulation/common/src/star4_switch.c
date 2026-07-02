#define _DEFAULT_SOURCE

#include "epic_clean.h"

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <pcap.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <time.h>
#include <unistd.h>

#define MAX_EVENTS EPIC_STAR4_HOSTS
#define DEFAULT_RX_BURST_PER_PORT 32
#define PCAP_BUFFER_BYTES (64 * 1024 * 1024)
#define PCAP_SEND_RETRIES 100000
#define PCAP_SEND_RETRY_US 10
#define SLOT_WORDS (EPIC_MAX_PAYLOAD_BYTES / sizeof(int32_t))
#define PORT_MASK(rank) (1u << (rank))
#define ALL_HOST_MASK ((1u << EPIC_STAR4_HOSTS) - 1u)

typedef struct {
    bool in_use;
    uint32_t psn;
    uint8_t data_mask;
    uint8_t down_ack_mask;
    uint8_t upload_ack_sent_mask;
    bool result_ready;
    bool result_sent[EPIC_STAR4_HOSTS];
    uint8_t allgather_sent_mask[EPIC_STAR4_HOSTS];
    uint8_t allgather_down_ack_mask[EPIC_STAR4_HOSTS];
    uint8_t *allgather_raw[EPIC_STAR4_HOSTS];
    union {
        int32_t accum[SLOT_WORDS];
        uint8_t bcast_raw[EPIC_MAX_PAYLOAD_BYTES];
    } data;
} slot_state_t;

typedef struct {
    epic_switch_config_t cfg;
    pcap_t *handles[EPIC_STAR4_HOSTS];
    int epoll_fd;
    slot_state_t slots[EPIC_PSN_WINDOW];
    uint32_t next_result_psn[EPIC_STAR4_HOSTS];
    uint32_t next_reduce_ack_psn;
    uint64_t rx_data;
    uint64_t rx_ack;
    uint64_t tx_data;
    uint64_t tx_ack;
    uint64_t slot_overwrites;
    int rx_burst_per_port;
    bool profile;
    bool trace;
    uint64_t prof_rx_packets;
    uint64_t prof_parse_fail;
    uint64_t prof_parse_ns;
    uint64_t prof_handle_ns;
    uint64_t prof_build_ns;
    uint64_t prof_send_ns;
} switch_ctx_t;

static volatile sig_atomic_t running = 1;

static void on_signal(int signo) {
    running = 0;
}

static uint32_t slot_idx(uint32_t psn) {
    return psn % EPIC_PSN_WINDOW;
}

static uint64_t nsec_now(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static void cleanup_slot(slot_state_t *slot) {
    for (int r = 0; r < EPIC_STAR4_HOSTS; r++) {
        free(slot->allgather_raw[r]);
        slot->allgather_raw[r] = NULL;
    }
}

static double avg_ns(uint64_t total, uint64_t count) {
    return count ? (double)total / (double)count : 0.0;
}

static int cfg_total_segments(const switch_ctx_t *ctx) {
    if (ctx->cfg.primitive == EPIC_PRIM_BARRIER) {
        return 1;
    }
    return (int)(((size_t)ctx->cfg.count * sizeof(int32_t)) /
                 (size_t)ctx->cfg.payload_bytes);
}

static int cfg_block_segments(const switch_ctx_t *ctx) {
    int total = cfg_total_segments(ctx);
    return total / EPIC_STAR4_HOSTS;
}

static uint32_t logical_segment(const switch_ctx_t *ctx, uint32_t psn) {
    return psn % (uint32_t)cfg_total_segments(ctx);
}

static uint32_t iter_base(const switch_ctx_t *ctx, uint32_t psn) {
    return psn / (uint32_t)cfg_total_segments(ctx);
}

static int rank_from_ordinal_excluding(int excluded_rank, int ordinal) {
    for (int r = 0; r < EPIC_STAR4_HOSTS; r++) {
        if (r == excluded_rank) {
            continue;
        }
        if (ordinal == 0) {
            return r;
        }
        ordinal--;
    }
    return 0;
}

static int ordinal_excluding(int excluded_rank, int rank) {
    int ordinal = 0;
    for (int r = 0; r < EPIC_STAR4_HOSTS; r++) {
        if (r == excluded_rank) {
            continue;
        }
        if (r == rank) {
            return ordinal;
        }
        ordinal++;
    }
    return 0;
}

static int reducescatter_dest_for_psn(const switch_ctx_t *ctx, uint32_t psn) {
    int block_segments = cfg_block_segments(ctx);
    return (int)(logical_segment(ctx, psn) / (uint32_t)block_segments);
}

static uint32_t reducescatter_out_psn(const switch_ctx_t *ctx, uint32_t psn) {
    int block_segments = cfg_block_segments(ctx);
    uint32_t local = logical_segment(ctx, psn) % (uint32_t)block_segments;
    return iter_base(ctx, psn) * (uint32_t)block_segments + local;
}

static uint32_t reducescatter_ack_to_data_psn(const switch_ctx_t *ctx, int rank,
                                              uint32_t ack_psn) {
    int block_segments = cfg_block_segments(ctx);
    uint32_t iter = ack_psn / (uint32_t)block_segments;
    uint32_t local = ack_psn % (uint32_t)block_segments;
    return iter * (uint32_t)cfg_total_segments(ctx) +
           (uint32_t)rank * (uint32_t)block_segments + local;
}

static uint32_t allgather_out_psn(const switch_ctx_t *ctx, uint32_t data_psn,
                                  int dest, int src) {
    int block_segments = cfg_block_segments(ctx);
    uint32_t local = data_psn % (uint32_t)block_segments;
    uint32_t iter = data_psn / (uint32_t)block_segments;
    uint32_t ordinal = (uint32_t)ordinal_excluding(dest, src);
    return iter * (uint32_t)(block_segments * (EPIC_STAR4_HOSTS - 1)) +
           local * (uint32_t)(EPIC_STAR4_HOSTS - 1) + ordinal;
}

static uint32_t allgather_ack_to_data_psn(const switch_ctx_t *ctx, int dest,
                                          uint32_t ack_psn, int *src_out) {
    int block_segments = cfg_block_segments(ctx);
    int peers = EPIC_STAR4_HOSTS - 1;
    uint32_t per_iter = (uint32_t)(block_segments * peers);
    uint32_t iter = ack_psn / per_iter;
    uint32_t rem = ack_psn % per_iter;
    uint32_t local = rem / (uint32_t)peers;
    int ordinal = (int)(rem % (uint32_t)peers);
    int src = rank_from_ordinal_excluding(dest, ordinal);
    *src_out = src;
    return iter * (uint32_t)block_segments + local;
}

static bool pcap_send_with_retry(pcap_t *handle, const uint8_t *frame, int len) {
    for (int attempt = 0; attempt < PCAP_SEND_RETRIES; attempt++) {
        if (pcap_sendpacket(handle, frame, len) == 0) {
            return true;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            return false;
        }
        usleep(PCAP_SEND_RETRY_US);
    }
    return false;
}

static void trace_event(switch_ctx_t *ctx, const char *event, int rank,
                        uint32_t psn, const char *extra) {
    if (!ctx->trace) {
        return;
    }
    printf("SWITCH_TRACE t_ns=%" PRIu64 " event=%s rank=%d psn=%u%s%s\n",
           nsec_now(), event, rank, psn, extra ? " " : "", extra ? extra : "");
}

static void reset_slot(slot_state_t *slot, uint32_t psn) {
    cleanup_slot(slot);
    memset(slot, 0, sizeof(*slot));
    slot->in_use = true;
    slot->psn = psn;
}

static slot_state_t *get_slot(switch_ctx_t *ctx, uint32_t psn) {
    slot_state_t *slot = &ctx->slots[slot_idx(psn)];
    if (!slot->in_use || slot->psn != psn) {
        if (slot->in_use && slot->psn != psn) {
            ctx->slot_overwrites++;
        }
        reset_slot(slot, psn);
    }
    return slot;
}

static char *trim(char *s) {
    while (isspace((unsigned char)*s)) {
        s++;
    }
    if (*s == '\0') {
        return s;
    }
    char *end = s + strlen(s) - 1;
    while (end > s && isspace((unsigned char)*end)) {
        *end-- = '\0';
    }
    return s;
}

static bool parse_int_value(const char *value, int *out) {
    char *end = NULL;
    errno = 0;
    long v = strtol(value, &end, 0);
    if (errno || !end || *end != '\0') {
        return false;
    }
    *out = (int)v;
    return true;
}

static bool parse_u32_value(const char *value, uint32_t *out) {
    char *end = NULL;
    errno = 0;
    unsigned long v = strtoul(value, &end, 0);
    if (errno || !end || *end != '\0' || v > 0xfffffffful) {
        return false;
    }
    *out = (uint32_t)v;
    return true;
}

static bool parse_port_key(epic_switch_config_t *cfg, const char *key,
                           const char *value) {
    int rank = -1;
    char field[64];
    if (sscanf(key, "port%d.%63s", &rank, field) != 2 ||
        rank < 0 || rank >= EPIC_STAR4_HOSTS) {
        return false;
    }

    epic_port_t *port = &cfg->ports[rank];
    port->rank = rank;
    if (strcmp(field, "dev") == 0) {
        snprintf(port->dev, sizeof(port->dev), "%s", value);
        return true;
    }
    if (strcmp(field, "host_ip") == 0) {
        return epic_parse_ipv4(value, &port->host_ip);
    }
    if (strcmp(field, "switch_ip") == 0) {
        return epic_parse_ipv4(value, &port->switch_ip);
    }
    if (strcmp(field, "host_mac") == 0) {
        return epic_parse_mac(value, port->host_mac);
    }
    if (strcmp(field, "switch_mac") == 0) {
        return epic_parse_mac(value, port->switch_mac);
    }
    if (strcmp(field, "host_qpn") == 0) {
        return parse_u32_value(value, &port->host_qpn);
    }
    if (strcmp(field, "switch_qpn") == 0) {
        return parse_u32_value(value, &port->switch_qpn);
    }
    return false;
}

static bool load_config(const char *path, epic_switch_config_t *cfg) {
    memset(cfg, 0, sizeof(*cfg));
    cfg->primitive = EPIC_PRIM_ALLREDUCE;
    cfg->mode = EPIC_MODE_NON_TERMINATION;
    cfg->root_rank = 0;
    cfg->count = EPIC_DEFAULT_COUNT;
    cfg->payload_bytes = EPIC_DEFAULT_PAYLOAD_BYTES;

    FILE *f = fopen(path, "r");
    if (!f) {
        perror("fopen config");
        return false;
    }

    char line[512];
    int lineno = 0;
    while (fgets(line, sizeof(line), f)) {
        lineno++;
        char *s = trim(line);
        if (*s == '\0' || *s == '#') {
            continue;
        }
        char *eq = strchr(s, '=');
        if (!eq) {
            fprintf(stderr, "%s:%d: expected key=value\n", path, lineno);
            fclose(f);
            return false;
        }
        *eq = '\0';
        char *key = trim(s);
        char *value = trim(eq + 1);

        bool ok = true;
        if (strcmp(key, "primitive") == 0) {
            ok = epic_parse_primitive(value, &cfg->primitive);
        } else if (strcmp(key, "mode") == 0) {
            ok = epic_parse_mode(value, &cfg->mode);
        } else if (strcmp(key, "root") == 0) {
            ok = parse_int_value(value, &cfg->root_rank);
        } else if (strcmp(key, "count") == 0) {
            ok = parse_int_value(value, &cfg->count);
        } else if (strcmp(key, "payload_bytes") == 0) {
            ok = parse_int_value(value, &cfg->payload_bytes);
        } else if (strncmp(key, "port", 4) == 0) {
            ok = parse_port_key(cfg, key, value);
        } else {
            fprintf(stderr, "%s:%d: unknown key %s\n", path, lineno, key);
            ok = false;
        }

        if (!ok) {
            fprintf(stderr, "%s:%d: invalid value for %s: %s\n", path, lineno,
                    key, value);
            fclose(f);
            return false;
        }
    }
    fclose(f);

    if (cfg->root_rank < 0 || cfg->root_rank >= EPIC_STAR4_HOSTS ||
        ((cfg->count <= 0 && cfg->primitive != EPIC_PRIM_BARRIER) ||
         cfg->count < 0) ||
        ((cfg->payload_bytes <= 0 && cfg->primitive != EPIC_PRIM_BARRIER) ||
         cfg->payload_bytes < 0) ||
        cfg->payload_bytes > EPIC_MAX_PAYLOAD_BYTES ||
        cfg->payload_bytes % (int)sizeof(uint32_t) != 0) {
        fprintf(stderr, "invalid root/payload config\n");
        return false;
    }
    if (cfg->primitive != EPIC_PRIM_BARRIER &&
        ((size_t)cfg->count * sizeof(int32_t)) % (size_t)cfg->payload_bytes != 0) {
        fprintf(stderr, "count*sizeof(int32_t) must be divisible by payload_bytes\n");
        return false;
    }
    if ((cfg->primitive == EPIC_PRIM_ALLGATHER ||
         cfg->primitive == EPIC_PRIM_REDUCESCATTER) &&
        (cfg->count % EPIC_STAR4_HOSTS != 0 ||
         ((size_t)cfg->count * sizeof(int32_t) / EPIC_STAR4_HOSTS) %
             (size_t)cfg->payload_bytes != 0)) {
        fprintf(stderr,
                "allgather/reducescatter count must be divisible by hosts and each block by payload_bytes\n");
        return false;
    }

    for (int i = 0; i < EPIC_STAR4_HOSTS; i++) {
        epic_port_t *p = &cfg->ports[i];
        if (!p->dev[0] || p->host_ip == 0 || p->switch_ip == 0 ||
            p->host_qpn == 0 || p->switch_qpn == 0) {
            fprintf(stderr, "port%d missing required config\n", i);
            return false;
        }
    }
    return true;
}

static bool setup_pcap(switch_ctx_t *ctx) {
    char errbuf[PCAP_ERRBUF_SIZE];
    ctx->epoll_fd = epoll_create1(0);
    if (ctx->epoll_fd < 0) {
        perror("epoll_create1");
        return false;
    }

    for (int i = 0; i < EPIC_STAR4_HOSTS; i++) {
        epic_port_t *port = &ctx->cfg.ports[i];
        pcap_t *h = pcap_create(port->dev, errbuf);
        if (!h) {
            fprintf(stderr, "pcap_create(%s): %s\n", port->dev, errbuf);
            return false;
        }
        pcap_set_snaplen(h, 8192);
        pcap_set_buffer_size(h, PCAP_BUFFER_BYTES);
        pcap_set_promisc(h, 1);
        pcap_set_timeout(h, 1);
        pcap_set_immediate_mode(h, 1);
        if (pcap_activate(h) != 0) {
            fprintf(stderr, "pcap_activate(%s): %s\n", port->dev, pcap_geterr(h));
            return false;
        }
        if (pcap_setdirection(h, PCAP_D_IN) != 0) {
            fprintf(stderr, "warning: pcap_setdirection(%s, IN): %s\n", port->dev,
                    pcap_geterr(h));
        }
        if (pcap_setnonblock(h, 1, errbuf) != 0) {
            fprintf(stderr, "pcap_setnonblock(%s): %s\n", port->dev, errbuf);
            return false;
        }

        char ip[INET_ADDRSTRLEN];
        epic_ipv4_to_str(port->host_ip, ip, sizeof(ip));
        char filter[128];
        snprintf(filter, sizeof(filter), "udp port %d and src host %s",
                 EPIC_ROCE_UDP_PORT, ip);
        struct bpf_program fp;
        if (pcap_compile(h, &fp, filter, 0, PCAP_NETMASK_UNKNOWN) == -1) {
            fprintf(stderr, "pcap_compile(%s): %s\n", port->dev, pcap_geterr(h));
            return false;
        }
        if (pcap_setfilter(h, &fp) == -1) {
            fprintf(stderr, "pcap_setfilter(%s): %s\n", port->dev, pcap_geterr(h));
            pcap_freecode(&fp);
            return false;
        }
        pcap_freecode(&fp);

        int fd = pcap_get_selectable_fd(h);
        if (fd < 0) {
            fprintf(stderr, "pcap_get_selectable_fd(%s) failed\n", port->dev);
            return false;
        }
        struct epoll_event ev;
        memset(&ev, 0, sizeof(ev));
        ev.events = EPOLLIN;
        ev.data.u32 = (uint32_t)i;
        if (epoll_ctl(ctx->epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
            perror("epoll_ctl");
            return false;
        }
        ctx->handles[i] = h;
    }
    return true;
}

static bool send_data_to_rank(switch_ctx_t *ctx, int rank, uint32_t psn,
                              const int32_t *payload) {
    epic_port_t *p = &ctx->cfg.ports[rank];
    uint8_t frame[sizeof(epic_eth_t) + sizeof(epic_ipv4_t) + sizeof(epic_udp_t) +
                  sizeof(epic_bth_t) + EPIC_MAX_PAYLOAD_BYTES + 4];
    uint64_t build0 = ctx->profile ? nsec_now() : 0;
    size_t len = epic_build_roce_data(frame, sizeof(frame), p->switch_mac,
                                      p->host_mac, p->switch_ip, p->host_ip,
                                      EPIC_ROCE_UDP_PORT, EPIC_ROCE_UDP_PORT,
                                      p->host_qpn, psn, payload,
                                      (size_t)ctx->cfg.payload_bytes);
    uint64_t build1 = ctx->profile ? nsec_now() : 0;
    if (ctx->profile) {
        ctx->prof_build_ns += build1 - build0;
    }
    if (!len) {
        return false;
    }
    uint64_t send0 = ctx->profile ? nsec_now() : 0;
    if (!pcap_send_with_retry(ctx->handles[rank], frame, (int)len)) {
        fprintf(stderr, "pcap_sendpacket data rank=%d: %s\n", rank,
                pcap_geterr(ctx->handles[rank]));
        return false;
    }
    uint64_t send1 = ctx->profile ? nsec_now() : 0;
    if (ctx->profile) {
        ctx->prof_send_ns += send1 - send0;
    }
    ctx->tx_data++;
    trace_event(ctx, "tx_data", rank, psn, NULL);
    return true;
}

static bool send_raw_to_rank(switch_ctx_t *ctx, int rank, uint32_t psn,
                             const uint8_t *payload, size_t payload_bytes) {
    epic_port_t *p = &ctx->cfg.ports[rank];
    uint8_t frame[sizeof(epic_eth_t) + sizeof(epic_ipv4_t) + sizeof(epic_udp_t) +
                  sizeof(epic_bth_t) + EPIC_MAX_PAYLOAD_BYTES + 4];
    uint64_t build0 = ctx->profile ? nsec_now() : 0;
    size_t len = epic_build_roce_data_raw(frame, sizeof(frame), p->switch_mac,
                                          p->host_mac, p->switch_ip, p->host_ip,
                                          EPIC_ROCE_UDP_PORT,
                                          EPIC_ROCE_UDP_PORT, p->host_qpn, psn,
                                          payload, payload_bytes);
    uint64_t build1 = ctx->profile ? nsec_now() : 0;
    if (ctx->profile) {
        ctx->prof_build_ns += build1 - build0;
    }
    if (!len) {
        return false;
    }
    uint64_t send0 = ctx->profile ? nsec_now() : 0;
    if (!pcap_send_with_retry(ctx->handles[rank], frame, (int)len)) {
        fprintf(stderr, "pcap_sendpacket raw data rank=%d: %s\n", rank,
                pcap_geterr(ctx->handles[rank]));
        return false;
    }
    uint64_t send1 = ctx->profile ? nsec_now() : 0;
    if (ctx->profile) {
        ctx->prof_send_ns += send1 - send0;
    }
    ctx->tx_data++;
    trace_event(ctx, "tx_data_raw", rank, psn, NULL);
    return true;
}

static bool send_empty_to_rank(switch_ctx_t *ctx, int rank, uint32_t psn) {
    epic_port_t *p = &ctx->cfg.ports[rank];
    uint8_t frame[sizeof(epic_eth_t) + sizeof(epic_ipv4_t) + sizeof(epic_udp_t) +
                  sizeof(epic_bth_t) + 4];
    uint64_t build0 = ctx->profile ? nsec_now() : 0;
    size_t len = epic_build_roce_data(frame, sizeof(frame), p->switch_mac,
                                      p->host_mac, p->switch_ip, p->host_ip,
                                      EPIC_ROCE_UDP_PORT, EPIC_ROCE_UDP_PORT,
                                      p->host_qpn, psn, NULL, 0);
    uint64_t build1 = ctx->profile ? nsec_now() : 0;
    if (ctx->profile) {
        ctx->prof_build_ns += build1 - build0;
    }
    if (!len) {
        return false;
    }
    uint64_t send0 = ctx->profile ? nsec_now() : 0;
    if (!pcap_send_with_retry(ctx->handles[rank], frame, (int)len)) {
        fprintf(stderr, "pcap_sendpacket barrier rank=%d: %s\n", rank,
                pcap_geterr(ctx->handles[rank]));
        return false;
    }
    uint64_t send1 = ctx->profile ? nsec_now() : 0;
    if (ctx->profile) {
        ctx->prof_send_ns += send1 - send0;
    }
    ctx->tx_data++;
    trace_event(ctx, "tx_barrier", rank, psn, NULL);
    return true;
}

static bool send_ack_to_rank(switch_ctx_t *ctx, int rank, uint32_t psn,
                             slot_state_t *slot) {
    epic_port_t *p = &ctx->cfg.ports[rank];
    uint8_t frame[sizeof(epic_eth_t) + sizeof(epic_ipv4_t) + sizeof(epic_udp_t) +
                  sizeof(epic_bth_t) + sizeof(epic_aeth_t) + 4];
    uint64_t build0 = ctx->profile ? nsec_now() : 0;
    size_t len = epic_build_roce_ack(frame, sizeof(frame), p->switch_mac,
                                    p->host_mac, p->switch_ip, p->host_ip,
                                    EPIC_ROCE_UDP_PORT, EPIC_ROCE_UDP_PORT,
                                    p->host_qpn, psn);
    uint64_t build1 = ctx->profile ? nsec_now() : 0;
    if (ctx->profile) {
        ctx->prof_build_ns += build1 - build0;
    }
    if (!len) {
        return false;
    }
    uint64_t send0 = ctx->profile ? nsec_now() : 0;
    if (!pcap_send_with_retry(ctx->handles[rank], frame, (int)len)) {
        fprintf(stderr, "pcap_sendpacket ack rank=%d: %s\n", rank,
                pcap_geterr(ctx->handles[rank]));
        return false;
    }
    uint64_t send1 = ctx->profile ? nsec_now() : 0;
    if (ctx->profile) {
        ctx->prof_send_ns += send1 - send0;
    }
    if (slot) {
        slot->upload_ack_sent_mask |= PORT_MASK(rank);
    }
    ctx->tx_ack++;
    trace_event(ctx, "tx_ack", rank, psn, NULL);
    return true;
}

static bool send_allreduce_result(switch_ctx_t *ctx, slot_state_t *slot,
                                  uint32_t psn, int only_rank) {
    if (only_rank >= 0) {
        if (!slot->result_sent[only_rank]) {
            slot->result_sent[only_rank] = true;
        }
        return send_data_to_rank(ctx, only_rank, psn, slot->data.accum);
    }

    for (int r = 0; r < EPIC_STAR4_HOSTS; r++) {
        if (!slot->result_sent[r]) {
            if (!send_data_to_rank(ctx, r, psn, slot->data.accum)) {
                return false;
            }
            slot->result_sent[r] = true;
        }
    }
    return true;
}

static bool send_barrier_release(switch_ctx_t *ctx, slot_state_t *slot,
                                 uint32_t psn, int only_rank) {
    if (only_rank >= 0) {
        if (!slot->result_sent[only_rank]) {
            slot->result_sent[only_rank] = true;
        }
        return send_empty_to_rank(ctx, only_rank, psn);
    }

    for (int r = 0; r < EPIC_STAR4_HOSTS; r++) {
        if (!slot->result_sent[r]) {
            if (!send_empty_to_rank(ctx, r, psn)) {
                return false;
            }
            slot->result_sent[r] = true;
        }
    }
    return true;
}

static bool send_reduce_result(switch_ctx_t *ctx, slot_state_t *slot,
                               uint32_t psn) {
    int root = ctx->cfg.root_rank;
    if (!slot->result_sent[root]) {
        if (!send_data_to_rank(ctx, root, psn, slot->data.accum)) {
            return false;
        }
        slot->result_sent[root] = true;
    }
    return true;
}

static bool send_reducescatter_result(switch_ctx_t *ctx, slot_state_t *slot,
                                      uint32_t psn) {
    int dest = reducescatter_dest_for_psn(ctx, psn);
    uint32_t out_psn = reducescatter_out_psn(ctx, psn);
    if (!slot->result_sent[dest]) {
        if (!send_data_to_rank(ctx, dest, out_psn, slot->data.accum)) {
            return false;
        }
        slot->result_sent[dest] = true;
    }
    return true;
}

static bool send_bcast_payload(switch_ctx_t *ctx, slot_state_t *slot,
                               uint32_t psn, int only_rank) {
    if (only_rank >= 0) {
        if (only_rank == ctx->cfg.root_rank) {
            return true;
        }
        slot->result_sent[only_rank] = true;
        return send_raw_to_rank(ctx, only_rank, psn, slot->data.bcast_raw,
                                (size_t)ctx->cfg.payload_bytes);
    }

    for (int r = 0; r < EPIC_STAR4_HOSTS; r++) {
        if (r == ctx->cfg.root_rank) {
            continue;
        }
        if (!slot->result_sent[r]) {
            if (!send_raw_to_rank(ctx, r, psn, slot->data.bcast_raw,
                                  (size_t)ctx->cfg.payload_bytes)) {
                return false;
            }
            slot->result_sent[r] = true;
        }
    }
    return true;
}

static bool send_allgather_payloads(switch_ctx_t *ctx, slot_state_t *slot,
                                    uint32_t psn, int dest) {
    for (int src = 0; src < EPIC_STAR4_HOSTS; src++) {
        if (src == dest) {
            continue;
        }
        if (slot->allgather_sent_mask[dest] & PORT_MASK(src)) {
            continue;
        }
        uint32_t out_psn = allgather_out_psn(ctx, psn, dest, src);
        if (!send_raw_to_rank(ctx, dest, out_psn, slot->allgather_raw[src],
                              (size_t)ctx->cfg.payload_bytes)) {
            return false;
        }
        slot->allgather_sent_mask[dest] |= PORT_MASK(src);
    }
    slot->result_sent[dest] = true;
    if (ctx->cfg.mode == EPIC_MODE_TERMINATION) {
        bool all_sent = true;
        for (int r = 0; r < EPIC_STAR4_HOSTS; r++) {
            all_sent = all_sent && slot->result_sent[r];
        }
        if (all_sent) {
            cleanup_slot(slot);
        }
    }
    return true;
}

static bool result_ready_for_psn(switch_ctx_t *ctx, uint32_t psn,
                                 slot_state_t **slot_out) {
    slot_state_t *slot = &ctx->slots[slot_idx(psn)];
    if (!slot->in_use || slot->psn != psn || !slot->result_ready) {
        return false;
    }
    *slot_out = slot;
    return true;
}

static bool drain_ordered_results_for_rank(switch_ctx_t *ctx, int rank) {
    while (true) {
        uint32_t psn = ctx->next_result_psn[rank];
        slot_state_t *slot = NULL;
        if (!result_ready_for_psn(ctx, psn, &slot) || slot->result_sent[rank]) {
            return true;
        }
        if (ctx->cfg.primitive == EPIC_PRIM_REDUCE) {
            if (!send_reduce_result(ctx, slot, psn)) {
                return false;
            }
        } else if (ctx->cfg.primitive == EPIC_PRIM_REDUCESCATTER) {
            if (!send_reducescatter_result(ctx, slot, psn)) {
                return false;
            }
        } else if (ctx->cfg.primitive == EPIC_PRIM_ALLREDUCE) {
            if (!send_allreduce_result(ctx, slot, psn, rank)) {
                return false;
            }
        } else if (ctx->cfg.primitive == EPIC_PRIM_ALLGATHER) {
            if (!send_allgather_payloads(ctx, slot, psn, rank)) {
                return false;
            }
        } else if (ctx->cfg.primitive == EPIC_PRIM_BARRIER) {
            if (!send_barrier_release(ctx, slot, psn, rank)) {
                return false;
            }
        }
        ctx->next_result_psn[rank]++;
    }
}

static bool drain_ordered_results(switch_ctx_t *ctx) {
    if (ctx->cfg.primitive == EPIC_PRIM_REDUCE ||
        ctx->cfg.primitive == EPIC_PRIM_REDUCESCATTER) {
        return drain_ordered_results_for_rank(ctx, ctx->cfg.root_rank);
    }
    if (ctx->cfg.primitive == EPIC_PRIM_ALLREDUCE ||
        ctx->cfg.primitive == EPIC_PRIM_ALLGATHER ||
        ctx->cfg.primitive == EPIC_PRIM_BARRIER) {
        for (int r = 0; r < EPIC_STAR4_HOSTS; r++) {
            if (!drain_ordered_results_for_rank(ctx, r)) {
                return false;
            }
        }
    }
    return true;
}

static void add_payload(slot_state_t *slot, const epic_roce_packet_t *pkt,
                        int payload_words) {
    const uint32_t *data = (const uint32_t *)pkt->payload;
    int i = 0;
    for (; i + 3 < payload_words; i += 4) {
        slot->data.accum[i] += (int32_t)ntohl(data[i]);
        slot->data.accum[i + 1] += (int32_t)ntohl(data[i + 1]);
        slot->data.accum[i + 2] += (int32_t)ntohl(data[i + 2]);
        slot->data.accum[i + 3] += (int32_t)ntohl(data[i + 3]);
    }
    for (; i < payload_words; i++) {
        slot->data.accum[i] += (int32_t)ntohl(data[i]);
    }
}

static void init_accum_payload(slot_state_t *slot, const epic_roce_packet_t *pkt,
                               int payload_words) {
    const uint32_t *data = (const uint32_t *)pkt->payload;
    int i = 0;
    for (; i + 3 < payload_words; i += 4) {
        slot->data.accum[i] = (int32_t)ntohl(data[i]);
        slot->data.accum[i + 1] = (int32_t)ntohl(data[i + 1]);
        slot->data.accum[i + 2] = (int32_t)ntohl(data[i + 2]);
        slot->data.accum[i + 3] = (int32_t)ntohl(data[i + 3]);
    }
    for (; i < payload_words; i++) {
        slot->data.accum[i] = (int32_t)ntohl(data[i]);
    }
}

static void copy_payload_raw(slot_state_t *slot, const epic_roce_packet_t *pkt) {
    memcpy(slot->data.bcast_raw, pkt->payload, pkt->payload_len);
}

static bool handle_reduce_like_data(switch_ctx_t *ctx, int rank,
                                    const epic_roce_packet_t *pkt) {
    slot_state_t *slot = get_slot(ctx, pkt->psn);
    uint8_t bit = PORT_MASK(rank);
    bool duplicate = (slot->data_mask & bit) != 0;

    if (duplicate) {
        if (ctx->cfg.mode == EPIC_MODE_TERMINATION ||
            (slot->upload_ack_sent_mask & bit)) {
            return send_ack_to_rank(ctx, rank, pkt->psn, slot);
        }
        if (ctx->cfg.primitive == EPIC_PRIM_ALLREDUCE &&
            slot->result_sent[rank]) {
            return send_allreduce_result(ctx, slot, pkt->psn, rank);
        }
        if (ctx->cfg.primitive == EPIC_PRIM_REDUCE &&
            rank == ctx->cfg.root_rank && slot->result_sent[rank]) {
            return send_reduce_result(ctx, slot, pkt->psn);
        }
        if (ctx->cfg.primitive == EPIC_PRIM_REDUCESCATTER &&
            slot->result_sent[reducescatter_dest_for_psn(ctx, pkt->psn)]) {
            return send_reducescatter_result(ctx, slot, pkt->psn);
        }
        return true;
    }

    if (pkt->payload_len != (size_t)ctx->cfg.payload_bytes) {
        fprintf(stderr, "rank=%d psn=%u payload_len=%zu expected=%d\n", rank,
                pkt->psn, pkt->payload_len, ctx->cfg.payload_bytes);
        return false;
    }

    int payload_words = ctx->cfg.payload_bytes / (int)sizeof(int32_t);
    if (slot->data_mask == 0) {
        init_accum_payload(slot, pkt, payload_words);
    } else {
        add_payload(slot, pkt, payload_words);
    }
    slot->data_mask |= bit;

    if (ctx->cfg.mode == EPIC_MODE_TERMINATION) {
        if (!send_ack_to_rank(ctx, rank, pkt->psn, slot)) {
            return false;
        }
    }

    if (slot->data_mask == ALL_HOST_MASK) {
        slot->result_ready = true;
        return drain_ordered_results(ctx);
    }
    return true;
}

static bool handle_bcast_data(switch_ctx_t *ctx, int rank,
                              const epic_roce_packet_t *pkt) {
    if (rank != ctx->cfg.root_rank) {
        return true;
    }

    slot_state_t *slot = get_slot(ctx, pkt->psn);
    bool duplicate = (slot->data_mask & PORT_MASK(rank)) != 0;

    if (duplicate) {
        if (ctx->cfg.mode == EPIC_MODE_TERMINATION ||
            (slot->upload_ack_sent_mask & PORT_MASK(rank))) {
            return send_ack_to_rank(ctx, rank, pkt->psn, slot);
        }
        return send_bcast_payload(ctx, slot, pkt->psn, -1);
    }

    if (pkt->payload_len != (size_t)ctx->cfg.payload_bytes) {
        fprintf(stderr, "bcast root psn=%u payload_len=%zu expected=%d\n",
                pkt->psn, pkt->payload_len, ctx->cfg.payload_bytes);
        return false;
    }

    copy_payload_raw(slot, pkt);
    slot->data_mask |= PORT_MASK(rank);

    if (ctx->cfg.mode == EPIC_MODE_TERMINATION) {
        if (!send_bcast_payload(ctx, slot, pkt->psn, -1)) {
            return false;
        }
        return send_ack_to_rank(ctx, rank, pkt->psn, slot);
    }
    return send_bcast_payload(ctx, slot, pkt->psn, -1);
}

static bool handle_allgather_data(switch_ctx_t *ctx, int rank,
                                  const epic_roce_packet_t *pkt) {
    slot_state_t *slot = get_slot(ctx, pkt->psn);
    uint8_t bit = PORT_MASK(rank);
    bool duplicate = (slot->data_mask & bit) != 0;

    if (duplicate) {
        if (ctx->cfg.mode == EPIC_MODE_TERMINATION ||
            (slot->upload_ack_sent_mask & bit)) {
            return send_ack_to_rank(ctx, rank, pkt->psn, slot);
        }
        if (slot->result_ready) {
            return drain_ordered_results(ctx);
        }
        return true;
    }

    if (pkt->payload_len != (size_t)ctx->cfg.payload_bytes) {
        fprintf(stderr, "allgather rank=%d psn=%u payload_len=%zu expected=%d\n",
                rank, pkt->psn, pkt->payload_len, ctx->cfg.payload_bytes);
        return false;
    }

    slot->allgather_raw[rank] = malloc((size_t)ctx->cfg.payload_bytes);
    if (!slot->allgather_raw[rank]) {
        perror("malloc allgather payload");
        return false;
    }
    memcpy(slot->allgather_raw[rank], pkt->payload, pkt->payload_len);
    slot->data_mask |= bit;

    if (ctx->cfg.mode == EPIC_MODE_TERMINATION) {
        if (!send_ack_to_rank(ctx, rank, pkt->psn, slot)) {
            return false;
        }
    }

    if (slot->data_mask == ALL_HOST_MASK) {
        slot->result_ready = true;
        return drain_ordered_results(ctx);
    }
    return true;
}

static bool handle_barrier_data(switch_ctx_t *ctx, int rank,
                                const epic_roce_packet_t *pkt) {
    slot_state_t *slot = get_slot(ctx, pkt->psn);
    uint8_t bit = PORT_MASK(rank);
    bool duplicate = (slot->data_mask & bit) != 0;

    if (duplicate) {
        if (ctx->cfg.mode == EPIC_MODE_TERMINATION ||
            (slot->upload_ack_sent_mask & bit)) {
            return send_ack_to_rank(ctx, rank, pkt->psn, slot);
        }
        if (slot->result_sent[rank]) {
            return send_barrier_release(ctx, slot, pkt->psn, rank);
        }
        return true;
    }

    if (pkt->payload_len != 0) {
        fprintf(stderr, "barrier rank=%d psn=%u payload_len=%zu expected=0\n",
                rank, pkt->psn, pkt->payload_len);
        return false;
    }

    slot->data_mask |= bit;

    if (ctx->cfg.mode == EPIC_MODE_TERMINATION) {
        if (!send_ack_to_rank(ctx, rank, pkt->psn, slot)) {
            return false;
        }
    }

    if (slot->data_mask == ALL_HOST_MASK) {
        slot->result_ready = true;
        return drain_ordered_results(ctx);
    }
    return true;
}

static bool drain_reduce_upload_acks(switch_ctx_t *ctx) {
    while (true) {
        uint32_t psn = ctx->next_reduce_ack_psn;
        slot_state_t *slot = &ctx->slots[slot_idx(psn)];
        int ack_rank = ctx->cfg.root_rank;
        if (ctx->cfg.primitive == EPIC_PRIM_REDUCESCATTER) {
            ack_rank = reducescatter_dest_for_psn(ctx, psn);
        }
        if (!slot->in_use || slot->psn != psn ||
            !(slot->down_ack_mask & PORT_MASK(ack_rank))) {
            return true;
        }
        for (int r = 0; r < EPIC_STAR4_HOSTS; r++) {
            if (!(slot->upload_ack_sent_mask & PORT_MASK(r))) {
                if (!send_ack_to_rank(ctx, r, psn, slot)) {
                    return false;
                }
            }
        }
        ctx->next_reduce_ack_psn++;
    }
}

static bool handle_ack(switch_ctx_t *ctx, int rank,
                       const epic_roce_packet_t *pkt) {
    uint32_t slot_psn = pkt->psn;
    int allgather_src = -1;
    if (ctx->cfg.primitive == EPIC_PRIM_REDUCESCATTER) {
        slot_psn = reducescatter_ack_to_data_psn(ctx, rank, pkt->psn);
    } else if (ctx->cfg.primitive == EPIC_PRIM_ALLGATHER) {
        slot_psn = allgather_ack_to_data_psn(ctx, rank, pkt->psn, &allgather_src);
    }

    slot_state_t *slot = get_slot(ctx, slot_psn);
    slot->down_ack_mask |= PORT_MASK(rank);

    if (ctx->cfg.mode == EPIC_MODE_TERMINATION) {
        return true;
    }

    if (ctx->cfg.primitive == EPIC_PRIM_ALLGATHER) {
        if (allgather_src < 0) {
            return true;
        }
        slot->allgather_down_ack_mask[allgather_src] |= PORT_MASK(rank);
        uint8_t receiver_mask = ALL_HOST_MASK & (uint8_t)~PORT_MASK(allgather_src);
        if ((slot->allgather_down_ack_mask[allgather_src] & receiver_mask) ==
                receiver_mask &&
            !(slot->upload_ack_sent_mask & PORT_MASK(allgather_src))) {
            if (!send_ack_to_rank(ctx, allgather_src, slot_psn, slot)) {
                return false;
            }
            free(slot->allgather_raw[allgather_src]);
            slot->allgather_raw[allgather_src] = NULL;
            return true;
        }
        return true;
    }

    if (ctx->cfg.primitive == EPIC_PRIM_ALLREDUCE ||
        ctx->cfg.primitive == EPIC_PRIM_BARRIER) {
        return send_ack_to_rank(ctx, rank, slot_psn, slot);
    }

    if (ctx->cfg.primitive == EPIC_PRIM_REDUCE ||
        ctx->cfg.primitive == EPIC_PRIM_REDUCESCATTER) {
        int ack_rank = ctx->cfg.root_rank;
        if (ctx->cfg.primitive == EPIC_PRIM_REDUCESCATTER) {
            ack_rank = reducescatter_dest_for_psn(ctx, slot_psn);
        }
        if (rank == ack_rank) {
            return drain_reduce_upload_acks(ctx);
        }
        return true;
    }

    uint8_t receiver_mask = ALL_HOST_MASK & (uint8_t)~PORT_MASK(ctx->cfg.root_rank);
    if (ctx->trace && ctx->cfg.primitive == EPIC_PRIM_BCAST) {
        char extra[96];
        snprintf(extra, sizeof(extra), "down_ack_mask=0x%x receiver_mask=0x%x",
                 slot->down_ack_mask, receiver_mask);
        trace_event(ctx, "bcast_ack_seen", rank, pkt->psn, extra);
    }
    if ((slot->down_ack_mask & receiver_mask) == receiver_mask &&
        !(slot->upload_ack_sent_mask & PORT_MASK(ctx->cfg.root_rank))) {
        trace_event(ctx, "bcast_ack_agg_complete", ctx->cfg.root_rank, pkt->psn,
                    NULL);
        return send_ack_to_rank(ctx, ctx->cfg.root_rank, pkt->psn, slot);
    }
    return true;
}

static bool handle_packet(switch_ctx_t *ctx, int rank, const uint8_t *frame,
                          size_t frame_len) {
    epic_roce_packet_t pkt;
    uint64_t parse0 = ctx->profile ? nsec_now() : 0;
    if (!epic_parse_roce(frame, frame_len, &pkt)) {
        if (ctx->profile) {
            ctx->prof_rx_packets++;
            ctx->prof_parse_fail++;
            ctx->prof_parse_ns += nsec_now() - parse0;
        }
        return true;
    }
    uint64_t parse1 = ctx->profile ? nsec_now() : 0;
    if (ctx->profile) {
        ctx->prof_rx_packets++;
        ctx->prof_parse_ns += parse1 - parse0;
    }

    bool ok = true;
    uint64_t handle0 = ctx->profile ? nsec_now() : 0;
    if (pkt.is_ack) {
        ctx->rx_ack++;
        trace_event(ctx, "rx_ack", rank, pkt.psn, NULL);
        ok = handle_ack(ctx, rank, &pkt);
    } else if (pkt.is_data) {
        ctx->rx_data++;
        trace_event(ctx, "rx_data", rank, pkt.psn, NULL);
        if (ctx->cfg.primitive == EPIC_PRIM_BCAST) {
            ok = handle_bcast_data(ctx, rank, &pkt);
        } else if (ctx->cfg.primitive == EPIC_PRIM_ALLGATHER) {
            ok = handle_allgather_data(ctx, rank, &pkt);
        } else if (ctx->cfg.primitive == EPIC_PRIM_BARRIER) {
            ok = handle_barrier_data(ctx, rank, &pkt);
        } else {
            ok = handle_reduce_like_data(ctx, rank, &pkt);
        }
    }
    if (ctx->profile) {
        ctx->prof_handle_ns += nsec_now() - handle0;
    }
    return ok;
}

static void event_loop(switch_ctx_t *ctx) {
    struct epoll_event events[MAX_EVENTS];
    while (running) {
        int nfds = epoll_wait(ctx->epoll_fd, events, MAX_EVENTS, 200);
        if (nfds < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("epoll_wait");
            break;
        }
        for (int i = 0; i < nfds; i++) {
            int rank = (int)events[i].data.u32;
            const uint8_t *packet = NULL;
            struct pcap_pkthdr *hdr = NULL;
            int rc = 0;
            int drained = 0;
            while (drained < ctx->rx_burst_per_port &&
                   (rc = pcap_next_ex(ctx->handles[rank], &hdr, &packet)) == 1) {
                drained++;
                if (!handle_packet(ctx, rank, packet, hdr->caplen)) {
                    running = 0;
                    break;
                }
            }
            if (rc == PCAP_ERROR) {
                fprintf(stderr, "pcap_next_ex rank=%d: %s\n", rank,
                        pcap_geterr(ctx->handles[rank]));
                running = 0;
                break;
            }
        }
    }
}

static void print_config(const epic_switch_config_t *cfg) {
    printf("SWITCH primitive=%s mode=%d root=%d count=%d payload_bytes=%d\n",
           epic_primitive_name(cfg->primitive), cfg->mode, cfg->root_rank,
           cfg->count, cfg->payload_bytes);
    for (int i = 0; i < EPIC_STAR4_HOSTS; i++) {
        const epic_port_t *p = &cfg->ports[i];
        char host_ip[INET_ADDRSTRLEN];
        char sw_ip[INET_ADDRSTRLEN];
        char host_mac[32];
        char sw_mac[32];
        epic_ipv4_to_str(p->host_ip, host_ip, sizeof(host_ip));
        epic_ipv4_to_str(p->switch_ip, sw_ip, sizeof(sw_ip));
        epic_mac_to_str(p->host_mac, host_mac, sizeof(host_mac));
        epic_mac_to_str(p->switch_mac, sw_mac, sizeof(sw_mac));
        printf("  port%d dev=%s host=%s/%s switch=%s/%s host_qpn=%u switch_qpn=%u\n",
               i, p->dev, host_ip, host_mac, sw_ip, sw_mac, p->host_qpn,
               p->switch_qpn);
    }
}

static void usage(const char *prog) {
    fprintf(stderr, "usage: %s --config /path/to/switch.conf [--rx-burst N] [--profile] [--trace]\n",
            prog);
}

int main(int argc, char **argv) {
    const char *config_path = NULL;
    bool profile = false;
    bool trace = false;
    int rx_burst = DEFAULT_RX_BURST_PER_PORT;
    static const struct option long_opts[] = {
        {"config", required_argument, NULL, 'c'},
        {"rx-burst", required_argument, NULL, 'b'},
        {"profile", no_argument, NULL, 'p'},
        {"trace", no_argument, NULL, 't'},
        {0, 0, 0, 0},
    };

    int c;
    while ((c = getopt_long(argc, argv, "", long_opts, NULL)) != -1) {
        switch (c) {
        case 'c':
            config_path = optarg;
            break;
        case 'b':
            rx_burst = atoi(optarg);
            break;
        case 'p':
            profile = true;
            break;
        case 't':
            trace = true;
            break;
        default:
            usage(argv[0]);
            return 2;
        }
    }
    if (!config_path) {
        usage(argv[0]);
        return 2;
    }
    if (rx_burst <= 0 || rx_burst > 256) {
        fprintf(stderr, "invalid --rx-burst: %d\n", rx_burst);
        return 2;
    }

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    epic_crc32_init();

    switch_ctx_t *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) {
        perror("calloc switch context");
        return 1;
    }
    ctx->epoll_fd = -1;
    ctx->rx_burst_per_port = rx_burst;
    ctx->profile = profile;
    ctx->trace = trace;

    if (!load_config(config_path, &ctx->cfg)) {
        return 1;
    }
    print_config(&ctx->cfg);
    fflush(stdout);

    if (!setup_pcap(ctx)) {
        return 1;
    }

    printf("SWITCH_READY profile=%d trace=%d rx_burst=%d\n",
           ctx->profile ? 1 : 0, ctx->trace ? 1 : 0,
           ctx->rx_burst_per_port);
    fflush(stdout);
    event_loop(ctx);

    printf("SWITCH_RESULT rx_data=%" PRIu64 " rx_ack=%" PRIu64
           " tx_data=%" PRIu64 " tx_ack=%" PRIu64
           " slot_overwrites=%" PRIu64 "\n",
           ctx->rx_data, ctx->rx_ack, ctx->tx_data, ctx->tx_ack,
           ctx->slot_overwrites);
    if (ctx->profile) {
        uint64_t parsed = ctx->rx_data + ctx->rx_ack;
        uint64_t tx = ctx->tx_data + ctx->tx_ack;
        uint64_t compute_ns = 0;
        if (ctx->prof_handle_ns > ctx->prof_build_ns + ctx->prof_send_ns) {
            compute_ns = ctx->prof_handle_ns - ctx->prof_build_ns - ctx->prof_send_ns;
        }
        printf("SWITCH_PROFILE rx_packets=%" PRIu64 " parsed=%" PRIu64
               " parse_fail=%" PRIu64 " tx_packets=%" PRIu64
               " avg_parse_ns=%.1f avg_handle_ns=%.1f avg_build_ns=%.1f"
               " avg_send_ns=%.1f avg_compute_est_ns=%.1f\n",
               ctx->prof_rx_packets, parsed, ctx->prof_parse_fail, tx,
               avg_ns(ctx->prof_parse_ns, ctx->prof_rx_packets),
               avg_ns(ctx->prof_handle_ns, parsed),
               avg_ns(ctx->prof_build_ns, tx),
               avg_ns(ctx->prof_send_ns, tx),
               avg_ns(compute_ns, parsed));
    }

    for (int i = 0; i < EPIC_STAR4_HOSTS; i++) {
        if (ctx->handles[i]) {
            pcap_close(ctx->handles[i]);
        }
    }
    if (ctx->epoll_fd >= 0) {
        close(ctx->epoll_fd);
    }
    free(ctx);
    return 0;
}
