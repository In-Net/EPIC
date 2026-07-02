#define _POSIX_C_SOURCE 200809L

#include "epic_clean.h"

#include <arpa/inet.h>
#include <errno.h>
#include <getopt.h>
#include <infiniband/verbs.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define MODE1_MAX_MESSAGE_BYTES (64 * 1024)
#define MODE1_MAX_POLL 32
#define MODE1_DEFAULT_SEND_WINDOW 512
#define MODE1_DEFAULT_RECV_WINDOW 512

typedef enum {
    MODE1_ROLE_HOST = 0,
    MODE1_ROLE_SWITCH = 1,
} mode1_role_t;

typedef struct {
    mode1_role_t role;
    int rank;
    epic_primitive_t primitive;
    int root_rank;
    int count;
    int iters;
    int warmup;
    int payload_bytes;
    int segment_window;
    int recv_window;
    int send_window;
    int qp_timeout;
    int ib_port;
    int gid_index;
    bool auto_gid_index;
    uint32_t local_ip;
    uint32_t switch_ip;
    uint32_t remote_qpn;
    char ib_dev[64];
    const char *endpoint_file;
    const char *switch_endpoint_file;
    const char *start_file;
    const char *config_file;
    const char *endpoint_dir;
} mode1_opts_t;

typedef struct {
    mode1_opts_t opts;
    struct ibv_context *ib_ctx;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_qp *qp;
    struct ibv_mr *send_mr;
    struct ibv_mr *recv_mr;
    uint8_t *send_buf;
    uint8_t *recv_buf;
    size_t total_bytes;
    int total_segments;
    int block_segments;
    int send_segments;
    int data_recv_segments;
    int control_recv_segments;
    int recv_entries_per_iter;
    int recv_total;
    int recv_posted_total;
    int recv_completed_total;
    int pending_send;
    int pending_recv;
    union ibv_gid local_gid;
    union ibv_gid remote_gid;
    enum ibv_mtu path_mtu;
} host_ctx_t;

typedef struct {
    int rank;
    char dev[64];
    uint32_t host_ip;
    uint32_t switch_ip;
    uint32_t host_qpn;
    int gid_index;
    struct ibv_context *ib_ctx;
    struct ibv_pd *pd;
    struct ibv_cq *send_cq;
    struct ibv_cq *recv_cq;
    struct ibv_qp *qp;
    struct ibv_mr *recv_mr;
    struct ibv_mr *send_mr;
    uint8_t *recv_area;
    uint8_t *send_area;
    int recv_total;
    int recv_posted_total;
    int recv_completed_total;
    int recv_window;
    int send_window;
    int pending_send;
    int send_head;
    int recv_seq_iter;
    union ibv_gid local_gid;
    union ibv_gid host_gid;
    enum ibv_mtu path_mtu;
} switch_port_t;

typedef struct {
    mode1_opts_t opts;
    switch_port_t ports[EPIC_STAR4_HOSTS];
    size_t total_bytes;
    int total_segments;
    int block_segments;
    uint8_t *aggregate_buf;
    uint8_t *gather_buf;
    uint16_t *degree;
    uint8_t *ready;
    int next_segment;
    int next_rs_segment[EPIC_STAR4_HOSTS];
    int next_ag_seq[EPIC_STAR4_HOSTS];
    uint64_t rx_msg;
    uint64_t tx_msg;
    uint64_t release_msg;
} switch_ctx_t;

static char *trim(char *s);

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static uint64_t realtime_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static uint32_t parse_u32_auto(const char *text) {
    char *end = NULL;
    errno = 0;
    unsigned long v = strtoul(text, &end, 0);
    if (errno || !end || *end != '\0' || v > 0xfffffffful) {
        fprintf(stderr, "invalid integer: %s\n", text);
        exit(2);
    }
    return (uint32_t)v;
}

static void usage(const char *prog) {
    fprintf(stderr,
            "usage: %s --role host|switch [common args]\n"
            "host:   --rank N --primitive P --root N --count N --iters N --local-ip IP --switch-ip IP "
            "--endpoint-file PATH --switch-endpoint-file PATH --start-file PATH [--dev DEV]\n"
            "switch: --config PATH --endpoint-dir DIR --start-file PATH\n",
            prog);
}

static bool parse_args(int argc, char **argv, mode1_opts_t *opts) {
    memset(opts, 0, sizeof(*opts));
    opts->role = MODE1_ROLE_HOST;
    opts->rank = -1;
    opts->primitive = EPIC_PRIM_ALLREDUCE;
    opts->root_rank = 0;
    opts->count = EPIC_DEFAULT_COUNT;
    opts->iters = EPIC_DEFAULT_ITERS;
    opts->payload_bytes = EPIC_DEFAULT_PAYLOAD_BYTES;
    opts->segment_window = EPIC_DEFAULT_SEGMENT_WINDOW;
    opts->recv_window = MODE1_DEFAULT_RECV_WINDOW;
    opts->send_window = MODE1_DEFAULT_SEND_WINDOW;
    opts->qp_timeout = EPIC_DEFAULT_QP_TIMEOUT;
    opts->ib_port = EPIC_DEFAULT_IB_PORT;
    opts->gid_index = EPIC_DEFAULT_GID_INDEX;
    opts->auto_gid_index = true;

    static const struct option long_opts[] = {
        {"role", required_argument, NULL, 'o'},
        {"rank", required_argument, NULL, 'r'},
        {"primitive", required_argument, NULL, 'p'},
        {"root", required_argument, NULL, 'R'},
        {"count", required_argument, NULL, 'c'},
        {"iters", required_argument, NULL, 'i'},
        {"warmup", required_argument, NULL, 'w'},
        {"payload-bytes", required_argument, NULL, 'b'},
        {"segment-window", required_argument, NULL, 'W'},
        {"recv-window", required_argument, NULL, 'V'},
        {"send-window", required_argument, NULL, 'X'},
        {"qp-timeout", required_argument, NULL, 'T'},
        {"local-ip", required_argument, NULL, 'l'},
        {"switch-ip", required_argument, NULL, 's'},
        {"remote-qpn", required_argument, NULL, 'q'},
        {"endpoint-file", required_argument, NULL, 'e'},
        {"switch-endpoint-file", required_argument, NULL, 'E'},
        {"start-file", required_argument, NULL, 'S'},
        {"dev", required_argument, NULL, 'd'},
        {"gid-index", required_argument, NULL, 'g'},
        {"ib-port", required_argument, NULL, 'P'},
        {"config", required_argument, NULL, 'C'},
        {"endpoint-dir", required_argument, NULL, 'D'},
        {0, 0, 0, 0},
    };

    int c;
    while ((c = getopt_long(argc, argv, "", long_opts, NULL)) != -1) {
        switch (c) {
        case 'o':
            if (strcmp(optarg, "host") == 0) {
                opts->role = MODE1_ROLE_HOST;
            } else if (strcmp(optarg, "switch") == 0) {
                opts->role = MODE1_ROLE_SWITCH;
            } else {
                fprintf(stderr, "invalid role: %s\n", optarg);
                return false;
            }
            break;
        case 'r':
            opts->rank = (int)parse_u32_auto(optarg);
            break;
        case 'p':
            if (!epic_parse_primitive(optarg, &opts->primitive)) {
                fprintf(stderr, "invalid primitive: %s\n", optarg);
                return false;
            }
            break;
        case 'R':
            opts->root_rank = (int)parse_u32_auto(optarg);
            break;
        case 'c':
            opts->count = (int)parse_u32_auto(optarg);
            break;
        case 'i':
            opts->iters = (int)parse_u32_auto(optarg);
            break;
        case 'w':
            opts->warmup = (int)parse_u32_auto(optarg);
            break;
        case 'b':
            opts->payload_bytes = (int)parse_u32_auto(optarg);
            break;
        case 'W':
            opts->segment_window = (int)parse_u32_auto(optarg);
            break;
        case 'V':
            opts->recv_window = (int)parse_u32_auto(optarg);
            break;
        case 'X':
            opts->send_window = (int)parse_u32_auto(optarg);
            break;
        case 'T':
            opts->qp_timeout = (int)parse_u32_auto(optarg);
            break;
        case 'l':
            if (!epic_parse_ipv4(optarg, &opts->local_ip)) {
                fprintf(stderr, "invalid local ip: %s\n", optarg);
                return false;
            }
            break;
        case 's':
            if (!epic_parse_ipv4(optarg, &opts->switch_ip)) {
                fprintf(stderr, "invalid switch ip: %s\n", optarg);
                return false;
            }
            break;
        case 'q':
            opts->remote_qpn = parse_u32_auto(optarg);
            break;
        case 'e':
            opts->endpoint_file = optarg;
            break;
        case 'E':
            opts->switch_endpoint_file = optarg;
            break;
        case 'S':
            opts->start_file = optarg;
            break;
        case 'd':
            snprintf(opts->ib_dev, sizeof(opts->ib_dev), "%s", optarg);
            break;
        case 'g':
            opts->gid_index = (int)parse_u32_auto(optarg);
            opts->auto_gid_index = false;
            break;
        case 'P':
            opts->ib_port = (int)parse_u32_auto(optarg);
            break;
        case 'C':
            opts->config_file = optarg;
            break;
        case 'D':
            opts->endpoint_dir = optarg;
            break;
        default:
            return false;
        }
    }

    if (opts->root_rank < 0 || opts->root_rank >= EPIC_STAR4_HOSTS ||
        opts->iters <= 0 || opts->warmup < 0 ||
        opts->segment_window <= 0 || opts->recv_window <= 0 ||
        opts->send_window <= 0 || opts->qp_timeout < 0 || opts->qp_timeout > 31 ||
        opts->payload_bytes < 0 || opts->payload_bytes > MODE1_MAX_MESSAGE_BYTES ||
        (opts->payload_bytes > 0 && opts->payload_bytes % (int)sizeof(uint32_t) != 0)) {
        return false;
    }
    if (opts->role == MODE1_ROLE_HOST) {
        return opts->rank >= 0 && opts->rank < EPIC_STAR4_HOSTS &&
               opts->local_ip != 0 && opts->switch_ip != 0 &&
               opts->endpoint_file && opts->switch_endpoint_file && opts->start_file;
    }
    return opts->config_file && opts->endpoint_dir && opts->start_file;
}

static bool is_source_rank(epic_primitive_t primitive, int rank, int root_rank) {
    if (primitive == EPIC_PRIM_BCAST) {
        return rank == root_rank;
    }
    return true;
}

static bool is_data_sink_rank(epic_primitive_t primitive, int rank, int root_rank) {
    if (primitive == EPIC_PRIM_REDUCE) {
        return rank == root_rank;
    }
    if (primitive == EPIC_PRIM_BCAST) {
        return rank != root_rank;
    }
    return true;
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

static struct ibv_context *open_selected_device(const char *name) {
    int num = 0;
    struct ibv_device **list = ibv_get_device_list(&num);
    if (!list || num <= 0) {
        fprintf(stderr, "ibv_get_device_list found no devices\n");
        return NULL;
    }

    struct ibv_device *selected = NULL;
    for (int i = 0; i < num; i++) {
        const char *dev_name = ibv_get_device_name(list[i]);
        if (!name || name[0] == '\0' || strcmp(name, dev_name) == 0) {
            selected = list[i];
            break;
        }
    }

    if (!selected) {
        fprintf(stderr, "RDMA device not found: %s\n", name ? name : "(first)");
    }
    struct ibv_context *ctx = selected ? ibv_open_device(selected) : NULL;
    ibv_free_device_list(list);
    return ctx;
}

static int find_gid_index(struct ibv_context *ctx, int ib_port, uint32_t local_ip,
                          int fallback) {
    struct ibv_port_attr port_attr;
    if (ibv_query_port(ctx, ib_port, &port_attr)) {
        return fallback;
    }

    for (int i = 0; i < port_attr.gid_tbl_len; i++) {
        union ibv_gid gid;
        if (ibv_query_gid(ctx, ib_port, i, &gid)) {
            continue;
        }
        if (memcmp(gid.raw + 12, &local_ip, sizeof(local_ip)) == 0) {
            return i;
        }
    }
    return fallback;
}

static enum ibv_mtu clamp_mtu(enum ibv_mtu active_mtu, int payload_bytes) {
    enum ibv_mtu needed = IBV_MTU_4096;
    if (payload_bytes <= 256) {
        needed = IBV_MTU_256;
    } else if (payload_bytes <= 512) {
        needed = IBV_MTU_512;
    } else if (payload_bytes <= 1024) {
        needed = IBV_MTU_1024;
    } else if (payload_bytes <= 2048) {
        needed = IBV_MTU_2048;
    }
    return active_mtu < needed ? active_mtu : needed;
}

static void gid_for_ip(union ibv_gid *dst, const union ibv_gid *base, uint32_t ip) {
    *dst = *base;
    memcpy(dst->raw + 12, &ip, sizeof(ip));
}

static bool modify_qp_init(struct ibv_qp *qp, int ib_port) {
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = ib_port;
    attr.pkey_index = 0;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                           IBV_ACCESS_REMOTE_READ;
    return ibv_modify_qp(qp, &attr,
                         IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
                             IBV_QP_ACCESS_FLAGS) == 0;
}

static bool modify_qp_rtr(struct ibv_qp *qp, int ib_port, int gid_index,
                          enum ibv_mtu path_mtu, uint32_t remote_qpn,
                          const union ibv_gid *remote_gid) {
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = path_mtu;
    attr.dest_qp_num = remote_qpn & 0x00ffffffu;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 12;
    attr.ah_attr.is_global = 1;
    attr.ah_attr.port_num = ib_port;
    attr.ah_attr.grh.dgid = *remote_gid;
    attr.ah_attr.grh.sgid_index = gid_index;
    attr.ah_attr.grh.hop_limit = 64;
    attr.ah_attr.grh.traffic_class = 0;
    attr.ah_attr.grh.flow_label = 0;

    return ibv_modify_qp(qp, &attr,
                         IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                             IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                             IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER) == 0;
}

static bool modify_qp_rts(struct ibv_qp *qp, int qp_timeout) {
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = (uint8_t)qp_timeout;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = 0;
    attr.max_rd_atomic = 1;

    return ibv_modify_qp(qp, &attr,
                         IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                             IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                             IBV_QP_MAX_QP_RD_ATOMIC) == 0;
}

static void wait_for_start(const char *path) {
    const struct timespec interval = {.tv_sec = 0, .tv_nsec = 10000000L};
    uint64_t deadline_ns = 0;
    while (true) {
        FILE *f = fopen(path, "r");
        if (f) {
            if (fscanf(f, "%" SCNu64, &deadline_ns) != 1) {
                deadline_ns = 0;
            }
            fclose(f);
            break;
        }
        nanosleep(&interval, NULL);
    }
    while (deadline_ns > 0 && realtime_ns() < deadline_ns) {
        nanosleep(&interval, NULL);
    }
}

static bool read_endpoint_qpn(const char *path, uint32_t *qpn) {
    const struct timespec interval = {.tv_sec = 0, .tv_nsec = 10000000L};
    uint64_t deadline = now_ns() + 120ull * 1000ull * 1000ull * 1000ull;
    while (now_ns() < deadline) {
        FILE *f = fopen(path, "r");
        if (f) {
            char line[256];
            while (fgets(line, sizeof(line), f)) {
                if (strncmp(line, "qpn=", 4) == 0) {
                    char *value = trim(line + 4);
                    *qpn = parse_u32_auto(value);
                    fclose(f);
                    return true;
                }
            }
            fclose(f);
        }
        nanosleep(&interval, NULL);
    }
    fprintf(stderr, "timeout waiting endpoint qpn: %s\n", path);
    return false;
}

static bool validate_layout(epic_primitive_t primitive, int count, int payload_bytes) {
    if (primitive == EPIC_PRIM_BARRIER) {
        return true;
    }
    size_t total_bytes = (size_t)count * sizeof(int32_t);
    if (count <= 0 || payload_bytes <= 0 || total_bytes % (size_t)payload_bytes != 0) {
        fprintf(stderr, "count*sizeof(int32_t) must be divisible by payload-bytes\n");
        return false;
    }
    if ((primitive == EPIC_PRIM_ALLGATHER ||
         primitive == EPIC_PRIM_REDUCESCATTER) &&
        (count % EPIC_STAR4_HOSTS != 0 ||
         (total_bytes / EPIC_STAR4_HOSTS) % (size_t)payload_bytes != 0)) {
        fprintf(stderr, "allgather/reducescatter count must be divisible by hosts and each block by payload-bytes\n");
        return false;
    }
    return true;
}

static void fill_send_buffer(host_ctx_t *ctx) {
    if (ctx->opts.primitive == EPIC_PRIM_BARRIER || !ctx->send_buf) {
        return;
    }
    uint32_t *buf = (uint32_t *)ctx->send_buf;
    int multiplier = ctx->opts.rank + 1;
    if (ctx->opts.primitive == EPIC_PRIM_BCAST) {
        multiplier = ctx->opts.root_rank + 1;
    }
    for (int i = 0; i < ctx->opts.count; i++) {
        buf[i] = htonl((uint32_t)(i * multiplier));
    }
    if (ctx->opts.primitive == EPIC_PRIM_ALLGATHER && ctx->recv_buf) {
        size_t block_bytes = ctx->total_bytes / EPIC_STAR4_HOSTS;
        size_t block_off = (size_t)ctx->opts.rank * block_bytes;
        memcpy(ctx->recv_buf + block_off, ctx->send_buf + block_off, block_bytes);
    }
}

static size_t host_send_offset_for_segment(host_ctx_t *ctx, int segment) {
    if (ctx->opts.primitive == EPIC_PRIM_ALLGATHER) {
        size_t block_bytes = ctx->total_bytes / EPIC_STAR4_HOSTS;
        return (size_t)ctx->opts.rank * block_bytes +
               (size_t)segment * (size_t)ctx->opts.payload_bytes;
    }
    return (size_t)segment * (size_t)ctx->opts.payload_bytes;
}

static size_t host_recv_offset_for_seq(host_ctx_t *ctx, int seq) {
    if (ctx->opts.primitive == EPIC_PRIM_ALLGATHER) {
        int peers = EPIC_STAR4_HOSTS - 1;
        int segment = seq / peers;
        int ordinal = seq % peers;
        int src = rank_from_ordinal_excluding(ctx->opts.rank, ordinal);
        size_t block_bytes = ctx->total_bytes / EPIC_STAR4_HOSTS;
        return (size_t)src * block_bytes +
               (size_t)segment * (size_t)ctx->opts.payload_bytes;
    }
    if (ctx->opts.primitive == EPIC_PRIM_REDUCESCATTER) {
        size_t block_bytes = ctx->total_bytes / EPIC_STAR4_HOSTS;
        return (size_t)ctx->opts.rank * block_bytes +
               (size_t)seq * (size_t)ctx->opts.payload_bytes;
    }
    return (size_t)seq * (size_t)ctx->opts.payload_bytes;
}

static bool host_write_endpoint(host_ctx_t *ctx) {
    FILE *f = fopen(ctx->opts.endpoint_file, "w");
    if (!f) {
        perror("fopen endpoint");
        return false;
    }
    char local_ip[INET_ADDRSTRLEN];
    char switch_ip[INET_ADDRSTRLEN];
    epic_ipv4_to_str(ctx->opts.local_ip, local_ip, sizeof(local_ip));
    epic_ipv4_to_str(ctx->opts.switch_ip, switch_ip, sizeof(switch_ip));
    fprintf(f, "rank=%d\n", ctx->opts.rank);
    fprintf(f, "qpn=%u\n", ctx->qp->qp_num);
    fprintf(f, "gid_index=%d\n", ctx->opts.gid_index);
    fprintf(f, "ib_port=%d\n", ctx->opts.ib_port);
    fprintf(f, "local_ip=%s\n", local_ip);
    fprintf(f, "switch_ip=%s\n", switch_ip);
    fclose(f);
    return true;
}

static bool host_init_context(host_ctx_t *ctx) {
    if (!validate_layout(ctx->opts.primitive, ctx->opts.count, ctx->opts.payload_bytes)) {
        return false;
    }

    if (ctx->opts.primitive == EPIC_PRIM_BARRIER) {
        ctx->total_segments = 1;
        ctx->block_segments = 1;
    } else {
        ctx->total_bytes = (size_t)ctx->opts.count * sizeof(int32_t);
        ctx->total_segments = (int)(ctx->total_bytes / (size_t)ctx->opts.payload_bytes);
        ctx->block_segments = ctx->total_segments / EPIC_STAR4_HOSTS;
    }

    bool source = is_source_rank(ctx->opts.primitive, ctx->opts.rank, ctx->opts.root_rank);
    bool data_sink = is_data_sink_rank(ctx->opts.primitive, ctx->opts.rank, ctx->opts.root_rank);
    if (ctx->opts.primitive == EPIC_PRIM_BARRIER) {
        ctx->send_segments = 1;
        ctx->data_recv_segments = 1;
    } else if (ctx->opts.primitive == EPIC_PRIM_ALLGATHER) {
        ctx->send_segments = ctx->block_segments;
        ctx->data_recv_segments = ctx->block_segments * (EPIC_STAR4_HOSTS - 1);
    } else if (ctx->opts.primitive == EPIC_PRIM_REDUCESCATTER) {
        ctx->send_segments = ctx->total_segments;
        ctx->data_recv_segments = ctx->block_segments;
    } else {
        ctx->send_segments = source ? ctx->total_segments : 0;
        ctx->data_recv_segments = data_sink ? ctx->total_segments : 0;
    }
    ctx->control_recv_segments = source ? 1 : 0;
    ctx->recv_entries_per_iter = ctx->data_recv_segments + ctx->control_recv_segments;
    if (ctx->send_segments > 0 && ctx->opts.segment_window > ctx->send_segments) {
        ctx->opts.segment_window = ctx->send_segments;
    }
    if (ctx->opts.segment_window <= 0) {
        ctx->opts.segment_window = 1;
    }
    int total_recv_entries = ctx->recv_entries_per_iter * (ctx->opts.warmup + ctx->opts.iters);
    if (total_recv_entries > 0 && ctx->opts.recv_window > total_recv_entries) {
        ctx->opts.recv_window = total_recv_entries;
    }
    if (ctx->opts.recv_window <= 0) {
        ctx->opts.recv_window = 1;
    }

    ctx->ib_ctx = open_selected_device(ctx->opts.ib_dev);
    if (!ctx->ib_ctx) {
        return false;
    }
    if (ctx->opts.auto_gid_index) {
        ctx->opts.gid_index = find_gid_index(ctx->ib_ctx, ctx->opts.ib_port,
                                             ctx->opts.local_ip, EPIC_DEFAULT_GID_INDEX);
    }
    struct ibv_port_attr port_attr;
    if (ibv_query_port(ctx->ib_ctx, ctx->opts.ib_port, &port_attr)) {
        perror("ibv_query_port");
        return false;
    }
    ctx->path_mtu = clamp_mtu(port_attr.active_mtu,
                              ctx->opts.payload_bytes > 0 ? ctx->opts.payload_bytes : 1);
    if (ibv_query_gid(ctx->ib_ctx, ctx->opts.ib_port, ctx->opts.gid_index,
                      &ctx->local_gid)) {
        perror("ibv_query_gid");
        return false;
    }
    gid_for_ip(&ctx->remote_gid, &ctx->local_gid, ctx->opts.switch_ip);

    ctx->pd = ibv_alloc_pd(ctx->ib_ctx);
    if (!ctx->pd) {
        perror("ibv_alloc_pd");
        return false;
    }
    int cq_depth = ctx->opts.segment_window * 2 + ctx->opts.recv_window * 2 + 128;
    ctx->cq = ibv_create_cq(ctx->ib_ctx, cq_depth, NULL, NULL, 0);
    if (!ctx->cq) {
        perror("ibv_create_cq");
        return false;
    }

    struct ibv_qp_init_attr qpia;
    memset(&qpia, 0, sizeof(qpia));
    qpia.qp_type = IBV_QPT_RC;
    qpia.sq_sig_all = 0;
    qpia.send_cq = ctx->cq;
    qpia.recv_cq = ctx->cq;
    qpia.cap.max_send_wr = ctx->opts.segment_window + 16;
    qpia.cap.max_recv_wr = ctx->opts.recv_window + 16;
    qpia.cap.max_send_sge = 1;
    qpia.cap.max_recv_sge = 1;
    ctx->qp = ibv_create_qp(ctx->pd, &qpia);
    if (!ctx->qp) {
        perror("ibv_create_qp");
        return false;
    }

    int mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                   IBV_ACCESS_REMOTE_READ;
    if (ctx->send_segments > 0 && ctx->opts.primitive != EPIC_PRIM_BARRIER) {
        if (posix_memalign((void **)&ctx->send_buf, 4096, ctx->total_bytes)) {
            perror("posix_memalign send");
            return false;
        }
        memset(ctx->send_buf, 0, ctx->total_bytes);
        ctx->send_mr = ibv_reg_mr(ctx->pd, ctx->send_buf, ctx->total_bytes, mr_flags);
        if (!ctx->send_mr) {
            perror("ibv_reg_mr send");
            return false;
        }
    }
    if (ctx->data_recv_segments > 0 && ctx->opts.primitive != EPIC_PRIM_BARRIER) {
        if (posix_memalign((void **)&ctx->recv_buf, 4096, ctx->total_bytes)) {
            perror("posix_memalign recv");
            return false;
        }
        memset(ctx->recv_buf, 0, ctx->total_bytes);
        ctx->recv_mr = ibv_reg_mr(ctx->pd, ctx->recv_buf, ctx->total_bytes, mr_flags);
        if (!ctx->recv_mr) {
            perror("ibv_reg_mr recv");
            return false;
        }
    }

    if (!modify_qp_init(ctx->qp, ctx->opts.ib_port)) {
        perror("ibv_modify_qp INIT");
        return false;
    }
    return true;
}

static bool host_connect_qp(host_ctx_t *ctx) {
    uint32_t qpn = 0;
    if (!read_endpoint_qpn(ctx->opts.switch_endpoint_file, &qpn)) {
        return false;
    }
    ctx->opts.remote_qpn = qpn;
    if (!modify_qp_rtr(ctx->qp, ctx->opts.ib_port, ctx->opts.gid_index,
                       ctx->path_mtu, ctx->opts.remote_qpn, &ctx->remote_gid)) {
        perror("ibv_modify_qp RTR");
        return false;
    }
    if (!modify_qp_rts(ctx->qp, ctx->opts.qp_timeout)) {
        perror("ibv_modify_qp RTS");
        return false;
    }
    return true;
}

static bool host_post_recv_seq(host_ctx_t *ctx, int seq) {
    struct ibv_recv_wr wr;
    struct ibv_recv_wr *bad = NULL;
    struct ibv_sge sge;
    memset(&wr, 0, sizeof(wr));
    memset(&sge, 0, sizeof(sge));
    wr.wr_id = (uint64_t)seq;

    if (ctx->opts.primitive != EPIC_PRIM_BARRIER && seq < ctx->data_recv_segments) {
        sge.addr = (uintptr_t)(ctx->recv_buf + host_recv_offset_for_seq(ctx, seq));
        sge.length = (uint32_t)ctx->opts.payload_bytes;
        sge.lkey = ctx->recv_mr->lkey;
        wr.sg_list = &sge;
        wr.num_sge = 1;
    }

    if (ibv_post_recv(ctx->qp, &wr, &bad)) {
        perror("ibv_post_recv host");
        return false;
    }
    return true;
}

static bool host_refill_recv_window(host_ctx_t *ctx) {
    if (ctx->recv_entries_per_iter <= 0) {
        return true;
    }
    while (ctx->recv_posted_total < ctx->recv_total &&
           ctx->recv_posted_total - ctx->recv_completed_total < ctx->opts.recv_window) {
        int seq = ctx->recv_posted_total % ctx->recv_entries_per_iter;
        if (!host_post_recv_seq(ctx, seq)) {
            return false;
        }
        ctx->recv_posted_total++;
    }
    return true;
}

static bool host_post_send_segment(host_ctx_t *ctx, int segment) {
    struct ibv_send_wr wr;
    struct ibv_send_wr *bad = NULL;
    struct ibv_sge sge;
    memset(&wr, 0, sizeof(wr));
    memset(&sge, 0, sizeof(sge));
    wr.wr_id = (uint64_t)segment;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;

    if (ctx->opts.primitive != EPIC_PRIM_BARRIER) {
        sge.addr = (uintptr_t)(ctx->send_buf + host_send_offset_for_segment(ctx, segment));
        sge.length = (uint32_t)ctx->opts.payload_bytes;
        sge.lkey = ctx->send_mr->lkey;
        wr.sg_list = &sge;
        wr.num_sge = 1;
    }

    if (ibv_post_send(ctx->qp, &wr, &bad)) {
        perror("ibv_post_send host");
        return false;
    }
    return true;
}

static bool host_verify_result(host_ctx_t *ctx) {
    if (ctx->opts.primitive == EPIC_PRIM_BARRIER || ctx->data_recv_segments <= 0) {
        return true;
    }

    uint32_t *buf = (uint32_t *)ctx->recv_buf;
    if (ctx->opts.primitive == EPIC_PRIM_ALLGATHER) {
        int block = ctx->opts.count / EPIC_STAR4_HOSTS;
        for (int r = 0; r < EPIC_STAR4_HOSTS; r++) {
            for (int i = 0; i < block; i++) {
                int idx = r * block + i;
                int32_t got = (int32_t)ntohl(buf[idx]);
                int32_t expected = idx * (r + 1);
                if (got != expected) {
                    fprintf(stderr, "rank=%d verify failed idx=%d got=%d expected=%d primitive=%s\n",
                            ctx->opts.rank, idx, got, expected,
                            epic_primitive_name(ctx->opts.primitive));
                    return false;
                }
            }
        }
        return true;
    }
    if (ctx->opts.primitive == EPIC_PRIM_REDUCESCATTER) {
        int block = ctx->opts.count / EPIC_STAR4_HOSTS;
        int start = ctx->opts.rank * block;
        int expected_multiplier = 0;
        for (int r = 0; r < EPIC_STAR4_HOSTS; r++) {
            expected_multiplier += r + 1;
        }
        for (int i = 0; i < block; i++) {
            int idx = start + i;
            int32_t got = (int32_t)ntohl(buf[idx]);
            int32_t expected = idx * expected_multiplier;
            if (got != expected) {
                fprintf(stderr, "rank=%d verify failed idx=%d got=%d expected=%d primitive=%s\n",
                        ctx->opts.rank, idx, got, expected,
                        epic_primitive_name(ctx->opts.primitive));
                return false;
            }
        }
        return true;
    }

    int expected_multiplier = 0;
    if (ctx->opts.primitive == EPIC_PRIM_BCAST) {
        expected_multiplier = ctx->opts.root_rank + 1;
    } else {
        for (int r = 0; r < EPIC_STAR4_HOSTS; r++) {
            expected_multiplier += r + 1;
        }
    }
    for (int i = 0; i < ctx->opts.count; i++) {
        int32_t got = (int32_t)ntohl(buf[i]);
        int32_t expected = i * expected_multiplier;
        if (got != expected) {
            fprintf(stderr, "rank=%d verify failed idx=%d got=%d expected=%d primitive=%s\n",
                    ctx->opts.rank, i, got, expected,
                    epic_primitive_name(ctx->opts.primitive));
            return false;
        }
    }
    return true;
}

static bool host_poll_until(host_ctx_t *ctx, int expected_send, int expected_recv,
                            int *send_posted) {
    int got_send = ctx->pending_send;
    int got_recv = ctx->pending_recv;
    struct ibv_wc wc[MODE1_MAX_POLL];
    ctx->pending_send = 0;
    ctx->pending_recv = 0;

    while (got_send < expected_send || got_recv < expected_recv) {
        int n = ibv_poll_cq(ctx->cq, MODE1_MAX_POLL, wc);
        if (n < 0) {
            fprintf(stderr, "ibv_poll_cq host failed: %d\n", n);
            return false;
        }
        if (n == 0) {
            continue;
        }
        for (int i = 0; i < n; i++) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                fprintf(stderr, "rank=%d completion failed wr_id=%" PRIu64 " status=%s opcode=%d vendor=0x%x\n",
                        ctx->opts.rank, wc[i].wr_id, ibv_wc_status_str(wc[i].status),
                        wc[i].opcode, wc[i].vendor_err);
                return false;
            }
            if (wc[i].opcode == IBV_WC_SEND) {
                got_send++;
                if (*send_posted < expected_send) {
                    if (!host_post_send_segment(ctx, *send_posted)) {
                        return false;
                    }
                    (*send_posted)++;
                }
            } else if (wc[i].opcode == IBV_WC_RECV) {
                got_recv++;
                ctx->recv_completed_total++;
                if (!host_refill_recv_window(ctx)) {
                    return false;
                }
            }
        }
    }
    if (got_send > expected_send) {
        ctx->pending_send = got_send - expected_send;
    }
    if (got_recv > expected_recv) {
        ctx->pending_recv = got_recv - expected_recv;
    }
    return true;
}

static bool host_run_one_iter(host_ctx_t *ctx, double *latency_us) {
    if (ctx->send_segments > 0) {
        fill_send_buffer(ctx);
    }

    int send_posted = 0;
    int initial = ctx->send_segments < ctx->opts.segment_window ? ctx->send_segments
                                                                : ctx->opts.segment_window;
    uint64_t t0 = now_ns();
    while (send_posted < initial) {
        if (!host_post_send_segment(ctx, send_posted)) {
            return false;
        }
        send_posted++;
    }
    if (!host_poll_until(ctx, ctx->send_segments, ctx->recv_entries_per_iter,
                         &send_posted)) {
        return false;
    }
    uint64_t t1 = now_ns();
    *latency_us = (double)(t1 - t0) / 1000.0;
    return host_verify_result(ctx);
}

static int run_host(mode1_opts_t *opts) {
    host_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.opts = *opts;
    if (!host_init_context(&ctx)) {
        return 1;
    }
    if (!host_write_endpoint(&ctx)) {
        return 1;
    }
    if (!host_connect_qp(&ctx)) {
        return 1;
    }

    char local_ip[INET_ADDRSTRLEN];
    char switch_ip[INET_ADDRSTRLEN];
    epic_ipv4_to_str(ctx.opts.local_ip, local_ip, sizeof(local_ip));
    epic_ipv4_to_str(ctx.opts.switch_ip, switch_ip, sizeof(switch_ip));
    printf("READY rank=%d qpn=%u primitive=%s mode=1 local_ip=%s switch_ip=%s gid_index=%d segments=%d send_segments=%d recv_segments=%d control_recv=%d segment_window=%d recv_window=%d qp_timeout=%d\n",
           ctx.opts.rank, ctx.qp->qp_num, epic_primitive_name(ctx.opts.primitive),
           local_ip, switch_ip, ctx.opts.gid_index, ctx.total_segments,
           ctx.send_segments, ctx.data_recv_segments, ctx.control_recv_segments,
           ctx.opts.segment_window, ctx.opts.recv_window, ctx.opts.qp_timeout);
    fflush(stdout);

    if (ctx.recv_entries_per_iter > 0) {
        ctx.recv_total = ctx.recv_entries_per_iter * (ctx.opts.warmup + ctx.opts.iters);
        if (!host_refill_recv_window(&ctx)) {
            return 1;
        }
    }

    wait_for_start(ctx.opts.start_file);

    double total_us = 0.0;
    for (int i = 0; i < ctx.opts.warmup; i++) {
        double latency_us = 0.0;
        if (!host_run_one_iter(&ctx, &latency_us)) {
            return 1;
        }
        printf("WARMUP rank=%d iter=%d latency_us=%.3f\n", ctx.opts.rank, i, latency_us);
        fflush(stdout);
    }
    for (int i = 0; i < ctx.opts.iters; i++) {
        double latency_us = 0.0;
        if (!host_run_one_iter(&ctx, &latency_us)) {
            return 1;
        }
        total_us += latency_us;
        printf("ITER rank=%d iter=%d latency_us=%.3f\n", ctx.opts.rank, i, latency_us);
        fflush(stdout);
    }
    printf("RESULT rank=%d primitive=%s mode=1 count=%d warmup=%d iters=%d segment_window=%d recv_window=%d qp_timeout=%d avg_latency_us=%.3f\n",
           ctx.opts.rank, epic_primitive_name(ctx.opts.primitive), ctx.opts.count,
           ctx.opts.warmup, ctx.opts.iters, ctx.opts.segment_window,
           ctx.opts.recv_window, ctx.opts.qp_timeout, total_us / ctx.opts.iters);
    return 0;
}

static char *trim(char *s) {
    while (*s == ' ' || *s == '\t' || *s == '\n' || *s == '\r') {
        s++;
    }
    char *end = s + strlen(s);
    while (end > s && (end[-1] == ' ' || end[-1] == '\t' ||
                       end[-1] == '\n' || end[-1] == '\r')) {
        *--end = '\0';
    }
    return s;
}

static bool switch_parse_config(switch_ctx_t *ctx) {
    FILE *f = fopen(ctx->opts.config_file, "r");
    if (!f) {
        perror("fopen config");
        return false;
    }
    for (int i = 0; i < EPIC_STAR4_HOSTS; i++) {
        ctx->ports[i].rank = i;
        ctx->ports[i].recv_window = ctx->opts.recv_window;
        ctx->ports[i].send_window = ctx->opts.send_window;
    }

    char line[512];
    while (fgets(line, sizeof(line), f)) {
        char *p = trim(line);
        if (*p == '\0' || *p == '#') {
            continue;
        }
        char *eq = strchr(p, '=');
        if (!eq) {
            continue;
        }
        *eq = '\0';
        char *key = trim(p);
        char *value = trim(eq + 1);
        if (strcmp(key, "primitive") == 0) {
            if (!epic_parse_primitive(value, &ctx->opts.primitive)) {
                fclose(f);
                return false;
            }
        } else if (strcmp(key, "root") == 0) {
            ctx->opts.root_rank = (int)parse_u32_auto(value);
        } else if (strcmp(key, "count") == 0) {
            ctx->opts.count = (int)parse_u32_auto(value);
        } else if (strcmp(key, "iters") == 0) {
            ctx->opts.iters = (int)parse_u32_auto(value);
        } else if (strcmp(key, "warmup") == 0) {
            ctx->opts.warmup = (int)parse_u32_auto(value);
        } else if (strcmp(key, "payload_bytes") == 0) {
            ctx->opts.payload_bytes = (int)parse_u32_auto(value);
        } else if (strcmp(key, "segment_window") == 0) {
            ctx->opts.segment_window = (int)parse_u32_auto(value);
        } else if (strcmp(key, "recv_window") == 0) {
            ctx->opts.recv_window = (int)parse_u32_auto(value);
        } else if (strcmp(key, "send_window") == 0) {
            ctx->opts.send_window = (int)parse_u32_auto(value);
        } else if (strcmp(key, "qp_timeout") == 0) {
            ctx->opts.qp_timeout = (int)parse_u32_auto(value);
        } else {
            int port = -1;
            char field[64];
            if (sscanf(key, "port%d.%63s", &port, field) == 2 &&
                port >= 0 && port < EPIC_STAR4_HOSTS) {
                switch_port_t *sp = &ctx->ports[port];
                if (strcmp(field, "dev") == 0) {
                    snprintf(sp->dev, sizeof(sp->dev), "%s", value);
                } else if (strcmp(field, "host_ip") == 0) {
                    if (!epic_parse_ipv4(value, &sp->host_ip)) {
                        fclose(f);
                        return false;
                    }
                } else if (strcmp(field, "switch_ip") == 0) {
                    if (!epic_parse_ipv4(value, &sp->switch_ip)) {
                        fclose(f);
                        return false;
                    }
                } else if (strcmp(field, "host_qpn") == 0) {
                    sp->host_qpn = parse_u32_auto(value);
                }
            }
        }
    }
    fclose(f);

    if (!validate_layout(ctx->opts.primitive, ctx->opts.count, ctx->opts.payload_bytes)) {
        return false;
    }
    if (ctx->opts.root_rank < 0 || ctx->opts.root_rank >= EPIC_STAR4_HOSTS) {
        return false;
    }
    for (int i = 0; i < EPIC_STAR4_HOSTS; i++) {
        if (ctx->ports[i].dev[0] == '\0' || ctx->ports[i].host_ip == 0 ||
            ctx->ports[i].switch_ip == 0 || ctx->ports[i].host_qpn == 0) {
            fprintf(stderr, "incomplete switch port %d config\n", i);
            return false;
        }
        ctx->ports[i].recv_window = ctx->opts.recv_window;
        ctx->ports[i].send_window = ctx->opts.send_window;
    }
    return true;
}

static int source_segments_for_rank(switch_ctx_t *ctx, int rank) {
    epic_primitive_t p = ctx->opts.primitive;
    if (!is_source_rank(p, rank, ctx->opts.root_rank)) {
        return 0;
    }
    if (p == EPIC_PRIM_BARRIER) {
        return 1;
    }
    if (p == EPIC_PRIM_ALLGATHER) {
        return ctx->block_segments;
    }
    return ctx->total_segments;
}

static int data_recv_segments_for_rank(switch_ctx_t *ctx, int rank) {
    epic_primitive_t p = ctx->opts.primitive;
    if (p == EPIC_PRIM_BARRIER) {
        return 1;
    }
    if (p == EPIC_PRIM_ALLGATHER) {
        return ctx->block_segments * (EPIC_STAR4_HOSTS - 1);
    }
    if (p == EPIC_PRIM_REDUCESCATTER) {
        return ctx->block_segments;
    }
    if (p == EPIC_PRIM_REDUCE) {
        return rank == ctx->opts.root_rank ? ctx->total_segments : 0;
    }
    if (p == EPIC_PRIM_BCAST) {
        return rank != ctx->opts.root_rank ? ctx->total_segments : 0;
    }
    return ctx->total_segments;
}

static bool needs_release_for_rank(switch_ctx_t *ctx, int rank) {
    return source_segments_for_rank(ctx, rank) > 0;
}

static bool switch_write_endpoint(switch_ctx_t *ctx, int rank) {
    char path[512];
    snprintf(path, sizeof(path), "%s/switch%d.endpoint", ctx->opts.endpoint_dir, rank);
    FILE *f = fopen(path, "w");
    if (!f) {
        perror("fopen switch endpoint");
        return false;
    }
    switch_port_t *sp = &ctx->ports[rank];
    char switch_ip[INET_ADDRSTRLEN];
    char host_ip[INET_ADDRSTRLEN];
    epic_ipv4_to_str(sp->switch_ip, switch_ip, sizeof(switch_ip));
    epic_ipv4_to_str(sp->host_ip, host_ip, sizeof(host_ip));
    fprintf(f, "rank=%d\n", rank);
    fprintf(f, "qpn=%u\n", sp->qp->qp_num);
    fprintf(f, "gid_index=%d\n", sp->gid_index);
    fprintf(f, "ib_port=%d\n", ctx->opts.ib_port);
    fprintf(f, "local_ip=%s\n", switch_ip);
    fprintf(f, "host_ip=%s\n", host_ip);
    fclose(f);
    return true;
}

static bool switch_open_port(switch_ctx_t *ctx, int rank) {
    switch_port_t *sp = &ctx->ports[rank];
    sp->ib_ctx = open_selected_device(sp->dev);
    if (!sp->ib_ctx) {
        return false;
    }
    sp->gid_index = find_gid_index(sp->ib_ctx, ctx->opts.ib_port, sp->switch_ip,
                                   EPIC_DEFAULT_GID_INDEX);
    struct ibv_port_attr port_attr;
    if (ibv_query_port(sp->ib_ctx, ctx->opts.ib_port, &port_attr)) {
        perror("ibv_query_port switch");
        return false;
    }
    sp->path_mtu = clamp_mtu(port_attr.active_mtu,
                             ctx->opts.payload_bytes > 0 ? ctx->opts.payload_bytes : 1);
    if (ibv_query_gid(sp->ib_ctx, ctx->opts.ib_port, sp->gid_index,
                      &sp->local_gid)) {
        perror("ibv_query_gid switch");
        return false;
    }
    gid_for_ip(&sp->host_gid, &sp->local_gid, sp->host_ip);

    sp->pd = ibv_alloc_pd(sp->ib_ctx);
    if (!sp->pd) {
        perror("ibv_alloc_pd switch");
        return false;
    }
    int cq_depth = sp->recv_window + sp->send_window + 256;
    sp->send_cq = ibv_create_cq(sp->ib_ctx, cq_depth, NULL, NULL, 0);
    sp->recv_cq = ibv_create_cq(sp->ib_ctx, cq_depth, NULL, NULL, 0);
    if (!sp->send_cq || !sp->recv_cq) {
        perror("ibv_create_cq switch");
        return false;
    }

    struct ibv_qp_init_attr qpia;
    memset(&qpia, 0, sizeof(qpia));
    qpia.qp_type = IBV_QPT_RC;
    qpia.sq_sig_all = 0;
    qpia.send_cq = sp->send_cq;
    qpia.recv_cq = sp->recv_cq;
    qpia.cap.max_send_wr = sp->send_window + 16;
    qpia.cap.max_recv_wr = sp->recv_window + 16;
    qpia.cap.max_send_sge = 1;
    qpia.cap.max_recv_sge = 1;
    sp->qp = ibv_create_qp(sp->pd, &qpia);
    if (!sp->qp) {
        perror("ibv_create_qp switch");
        return false;
    }

    int mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                   IBV_ACCESS_REMOTE_READ;
    if (source_segments_for_rank(ctx, rank) > 0 &&
        ctx->opts.primitive != EPIC_PRIM_BARRIER) {
        size_t recv_bytes = (size_t)sp->recv_window * (size_t)ctx->opts.payload_bytes;
        if (posix_memalign((void **)&sp->recv_area, 4096, recv_bytes)) {
            perror("posix_memalign switch recv");
            return false;
        }
        sp->recv_mr = ibv_reg_mr(sp->pd, sp->recv_area, recv_bytes, mr_flags);
        if (!sp->recv_mr) {
            perror("ibv_reg_mr switch recv");
            return false;
        }
    }
    if (data_recv_segments_for_rank(ctx, rank) > 0 &&
        ctx->opts.primitive != EPIC_PRIM_BARRIER) {
        size_t send_bytes = (size_t)sp->send_window * (size_t)ctx->opts.payload_bytes;
        if (posix_memalign((void **)&sp->send_area, 4096, send_bytes)) {
            perror("posix_memalign switch send");
            return false;
        }
        sp->send_mr = ibv_reg_mr(sp->pd, sp->send_area, send_bytes, mr_flags);
        if (!sp->send_mr) {
            perror("ibv_reg_mr switch send");
            return false;
        }
    }

    if (!modify_qp_init(sp->qp, ctx->opts.ib_port) ||
        !modify_qp_rtr(sp->qp, ctx->opts.ib_port, sp->gid_index, sp->path_mtu,
                       sp->host_qpn, &sp->host_gid) ||
        !modify_qp_rts(sp->qp, ctx->opts.qp_timeout)) {
        perror("ibv_modify_qp switch");
        return false;
    }
    return switch_write_endpoint(ctx, rank);
}

static bool switch_post_recv(switch_ctx_t *ctx, int rank) {
    switch_port_t *sp = &ctx->ports[rank];
    if (sp->recv_posted_total >= sp->recv_total) {
        return true;
    }
    struct ibv_recv_wr wr;
    struct ibv_recv_wr *bad = NULL;
    struct ibv_sge sge;
    memset(&wr, 0, sizeof(wr));
    memset(&sge, 0, sizeof(sge));
    int slot = sp->recv_posted_total % sp->recv_window;
    wr.wr_id = (uint64_t)slot;
    if (ctx->opts.primitive != EPIC_PRIM_BARRIER) {
        sge.addr = (uintptr_t)(sp->recv_area +
                               (size_t)slot * (size_t)ctx->opts.payload_bytes);
        sge.length = (uint32_t)ctx->opts.payload_bytes;
        sge.lkey = sp->recv_mr->lkey;
        wr.sg_list = &sge;
        wr.num_sge = 1;
    }
    if (ibv_post_recv(sp->qp, &wr, &bad)) {
        perror("ibv_post_recv switch");
        return false;
    }
    sp->recv_posted_total++;
    return true;
}

static bool switch_refill_recvs(switch_ctx_t *ctx, int rank) {
    switch_port_t *sp = &ctx->ports[rank];
    while (sp->recv_posted_total < sp->recv_total &&
           sp->recv_posted_total - sp->recv_completed_total < sp->recv_window) {
        if (!switch_post_recv(ctx, rank)) {
            return false;
        }
    }
    return true;
}

static bool switch_poll_send_port(switch_port_t *sp) {
    struct ibv_wc wc[MODE1_MAX_POLL];
    int n;
    do {
        n = ibv_poll_cq(sp->send_cq, MODE1_MAX_POLL, wc);
        if (n < 0) {
            fprintf(stderr, "ibv_poll_cq send failed: %d\n", n);
            return false;
        }
        for (int i = 0; i < n; i++) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                fprintf(stderr, "switch send completion failed rank=%d wr_id=%" PRIu64 " status=%s vendor=0x%x\n",
                        sp->rank, wc[i].wr_id, ibv_wc_status_str(wc[i].status),
                        wc[i].vendor_err);
                return false;
            }
            sp->pending_send--;
        }
    } while (n == MODE1_MAX_POLL);
    return true;
}

static bool switch_wait_send_slot(switch_ctx_t *ctx, int rank) {
    switch_port_t *sp = &ctx->ports[rank];
    while (sp->pending_send >= sp->send_window) {
        if (!switch_poll_send_port(sp)) {
            return false;
        }
    }
    for (int i = 0; i < EPIC_STAR4_HOSTS; i++) {
        if (!switch_poll_send_port(&ctx->ports[i])) {
            return false;
        }
    }
    return true;
}

static bool switch_post_send_data(switch_ctx_t *ctx, int rank, const uint8_t *data,
                                  size_t len, bool release) {
    switch_port_t *sp = &ctx->ports[rank];
    if (!switch_wait_send_slot(ctx, rank)) {
        return false;
    }

    struct ibv_send_wr wr;
    struct ibv_send_wr *bad = NULL;
    struct ibv_sge sge;
    memset(&wr, 0, sizeof(wr));
    memset(&sge, 0, sizeof(sge));
    wr.wr_id = release ? 0xf0000000u : (uint64_t)sp->send_head;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;

    if (len > 0) {
        int slot = sp->send_head;
        uint8_t *dst = sp->send_area + (size_t)slot * (size_t)ctx->opts.payload_bytes;
        memcpy(dst, data, len);
        sge.addr = (uintptr_t)dst;
        sge.length = (uint32_t)len;
        sge.lkey = sp->send_mr->lkey;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        sp->send_head = (sp->send_head + 1) % sp->send_window;
    }

    if (ibv_post_send(sp->qp, &wr, &bad)) {
        perror("ibv_post_send switch");
        return false;
    }
    sp->pending_send++;
    if (release) {
        ctx->release_msg++;
    } else {
        ctx->tx_msg++;
    }
    return true;
}

static bool switch_wait_all_sends(switch_ctx_t *ctx) {
    bool done = false;
    while (!done) {
        done = true;
        for (int i = 0; i < EPIC_STAR4_HOSTS; i++) {
            if (!switch_poll_send_port(&ctx->ports[i])) {
                return false;
            }
            if (ctx->ports[i].pending_send > 0) {
                done = false;
            }
        }
    }
    return true;
}

static void aggregate_add(uint8_t *dst_bytes, const uint8_t *src_bytes, size_t bytes) {
    uint32_t *dst = (uint32_t *)dst_bytes;
    const uint32_t *src = (const uint32_t *)src_bytes;
    size_t words = bytes / sizeof(uint32_t);
    for (size_t i = 0; i < words; i++) {
        uint32_t sum = ntohl(dst[i]) + ntohl(src[i]);
        dst[i] = htonl(sum);
    }
}

static bool switch_drain_ready(switch_ctx_t *ctx) {
    epic_primitive_t p = ctx->opts.primitive;
    if (p == EPIC_PRIM_ALLREDUCE) {
        while (ctx->next_segment < ctx->total_segments &&
               ctx->degree[ctx->next_segment] == EPIC_STAR4_HOSTS) {
            uint8_t *data = ctx->aggregate_buf +
                            (size_t)ctx->next_segment * (size_t)ctx->opts.payload_bytes;
            for (int r = 0; r < EPIC_STAR4_HOSTS; r++) {
                if (!switch_post_send_data(ctx, r, data, ctx->opts.payload_bytes, false)) {
                    return false;
                }
            }
            ctx->next_segment++;
        }
    } else if (p == EPIC_PRIM_REDUCE) {
        while (ctx->next_segment < ctx->total_segments &&
               ctx->degree[ctx->next_segment] == EPIC_STAR4_HOSTS) {
            uint8_t *data = ctx->aggregate_buf +
                            (size_t)ctx->next_segment * (size_t)ctx->opts.payload_bytes;
            if (!switch_post_send_data(ctx, ctx->opts.root_rank, data,
                                       ctx->opts.payload_bytes, false)) {
                return false;
            }
            ctx->next_segment++;
        }
    } else if (p == EPIC_PRIM_REDUCESCATTER) {
        for (int r = 0; r < EPIC_STAR4_HOSTS; r++) {
            int end = (r + 1) * ctx->block_segments;
            while (ctx->next_rs_segment[r] < end &&
                   ctx->degree[ctx->next_rs_segment[r]] == EPIC_STAR4_HOSTS) {
                uint8_t *data = ctx->aggregate_buf +
                                (size_t)ctx->next_rs_segment[r] *
                                    (size_t)ctx->opts.payload_bytes;
                if (!switch_post_send_data(ctx, r, data, ctx->opts.payload_bytes, false)) {
                    return false;
                }
                ctx->next_rs_segment[r]++;
            }
        }
    } else if (p == EPIC_PRIM_BCAST) {
        while (ctx->next_segment < ctx->total_segments &&
               ctx->degree[ctx->next_segment] == 1) {
            uint8_t *data = ctx->aggregate_buf +
                            (size_t)ctx->next_segment * (size_t)ctx->opts.payload_bytes;
            for (int r = 0; r < EPIC_STAR4_HOSTS; r++) {
                if (r == ctx->opts.root_rank) {
                    continue;
                }
                if (!switch_post_send_data(ctx, r, data, ctx->opts.payload_bytes, false)) {
                    return false;
                }
            }
            ctx->next_segment++;
        }
    } else if (p == EPIC_PRIM_ALLGATHER) {
        int peers = EPIC_STAR4_HOSTS - 1;
        size_t block_bytes = ctx->total_bytes / EPIC_STAR4_HOSTS;
        for (int dst = 0; dst < EPIC_STAR4_HOSTS; dst++) {
            while (ctx->next_ag_seq[dst] < ctx->block_segments * peers) {
                int seq = ctx->next_ag_seq[dst];
                int segment = seq / peers;
                int ordinal = seq % peers;
                int src = rank_from_ordinal_excluding(dst, ordinal);
                size_t ready_idx = (size_t)src * (size_t)ctx->block_segments +
                                   (size_t)segment;
                if (!ctx->ready[ready_idx]) {
                    break;
                }
                uint8_t *data = ctx->gather_buf + (size_t)src * block_bytes +
                                (size_t)segment * (size_t)ctx->opts.payload_bytes;
                if (!switch_post_send_data(ctx, dst, data, ctx->opts.payload_bytes, false)) {
                    return false;
                }
                ctx->next_ag_seq[dst]++;
            }
        }
    }
    return true;
}

static bool switch_process_recv(switch_ctx_t *ctx, int rank, const uint8_t *data,
                                size_t len) {
    switch_port_t *sp = &ctx->ports[rank];
    int seq = sp->recv_seq_iter++;
    epic_primitive_t p = ctx->opts.primitive;
    ctx->rx_msg++;

    if (p == EPIC_PRIM_BARRIER) {
        ctx->degree[0]++;
        return true;
    }
    if (len != (size_t)ctx->opts.payload_bytes) {
        fprintf(stderr, "switch rank=%d bad recv len=%zu expected=%d\n",
                rank, len, ctx->opts.payload_bytes);
        return false;
    }

    if (p == EPIC_PRIM_ALLREDUCE || p == EPIC_PRIM_REDUCE ||
        p == EPIC_PRIM_REDUCESCATTER) {
        int segment = seq;
        if (segment < 0 || segment >= ctx->total_segments) {
            fprintf(stderr, "switch rank=%d bad segment=%d\n", rank, segment);
            return false;
        }
        uint8_t *dst = ctx->aggregate_buf +
                       (size_t)segment * (size_t)ctx->opts.payload_bytes;
        if (ctx->degree[segment] == 0) {
            memset(dst, 0, (size_t)ctx->opts.payload_bytes);
        }
        aggregate_add(dst, data, (size_t)ctx->opts.payload_bytes);
        ctx->degree[segment]++;
        return switch_drain_ready(ctx);
    }
    if (p == EPIC_PRIM_BCAST) {
        int segment = seq;
        uint8_t *dst = ctx->aggregate_buf +
                       (size_t)segment * (size_t)ctx->opts.payload_bytes;
        memcpy(dst, data, (size_t)ctx->opts.payload_bytes);
        ctx->degree[segment] = 1;
        return switch_drain_ready(ctx);
    }
    if (p == EPIC_PRIM_ALLGATHER) {
        int segment = seq;
        if (segment < 0 || segment >= ctx->block_segments) {
            fprintf(stderr, "switch allgather rank=%d bad segment=%d\n", rank, segment);
            return false;
        }
        size_t block_bytes = ctx->total_bytes / EPIC_STAR4_HOSTS;
        uint8_t *dst = ctx->gather_buf + (size_t)rank * block_bytes +
                       (size_t)segment * (size_t)ctx->opts.payload_bytes;
        memcpy(dst, data, (size_t)ctx->opts.payload_bytes);
        ctx->ready[(size_t)rank * (size_t)ctx->block_segments + (size_t)segment] = 1;
        return switch_drain_ready(ctx);
    }
    return false;
}

static bool switch_poll_recvs(switch_ctx_t *ctx, int *processed) {
    struct ibv_wc wc[MODE1_MAX_POLL];
    for (int r = 0; r < EPIC_STAR4_HOSTS; r++) {
        switch_port_t *sp = &ctx->ports[r];
        int n = ibv_poll_cq(sp->recv_cq, MODE1_MAX_POLL, wc);
        if (n < 0) {
            fprintf(stderr, "ibv_poll_cq recv failed: %d\n", n);
            return false;
        }
        for (int i = 0; i < n; i++) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                fprintf(stderr, "switch recv completion failed rank=%d wr_id=%" PRIu64 " status=%s vendor=0x%x\n",
                        r, wc[i].wr_id, ibv_wc_status_str(wc[i].status),
                        wc[i].vendor_err);
                return false;
            }
            int slot = (int)wc[i].wr_id;
            const uint8_t *data = NULL;
            if (ctx->opts.primitive != EPIC_PRIM_BARRIER) {
                data = sp->recv_area + (size_t)slot * (size_t)ctx->opts.payload_bytes;
            }
            if (!switch_process_recv(ctx, r, data, wc[i].byte_len)) {
                return false;
            }
            sp->recv_completed_total++;
            (*processed)++;
            if (!switch_refill_recvs(ctx, r)) {
                return false;
            }
        }
    }
    for (int r = 0; r < EPIC_STAR4_HOSTS; r++) {
        if (!switch_poll_send_port(&ctx->ports[r])) {
            return false;
        }
    }
    return true;
}

static bool switch_reset_iteration(switch_ctx_t *ctx) {
    for (int r = 0; r < EPIC_STAR4_HOSTS; r++) {
        ctx->ports[r].recv_seq_iter = 0;
        ctx->next_rs_segment[r] = r * ctx->block_segments;
        ctx->next_ag_seq[r] = 0;
    }
    ctx->next_segment = 0;
    if (ctx->degree) {
        memset(ctx->degree, 0, (size_t)ctx->total_segments * sizeof(ctx->degree[0]));
    }
    if (ctx->ready) {
        memset(ctx->ready, 0,
               (size_t)EPIC_STAR4_HOSTS * (size_t)ctx->block_segments *
                   sizeof(ctx->ready[0]));
    }
    return true;
}

static int switch_expected_rx_iter(switch_ctx_t *ctx) {
    int total = 0;
    for (int r = 0; r < EPIC_STAR4_HOSTS; r++) {
        total += source_segments_for_rank(ctx, r);
    }
    return total;
}

static bool switch_finish_iteration(switch_ctx_t *ctx) {
    if (ctx->opts.primitive == EPIC_PRIM_BARRIER) {
        for (int r = 0; r < EPIC_STAR4_HOSTS; r++) {
            if (!switch_post_send_data(ctx, r, NULL, 0, false)) {
                return false;
            }
        }
    } else if (!switch_drain_ready(ctx)) {
        return false;
    }
    if (!switch_wait_all_sends(ctx)) {
        return false;
    }
    for (int r = 0; r < EPIC_STAR4_HOSTS; r++) {
        if (needs_release_for_rank(ctx, r)) {
            if (!switch_post_send_data(ctx, r, NULL, 0, true)) {
                return false;
            }
        }
    }
    return switch_wait_all_sends(ctx);
}

static bool switch_run_one_iter(switch_ctx_t *ctx, double *latency_us) {
    if (!switch_reset_iteration(ctx)) {
        return false;
    }
    int expected_rx = switch_expected_rx_iter(ctx);
    int processed = 0;
    uint64_t t0 = now_ns();
    while (processed < expected_rx) {
        if (!switch_poll_recvs(ctx, &processed)) {
            return false;
        }
    }
    if (!switch_finish_iteration(ctx)) {
        return false;
    }
    uint64_t t1 = now_ns();
    *latency_us = (double)(t1 - t0) / 1000.0;
    return true;
}

static bool switch_init_context(switch_ctx_t *ctx) {
    if (!switch_parse_config(ctx)) {
        return false;
    }
    if (ctx->opts.primitive == EPIC_PRIM_BARRIER) {
        ctx->total_segments = 1;
        ctx->block_segments = 1;
    } else {
        ctx->total_bytes = (size_t)ctx->opts.count * sizeof(int32_t);
        ctx->total_segments = (int)(ctx->total_bytes / (size_t)ctx->opts.payload_bytes);
        ctx->block_segments = ctx->total_segments / EPIC_STAR4_HOSTS;
    }

    if (ctx->opts.primitive == EPIC_PRIM_ALLREDUCE ||
        ctx->opts.primitive == EPIC_PRIM_REDUCE ||
        ctx->opts.primitive == EPIC_PRIM_REDUCESCATTER ||
        ctx->opts.primitive == EPIC_PRIM_BCAST) {
        if (posix_memalign((void **)&ctx->aggregate_buf, 4096,
                           (size_t)ctx->total_segments *
                               (size_t)(ctx->opts.payload_bytes > 0
                                            ? ctx->opts.payload_bytes
                                            : 1))) {
            perror("posix_memalign aggregate");
            return false;
        }
    }
    if (ctx->opts.primitive == EPIC_PRIM_ALLGATHER) {
        if (posix_memalign((void **)&ctx->gather_buf, 4096, ctx->total_bytes)) {
            perror("posix_memalign gather");
            return false;
        }
        ctx->ready = calloc((size_t)EPIC_STAR4_HOSTS * (size_t)ctx->block_segments,
                            sizeof(ctx->ready[0]));
        if (!ctx->ready) {
            perror("calloc ready");
            return false;
        }
    }
    ctx->degree = calloc((size_t)ctx->total_segments, sizeof(ctx->degree[0]));
    if (!ctx->degree) {
        perror("calloc degree");
        return false;
    }

    if (mkdir(ctx->opts.endpoint_dir, 0775) && errno != EEXIST) {
        perror("mkdir endpoint-dir");
        return false;
    }

    int total_iters = ctx->opts.warmup + ctx->opts.iters;
    for (int r = 0; r < EPIC_STAR4_HOSTS; r++) {
        switch_port_t *sp = &ctx->ports[r];
        int source_segments = source_segments_for_rank(ctx, r);
        sp->recv_total = source_segments * total_iters;
        if (sp->recv_window > sp->recv_total && sp->recv_total > 0) {
            sp->recv_window = sp->recv_total;
        }
        if (sp->recv_window <= 0) {
            sp->recv_window = 1;
        }
        if (!switch_open_port(ctx, r)) {
            return false;
        }
    }
    for (int r = 0; r < EPIC_STAR4_HOSTS; r++) {
        if (!switch_refill_recvs(ctx, r)) {
            return false;
        }
    }
    return true;
}

static int run_switch(mode1_opts_t *opts) {
    switch_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.opts = *opts;
    if (!switch_init_context(&ctx)) {
        return 1;
    }

    printf("SWITCH_READY primitive=%s mode=1 root=%d payload_bytes=%d total_segments=%d segment_window=%d recv_window=%d send_window=%d\n",
           epic_primitive_name(ctx.opts.primitive), ctx.opts.root_rank,
           ctx.opts.payload_bytes, ctx.total_segments, ctx.opts.segment_window,
           ctx.opts.recv_window, ctx.opts.send_window);
    fflush(stdout);

    wait_for_start(ctx.opts.start_file);

    double total_us = 0.0;
    for (int i = 0; i < ctx.opts.warmup; i++) {
        double latency_us = 0.0;
        if (!switch_run_one_iter(&ctx, &latency_us)) {
            return 1;
        }
        printf("SWITCH_WARMUP iter=%d latency_us=%.3f\n", i, latency_us);
        fflush(stdout);
    }
    for (int i = 0; i < ctx.opts.iters; i++) {
        double latency_us = 0.0;
        if (!switch_run_one_iter(&ctx, &latency_us)) {
            return 1;
        }
        total_us += latency_us;
        printf("SWITCH_ITER iter=%d latency_us=%.3f\n", i, latency_us);
        fflush(stdout);
    }
    printf("SWITCH_RESULT status=ok primitive=%s mode=1 count=%d warmup=%d iters=%d rx_msg=%" PRIu64 " tx_msg=%" PRIu64 " release_msg=%" PRIu64 " avg_latency_us=%.3f\n",
           epic_primitive_name(ctx.opts.primitive), ctx.opts.count, ctx.opts.warmup,
           ctx.opts.iters, ctx.rx_msg, ctx.tx_msg, ctx.release_msg,
           total_us / ctx.opts.iters);
    return 0;
}

int main(int argc, char **argv) {
    mode1_opts_t opts;
    if (!parse_args(argc, argv, &opts)) {
        usage(argv[0]);
        return 2;
    }
    if (opts.role == MODE1_ROLE_HOST) {
        return run_host(&opts);
    }
    return run_switch(&opts);
}
