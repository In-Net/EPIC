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

typedef struct {
    int rank;
    epic_primitive_t primitive;
    epic_mode_t mode;
    int root_rank;
    int count;
    int iters;
    int warmup;
    int payload_bytes;
    int segment_window;
    int recv_window;
    int qp_timeout;
    char ib_dev[64];
    int ib_port;
    int gid_index;
    bool auto_gid_index;
    uint32_t local_ip;
    uint32_t switch_ip;
    uint32_t switch_qpn;
    const char *endpoint_file;
    const char *start_file;
} host_opts_t;

typedef struct {
    host_opts_t opts;
    struct ibv_context *ib_ctx;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_qp *qp;
    struct ibv_mr *send_mr;
    struct ibv_mr *recv_mr;
    uint8_t *send_buf;
    uint8_t *recv_buf;
    size_t total_bytes;
    int segments;
    int total_segments;
    int send_segments;
    int recv_segments;
    int block_segments;
    int pending_send;
    int pending_recv;
    int recv_total;
    int recv_posted_total;
    int recv_completed_total;
    union ibv_gid local_gid;
    union ibv_gid switch_gid;
    enum ibv_mtu path_mtu;
} host_ctx_t;

static bool is_source(const host_opts_t *opts);
static bool is_sink(const host_opts_t *opts);
static bool is_barrier(const host_opts_t *opts);
static int expected_recv_count(host_ctx_t *ctx);

static void usage(const char *prog) {
    fprintf(stderr,
            "usage: %s --rank N --primitive allreduce|reduce|broadcast|allgather|reducescatter|barrier --mode 2|3 "
            "--root N --count N --iters N [--warmup N] --switch-ip IP --local-ip IP "
            "--switch-qpn QPN --endpoint-file PATH --start-file PATH "
            "[--segment-window N] [--recv-window N] [--qp-timeout N] [--dev DEV]\n",
            prog);
}

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

static bool parse_args(int argc, char **argv, host_opts_t *opts) {
    memset(opts, 0, sizeof(*opts));
    opts->rank = -1;
    opts->mode = EPIC_MODE_NON_TERMINATION;
    opts->root_rank = 0;
    opts->count = EPIC_DEFAULT_COUNT;
    opts->iters = EPIC_DEFAULT_ITERS;
    opts->payload_bytes = EPIC_DEFAULT_PAYLOAD_BYTES;
    opts->segment_window = EPIC_DEFAULT_SEGMENT_WINDOW;
    opts->recv_window = EPIC_DEFAULT_RECV_WINDOW;
    opts->qp_timeout = EPIC_DEFAULT_QP_TIMEOUT;
    opts->ib_port = EPIC_DEFAULT_IB_PORT;
    opts->gid_index = EPIC_DEFAULT_GID_INDEX;
    opts->auto_gid_index = true;
    opts->switch_qpn = EPIC_FAKE_SWITCH_QPN_BASE;

    static const struct option long_opts[] = {
        {"rank", required_argument, NULL, 'r'},
        {"primitive", required_argument, NULL, 'p'},
        {"mode", required_argument, NULL, 'm'},
        {"root", required_argument, NULL, 'R'},
        {"count", required_argument, NULL, 'c'},
        {"iters", required_argument, NULL, 'i'},
        {"warmup", required_argument, NULL, 'w'},
        {"payload-bytes", required_argument, NULL, 'b'},
        {"segment-window", required_argument, NULL, 'W'},
        {"recv-window", required_argument, NULL, 'V'},
        {"qp-timeout", required_argument, NULL, 'T'},
        {"switch-ip", required_argument, NULL, 's'},
        {"local-ip", required_argument, NULL, 'l'},
        {"switch-qpn", required_argument, NULL, 'q'},
        {"endpoint-file", required_argument, NULL, 'e'},
        {"start-file", required_argument, NULL, 'S'},
        {"dev", required_argument, NULL, 'd'},
        {"gid-index", required_argument, NULL, 'g'},
        {"ib-port", required_argument, NULL, 'P'},
        {0, 0, 0, 0},
    };

    int c;
    while ((c = getopt_long(argc, argv, "", long_opts, NULL)) != -1) {
        switch (c) {
        case 'r':
            opts->rank = (int)parse_u32_auto(optarg);
            break;
        case 'p':
            if (!epic_parse_primitive(optarg, &opts->primitive)) {
                fprintf(stderr, "invalid primitive: %s\n", optarg);
                return false;
            }
            break;
        case 'm':
            if (!epic_parse_mode(optarg, &opts->mode)) {
                fprintf(stderr, "invalid mode: %s\n", optarg);
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
        case 'T':
            opts->qp_timeout = (int)parse_u32_auto(optarg);
            break;
        case 's':
            if (!epic_parse_ipv4(optarg, &opts->switch_ip)) {
                fprintf(stderr, "invalid switch ip: %s\n", optarg);
                return false;
            }
            break;
        case 'l':
            if (!epic_parse_ipv4(optarg, &opts->local_ip)) {
                fprintf(stderr, "invalid local ip: %s\n", optarg);
                return false;
            }
            break;
        case 'q':
            opts->switch_qpn = parse_u32_auto(optarg);
            break;
        case 'e':
            opts->endpoint_file = optarg;
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
        default:
            return false;
        }
    }

    if (opts->rank < 0 || opts->rank >= EPIC_STAR4_HOSTS ||
        opts->root_rank < 0 || opts->root_rank >= EPIC_STAR4_HOSTS ||
        ((opts->count <= 0 && opts->primitive != EPIC_PRIM_BARRIER) ||
         opts->count < 0) ||
        opts->iters <= 0 || opts->warmup < 0 ||
        opts->segment_window <= 0 || opts->segment_window > EPIC_PSN_WINDOW ||
        opts->recv_window <= 0 || opts->recv_window > EPIC_PSN_WINDOW ||
        opts->qp_timeout < 0 || opts->qp_timeout > 31 ||
        ((opts->payload_bytes <= 0 && opts->primitive != EPIC_PRIM_BARRIER) ||
         opts->payload_bytes < 0) ||
        opts->payload_bytes > EPIC_MAX_PAYLOAD_BYTES ||
        opts->payload_bytes % (int)sizeof(uint32_t) != 0 ||
        opts->local_ip == 0 || opts->switch_ip == 0 ||
        !opts->endpoint_file || !opts->start_file) {
        return false;
    }
    if (opts->primitive != EPIC_PRIM_BARRIER &&
        ((size_t)opts->count * sizeof(int32_t)) % (size_t)opts->payload_bytes != 0) {
        fprintf(stderr, "count*sizeof(int32_t) must be divisible by payload-bytes\n");
        return false;
    }
    if ((opts->primitive == EPIC_PRIM_ALLGATHER ||
         opts->primitive == EPIC_PRIM_REDUCESCATTER) &&
        (opts->count % EPIC_STAR4_HOSTS != 0 ||
         ((size_t)opts->count * sizeof(int32_t) / EPIC_STAR4_HOSTS) %
             (size_t)opts->payload_bytes != 0)) {
        fprintf(stderr,
                "allgather/reducescatter count must be divisible by hosts and each block by payload-bytes\n");
        return false;
    }
    return true;
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
        fprintf(stderr, "RDMA device not found: %s\n", name);
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
    enum ibv_mtu needed = IBV_MTU_1024;
    if (payload_bytes <= 256) {
        needed = IBV_MTU_256;
    } else if (payload_bytes <= 512) {
        needed = IBV_MTU_512;
    } else if (payload_bytes <= 1024) {
        needed = IBV_MTU_1024;
    } else if (payload_bytes <= 2048) {
        needed = IBV_MTU_2048;
    } else {
        needed = IBV_MTU_4096;
    }
    return active_mtu < needed ? active_mtu : needed;
}

static int modify_qp_init(host_ctx_t *ctx) {
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = ctx->opts.ib_port;
    attr.pkey_index = 0;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                           IBV_ACCESS_REMOTE_READ;
    return ibv_modify_qp(ctx->qp, &attr,
                         IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
                             IBV_QP_ACCESS_FLAGS);
}

static int modify_qp_rtr(host_ctx_t *ctx) {
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = ctx->path_mtu;
    attr.dest_qp_num = ctx->opts.switch_qpn & 0x00ffffffu;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 12;
    attr.ah_attr.is_global = 1;
    attr.ah_attr.port_num = ctx->opts.ib_port;
    attr.ah_attr.grh.dgid = ctx->switch_gid;
    attr.ah_attr.grh.sgid_index = ctx->opts.gid_index;
    attr.ah_attr.grh.hop_limit = 64;
    attr.ah_attr.grh.traffic_class = 0;
    attr.ah_attr.grh.flow_label = 0;

    return ibv_modify_qp(ctx->qp, &attr,
                         IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                             IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                             IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);
}

static int modify_qp_rts(host_ctx_t *ctx) {
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = (uint8_t)ctx->opts.qp_timeout;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = 0;
    attr.max_rd_atomic = 1;

    return ibv_modify_qp(ctx->qp, &attr,
                         IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                             IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                             IBV_QP_MAX_QP_RD_ATOMIC);
}

static bool init_context(host_ctx_t *ctx) {
    ctx->ib_ctx = open_selected_device(ctx->opts.ib_dev);
    if (!ctx->ib_ctx) {
        perror("ibv_open_device");
        return false;
    }

    if (ctx->opts.auto_gid_index) {
        ctx->opts.gid_index =
            find_gid_index(ctx->ib_ctx, ctx->opts.ib_port, ctx->opts.local_ip,
                           EPIC_DEFAULT_GID_INDEX);
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
    ctx->switch_gid = ctx->local_gid;
    memcpy(ctx->switch_gid.raw + 12, &ctx->opts.switch_ip,
           sizeof(ctx->opts.switch_ip));

    if (is_barrier(&ctx->opts)) {
        ctx->total_segments = 1;
        ctx->block_segments = 1;
    } else {
        ctx->total_segments =
            (int)(((size_t)ctx->opts.count * sizeof(int32_t)) / ctx->opts.payload_bytes);
        ctx->block_segments = ctx->total_segments / EPIC_STAR4_HOSTS;
    }
    if (ctx->opts.primitive == EPIC_PRIM_ALLGATHER) {
        ctx->send_segments = ctx->block_segments;
        ctx->recv_segments = ctx->block_segments * (EPIC_STAR4_HOSTS - 1);
    } else if (ctx->opts.primitive == EPIC_PRIM_REDUCESCATTER) {
        ctx->send_segments = ctx->total_segments;
        ctx->recv_segments = ctx->block_segments;
    } else {
        ctx->send_segments = is_source(&ctx->opts) ? ctx->total_segments : 0;
        ctx->recv_segments = is_sink(&ctx->opts) ? ctx->total_segments : 0;
    }
    ctx->segments = ctx->send_segments > 0 ? ctx->send_segments : ctx->recv_segments;
    if (ctx->segments <= 0) {
        ctx->segments = 1;
    }
    ctx->total_bytes = (size_t)ctx->opts.count * sizeof(int32_t);
    bool source = is_source(&ctx->opts);
    bool sink = is_sink(&ctx->opts);
    if (ctx->send_segments > 0 && ctx->opts.segment_window > ctx->send_segments) {
        ctx->opts.segment_window = ctx->send_segments;
    }
    int total_iters = ctx->opts.warmup + ctx->opts.iters;
    int max_recv_window = ctx->recv_segments * total_iters;
    if (ctx->opts.recv_window > max_recv_window) {
        ctx->opts.recv_window = max_recv_window;
    }
    if (ctx->opts.recv_window <= 0) {
        ctx->opts.recv_window = 1;
    }

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

    if (source && !is_barrier(&ctx->opts)) {
        if (posix_memalign((void **)&ctx->send_buf, 4096, ctx->total_bytes)) {
            perror("posix_memalign send");
            return false;
        }
        memset(ctx->send_buf, 0, ctx->total_bytes);
    }
    if (sink && !is_barrier(&ctx->opts)) {
        if (posix_memalign((void **)&ctx->recv_buf, 4096, ctx->total_bytes)) {
            perror("posix_memalign recv");
            return false;
        }
        memset(ctx->recv_buf, 0, ctx->total_bytes);
    }

    int mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                   IBV_ACCESS_REMOTE_READ;
    if (source && !is_barrier(&ctx->opts)) {
        ctx->send_mr =
            ibv_reg_mr(ctx->pd, ctx->send_buf, ctx->total_bytes, mr_flags);
        if (!ctx->send_mr) {
            perror("ibv_reg_mr send");
            return false;
        }
    }
    if (sink && !is_barrier(&ctx->opts)) {
        ctx->recv_mr =
            ibv_reg_mr(ctx->pd, ctx->recv_buf, ctx->total_bytes, mr_flags);
        if (!ctx->recv_mr) {
            perror("ibv_reg_mr recv");
            return false;
        }
    }

    if (modify_qp_init(ctx) || modify_qp_rtr(ctx) || modify_qp_rts(ctx)) {
        perror("ibv_modify_qp");
        return false;
    }

    return true;
}

static bool write_endpoint(host_ctx_t *ctx) {
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
    fprintf(f, "switch_qpn=%u\n", ctx->opts.switch_qpn);
    fprintf(f, "gid_index=%d\n", ctx->opts.gid_index);
    fprintf(f, "ib_port=%d\n", ctx->opts.ib_port);
    fprintf(f, "segment_window=%d\n", ctx->opts.segment_window);
    fprintf(f, "recv_window=%d\n", ctx->opts.recv_window);
    fprintf(f, "qp_timeout=%d\n", ctx->opts.qp_timeout);
    fprintf(f, "local_ip=%s\n", local_ip);
    fprintf(f, "switch_ip=%s\n", switch_ip);
    fclose(f);
    return true;
}

static void wait_for_start(const char *path) {
    const struct timespec interval = {
        .tv_sec = 0,
        .tv_nsec = 10000000L,
    };
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

static bool is_source(const host_opts_t *opts) {
    if (opts->primitive == EPIC_PRIM_BARRIER) {
        return true;
    }
    if (opts->primitive == EPIC_PRIM_ALLGATHER ||
        opts->primitive == EPIC_PRIM_REDUCESCATTER) {
        return true;
    }
    if (opts->primitive == EPIC_PRIM_BCAST) {
        return opts->rank == opts->root_rank;
    }
    return true;
}

static bool is_sink(const host_opts_t *opts) {
    if (opts->primitive == EPIC_PRIM_BARRIER) {
        return true;
    }
    if (opts->primitive == EPIC_PRIM_ALLGATHER ||
        opts->primitive == EPIC_PRIM_REDUCESCATTER) {
        return true;
    }
    if (opts->primitive == EPIC_PRIM_REDUCE) {
        return opts->rank == opts->root_rank;
    }
    if (opts->primitive == EPIC_PRIM_BCAST) {
        return opts->rank != opts->root_rank;
    }
    return true;
}

static bool is_barrier(const host_opts_t *opts) {
    return opts->primitive == EPIC_PRIM_BARRIER;
}

static void fill_send_buffer(host_ctx_t *ctx) {
    if (is_barrier(&ctx->opts)) {
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

static size_t recv_offset_for_seq(host_ctx_t *ctx, int seq) {
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

static bool post_recv_seq(host_ctx_t *ctx, int seq) {
    if (is_barrier(&ctx->opts)) {
        struct ibv_recv_wr wr;
        struct ibv_recv_wr *bad = NULL;
        memset(&wr, 0, sizeof(wr));
        wr.wr_id = (uint64_t)seq;
        wr.sg_list = NULL;
        wr.num_sge = 0;
        if (ibv_post_recv(ctx->qp, &wr, &bad)) {
            perror("ibv_post_recv barrier");
            return false;
        }
        return true;
    }

    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)(ctx->recv_buf + recv_offset_for_seq(ctx, seq));
    sge.length = (uint32_t)ctx->opts.payload_bytes;
    sge.lkey = ctx->recv_mr->lkey;

    struct ibv_recv_wr wr;
    struct ibv_recv_wr *bad = NULL;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = (uint64_t)seq;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    if (ibv_post_recv(ctx->qp, &wr, &bad)) {
        perror("ibv_post_recv");
        return false;
    }
    return true;
}

static bool refill_recv_window(host_ctx_t *ctx) {
    int per_iter_recv = expected_recv_count(ctx);
    if (per_iter_recv <= 0) {
        return true;
    }
    while (ctx->recv_posted_total < ctx->recv_total &&
           ctx->recv_posted_total - ctx->recv_completed_total <
               ctx->opts.recv_window) {
        int seq = ctx->recv_posted_total % per_iter_recv;
        if (!post_recv_seq(ctx, seq)) {
            return false;
        }
        ctx->recv_posted_total++;
    }
    return true;
}

static size_t send_offset_for_segment(host_ctx_t *ctx, int segment) {
    if (ctx->opts.primitive == EPIC_PRIM_ALLGATHER) {
        size_t block_bytes = ctx->total_bytes / EPIC_STAR4_HOSTS;
        return (size_t)ctx->opts.rank * block_bytes +
               (size_t)segment * (size_t)ctx->opts.payload_bytes;
    }
    return (size_t)segment * (size_t)ctx->opts.payload_bytes;
}

static bool post_send_segment(host_ctx_t *ctx, int segment) {
    if (is_barrier(&ctx->opts)) {
        struct ibv_send_wr wr;
        struct ibv_send_wr *bad = NULL;
        memset(&wr, 0, sizeof(wr));
        wr.wr_id = (uint64_t)segment;
        wr.sg_list = NULL;
        wr.num_sge = 0;
        wr.opcode = IBV_WR_SEND;
        wr.send_flags = IBV_SEND_SIGNALED;
        if (ibv_post_send(ctx->qp, &wr, &bad)) {
            perror("ibv_post_send barrier");
            return false;
        }
        return true;
    }

    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)(ctx->send_buf + send_offset_for_segment(ctx, segment));
    sge.length = (uint32_t)ctx->opts.payload_bytes;
    sge.lkey = ctx->send_mr->lkey;

    struct ibv_send_wr wr;
    struct ibv_send_wr *bad = NULL;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = (uint64_t)segment;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;

    if (ibv_post_send(ctx->qp, &wr, &bad)) {
        perror("ibv_post_send");
        return false;
    }
    return true;
}

static bool post_initial_sends(host_ctx_t *ctx, int expected_send,
                               int *send_posted) {
    int limit = expected_send < ctx->opts.segment_window ? expected_send
                                                         : ctx->opts.segment_window;
    while (*send_posted < limit) {
        if (!post_send_segment(ctx, *send_posted)) {
            return false;
        }
        (*send_posted)++;
    }
    return true;
}

static bool poll_until_windowed(host_ctx_t *ctx, int expected_send,
                                int expected_recv, int *send_posted) {
    int got_send = ctx->pending_send;
    int got_recv = ctx->pending_recv;
    struct ibv_wc wc[32];

    ctx->pending_send = 0;
    ctx->pending_recv = 0;

    while (got_send < expected_send || got_recv < expected_recv) {
        int n = ibv_poll_cq(ctx->cq, 32, wc);
        if (n < 0) {
            fprintf(stderr, "ibv_poll_cq failed: %d\n", n);
            return false;
        }
        if (n == 0) {
            continue;
        }
        for (int i = 0; i < n; i++) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                fprintf(stderr,
                        "rank=%d completion failed wr_id=%" PRIu64 " status=%s opcode=%d vendor=0x%x\n",
                        ctx->opts.rank, wc[i].wr_id, ibv_wc_status_str(wc[i].status),
                        wc[i].opcode, wc[i].vendor_err);
                return false;
            }
            if (wc[i].opcode == IBV_WC_SEND) {
                got_send++;
                if (*send_posted < expected_send) {
                    if (!post_send_segment(ctx, *send_posted)) {
                        return false;
                    }
                    (*send_posted)++;
                }
            } else if (wc[i].opcode == IBV_WC_RECV) {
                got_recv++;
                ctx->recv_completed_total++;
                if (!refill_recv_window(ctx)) {
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

static bool verify_result(host_ctx_t *ctx) {
    if (is_barrier(&ctx->opts)) {
        return true;
    }
    if (!is_sink(&ctx->opts)) {
        return true;
    }

    uint32_t *buf = (uint32_t *)ctx->recv_buf;
    int expected_multiplier = 0;
    if (ctx->opts.primitive == EPIC_PRIM_ALLGATHER) {
        int block = ctx->opts.count / EPIC_STAR4_HOSTS;
        for (int r = 0; r < EPIC_STAR4_HOSTS; r++) {
            for (int i = 0; i < block; i++) {
                int idx = r * block + i;
                int32_t got = (int32_t)ntohl(buf[idx]);
                int32_t expected = idx * (r + 1);
                if (got != expected) {
                    fprintf(stderr,
                            "rank=%d verify failed at idx=%d got=%d expected=%d primitive=%s\n",
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
        for (int r = 0; r < EPIC_STAR4_HOSTS; r++) {
            expected_multiplier += r + 1;
        }
        for (int i = 0; i < block; i++) {
            int idx = start + i;
            int32_t got = (int32_t)ntohl(buf[idx]);
            int32_t expected = idx * expected_multiplier;
            if (got != expected) {
                fprintf(stderr,
                        "rank=%d verify failed at idx=%d got=%d expected=%d primitive=%s\n",
                        ctx->opts.rank, idx, got, expected,
                        epic_primitive_name(ctx->opts.primitive));
                return false;
            }
        }
        return true;
    }
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
            fprintf(stderr,
                    "rank=%d verify failed at idx=%d got=%d expected=%d primitive=%s\n",
                    ctx->opts.rank, i, got, expected,
                    epic_primitive_name(ctx->opts.primitive));
            return false;
        }
    }
    return true;
}

static int expected_recv_count(host_ctx_t *ctx) {
    return ctx->recv_segments;
}

static int expected_send_count(host_ctx_t *ctx) {
    return ctx->send_segments;
}

static bool run_one_iter(host_ctx_t *ctx, int iter, double *latency_us) {
    (void)iter;

    int expected_recv = expected_recv_count(ctx);
    int expected_send = expected_send_count(ctx);
    int send_posted = 0;

    if (expected_send) {
        fill_send_buffer(ctx);
    }

    uint64_t t0 = now_ns();
    if (expected_send && !post_initial_sends(ctx, expected_send, &send_posted)) {
        return false;
    }

    if (!poll_until_windowed(ctx, expected_send, expected_recv, &send_posted)) {
        return false;
    }
    uint64_t t1 = now_ns();

    *latency_us = (double)(t1 - t0) / 1000.0;
    return verify_result(ctx);
}

int main(int argc, char **argv) {
    host_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));

    if (!parse_args(argc, argv, &ctx.opts)) {
        usage(argv[0]);
        return 2;
    }

    if (!init_context(&ctx)) {
        return 1;
    }

    if (!write_endpoint(&ctx)) {
        return 1;
    }

    char local_ip[INET_ADDRSTRLEN];
    char switch_ip[INET_ADDRSTRLEN];
    epic_ipv4_to_str(ctx.opts.local_ip, local_ip, sizeof(local_ip));
    epic_ipv4_to_str(ctx.opts.switch_ip, switch_ip, sizeof(switch_ip));
    printf("READY rank=%d qpn=%u primitive=%s mode=%d local_ip=%s switch_ip=%s gid_index=%d segments=%d send_segments=%d recv_segments=%d segment_window=%d recv_window=%d qp_timeout=%d\n",
           ctx.opts.rank, ctx.qp->qp_num, epic_primitive_name(ctx.opts.primitive),
           ctx.opts.mode, local_ip, switch_ip, ctx.opts.gid_index, ctx.total_segments,
           ctx.send_segments, ctx.recv_segments, ctx.opts.segment_window,
           ctx.opts.recv_window, ctx.opts.qp_timeout);
    fflush(stdout);

    int per_iter_recv = expected_recv_count(&ctx);
    if (per_iter_recv > 0) {
        ctx.recv_total = per_iter_recv * (ctx.opts.warmup + ctx.opts.iters);
        if (!refill_recv_window(&ctx)) {
            return 1;
        }
    }

    wait_for_start(ctx.opts.start_file);

    double total_us = 0.0;
    for (int i = 0; i < ctx.opts.warmup; i++) {
        double latency_us = 0.0;
        if (!run_one_iter(&ctx, i, &latency_us)) {
            return 1;
        }
        printf("WARMUP rank=%d iter=%d latency_us=%.3f\n",
               ctx.opts.rank, i, latency_us);
        fflush(stdout);
    }

    for (int i = 0; i < ctx.opts.iters; i++) {
        double latency_us = 0.0;
        if (!run_one_iter(&ctx, i, &latency_us)) {
            return 1;
        }
        total_us += latency_us;
        printf("ITER rank=%d iter=%d latency_us=%.3f\n", ctx.opts.rank, i, latency_us);
        fflush(stdout);
    }

    printf("RESULT rank=%d primitive=%s mode=%d count=%d warmup=%d iters=%d segment_window=%d recv_window=%d qp_timeout=%d avg_latency_us=%.3f\n",
           ctx.opts.rank, epic_primitive_name(ctx.opts.primitive), ctx.opts.mode,
           ctx.opts.count, ctx.opts.warmup, ctx.opts.iters, ctx.opts.segment_window,
           ctx.opts.recv_window, ctx.opts.qp_timeout, total_us / ctx.opts.iters);
    return 0;
}
