#define _GNU_SOURCE

#include <arpa/inet.h>
#include <errno.h>
#include <getopt.h>
#include <infiniband/verbs.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define DEFAULT_PORT 18515
#define DEFAULT_IB_DEV "rxe_data0"
#define DEFAULT_IB_PORT 1
#define DEFAULT_GID_INDEX 1
#define DEFAULT_SIZE 4096
#define DEFAULT_ITERS 100
#define DEFAULT_CHUNK 4096
#define DEFAULT_WINDOW 256

typedef enum {
    TRANSPORT_TCP,
    TRANSPORT_ROCE,
} transport_t;

typedef struct {
    transport_t transport;
    bool server;
    const char *bind_ip;
    const char *peer_ip;
    int port;
    const char *ib_dev;
    int ib_port;
    int gid_index;
    size_t size;
    int iters;
    size_t chunk;
    int window;
} opts_t;

typedef struct {
    uint32_t qpn;
    uint32_t psn;
    uint32_t chunk;
    uint32_t window;
    uint8_t gid[16];
} peer_info_t;

typedef struct {
    struct ibv_context *ctx;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_qp *qp;
    struct ibv_mr *send_mr;
    struct ibv_mr *recv_mr;
    uint8_t *send_buf;
    uint8_t *recv_buf;
    size_t chunk;
    int window;
    uint32_t psn;
    union ibv_gid gid;
} roce_ctx_t;

static uint64_t nsec_now(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static void die_errno(const char *msg) {
    perror(msg);
    exit(1);
}

static void usage(const char *prog) {
    fprintf(stderr,
            "usage: %s --transport tcp|roce (--server|--client IP) "
            "--bind-ip IP [--port N] [--size B] [--iters N] "
            "[--chunk B] [--window N] [--ib-dev DEV] [--gid-index N]\n",
            prog);
}

static uint64_t parse_u64(const char *s) {
    char *end = NULL;
    errno = 0;
    uint64_t v = strtoull(s, &end, 0);
    if (errno || !end || *end != '\0') {
        fprintf(stderr, "invalid integer: %s\n", s);
        exit(2);
    }
    return v;
}

static opts_t parse_opts(int argc, char **argv) {
    opts_t opts;
    memset(&opts, 0, sizeof(opts));
    opts.transport = TRANSPORT_TCP;
    opts.port = DEFAULT_PORT;
    opts.ib_dev = DEFAULT_IB_DEV;
    opts.ib_port = DEFAULT_IB_PORT;
    opts.gid_index = DEFAULT_GID_INDEX;
    opts.size = DEFAULT_SIZE;
    opts.iters = DEFAULT_ITERS;
    opts.chunk = DEFAULT_CHUNK;
    opts.window = DEFAULT_WINDOW;

    static const struct option long_opts[] = {
        {"transport", required_argument, NULL, 't'},
        {"server", no_argument, NULL, 's'},
        {"client", required_argument, NULL, 'c'},
        {"bind-ip", required_argument, NULL, 'b'},
        {"port", required_argument, NULL, 'p'},
        {"size", required_argument, NULL, 'z'},
        {"iters", required_argument, NULL, 'n'},
        {"chunk", required_argument, NULL, 'k'},
        {"window", required_argument, NULL, 'w'},
        {"ib-dev", required_argument, NULL, 'd'},
        {"ib-port", required_argument, NULL, 'i'},
        {"gid-index", required_argument, NULL, 'g'},
        {0, 0, 0, 0},
    };

    int c;
    while ((c = getopt_long(argc, argv, "", long_opts, NULL)) != -1) {
        switch (c) {
        case 't':
            if (strcmp(optarg, "tcp") == 0) {
                opts.transport = TRANSPORT_TCP;
            } else if (strcmp(optarg, "roce") == 0) {
                opts.transport = TRANSPORT_ROCE;
            } else {
                usage(argv[0]);
                exit(2);
            }
            break;
        case 's':
            opts.server = true;
            break;
        case 'c':
            opts.server = false;
            opts.peer_ip = optarg;
            break;
        case 'b':
            opts.bind_ip = optarg;
            break;
        case 'p':
            opts.port = (int)parse_u64(optarg);
            break;
        case 'z':
            opts.size = (size_t)parse_u64(optarg);
            break;
        case 'n':
            opts.iters = (int)parse_u64(optarg);
            break;
        case 'k':
            opts.chunk = (size_t)parse_u64(optarg);
            break;
        case 'w':
            opts.window = (int)parse_u64(optarg);
            break;
        case 'd':
            opts.ib_dev = optarg;
            break;
        case 'i':
            opts.ib_port = (int)parse_u64(optarg);
            break;
        case 'g':
            opts.gid_index = (int)parse_u64(optarg);
            break;
        default:
            usage(argv[0]);
            exit(2);
        }
    }

    if (!opts.bind_ip || opts.port <= 0 || opts.port > 65535 ||
        opts.size == 0 || opts.iters <= 0 || opts.chunk == 0 ||
        opts.window <= 0 || (!opts.server && !opts.peer_ip) ||
        opts.size % opts.chunk != 0) {
        usage(argv[0]);
        exit(2);
    }
    return opts;
}

static int make_bound_socket(const char *ip) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        die_errno("socket");
    }
    int one = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one))) {
        die_errno("setsockopt SO_REUSEADDR");
    }
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) && errno != ENOPROTOOPT) {
        die_errno("setsockopt TCP_NODELAY");
    }
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
        fprintf(stderr, "bad bind ip: %s\n", ip);
        exit(2);
    }
    addr.sin_port = 0;
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr))) {
        die_errno("bind local");
    }
    return fd;
}

static int accept_one(const opts_t *opts) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        die_errno("socket listen");
    }
    int one = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one))) {
        die_errno("setsockopt SO_REUSEADDR");
    }
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) && errno != ENOPROTOOPT) {
        die_errno("setsockopt TCP_NODELAY");
    }
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    if (inet_pton(AF_INET, opts->bind_ip, &addr.sin_addr) != 1) {
        fprintf(stderr, "bad bind ip: %s\n", opts->bind_ip);
        exit(2);
    }
    addr.sin_port = htons((uint16_t)opts->port);
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr))) {
        die_errno("bind listen");
    }
    if (listen(fd, 1)) {
        die_errno("listen");
    }
    int conn = accept(fd, NULL, NULL);
    if (conn < 0) {
        die_errno("accept");
    }
    if (setsockopt(conn, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) && errno != ENOPROTOOPT) {
        die_errno("setsockopt accepted TCP_NODELAY");
    }
    close(fd);
    return conn;
}

static int connect_one(const opts_t *opts) {
    int fd = make_bound_socket(opts->bind_ip);
    struct sockaddr_in peer;
    memset(&peer, 0, sizeof(peer));
    peer.sin_family = AF_INET;
    if (inet_pton(AF_INET, opts->peer_ip, &peer.sin_addr) != 1) {
        fprintf(stderr, "bad peer ip: %s\n", opts->peer_ip);
        exit(2);
    }
    peer.sin_port = htons((uint16_t)opts->port);
    while (connect(fd, (struct sockaddr *)&peer, sizeof(peer)) != 0) {
        if (errno != ECONNREFUSED && errno != ETIMEDOUT && errno != ENETUNREACH) {
            die_errno("connect");
        }
        usleep(100000);
    }
    return fd;
}

static void read_full(int fd, void *buf, size_t len) {
    uint8_t *p = buf;
    while (len) {
        ssize_t n = read(fd, p, len);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            die_errno("read");
        }
        if (n == 0) {
            fprintf(stderr, "unexpected EOF\n");
            exit(1);
        }
        p += n;
        len -= (size_t)n;
    }
}

static void write_full(int fd, const void *buf, size_t len) {
    const uint8_t *p = buf;
    while (len) {
        ssize_t n = write(fd, p, len);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            die_errno("write");
        }
        p += n;
        len -= (size_t)n;
    }
}

static void run_tcp_server(const opts_t *opts, int fd) {
    uint8_t *buf = malloc(opts->chunk);
    if (!buf) {
        die_errno("malloc tcp server");
    }
    memset(buf, 0x5a, opts->chunk);
    size_t chunks = opts->size / opts->chunk;
    for (int iter = 0; iter < opts->iters; iter++) {
        for (size_t i = 0; i < chunks; i++) {
            read_full(fd, buf, opts->chunk);
        }
        for (size_t i = 0; i < chunks; i++) {
            write_full(fd, buf, opts->chunk);
        }
    }
    free(buf);
}

static void run_tcp_client(const opts_t *opts, int fd) {
    uint8_t *buf = malloc(opts->chunk);
    if (!buf) {
        die_errno("malloc tcp client");
    }
    memset(buf, 0xa5, opts->chunk);
    size_t chunks = opts->size / opts->chunk;
    uint64_t t0 = nsec_now();
    for (int iter = 0; iter < opts->iters; iter++) {
        for (size_t i = 0; i < chunks; i++) {
            write_full(fd, buf, opts->chunk);
        }
        for (size_t i = 0; i < chunks; i++) {
            read_full(fd, buf, opts->chunk);
        }
    }
    uint64_t t1 = nsec_now();
    uint64_t bytes = (uint64_t)opts->size * (uint64_t)opts->iters * 2ull;
    double sec = (double)(t1 - t0) / 1e9;
    printf("RESULT transport=tcp role=client size=%zu chunk=%zu iters=%d "
           "bytes=%" PRIu64 " seconds=%.6f mbit_per_sec=%.2f usec_per_iter=%.2f\n",
           opts->size, opts->chunk, opts->iters, bytes, sec,
           (double)bytes * 8.0 / sec / 1e6,
           sec * 1e6 / (double)opts->iters);
    free(buf);
}

static struct ibv_context *open_device(const char *name) {
    int num = 0;
    struct ibv_device **list = ibv_get_device_list(&num);
    if (!list || num <= 0) {
        fprintf(stderr, "no ibverbs devices\n");
        exit(1);
    }
    struct ibv_device *dev = NULL;
    for (int i = 0; i < num; i++) {
        if (strcmp(ibv_get_device_name(list[i]), name) == 0) {
            dev = list[i];
            break;
        }
    }
    if (!dev) {
        fprintf(stderr, "ib device not found: %s\n", name);
        exit(1);
    }
    struct ibv_context *ctx = ibv_open_device(dev);
    ibv_free_device_list(list);
    if (!ctx) {
        die_errno("ibv_open_device");
    }
    return ctx;
}

static enum ibv_mtu pick_mtu(size_t chunk) {
    if (chunk <= 256) return IBV_MTU_256;
    if (chunk <= 512) return IBV_MTU_512;
    if (chunk <= 1024) return IBV_MTU_1024;
    if (chunk <= 2048) return IBV_MTU_2048;
    return IBV_MTU_4096;
}

static void init_roce(roce_ctx_t *r, const opts_t *opts) {
    memset(r, 0, sizeof(*r));
    r->chunk = opts->chunk;
    r->window = opts->window;
    r->psn = 0x123400u;
    r->ctx = open_device(opts->ib_dev);
    if (ibv_query_gid(r->ctx, opts->ib_port, opts->gid_index, &r->gid)) {
        die_errno("ibv_query_gid");
    }
    r->pd = ibv_alloc_pd(r->ctx);
    if (!r->pd) {
        die_errno("ibv_alloc_pd");
    }
    int cq_depth = opts->window * 4 + 128;
    r->cq = ibv_create_cq(r->ctx, cq_depth, NULL, NULL, 0);
    if (!r->cq) {
        die_errno("ibv_create_cq");
    }
    struct ibv_qp_init_attr qpia;
    memset(&qpia, 0, sizeof(qpia));
    qpia.qp_type = IBV_QPT_RC;
    qpia.send_cq = r->cq;
    qpia.recv_cq = r->cq;
    qpia.cap.max_send_wr = opts->window + 16;
    qpia.cap.max_recv_wr = opts->window + 16;
    qpia.cap.max_send_sge = 1;
    qpia.cap.max_recv_sge = 1;
    r->qp = ibv_create_qp(r->pd, &qpia);
    if (!r->qp) {
        die_errno("ibv_create_qp");
    }
    size_t bytes = opts->chunk * (size_t)opts->window;
    if (posix_memalign((void **)&r->send_buf, 4096, bytes) ||
        posix_memalign((void **)&r->recv_buf, 4096, bytes)) {
        die_errno("posix_memalign roce");
    }
    memset(r->send_buf, 0xa5, bytes);
    memset(r->recv_buf, 0, bytes);
    int flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    r->send_mr = ibv_reg_mr(r->pd, r->send_buf, bytes, flags);
    r->recv_mr = ibv_reg_mr(r->pd, r->recv_buf, bytes, flags);
    if (!r->send_mr || !r->recv_mr) {
        die_errno("ibv_reg_mr");
    }

    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = opts->ib_port;
    attr.pkey_index = 0;
    attr.qp_access_flags = flags;
    if (ibv_modify_qp(r->qp, &attr,
                      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
                          IBV_QP_ACCESS_FLAGS)) {
        die_errno("ibv_modify_qp INIT");
    }
}

static void exchange_peer(int fd, const roce_ctx_t *r, const opts_t *opts,
                          peer_info_t *peer) {
    peer_info_t local;
    memset(&local, 0, sizeof(local));
    local.qpn = htonl(r->qp->qp_num);
    local.psn = htonl(r->psn);
    local.chunk = htonl((uint32_t)opts->chunk);
    local.window = htonl((uint32_t)opts->window);
    memcpy(local.gid, r->gid.raw, sizeof(local.gid));
    write_full(fd, &local, sizeof(local));
    read_full(fd, peer, sizeof(*peer));
    peer->qpn = ntohl(peer->qpn);
    peer->psn = ntohl(peer->psn);
    peer->chunk = ntohl(peer->chunk);
    peer->window = ntohl(peer->window);
    if (peer->chunk != opts->chunk) {
        fprintf(stderr, "peer chunk mismatch\n");
        exit(1);
    }
}

static void connect_roce(roce_ctx_t *r, const opts_t *opts, const peer_info_t *peer) {
    union ibv_gid dgid;
    memcpy(dgid.raw, peer->gid, sizeof(dgid.raw));
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = pick_mtu(opts->chunk);
    attr.dest_qp_num = peer->qpn;
    attr.rq_psn = peer->psn;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 12;
    attr.ah_attr.is_global = 1;
    attr.ah_attr.port_num = opts->ib_port;
    attr.ah_attr.grh.dgid = dgid;
    attr.ah_attr.grh.sgid_index = opts->gid_index;
    attr.ah_attr.grh.hop_limit = 64;
    if (ibv_modify_qp(r->qp, &attr,
                      IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                          IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                          IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER)) {
        die_errno("ibv_modify_qp RTR");
    }
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 0x12;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = r->psn;
    attr.max_rd_atomic = 1;
    if (ibv_modify_qp(r->qp, &attr,
                      IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                          IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                          IBV_QP_MAX_QP_RD_ATOMIC)) {
        die_errno("ibv_modify_qp RTS");
    }
}

static void post_recv(roce_ctx_t *r, int slot) {
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)(r->recv_buf + (size_t)slot * r->chunk);
    sge.length = (uint32_t)r->chunk;
    sge.lkey = r->recv_mr->lkey;
    struct ibv_recv_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = (uint64_t)slot;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    struct ibv_recv_wr *bad = NULL;
    if (ibv_post_recv(r->qp, &wr, &bad)) {
        die_errno("ibv_post_recv");
    }
}

static void post_send(roce_ctx_t *r, int slot) {
    struct ibv_sge sge;
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)(r->send_buf + (size_t)slot * r->chunk);
    sge.length = (uint32_t)r->chunk;
    sge.lkey = r->send_mr->lkey;
    struct ibv_send_wr wr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = (uint64_t)slot;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    struct ibv_send_wr *bad = NULL;
    if (ibv_post_send(r->qp, &wr, &bad)) {
        die_errno("ibv_post_send");
    }
}

static void poll_one(roce_ctx_t *r, struct ibv_wc *wc) {
    while (true) {
        int n = ibv_poll_cq(r->cq, 1, wc);
        if (n < 0) {
            fprintf(stderr, "ibv_poll_cq failed\n");
            exit(1);
        }
        if (n == 1) {
            if (wc->status != IBV_WC_SUCCESS) {
                fprintf(stderr, "WC error status=%s opcode=%d vendor=%u\n",
                        ibv_wc_status_str(wc->status), wc->opcode, wc->vendor_err);
                exit(1);
            }
            return;
        }
    }
}

static void run_roce_server(const opts_t *opts, int fd) {
    roce_ctx_t r;
    init_roce(&r, opts);
    peer_info_t peer;
    exchange_peer(fd, &r, opts, &peer);
    connect_roce(&r, opts, &peer);
    uint64_t chunks = ((uint64_t)opts->size / opts->chunk) * (uint64_t)opts->iters;
    int initial_recvs = chunks < (uint64_t)opts->window ? (int)chunks : opts->window;
    uint64_t recv_posted = 0;
    for (int i = 0; i < initial_recvs; i++) {
        post_recv(&r, i);
        recv_posted++;
    }
    uint64_t recv_done = 0, send_done = 0, send_posted = 0;
    int *pending_slots = calloc((size_t)opts->window, sizeof(*pending_slots));
    if (!pending_slots) {
        die_errno("calloc pending_slots");
    }
    int pending_head = 0, pending_tail = 0, pending_count = 0;
    struct ibv_wc wc;
    while (recv_done < chunks || send_done < chunks) {
        while (pending_count > 0 &&
               send_posted - send_done < (uint64_t)opts->window) {
            int slot = pending_slots[pending_head];
            pending_head = (pending_head + 1) % opts->window;
            pending_count--;
            post_send(&r, slot);
            send_posted++;
            if (recv_posted < chunks) {
                post_recv(&r, slot);
                recv_posted++;
            }
        }
        poll_one(&r, &wc);
        int slot = (int)wc.wr_id;
        if (wc.opcode & IBV_WC_RECV) {
            recv_done++;
            if (pending_count == opts->window) {
                fprintf(stderr, "server pending send queue overflow\n");
                exit(1);
            }
            pending_slots[pending_tail] = slot;
            pending_tail = (pending_tail + 1) % opts->window;
            pending_count++;
        } else {
            send_done++;
        }
    }
    free(pending_slots);
}

static void run_roce_client(const opts_t *opts, int fd) {
    roce_ctx_t r;
    init_roce(&r, opts);
    peer_info_t peer;
    exchange_peer(fd, &r, opts, &peer);
    connect_roce(&r, opts, &peer);
    for (int i = 0; i < opts->window; i++) {
        post_recv(&r, i);
    }
    uint64_t chunks = ((uint64_t)opts->size / opts->chunk) * (uint64_t)opts->iters;
    uint64_t send_posted = 0, send_done = 0, recv_done = 0;
    int next_send_slot = 0;
    uint64_t t0 = nsec_now();
    while (send_posted < chunks && send_posted - send_done < (uint64_t)opts->window) {
        post_send(&r, next_send_slot);
        next_send_slot = (next_send_slot + 1) % opts->window;
        send_posted++;
    }
    struct ibv_wc wc;
    while (recv_done < chunks) {
        poll_one(&r, &wc);
        if (wc.opcode & IBV_WC_RECV) {
            recv_done++;
            int slot = (int)wc.wr_id;
            if (recv_done + (uint64_t)opts->window <= chunks) {
                post_recv(&r, slot);
            }
        } else {
            send_done++;
        }
        while (send_posted < chunks && send_posted - send_done < (uint64_t)opts->window) {
            post_send(&r, next_send_slot);
            next_send_slot = (next_send_slot + 1) % opts->window;
            send_posted++;
        }
    }
    while (send_done < chunks) {
        poll_one(&r, &wc);
        if (!(wc.opcode & IBV_WC_RECV)) {
            send_done++;
        }
    }
    uint64_t t1 = nsec_now();
    uint64_t bytes = (uint64_t)opts->size * (uint64_t)opts->iters * 2ull;
    double sec = (double)(t1 - t0) / 1e9;
    printf("RESULT transport=roce role=client size=%zu chunk=%zu window=%d iters=%d "
           "bytes=%" PRIu64 " seconds=%.6f mbit_per_sec=%.2f usec_per_iter=%.2f\n",
           opts->size, opts->chunk, opts->window, opts->iters, bytes, sec,
           (double)bytes * 8.0 / sec / 1e6,
           sec * 1e6 / (double)opts->iters);
}

int main(int argc, char **argv) {
    opts_t opts = parse_opts(argc, argv);
    int fd = opts.server ? accept_one(&opts) : connect_one(&opts);
    if (opts.transport == TRANSPORT_TCP) {
        if (opts.server) {
            run_tcp_server(&opts, fd);
        } else {
            run_tcp_client(&opts, fd);
        }
    } else {
        if (opts.server) {
            run_roce_server(&opts, fd);
        } else {
            run_roce_client(&opts, fd);
        }
    }
    close(fd);
    return 0;
}
