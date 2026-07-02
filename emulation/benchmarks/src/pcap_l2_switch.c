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
#define SEND_RETRY_MAX 100000
#define ETHERTYPE_IPV4 0x0800

typedef struct {
    int rank;
    char dev[16];
    uint8_t mac[6];
} l2_port_t;

typedef struct {
    l2_port_t ports[EPIC_STAR4_HOSTS];
    pcap_t *handles[EPIC_STAR4_HOSTS];
    int epoll_fd;
    bool profile;
    uint64_t rx_packets;
    uint64_t rx_bytes;
    uint64_t fwd_packets;
    uint64_t fwd_bytes;
    uint64_t send_calls;
    uint64_t unicast_packets;
    uint64_t unknown_unicast;
    uint64_t rx_ipv4;
    uint64_t rx_roce_udp4791;
    uint64_t send_eagain;
    uint64_t dropped;
    uint64_t total_ns;
    uint64_t lookup_ns;
    uint64_t send_ns;
} l2_ctx_t;

static volatile sig_atomic_t running = 1;

static void on_signal(int signo) {
    (void)signo;
    running = 0;
}

static uint64_t nsec_now(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
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

static bool parse_port_key(l2_ctx_t *ctx, const char *key, const char *value) {
    int rank = -1;
    char field[64];
    if (sscanf(key, "port%d.%63s", &rank, field) != 2 ||
        rank < 0 || rank >= EPIC_STAR4_HOSTS) {
        return false;
    }
    if (strcmp(field, "dev") != 0) {
        if (strcmp(field, "host_mac") == 0) {
            return epic_parse_mac(value, ctx->ports[rank].mac);
        }
        return true;
    }
    ctx->ports[rank].rank = rank;
    snprintf(ctx->ports[rank].dev, sizeof(ctx->ports[rank].dev), "%s", value);
    return true;
}

static bool load_config(l2_ctx_t *ctx, const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) {
        perror("fopen l2 config");
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
            fprintf(stderr, "%s:%d expected key=value\n", path, lineno);
            fclose(f);
            return false;
        }
        *eq = '\0';
        char *key = trim(s);
        char *value = trim(eq + 1);
        if (!parse_port_key(ctx, key, value)) {
            fprintf(stderr, "%s:%d invalid key/value %s=%s\n", path, lineno, key,
                    value);
            fclose(f);
            return false;
        }
    }
    fclose(f);

    for (int i = 0; i < EPIC_STAR4_HOSTS; i++) {
        if (!ctx->ports[i].dev[0]) {
            fprintf(stderr, "port%d missing dev config\n", i);
            return false;
        }
        bool mac_zero = true;
        for (size_t j = 0; j < 6; j++) {
            mac_zero = mac_zero && ctx->ports[i].mac[j] == 0;
        }
        if (mac_zero) {
            fprintf(stderr, "port%d missing host_mac config\n", i);
            return false;
        }
    }
    return true;
}

static bool setup_pcap(l2_ctx_t *ctx) {
    char errbuf[PCAP_ERRBUF_SIZE];
    ctx->epoll_fd = epoll_create1(0);
    if (ctx->epoll_fd < 0) {
        perror("epoll_create1");
        return false;
    }

    for (int i = 0; i < EPIC_STAR4_HOSTS; i++) {
        l2_port_t *p = &ctx->ports[i];
        pcap_t *h = pcap_create(p->dev, errbuf);
        if (!h) {
            fprintf(stderr, "pcap_create(%s): %s\n", p->dev, errbuf);
            return false;
        }
        pcap_set_snaplen(h, 65535);
        pcap_set_promisc(h, 1);
        pcap_set_timeout(h, 1);
        pcap_set_immediate_mode(h, 1);
        if (pcap_activate(h) != 0) {
            fprintf(stderr, "pcap_activate(%s): %s\n", p->dev, pcap_geterr(h));
            return false;
        }
        if (pcap_setdirection(h, PCAP_D_IN) != 0) {
            fprintf(stderr, "warning: pcap_setdirection(%s, IN): %s\n", p->dev,
                    pcap_geterr(h));
        }
        struct bpf_program fp;
        if (pcap_compile(h, &fp, "ip", 0, PCAP_NETMASK_UNKNOWN) == -1) {
            fprintf(stderr, "pcap_compile(%s): %s\n", p->dev, pcap_geterr(h));
            return false;
        }
        if (pcap_setfilter(h, &fp) == -1) {
            fprintf(stderr, "pcap_setfilter(%s): %s\n", p->dev, pcap_geterr(h));
            pcap_freecode(&fp);
            return false;
        }
        pcap_freecode(&fp);
        if (pcap_setnonblock(h, 1, errbuf) != 0) {
            fprintf(stderr, "pcap_setnonblock(%s): %s\n", p->dev, errbuf);
            return false;
        }
        int fd = pcap_get_selectable_fd(h);
        if (fd < 0) {
            fprintf(stderr, "pcap_get_selectable_fd(%s) failed\n", p->dev);
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

static int lookup_mac(l2_ctx_t *ctx, const uint8_t mac[6]) {
    for (int i = 0; i < EPIC_STAR4_HOSTS; i++) {
        if (memcmp(ctx->ports[i].mac, mac, 6) == 0) {
            return i;
        }
    }
    return -1;
}

static void count_payload(l2_ctx_t *ctx, const uint8_t *packet, size_t len) {
    if (len < sizeof(epic_eth_t)) {
        return;
    }
    const epic_eth_t *eth = (const epic_eth_t *)packet;
    uint16_t ethertype = ntohs(eth->ether_type);
    if (ethertype != ETHERTYPE_IPV4 ||
        len < sizeof(epic_eth_t) + sizeof(epic_ipv4_t)) {
        return;
    }
    ctx->rx_ipv4++;
    const epic_ipv4_t *ip = (const epic_ipv4_t *)(packet + sizeof(*eth));
    size_t ihl = (ip->version_ihl & 0x0fu) * 4u;
    size_t ip_total = ntohs(ip->total_length);
    if (ip->protocol == IPPROTO_UDP &&
        ip_total >= ihl + sizeof(epic_udp_t) &&
        sizeof(*eth) + ihl + sizeof(epic_udp_t) <= len) {
        const epic_udp_t *udp = (const epic_udp_t *)((const uint8_t *)ip + ihl);
        if (ntohs(udp->src_port) == EPIC_ROCE_UDP_PORT ||
            ntohs(udp->dst_port) == EPIC_ROCE_UDP_PORT) {
            ctx->rx_roce_udp4791++;
        }
    }
}

static void forward_one(l2_ctx_t *ctx, int ingress, int egress,
                        const uint8_t *packet, size_t len) {
    if (egress == ingress) {
        return;
    }
    uint64_t t0 = ctx->profile ? nsec_now() : 0;
    int rc = -1;
    for (int attempt = 0; attempt <= SEND_RETRY_MAX; attempt++) {
        errno = 0;
        rc = pcap_sendpacket(ctx->handles[egress], packet, (int)len);
        if (rc == 0) {
            break;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            break;
        }
        ctx->send_eagain++;
        if ((attempt & 0x3f) == 0) {
            usleep(1);
        }
    }
    if (rc != 0) {
        fprintf(stderr, "pcap_sendpacket egress=%d: %s\n", egress,
                pcap_geterr(ctx->handles[egress]));
        ctx->dropped++;
        return;
    }
    uint64_t t1 = ctx->profile ? nsec_now() : 0;
    if (ctx->profile) {
        ctx->send_ns += t1 - t0;
    }
    ctx->send_calls++;
    ctx->fwd_packets++;
    ctx->fwd_bytes += len;
}

static void handle_packet(l2_ctx_t *ctx, int ingress, const uint8_t *packet,
                          size_t len) {
    uint64_t t_start = ctx->profile ? nsec_now() : 0;
    if (len < sizeof(epic_eth_t)) {
        ctx->dropped++;
        return;
    }

    ctx->rx_packets++;
    ctx->rx_bytes += len;
    count_payload(ctx, packet, len);

    const epic_eth_t *eth = (const epic_eth_t *)packet;
    uint64_t t_lookup0 = ctx->profile ? nsec_now() : 0;
    int egress = lookup_mac(ctx, eth->dst_mac);
    uint64_t t_lookup1 = ctx->profile ? nsec_now() : 0;
    if (ctx->profile) {
        ctx->lookup_ns += t_lookup1 - t_lookup0;
    }

    if (egress >= 0) {
        ctx->unicast_packets++;
        forward_one(ctx, ingress, egress, packet, len);
    } else {
        ctx->unknown_unicast++;
        ctx->dropped++;
    }

    if (ctx->profile) {
        ctx->total_ns += nsec_now() - t_start;
    }
}

static void event_loop(l2_ctx_t *ctx) {
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
            int ingress = (int)events[i].data.u32;
            const uint8_t *packet = NULL;
            struct pcap_pkthdr *hdr = NULL;
            int rc;
            while ((rc = pcap_next_ex(ctx->handles[ingress], &hdr, &packet)) == 1) {
                handle_packet(ctx, ingress, packet, hdr->caplen);
            }
            if (rc == PCAP_ERROR) {
                fprintf(stderr, "pcap_next_ex ingress=%d: %s\n", ingress,
                        pcap_geterr(ctx->handles[ingress]));
                running = 0;
                break;
            }
        }
    }
}

static double avg_ns(uint64_t ns, uint64_t n) {
    return n == 0 ? 0.0 : (double)ns / (double)n;
}

static void print_result(const l2_ctx_t *ctx) {
    printf("PCAP_L2_SWITCH_RESULT rx_packets=%" PRIu64 " rx_bytes=%" PRIu64
           " fwd_packets=%" PRIu64 " fwd_bytes=%" PRIu64
           " send_calls=%" PRIu64 " unicast_packets=%" PRIu64
           " unknown_unicast=%" PRIu64
           " rx_ipv4=%" PRIu64 " rx_roce_udp4791=%" PRIu64
           " send_eagain=%" PRIu64 " dropped=%" PRIu64
           " avg_total_ns=%.1f avg_lookup_ns=%.1f avg_send_ns=%.1f\n",
           ctx->rx_packets, ctx->rx_bytes, ctx->fwd_packets, ctx->fwd_bytes,
           ctx->send_calls, ctx->unicast_packets, ctx->unknown_unicast,
           ctx->rx_ipv4, ctx->rx_roce_udp4791, ctx->send_eagain, ctx->dropped,
           avg_ns(ctx->total_ns, ctx->rx_packets),
           avg_ns(ctx->lookup_ns, ctx->rx_packets),
           avg_ns(ctx->send_ns, ctx->send_calls));
}

static void usage(const char *prog) {
    fprintf(stderr, "usage: %s --config /tmp/epic-bench/l2.conf [--profile]\n",
            prog);
}

int main(int argc, char **argv) {
    const char *config = NULL;
    bool profile = false;
    static const struct option long_opts[] = {
        {"config", required_argument, NULL, 'c'},
        {"profile", no_argument, NULL, 'p'},
        {0, 0, 0, 0},
    };

    int c;
    while ((c = getopt_long(argc, argv, "", long_opts, NULL)) != -1) {
        switch (c) {
        case 'c':
            config = optarg;
            break;
        case 'p':
            profile = true;
            break;
        default:
            usage(argv[0]);
            return 2;
        }
    }
    if (!config) {
        usage(argv[0]);
        return 2;
    }

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);

    l2_ctx_t *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) {
        perror("calloc");
        return 1;
    }
    ctx->epoll_fd = -1;
    ctx->profile = profile;

    if (!load_config(ctx, config) || !setup_pcap(ctx)) {
        return 1;
    }

    printf("PCAP_L2_SWITCH_READY profile=%d\n", ctx->profile ? 1 : 0);
    fflush(stdout);
    event_loop(ctx);
    print_result(ctx);

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
