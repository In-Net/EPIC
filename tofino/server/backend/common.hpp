#ifndef __NETCCL_BACKEND_COMMON_HPP__
#define __NETCCL_BACKEND_COMMON_HPP__

#include <unordered_map>
#include <endian.h>

#include <rte_eal.h>
#include <rte_ethdev.h>
#include <rte_cycles.h>
#include <rte_lcore.h>
#include <rte_mbuf.h>
#include <rte_flow.h>

#include "../common/common.hpp"
#include "header.hpp"
#include "process_buffer.hpp"
#include "net_utils.hpp"

#define MAX_WIN_LEN 1024 // 256KB, it can be larger
#define MAX_CWND (MAX_WIN_LEN - 1)
#define MAX_PKT_BURST 32
#define RX_BUFFER_CACHE 256
#define CC_REACTION_TIME 10 // in us
#define CC_FAST_RECOEVERY_CNT 5
#define CC_AI_FULL_INCREASE_TIME 10000 // in us
#define CC_AI_FULL_INCREASE_ITER (1.0 * CC_AI_FULL_INCREASE_TIME / CC_REACTION_TIME)
#define CC_AI (1.0 * MAX_WIN_LEN / CC_AI_FULL_INCREASE_ITER)
#define CC_HI_FULL_INCREASE_TIME 200 // in us
#define CC_HI_FULL_INCREASE_ITER (1.0 * CC_HI_FULL_INCREASE_TIME / CC_REACTION_TIME)
#define CC_HI (2.0 * MAX_WIN_LEN / (CC_HI_FULL_INCREASE_ITER * CC_HI_FULL_INCREASE_ITER))
#define CC_G (1.0/256)
#define CC_CWND_MIN 10
// #define CC_HYPER_INCREASE
#define TIMEOUT_TIME 1000

#define IPPROTO_NETCCL 0xfe

#define SWITCH_MAC "02:00:00:00:00:00" // NOTE: also defined in switch/topo.json, must be consistant

using ts_t = uint32_t;
using win_size_t = uint32_t;

#define INVALID_POSITION ((win_size_t)-1)

struct sub_op_t {
    op_t *op;
    void *data;
    uint32_t size;
    agg_size_t agg_addr;
    agg_size_t agg_len;
    // uint32_t pkt_sent;
    // psn_t start_psn;
} ALIGN64;

struct pkt_state_t {
    // uint32_t psn;               
    // uint32_t offset;            
    // uint16_t size;             
    bool acked;
    sub_op_t *sub_op;
    void *data;
    uint32_t size;
    psn_t psn;
    bool is_last;
    struct rte_mbuf *pkt;
};

struct cc_t {
    double cwnd, target_cwnd, loss_ratio;
    ts_t prev_decrease_ts, prev_adjust_ts;
    int adjust_cnt;
    cc_t () {
        target_cwnd = MAX_CWND;
        cwnd = CC_CWND_MIN;
        loss_ratio = 0;
        prev_decrease_ts = 0;
        prev_adjust_ts = 0;
        adjust_cnt = -CC_FAST_RECOEVERY_CNT;// starts from fast recovery
    }
    void reinit(ts_t now) {// used in full retransmission
        target_cwnd = cwnd;
        cwnd = CC_CWND_MIN;
        prev_decrease_ts = now;
        prev_adjust_ts = now;
        adjust_cnt = -CC_FAST_RECOEVERY_CNT;
    }
};

struct window_t {
    ts_t last_ack_time_stamp;
    win_size_t to_send;         
    win_size_t to_ack;
    // win_size_t tx_ptr;
    psn_t left_psn, right_psn;        
    cc_t cc;
    // double old_cwnd;
    // int cwnd_inc_iter;
    pkt_state_t state[MAX_WIN_LEN];
    window_t (struct rte_mempool *tx_pool, void *prefill_bytes, size_t prefill_size) {
        last_ack_time_stamp = 0;
        to_send = to_ack = 0;
        // tx_ptr = 0;
        left_psn = right_psn = 0;
        // cwnd = 1;// for debug
        for(auto &stt: state) {
            stt = {.pkt = rte_pktmbuf_alloc(tx_pool)};// other fields are zero 
            // assume prefill_bytes <= size of pktmbuf
            memcpy(rte_pktmbuf_mtod(stt.pkt, void*), prefill_bytes, prefill_size);// init packet headers (ETH & IP) without increasing mbuf->pkt_len
        }
    }
} ALIGN64;

struct thread_group_ctx_t {
    psn_t psn;
};

struct thread_ctx_t{
    int port_id;
    int worker_id;// == queue_id
    struct rte_mempool* tx_mbuf_pool;
    struct rte_mempool* rx_mbuf_pool;
    queue_pair_t<sub_op_t*, QUEUE_SIZE> *qp;
    struct window_t *window;
    queue_process_buffer *process_buffer;
    std::unordered_map<group_id_t, thread_group_ctx_t> group_ctx;
    std::shared_ptr<spdlog::logger> logger;
} ALIGN64;


// #define BACKEND_SEGMENT_SIZE_LIMIT (8 * 1024)
// #define BACKEND_SEGMENT_NPACKET (BACKEND_SEGMENT_SIZE_LIMIT/PAYLOAD_SIZE)
// #define BACKEND_SEGMENT_SIZE (BACKEND_SEGMENT_NPACKET*PAYLOAD_SIZE)

inline std::atomic<bool> daemon_quit;

inline managed_shared_memory *segment;
inline string shm_name;
inline shm_op_qp_t *shm_qp;
inline int nthread, nworker;
inline vector<thread_ctx_t>thread_ctx;

void shm_init(string);
// void worker_init(int);
int worker_loop(void*);
void load_balancer();

#endif