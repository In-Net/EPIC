#include "common.hpp"

#include <queue>
#include <boost/circular_buffer.hpp>

// void worker_init(int _nworker)
// {
//     nworker = _nworker;
//     worker_qp.resize(nworker);
//     for(auto &qp_ptr: worker_qp) {
//         qp_ptr = new std::remove_reference_t<decltype(*qp_ptr)>();
//     }
// }

#define LOG_INTERVAL 100000 // 100 ms

thread_local bool PACKET_LOOPBACK;
thread_local struct thread_ctx_t *ctx;

thread_local ts_t global_ts;

thread_local win_size_t current_switch_window_limit = CC_CWND_MIN;// assume CC_CWND_MIN * thread_cnt <= 1/2 switch memory size
thread_local group_id_t current_group_id = 0;// group 0 will not be used

thread_local uint8_t tos_minitor;
thread_local bool pkt_loss_flag;
thread_local bool traffic_todo;// TODO: remove this
thread_local bool cwnd_achieved;
thread_local size_t cwnd_inc_cnt, cwnd_dec_cnt;
thread_local size_t shift_send_cnt, shift_ack_cnt;
thread_local size_t total_shift_send_cnt, total_shift_ack_cnt;

thread_local size_t ack_cnt, prev_ack_cnt, prev_ack_cnt_cc;

thread_local size_t miss_queue_cnt;

thread_local size_t fast_retrans_trigger_cnt, fast_retrans_pkt_cnt, full_retrans_trigger_cnt, full_retrans_pkt_cnt;
thread_local size_t in_range_reduntant_ack_cnt, out_range_reduntant_ack_cnt;

// thread_local int cwnd_cnt[MAX_WIN_LEN] = {};
thread_local int win_cnt[MAX_WIN_LEN] = {};
// thread_local int cwnd_sum[MAX_WIN_LEN] = {};
thread_local int win_sum[MAX_WIN_LEN] = {};

// thread_local int rx_batch_cnt[MAX_PKT_BURST + 1] = {};

inline void memcpy_htobe32_inline(void *dst, const void *src, size_t size)
{
    // assert size % 4 == 0
    uint32_t *dst32 = (uint32_t *)dst;
    const uint32_t *src32 = (const uint32_t *)src;
    size_t size32 = size / sizeof(uint32_t);
    // should compile with -mavx512bw for best performance
    for(size_t i = 0; i < size32; i++) {
        dst32[i] = htobe32(src32[i]);
    }
}

void memcpy_htobe32(void *dst, const void *src, size_t size)
{
    memcpy_htobe32_inline(dst, src, size);
}

template<size_t size>
void memcpy_htobe32_fixed(void *dst, const void *src)
{
    memcpy_htobe32_inline(dst, src, size);
}

template void memcpy_htobe32_fixed<PAYLOAD_SIZE>(void *dst, const void *src);

inline void memcpy_htobe32_select(void *dst, const void *src, size_t size)
{
    if(likely(size == PAYLOAD_SIZE)) memcpy_htobe32_fixed<PAYLOAD_SIZE>(dst, src);
    else memcpy_htobe32(dst, src, size);
}

#define memcpy_be32toh_select memcpy_htobe32_select

static void test_loop()
{
    ctx->logger->info("worker test thread: {}", gettid());
    auto start = std::chrono::high_resolution_clock::now();
    uint64_t loop_cnt = 0;
    queue_pair_t<sub_op_t*, QUEUE_SIZE> *qp = ctx->qp;
    while(!loadr(daemon_quit)) {
        loop_cnt ++;
        sub_op_t *sub_op;
        if(!qp->front_req(sub_op)) continue;
        // std::cerr << "test thread: get req\n";
        // std::cerr << "test thread: push resp\n";

        // op_test_time = std::chrono::high_resolution_clock::now();
        // std::chrono::duration<double, std::micro> duration = op_test_time - op_start;
        // std::cerr << "test " << duration.count() << "\n"; // use buffer

        qp->pop_req();
        qp->push_resp(sub_op);
        // std::cerr << "test thread: done\n";
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;
    ctx->logger->info("worker test loop: {} {}", loop_cnt, loop_cnt/duration.count());
}

namespace local{

thread_local std::queue<struct rte_mbuf *>local_queue;
thread_local struct rte_mempool *local_pool;

void send(struct rte_mbuf *pkt)
{
    if(local_pool == NULL) {
        string name = string("LOCAL") + std::to_string(ctx->worker_id);
        local_pool = rte_pktmbuf_pool_create(name.data(), MAX_WIN_LEN, 64, 0, RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());
    }
    struct rte_mbuf *pkt_copy = rte_pktmbuf_copy(pkt, local_pool, 0, UINT32_MAX);
    local_queue.push(pkt_copy);
}

struct rte_mbuf * receive()
{
    if(local_queue.empty()) return NULL;
    auto ret = local_queue.front();
    local_queue.pop();
    return ret;
}

}

ts_t get_time_stamp_imm() // microseconds
{
    auto d = std::chrono::high_resolution_clock::now().time_since_epoch();
    ts_t ts = (ts_t)std::chrono::duration_cast<std::chrono::microseconds>(d).count();
    return ts;
}

ts_t get_time_stamp() // microseconds
{
    thread_local static uint32_t cnt;
    thread_local static ts_t ts;
    if(unlikely((cnt & 0x3) == 0)) {
        ts = get_time_stamp_imm();
    }
    cnt ++;
    return ts;
}

inline netccl_pkt* get_header(struct rte_mbuf *pkt)
{
    return rte_pktmbuf_mtod(pkt, netccl_pkt*);
}

inline psn_t get_psn(struct rte_mbuf *pkt) 
{
    return betoh(get_header(pkt)->inc.psn);
}

inline group_id_t get_group_id(struct rte_mbuf *pkt) 
{
    return betoh(get_header(pkt)->inc.group_id);
}

// inline uint8_t get_queueid(struct rte_mbuf *pkt) // for DEBUG
// {
//     return get_header(pkt)->inc.queue_id;
// }

inline void mark_acked(win_size_t position)
{
    // updating last_ack_time -> add ack_cnt: time -= 30
    ack_cnt ++;
    ctx->window->state[position].acked = true;
}

inline bool has_acked(win_size_t position)
{
    return ctx->window->state[position].acked;
}

inline bool window_tx_empty()
{
    // return ctx->window->to_ack == ctx->window->tx_ptr;
    return ctx->window->to_ack == ctx->window->to_send;
}

inline win_size_t window_len()
{
    return ctx->window->to_send >= ctx->window->to_ack ? ctx->window->to_send - ctx->window->to_ack : MAX_WIN_LEN + ctx->window->to_send - ctx->window->to_ack;
}

inline win_size_t get_prev_position(win_size_t pos)
{
    static_assert((MAX_WIN_LEN & (MAX_WIN_LEN-1)) == 0);
    return (pos - 1) & (MAX_WIN_LEN - 1);
    // return pos == 0 ? MAX_WIN_LEN - 1 : pos - 1;
}

inline win_size_t get_next_position(win_size_t pos)
{
    static_assert((MAX_WIN_LEN & (MAX_WIN_LEN-1)) == 0);
    return (pos + 1) & (MAX_WIN_LEN - 1);
    // return pos == MAX_WIN_LEN - 1 ? 0 : pos + 1;
}

win_size_t get_position(struct rte_mbuf *pkt)
{
    // NOTE: Since we will use a barrier between different group calls, 
    // the window only contains packets of ONE group, so the PSN in the window is continous
    // NOTE: Do not calculate the position by psn % MAX_WIN_LEN, since we change groups
    auto window = ctx->window;
    win_size_t position = INVALID_POSITION;
    if(unlikely(window_tx_empty()) || current_group_id != get_group_id(pkt)) {
        // ctx->logger->warn("1.1 psn {} to_ack==to_send=={}", get_psn(pkt), window->to_ack);
        return position;// empty window
    }
    psn_t psn = get_psn(pkt);
    psn_t left_psn = window->left_psn;// cache left_psn & right_psn, time -= 2
    psn_t right_psn = window->right_psn;
    if(likely(psn - left_psn <= right_psn - left_psn)) {// psn range is [0, UINT32_MAX]
        position = psn - left_psn + window->to_ack;
        if(position >= MAX_WIN_LEN) position -= MAX_WIN_LEN;// position range is [0, MAX_WIN_LEN-1]
    }
    else {
        // ctx->logger->warn("1.2: psn {} left_psn {} right_psn {} to_ack {} to_send {}", psn, left_psn, right_psn, window->to_ack, window->to_send);
    }
    return position;
}

void transmit(win_size_t position) // send without free
{
    struct rte_mbuf *pkt = ctx->window->state[position].pkt;
    if(unlikely(PACKET_LOOPBACK)) {
        local::send(pkt);
        return;
    }
    rte_pktmbuf_refcnt_update(pkt, 1);// avoid free this mbuf
#ifdef rte_eth_tx_queue_count
    while(ctx->process_buffer->tx_buffered_pkt_cnt() >= 2 * MAX_PKT_BURST);
#endif
    ctx->process_buffer->send(pkt);// call rte_eth_tx_burst
}

void transmit(win_size_t begin, win_size_t end)
{
    for(win_size_t i = begin; i != end; i = get_next_position(i)) 
        transmit(i);
}

size_t transmit_unacked(win_size_t begin, win_size_t end)
{
    size_t cnt = 0;
    auto state = ctx->window->state;
    for(win_size_t i = begin; i != end; i = get_next_position(i)) 
        if(!state[i].acked) {
            cnt ++;
            transmit(i);
        }
    return cnt;
}

void complete_pkt(struct pkt_state_t *state)
{
    // memcpy cannot be put here, because it needs to cache RX packets
    if(unlikely(state->is_last)) {
        // last packet of a sub_op
        bool ret = ctx->qp->push_resp(state->sub_op);
        assert(ret == true);
    }
}

void shift_ack()
{
    auto window = ctx->window;
    // auto to_ack = window->to_ack, tx_ptr = window->tx_ptr;
    // while(to_ack != tx_ptr && has_acked(to_ack)) {
    auto to_ack = window->to_ack, to_send = window->to_send;
    while(to_ack != to_send && has_acked(to_ack)) {
        complete_pkt(&window->state[to_ack]);
        to_ack = get_next_position(to_ack);
        shift_ack_cnt ++;
    }
    window->to_ack = to_ack;
    window->left_psn = window_tx_empty() ? 0 : get_psn(window->state[to_ack].pkt);
}

void collect_retrans(win_size_t position)
{
    // make sure at least one packet needs retransmission
    auto prev_position = position;
    size_t pkt_cnt = 0;
    while(prev_position != ctx->window->to_ack) {
        auto prev_prev_position = get_prev_position(prev_position);
        if(has_acked(prev_prev_position)) break;
        prev_position = prev_prev_position;
        pkt_cnt++;
    }
    fast_retrans_pkt_cnt += pkt_cnt;
    fast_retrans_trigger_cnt ++;
    pkt_loss_flag = true;
    // Fast retransmission, assume the network keeps packet order of most packets
    // This seldom transmit more than MAX_PKT_BURST packets
    // if(pkt_cnt) spdlog::warn("fast_retrans [{},{})", prev_position, position);
    transmit(prev_position, position);

    // auto window = ctx->window;
    // window->tx_ptr = prev_position;// GBN
    // window->right_psn = window_tx_empty() ? 0 : get_psn(window->state[get_prev_position(window->tx_ptr)].pkt);
}

void rx_process_one(struct rte_mbuf *pkt)
{
    // Make sure flow director works correctly, so we can delete these two "if"
    // delete this: time 32->31
    // if(unlikely(get_queueid(pkt) != ctx->worker_id)) {
    //     miss_queue_cnt ++; 
    //     ctx->logger->error("PKT_Q {} WORKER_Q {}", get_queueid(pkt), ctx->worker_id); 
    //     return;
    // }
    // delete this: time 32->27
    // if(unlikely(pkt->pkt_len != sizeof(netccl_pkt))) {
    //     ctx->logger->warn("packet size mismatch: expect {}, get {}", sizeof(netccl_pkt), pkt->pkt_len);
    //     return;
    // }
    
    // debug
    // printf("%d\n", pkt->pkt_len);
    // for(size_t i = 0; i < sizeof(netccl_pkt) - sizeof(netccl_payload); i++) {
    //     printf("%x ", ((uint8_t*)get_header(pkt))[i]);
    // }
    // printf("\n");//bug here
    // fflush(stdout);
    tos_minitor |= get_header(pkt)->ip.type_of_service;

    auto position = get_position(pkt);
    if(unlikely(position == INVALID_POSITION || has_acked(position))) {
        if(position == INVALID_POSITION) out_range_reduntant_ack_cnt ++;
        else in_range_reduntant_ack_cnt ++;
        return; // has an invalid psn
    }

    mark_acked(position);
    pkt_state_t *state = &ctx->window->state[position];
    op_t *op = state->sub_op->op;
    int error_code = 0;
    if(need_rx(op)) {
        // if we want to aggregate date on the switch, we must use big endian
        if(op->op_code == OP_ALL_REDUCE || op->op_code == OP_REDUCE)
            memcpy_be32toh_select(state->data, get_header(pkt)->payload.data, state->size);
        else // otherwise, just copy the data, and it can be unaligned
            memcpy(state->data, get_header(pkt)->payload.data, state->size);
        // directly copy the RX packet to the shared memory, otherwise we need buffer these packets
        if(unlikely(pkt->pkt_len != sizeof(netccl_pkt))) {
            error_code = 1;
        }
    }
    else if(unlikely(pkt->pkt_len != max((size_t)60, sizeof(netccl_pkt) - size_of_member(netccl_pkt,payload)))) {
        error_code = 2;
    }

    if(unlikely(error_code != 0)) {
        ctx->logger->error("RX: error_code {} op->op_code {} psn {} ip_len {} pkt_len {}", error_code, op->op_code, get_psn(pkt), (int)betoh(get_header(pkt)->ip.total_length), pkt->pkt_len);
        for(size_t i = 0; i < pkt->pkt_len; i++) {
            fprintf(stderr, "%02x ", (unsigned int)((unsigned char*)get_header(pkt))[i]);
        }
        fprintf(stderr, "\n");
    }
    
    // move this outside: time -0.3?
    // if(likely(position == ctx->window->to_ack)) {
    //     shift_ack();
    //     return;
    // }

    // add this: time -2
    if(likely(position == ctx->window->to_ack || has_acked(get_prev_position(position)))) return;

    // delete inline this: -2
    collect_retrans(position);
}

void rx_process()
{
    // struct rte_mbuf *pkts[MAX_PKT_BURST];
    // for(int i = 0; i < MAX_PKT_BURST; i++) {
    //     struct rte_mbuf *pkt = unlikely(PACKET_LOOPBACK) ? local::receive() : ctx->process_buffer->receive();
    //     pkts[i] = pkt;
    //     if(pkt == NULL) break;
    //     __builtin_prefetch(pkt, 0, 0);
    //     // rx_process_one(pkt);
    //     // ctx->process_buffer->drop(pkt);
    // }

    // infinite loop: RX has a higher priority than TX
    size_t process_cnt = 0;
    while(1) {
        struct rte_mbuf *pkt = unlikely(PACKET_LOOPBACK) ? local::receive() : ctx->process_buffer->receive();
        if(pkt == NULL) break;
        rx_process_one(pkt);
        ctx->process_buffer->drop(pkt);
        process_cnt ++;
        if(ctx->process_buffer->rx_sw_buffered_pkt_cnt() == 0 && ctx->process_buffer->rx_hw_buffered_pkt_cnt() < MAX_PKT_BURST)
            break;
    }
    auto idx = process_cnt % MAX_PKT_BURST;
    if(idx == 0 && process_cnt != 0) idx = MAX_PKT_BURST;
    // rx_batch_cnt[idx] ++;
    shift_ack();
}

void fill_pkt(pkt_state_t *state)
{
    struct rte_mbuf *pkt = state->pkt;
    sub_op_t *sub_op = state->sub_op;
    op_t *op = sub_op->op;
    group_t *group = &op->group;
    /*
    rte_pktmbuf_reset(pkt);// reset the pointer of rte_pktmbuf_append
    netccl_pkt *header = (netccl_pkt*) rte_pktmbuf_append(pkt, sizeof(netccl_pkt));
    */
    netccl_pkt *header = get_header(pkt);
    // many fields have been prefilled by constructor of `struct window_t` in dpdk_init()

    // header->ip.type_of_service = htobe((uint8_t)(ctx->worker_id << 2 | 0x1));// use TOS as queue ID for flow director
    bool has_payload = need_tx(op);
    size_t pkt_size = has_payload ? sizeof(netccl_pkt) : sizeof(netccl_pkt) - sizeof(netccl_payload);

    pkt->data_len = pkt_size;
    pkt->pkt_len = pkt_size;

    htobe(&header->ip.total_length, pkt_size - sizeof(rte_ether_hdr));
    htobe(&header->ip.dst_addr, group->switch_ip);

    // agg_len and agg_addr of sub_op
    agg_size_t sub_agg_len = sub_op->agg_len;// op->group.agg_len / nworker;// allow agg_len % nworker != 0
    agg_size_t sub_agg_addr = sub_op->agg_addr;// op->group.agg_addr + sub_agg_len * ctx->worker_id;
    psn_t psn = state->psn;
    header->inc = { .coll_type = htobe(op->op_code),
                    // .queue_id = htobe((uint8_t)ctx->worker_id), 
                    .group_id = htobe(group->group_id),
                    .rank = htobe(group->rank),
                    .root = htobe(op->root),
                    .agg_addr = htobe((agg_size_t)(sub_agg_addr + psn % sub_agg_len)), 
                    .psn = htobe(psn), 
                    };
    /*
    psn_t psn = sub_op->start_psn + sub_op->pkt_sent;
    header->inc.psn = htobe((psn_t)psn);
    header->inc.queue_id = htobe((uint8_t)ctx->worker_id);
    */
    // static thread_local bool first = 1;
    // if(first) {
    //     first = 0;
    //     for(size_t i = 0; i < sizeof(netccl_pkt)-sizeof(netccl_payload); i++) {
    //         ctx->logger->info("{}: {:x}", i, ((uint8_t*)header)[i]);
    //     }
    // }
    if(has_payload) {
        // if we want to aggregate date on the switch, we must use big endian
        if(op->op_code == OP_ALL_REDUCE || op->op_code == OP_REDUCE)
            memcpy_htobe32_select(header->payload.data, state->data, state->size);
        else // otherwise, just copy the data, and it can be unaligned
            memcpy(header->payload.data, state->data, state->size);
        // padding
        if(unlikely(state->size < PAYLOAD_SIZE)) memset((char*)header->payload.data + state->size, 0, PAYLOAD_SIZE - state->size);
    }
}
 
// TODO: add a builtin barrier in gen_pkt() to support resource sharing
bool gen_pkt(pkt_state_t *state)
{
    thread_local static uint32_t pkt_sent, start_psn, pkt_cnt;
    thread_local static sub_op_t *sub_op;// current processing sub op, can be NULL
    if(unlikely(!sub_op)) {
        if(unlikely(!ctx->qp->front_req(sub_op))) {
            traffic_todo = false;
            return false;
        }
        // Diffrent groups should not use the same window at the same time, 
        // because, at least, they have different PSN.
        // Thus, we switch the group only when the window is empty.
        if(current_group_id != sub_op->op->group.group_id && !window_tx_empty()) {
            sub_op = NULL;
            traffic_todo = false;
            return false;
        }
        current_group_id = sub_op->op->group.group_id;
        current_switch_window_limit = sub_op->agg_len / 2;// sub_op->agg_len is the same for all OPs in the same group
        
        traffic_todo = true;
        auto &psn = ctx->group_ctx[sub_op->op->group.group_id].psn;
        start_psn = psn;
        pkt_cnt = (sub_op->size + PAYLOAD_SIZE - 1) / PAYLOAD_SIZE;
        // flag is_last requires pkt_cnt >= 1
        // if sub_op->size == 0, fill_pkt() and rx_process_one() copies zero bytes of payload
        if(pkt_cnt == 0) pkt_cnt = 1;
        pkt_sent = 0;
        psn += pkt_cnt;
    }
    uint32_t size_sent = pkt_sent * PAYLOAD_SIZE;
    state->acked = false;
    state->sub_op = sub_op;
    state->data = (char*)sub_op->data + size_sent;
    state->size = std::min((uint32_t)PAYLOAD_SIZE, sub_op->size - size_sent);
    state->psn = start_psn + pkt_sent;
    state->is_last = pkt_sent + 1 == pkt_cnt;
    //state->pkt is kept unchanged

    fill_pkt(state);

    pkt_sent ++;

    if(unlikely(state->is_last)) {
        ctx->qp->pop_req();
        sub_op = NULL;
    }
    return true;
}

void shift_send()
{
    auto window = ctx->window;
    win_size_t win_len = window_len();

    // assert(window->cc.cwnd < MAX_WIN_LEN);
    // NOTE: cwnd is a floating point number

    // limit the transmission volumn 
    auto expect_win_len = std::min((win_size_t)MAX_WIN_LEN - 1,                                 // maximum
                          std::min((win_size_t)current_switch_window_limit,                             // switch memory limit
                          std::min((win_size_t)window->cc.cwnd,                                 // CC limit
                                    win_len + (win_size_t)MAX_PKT_BURST)));                     // burst limit
    auto to_send = window->to_send;
    while(win_len < expect_win_len) {// window->cc.cwnd < MAX_WIN_LEN
        if(unlikely(!gen_pkt(&window->state[to_send]))) break;
        if(unlikely(win_len == 0)) window->left_psn = get_psn(window->state[window->to_ack].pkt);// to_send == to_ack
        transmit(to_send);
        to_send = get_next_position(to_send);
        win_len ++;
        shift_send_cnt ++;
    }
    if(win_len >= (win_size_t)window->cc.cwnd) cwnd_achieved = true;
    window->to_send = to_send;
    // TODO: split pkt gen and trans
    // window->left_psn = window_tx_empty() ? 0 : get_psn(window->state[window->to_ack].pkt);
    window->right_psn = window_tx_empty() ? 0 : get_psn(window->state[get_prev_position(window->to_send)].pkt);

    // auto tx_ptr = window->tx_ptr;
    // for(size_t i = 0; i < MAX_PKT_BURST; i++) {
    //     if(tx_ptr == to_send) break;
    //     transmit(tx_ptr);
    //     tx_ptr = get_next_position(tx_ptr);
    // }
    // window->tx_ptr = tx_ptr;
    // window->right_psn = window_tx_empty() ? 0 : get_psn(window->state[get_prev_position(tx_ptr)].pkt);
}   

void full_retrans()
{
    auto window = ctx->window;
    if(window->to_ack == window->to_send) return;
    window->last_ack_time_stamp = global_ts;
    // spdlog::warn("full retrans {} pkts", window_len());
    full_retrans_trigger_cnt++;
    full_retrans_pkt_cnt += transmit_unacked(window->to_ack, window->to_send);
    
    ctx->window->cc.reinit(global_ts);

    // full_retrans_pkt_cnt += window_len();
    // window->tx_ptr = window->to_ack;// GBN
    // window->right_psn = window_tx_empty() ? 0 : get_psn(window->state[get_prev_position(window->tx_ptr)].pkt);
}

void tx_process()
{
    auto window = ctx->window;

    if(ack_cnt != prev_ack_cnt) {
        prev_ack_cnt = ack_cnt;
        window->last_ack_time_stamp = global_ts;
    }

    if(unlikely(global_ts - window->last_ack_time_stamp >= TIMEOUT_TIME)) {// no new ACK is received for a lone time
        full_retrans();
    }

    shift_send();

    // check after expand the window
    if(unlikely(window_tx_empty())) window->last_ack_time_stamp = global_ts;// keep time stamp the newest when there is no transmission
}

// DCQCN
void adjust_rate(bool congestion)
{
    auto &cc = ctx->window->cc;
    int &adjust_cnt = cc.adjust_cnt;
    double &cwnd = cc.cwnd; 
    double &target_cwnd = cc.target_cwnd;
    double &loss_ratio = cc.loss_ratio; 
    if(congestion) {
        loss_ratio = (1.0 - CC_G) * loss_ratio + CC_G;
        target_cwnd = cwnd;
        cwnd = cwnd * (1.0 - loss_ratio / 2.0);
        adjust_cnt = -CC_FAST_RECOEVERY_CNT; // negative value means fast recovery
        cwnd_dec_cnt ++;
    }
    else {
        loss_ratio = (1.0 - CC_G) * loss_ratio;
        // do not increase cwnd if window_size has not achieved cwnd
        if(cwnd_achieved) {
            if(adjust_cnt >= 0) {
    #ifdef CC_HYPER_INCREASE // hyper increase
                target_cwnd = target_cwnd + CC_HI * adjust_cnt;
    #else // addictive increase
                target_cwnd = target_cwnd + CC_AI;
    #endif
            }
            // else, do fast recovery, only change cwnd
            adjust_cnt ++;
            cwnd = (target_cwnd + cwnd) / 2.0;
            cwnd_inc_cnt ++;
        }
    }
    cwnd_achieved = false;
    if(cwnd < CC_CWND_MIN) cwnd = CC_CWND_MIN;// cwnd cannot be 0
}

void congestion_control()
{
    auto &cc = ctx->window->cc;
    auto &prev_decrease_ts = cc.prev_decrease_ts;
    auto &prev_adjust_ts = cc.prev_adjust_ts; 
    if(likely(global_ts - prev_decrease_ts < CC_REACTION_TIME)) return;
    if(unlikely(ack_cnt == prev_ack_cnt_cc)) return;// require at least one ACK 
    
    const uint8_t ECN_MASK = 0x3, CONGESTION_OCCUR = 0x3;
    if(likely((tos_minitor & ECN_MASK) != CONGESTION_OCCUR && pkt_loss_flag == false)) {
        // no congestion
        if(likely(global_ts - prev_adjust_ts < CC_REACTION_TIME)) return;
        // increase cwnd
        adjust_rate(false);
        prev_adjust_ts = global_ts;
    }
    else {
        // congestion exists, decrease cwnd
        adjust_rate(true);
        prev_decrease_ts = global_ts;
        prev_adjust_ts = global_ts;
        tos_minitor = 0;
        pkt_loss_flag = false;
    }       
    if(traffic_todo == true) {
        // cwnd_cnt[(int)cc.cwnd]++;
        win_cnt[window_len()]++;
    }
    prev_ack_cnt_cc = ack_cnt;
}

int worker_loop(void* arg)
{
    ctx = (struct thread_ctx_t *) arg;
    if(getenv("NETCCL_WORKER_LOOPBACK") != NULL) {
        test_loop();
        return 0;
    }
    if(getenv("NETCCL_PACKET_LOOPBACK") != NULL) {
        PACKET_LOOPBACK = true;
    }
    ctx->logger->info("worker thread: {}", gettid());
    auto start = std::chrono::high_resolution_clock::now();
    uint64_t loop_cnt = 0;
    ts_t base_ts = get_time_stamp();
    ts_t tr = 0, tt = 0, log_ts = 0;
    while(!loadr(daemon_quit)) {
        loop_cnt ++;
        global_ts = get_time_stamp() - base_ts;
        ts_t t1 = get_time_stamp_imm();
        std::atomic_thread_fence(std::memory_order_seq_cst);
        rx_process();
        std::atomic_thread_fence(std::memory_order_seq_cst);
        ts_t t2 = get_time_stamp_imm();
        std::atomic_thread_fence(std::memory_order_seq_cst);
        tx_process();// make congestion_control after tx_process, so it can get the enlarged window size
        std::atomic_thread_fence(std::memory_order_seq_cst);
        ts_t t3 = get_time_stamp_imm();
        if(traffic_todo) {
            tt += t2 - t1;
            tr += t3 - t2;
        }
        congestion_control();

        if(unlikely(global_ts - log_ts > LOG_INTERVAL)) {
            double dt = (global_ts - log_ts) * 1e-6;
            log_ts = global_ts;
            total_shift_send_cnt += shift_send_cnt;
            total_shift_ack_cnt += shift_ack_cnt;

            if(ctx->process_buffer->total_tx_nb != 0) {
                ctx->logger->info("TIME {:.4f}s", global_ts * 1e-6);
                ctx->logger->info("SW_TPT {:.4f}Gbps SW_GPT {:.4f}Gbps", 
                    1e-9 * 8 * shift_ack_cnt * sizeof(netccl_pkt) / dt, 1e-9 * 8 * shift_ack_cnt * sizeof(netccl_payload) / dt);
                ctx->logger->info("TOTAL_TX {} TOTAL_RX {} WIN {} CWND {:.1f} PSN {}~{} TOTAL_SHIFT_SEND {} TOTAL_SHIFT_ACK {}", 
                    ctx->process_buffer->total_tx_nb, ctx->process_buffer->total_rx_nb, window_len(), ctx->window->cc.cwnd, 
                    ctx->window->left_psn, ctx->window->right_psn, total_shift_send_cnt, total_shift_ack_cnt);
                ctx->logger->info("CWND_INCREASE_CNT {} CWND_DECREASE_CNT {}", cwnd_inc_cnt, cwnd_dec_cnt);
                ctx->logger->info("FAST_RETRANS_TRIGGER {} FAST_RETRANS_PKT {} FULL_RETRANS_TRIGGER {} FULL_RETRANS_PKT {}", 
                    fast_retrans_trigger_cnt, fast_retrans_pkt_cnt, full_retrans_trigger_cnt, full_retrans_pkt_cnt);
                ctx->logger->info("IN_RANGE_REDUNTANT_ACK {} OUT_RANGE_REDUNTANT_ACK {} Q_MISS_PKT {}", 
                    in_range_reduntant_ack_cnt, out_range_reduntant_ack_cnt, miss_queue_cnt); 
                ctx->logger->info("TR {} TT {}", tr, tt); 
                for(size_t i = 0; i < MAX_WIN_LEN; i++) {
                    win_sum[i] = i == 0 ? win_cnt[i] : win_sum[i-1] + win_cnt[i];
                }
                // double frac[] = {0, 0.01, 0.05, 0.1, 0.3, 0.5, 0.7, 0.9, 0.95, 0.99, 1};
                double frac[] = {0, 0.1, 0.5, 0.9, 1};
                ctx->logger->info("window size:");
                for(size_t i = 0, cur = 0; i < MAX_WIN_LEN; i ++) {
                    while(1.0*win_sum[i]/win_sum[MAX_WIN_LEN-1] > frac[cur]) {
                        ctx->logger->info("{}%: {}", (int)(frac[cur]*100), i);
                        cur++;
                    }
                    if(win_sum[i] == win_sum[MAX_WIN_LEN-1]) {
                        ctx->logger->info("{}%: {}", (int)(frac[cur]*100), i);
                        break;
                    }
                }
                // ctx->logger->info("rx batch cnt:");
                // ctx->logger->info("{}: {}", 0, rx_batch_cnt[0]);
                // for(size_t i = 1; i <= MAX_PKT_BURST; i++) {
                //     ctx->logger->info("{}K+{}: {}", MAX_PKT_BURST, i, rx_batch_cnt[i]);
                // }
            }
            ctx->process_buffer->total_tx_nb = ctx->process_buffer->total_rx_nb = 0;
            shift_send_cnt = shift_ack_cnt = 0;
            cwnd_inc_cnt = cwnd_dec_cnt = 0;
            fast_retrans_trigger_cnt = fast_retrans_pkt_cnt = full_retrans_trigger_cnt = full_retrans_pkt_cnt = 0;
            in_range_reduntant_ack_cnt = out_range_reduntant_ack_cnt = miss_queue_cnt = 0;
            tr = tt = 0;
            memset(win_cnt, 0, sizeof(win_cnt));
            // memset(rx_batch_cnt, 0, sizeof(rx_batch_cnt));
        }
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;
    ctx->logger->info("worker loop: {} {}", loop_cnt, loop_cnt/duration.count());
    return 0;
}