#ifndef __NETCCL_BACKEND_PROCESS_BUFFER_HPP__
#define __NETCCL_BACKEND_PROCESS_BUFFER_HPP__

class queue_process_buffer {
private:
    const uint16_t portid;
    const uint16_t queueid;
    const uint16_t rx_size;
    const uint16_t tx_size;
    uint16_t rx_nb;
    uint16_t tx_nb;
    // uint16_t drop_nb;
    uint16_t rx_iter;
    uint16_t flush_timer;
    uint16_t receive_timer;
    struct rte_mbuf **rx_bufs;
    struct rte_mbuf **tx_bufs;
    // struct rte_mbuf **drop_bufs;
    // void (*prefetch_hook) (struct rte_mbuf *buf);
public:
    uint32_t total_rx_nb;
    uint32_t total_tx_nb;
    uint32_t total_drop_nb;
    uint32_t total_alloc_nb;

private:    
    void check_flush_timer() {
        if(likely(flush_timer < tx_size * 2)) {
            flush_timer ++;
            return;
        }
        flush_once();// clears timer inside
    }

    template<bool immediate>
    uint16_t batch_receive() {
        if(immediate) return rte_eth_rx_burst(portid, queueid, rx_bufs, rx_size);

        int queueing_pkt_cnt = rte_eth_rx_queue_count(portid, queueid);
        if(queueing_pkt_cnt < rx_size && receive_timer < 1) {// returns empty for at most one call when there are packets to be received
            receive_timer ++;
            return 0;
        }
        receive_timer = 0;
        return rte_eth_rx_burst(portid, queueid, rx_bufs, rx_size);
    }

    template<bool immediate>
    struct rte_mbuf * receive_impl() {
        check_flush_timer();
        if(unlikely(rx_iter == rx_nb)) {
            rx_iter = 0;
            rx_nb = batch_receive<immediate>();
            // for(uint16_t i = 0; i < rx_nb; i++) prefetch_hook(rx_bufs[i]);
            if(rx_nb == 0) return NULL;
        }
        total_rx_nb ++;
        return rx_bufs[rx_iter ++];
    }

public:
    queue_process_buffer(uint16_t _portid, uint16_t _queueid, uint16_t _rx_size, uint16_t _tx_size) 
        : portid(_portid), queueid(_queueid), rx_size(_rx_size), tx_size(_tx_size) 
    {
        rx_nb = tx_nb = rx_iter = flush_timer = receive_timer = 0;
        // drop_nb = 0;
        rx_bufs = new struct rte_mbuf * [rx_size]();
        tx_bufs = new struct rte_mbuf * [tx_size]();
        // drop_bufs = new struct rte_mbuf * [rx_size]();
        total_rx_nb = total_tx_nb = total_drop_nb = total_alloc_nb = 0;
        // prefetch_hook = empty_prefetch_hook;
        assert(rx_size % 8 == 0);
    }

    // void register_prefetch_hook(void (*hook) (struct rte_mbuf *buf)) 
    // {
    //     prefetch_hook = hook;
    // }

    void flush() {
        uint16_t complete_nb = rte_eth_tx_burst(portid, queueid, tx_bufs, tx_nb);
        while(unlikely(complete_nb < tx_nb)) {
            complete_nb += rte_eth_tx_burst(portid, queueid, tx_bufs + complete_nb, tx_nb - complete_nb);
        }
        tx_nb = 0;
        flush_timer = 0;
    }

    void flush_once() {
        if(likely(tx_nb)) {
            uint16_t complete_nb = rte_eth_tx_burst(portid, queueid, tx_bufs, tx_nb);
            if(unlikely(complete_nb < tx_nb)) {
                memmove(tx_bufs, tx_bufs + complete_nb, tx_nb - complete_nb);
            }
            tx_nb -= complete_nb;
        }    
        flush_timer = 0;
    }

    // size_t rx_current_batch_size() {
    //     return rx_nb;
    // }

    // size_t rx_current_batch_left() {
    //     return rx_nb - rx_iter;
    // }

    // size_t rx_queueing_pkt_cnt() {
    //     return rte_eth_rx_queue_count(portid, queueid);
    // }

    struct rte_mbuf * receive() {// buffered receive
        return receive_impl<false>();
    }

    struct rte_mbuf * receive_imm() {// immediate receive
        return receive_impl<true>();
    }

    void send(struct rte_mbuf * buf) {
        total_tx_nb ++;

        tx_bufs[tx_nb ++] = buf;
        // if all the sending packet is receive from the same queue, usually there is "tx_nb < tx_size"
        if(tx_nb == tx_size) flush_once();
    }

    void drop(struct rte_mbuf * buf) {
        total_drop_nb ++;
        // drop_bufs[drop_nb] = buf;
        // drop_nb ++;
        // if(drop_nb == rx_size) {
        //     rte_pktmbuf_free_bulk(drop_bufs, drop_nb);
        //     drop_nb = 0;
        // }   
        rte_pktmbuf_free(buf);// faster 
    }
} __attribute__((aligned(64)));

#endif