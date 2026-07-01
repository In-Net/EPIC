// #include <spdlog/spdlog.h>

#include "common.hpp"

static void test_loop()
{
    spdlog::info("load balancer test thread: {}", gettid());
    auto start = std::chrono::high_resolution_clock::now();
    uint64_t loop_cnt = 0;
    while(!loadr(daemon_quit)) {
        loop_cnt ++;
        offset_ptr<op_t> op_offset_ptr;
        if(!shm_qp->front_req(op_offset_ptr)) {
            netccl_yield();
            continue;
        }
        // std::cerr << "test thread: get req\n";
        // std::cerr << "test thread: push resp\n";

        // op_test_time = std::chrono::high_resolution_clock::now();
        // std::chrono::duration<double, std::micro> duration = op_test_time - op_start;
        // std::cerr << "test " << duration.count() << "\n"; // use buffer

        shm_qp->pop_req();
        shm_qp->push_resp(op_offset_ptr);
        // std::cerr << "test thread: done\n";
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;
    spdlog::info("load balancer test loop: {} {}", loop_cnt, loop_cnt/duration.count());
}


// static struct sub_op_t *create_sub_op(op_t *op, uint32_t offset, uint32_t size)
// {
//     return new sub_op_t{.op = op, .offset = offset, .size = size};
// }

static bool try_complete_op()
{
    op_t *op;
    for(int i = 0; i < nworker; i++) {
        sub_op_t* sub_op;
        if(!thread_ctx[i].qp->front_resp(sub_op)) return false;
    }
    for(int i = 0; i < nworker; i++) {
        sub_op_t* sub_op = NULL;
        thread_ctx[i].qp->front_resp(sub_op);
        thread_ctx[i].qp->pop_resp();
        op = sub_op->op;
        // spdlog::info("free {}", (void*)sub_op);
        delete sub_op;
    }
    shm_qp->push_resp(op);
    return true;
}

static bool try_post_op()
{
    offset_ptr<op_t> op_offset_ptr;
    if(!shm_qp->front_req(op_offset_ptr)) return false;
    // std::cerr << "test thread: get req\n";
    // std::cerr << "test thread: push resp\n";

    // op_test_time = std::chrono::high_resolution_clock::now();
    // std::chrono::duration<double, std::micro> duration = op_test_time - op_start;
    // std::cerr << "test " << duration.count() << "\n"; // use buffer
    op_t *op = op_offset_ptr.get();
    shm_qp->pop_req();

    // int prefered_worker = op->group_msn % nworker; // TODO: do not split small message
    void *data = op->shm_buffer.get();
    uint32_t offset = 0;
    uint32_t size_in_int32 = op->size / sizeof(int32_t);
    uint32_t additional_size = op->size % sizeof(int32_t);// for broadcast and all_gather, this may not be zero
    if(op->op_code == OP_ALL_REDUCE || op->op_code == OP_REDUCE) 
        assert(additional_size == 0);
    for(int i = 0; i < nworker; i++) {
        uint32_t sub_size_in_int32 = size_in_int32 / nworker + ((uint32_t)i < size_in_int32 % nworker);
        uint32_t sub_size = sub_size_in_int32 * sizeof(int32_t);
        if(i == nworker - 1) sub_size += additional_size;// allocate additional_size to the last worker
        agg_size_t agg_len = op->group.agg_len / nworker;
        agg_size_t agg_addr = op->group.agg_addr + agg_len * i;
        sub_op_t *sub_op = new sub_op_t{.op = op, .data = (char*)data + offset, .size = sub_size, .agg_addr = agg_addr, .agg_len = agg_len};// initialize other fields later
        // sub_op_t *sub_op = create_sub_op(op, offset, sub_size);
        // spdlog::info("alloc {}", (void*)sub_op);
        thread_ctx[i].qp->push_req(sub_op);
        offset += sub_size;
    }
    return true;
}

void load_balancer() // arg == NULL
{
    if(getenv("NETCCL_LB_LOOPBACK") != NULL) {
        test_loop();
        return;
    }
    if(nworker == 0) {
        spdlog::error("No worker for load balancer");
        return;
    }
    spdlog::info("load balancer thread: {}", gettid());
    auto start = std::chrono::high_resolution_clock::now();
    uint64_t loop_cnt = 0;
    while(!loadr(daemon_quit)) {
        loop_cnt ++;
        bool ret = false;
        ret |= try_complete_op();
        ret |= try_post_op();
        if(!ret) netccl_yield();
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;
    spdlog::info("load balancer loop: {} {}", loop_cnt, loop_cnt/duration.count());
}