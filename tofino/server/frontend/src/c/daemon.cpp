#include "common.hpp"

// daemon thread does not have GIL, acquire GIL before py::print
#define py_print(...) {py::gil_scoped_acquire acquire; py::print(__VA_ARGS__);}

// std::chrono::time_point<std::chrono::high_resolution_clock> op_start, op_end;

bool try_complete_op()
{
    offset_ptr<op_t> op_offset_ptr;
    if(!shm->qp.front_resp(op_offset_ptr)) return false;
    // std::cerr << "daemon thread: complete: get resp\n";
    op_t *op = op_offset_ptr.get();
    op_with_fut_t *op_with_fut = op->op_with_fut;

    // copy to device memory
    if(need_rx(op))
    {
        if(!op_with_fut->copying) {
            op_with_fut->copying = true;
            if(op_with_fut->is_first && !need_tx(op)) {
                CUDA_CHECK(cudaEventRecord(op_with_fut->event, op_with_fut->stream));
                CUDA_CHECK(cudaStreamWaitEvent(h2d_stream, op_with_fut->event));
            }
            if(op->size) { // barrier is a zero-length all_reduce, it should not generate a memcpy
                CUDA_CHECK(cudaMemcpyAsync(op_with_fut->data, op->shm_buffer.get(), op->size, cudaMemcpyHostToDevice, h2d_stream));
                CUDA_CHECK(cudaEventRecord(op_with_fut->event, h2d_stream));
            }    
            if(op_with_fut->is_last) {
                CUDA_CHECK(cudaStreamWaitEvent(op_with_fut->stream, op_with_fut->event));
            }
            return true;
        }
        // TODO: check this:
        // The wait is not necessary, but if we do not wait here, 
        // some async collectives will cause some tensor to have NANs.
        // I have not found the reason yet.
        if(cudaEventQuery(op_with_fut->event) != cudaSuccess) {
            return false;// check its completion later
        }
        op_with_fut->copying = false;
    }
    if(op_with_fut->fut_wrapper) {
        // GIL will be acquired in functions registered by then() in PythonFutureWrapper
        // op_with_fut->fut_wrapper->fut->markCompleted();
        std::thread fut_thread([](intrusive_ptr<Future> fut){fut->markCompleted();}, op_with_fut->fut_wrapper->fut);// TODO: change this to a daemon thread too
        fut_thread.detach();
        // this callback does not have a DDL
    }

    // complete this op
    // std::cerr << "daemon thread: complete: push resp\n";
    shm->qp.pop_resp();
    bool ret = py_qp.push_resp(op_with_fut);
    assert(ret == true);// to free memory
    // std::cerr << "daemon thread: complete: done\n";

    // op_end = std::chrono::high_resolution_clock::now();
    // std::chrono::duration<double, std::micro> duration = op_end - op_start;
    // std::cout << "daemon " << duration.count() << "\n"; // use buffer
    return true;
}

bool try_post_op()
{
    op_with_fut_t *op_with_fut;
    if(!py_qp.front_req(op_with_fut)) return false;

    // op_start = std::chrono::high_resolution_clock::now();

    // std::cerr << "daemon thread: post: get req\n";
    op_t *op = op_with_fut->op;

    // copy to host memory
    if(need_tx(op))
    {
        if(!op_with_fut->copying) {
            op_with_fut->copying = true;
            if(op_with_fut->is_first) {
                CUDA_CHECK(cudaEventRecord(op_with_fut->event, op_with_fut->stream));
                CUDA_CHECK(cudaStreamWaitEvent(d2h_stream, op_with_fut->event));
            }
            if(op->size) {// barrier is a zero-length all_reduce, it should not generate a memcpy
                CUDA_CHECK(cudaMemcpyAsync(op->shm_buffer.get(), op_with_fut->data, op->size, cudaMemcpyDeviceToHost, d2h_stream));
                CUDA_CHECK(cudaEventRecord(op_with_fut->event, d2h_stream));
            }
            if(op_with_fut->is_last && !need_rx(op)) {
                CUDA_CHECK(cudaStreamWaitEvent(op_with_fut->stream, op_with_fut->event));
            }
            return true;
        }
        if(cudaEventQuery(op_with_fut->event) != cudaSuccess) {
            return false;// check its completion later
        }
        op_with_fut->copying = false;
    }
    // std::cerr << "daemon thread: post: push req\n"; 
    py_qp.pop_req();
    bool ret = shm->qp.push_req(offset_ptr(op));
    // bool ret = shm->qp.push_resp(offset_ptr(op));
    assert(ret == true);
    // std::cerr << "daemon thread: post: done\n"; 
    return true;
}

// TODO: the daemon thread can be removed, and functions of the daemon thread can be invoked by the python thread (in api.cpp)
void daemon_loop(int device)
{
    // NOTE: Although CUDA context is sharable among threads, a thread choose device 0's context at startup,
    //       so we need to change the device here
    CUDA_CHECK(cudaSetDevice(device)); 
    spdlog::info("daemon thread {}, device {}", gettid(), device);
    auto start = std::chrono::high_resolution_clock::now();
    uint64_t loop_cnt = 0;
    while(!loadr(daemon_quit)) {
        loop_cnt ++;
        bool ret = false;
        ret |= try_complete_op();// do not use ||
        ret |= try_post_op();
        if(!ret) netccl_yield();
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;
    spdlog::info("daemon loop: {} {}", loop_cnt, loop_cnt/duration.count());
}

void test_loop()
{
    spdlog::info("test thread {}", gettid());
    auto start = std::chrono::high_resolution_clock::now();
    uint64_t loop_cnt = 0;
    while(!loadr(daemon_quit)) {
        loop_cnt ++;
        offset_ptr<op_t> op_offset_ptr;
        if(!shm->qp.front_req(op_offset_ptr)) {
            netccl_yield();
            continue;
        }
        // std::cerr << "test thread: get req\n";
        // std::cerr << "test thread: push resp\n";

        // op_test_time = std::chrono::high_resolution_clock::now();
        // std::chrono::duration<double, std::micro> duration = op_test_time - op_start;
        // std::cout << "test " << duration.count() << "\n"; // use buffer

        shm->qp.pop_req();
        shm->qp.push_resp(op_offset_ptr);
        // std::cerr << "test thread: done\n";
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;
    spdlog::info("test loop: {} {}", loop_cnt, loop_cnt/duration.count());
}

// void tx_loop()
// {
//     while(!loadr(daemon_quit)) {
//         try_post_op();
//     }
// }

// void rx_loop()
// {
//     while(!loadr(daemon_quit)) {
//         try_complete_op();
//     }
// }