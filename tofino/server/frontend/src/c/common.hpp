#ifndef __NETCCL_FRONTEND_COMMON_HPP__
#define __NETCCL_FRONTEND_COMMON_HPP__

#include <cuda.h>
#include <cuda_runtime.h>
#include <torch/extension.h>
#include <torch/csrc/jit/python/pybind_utils.h> // for PythonFutureWrapper
#include <torch/csrc/cuda/Stream.h> // for THCPStream

#include "../../../common/common.hpp"

using c10::ivalue::Future;
using c10::make_intrusive;
using c10::intrusive_ptr;
using torch::jit::PythonFutureWrapper;

// Tune these three numbers to get best performance
#define FRONTEND_SEGMENT_SIZE (2 * 1024 * 1024) // should <= 1G
#define SHM_COPY_BUFFER_SIZE (8 * 1024 * 1024)

#define SHM_MISC_SIZE (1 * 1024 * 1024)

inline void cuda_check(cudaError_t err, const char* const func, const char* const file, const int line)
{
    if (err != cudaSuccess)
    {
        std::cerr << "CUDA Runtime Error at: " << file << ":" << line
                  << std::endl;
        std::cerr << cudaGetErrorString(err) << ": " << func << std::endl;
        // std::exit(EXIT_FAILURE);
    }
}
#define CUDA_CHECK(val) cuda_check((val), #val, __FILE__, __LINE__)
#define CUDA_CHECK_KERNEL(...) {__VA_ARGS__; cuda_check(cudaPeekAtLastError(), #__VA_ARGS__, __FILE__, __LINE__);} 

struct op_with_fut_t {
    bool copying;
    void *data;
    cudaStream_t stream;
    bool is_first;
    bool is_last;
    cudaEvent_t event;
    shared_ptr<PythonFutureWrapper> fut_wrapper;
    op_t *op;
};

struct copy_buffer_t {// accessed by only one thread
    // allocated range is [tail, head) 
    // head, tail is in range [0, SHM_COPY_BUFFER_SIZE]
    uint32_t head, tail; 
    char ALIGN4096 buffer[SHM_COPY_BUFFER_SIZE + 1]; // use at most SHM_COPY_BUFFER_SIZE bytes
    copy_buffer_t () {
        head = tail = 0;
    }
    uint32_t available() {
        if(tail == 0) return SHM_COPY_BUFFER_SIZE - head;
        if(head >= tail) return max(SHM_COPY_BUFFER_SIZE + 1 - head, tail - 1);
        return tail - 1 - head;
    }
    void* fifo_allocate(uint32_t size) {
        auto avail = available();
        if(size > avail) return NULL;
        void* ret = buffer + head;
        // py::print("allocate ", size);
        // py::print("before: head ", head, "tail ", tail);
        head += size;
        if(head == SHM_COPY_BUFFER_SIZE + 1) head = 0;
        if(head > SHM_COPY_BUFFER_SIZE + 1) ret = buffer, head = size;
        // py::print("after: head ", head, "tail ", tail);
        return ret;
    }
    void fifo_free(uint32_t size) {
        assert(size <= SHM_COPY_BUFFER_SIZE);
        // py::print("free ", size);
        // py::print("before: head ", head, "tail ", tail);
        if(unlikely(tail + size > SHM_COPY_BUFFER_SIZE)) {
            assert(head < tail);
            tail = 0;
        }
        assert((head >= tail) == (head >= tail + size));
        tail += size;
        // py::print("after: head ", head, "tail ", tail);
    }
};

struct op_buffer_t {// accessed by only one thread
    uint32_t head, cnt;
    op_t ALIGN64 buffer[QUEUE_SIZE];
    op_buffer_t () {
        head = cnt = 0;
    }
    uint32_t available() {
        return QUEUE_SIZE - cnt;
    }
    op_t* fifo_allocate() {
        if(cnt == QUEUE_SIZE) return NULL;
        op_t *ret = buffer + head;
        cnt ++;
        head ++;
        if(head == QUEUE_SIZE) head = 0;
        return ret;
    }
    void fifo_free() {
        assert(cnt != 0);
        cnt --;
    }
};

struct shm_t {// TODO: split this
    copy_buffer_t copy_buffer;// accessed by only one thread
    op_buffer_t op_buffer;// accessed by only one thread
    shm_op_qp_t qp;
};

#define SHM_SIZE (sizeof(shm_t) + SHM_MISC_SIZE)

inline queue_pair_t<op_with_fut_t*, QUEUE_SIZE> py_qp;
inline shm_t *shm; // only for qp access
inline std::atomic<bool> daemon_quit;
inline cudaStream_t h2d_stream, d2h_stream;

#endif