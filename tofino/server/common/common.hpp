#ifndef __NETCCL_COMMON_COMMON_HPP__
#define __NETCCL_COMMON_COMMON_HPP__

#include <iostream>
#include <cstdio>
#include <vector>
#include <climits>
#include <atomic>
#include <thread>
#include <chrono>
#include <map>
#include <assert.h>
#include <stdint.h>

#include <numa.h>
#include <pthread.h>
#include <boost/lockfree/spsc_queue.hpp>
#include <boost/interprocess/shared_memory_object.hpp> 
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/allocators/allocator.hpp>

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>

using std::shared_ptr;
using std::make_shared;
using std::max;
using std::min;
using std::vector;
using std::string;
using boost::interprocess::shared_memory_object;
using boost::interprocess::offset_ptr;
using boost::interprocess::managed_shared_memory;
using boost::interprocess::basic_managed_external_buffer;
using boost::interprocess::allocator;
using boost::interprocess::create_only;
using boost::interprocess::open_only;
using boost::interprocess::open_or_create;

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
#define CACHE_LINE_SIZE 64
#define ALIGN64 __attribute__((aligned(64))) // packed is not needed
#define ALIGN4096 __attribute__((aligned(4096)))
#define PACKED __attribute__((packed))
// #define basetype(x) std::remove_pointer<decltype(x)>::type
#define loadr(x) x.load(std::memory_order_relaxed)
#define storer(x, y) x.store(y, std::memory_order_relaxed)

#define nullinst(T) (*(T*)NULL)
#define offset_of_member(T, member) ((size_t)&nullinst(T).member)
#define size_of_member(T, member) sizeof(nullinst(T).member)
#define type_of_member(T, member) decltype(nullinst(T).member)

#define QUEUE_SIZE (1024)

// #define NETCCL_ENABLE_YIELD 

// template<typename T>
// using shm_allocator = allocator<T, managed_shared_memory::segment_manager>;

template<typename T, int SIZE, typename ... Options>
using queue = boost::lockfree::spsc_queue<T, boost::lockfree::capacity<SIZE>, boost::lockfree::fixed_sized<true>, Options...>;

struct op_with_fut_t;

enum OP_CODE : int32_t {
    OP_ALL_REDUCE = 1,
    OP_REDUCE,
    OP_BROADCAST,
    OP_REDUCE_SCATTER, // TODO: support native reduce_scatter and all_gather, instead of implementing by reduce and broadcast
    OP_ALL_GATHER
};

using group_id_t = uint8_t;
using rank_t = uint8_t;
using agg_size_t = uint16_t;
using op_code_t = uint8_t;

struct group_t {
    group_id_t group_id;
    rank_t size;
    rank_t rank;// this process's rank
    agg_size_t agg_addr;
    agg_size_t agg_len;
    uint32_t switch_ip;
};

struct op_t {
    op_code_t op_code;
    rank_t root;
    uint32_t size;
    offset_ptr<void> shm_buffer;// in shared memory
    group_t group;
    op_with_fut_t *op_with_fut;
} ALIGN64;

template<typename T, int SIZE>
struct queue_pair_t {
    // spsc_queue with capacity<> does not need allocator<> for shared memory
    queue<T, SIZE> ALIGN64 wq;// work queue
    queue<T, SIZE> ALIGN64 cq;// complete queue
    size_t ALIGN64 num_in_queue;// accessed by only one thread
    queue_pair_t() {
        num_in_queue = 0;
    }
    size_t available() {// requester only 
        return SIZE - num_in_queue;
    }
    bool push_req(const T &e) {// requester only 
        if(num_in_queue == SIZE) {
            return false;// limit the queue usage
        }
        num_in_queue ++;
        bool ret = wq.push(e);
        assert(ret == true);
        return true;
    }
    bool front_req(T &e) {// responser only
        if(wq.read_available() > 0) {
            e = wq.front();
            return true;
        }
        return false;
    }
    void pop_req() {// responser only
        assert(wq.read_available() > 0);
        wq.pop();
    }
    bool push_resp(const T &e) {// responser only
        bool ret = cq.push(e);
        assert(ret == true);
        return ret;
    }
    bool front_resp(T &e) {// requester only
        if(cq.read_available() > 0) {
            e = cq.front();
            return true;
        }
        return false;
    }
    void pop_resp() {// requester only
        assert(cq.read_available() > 0);
        cq.pop();
        num_in_queue --;
    }
} ALIGN64;

using shm_op_qp_t = queue_pair_t<offset_ptr<op_t>, QUEUE_SIZE>;

inline bool need_tx(op_t *op) {
    return op->op_code == OP_ALL_REDUCE || 
        (op->op_code == OP_REDUCE && op->root != op->group.rank) || 
        (op->op_code == OP_BROADCAST && op->root == op->group.rank);
}

inline bool need_rx(op_t *op) {
    return op->op_code == OP_ALL_REDUCE || 
        (op->op_code == OP_REDUCE && op->root == op->group.rank) || 
        (op->op_code == OP_BROADCAST && op->root != op->group.rank);
}

inline void netccl_yield()
{
#ifdef NETCCL_ENABLE_YIELD
    sched_yield();
#endif
}

static string get_env(string env) {
    char* env_chr = getenv(env.data());
    if(env_chr == NULL) return string();
    return string(env_chr);
}

static string get_env(string env, string default_val) {
    string ret = get_env(env);
    if(ret.empty()) ret = default_val;
    return ret;
}

static string get_env_required(string env) {
    string ret = get_env(env);
    if(ret.empty()) {
        spdlog::error("Env {} not found", env);
        exit(1);
    }
    return ret;
}

#endif