#include "common.hpp"

// reduce: send or recv
// broadcast: send or recv (drop a packet at egress)
// allreduce: send, with recv in ACK
managed_shared_memory *segment;
vector<std::thread> daemon_thread;



// call stack of Tensor:
// torch/_tensor.py
// torch/_C/__init__.pyi.in
// aten/src/ATen/core/TensorBase.h

// call stack of all_reduce:
// torch/distributed/distributed_c10d.py
// torch/_C/_distributed_c10d.pyi
// torch/csrc/distributed/c10d/init.cpp
// torch/csrc/distributed/c10d/ProcessGroupWrapper.cpp
// torch/csrc/distributed/c10d/Backend.hpp (virtual functions)
// torch/csrc/distributed/c10d/ProcessGroupNCCL.hpp

// call stack of Future:
// torch/futures/__init__.py
// torch/_C/__init__.pyi.in
// torch/csrc/jit/python/init.cpp
// torch/csrc/jit/python/pybind_utils.h (PythonFutureWrapper) 
// aten/src/ATen/core/ivalue_inl.h (c10::ivalue::Future)
// aten/src/ATen/core/ivalue.cpp

// call stack of cuda.Stream
// torch/cuda/streams.py
// torch/_C/__init__.pyi.in (_CudaStreamBase)
// torch/csrc/cuda/Stream.cpp (THCPStream )
// struct THCPStream : THPStream {
//   at::cuda::CUDAStream cuda_stream; // at has "using namespace c10", which imported all c10 definitions
// };
// c10/cuda/CUDAStream.cpp
// All default pytorch streams have streamID==0 and associate with cudaStream_t==NULL, i.e., they are the same stream 
// Non-default pytorch streams are pre-allocated in a pool


// should be in daemon.h
void daemon_loop(int device);
void test_loop();
// void tx_loop();
// void rx_loop();


// extern std::chrono::time_point<std::chrono::high_resolution_clock> op_start, op_end;
// std::chrono::time_point<std::chrono::high_resolution_clock> op_test_time;

string shm_name;

void shm_init()
{
    shared_memory_object::remove(shm_name.data());
    segment = new managed_shared_memory(create_only, shm_name.data(), SHM_SIZE);// need hugepage?

    // void *buf = numa_alloc_onnode(SHM_SIZE, 0);
    // shm = new(buf) shm_t;
    std::cerr << sizeof(shm_t) << ' ' << alignof(shm_t) << std::endl;
    // construct() has BUG and will fail for 4096 alignment, use allocate_aligned instead
    shm = new(segment->allocate_aligned(sizeof(shm_t), alignof(shm_t))) shm_t();
    segment->construct<offset_ptr<decltype(shm->qp)>>("queue_pair")(&shm->qp);
    assert(shm != NULL);
    // shm = new shm_t();
    
    numa_set_strict(1);// let numa_interleave_memory fail when it cannot interleave memory
    numa_exit_on_error = 1;// let the program exit when numa_interleave_memory fails
    numa_interleave_memory(shm->copy_buffer.buffer, sizeof(shm->copy_buffer.buffer), numa_all_nodes_ptr);// make cudaMemcpyAsync and memcpy faster
    memset(shm->copy_buffer.buffer, 0xff, sizeof(shm->copy_buffer.buffer));// touch it, make sure the memory is allocated
    CUDA_CHECK(cudaHostRegister(shm->copy_buffer.buffer, sizeof(shm->copy_buffer.buffer), cudaHostRegisterDefault));// make cudaMemcpyAsync faster

    // py::print((uint64_t)shm, (uint64_t)segment->get_address(), segment->get_size());
    // py::print("copy_buffer", shm->copy_buffer.available());
    // py::print("op_buffer", shm->op_buffer.available());
    // managed_external_buffer copy_buffer(create_only, &shm->copy_buffer, sizeof(shm->copy_buffer));
    // managed_external_buffer op_buffer(create_only, &shm->op_buffer, sizeof(shm->op_buffer));
    // copy_buffer_allocator = new allocator<int32_t, managed_external_buffer::segment_manager>(copy_buffer.get_segment_manager());
    // op_allocator = new allocator<int32_t, managed_external_buffer::segment_manager>(copy_buffer.get_segment_manager());    
}

void shm_finalize()
{
    shm = NULL;
    delete segment;
    shared_memory_object::remove(shm_name.data());
}

void stream_init()
{
    // We need two streams to parallel copy of host2dev and dev2host.
    // We do not use the default stream (0) because of the default stream synchronization behavior,
    // and do not need any explicit synchronization between default and non-default streams because of that too.
    // See https://docs.nvidia.com/cuda/cuda-runtime-api/stream-sync-behavior.html#stream-sync-behavior
    cudaStreamCreate(&h2d_stream);
    cudaStreamCreate(&d2h_stream);
}

void stream_finalize()
{
    cudaStreamDestroy(h2d_stream);
    cudaStreamDestroy(d2h_stream);
}

void thread_init()
{
    int device;
    CUDA_CHECK(cudaGetDevice(&device));
    // TODO: the daemon thread can be removed, and functions of the daemon thread can be invoked by the python thread
    daemon_thread.push_back(std::thread(daemon_loop, device));
    // std::cerr << "daemon thread: " << daemon_thread.back().get_id() << '\n';// this is POSIX thread id, not linux tid
    // daemon_thread.push_back(std::thread(tx_loop));
    // daemon_thread.push_back(std::thread(rx_loop));
    if(getenv("NETCCL_FRONTEND_LOOPBACK") != NULL) daemon_thread.push_back(std::thread(test_loop));
    // std::cerr << "test thread: " << daemon_thread.back().get_id() << '\n';
}

void thread_finalize()
{
    daemon_quit.store(true);
    for(auto &thread: daemon_thread) thread.join();
}

void init(string _shm_name)
{
    shm_name = _shm_name;
    shm_init();
    stream_init();
    thread_init();
}

void finalize()
{
    thread_finalize();
    stream_finalize();
    shm_finalize();
}

shared_ptr<PythonFutureWrapper> create_fut_wrapper()
{
    intrusive_ptr<Future> fut;
    shared_ptr<PythonFutureWrapper> fut_wrapper; //NULL
    // refer to torch/csrc/jit/python/init.cpp
    fut = make_intrusive<Future>(c10::PyObjectType::get());
    // fut->markCompleted();
    fut_wrapper = make_shared<PythonFutureWrapper>(fut); 
    return fut_wrapper;
}

op_with_fut_t * create_op(op_code_t op_code, rank_t root, void *data, uint32_t size, group_t group, cudaStream_t stream, 
    bool is_first, bool is_last, bool generate_future)
{
    op_with_fut_t *op_with_fut;
    op_t *op;
    void *copy_buffer;
    shared_ptr<PythonFutureWrapper> fut_wrapper;
    cudaEvent_t event;

    op_with_fut = new op_with_fut_t;
    op = shm->op_buffer.fifo_allocate();//{op_code, size, data, group, fut};
    assert(op != NULL);
    copy_buffer = shm->copy_buffer.fifo_allocate(size);
    assert(copy_buffer != NULL);
    fut_wrapper = generate_future ? create_fut_wrapper() : NULL;
    CUDA_CHECK(cudaEventCreate(&event)); 

    op->op_code = op_code;
    op->root = root;
    op->size = size;
    op->shm_buffer = copy_buffer;
    op->group = group;
    op->op_with_fut = op_with_fut;

    op_with_fut->copying = false;
    op_with_fut->data = data;
    op_with_fut->event = event;
    op_with_fut->stream = stream;
    op_with_fut->is_first = is_first;
    op_with_fut->is_last = is_last;
    op_with_fut->fut_wrapper = fut_wrapper;
    op_with_fut->op = op;

    return op_with_fut;
}

void destroy_op(op_with_fut_t *op_with_fut)
{
    CUDA_CHECK(cudaEventDestroy(op_with_fut->event)); 
    op_with_fut->fut_wrapper = NULL;// free shared_ptr
    shm->copy_buffer.fifo_free(op_with_fut->op->size);
    shm->op_buffer.fifo_free();
    delete op_with_fut;
}

// Receive all existing responses in cq,
// poll until at least least_num responses all received
void recv_all_resp()
{
    op_with_fut_t *op_with_fut;
    while(py_qp.front_resp(op_with_fut)) {
        destroy_op(op_with_fut);
        py_qp.pop_resp();
    } 
}

// post to queue
shared_ptr<PythonFutureWrapper> post_sub_collective(op_code_t op_code, rank_t root, void *data, uint32_t size, group_t group, cudaStream_t stream, 
    bool is_first, bool is_last, bool generate_future)
{
    // reserve memory
    bool available;
    while(1) {
        recv_all_resp(); 
        available = shm->op_buffer.available() > 0 && shm->copy_buffer.available() >= size;
        if(available) break;
        netccl_yield();
    }

    // create op
    auto op_with_fut = create_op(op_code, root, data, size, group, stream, is_first, is_last, generate_future);
    bool ret = py_qp.push_req(op_with_fut);
    assert(ret == true);// because shm->op_buffer and all queue pairs are in the same size
    return op_with_fut->fut_wrapper;
}

static_assert(FRONTEND_SEGMENT_SIZE % sizeof(int32_t) == 0);
static_assert(FRONTEND_SEGMENT_SIZE < (1<<30));

// segmentation
shared_ptr<PythonFutureWrapper> post_collective_c(op_code_t op_code, rank_t root, void *data, uint64_t size, group_t group, cudaStream_t stream, bool async_op) 
{
    shared_ptr<PythonFutureWrapper> future;
    // NOTE: post at least one segment even if size == 0
    for(uint64_t offset = 0; ; ) {
        void *seg_data = (void*)((char*)data + offset);
        uint32_t seg_size = FRONTEND_SEGMENT_SIZE;
        bool is_first = offset == 0;
        bool is_last = false;
        bool generate_future = false;
        if(size - offset <= FRONTEND_SEGMENT_SIZE) {// the last segment
            seg_size = size - offset;
            generate_future = async_op;
            is_last = true;
        }
        future = post_sub_collective(op_code, root, seg_data, seg_size, group, stream, is_first, is_last, generate_future);
        offset += FRONTEND_SEGMENT_SIZE;
        if(!(offset < size)) break;
    }
    // do not return until all CUDA operations have been posted
    do {recv_all_resp();}
    while(shm->op_buffer.available() != QUEUE_SIZE);
    // at this point, all CUDA operations have been posted, but may not be completed
    return future;// future == NULL for sync op
}

// type convertion
// torch.Future is defined as py::class_<PythonFutureWrapper, std::shared_ptr<PythonFutureWrapper>>
shared_ptr<PythonFutureWrapper> post_collective(py::int_ op_code, py::int_ root, torch::Tensor tensor, const group_t &group, py::object stream, py::bool_ async_op)
{   
    // refer to torch/csrc/jit/python/init.cpp
    op_code_t op_code_c = op_code;
    rank_t root_c = root;
    void * data = tensor.data_ptr();
    uint64_t size = tensor.element_size() * tensor.numel();
    group_t group_c = group;
    cudaStream_t stream_c = ((THCPStream* )stream.ptr())->cuda_stream.stream();
    bool async_op_c = async_op;

    return post_collective_c(op_code_c, root_c, data, size, group_c, stream_c, async_op_c);
}

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) {
    // We do not use py::call_guard<py::gil_scoped_release>().
    // All these function only allow one thread to call.
    m.def("post_collective", &post_collective);
    m.def("init", &init);
    m.def("finalize", &finalize);
    py::class_<group_t>(m, "netccl_group")
        .def(py::init<>())
        .def_readwrite("group_id", &group_t::group_id)
        .def_readwrite("size", &group_t::size)
        .def_readwrite("rank", &group_t::rank)
        .def_readwrite("agg_addr", &group_t::agg_addr)
        .def_readwrite("agg_len", &group_t::agg_len)
        .def_readwrite("switch_ip", &group_t::switch_ip);
}

/* These APIs are for Huawei */

void all_reduce(void *data, uint64_t size, group_t group, cudaStream_t stream)
{
    post_collective_c(OP_ALL_REDUCE, 0, data, size, group, stream, false);
}

void reduce(void *data, rank_t root, uint64_t size, group_t group, cudaStream_t stream)
{
    post_collective_c(OP_REDUCE, root, data, size, group, stream, false);
}

void broadcast(void *data, rank_t root, uint64_t size, group_t group, cudaStream_t stream)
{
    post_collective_c(OP_BROADCAST, root, data, size, group, stream, false);
}

void reduce_scatter(void *data, uint64_t size, group_t group, cudaStream_t stream)
{
    post_collective_c(OP_REDUCE, 0, data, size, group, stream, false);
}

void all_gather(void *data, uint64_t size, group_t group, cudaStream_t stream)
{
    post_collective_c(OP_ALL_GATHER, 0, data, size, group, stream, false);
}