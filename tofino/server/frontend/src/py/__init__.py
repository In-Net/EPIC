import torch
import torch.distributed as dist
import netccl._C
import warnings
import atexit
import signal
import grpc
import socket
import struct
import pickle
import os, sys
import inspect
import time

# required by init_process_group

from datetime import timedelta
from typing import Any, Callable, Dict, Optional, Tuple, Union, List
from torch.distributed import Backend, Store, default_pg_timeout

# grpc

# this does not work well because netccl_pb2_grpc imports netccl_pb2, which cannot be found in global view
# from . import netccl_pb2
# from . import netccl_pb2_grpc

grpc_file_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, grpc_file_path)
import netccl_pb2, netccl_pb2_grpc
sys.path.remove(grpc_file_path)


controller_addr = os.environ["EPIC_CONTROLLER_ADDR"]
print(f"connect to {controller_addr}")
channel = grpc.insecure_channel(controller_addr)
stub = netccl_pb2_grpc.INCStub(channel)

# constants

INC_SIZE_LIMIT = 32 * 1024
INC_SHARED = False

PKT_PAYLOAD_SIZE = 256
AGG_LEN_PER_GROUP = 1024

OP_ALL_REDUCE = 1
OP_REDUCE = 2 
OP_BROADCAST = 3
OP_REDUCE_SCATTER = 4 # TODO: support native reduce_scatter and all_gather, instead of implementing by reduce and broadcast
OP_ALL_GATHER = 5

SEGMENT_SIZE = 256 * 1024

# global variables

group_ctx_dict = {}
non_fallback_cnt = 0 
barrier_tensor = None
# buffer_size = 256 * 1024 * 1024
# buffer_len = buffer_size // 4
# float_buffer_tensor = None
# int_buffer_tensor = None

# class

class WorkNetCCL(dist.Work):
    def __init__(self):
        super().__init__()
        self._fut = None

    def __init__(self, fut):
        super().__init__()
        self._fut = fut

    def is_completed(self):
        return self._fut.done()

    def wait(self):
        self._fut.wait()
        return True

    def get_future(self):
        return self._fut

# Store fallback functions 

export_func = [
    "init_process_group", 
    "new_group", 
    "all_reduce", 
    "all_gather_into_tensor", 
    "reduce_scatter_tensor", 
    "broadcast", 
    "reduce", 
    "gather", 
    "scatter", 
    "all_to_all_single", 
    "barrier",
    # Old API
    "_all_gather_base",
    "_reduce_scatter_base"
    ]
for func in export_func:
    exec(f"fallback_{func} = dist.{func}")


# NetCCL functions
group_id_list = []

# I don't understand, but thus func causes memory leak
def print_caller_info():
    stack = inspect.stack()
    caller_frame = stack[2] # this func -> caller func -> caller's caller
    filename = caller_frame.filename  
    lineno = caller_frame.lineno      
    function = caller_frame.function  
    print(f"Called from file: {filename}, line: {lineno}, function: {function}")

@atexit.register
def exit_handler():
    for group_id in group_id_list:
        stub.DestroyGroup(netccl_pb2.DestroyGroupRequest(GroupID=group_id))
    netccl._C.finalize()
    print(f"Number of NetCCL non-fallback calls: {non_fallback_cnt}")

def int_to_ip(ip_int):
    return socket.inet_ntoa(struct.pack('!I', ip_int))

def remote_create_group(local_ip_list): # -> group_id, agg_addr, agg_len, remote_ip_list
    req = netccl_pb2.CreateGroupRequest()
    for ip in local_ip_list:
        req.Member.append(netccl_pb2.MemberInfo(IP=ip))
    resp = stub.CreateGroup(req)
    group_id = resp.GroupID
    agg_addr = resp.AggAddr
    agg_len = resp.AggLen 
    remote_ip_list = [member.IP for member in resp.Member]
    group_id_list.append(group_id)
    print(f"create group response: group_id={group_id}, agg_addr={agg_addr}, agg_len={agg_len}, \
remote_ip_list={[f'{int_to_ip(ip)}' for ip in remote_ip_list]}")
    return (group_id, agg_addr, agg_len, remote_ip_list)


def alloc_group_ctx(group):
    # global group_ctx_dict
    group_ctx = netccl._C.netccl_group()
    group_ctx_dict[group] = group_ctx
    
    rank = dist.get_rank(group)
    group_size = dist.get_world_size(group)
    local_ip = os.environ["EPIC_BIND_ADDR"]
    local_ip_int = int.from_bytes(socket.inet_aton(local_ip), byteorder='big')
    local_ip_list = None if rank!=0 else [None for _ in range(group_size)]

    root_global_rank = dist.get_global_rank(group, 0)
    dist.gather_object(local_ip_int, local_ip_list, dst=root_global_rank, group=group) # gather_object runs on CPU
    if rank == 0:
        group_id, agg_addr, agg_len, remote_ip_list = remote_create_group(local_ip_list)
        if group_id == 0: # failed
            remote_ip_list = [None for _ in range(group_size)]
    
    scatter_input_list = [(None if rank!=0 else [group_id, agg_addr, agg_len, remote_ip_list[i]]) for i in range(group_size)]
    scatter_output_list = [None]
    dist.scatter_object_list(scatter_output_list, scatter_input_list, src=root_global_rank, group=group)
    group_id, agg_addr, agg_len, remote_ip = scatter_output_list[0]

    if group_id == 0:
        print("No INC resource")
        del group_ctx
        del group_ctx_dict[group]
        return

    group_ctx.group_id = group_id
    group_ctx.size = group_size
    group_ctx.rank = rank
    group_ctx.agg_addr = agg_addr
    group_ctx.agg_len = agg_len # 1MB
    group_ctx.switch_ip = remote_ip 

def is_local_group(group): # TODO: test if all ranks of a group are on the same machine
    return dist.get_world_size(group) == 1 # strawman solution
    # local_ranks = []
    # group_ranks = dist.get_group_ranks(group)

def try_alloc_group_ctx(group):
    if group == dist.GroupMember.NON_GROUP_MEMBER or \
        dist.get_backend(group) != "nccl" or \
        is_local_group(group): # or mechine_count(group) <= 2
        return
    alloc_group_ctx(group)

def get_group_ctx(group):
    return group_ctx_dict.get(group)

def init_process_group(
    backend: Union[str, Backend] = None,
    init_method: Optional[str] = None,
    timeout: timedelta = default_pg_timeout,
    world_size: int = -1,
    rank: int = -1,
    store: Optional[Store] = None,
    group_name: str = "",
    pg_options: Optional[Any] = None,
    enable_inc = True,
):
    fallback_init_process_group(backend, init_method, timeout, world_size, rank, store, group_name, pg_options) # return None
    if enable_inc: # Megatron also uses gloo for some transmission
        # netccl._C.init(f"netccl_{os.environ["MASTER_ADDR"]}:{os.environ["MASTER_PORT"]}_{dist.get_rank()}")
        try_alloc_group_ctx(dist.GroupMember.WORLD)

    global barrier_tensor
    # global float_buffer_tensor, int_buffer_tensor
    netccl._C.init("netccl")
    barrier_tensor = torch.zeros(0, dtype=torch.float32).cuda() # empty tensor
    # float_buffer_tensor = torch.zeros(buffer_len, dtype=torch.float32).cuda()
    # int_buffer_tensor = torch.zeros(buffer_len, dtype=torch.int32).cuda()
    print("init_process_group: group init finished")

def new_group(ranks=None, timeout = default_pg_timeout, backend=None, pg_options=None, use_local_synchronization=False, enable_inc=True):
    group = fallback_new_group(ranks, timeout, backend, pg_options, use_local_synchronization)
    # group is NON_GROUP_MEMBER if rank is not in ranks
    if enable_inc: # Megatron also uses gloo for some transmission
        try_alloc_group_ctx(group)
    return group 

def check_inc_requirement(reduce_like, tensor, op, group):
    if get_group_ctx(group) == None:
        return False
    if reduce_like and (op != dist.ReduceOp.SUM or tensor.dtype != torch.float32):
        return False
    # reduce/allreduce needs an additional barrier/all_reduce which degrades performance of small tensors
    # if tensor.numel() * tensor.element_size() < INC_SIZE_LIMIT:
    #     return False
    return True

def create_completed_work():
    fut = torch.futures.Future()
    fut.set_result(None) 
    work = WorkNetCCL(fut)
    return work

# TODO: Current post_collective() is half-async, it requires CPU to check GPU memory copy, and uses the main thread.
# To support full-async op, we need to create (1) a new stream for GPU computing (2) a new thread for post_collective()

def call_c_collective(op_code, root_global_rank, tensor, group, group_ctx, async_op) -> dist.Work: 
    try:
        # print(f"call_c_collective {op_code}")
        # PyTorch uses global rank but NetCCL uses group rank
        root_group_rank = dist.get_group_rank(group, root_global_rank)
        # returns None if async_op == false
        fut = netccl._C.post_collective(op_code, root_group_rank, tensor, group_ctx, torch.cuda.current_stream(), async_op) 
        # fut = torch.futures.Future()
        # fut.set_result(None)
    except Exception as e:
        print(e)

    if async_op:
        work = WorkNetCCL()
        work._fut = fut
        return work
    return None

# Do quantize on gpu is as fast as cudaMemcpy
# Since tensor.type() has no in-place version, we use a buffer instead.
# def quantize(tensor):
#     global quantize_buffer
#     tensor_len = tensor.numel()
#     if quantize_buffer == None or quantize_buffer.numel() < tensor_len:
#         # quantize_buffer = torch.zeros(tensor_len, dtype=torch.int32).cuda()
#         quantize_buffer = torch.zeros(1024*1024*1024//4, dtype=torch.int32).cuda()
#     quantize_buffer_slice = quantize_buffer[:tensor_len]
#     quantize_buffer_slice.copy_(tensor)
#     return quantize_buffer_slice
    

# def wait_and_start_fut_in_stream(fut_prev, fut_next, stream):
#     torch.cuda.set_stream(stream)
#     fut_prev.wait() # The order is important. It must be placed here.
#     fut_next.set_result(None)

# def link_to(fut_next):
#     def link_from(fut_prev):
#         fut_prev = fut_prev.value().get_future() 
#         # Non-blocking, fut_prev.value() returns Work
#         # This changes stream
#         current_stream = torch.cuda.current_stream()
#         fut_prev.then(lambda _: wait_and_start_fut_in_stream(fut_prev, fut_next, current_stream))
#     return link_from

def fallback_collective(op_code, root_global_rank, tensor, op, group, async_op):
    if op_code == OP_ALL_REDUCE:
        return fallback_all_reduce(tensor, op, group, async_op)
    elif op_code == OP_REDUCE:
        return fallback_reduce(tensor, root_global_rank, op, group, async_op)
    elif op_code == OP_BROADCAST:
        return fallback_broadcast(tensor, root_global_rank, group, async_op)
    else:
        raise NotImplementedError(f"op_code {op_code} not supported for fallback_collective()")

# event_s = None
# event_e = None

def call_collective(op_code, root_global_rank, tensor, op, group, async_op, enable_inc) -> dist.Work: # 240us vs. 340us (async allreduce)
    # with no grad?

    # global event_s, event_e
    # if event_e != None:
    #     event_e.synchronize()
    #     print(f"time {event_s.elapsed_time(event_e)}")
    #     event_e = None
    # event_s = torch.cuda.Event(enable_timing=True)
    # event_s.record(torch.cuda.current_stream())

    # outside call costs 20us
    # TODO: remove this
    assert str(tensor.device).startswith("cuda") 

    if group == dist.GroupMember.NON_GROUP_MEMBER:
        print("Process is not in group")
        sys.exit(1)

    if group == None:
        group = dist.GroupMember.WORLD

    reduce_like = True if op_code in [OP_ALL_REDUCE, OP_REDUCE, OP_REDUCE_SCATTER] else False
    
    # print(f"op {op_code} root_global_rank {root_global_rank} dev {tensor.device} addr {tensor.data_ptr()} dtype {tensor.element_size()} len {tensor.numel()} async {async_op} stream {torch.cuda.current_stream().stream_id}")

    if not enable_inc or not check_inc_requirement(reduce_like, tensor, op, group):
        return fallback_collective(op_code, root_global_rank, tensor, op, group, async_op)
    
    group_ctx = get_group_ctx(group)

    if not reduce_like: # for broadcast and all_gather
        # return fallback_collective(op_code, root_global_rank, tensor, op, group, async_op)
        call_c_collective(op_code, root_global_rank, tensor, group, group_ctx, False)  
        # if tensor.isnan().any():
        #     print("NAN detected after broadcast")

        return create_completed_work() if async_op else None
        
    # below: for all_reduce, reduce, reduce_scatter

    max_abs = None # torch.tensor([1.0], dtype=torch.float32).cuda()
    max_abs_value = None
    factor = None
    tensor_int32 = None

    # all above costs 30us

        
    # nan_count = torch.isnan(tensor).sum()
    # total_count = tensor.numel()
    # if nan_count > 0:
    #     print(f"nan_count: {nan_count}, total_count: {total_count}")

    # abs().max() is faster than aminmax() or buffered torch.abs(out=...)

    # numel = tensor.numel()
    # tensor_view = tensor.view(numel)
    # float_buffer = float_buffer_tensor[:numel]
    # int_buffer = int_buffer_tensor[:numel]
    # torch.abs(tensor_view, out=float_buffer)
    # max_abs = float_buffer.max()

    max_abs = tensor.abs().max()
    fallback_all_reduce(max_abs, dist.ReduceOp.SUM, group) 


    # 0x7fffff80 == 2**31 - 2**7, which is the max value of FP32 in range of int32
    # Using item() increases performance, because it makes factor on CPU

    max_abs_value = max_abs.item() # on CPU
    if max_abs_value != 0:
        factor = 0x7fffff80 / max_abs_value # factor is 64-bit in python
        if factor == 0:
            print("factor should not be zero")
            sys.exit(1)

    # max_abs_value is NAN or FP32(factor) is INF
    if max_abs_value == 0: 
        # do nothing
        return create_completed_work() if async_op else None
    elif max_abs_value != max_abs_value or factor > torch.finfo(torch.float32).max: 
        if max_abs_value != max_abs_value:
            print("NAN detected")
        return fallback_collective(op_code, root_global_rank, tensor, op, group, async_op)
    else:
        # return fallback_collective(op_code, root_global_rank, tensor, op, group, async_op)

        # tensor_view.mul_(factor)
        # tensor_int32 = int_buffer
        # tensor_int32.copy_(tensor_view)
        
        tensor.mul_(factor)
        tensor_int32 = tensor.type(torch.int32)
        # tensor_int32 = quantize(tensor) 
        # print(0, type(tensor_int32))
        # In fact, on the gpu, the overflowed value will be reduced to INT_MAX; while on cpu, it may be negative
        # tensor.untyped_storage().byteswap(torch.int32) # only can be called on cpu
        # return fallback_all_reduce(tensor_int32, op, group, async_op)  
        # return fallback_all_reduce(tensor, op, group, async_op)  
        
        call_c_collective(op_code, root_global_rank, tensor_int32, group, group_ctx, False)

        # tensor.untyped_storage().byteswap(torch.int32)
        # print(1, type(tensor_int32))
        need_rx = op_code == OP_ALL_REDUCE or (op_code == OP_REDUCE and group_ctx.rank == root_global_rank)
        if need_rx:
            if op_code == OP_ALL_REDUCE:
                tensor.copy_(tensor_int32)
            else:
                tensor.add_(tensor_int32) # aggregate the data of root_global_rank for reduce
        tensor.div_(factor)
        # nan_count = torch.isnan(tensor).sum()
        # if nan_count > 0:
        #     print(f"nan_count: {nan_count}")

        global non_fallback_cnt
        non_fallback_cnt += 1

        # event_e = torch.cuda.Event(enable_timing=True)
        # event_e.record(torch.cuda.current_stream())

        return create_completed_work() if async_op else None


def all_reduce(tensor, op=dist.ReduceOp.SUM, group=None, async_op=False, enable_inc=True):
    # print("all_reduce")
    # print_caller_info()
    root = 0 if group == None else dist.get_global_rank(group, 0) # useless, only to keep the format the same
    return call_collective(OP_ALL_REDUCE, root, tensor, op, group, async_op, enable_inc)
    
def reduce(tensor, dst, op=dist.ReduceOp.SUM, group=None, async_op=False, enable_inc=True):
    # print("reduce")
    # print_caller_info()
    return call_collective(OP_REDUCE, dst, tensor, op, group, async_op, enable_inc)
    
def broadcast(tensor, src, group=None, async_op=False, enable_inc=True):
    # print("broadcast")
    # print_caller_info()
    return call_collective(OP_BROADCAST, src, tensor, None, group, async_op, enable_inc)

# TODO: implement native all_gather and reduce_scatter in C
def reduce_scatter_tensor(output, input, op=dist.ReduceOp.SUM, group=None, async_op=False, enable_inc=True):
    # print("reduce_scatter_tensor")
    # print_caller_info()
    if group == None:
        group = dist.GroupMember.WORLD
    if not enable_inc or not check_inc_requirement(True, input, op, group):
        return fallback_reduce_scatter_tensor(output, input, op, group, async_op)

    output_len = output.numel()
    input_len = input.numel()
    output_view = output.view(output_len)
    input_view = input.view(input_len) # the input tensor may have an additional dimension or not
    seg_len = output_len
    rank = dist.get_rank(group)
    tensor_list = [input_view[seg_len * r: seg_len * (r + 1)] for r in range(dist.get_world_size(group))]

    output_view.copy_(tensor_list[rank])
    tensor_list[rank] = output_view
    fut = None

    for r, tensor in enumerate(tensor_list):
        root_global_rank = dist.get_global_rank(group, r)
        fut = call_collective(OP_REDUCE, root_global_rank, tensor, op, group, async_op, enable_inc)
    return fut

def all_gather_into_tensor(output_tensor, input_tensor, group=None, async_op=False, enable_inc=True):
    # print("all_gather_into_tensor")
    # print_caller_info()
    input = input_tensor
    output = output_tensor

    if group == None:
        group = dist.GroupMember.WORLD
    if not enable_inc or not check_inc_requirement(False, input, None, group):
        return fallback_all_gather_into_tensor(output, input, group, async_op)

    output_len = output.numel()
    input_len = input.numel()
    output_view = output.view(output_len)
    input_view = input.view(input_len) # the input tensor may have an additional dimension or not
    seg_len = input_len
    rank = dist.get_rank(group)
    tensor_list = [output_view[seg_len * r: seg_len * (r + 1)] for r in range(dist.get_world_size(group))]

    tensor_list[rank].copy_(input_view)
    fut = None

    for r, tensor in enumerate(tensor_list):
        root_global_rank = dist.get_global_rank(group, r)
        fut = call_collective(OP_BROADCAST, root_global_rank, tensor, None, group, async_op, enable_inc)
    return fut

# def reduce(tensor, dst, op=dist.ReduceOp.SUM, group=None, async_op=False):
#     # print("netccl reduce")
#     return fallback_reduce(tensor, dst, op, group, async_op)

# def broadcast(tensor, src, group=None, async_op=False):
#     # print("netccl broadcast")
#     return fallback_broadcast(tensor, src, group, async_op)

# def reduce_scatter_tensor(output, input, op=dist.ReduceOp.SUM, group=None, async_op=False):
#     # print("netccl reduce_scatter_tensor")
#     return fallback_reduce_scatter_tensor(output, input, op, group, async_op)

# def all_gather_into_tensor(output_tensor, input_tensor, group=None, async_op=False):
#     # print("netccl all_gather_into_tensor")
#     return fallback_all_gather_into_tensor(output_tensor, input_tensor, group, async_op)

# TODO: this version lack the support for device_ids and may cause BUGs
def barrier(group=None, async_op=False, device_ids=None, enable_inc=True):
    # print("barrier")
    # print_caller_info()
    fallback_barrier(group, async_op, device_ids)
    # group_ctx = get_group_ctx(group)
    # if (device_ids == None or device_ids == torch.cuda.current_device()) and group_ctx != None:
    #     netccl._C.post_collective(OP_ALL_REDUCE, 0, barrier_tensor, group_ctx, torch.cuda.current_stream(), False) 
    #     return create_completed_work() if async_op else None
    # else:
    #     return fallback_barrier(group, async_op, device_ids)

gather = fallback_gather
scatter = fallback_scatter
all_to_all_single = fallback_all_to_all_single

_all_gather_base = all_gather_into_tensor
_reduce_scatter_base = reduce_scatter_tensor


# Overwrite native functions of torch.distributed

for func in export_func:
    exec(f"dist.{func} = {func}")


print("NetCCL imported")