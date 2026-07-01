def NameSpace(): # run all the code in a function, because this file is executed through exec() which will cause some namespace problems
    import json 
    import sys
    import os
    from concurrent import futures
    import logging
    import warnings
    import signal
    import threading

    import grpc # BUG: only the first "import grpc" succeed，others will fail

    import netccl_pb2, netccl_pb2_grpc

    config_dir = "."
    with open(config_dir + "/topo.json") as f: # symbolic link
        topo = json.load(f)

    # exec() can only access local/global/builtin variables, enclosing ones are not included
    # Thus, we declare bfrt_program as a global variable
    global bfrt_program
    program_name = bfrt.p4_programs_list[0]
    bfrt_program = eval('bfrt.' + program_name)

    port = topo["port"]
    MAC = topo["MAC"]
    IP = topo["IP"]
    switch_MAC = topo["switch_MAC"]
    switch_IP = topo["switch_IP"] # must be consistant with env NETCCL_SWITCH_ADDR
    recir_port = topo["recir_port"] # can add more

    ADDR_BW = 14 # 16K
    MTU_BW = 8 # 256
    MTU_SIZE = 1<<MTU_BW # 256
    SWITCH_MEM_SIZE = 1<<(ADDR_BW+MTU_BW) # 4MB

    MAX_GROUP_NUM = 256 
    MAX_SERVER_NUM = 256
    MAX_WORKER_NUM = 1024

    # 1024 packets == 512 server windows size
    # NOTE: this is the minimal size to saturate 100Gbps ethernet in my environment
    ALLOC_SIZE = 256 * 1024 

    IP_int = [int(ip,0) for ip in IP] # ip is a string

    def ip2port(ip):
        try:
            idx = IP_int.index(ip)
            return port[idx]
        except:
            return "511" # 0x1ff
        
    def ispower2(x):
        return x > 0 and (x & (x-1)) == 0

    def upperalign(x, y):
        return (x + y - 1)//y*y

    def addr_in_seg(seg, size): 
        # assert size % MTU == 2**k
        addr = upperalign(seg[0], size)
        if addr + size <= seg[1]: 
            return addr
        return None

    class INCServicerImpl(netccl_pb2_grpc.INCServicer):
        def reinit(self):
            self.free_group = [i for i in range(MAX_GROUP_NUM) if i > 0]
            self.free_node = [i for i in range(MAX_WORKER_NUM) if i > 0]
            self.recir_port_cnt = [0 for _ in range(len(recir_port))]
            self.undo = {}
            self.memseg = [[0, SWITCH_MEM_SIZE]] # [[ptr_l, ptr_r], ...]
            bfrt_program.pipe.Ingress.Forward.recirculate_table.clear()
            bfrt_program.pipe.Ingress.metadata_table.clear()
            bfrt_program.pipe.Egress.restore_table.clear()
            for mgid in self.free_group:
                try:
                    bfrt.pre.mgid.delete(mgid)
                except:
                    pass
            for node in self.free_node:
                try:
                    bfrt.pre.node.delete(node)
                except:
                    pass

        def __init__(self, server):
            super().__init__()
            self.server = server
            self.reinit()

        def debug_exec(self, str):
            print(str)
            return exec(str)

        def select_recir_port_idx(self):
            return self.recir_port_cnt.index(min(self.recir_port_cnt))

        def fit_seg(self, size):
            if size % MTU_SIZE != 0 or not ispower2(size//MTU_SIZE):
                print("Invalid size")
                return 
            for index, seg in enumerate(self.memseg):
                addr = addr_in_seg(seg, size)
                if addr != None:
                    return (index, addr)
            return (None, None)

        def alloc_seg(self, index, addr_l, addr_r):
            seg = self.memseg[index]
            lseg = None
            rseg = None
            if seg[0] != addr_l:
                lseg = [seg[0], addr_l]
            if seg[1] != addr_r:
                rseg = [addr_r, seg[1]]
            del self.memseg[index]
            if rseg:
                self.memseg.insert(index, rseg)
            if lseg:
                self.memseg.insert(index, lseg)
            
        def free_seg(self, addr_l, addr_r):
            r_index = len(self.memseg)
            for index, seg in enumerate(self.memseg):
                if seg[1] <= addr_l:
                    continue
                if seg[0] < addr_r:
                    print("Invalid free")
                    exit(1)
                r_index = index
                break
            l_index = r_index - 1
            index = r_index
            if l_index >= 0 and self.memseg[l_index][1] == addr_l:
                addr_l = self.memseg[l_index][0]
                index = l_index
                del self.memseg[l_index]
            if r_index < len(self.memseg) and self.memseg[r_index][0] == addr_r:
                addr_r = self.memseg[r_index][1]
                del self.memseg[r_index]
            self.memseg.insert(index, [addr_l, addr_r])

        def CreateGroup(self, request, context):
            group_size = len(request.Member)
            # use constant value
            seg_size = ALLOC_SIZE
            # seg_size = request.MemorySize
            seg_index, seg_addr = self.fit_seg(seg_size)
            if len(self.free_group) < 1 or len(self.free_node) < group_size or seg_index == None:
                print("No resource")
                return netccl_pb2.CreateGroupResponse() # GroupID == 0 (NULL group)

            agg_addr = seg_addr // MTU_SIZE
            agg_addr_len = seg_size // MTU_SIZE

            undo_funcs = []

            self.debug_exec(f"self.alloc_seg({seg_index}, addr_l={seg_addr}, addr_r={seg_addr + seg_size})")
            undo_funcs.append(f"self.free_seg(addr_l={seg_addr}, addr_r={seg_addr + seg_size})")

            groupid = self.free_group[0]
            self.debug_exec(f"self.free_group = self.free_group[1:]")
            undo_funcs.append(f"self.free_group.append({groupid})")
            

            node = self.free_node[0:group_size]
            self.debug_exec(f"self.free_node = self.free_node[{group_size}:]")
            undo_funcs.append(f"self.free_node += {node}")
            

            recir_port_idx = self.select_recir_port_idx()
            self.debug_exec(f"self.recir_port_cnt[{recir_port_idx}] += 1")
            undo_funcs.append(f"self.recir_port_cnt[{recir_port_idx}] -= 1")


            self.debug_exec(f"bfrt_program.pipe.Ingress.Forward.recirculate_table.add_with_forward({groupid}, {recir_port[recir_port_idx]})")
            undo_funcs.append(f"bfrt_program.pipe.Ingress.Forward.recirculate_table.delete({groupid})")

            self.debug_exec(f"bfrt_program.pipe.Ingress.metadata_table.add_with_get_allreduce_metadata\
    (group_id={groupid}, bitmap_mask={(1<<group_size)-1:#x})")
            undo_funcs.append(f"bfrt_program.pipe.Ingress.metadata_table.delete\
    (group_id={groupid})")

            for i in range(group_size):
                self.debug_exec(f"bfrt_program.pipe.Egress.restore_table.add_with_restore_fields\
    (group_id={groupid}, rank={i}, sip={switch_IP}, dip={request.Member[i].IP:#x})")
                undo_funcs.append(f"bfrt_program.pipe.Egress.restore_table.delete\
    (group_id={groupid}, rank={i})")

                self.debug_exec(f"bfrt.pre.node.add({node[i]}, {i}, None, [{ip2port(request.Member[i].IP)}])")
                undo_funcs.append(f"bfrt.pre.node.delete({node[i]})")

            self.debug_exec(f"bfrt.pre.mgid.add({groupid}, {node}, {[False]*group_size}, {[0]*group_size})")
            undo_funcs.append(f"bfrt.pre.mgid.delete({groupid})")

            self.debug_exec(f"_ = [bfrt_program.pipe.Ingress.reg_bitmap.mod(i, 0, 0) for i in range({agg_addr}, {agg_addr + agg_addr_len})]")

            self.undo[groupid] = undo_funcs

            ret = netccl_pb2.CreateGroupResponse()
            for member in request.Member:
                # int(X, base=0) : determine base by the prefix 
                ret.Member.append(netccl_pb2.MemberInfo(IP=int(switch_IP, 0))) # VQP uses the same QPN of QP
            ret.GroupID = groupid
            ret.AggAddr = agg_addr
            ret.AggLen = agg_addr_len
            return ret
        
        def DestroyGroup(self, request, context):
            groupid = request.GroupID
            if not groupid in self.undo:
                return netccl_pb2.DestroyGroupResponse()
            print(f"Destroy group {groupid}")
            for func in self.undo[groupid][::-1]: # reverse
                self.debug_exec(func)
            del self.undo[groupid]
            return netccl_pb2.DestroyGroupResponse()

        def Stop(self, request, context):
            self.server.stop(3)
            print("Stopped")
            return netccl_pb2.StopResponse()

        def Init(self, request, context):
            self.reinit()
            print("Reinit completed")
            return netccl_pb2.InitResponse()

    def serve(): # should import grpc here
        port = "50051"
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        netccl_pb2_grpc.add_INCServicer_to_server(INCServicerImpl(server), server) # pass server into INCServicerImpl
        server.add_insecure_port("[::]:" + port)
        server.start()
        print("Server started, listening on " + port)
        server.wait_for_termination()

    logging.basicConfig()
    serve()

NameSpace()
