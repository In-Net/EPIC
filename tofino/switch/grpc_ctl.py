#!/usr/bin/env python3.8
# assert python3.8 is used by SDE, possibly installed at /root/bf-sde-9.7.0/install/bin/python3.8
import os, sys
import grpc 
import netccl_pb2, netccl_pb2_grpc

if len(sys.argv) != 2 or not sys.argv[1] in ["stop", "init"]:
    print(f"usage: {sys.argv[0]} {{stop|init}}")
    sys.exit(1)

controller_addr = "localhost:50051"
channel = grpc.insecure_channel(controller_addr)
stub = netccl_pb2_grpc.INCStub(channel)
if sys.argv[1] == "stop":
    stub.Stop(netccl_pb2.StopRequest())
elif sys.argv[1] == "init":
    stub.Init(netccl_pb2.InitRequest())