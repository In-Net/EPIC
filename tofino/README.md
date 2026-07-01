# EPIC-tofino

## Prerequisite

EPIC-tofino includes three modules, the frontend, the backend and the switch module, their dependencies are listed repectively.

### Frontend

The EPIC-tofino frontend is an extension of PyTorch and requires the environment to have PyTorch, Boost, argparse, and spdlog installed.

We suggest to run frontend in container environment and use `nvcr.io/nvidia/pytorch` image to avoid complicated dependency installation. This image includes PyTorch, CUDA, CuDNN, NCCL and many other packages for NVIDIA GPUs. To install Boost in the container, run `apt install libboost-all-dev`. To install gRPC, run `pip3 install grpcio-tools`. Argparse and spdlog do not need installation as they have already been included in the repository.

### Backend

The EPIC-tofino backend run directly on the host instead of in the container. It requies DPDK, argparse and spdlog to be installed. To install DPDK, follow the guild https://doc.dpdk.org/guides/linux_gsg/build_dpdk.html#compiling-and-installing-dpdk-system-wide.

### Switch

The EPIC-tofino switch requires SDE environment of Intel Tofino, which is distributed with the sale of the switch hardware.

### Hardware

EPIC-tofino relies on RDMA to transfers some control messages. Users must run NCCL with RDMA backend, not TCP.

### Version specification

We only show tested version, other versions may also work.

|Software|Version|
|---|---|
|nvcr.io/nvidia/pytorch|23.10-py3|
|Python|3.10|
|PyTorch|2.1.0a0+32f93b1|
|NCCL|2.19.3|
|GPU driver|535.104.05|
|host OS|Ubuntu 22.04 5.4.0-26-generic|
|Boost|1.74|
|argparse||
|spdlog||
|DPDK|22.11|
|SDE|9.7|

## How to import EPIC-tofino 

1. Prepare a program written with torch.distributed

Make sure that the program only use the GPU with `DEVICE_ID == LOCAL_RANK`, and has set the default CUDA device so tensor.cuda() moves the tensor to that device

2. Import the package at the beginning of the program

## How to run EPIC-tofino

### 0. Create a config file for the controller

Edit `switch/topo.json` which specifies the topology of the network. Below is the explaination of the fields:

```
port: A list of the port numbers of the switch for each directly linked servers.
MAC: A list of the MAC addresses of each directly linked servers' NIC. Should be consistent with the order of the port numbers.
IP: A list of the IP addresses of each directly linked servers' NIC. Should be consistent with the order of the port numbers.
switch_MAC: The virtual MAC address of the switch, suggested to be 02:00:00:00:00:00.
switch_IP: The virtual IP address of the switch. Should be in the same subnet as the servers.
recir_port: A list of the port numbers for the switch to recirculate packets. Should be different from the port numbers of the servers. 
            For the best performance, the number of these ports should be larger than the number of concurrent running collective groups (communicators).
```

### 1. Start the switch program and the controller

```
cd ./switch
$SDE/run_switchd.sh -p netccl > /tmp/switchd.log 2>&1 &
$SDE/run_bfshell.sh -b `pwd`/bfrt_init.py
$SDE/run_bfshell.sh -b `pwd`/bfrt_grpc.py
```

### 2. Start the user program (frontend) in the container

```
EPIC_BIND_ADDR=<IP> EPIC_CONTROLLER_ADDR=<IP:port> <program> <args>
```

### 3. Start the backend program on the host 

```
sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH EPIC_BIND_ADDR=<IP> ./build/main <NTHREADS>
```
