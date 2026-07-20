# EPIC VM Emulation

This repository contains a clean VM-based emulation of EPIC collective communication modes. It keeps only the implementation code needed to build the emulated host and switch data paths. Topology setup scripts, VM provisioning files, experiment logs, generated results, and paper artifacts are intentionally excluded.

The implementation targets a four-host, one-switch emulation environment. Hosts use SoftRoCE through `libibverbs`; the software switch uses `libpcap` for packet I/O in Mode II and Mode III.

## Contents

- `common/src/mode1_node.c`: Mode I implementation. The same binary can run either the host role or the switch role over RDMA.
- `common/src/primitive_host.c`: Host runtime for Mode II and Mode III.
- `common/src/star4_switch.c`: Four-port pcap switch for Mode II and Mode III.
- `common/src/roce_packet.c` and `common/include/epic_clean.h`: shared packet parsing, packet construction, protocol definitions, and utility code.
- `benchmarks/src/pcap_l2_switch.c`: minimal pcap L2 forwarding benchmark.
- `benchmarks/src/pure_pingpong.c`: RDMA ping-pong benchmark used to measure the host/SoftRoCE path.
- `mpi_calibration/src/mpi_collectives.c`: MPI collective benchmark source used for baseline calibration.
- `common/scripts/*.py`: offline result and trace analysis helpers.

## Supported Collectives

The clean implementation supports six collectives:

- `allreduce`
- `reduce`
- `broadcast`
- `allgather`
- `reducescatter`
- `barrier`

Mode II and Mode III are selected at runtime by `--mode 2` or `--mode 3` in `primitive_host`, and by the `mode` field in the switch configuration. Mode II implements non-termination with ACK reflection or aggregation. Mode III terminates ACKs hop by hop at the switch.

## Dependencies

Required build dependencies:

- CMake 3.16 or newer
- A C11 compiler
- `pkg-config`
- `libpcap`
- `libibverbs`

On Ubuntu, the basic packages are:

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake pkg-config libpcap-dev libibverbs-dev
```

To build and run the MPI benchmark source, install an MPI implementation separately, such as Open MPI or MPICH.

## Build

Build all CMake targets:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
```

The build produces:

- `build/mode1_node`
- `build/primitive_host`
- `build/star4_switch`
- `build/pcap_l2_switch`
- `build/pure_pingpong`

The helper script runs the same build flow:

```bash
./common/scripts/build_clean.sh
```

## Running the Emulation

This repository does not create VM topology or network devices. Before running the binaries, prepare a four-host, one-switch environment with SoftRoCE devices on the hosts and pcap-visible switch interfaces. Each experiment should run one primitive and one mode in a fresh environment if strict isolation is desired.

Mode I uses `mode1_node` for both endpoint roles:

```bash
./build/mode1_node --role switch --config switch.conf --endpoint-dir endpoints --start-file start.time
./build/mode1_node --role host --rank 0 --primitive allreduce --root 0 --count 1024 \
  --iters 100 --local-ip 10.0.0.1 --switch-ip 10.0.0.101 \
  --endpoint-file endpoints/host0.endpoint \
  --switch-endpoint-file endpoints/switch0.endpoint \
  --start-file start.time
```

Mode II and Mode III use `star4_switch` on the software switch and `primitive_host` on each host:

```bash
./build/star4_switch --config switch.conf

./build/primitive_host --rank 0 --primitive allreduce --mode 2 --root 0 \
  --count 1024 --iters 100 --switch-ip 10.0.0.101 --local-ip 10.0.0.1 \
  --switch-qpn 12345 --endpoint-file endpoints/host0.endpoint \
  --start-file start.time
```

The exact interface names, QPN exchange, start-time coordination, and switch configuration are environment-specific and are not included in this repository.

## MPI Baseline Source

The MPI benchmark source is kept under `mpi_calibration/src/mpi_collectives.c`. It is not built by CMake because MPI toolchains vary across systems. Build it with the MPI compiler wrapper available in your environment:

```bash
mpicc -O3 -Wall -Wextra -o mpi_collectives mpi_calibration/src/mpi_collectives.c
```

## Repository Scope

This repository is a code release for the VM emulation data path. It intentionally excludes:

- VM creation and teardown scripts
- topology configuration files
- SSH deployment scripts
- rate limiting and MTU setup scripts
- generated experiment results
- paper tables, figures, and rebuttal notes

Those files are environment-specific and were omitted to keep the repository focused on the implementation.
