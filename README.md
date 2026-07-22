# EPIC: Abstraction and Polymorphism of In-Network Collectives on Ethernet

This repository contains the open-source artifacts for our ACM SIGCOMM 2026 paper, **“EPIC: Abstraction and Polymorphism of In-Network Collectives on Ethernet.”**

EPIC (Ethernet Polymorphic In-network Collectives) is an in-network collective communication protocol and reference system for the open Ethernet ecosystem. Following the principle of **unified abstraction, polymorphic realization**, EPIC defines a common abstraction and interoperable interfaces while providing three data-plane modes for devices with different hardware capabilities. It supports six collective primitives: AllReduce, Reduce, Broadcast, AllGather, ReduceScatter, and Barrier.

## Repository Structure

| Directory | Description |
| --- | --- |
| [`verify/`](verify/README.md) | TLA+/TLC formal verification of EPIC-II and EPIC-III under packet loss, reordering, and duplication. |
| [`flow-sim/`](flow-sim/README.md) | OMNeT++ flow-level simulations for evaluating resource management and job completion time. |
| [`pkt-sim/`](pkt-sim/README.md) | ns-3 packet-level simulations of Ring, EPIC-II, and EPIC-III, including packet-loss experiments. |
| [`emulation/`](emulation/README.md) | VM-based implementation used to evaluate EPIC's collective modes over RoCE. |
| [`tofino/`](tofino/README.md) | Tofino prototype, including the host frontend/backend and the P4 switch implementation. |
| [`np/`](np/README.md) | Network-processor implementations of EPIC-II and EPIC-III for AllReduce and Barrier. |

Please refer to the README in each subdirectory for prerequisites, build instructions, and usage.

## Citation

```bibtex
TBD
```
