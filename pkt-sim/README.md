# EPIC ns-3 Packet-Level Simulation

This directory contains **In-Network Collective (INC) simulations** built on ns-3.43.

It implements three collective communication schemes and compares their performance under packet loss:

| Scheme | Topology | Description |
| --- | --- | --- |
| **Ring** | 8 ranks connected in a ring | A conventional Ring algorithm used as the baseline. |
| **EPIC-II** | 8 workers connected to one switch in a star topology | The switch performs in-network aggregation, while the endpoints use a sliding window and timeout-based retransmission with ACKs. |
| **EPIC-III** | Same as EPIC-II | Adds NAK-based fast retransmission to EPIC-II. |

All links use **100 Gbps bandwidth and 1 us latency**, and applications start at `t=1s`. The aggregation logic runs over UDP and uses custom ns-3 `Tag` objects to carry metadata such as sequence/PSN, rank ID, and packet type.

---

## Contents

The upstream ns-3 source is **not modified**. All EPIC code is implemented as standalone simulation programs under `scratch/` (approximately 10,000 lines). Each `.cc` file is self-contained and includes the application definitions, topology setup, and `main()` function, so some code is duplicated across simulations.

### 1. Ring baseline (`scratch/Ring*.cc`)

| File | Collective |
| --- | --- |
| [RingAllReduce.cc](scratch/RingAllReduce.cc) | AllReduce |
| [RingReduce.cc](scratch/RingReduce.cc) | Reduce |
| [RingReduceScatter.cc](scratch/RingReduceScatter.cc) | ReduceScatter |
| [RingAllGather.cc](scratch/RingAllGather.cc) | AllGather |
| [RingBroadcast.cc](scratch/RingBroadcast.cc) | Broadcast |

### 2. EPIC-II (`scratch/EPIC2*.cc`)

The switch-side `ReceiverApp` performs aggregation. Each worker-side `SenderApp` maintains a single sliding window and retransmits packets after an RTO. Packet types are `DATA`, `AGGREGATED`, and `ACK`.

[EPIC2AllReduce.cc](scratch/EPIC2AllReduce.cc), [EPIC2Reduce.cc](scratch/EPIC2Reduce.cc), [EPIC2ReduceScatter.cc](scratch/EPIC2ReduceScatter.cc), [EPIC2AllGather.cc](scratch/EPIC2AllGather.cc), and [EPIC2Broadcast.cc](scratch/EPIC2Broadcast.cc)

### 3. EPIC-III (`scratch/EPIC3*.cc`)

EPIC-III extends EPIC-II with NAK-based fast retransmission. When the receiver detects a sequence gap, it immediately returns a NAK instead of waiting for the RTO to expire.

[EPIC3AllReduce.cc](scratch/EPIC3AllReduce.cc), [EPIC3Reduce.cc](scratch/EPIC3Reduce.cc), [EPIC3ReduceScatter.cc](scratch/EPIC3ReduceScatter.cc), [EPIC3AllGather.cc](scratch/EPIC3AllGather.cc), and [EPIC3Broadcast.cc](scratch/EPIC3Broadcast.cc)

### 4. Packet-loss experiments

[EPIC2AllReduce_PacketLoss.cc](scratch/EPIC2AllReduce_PacketLoss.cc) and [EPIC3AllReduce_PacketLoss.cc](scratch/EPIC3AllReduce_PacketLoss.cc)

| Parameter | Description |
| --- | --- |
| `--lossRate` | Packet-loss rate on each lossy link (0–1; 0 means no loss). |
| `--lossLinkNum` | Number of lossy links (1–8). |
| `--retxTimeoutUs` | Retransmission timeout in microseconds. |
| `--dataBytes` | Total data size in bytes. |

## Build

Configure ns-3 before the first build:

```bash
./ns3 configure
```

Build all targets:

```bash
./ns3 build
```

---

## Run

```bash
# Ring baseline
./ns3 run "RingAllReduce"

# EPIC-II / EPIC-III
./ns3 run EPIC2AllReduce
./ns3 run EPIC3AllReduce

# Packet-loss experiment: all four parameters are required
./ns3 run "EPIC3AllReduce_PacketLoss --lossRate=0.001 --lossLinkNum=1 --retxTimeoutUs=100 --dataBytes=16777600"
```

For general ns-3 usage, refer to the [official ns-3 documentation](https://www.nsnam.org).
