# NP Collective: In-Network Collective Communication Primitives

Collective primitives whose reduction and forwarding run inside a programmable network processor
(NP) switch. Two protocol models — ModelⅡ (the switch ACKs a contribution immediately) and ModelⅢ
(the switch takes custody and acknowledges hop by hop, with a Pipe window) — × four primitives
(barrier, allreduce, bcast, reduce), 8 independent implementations. Source only.

## 1. Layout

```text
NP/
├── model2/     # barrier / allreduce / bcast / reduce
└── model3/     # barrier / allreduce / bcast / reduce
```

Every primitive directory has the same structure:

```text
<model>/<primitive>/
├── README.md                       # protocol design and correctness criteria
├── server/                         # endpoint program: ibverbs bench + Makefile
└── switch/
    ├── dataplane/                  # NPC++ sources executed on the NP
    │   ├── parser.npc              #   packet parsing
    │   ├── control_flow.npc        #   entry point
    │   ├── epic_state.npc          #   slot state tables and debug counters
    │   ├── epic_roce_handlers.npc  #   aggregation, dedup, ACK, window checks
    │   ├── epic_packet_actions.npc #   fan-out / ACK / result assembly
    │   ├── epic_slot_wipe.npc      #   slot reclamation
    │   ├── *.h, lib/npc_arch.h     #   headers, types, address constants
    │   └── icompCommandjson.txt    #   NPC++ compiler command template
    └── onboard/                    # counter name↔ID mapping, table init, CMakeLists.txt
```

The barrier data plane is a single-file implementation; the other primitives are split into modules.

## 2. Requirements

One control host + **4 compute nodes (ranks)** + one NP switch running this data plane, with the
nodes attached directly to the switch.

| Role | Requirements |
|---|---|
| Control host | NP vendor toolchain: NPC++ compiler (`icomp`) and the on-board loader (`on_board_test`); `cmake`, `gcc` |
| Compute node | RoCEv2-capable RDMA NIC; `libibverbs`; `g++` (C++17); an address on the data-plane subnet and its GID index |
| Network | Static ARP for the switch data-plane address; matching NIC MTU; ICRC receive checking disabled (the NP rewrites RoCE headers without recomputing ICRC) |

## 3. Build, Run and Test

### 3.1 Data plane

Place `switch/dataplane/` into the toolchain application directory and invoke the NPC++ compiler as
described by `icompCommandjson.txt`:

```text
-preproccmd "gcc -E -x c -I <appdir>/lib -DEPIC_FAKE_AGG=0 -DEPIC_DEBUG=1"
-outf <scenario> -outfdir <appdir>/bin/
"<appdir>/parser.npc" "<appdir>/control_flow.npc"
```

Output: `<scenario>.bin` plus table configuration JSON. Scenario is `only_fain` for
allreduce/bcast/reduce and `barrier_rdma` for barrier. Macros: `EPIC_FAKE_AGG` (0 = real
element-wise sum, 1 = skip it), `EPIC_DEBUG` (debug counters, required by the test criteria),
`EPIC_UC_MODE` (UC fast path). When porting to another subnet, adjust `SWITCH_MAC`, `VEP_IP` and the
per-rank IP↔channel mapping in `switch/dataplane/types_constant.h`, and `VEP_IP` /
`BARRIER_RDMA_VEP_IP` on the endpoint side.

### 3.2 On-board control program

Copy `switch/onboard/` into `apps/<scenario>/` of the on-board program and build `on_board_test`.
Primitives share a scenario directory, so re-sync and rebuild when switching primitives, otherwise
the build fails with `DEBUG_CNTID_xxx was not declared` or keeps the previous counter mapping.

`on_board_test` loads `<scenario>.bin` and stays resident; on stdin `C` clears the debug counters,
`R` reads them, `Q` exits. Restarting it reloads the image and clears the state tables — do this
before every run, since `C` clears counters only.

### 3.3 Endpoint program

```bash
cd <model>/<primitive>/server && make
```

Binaries: `allreduce_bw`, `bcast_bw`, `reduce_bw`, `barrier_rdma_bw`.

### 3.4 Running

Start on all 4 ranks simultaneously (a TCP start barrier is built in, master is rank 1):

```bash
# allreduce (for bcast / reduce use --bcast --bcast-root R / --reduce --reduce-root R)
./allreduce_bw -r <rank> -d <rdma_dev> -x <gid_index> \
               -k 64 -m 256 -S <bytes> -w 1 -b 1 -T 1 \
               --allreduce --num-ranks 4 --iters 1 --recycle \
               --barrier-ip <master_ip> --barrier-port <port> --barrier-master 1

# barrier
./barrier_rdma_bw -r <rank> -d <rdma_dev> -x <gid_index> \
                  --rounds <N> --round-start <psn> --timeout-ms 5000 \
                  --master-ip <master_ip> --sync-port <port>
```

`-r` rank, `-d` RDMA device, `-x` GID index, `-k` QPs (power of two), `-m` payload bytes (256),
`-S` bytes per rank, `-w`/`-b` per-QP window and batch, `-T` threads (must divide `-k`).
Add `--no-verify` when the data plane is built with `EPIC_FAKE_AGG=1`. Full options: the usage output
of each `server/*.cpp` (barrier supports `--help`).

### 3.5 Testing

Each process prints JSON metrics on exit. A run passes only if every rank exits with `mismatch=0`,
`bad_qp=0`, `pass=true` and no send-completion error; the delivery counts match the primitive
(allreduce: `N` per rank; reduce: `N` at the root only; bcast: `0` at the root, `N` elsewhere); and
the switch counter identities hold, read with the `R` command. For ModelⅡ allreduce, with `N` slots
per rank and `R` ranks: `UP_DATA = AGG = TX_ACK = R·N`, `BUFFERED = (R−1)·N`, `TX_RESULT = N`,
`TX_FANOUT = R·N`, `WIPE = N`; ModelⅢ adds `ACK_COMPLETE = PIPE_ADVANCE = WIPE = N` and
`DOWN_NAK = 0`. Each primitive's own `README.md` lists its full set.
