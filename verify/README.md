# EPIC Data Plane Verification

This repository contains the [TLA+](https://github.com/tlaplus/tlaplus) formal verification specifications for the EPIC data plane protocol. Using the TLC model checker, we exhaustively verify the correctness of EPIC's IncEngine logic under diverse network conditions, including packet loss, out-of-order delivery, and duplication.

## Overview

We verify two of EPIC's three polymorphic modes:

- **Mode-II (Connection Translated)** — `trans_*` specifications
- **Mode-III (Connection Augmented)** — `aug_*` specifications

Mode-I (Connection Terminated) inherits correctness from the underlying RoCE transport layer and does not require separate verification.

For each mode, we verify three collective communication primitives: **AllReduce**, **Reduce**, and **Broadcast**. The remaining primitives (Barrier, ReduceScatter, AllGather) are compositions of these three and their correctness follows by construction.

### Verified Properties

- **Computational accuracy** — the final result at each rank matches the expected server-side reduction.
- **Protocol termination** — the protocol eventually reaches a terminal state under all possible network conditions (enabled by setting `TERMINATION_CHECK = TRUE` in the `.cfg` file).

## Directory Structure

```
src/
├── trans_allreduce/     # Mode-II AllReduce
├── trans_reduce/        # Mode-II Reduce
├── trans_broadcast/     # Mode-II Broadcast
├── aug_allreduce/       # Mode-III AllReduce
├── aug_reduce/          # Mode-III Reduce
└── aug_broadcast/       # Mode-III Broadcast
```

Each directory contains three files:

| File    | Description |
|---------|-------------|
| `.tla`  | TLA+ specification |
| `.cfg`  | TLC model checker configuration |
| `.inc`  | VeriNC source file |

The `.tla` and `.cfg` files are generated from the `.inc` files, using our internal tool [VeriNC](https://arxiv.org/abs/2604.10186).

External users should work directly with the `.tla` and `.cfg` files. The `.inc` files are provided for reference only.

### Topology

All specifications use a **Tree-3-2** topology (tree depth 3, fan-in 2):

```
        r          (root switch)
       / \
     s12   s34     (intermediate switches)
    / \   / \
  c1  c2 c3  c4   (compute nodes)
```

Node identifiers in the `.cfg` files: `r = 1`, `s12 = 2`, `s34 = 3`, `c1..c4 = 4..7`.

## Running the Model Checker

### Option 1: VS Code TLA+ Extension (Recommended)

1. Install the [TLA+ extension](https://marketplace.visualstudio.com/items?itemName=alygin.vscode-tlaplus) for VS Code.
2. Open a `.tla` file (e.g., `src/trans_allreduce/trans_allreduce.tla`).
3. Run the model checker via the command palette: **TLA+: Check Model with TLC**.

The extension handles all dependencies automatically.

### Option 2: Command Line

Prerequisites:

- **Java** 17 or later
- **TLA+ tools**: `tla2tools.jar` ([download](https://github.com/tlaplus/tlaplus/releases))
- **CommunityModules**: `CommunityModules-deps.jar` ([download](https://github.com/tlaplus/CommunityModules/releases)) — required for `Bitwise`, `FiniteSetsExt`, `SequencesExt`, and `Functions` operators

```bash
java -cp tla2tools.jar:CommunityModules-deps.jar tlc2.TLC \
    -config src/trans_allreduce/trans_allreduce.cfg \
    src/trans_allreduce/trans_allreduce.tla
```

## Configuration

Key parameters in the `.cfg` files control the verification scope:

| Parameter           | Description                                   | Default |
|---------------------|-----------------------------------------------|---------|
| `TERMINATION_CHECK` | Check protocol termination (liveness)         | `FALSE` |
| `MAX_LOSS`          | Max packet losses allowed during verification | `0`     |
| `MAX_OUT_OF_ORDER`  | Max out-of-order deliveries allowed            | `0`     |
| `MAX_DUPLICATION`   | Max packet duplications allowed               | `0`     |
| `REQ_NUM`           | Number of request packets per client           | `1`     |
| `WINDOW_SIZE`       | Sliding window size for flow control           | `1`     |

Increasing `MAX_LOSS`, `MAX_OUT_OF_ORDER`, or `MAX_DUPLICATION` enables verification under more adversarial network conditions at the cost of larger state spaces.

## Expected Output

A successful run prints:

```
Model checking completed. No error found.
```

along with statistics on the number of states explored (diameter, total states, distinct states).

## Citation

See the [main repository](https://github.com/In-Net/EPIC) for citation information.
