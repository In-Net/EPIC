#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$ROOT_DIR/build}"

cmake -S "$ROOT_DIR" -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Release
cmake --build "$BUILD_DIR" -j"$(nproc)"

printf "Built binaries:\n"
printf "  %s\n" "$BUILD_DIR/primitive_host"
printf "  %s\n" "$BUILD_DIR/star4_switch"
printf "  %s\n" "$BUILD_DIR/mode1_node"
printf "  %s\n" "$BUILD_DIR/pcap_l2_switch"
printf "  %s\n" "$BUILD_DIR/pure_pingpong"
