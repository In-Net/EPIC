// This file is for Huawei

#include <stdint.h>
#include <cuda_runtime.h>

#include "../../../common/common.hpp"

void all_reduce(void *data, uint64_t size, group_t group, cudaStream_t stream);

void reduce(void *data, rank_t root, uint64_t size, group_t group, cudaStream_t stream);

void broadcast(void *data, rank_t root, uint64_t size, group_t group, cudaStream_t stream);

void reduce_scatter(void *data, uint64_t size, group_t group, cudaStream_t stream);

void all_gather(void *data, uint64_t size, group_t group, cudaStream_t stream);

void init(string _shm_name);

void finalize();