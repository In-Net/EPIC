#include <mpi.h>

#include <getopt.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef enum {
    PRIM_ALLREDUCE,
    PRIM_REDUCE,
    PRIM_BCAST,
    PRIM_ALLGATHER,
    PRIM_REDUCESCATTER,
    PRIM_BARRIER,
} primitive_t;

typedef struct {
    primitive_t primitive;
    int count;
    int iters;
    int warmup;
    int root;
    int chunk_count;
    int pipeline_depth;
    int chunk_gap_us;
} opts_t;

static const char *primitive_name(primitive_t p) {
    switch (p) {
    case PRIM_ALLREDUCE:
        return "allreduce";
    case PRIM_REDUCE:
        return "reduce";
    case PRIM_BCAST:
        return "broadcast";
    case PRIM_ALLGATHER:
        return "allgather";
    case PRIM_REDUCESCATTER:
        return "reducescatter";
    case PRIM_BARRIER:
        return "barrier";
    }
    return "unknown";
}

static int parse_primitive(const char *s, primitive_t *out) {
    if (strcmp(s, "allreduce") == 0) {
        *out = PRIM_ALLREDUCE;
        return 1;
    }
    if (strcmp(s, "reduce") == 0) {
        *out = PRIM_REDUCE;
        return 1;
    }
    if (strcmp(s, "broadcast") == 0 || strcmp(s, "bcast") == 0) {
        *out = PRIM_BCAST;
        return 1;
    }
    if (strcmp(s, "allgather") == 0) {
        *out = PRIM_ALLGATHER;
        return 1;
    }
    if (strcmp(s, "reducescatter") == 0 ||
        strcmp(s, "reduce_scatter") == 0 ||
        strcmp(s, "reduce-scatter") == 0) {
        *out = PRIM_REDUCESCATTER;
        return 1;
    }
    if (strcmp(s, "barrier") == 0) {
        *out = PRIM_BARRIER;
        return 1;
    }
    return 0;
}

static int parse_int(const char *s) {
    char *end = NULL;
    long v = strtol(s, &end, 0);
    if (!end || *end != '\0' || v < 0 || v > 2147483647L) {
        fprintf(stderr, "invalid integer: %s\n", s);
        MPI_Abort(MPI_COMM_WORLD, 2);
    }
    return (int)v;
}

static void usage(const char *prog, int rank) {
    if (rank == 0) {
        fprintf(stderr,
                "usage: %s --primitive allreduce|reduce|broadcast|allgather|reducescatter|barrier "
                "[--count N] [--iters N] [--warmup N] [--root N] "
                "[--chunk-count N] [--pipeline-depth N] [--chunk-gap-us N]\n",
                prog);
    }
}

static int parse_args(int argc, char **argv, opts_t *opts, int rank) {
    opts->primitive = PRIM_ALLREDUCE;
    opts->count = 4096;
    opts->iters = 100;
    opts->warmup = 10;
    opts->root = 0;
    opts->chunk_count = 0;
    opts->pipeline_depth = 1;
    opts->chunk_gap_us = 0;

    static const struct option long_opts[] = {
        {"primitive", required_argument, NULL, 'p'},
        {"count", required_argument, NULL, 'c'},
        {"iters", required_argument, NULL, 'i'},
        {"warmup", required_argument, NULL, 'w'},
        {"root", required_argument, NULL, 'r'},
        {"chunk-count", required_argument, NULL, 'C'},
        {"pipeline-depth", required_argument, NULL, 'P'},
        {"chunk-gap-us", required_argument, NULL, 'G'},
        {0, 0, 0, 0},
    };

    int c;
    while ((c = getopt_long(argc, argv, "", long_opts, NULL)) != -1) {
        switch (c) {
        case 'p':
            if (!parse_primitive(optarg, &opts->primitive)) {
                if (rank == 0) {
                    fprintf(stderr, "invalid primitive: %s\n", optarg);
                }
                return 0;
            }
            break;
        case 'c':
            opts->count = parse_int(optarg);
            break;
        case 'i':
            opts->iters = parse_int(optarg);
            break;
        case 'w':
            opts->warmup = parse_int(optarg);
            break;
        case 'r':
            opts->root = parse_int(optarg);
            break;
        case 'C':
            opts->chunk_count = parse_int(optarg);
            break;
        case 'P':
            opts->pipeline_depth = parse_int(optarg);
            break;
        case 'G':
            opts->chunk_gap_us = parse_int(optarg);
            break;
        default:
            return 0;
        }
    }

    if ((opts->count <= 0 && opts->primitive != PRIM_BARRIER) ||
        opts->iters <= 0 || opts->root < 0) {
        return 0;
    }
    return 1;
}

static int chunk_count_for(const opts_t *opts) {
    if (opts->primitive == PRIM_BARRIER) {
        return 0;
    }
    if (opts->chunk_count <= 0 || opts->chunk_count >= opts->count) {
        return opts->count;
    }
    return opts->chunk_count;
}

static void sleep_chunk_gap(const opts_t *opts) {
    if (opts->chunk_gap_us <= 0) {
        return;
    }
    struct timespec ts;
    ts.tv_sec = opts->chunk_gap_us / 1000000;
    ts.tv_nsec = (long)(opts->chunk_gap_us % 1000000) * 1000L;
    nanosleep(&ts, NULL);
}

static void init_buffers(const opts_t *opts, int rank, int32_t *sendbuf,
                         int32_t *recvbuf) {
    if (opts->primitive == PRIM_BARRIER) {
        return;
    }

    for (int i = 0; i < opts->count; i++) {
        sendbuf[i] = i * (rank + 1);
        recvbuf[i] = 0;
    }

    if (opts->primitive == PRIM_BCAST) {
        if (rank == opts->root) {
            for (int i = 0; i < opts->count; i++) {
                recvbuf[i] = i * (opts->root + 1);
            }
        } else {
            memset(recvbuf, 0, (size_t)opts->count * sizeof(*recvbuf));
        }
    }
}

static int verify(const opts_t *opts, int rank, int world, const int32_t *recvbuf) {
    int multiplier = 0;

    if (opts->primitive == PRIM_BARRIER) {
        return 1;
    }

    if ((opts->primitive == PRIM_ALLGATHER ||
         opts->primitive == PRIM_REDUCESCATTER) &&
        opts->count % world != 0) {
        fprintf(stderr, "count must be divisible by world for primitive=%s\n",
                primitive_name(opts->primitive));
        return 0;
    }

    if (opts->primitive == PRIM_ALLGATHER) {
        int block = opts->count / world;
        for (int r = 0; r < world; r++) {
            for (int i = 0; i < block; i++) {
                int idx = r * block + i;
                int32_t expected = idx * (r + 1);
                if (recvbuf[idx] != expected) {
                    fprintf(stderr,
                            "rank=%d verify failed primitive=%s idx=%d got=%d expected=%d\n",
                            rank, primitive_name(opts->primitive), idx,
                            recvbuf[idx], expected);
                    return 0;
                }
            }
        }
        return 1;
    }

    if (opts->primitive == PRIM_REDUCESCATTER) {
        int block = opts->count / world;
        int start = rank * block;
        for (int r = 0; r < world; r++) {
            multiplier += r + 1;
        }
        for (int i = 0; i < block; i++) {
            int idx = start + i;
            int32_t expected = idx * multiplier;
            if (recvbuf[idx] != expected) {
                fprintf(stderr,
                        "rank=%d verify failed primitive=%s idx=%d got=%d expected=%d\n",
                        rank, primitive_name(opts->primitive), idx,
                        recvbuf[idx], expected);
                return 0;
            }
        }
        return 1;
    }

    if (opts->primitive == PRIM_BCAST) {
        multiplier = opts->root + 1;
    } else {
        for (int r = 0; r < world; r++) {
            multiplier += r + 1;
        }
    }

    if (opts->primitive == PRIM_REDUCE && rank != opts->root) {
        return 1;
    }

    for (int i = 0; i < opts->count; i++) {
        int32_t expected = i * multiplier;
        if (recvbuf[i] != expected) {
            fprintf(stderr,
                    "rank=%d verify failed primitive=%s idx=%d got=%d expected=%d\n",
                    rank, primitive_name(opts->primitive), i, recvbuf[i], expected);
            return 0;
        }
    }
    return 1;
}

static void run_blocking_collective(const opts_t *opts, int32_t *sendbuf,
                                    int32_t *recvbuf, int32_t *scratchbuf,
                                    int offset, int count, int rank, int world) {
    if (opts->primitive == PRIM_ALLREDUCE) {
        MPI_Allreduce(sendbuf + offset, recvbuf + offset, count, MPI_INT32_T,
                      MPI_SUM, MPI_COMM_WORLD);
    } else if (opts->primitive == PRIM_REDUCE) {
        MPI_Reduce(sendbuf + offset, recvbuf + offset, count, MPI_INT32_T,
                   MPI_SUM, opts->root, MPI_COMM_WORLD);
    } else if (opts->primitive == PRIM_BCAST) {
        MPI_Bcast(recvbuf + offset, count, MPI_INT32_T, opts->root,
                  MPI_COMM_WORLD);
    } else if (opts->primitive == PRIM_ALLGATHER) {
        int total_block = opts->count / world;
        int block_offset = offset / world;
        int chunk_block = count / world;
        int recvcounts[world];
        int displs[world];
        for (int r = 0; r < world; r++) {
            recvcounts[r] = chunk_block;
            displs[r] = r * total_block + block_offset;
        }
        MPI_Allgatherv(sendbuf + rank * total_block + block_offset,
                       chunk_block, MPI_INT32_T, recvbuf, recvcounts, displs,
                       MPI_INT32_T, MPI_COMM_WORLD);
    } else if (opts->primitive == PRIM_REDUCESCATTER) {
        int total_block = opts->count / world;
        int block_offset = offset / world;
        int chunk_block = count / world;
        if (count == opts->count) {
            MPI_Reduce_scatter_block(sendbuf, recvbuf + rank * total_block,
                                     chunk_block, MPI_INT32_T, MPI_SUM,
                                     MPI_COMM_WORLD);
        } else {
            for (int r = 0; r < world; r++) {
                memcpy(scratchbuf + r * chunk_block,
                       sendbuf + r * total_block + block_offset,
                       (size_t)chunk_block * sizeof(*scratchbuf));
            }
            MPI_Reduce_scatter_block(scratchbuf,
                                     recvbuf + rank * total_block + block_offset,
                                     chunk_block, MPI_INT32_T, MPI_SUM,
                                     MPI_COMM_WORLD);
        }
    } else {
        MPI_Barrier(MPI_COMM_WORLD);
    }
}

static void start_nonblocking_collective(const opts_t *opts, int32_t *sendbuf,
                                         int32_t *recvbuf, int offset, int count,
                                         int rank, int world,
                                         MPI_Request *request) {
    if (opts->primitive == PRIM_ALLREDUCE) {
        MPI_Iallreduce(sendbuf + offset, recvbuf + offset, count, MPI_INT32_T,
                       MPI_SUM, MPI_COMM_WORLD, request);
    } else if (opts->primitive == PRIM_REDUCE) {
        MPI_Ireduce(sendbuf + offset, recvbuf + offset, count, MPI_INT32_T,
                    MPI_SUM, opts->root, MPI_COMM_WORLD, request);
    } else if (opts->primitive == PRIM_BCAST) {
        MPI_Ibcast(recvbuf + offset, count, MPI_INT32_T, opts->root,
                   MPI_COMM_WORLD, request);
    } else if (opts->primitive == PRIM_ALLGATHER) {
        int block = count / world;
        MPI_Iallgather(sendbuf + offset + rank * block, block, MPI_INT32_T,
                       recvbuf + offset, block, MPI_INT32_T, MPI_COMM_WORLD,
                       request);
    } else if (opts->primitive == PRIM_REDUCESCATTER) {
        int block = count / world;
        MPI_Ireduce_scatter_block(sendbuf + offset,
                                  recvbuf + offset + rank * block, block,
                                  MPI_INT32_T, MPI_SUM, MPI_COMM_WORLD,
                                  request);
    } else {
        MPI_Ibarrier(MPI_COMM_WORLD, request);
    }
}

static void run_collective(const opts_t *opts, int32_t *sendbuf,
                           int32_t *recvbuf, int32_t *scratchbuf, int rank,
                           int world) {
    if (opts->primitive == PRIM_BARRIER) {
        MPI_Barrier(MPI_COMM_WORLD);
        return;
    }

    int chunk_count = chunk_count_for(opts);
    if ((opts->primitive == PRIM_ALLGATHER ||
         opts->primitive == PRIM_REDUCESCATTER) &&
        (opts->count % world != 0 || chunk_count % world != 0)) {
        fprintf(stderr,
                "count and chunk_count must be divisible by world for primitive=%s\n",
                primitive_name(opts->primitive));
        MPI_Abort(MPI_COMM_WORLD, 2);
    }

    int chunks = (opts->count + chunk_count - 1) / chunk_count;

    if (chunks == 1 || opts->pipeline_depth == 1 ||
        opts->primitive == PRIM_ALLGATHER ||
        opts->primitive == PRIM_REDUCESCATTER) {
        for (int chunk = 0; chunk < chunks; chunk++) {
            int offset = chunk * chunk_count;
            int this_count = chunk_count;
            if (offset + this_count > opts->count) {
                this_count = opts->count - offset;
            }
            run_blocking_collective(opts, sendbuf, recvbuf, scratchbuf, offset,
                                    this_count, rank, world);
            if (chunk + 1 < chunks) {
                sleep_chunk_gap(opts);
            }
        }
        return;
    }

    int window = opts->pipeline_depth;
    if (window <= 0 || window > chunks) {
        window = chunks;
    }

    MPI_Request *requests = calloc((size_t)window, sizeof(*requests));
    if (!requests) {
        fprintf(stderr, "failed to allocate MPI requests\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    for (int base = 0; base < chunks; base += window) {
        int batch = chunks - base;
        if (batch > window) {
            batch = window;
        }
        for (int i = 0; i < batch; i++) {
            int chunk = base + i;
            int offset = chunk * chunk_count;
            int this_count = chunk_count;
            if (offset + this_count > opts->count) {
                this_count = opts->count - offset;
            }
            start_nonblocking_collective(opts, sendbuf, recvbuf, offset,
                                         this_count, rank, world, &requests[i]);
        }
        MPI_Waitall(batch, requests, MPI_STATUSES_IGNORE);
        if (base + batch < chunks) {
            sleep_chunk_gap(opts);
        }
    }

    free(requests);
}

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);

    int rank = 0;
    int world = 0;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world);

    opts_t opts;
    if (!parse_args(argc, argv, &opts, rank) || opts.root >= world) {
        usage(argv[0], rank);
        MPI_Finalize();
        return 2;
    }

    if ((opts.primitive == PRIM_ALLGATHER ||
         opts.primitive == PRIM_REDUCESCATTER) &&
        opts.count % world != 0) {
        if (rank == 0) {
            fprintf(stderr, "count must be divisible by world for primitive=%s\n",
                    primitive_name(opts.primitive));
        }
        MPI_Finalize();
        return 2;
    }

    size_t alloc_count = opts.count > 0 ? (size_t)opts.count : 1u;
    int32_t *sendbuf = malloc(alloc_count * sizeof(*sendbuf));
    int32_t *recvbuf = malloc(alloc_count * sizeof(*recvbuf));
    int32_t *scratchbuf = NULL;
    if (opts.primitive == PRIM_REDUCESCATTER) {
        scratchbuf = malloc(alloc_count * sizeof(*scratchbuf));
    }
    if (!sendbuf || !recvbuf ||
        (opts.primitive == PRIM_REDUCESCATTER && !scratchbuf)) {
        fprintf(stderr, "rank=%d malloc failed\n", rank);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    double sum_max_us = 0.0;
    double min_max_us = 1.0e100;
    double max_max_us = 0.0;

    for (int iter = -opts.warmup; iter < opts.iters; iter++) {
        init_buffers(&opts, rank, sendbuf, recvbuf);
        MPI_Barrier(MPI_COMM_WORLD);

        double t0 = MPI_Wtime();
        run_collective(&opts, sendbuf, recvbuf, scratchbuf, rank, world);
        double t1 = MPI_Wtime();

        int local_ok = verify(&opts, rank, world, recvbuf);
        int global_ok = 0;
        MPI_Allreduce(&local_ok, &global_ok, 1, MPI_INT, MPI_LAND,
                      MPI_COMM_WORLD);
        if (!global_ok) {
            MPI_Abort(MPI_COMM_WORLD, 3);
        }

        double local_us = (t1 - t0) * 1000000.0;
        double iter_max_us = 0.0;
        double iter_min_us = 0.0;
        double iter_sum_us = 0.0;
        MPI_Reduce(&local_us, &iter_max_us, 1, MPI_DOUBLE, MPI_MAX, 0,
                   MPI_COMM_WORLD);
        MPI_Reduce(&local_us, &iter_min_us, 1, MPI_DOUBLE, MPI_MIN, 0,
                   MPI_COMM_WORLD);
        MPI_Reduce(&local_us, &iter_sum_us, 1, MPI_DOUBLE, MPI_SUM, 0,
                   MPI_COMM_WORLD);

        if (rank == 0 && iter >= 0) {
            double iter_avg_us = iter_sum_us / world;
            printf("ITER primitive=%s iter=%d rank_max_us=%.3f rank_avg_us=%.3f rank_min_us=%.3f\n",
                   primitive_name(opts.primitive), iter, iter_max_us, iter_avg_us,
                   iter_min_us);
            sum_max_us += iter_max_us;
            if (iter_max_us < min_max_us) {
                min_max_us = iter_max_us;
            }
            if (iter_max_us > max_max_us) {
                max_max_us = iter_max_us;
            }
        }
    }

    if (rank == 0) {
        printf("RESULT primitive=%s world=%d count=%d iters=%d warmup=%d root=%d "
               "chunk_count=%d pipeline_depth=%d chunk_gap_us=%d "
               "avg_rank_max_us=%.3f min_rank_max_us=%.3f max_rank_max_us=%.3f\n",
               primitive_name(opts.primitive), world, opts.count, opts.iters,
               opts.warmup, opts.root, chunk_count_for(&opts),
               opts.pipeline_depth, opts.chunk_gap_us, sum_max_us / opts.iters, min_max_us,
               max_max_us);
        fflush(stdout);
    }

    free(sendbuf);
    free(recvbuf);
    free(scratchbuf);
    MPI_Finalize();
    return 0;
}
