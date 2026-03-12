/*
 * datagen.c - Graph Data Generator for Benchmarks
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Generates CSV graph data in various topologies for benchmark workloads.
 *
 * Usage:
 *   datagen --nodes N --type {chain|random|cycle|tree}
 *           [--edges M] [--seed S] [--weighted] [--output path.csv]
 */

#ifndef _MSC_VER
#include <getopt.h>
#else
extern int getopt(int argc, char *const argv[], const char *optstring);
extern int optind;
extern char *optarg;

/* Stub struct option for MSVC compatibility (getopt_long not used) */
struct option {
    const char *name;
    int has_arg;
    int *flag;
    int val;
};
#define no_argument 0
#define required_argument 1
#endif
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

enum graph_type {
    GRAPH_CHAIN,
    GRAPH_RANDOM,
    GRAPH_CYCLE,
    GRAPH_TREE,
};

static void
usage(const char *prog)
{
    fprintf(stderr,
            "Usage: %s --nodes N --type {chain|random|cycle|tree}\n"
            "          [--edges M] [--seed S] [--weighted] [--output FILE]\n"
            "\n"
            "Options:\n"
            "  --nodes N     Number of nodes (required)\n"
            "  --type TYPE   Graph topology (required)\n"
            "  --edges M     Number of edges (random only, default: 2*N)\n"
            "  --seed S      Random seed (default: time-based)\n"
            "  --weighted    Add weight column (for SSSP)\n"
            "  --output FILE Output file (default: stdout)\n",
            prog);
}

static enum graph_type
parse_type(const char *s)
{
    if (strcmp(s, "chain") == 0)
        return GRAPH_CHAIN;
    if (strcmp(s, "random") == 0)
        return GRAPH_RANDOM;
    if (strcmp(s, "cycle") == 0)
        return GRAPH_CYCLE;
    if (strcmp(s, "tree") == 0)
        return GRAPH_TREE;
    fprintf(stderr, "error: unknown graph type '%s'\n", s);
    exit(1);
}

static void
emit_edge(FILE *out, int32_t src, int32_t dst, int weighted, uint32_t *rng)
{
    if (weighted) {
        /* Simple LCG for weight 1..10 */
        *rng = *rng * 1103515245u + 12345u;
        int32_t w = (int32_t)((*rng >> 16) % 10) + 1;
        fprintf(out, "%d,%d,%d\n", src, dst, w);
    } else {
        fprintf(out, "%d,%d\n", src, dst);
    }
}

static void
gen_chain(FILE *out, int32_t n, int weighted, uint32_t *rng)
{
    for (int32_t i = 1; i < n; i++)
        emit_edge(out, i, i + 1, weighted, rng);
}

static void
gen_cycle(FILE *out, int32_t n, int weighted, uint32_t *rng)
{
    for (int32_t i = 1; i < n; i++)
        emit_edge(out, i, i + 1, weighted, rng);
    emit_edge(out, n, 1, weighted, rng);
}

static void
gen_tree(FILE *out, int32_t n, int weighted, uint32_t *rng)
{
    /* Binary tree: node i has children 2i and 2i+1 */
    for (int32_t i = 1; i <= n; i++) {
        int32_t left = 2 * i;
        int32_t right = 2 * i + 1;
        if (left <= n)
            emit_edge(out, i, left, weighted, rng);
        if (right <= n)
            emit_edge(out, i, right, weighted, rng);
    }
}

static void
gen_random(FILE *out, int32_t n, int32_t m, int weighted, uint32_t *rng)
{
    for (int32_t i = 0; i < m; i++) {
        *rng = *rng * 1103515245u + 12345u;
        int32_t src = (int32_t)((*rng >> 16) % (uint32_t)n) + 1;
        *rng = *rng * 1103515245u + 12345u;
        int32_t dst = (int32_t)((*rng >> 16) % (uint32_t)n) + 1;
        emit_edge(out, src, dst, weighted, rng);
    }
}

int
main(int argc, char **argv)
{
    int32_t nodes = 0;
    int32_t edges = 0;
    uint32_t seed = (uint32_t)time(NULL);
    int weighted = 0;
    const char *output = NULL;
    const char *type_str = NULL;

    static struct option long_opts[] = {
        { "nodes", required_argument, NULL, 'n' },
        { "edges", required_argument, NULL, 'e' },
        { "type", required_argument, NULL, 't' },
        { "seed", required_argument, NULL, 's' },
        { "weighted", no_argument, NULL, 'w' },
        { "output", required_argument, NULL, 'o' },
        { "help", no_argument, NULL, 'h' },
        { NULL, 0, NULL, 0 },
    };

    int opt;
#ifndef _MSC_VER
    while ((opt = getopt_long(argc, argv, "n:e:t:s:wo:h", long_opts, NULL))
           != -1) {
#else
    /* MSVC: getopt_long not available; use simple getopt fallback */
    while ((opt = getopt(argc, argv, "n:e:t:s:wo:h"))
           != -1) {
#endif
        switch (opt) {
        case 'n':
            nodes = (int32_t)strtol(optarg, NULL, 10);
            break;
        case 'e':
            edges = (int32_t)strtol(optarg, NULL, 10);
            break;
        case 't':
            type_str = optarg;
            break;
        case 's':
            seed = (uint32_t)strtoul(optarg, NULL, 10);
            break;
        case 'w':
            weighted = 1;
            break;
        case 'o':
            output = optarg;
            break;
        case 'h':
        default:
            usage(argv[0]);
            return (opt == 'h') ? 0 : 1;
        }
    }

    if (nodes <= 0 || !type_str) {
        usage(argv[0]);
        return 1;
    }

    enum graph_type gtype = parse_type(type_str);

    if (edges <= 0 && gtype == GRAPH_RANDOM)
        edges = 2 * nodes;

    FILE *out = stdout;
    if (output) {
        out = fopen(output, "w");
        if (!out) {
            perror(output);
            return 1;
        }
    }

    uint32_t rng = seed;

    switch (gtype) {
    case GRAPH_CHAIN:
        gen_chain(out, nodes, weighted, &rng);
        break;
    case GRAPH_RANDOM:
        gen_random(out, nodes, edges, weighted, &rng);
        break;
    case GRAPH_CYCLE:
        gen_cycle(out, nodes, weighted, &rng);
        break;
    case GRAPH_TREE:
        gen_tree(out, nodes, weighted, &rng);
        break;
    }

    if (output)
        fclose(out);

    return 0;
}
