/*
 * main.c - wirelog CLI Entry Point
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Reads a .dl file and executes it through the full Datalog pipeline.
 *
 * Usage: wirelog [options] <file.dl>
 *   --workers N   Number of worker threads (default: 1)
 *   --help        Show usage information
 */

#include "driver.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void
print_usage(const char *progname)
{
    fprintf(stderr, "Usage: %s [options] <file.dl>\n", progname);
    fprintf(stderr, "\nOptions:\n");
    fprintf(stderr,
            "  --workers N   Number of DD worker threads (default: 1)\n");
    fprintf(stderr, "  --help        Show this help message\n");
}

int
main(int argc, char *argv[])
{
    const char *filepath = NULL;
    uint32_t num_workers = 1;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            print_usage(argv[0]);
            return 0;
        }
        if (strcmp(argv[i], "--workers") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "error: --workers requires an argument\n");
                return 1;
            }
            long val = strtol(argv[++i], NULL, 10);
            if (val < 1 || val > 256) {
                fprintf(stderr, "error: workers must be between 1 and 256\n");
                return 1;
            }
            num_workers = (uint32_t)val;
            continue;
        }
        if (argv[i][0] == '-') {
            fprintf(stderr, "error: unknown option '%s'\n", argv[i]);
            print_usage(argv[0]);
            return 1;
        }
        if (filepath) {
            fprintf(stderr, "error: multiple input files not supported\n");
            return 1;
        }
        filepath = argv[i];
    }

    if (!filepath) {
        fprintf(stderr, "error: no input file specified\n");
        print_usage(argv[0]);
        return 1;
    }

    char *source = wl_read_file(filepath);
    if (!source) {
        fprintf(stderr, "error: cannot read file '%s'\n", filepath);
        return 1;
    }

    int rc = wl_run_pipeline(source, num_workers, stdout);
    free(source);

    if (rc != 0) {
        fprintf(stderr, "error: execution failed\n");
        return 1;
    }

    return 0;
}
