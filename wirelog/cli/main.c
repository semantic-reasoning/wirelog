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
 *   --workers N                Number of worker threads (default: 1)
 *   --watch [interval]         Watch mode: re-evaluate on stdin input
 *   --load-adapter PATH        Load adapter plugin (repeatable, requires
 *                              -Dio_plugin_dlopen=enabled)
 *   --help                     Show usage information
 */

#include "driver.h"

#ifdef WL_HAVE_PLUGIN_LOADER
#include "plugin_loader.h"
#endif

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_ADAPTER_PATHS 16

static void
print_usage(const char *progname)
{
    fprintf(stderr, "Usage: %s [options] <file.dl>\n", progname);
    fprintf(stderr, "\nOptions:\n");
    fprintf(stderr, "  --workers N          Number of worker threads "
        "(default: 1)\n");
    fprintf(stderr, "  --delta              Execute in delta-query mode "
        "(emit delta tuples to stdout)\n");
    fprintf(stderr,
        "  --watch [N]          Watch mode: read facts from stdin and "
        "re-evaluate (interval N ms, default 1000)\n");
    fprintf(stderr, "  --load-adapter PATH  Load adapter plugin "
        "(repeatable"
#ifndef WL_HAVE_PLUGIN_LOADER
        ", disabled in this build"
#endif
        ")\n");
    fprintf(stderr, "  --help               Show this help message\n");
}

int
main(int argc, char *argv[])
{
    const char *filepath = NULL;
    uint32_t num_workers = 1;
    bool delta_mode = false;
    bool watch_mode = false;
    uint32_t watch_interval_ms = 1000;
    const char *adapter_paths[MAX_ADAPTER_PATHS];
    int n_adapters = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            print_usage(argv[0]);
            return 0;
        }
        if (strcmp(argv[i], "--delta") == 0) {
            delta_mode = true;
            continue;
        }
        if (strcmp(argv[i], "--watch") == 0) {
            watch_mode = true;
            /* Accept optional interval argument if next token is a number */
            if (i + 1 < argc && argv[i + 1][0] != '-') {
                long val = strtol(argv[i + 1], NULL, 10);
                if (val > 0) {
                    watch_interval_ms = (uint32_t)val;
                    i++;
                }
            }
            continue;
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
        if (strcmp(argv[i], "--load-adapter") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr,
                    "error: --load-adapter requires a path argument\n");
                return 1;
            }
#ifdef WL_HAVE_PLUGIN_LOADER
            if (n_adapters >= MAX_ADAPTER_PATHS) {
                fprintf(stderr, "error: too many --load-adapter options "
                    "(max %d)\n",
                    MAX_ADAPTER_PATHS);
                return 1;
            }
            adapter_paths[n_adapters++] = argv[++i];
#else
            fprintf(stderr,
                "error: --load-adapter is not available in this build.\n"
                "Rebuild with: meson setup -Dio_plugin_dlopen=enabled\n");
            return 1;
#endif
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

#ifdef WL_HAVE_PLUGIN_LOADER
    /* Load adapter plugins before reading the program */
    for (int i = 0; i < n_adapters; i++) {
        if (wl_plugin_load(adapter_paths[i]) != 0) {
            return 1;
        }
    }
#endif
    (void)adapter_paths;
    (void)n_adapters;

    char *source = wl_read_file(filepath);
    if (!source) {
        fprintf(stderr, "error: cannot read file '%s'\n", filepath);
        return 1;
    }

    int rc = wl_run_pipeline(source, num_workers, delta_mode, watch_mode,
            watch_interval_ms, stdout);
    free(source);

#ifdef WL_HAVE_PLUGIN_LOADER
    wl_plugin_unload_all();
#endif

    if (rc != 0) {
        fprintf(stderr, "error: execution failed\n");
        return 1;
    }

    return 0;
}
