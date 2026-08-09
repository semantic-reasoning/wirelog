/*
 * test_cli_watch.c - CLI watch-mode tests
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

#include "../wirelog/cli/driver.h"

#include "test_tmpdir.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int
main(void)
{
    char inpath[512];
    test_tmppath(inpath, sizeof(inpath), "wirelog_test_watch_string.in");
    FILE *in = fopen(inpath, "w");
    if (!in) {
        fprintf(stderr, "cannot create watch input file\n");
        return 1;
    }
    const size_t value_len = 5000;
    char *value = (char *)malloc(value_len + 1);
    if (!value) {
        fclose(in);
        remove(inpath);
        fprintf(stderr, "cannot allocate watch value\n");
        return 1;
    }
    memset(value, 'A', value_len);
    value[value_len] = '\0';
    fprintf(in, "person \"%s\"\n", value);
    fclose(in);

    if (!freopen(inpath, "r", stdin)) {
        free(value);
        remove(inpath);
        fprintf(stderr, "cannot redirect stdin\n");
        return 1;
    }

    char outpath[512];
    test_tmppath(outpath, sizeof(outpath), "wirelog_test_watch_string.out");
    FILE *out = fopen(outpath, "w");
    if (!out) {
        free(value);
        remove(inpath);
        fprintf(stderr, "cannot create output file\n");
        return 1;
    }

    const char *src = ".decl person(name: string)\n"
        ".decl seen(name: string)\n"
        "seen(name) :- person(name).\n";

    int rc = wl_run_pipeline(src, 1, false, true, 0, out);
    fclose(out);
    if (rc != 0) {
        free(value);
        remove(inpath);
        remove(outpath);
        fprintf(stderr, "wl_run_pipeline returned %d\n", rc);
        return 1;
    }

    char *output = wl_read_file(outpath);
    if (!output) {
        free(value);
        remove(inpath);
        remove(outpath);
        fprintf(stderr, "cannot read output file\n");
        return 1;
    }

    int ok = strstr(output, value) != NULL;
    char *first = strstr(output, "seen(");
    if (first && strstr(first + 5, "seen(") != NULL)
        ok = 0;
    if (!ok)
        fprintf(stderr, "expected one complete wide watch tuple\n");

    free(output);
    free(value);
    remove(inpath);
    remove(outpath);
    return ok ? 0 : 1;
}
