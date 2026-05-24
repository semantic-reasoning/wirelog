/*
 * csv_reader_fuzz.c - libFuzzer entrypoint for CSV line parser surfaces.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * This is a csv-reader line-level fuzz target for Issue #743.
 */

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "../wirelog/io/csv_reader.h"
#include "../wirelog/intern.h"

#define CSV_FUZZ_MAX_BYTES 4096
#define CSV_FUZZ_MAX_FIELDS 8

int
LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    size_t copy_len = size < CSV_FUZZ_MAX_BYTES ? size : CSV_FUZZ_MAX_BYTES;

    char *line = (char *)malloc(copy_len + 1);
    if (!line) {
        return 0;
    }

    memcpy(line, data, copy_len);
    line[copy_len] = '\0';

    int64_t values[CSV_FUZZ_MAX_FIELDS];
    uint32_t count = 0;
    (void)wl_csv_parse_line(line, ',', values, CSV_FUZZ_MAX_FIELDS, &count);

    wirelog_column_type_t col_types[2] = {
        WIRELOG_TYPE_INT64,
        WIRELOG_TYPE_STRING,
    };
    int64_t ex_values[2];
    uint32_t ex_count = 0;
    wl_intern_t *intern = wl_intern_create();
    if (intern) {
        (void)wl_csv_parse_line_ex(line, ',', col_types, 2, ex_values,
            &ex_count, intern);
        wl_intern_free(intern);
    }

    (void)count;
    (void)ex_count;
    free(line);
    return 0;
}
