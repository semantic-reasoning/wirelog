/*
 * intern_fuzz.c - libFuzzer entrypoint for intern-table APIs.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "../wirelog/intern.h"

#define MAX_INTERN_INPUT 2048
#define MAX_TOKEN_BYTES 256

static void
exercise_intern_tokens(wl_intern_t *intern, const char *input)
{
    char token_buf[MAX_TOKEN_BYTES + 1];
    const char *cursor = input;
    size_t token_len = 0;

    while (1) {
        char c = *cursor;
        if (c == '\0' || c == ',' || c == '\t' || c == '\n' || c == '\r' ||
            c == ' ') {
            if (token_len > 0) {
                token_buf[token_len] = '\0';

                int64_t id1 = wl_intern_put(intern, token_buf);
                int64_t id2 = wl_intern_put(intern, token_buf);
                int64_t get_id = wl_intern_get(intern, token_buf);
                uint32_t count = wl_intern_count(intern);

                (void)get_id;
                (void)count;

                if (id1 >= 0 && id1 == id2 && id1 == get_id) {
                    const char *rev = wl_intern_reverse(intern, id1);
                    if (rev) {
                        volatile size_t rev_len = strlen(rev);
                        (void)rev_len;
                    }
                }

                token_len = 0;
            }
            if (c == '\0')
                break;
        } else {
            if (token_len < MAX_TOKEN_BYTES)
                token_buf[token_len++] = c;
        }
        cursor++;
    }
}

int
LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    size_t copy_len = size < MAX_INTERN_INPUT ? size : MAX_INTERN_INPUT;

    char *input = (char *)malloc(copy_len + 1);
    if (!input)
        return 0;

    memcpy(input, data, copy_len);
    input[copy_len] = '\0';

    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        free(input);
        return 0;
    }

    exercise_intern_tokens(intern, input);

    wl_intern_free(intern);
    free(input);
    return 0;
}
