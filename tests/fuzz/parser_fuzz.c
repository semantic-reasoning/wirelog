/*
 * parser_fuzz.c - libFuzzer entrypoint for the FlowLog parser.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * This is a parser-only fuzz target for Issue #743.
 */

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "../wirelog/parser/parser.h"

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    char errbuf[256] = {0};
    char *source = (char *)malloc(size + 1);
    if (!source) {
        return 0;
    }

    memcpy(source, data, size);
    source[size] = '\0';

    wl_parser_ast_node_t *ast = wl_parser_parse_string(source, errbuf,
            sizeof(errbuf));
    if (ast) {
        wl_parser_ast_node_free(ast);
    }

    free(source);
    return 0;
}
