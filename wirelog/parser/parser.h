/*
 * parser.h - wirelog Recursive Descent Parser Interface
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 */

#ifndef WIRELOG_PARSER_IMPL_H
#define WIRELOG_PARSER_IMPL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "ast.h"
#include "lexer.h"

/**
 * wl_parser_parse_string:
 * @source: Datalog program text
 * @error_buf: (out) (optional): Buffer for error message
 * @error_buf_size: Size of error buffer
 *
 * Parse a Datalog program from a string.
 *
 * Returns: (transfer full): AST root (WL_PARSER_AST_NODE_PROGRAM), or NULL on error
 */
wl_parser_ast_node_t *
wl_parser_parse_string(const char *source, char *error_buf,
                       size_t error_buf_size);

#ifdef __cplusplus
}
#endif

#endif /* WIRELOG_PARSER_IMPL_H */
