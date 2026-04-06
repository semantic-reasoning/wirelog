/*
 * parser.c - wirelog Recursive Descent Parser
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * Grammar (FlowLog-compatible):
 *
 *   program     = (declaration | input_dir | output_dir | printsize_dir | rule | fact)*
 *   declaration = ".decl" IDENT "(" attributes? ")"
 *   input_dir   = ".input" IDENT "(" input_params? ")"
 *   output_dir  = ".output" IDENT
 *   printsize   = ".printsize" IDENT
 *   rule        = head ":-" predicates "." ".plan"?
 *   head        = IDENT "(" head_args? ")"
 *   head_args   = head_arg ("," head_arg)*
 *   head_arg    = aggregate_expr | arithmetic_expr
 *   predicates  = predicate ("," predicate)*
 *   predicate   = atom | negative_atom | compare_expr | boolean
 *   atom        = IDENT "(" atom_args? ")"
 *   negative    = "!" atom
 *   atom_args   = atom_arg ("," atom_arg)*
 *   atom_arg    = IDENT | INTEGER | STRING | "_"
 *   arith_expr  = factor (arith_op factor)*
 *   factor      = IDENT | INTEGER | STRING
 *   compare     = arith_expr compare_op arith_expr
 *   aggregate   = agg_op "(" arith_expr ")"
 */

#include "parser.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Utility                                                                  */
/* ======================================================================== */

static char *
strdup_safe(const char *s)
{
    if (!s)
        return NULL;
    size_t len = strlen(s);
    char *dup = (char *)malloc(len + 1);
    if (dup) {
        memcpy(dup, s, len + 1);
    }
    return dup;
}

/* ======================================================================== */
/* Parser State                                                             */
/* ======================================================================== */

typedef struct {
    wl_parser_lexer_t lexer;
    wl_parser_lexer_token_t current;
    wl_parser_lexer_token_t previous;
    bool had_error;
    char error_msg[512];
} wl_parser_t;

/* ======================================================================== */
/* Error Handling                                                           */
/* ======================================================================== */

static void
parser_error(wl_parser_t *parser, const char *msg)
{
    if (parser->had_error)
        return;
    parser->had_error = true;
    snprintf(parser->error_msg, sizeof(parser->error_msg),
        "line %u, col %u: %s (got %s)", parser->current.line,
        parser->current.col, msg,
        wl_parser_lexer_token_type_str(parser->current.type));
}

/* ======================================================================== */
/* Token Consumption                                                        */
/* ======================================================================== */

static void
parser_advance(wl_parser_t *parser)
{
    parser->previous = parser->current;
    parser->current = wl_parser_lexer_next_token(&parser->lexer);

    if (parser->current.type == WL_PARSER_LEXER_TOK_ERROR) {
        parser_error(parser, parser->lexer.error_msg);
    }
}

static bool
parser_check(const wl_parser_t *parser, wl_parser_lexer_token_type_t type)
{
    return parser->current.type == type;
}

static bool
parser_match(wl_parser_t *parser, wl_parser_lexer_token_type_t type)
{
    if (!parser_check(parser, type))
        return false;
    parser_advance(parser);
    return true;
}

static bool
parser_consume(wl_parser_t *parser, wl_parser_lexer_token_type_t type,
    const char *msg)
{
    if (parser_check(parser, type)) {
        parser_advance(parser);
        return true;
    }
    parser_error(parser, msg);
    return false;
}

static char *
token_to_name(const wl_parser_lexer_token_t *token)
{
    char *s = (char *)malloc(token->length + 1);
    if (s) {
        memcpy(s, token->start, token->length);
        s[token->length] = '\0';
    }
    return s;
}

static char *
token_to_str_value(const wl_parser_lexer_token_t *token)
{
    /* Strip quotes from string token */
    if (token->length >= 2) {
        uint32_t inner = token->length - 2;
        char *s = (char *)malloc(inner + 1);
        if (s) {
            memcpy(s, token->start + 1, inner);
            s[inner] = '\0';
        }
        return s;
    }
    return (char *)calloc(1, 1);
}

/* ======================================================================== */
/* Forward Declarations                                                     */
/* ======================================================================== */

static wl_parser_ast_node_t *
parse_arithmetic_expr(wl_parser_t *parser);

/* ======================================================================== */
/* String Function Helpers                                                  */
/* ======================================================================== */

static wirelog_str_fn_t
token_to_str_fn(wl_parser_lexer_token_type_t type)
{
    switch (type) {
    case WL_PARSER_LEXER_TOK_STRLEN:      return WL_STR_FN_STRLEN;
    case WL_PARSER_LEXER_TOK_CAT:         return WL_STR_FN_CAT;
    case WL_PARSER_LEXER_TOK_SUBSTR:      return WL_STR_FN_SUBSTR;
    case WL_PARSER_LEXER_TOK_CONTAINS:    return WL_STR_FN_CONTAINS;
    case WL_PARSER_LEXER_TOK_STR_PREFIX:  return WL_STR_FN_STR_PREFIX;
    case WL_PARSER_LEXER_TOK_STR_SUFFIX:  return WL_STR_FN_STR_SUFFIX;
    case WL_PARSER_LEXER_TOK_STR_ORD:     return WL_STR_FN_STR_ORD;
    case WL_PARSER_LEXER_TOK_TO_UPPER:    return WL_STR_FN_TO_UPPER;
    case WL_PARSER_LEXER_TOK_TO_LOWER:    return WL_STR_FN_TO_LOWER;
    case WL_PARSER_LEXER_TOK_STR_REPLACE: return WL_STR_FN_STR_REPLACE;
    case WL_PARSER_LEXER_TOK_TRIM:        return WL_STR_FN_TRIM;
    case WL_PARSER_LEXER_TOK_TO_STRING:   return WL_STR_FN_TO_STRING;
    case WL_PARSER_LEXER_TOK_TO_NUMBER:   return WL_STR_FN_TO_NUMBER;
    default:                               return WL_STR_FN_STRLEN;
    }
}

static bool
is_string_fn_token(wl_parser_lexer_token_type_t type)
{
    return type == WL_PARSER_LEXER_TOK_STRLEN
           || type == WL_PARSER_LEXER_TOK_CAT
           || type == WL_PARSER_LEXER_TOK_SUBSTR
           || type == WL_PARSER_LEXER_TOK_CONTAINS
           || type == WL_PARSER_LEXER_TOK_STR_PREFIX
           || type == WL_PARSER_LEXER_TOK_STR_SUFFIX
           || type == WL_PARSER_LEXER_TOK_STR_ORD
           || type == WL_PARSER_LEXER_TOK_TO_UPPER
           || type == WL_PARSER_LEXER_TOK_TO_LOWER
           || type == WL_PARSER_LEXER_TOK_STR_REPLACE
           || type == WL_PARSER_LEXER_TOK_TRIM
           || type == WL_PARSER_LEXER_TOK_TO_STRING
           || type == WL_PARSER_LEXER_TOK_TO_NUMBER;
}

/* Parse a string function call: fn(arg [, arg]*) */
static wl_parser_ast_node_t *
parse_string_fn_expr(wl_parser_t *parser)
{
    uint32_t line = parser->current.line;
    uint32_t col = parser->current.col;
    wirelog_str_fn_t fn = token_to_str_fn(parser->current.type);
    parser_advance(parser); /* consume keyword */

    if (!parser_consume(parser, WL_PARSER_LEXER_TOK_LPAREN,
        "expected '(' after string function")) {
        return NULL;
    }

    wl_parser_ast_node_t *node
        = wl_parser_ast_node_create(WL_PARSER_AST_NODE_STR_FUNCTION, line, col);
    if (!node)
        return NULL;
    node->str_fn = fn;

    if (!parser_check(parser, WL_PARSER_LEXER_TOK_RPAREN)) {
        wl_parser_ast_node_t *arg = parse_arithmetic_expr(parser);
        if (!arg) {
            wl_parser_ast_node_free(node);
            return NULL;
        }
        wl_parser_ast_node_add_child(node, arg);

        while (parser_match(parser, WL_PARSER_LEXER_TOK_COMMA)) {
            arg = parse_arithmetic_expr(parser);
            if (!arg) {
                wl_parser_ast_node_free(node);
                return NULL;
            }
            wl_parser_ast_node_add_child(node, arg);
        }
    }

    if (!parser_consume(parser, WL_PARSER_LEXER_TOK_RPAREN,
        "expected ')' after string function arguments")) {
        wl_parser_ast_node_free(node);
        return NULL;
    }

    return node;
}

/* ======================================================================== */
/* Expression Parsing                                                       */
/* ======================================================================== */

static wl_parser_ast_node_t *
parse_factor(wl_parser_t *parser)
{
    if (parser->had_error)
        return NULL;

    uint32_t line = parser->current.line;
    uint32_t col = parser->current.col;

    if (parser_match(parser, WL_PARSER_LEXER_TOK_IDENT)) {
        wl_parser_ast_node_t *node
            = wl_parser_ast_node_create(WL_PARSER_AST_NODE_VARIABLE, line, col);
        node->name = token_to_name(&parser->previous);
        return node;
    }

    if (parser_match(parser, WL_PARSER_LEXER_TOK_INTEGER)) {
        wl_parser_ast_node_t *node
            = wl_parser_ast_node_create(WL_PARSER_AST_NODE_INTEGER, line, col);
        node->int_value = parser->previous.int_value;
        return node;
    }

    if (parser_match(parser, WL_PARSER_LEXER_TOK_STRING)) {
        wl_parser_ast_node_t *node
            = wl_parser_ast_node_create(WL_PARSER_AST_NODE_STRING, line, col);
        node->str_value = token_to_str_value(&parser->previous);
        return node;
    }

    /* Bitwise binary function calls: band(expr, expr), etc. */
    if (parser->current.type == WL_PARSER_LEXER_TOK_BAND
        || parser->current.type == WL_PARSER_LEXER_TOK_BOR
        || parser->current.type == WL_PARSER_LEXER_TOK_BXOR
        || parser->current.type == WL_PARSER_LEXER_TOK_BSHL
        || parser->current.type == WL_PARSER_LEXER_TOK_BSHR) {
        wirelog_arith_op_t op;
        switch (parser->current.type) {
        case WL_PARSER_LEXER_TOK_BAND:
            op = WIRELOG_ARITH_BAND;
            break;
        case WL_PARSER_LEXER_TOK_BOR:
            op = WIRELOG_ARITH_BOR;
            break;
        case WL_PARSER_LEXER_TOK_BXOR:
            op = WIRELOG_ARITH_BXOR;
            break;
        case WL_PARSER_LEXER_TOK_BSHL:
            op = WIRELOG_ARITH_SHL;
            break;
        default: /* BSHR */
            op = WIRELOG_ARITH_SHR;
            break;
        }
        parser_advance(parser); /* consume keyword */

        if (!parser_consume(parser, WL_PARSER_LEXER_TOK_LPAREN,
            "expected '(' after bitwise operator")) {
            return NULL;
        }
        wl_parser_ast_node_t *left_arg = parse_arithmetic_expr(parser);
        if (!left_arg)
            return NULL;
        if (!parser_consume(parser, WL_PARSER_LEXER_TOK_COMMA,
            "expected ',' in bitwise binary operator")) {
            wl_parser_ast_node_free(left_arg);
            return NULL;
        }
        wl_parser_ast_node_t *right_arg = parse_arithmetic_expr(parser);
        if (!right_arg) {
            wl_parser_ast_node_free(left_arg);
            return NULL;
        }
        if (!parser_consume(parser, WL_PARSER_LEXER_TOK_RPAREN,
            "expected ')' after bitwise operator arguments")) {
            wl_parser_ast_node_free(left_arg);
            wl_parser_ast_node_free(right_arg);
            return NULL;
        }
        wl_parser_ast_node_t *bin = wl_parser_ast_node_create(
            WL_PARSER_AST_NODE_BINARY_EXPR, line, col);
        bin->arith_op = op;
        wl_parser_ast_node_add_child(bin, left_arg);
        wl_parser_ast_node_add_child(bin, right_arg);
        return bin;
    }

    /* Bitwise unary: bnot(expr) */
    if (parser->current.type == WL_PARSER_LEXER_TOK_BNOT) {
        parser_advance(parser); /* consume bnot */
        if (!parser_consume(parser, WL_PARSER_LEXER_TOK_LPAREN,
            "expected '(' after bnot")) {
            return NULL;
        }
        wl_parser_ast_node_t *arg = parse_arithmetic_expr(parser);
        if (!arg)
            return NULL;
        if (!parser_consume(parser, WL_PARSER_LEXER_TOK_RPAREN,
            "expected ')' after bnot argument")) {
            wl_parser_ast_node_free(arg);
            return NULL;
        }
        wl_parser_ast_node_t *unary = wl_parser_ast_node_create(
            WL_PARSER_AST_NODE_BINARY_EXPR, line, col);
        unary->arith_op = WIRELOG_ARITH_BNOT;
        wl_parser_ast_node_add_child(unary, arg);
        return unary;
    }

    /* Hash function: hash(expr) */
    if (parser->current.type == WL_PARSER_LEXER_TOK_HASH) {
        parser_advance(parser); /* consume hash */
        if (!parser_consume(parser, WL_PARSER_LEXER_TOK_LPAREN,
            "expected '(' after hash")) {
            return NULL;
        }
        wl_parser_ast_node_t *arg = parse_arithmetic_expr(parser);
        if (!arg)
            return NULL;
        if (!parser_consume(parser, WL_PARSER_LEXER_TOK_RPAREN,
            "expected ')' after hash argument")) {
            wl_parser_ast_node_free(arg);
            return NULL;
        }
        wl_parser_ast_node_t *unary = wl_parser_ast_node_create(
            WL_PARSER_AST_NODE_BINARY_EXPR, line, col);
        unary->arith_op = WIRELOG_ARITH_HASH;
        wl_parser_ast_node_add_child(unary, arg);
        return unary;
    }

    /* MD5 hash function: md5(expr) */
    if (parser->current.type == WL_PARSER_LEXER_TOK_MD5) {
        parser_advance(parser); /* consume md5 */
        if (!parser_consume(parser, WL_PARSER_LEXER_TOK_LPAREN,
            "expected '(' after md5")) {
            return NULL;
        }
        wl_parser_ast_node_t *arg = parse_arithmetic_expr(parser);
        if (!arg)
            return NULL;
        if (!parser_consume(parser, WL_PARSER_LEXER_TOK_RPAREN,
            "expected ')' after md5 argument")) {
            wl_parser_ast_node_free(arg);
            return NULL;
        }
        wl_parser_ast_node_t *unary = wl_parser_ast_node_create(
            WL_PARSER_AST_NODE_BINARY_EXPR, line, col);
        unary->arith_op = WIRELOG_ARITH_MD5;
        wl_parser_ast_node_add_child(unary, arg);
        return unary;
    }

    /* SHA-1 hash function: sha1(expr) */
    if (parser->current.type == WL_PARSER_LEXER_TOK_SHA1) {
        parser_advance(parser); /* consume sha1 */
        if (!parser_consume(parser, WL_PARSER_LEXER_TOK_LPAREN,
            "expected '(' after sha1")) {
            return NULL;
        }
        wl_parser_ast_node_t *arg = parse_arithmetic_expr(parser);
        if (!arg)
            return NULL;
        if (!parser_consume(parser, WL_PARSER_LEXER_TOK_RPAREN,
            "expected ')' after sha1 argument")) {
            wl_parser_ast_node_free(arg);
            return NULL;
        }
        wl_parser_ast_node_t *unary = wl_parser_ast_node_create(
            WL_PARSER_AST_NODE_BINARY_EXPR, line, col);
        unary->arith_op = WIRELOG_ARITH_SHA1;
        wl_parser_ast_node_add_child(unary, arg);
        return unary;
    }

    /* SHA-256 hash function: sha256(expr) */
    if (parser->current.type == WL_PARSER_LEXER_TOK_SHA256) {
        parser_advance(parser); /* consume sha256 */
        if (!parser_consume(parser, WL_PARSER_LEXER_TOK_LPAREN,
            "expected '(' after sha256")) {
            return NULL;
        }
        wl_parser_ast_node_t *arg = parse_arithmetic_expr(parser);
        if (!arg)
            return NULL;
        if (!parser_consume(parser, WL_PARSER_LEXER_TOK_RPAREN,
            "expected ')' after sha256 argument")) {
            wl_parser_ast_node_free(arg);
            return NULL;
        }
        wl_parser_ast_node_t *unary = wl_parser_ast_node_create(
            WL_PARSER_AST_NODE_BINARY_EXPR, line, col);
        unary->arith_op = WIRELOG_ARITH_SHA256;
        wl_parser_ast_node_add_child(unary, arg);
        return unary;
    }

    /* SHA-512 hash function: sha512(expr) */
    if (parser->current.type == WL_PARSER_LEXER_TOK_SHA512) {
        parser_advance(parser); /* consume sha512 */
        if (!parser_consume(parser, WL_PARSER_LEXER_TOK_LPAREN,
            "expected '(' after sha512")) {
            return NULL;
        }
        wl_parser_ast_node_t *arg = parse_arithmetic_expr(parser);
        if (!arg)
            return NULL;
        if (!parser_consume(parser, WL_PARSER_LEXER_TOK_RPAREN,
            "expected ')' after sha512 argument")) {
            wl_parser_ast_node_free(arg);
            return NULL;
        }
        wl_parser_ast_node_t *unary = wl_parser_ast_node_create(
            WL_PARSER_AST_NODE_BINARY_EXPR, line, col);
        unary->arith_op = WIRELOG_ARITH_SHA512;
        wl_parser_ast_node_add_child(unary, arg);
        return unary;
    }

    /* HMAC-SHA-256 hash function: hmac_sha256(msg, key) */
    if (parser->current.type == WL_PARSER_LEXER_TOK_HMAC_SHA256) {
        parser_advance(parser); /* consume hmac_sha256 */
        if (!parser_consume(parser, WL_PARSER_LEXER_TOK_LPAREN,
            "expected '(' after hmac_sha256")) {
            return NULL;
        }
        wl_parser_ast_node_t *msg = parse_arithmetic_expr(parser);
        if (!msg)
            return NULL;
        if (!parser_consume(parser, WL_PARSER_LEXER_TOK_COMMA,
            "expected ',' between hmac_sha256 arguments")) {
            wl_parser_ast_node_free(msg);
            return NULL;
        }
        wl_parser_ast_node_t *key = parse_arithmetic_expr(parser);
        if (!key) {
            wl_parser_ast_node_free(msg);
            return NULL;
        }
        if (!parser_consume(parser, WL_PARSER_LEXER_TOK_RPAREN,
            "expected ')' after hmac_sha256 arguments")) {
            wl_parser_ast_node_free(msg);
            wl_parser_ast_node_free(key);
            return NULL;
        }
        wl_parser_ast_node_t *node = wl_parser_ast_node_create(
            WL_PARSER_AST_NODE_BINARY_EXPR, line, col);
        node->arith_op = WIRELOG_ARITH_HMAC_SHA256;
        wl_parser_ast_node_add_child(node, msg);
        wl_parser_ast_node_add_child(node, key);
        return node;
    }

    /* UUID4 function: uuid4() - nullary */
    if (parser->current.type == WL_PARSER_LEXER_TOK_UUID4) {
        parser_advance(parser); /* consume uuid4 */
        if (!parser_consume(parser, WL_PARSER_LEXER_TOK_LPAREN,
            "expected '(' after uuid4")) {
            return NULL;
        }
        if (!parser_consume(parser, WL_PARSER_LEXER_TOK_RPAREN,
            "expected ')' after uuid4")) {
            return NULL;
        }
        wl_parser_ast_node_t *node = wl_parser_ast_node_create(
            WL_PARSER_AST_NODE_BINARY_EXPR, line, col);
        node->arith_op = WIRELOG_ARITH_UUID4;
        /* nullary: no child added (0-arg function) */
        return node;
    }

    /* UUID5 function: uuid5(namespace, name) - binary */
    if (parser->current.type == WL_PARSER_LEXER_TOK_UUID5) {
        parser_advance(parser); /* consume uuid5 */
        if (!parser_consume(parser, WL_PARSER_LEXER_TOK_LPAREN,
            "expected '(' after uuid5")) {
            return NULL;
        }
        wl_parser_ast_node_t *ns = parse_arithmetic_expr(parser);
        if (!ns)
            return NULL;
        if (!parser_consume(parser, WL_PARSER_LEXER_TOK_COMMA,
            "expected ',' between uuid5 arguments")) {
            wl_parser_ast_node_free(ns);
            return NULL;
        }
        wl_parser_ast_node_t *name = parse_arithmetic_expr(parser);
        if (!name) {
            wl_parser_ast_node_free(ns);
            return NULL;
        }
        if (!parser_consume(parser, WL_PARSER_LEXER_TOK_RPAREN,
            "expected ')' after uuid5 arguments")) {
            wl_parser_ast_node_free(ns);
            wl_parser_ast_node_free(name);
            return NULL;
        }
        wl_parser_ast_node_t *node = wl_parser_ast_node_create(
            WL_PARSER_AST_NODE_BINARY_EXPR, line, col);
        node->arith_op = WIRELOG_ARITH_UUID5;
        wl_parser_ast_node_add_child(node, ns);
        wl_parser_ast_node_add_child(node, name);
        return node;
    }

    /* String function calls: strlen(expr), cat(expr, expr), etc. */
    if (is_string_fn_token(parser->current.type)) {
        return parse_string_fn_expr(parser);
    }

    parser_error(parser, "expected variable, integer, or string");
    return NULL;
}

static wirelog_arith_op_t
token_to_arith_op(wl_parser_lexer_token_type_t type)
{
    switch (type) {
    case WL_PARSER_LEXER_TOK_PLUS:
        return WIRELOG_ARITH_ADD;
    case WL_PARSER_LEXER_TOK_MINUS:
        return WIRELOG_ARITH_SUB;
    case WL_PARSER_LEXER_TOK_STAR:
        return WIRELOG_ARITH_MUL;
    case WL_PARSER_LEXER_TOK_SLASH:
        return WIRELOG_ARITH_DIV;
    case WL_PARSER_LEXER_TOK_PERCENT:
        return WIRELOG_ARITH_MOD;
    default:
        return WIRELOG_ARITH_ADD;
    }
}

static bool
is_arith_op(wl_parser_lexer_token_type_t type)
{
    return type == WL_PARSER_LEXER_TOK_PLUS || type == WL_PARSER_LEXER_TOK_MINUS
           || type == WL_PARSER_LEXER_TOK_STAR
           || type == WL_PARSER_LEXER_TOK_SLASH
           || type == WL_PARSER_LEXER_TOK_PERCENT;
}

static bool
is_bitwise_token(wl_parser_lexer_token_type_t type)
{
    return type == WL_PARSER_LEXER_TOK_BAND || type == WL_PARSER_LEXER_TOK_BOR
           || type == WL_PARSER_LEXER_TOK_BXOR
           || type == WL_PARSER_LEXER_TOK_BNOT
           || type == WL_PARSER_LEXER_TOK_BSHL
           || type == WL_PARSER_LEXER_TOK_BSHR
           || type == WL_PARSER_LEXER_TOK_HASH
           || type == WL_PARSER_LEXER_TOK_MD5
           || type == WL_PARSER_LEXER_TOK_SHA1
           || type == WL_PARSER_LEXER_TOK_SHA256
           || type == WL_PARSER_LEXER_TOK_SHA512
           || type == WL_PARSER_LEXER_TOK_HMAC_SHA256
           || type == WL_PARSER_LEXER_TOK_UUID4
           || type == WL_PARSER_LEXER_TOK_UUID5;
}

static wl_parser_ast_node_t *
parse_arithmetic_expr(wl_parser_t *parser)
{
    wl_parser_ast_node_t *left = parse_factor(parser);
    if (!left || parser->had_error)
        return left;

    while (is_arith_op(parser->current.type)) {
        uint32_t line = parser->current.line;
        uint32_t col = parser->current.col;
        wirelog_arith_op_t op = token_to_arith_op(parser->current.type);
        parser_advance(parser);

        wl_parser_ast_node_t *right = parse_factor(parser);
        if (!right) {
            wl_parser_ast_node_free(left);
            return NULL;
        }

        wl_parser_ast_node_t *bin = wl_parser_ast_node_create(
            WL_PARSER_AST_NODE_BINARY_EXPR, line, col);
        bin->arith_op = op;
        wl_parser_ast_node_add_child(bin, left);
        wl_parser_ast_node_add_child(bin, right);
        left = bin;
    }

    return left;
}

static wirelog_agg_fn_t
token_to_agg_fn(wl_parser_lexer_token_type_t type)
{
    switch (type) {
    case WL_PARSER_LEXER_TOK_COUNT:
        return WIRELOG_AGG_COUNT;
    case WL_PARSER_LEXER_TOK_SUM:
        return WIRELOG_AGG_SUM;
    case WL_PARSER_LEXER_TOK_MIN:
        return WIRELOG_AGG_MIN;
    case WL_PARSER_LEXER_TOK_MAX:
        return WIRELOG_AGG_MAX;
    case WL_PARSER_LEXER_TOK_AVG:
        return WIRELOG_AGG_AVG;
    default:
        return WIRELOG_AGG_COUNT;
    }
}

static bool
is_aggregate_token(wl_parser_lexer_token_type_t type)
{
    return type == WL_PARSER_LEXER_TOK_COUNT || type == WL_PARSER_LEXER_TOK_SUM
           || type == WL_PARSER_LEXER_TOK_MIN || type == WL_PARSER_LEXER_TOK_MAX
           || type == WL_PARSER_LEXER_TOK_AVG;
}

static wl_parser_ast_node_t *
parse_aggregate_expr(wl_parser_t *parser)
{
    uint32_t line = parser->current.line;
    uint32_t col = parser->current.col;
    wirelog_agg_fn_t fn = token_to_agg_fn(parser->current.type);
    parser_advance(parser); /* consume aggregate keyword */

    if (!parser_consume(parser, WL_PARSER_LEXER_TOK_LPAREN,
        "expected '(' after aggregate")) {
        return NULL;
    }

    wl_parser_ast_node_t *expr = parse_arithmetic_expr(parser);
    if (!expr)
        return NULL;

    if (!parser_consume(parser, WL_PARSER_LEXER_TOK_RPAREN,
        "expected ')' after aggregate expr")) {
        wl_parser_ast_node_free(expr);
        return NULL;
    }

    wl_parser_ast_node_t *agg
        = wl_parser_ast_node_create(WL_PARSER_AST_NODE_AGGREGATE, line, col);
    agg->agg_fn = fn;
    wl_parser_ast_node_add_child(agg, expr);
    return agg;
}

/* ======================================================================== */
/* Head Argument Parsing                                                    */
/* ======================================================================== */

static wl_parser_ast_node_t *
parse_head_arg(wl_parser_t *parser)
{
    if (is_aggregate_token(parser->current.type)) {
        return parse_aggregate_expr(parser);
    }
    return parse_arithmetic_expr(parser);
}

/* ======================================================================== */
/* Atom Argument Parsing                                                    */
/* ======================================================================== */

static wl_parser_ast_node_t *
parse_atom_arg(wl_parser_t *parser)
{
    uint32_t line = parser->current.line;
    uint32_t col = parser->current.col;

    if (parser_match(parser, WL_PARSER_LEXER_TOK_UNDERSCORE)) {
        return wl_parser_ast_node_create(WL_PARSER_AST_NODE_WILDCARD, line,
                   col);
    }

    if (parser_match(parser, WL_PARSER_LEXER_TOK_IDENT)) {
        wl_parser_ast_node_t *node
            = wl_parser_ast_node_create(WL_PARSER_AST_NODE_VARIABLE, line, col);
        node->name = token_to_name(&parser->previous);
        return node;
    }

    if (parser_match(parser, WL_PARSER_LEXER_TOK_INTEGER)) {
        wl_parser_ast_node_t *node
            = wl_parser_ast_node_create(WL_PARSER_AST_NODE_INTEGER, line, col);
        node->int_value = parser->previous.int_value;
        return node;
    }

    if (parser_match(parser, WL_PARSER_LEXER_TOK_STRING)) {
        wl_parser_ast_node_t *node
            = wl_parser_ast_node_create(WL_PARSER_AST_NODE_STRING, line, col);
        node->str_value = token_to_str_value(&parser->previous);
        return node;
    }

    parser_error(parser, "expected atom argument");
    return NULL;
}

/* ======================================================================== */
/* Atom Parsing                                                             */
/* ======================================================================== */

static wl_parser_ast_node_t *
parse_atom(wl_parser_t *parser)
{
    uint32_t line = parser->previous.line;
    uint32_t col = parser->previous.col;

    /* IDENT already consumed */
    wl_parser_ast_node_t *atom
        = wl_parser_ast_node_create(WL_PARSER_AST_NODE_ATOM, line, col);
    atom->name = token_to_name(&parser->previous);

    if (!parser_consume(parser, WL_PARSER_LEXER_TOK_LPAREN,
        "expected '(' after relation name")) {
        wl_parser_ast_node_free(atom);
        return NULL;
    }

    /* Parse arguments */
    if (!parser_check(parser, WL_PARSER_LEXER_TOK_RPAREN)) {
        wl_parser_ast_node_t *arg = parse_atom_arg(parser);
        if (!arg) {
            wl_parser_ast_node_free(atom);
            return NULL;
        }
        wl_parser_ast_node_add_child(atom, arg);

        while (parser_match(parser, WL_PARSER_LEXER_TOK_COMMA)) {
            arg = parse_atom_arg(parser);
            if (!arg) {
                wl_parser_ast_node_free(atom);
                return NULL;
            }
            wl_parser_ast_node_add_child(atom, arg);
        }
    }

    if (!parser_consume(parser, WL_PARSER_LEXER_TOK_RPAREN,
        "expected ')' after arguments")) {
        wl_parser_ast_node_free(atom);
        return NULL;
    }

    return atom;
}

/* ======================================================================== */
/* Predicate Parsing                                                        */
/* ======================================================================== */

static bool
is_compare_op(wl_parser_lexer_token_type_t type)
{
    return type == WL_PARSER_LEXER_TOK_EQ || type == WL_PARSER_LEXER_TOK_NEQ
           || type == WL_PARSER_LEXER_TOK_LT || type == WL_PARSER_LEXER_TOK_GT
           || type == WL_PARSER_LEXER_TOK_LTE
           || type == WL_PARSER_LEXER_TOK_GTE;
}

static wirelog_cmp_op_t
token_to_cmp_op(wl_parser_lexer_token_type_t type)
{
    switch (type) {
    case WL_PARSER_LEXER_TOK_EQ:
        return WIRELOG_CMP_EQ;
    case WL_PARSER_LEXER_TOK_NEQ:
        return WIRELOG_CMP_NEQ;
    case WL_PARSER_LEXER_TOK_LT:
        return WIRELOG_CMP_LT;
    case WL_PARSER_LEXER_TOK_GT:
        return WIRELOG_CMP_GT;
    case WL_PARSER_LEXER_TOK_LTE:
        return WIRELOG_CMP_LTE;
    case WL_PARSER_LEXER_TOK_GTE:
        return WIRELOG_CMP_GTE;
    default:
        return WIRELOG_CMP_EQ;
    }
}

static wl_parser_ast_node_t *
parse_predicate(wl_parser_t *parser)
{
    if (parser->had_error)
        return NULL;

    uint32_t line = parser->current.line;
    uint32_t col = parser->current.col;

    /* Boolean predicate */
    if (parser_match(parser, WL_PARSER_LEXER_TOK_TRUE)) {
        wl_parser_ast_node_t *node
            = wl_parser_ast_node_create(WL_PARSER_AST_NODE_BOOLEAN, line, col);
        node->bool_value = true;
        return node;
    }
    if (parser_match(parser, WL_PARSER_LEXER_TOK_FALSE)) {
        wl_parser_ast_node_t *node
            = wl_parser_ast_node_create(WL_PARSER_AST_NODE_BOOLEAN, line, col);
        node->bool_value = false;
        return node;
    }

    /* Negative atom */
    if (parser_match(parser, WL_PARSER_LEXER_TOK_BANG)) {
        if (!parser_consume(parser, WL_PARSER_LEXER_TOK_IDENT,
            "expected relation name after '!'")) {
            return NULL;
        }
        wl_parser_ast_node_t *atom = parse_atom(parser);
        if (!atom)
            return NULL;

        wl_parser_ast_node_t *neg
            = wl_parser_ast_node_create(WL_PARSER_AST_NODE_NEGATION, line, col);
        wl_parser_ast_node_add_child(neg, atom);
        return neg;
    }

    /* Atom or comparison (both start with IDENT) */
    if (parser_match(parser, WL_PARSER_LEXER_TOK_IDENT)) {
        /* Check if followed by '(' => atom */
        if (parser_check(parser, WL_PARSER_LEXER_TOK_LPAREN)) {
            return parse_atom(parser);
        }

        /* Otherwise it's a comparison: arith_expr compare_op arith_expr
        * We already consumed the identifier, so build the left side. */
        wl_parser_ast_node_t *left_var = wl_parser_ast_node_create(
            WL_PARSER_AST_NODE_VARIABLE, parser->previous.line,
            parser->previous.col);
        left_var->name = token_to_name(&parser->previous);

        /* Build left arithmetic expression */
        wl_parser_ast_node_t *left = left_var;
        while (is_arith_op(parser->current.type)) {
            uint32_t op_line = parser->current.line;
            uint32_t op_col = parser->current.col;
            wirelog_arith_op_t op = token_to_arith_op(parser->current.type);
            parser_advance(parser);
            wl_parser_ast_node_t *right_factor = parse_factor(parser);
            if (!right_factor) {
                wl_parser_ast_node_free(left);
                return NULL;
            }
            wl_parser_ast_node_t *bin = wl_parser_ast_node_create(
                WL_PARSER_AST_NODE_BINARY_EXPR, op_line, op_col);
            bin->arith_op = op;
            wl_parser_ast_node_add_child(bin, left);
            wl_parser_ast_node_add_child(bin, right_factor);
            left = bin;
        }

        if (!is_compare_op(parser->current.type)) {
            parser_error(parser, "expected comparison operator");
            wl_parser_ast_node_free(left);
            return NULL;
        }

        wirelog_cmp_op_t cmp_op = token_to_cmp_op(parser->current.type);
        uint32_t cmp_line = parser->current.line;
        uint32_t cmp_col = parser->current.col;
        parser_advance(parser);

        wl_parser_ast_node_t *right = parse_arithmetic_expr(parser);
        if (!right) {
            wl_parser_ast_node_free(left);
            return NULL;
        }

        wl_parser_ast_node_t *cmp = wl_parser_ast_node_create(
            WL_PARSER_AST_NODE_COMPARISON, cmp_line, cmp_col);
        cmp->cmp_op = cmp_op;
        wl_parser_ast_node_add_child(cmp, left);
        wl_parser_ast_node_add_child(cmp, right);
        return cmp;
    }

    /* Comparison starting with bitwise expression: band(x,y) = z, etc. */
    if (is_bitwise_token(parser->current.type)) {
        wl_parser_ast_node_t *left = parse_arithmetic_expr(parser);
        if (!left)
            return NULL;

        if (!is_compare_op(parser->current.type)) {
            parser_error(
                parser,
                "expected comparison operator after bitwise expression");
            wl_parser_ast_node_free(left);
            return NULL;
        }

        wirelog_cmp_op_t cmp_op = token_to_cmp_op(parser->current.type);
        uint32_t cmp_line = parser->current.line;
        uint32_t cmp_col = parser->current.col;
        parser_advance(parser);

        wl_parser_ast_node_t *right = parse_arithmetic_expr(parser);
        if (!right) {
            wl_parser_ast_node_free(left);
            return NULL;
        }

        wl_parser_ast_node_t *cmp = wl_parser_ast_node_create(
            WL_PARSER_AST_NODE_COMPARISON, cmp_line, cmp_col);
        cmp->cmp_op = cmp_op;
        wl_parser_ast_node_add_child(cmp, left);
        wl_parser_ast_node_add_child(cmp, right);
        return cmp;
    }

    /* Comparison starting with integer constant */
    if (parser_check(parser, WL_PARSER_LEXER_TOK_INTEGER)) {
        wl_parser_ast_node_t *left = parse_arithmetic_expr(parser);
        if (!left)
            return NULL;

        if (!is_compare_op(parser->current.type)) {
            parser_error(parser, "expected comparison operator");
            wl_parser_ast_node_free(left);
            return NULL;
        }

        wirelog_cmp_op_t cmp_op = token_to_cmp_op(parser->current.type);
        uint32_t cmp_line = parser->current.line;
        uint32_t cmp_col = parser->current.col;
        parser_advance(parser);

        wl_parser_ast_node_t *right = parse_arithmetic_expr(parser);
        if (!right) {
            wl_parser_ast_node_free(left);
            return NULL;
        }

        wl_parser_ast_node_t *cmp = wl_parser_ast_node_create(
            WL_PARSER_AST_NODE_COMPARISON, cmp_line, cmp_col);
        cmp->cmp_op = cmp_op;
        wl_parser_ast_node_add_child(cmp, left);
        wl_parser_ast_node_add_child(cmp, right);
        return cmp;
    }

    /* String function as body predicate (contains, str_prefix, str_suffix)
     * or as left-hand side of a comparison: strlen(x) > 5 */
    if (is_string_fn_token(parser->current.type)) {
        wl_parser_ast_node_t *left = parse_arithmetic_expr(parser);
        if (!left)
            return NULL;

        if (is_compare_op(parser->current.type)) {
            wirelog_cmp_op_t cmp_op = token_to_cmp_op(parser->current.type);
            uint32_t cmp_line = parser->current.line;
            uint32_t cmp_col = parser->current.col;
            parser_advance(parser);

            wl_parser_ast_node_t *right = parse_arithmetic_expr(parser);
            if (!right) {
                wl_parser_ast_node_free(left);
                return NULL;
            }

            wl_parser_ast_node_t *cmp = wl_parser_ast_node_create(
                WL_PARSER_AST_NODE_COMPARISON, cmp_line, cmp_col);
            cmp->cmp_op = cmp_op;
            wl_parser_ast_node_add_child(cmp, left);
            wl_parser_ast_node_add_child(cmp, right);
            return cmp;
        }

        /* Standalone boolean string predicate: contains(x, y), str_prefix(x, y), etc. */
        return left;
    }

    parser_error(parser, "expected predicate");
    return NULL;
}

/* ======================================================================== */
/* Head Parsing                                                             */
/* ======================================================================== */

static wl_parser_ast_node_t *
parse_head(wl_parser_t *parser)
{
    uint32_t line = parser->current.line;
    uint32_t col = parser->current.col;

    if (!parser_consume(parser, WL_PARSER_LEXER_TOK_IDENT,
        "expected relation name in head")) {
        return NULL;
    }

    wl_parser_ast_node_t *head
        = wl_parser_ast_node_create(WL_PARSER_AST_NODE_HEAD, line, col);
    head->name = token_to_name(&parser->previous);

    if (!parser_consume(parser, WL_PARSER_LEXER_TOK_LPAREN,
        "expected '(' after head name")) {
        wl_parser_ast_node_free(head);
        return NULL;
    }

    /* Parse head arguments */
    if (!parser_check(parser, WL_PARSER_LEXER_TOK_RPAREN)) {
        wl_parser_ast_node_t *arg = parse_head_arg(parser);
        if (!arg) {
            wl_parser_ast_node_free(head);
            return NULL;
        }
        wl_parser_ast_node_add_child(head, arg);

        while (parser_match(parser, WL_PARSER_LEXER_TOK_COMMA)) {
            arg = parse_head_arg(parser);
            if (!arg) {
                wl_parser_ast_node_free(head);
                return NULL;
            }
            wl_parser_ast_node_add_child(head, arg);
        }
    }

    if (!parser_consume(parser, WL_PARSER_LEXER_TOK_RPAREN,
        "expected ')' in head")) {
        wl_parser_ast_node_free(head);
        return NULL;
    }

    return head;
}

/* ======================================================================== */
/* Fact / Rule Parsing                                                      */
/* ======================================================================== */

static wl_parser_ast_node_t *
parse_fact(wl_parser_t *parser, wl_parser_ast_node_t *head)
{
    /* Convert HEAD node to FACT node, reusing name and children */
    wl_parser_ast_node_t *fact = wl_parser_ast_node_create(
        WL_PARSER_AST_NODE_FACT, head->line, head->col);
    fact->name = head->name;
    head->name = NULL;

    /* Move children from head to fact */
    fact->children = head->children;
    fact->child_count = head->child_count;
    fact->child_capacity = head->child_capacity;
    head->children = NULL;
    head->child_count = 0;
    head->child_capacity = 0;

    wl_parser_ast_node_free(head);

    /* Validate: fact arguments must be constants (INTEGER or STRING) */
    for (uint32_t i = 0; i < fact->child_count; i++) {
        wl_parser_ast_node_type_t arg_type = fact->children[i]->type;
        if (arg_type != WL_PARSER_AST_NODE_INTEGER
            && arg_type != WL_PARSER_AST_NODE_STRING) {
            parser_error(
                parser, "fact arguments must be constants (integer or string)");
            wl_parser_ast_node_free(fact);
            return NULL;
        }
    }

    if (!parser_consume(parser, WL_PARSER_LEXER_TOK_DOT,
        "expected '.' at end of fact")) {
        wl_parser_ast_node_free(fact);
        return NULL;
    }

    return fact;
}

static wl_parser_ast_node_t *
parse_rule_or_fact(wl_parser_t *parser)
{
    uint32_t line = parser->current.line;
    uint32_t col = parser->current.col;

    wl_parser_ast_node_t *head = parse_head(parser);
    if (!head)
        return NULL;

    /* Fact: head followed by '.' */
    if (parser_check(parser, WL_PARSER_LEXER_TOK_DOT)) {
        return parse_fact(parser, head);
    }

    /* Rule: head followed by ':-' */
    if (!parser_consume(parser, WL_PARSER_LEXER_TOK_HORN,
        "expected ':-' or '.' after head")) {
        wl_parser_ast_node_free(head);
        return NULL;
    }

    wl_parser_ast_node_t *rule
        = wl_parser_ast_node_create(WL_PARSER_AST_NODE_RULE, line, col);
    wl_parser_ast_node_add_child(rule, head);

    /* Parse body predicates */
    wl_parser_ast_node_t *pred = parse_predicate(parser);
    if (!pred) {
        wl_parser_ast_node_free(rule);
        return NULL;
    }
    wl_parser_ast_node_add_child(rule, pred);

    while (parser_match(parser, WL_PARSER_LEXER_TOK_COMMA)) {
        pred = parse_predicate(parser);
        if (!pred) {
            wl_parser_ast_node_free(rule);
            return NULL;
        }
        wl_parser_ast_node_add_child(rule, pred);
    }

    if (!parser_consume(parser, WL_PARSER_LEXER_TOK_DOT,
        "expected '.' at end of rule")) {
        wl_parser_ast_node_free(rule);
        return NULL;
    }

    /* Optional .plan marker */
    if (parser_match(parser, WL_PARSER_LEXER_TOK_PLAN)) {
        rule->is_planning = true;
    }

    return rule;
}

/* ======================================================================== */
/* Declaration Parsing                                                      */
/* ======================================================================== */

static wl_parser_ast_node_t *
parse_declaration(wl_parser_t *parser)
{
    /* .decl already consumed */
    uint32_t line = parser->previous.line;
    uint32_t col = parser->previous.col;

    if (!parser_consume(parser, WL_PARSER_LEXER_TOK_IDENT,
        "expected relation name after .decl")) {
        return NULL;
    }

    wl_parser_ast_node_t *decl
        = wl_parser_ast_node_create(WL_PARSER_AST_NODE_DECL, line, col);
    decl->name = token_to_name(&parser->previous);

    if (!parser_consume(parser, WL_PARSER_LEXER_TOK_LPAREN,
        "expected '(' after .decl name")) {
        wl_parser_ast_node_free(decl);
        return NULL;
    }

    /* Parse attribute declarations: name : type */
    if (!parser_check(parser, WL_PARSER_LEXER_TOK_RPAREN)) {
        for (;;) {
            uint32_t attr_line = parser->current.line;
            uint32_t attr_col = parser->current.col;

            if (!parser_consume(parser, WL_PARSER_LEXER_TOK_IDENT,
                "expected attribute name")) {
                wl_parser_ast_node_free(decl);
                return NULL;
            }
            char *attr_name = token_to_name(&parser->previous);

            if (!parser_consume(parser, WL_PARSER_LEXER_TOK_COLON,
                "expected ':' after attribute name")) {
                free(attr_name);
                wl_parser_ast_node_free(decl);
                return NULL;
            }

            /* Type: int32, int64, string, or symbol */
            const char *type_str = NULL;
            if (parser_match(parser, WL_PARSER_LEXER_TOK_INT32)) {
                type_str = "int32";
            } else if (parser_match(parser, WL_PARSER_LEXER_TOK_INT64)) {
                type_str = "int64";
            } else if (parser_match(parser, WL_PARSER_LEXER_TOK_STRING_TYPE)) {
                type_str = "string";
            } else if (parser_match(parser, WL_PARSER_LEXER_TOK_SYMBOL_TYPE)) {
                type_str = "symbol";
            } else {
                parser_error(parser,
                    "expected type (int32, int64, string, or symbol)");
                free(attr_name);
                wl_parser_ast_node_free(decl);
                return NULL;
            }

            wl_parser_ast_node_t *param = wl_parser_ast_node_create(
                WL_PARSER_AST_NODE_TYPED_PARAM, attr_line, attr_col);
            param->name = attr_name;
            param->type_name = strdup_safe(type_str);
            wl_parser_ast_node_add_child(decl, param);

            if (!parser_match(parser, WL_PARSER_LEXER_TOK_COMMA))
                break;
        }
    }

    if (!parser_consume(parser, WL_PARSER_LEXER_TOK_RPAREN,
        "expected ')' after .decl")) {
        wl_parser_ast_node_free(decl);
        return NULL;
    }

    return decl;
}

static wl_parser_ast_node_t *
parse_input_directive(wl_parser_t *parser)
{
    /* .input already consumed */
    uint32_t line = parser->previous.line;
    uint32_t col = parser->previous.col;

    if (!parser_consume(parser, WL_PARSER_LEXER_TOK_IDENT,
        "expected relation name after .input")) {
        return NULL;
    }

    wl_parser_ast_node_t *input
        = wl_parser_ast_node_create(WL_PARSER_AST_NODE_INPUT, line, col);
    input->name = token_to_name(&parser->previous);

    if (!parser_consume(parser, WL_PARSER_LEXER_TOK_LPAREN,
        "expected '(' after .input name")) {
        wl_parser_ast_node_free(input);
        return NULL;
    }

    /* Parse key=value parameters */
    if (!parser_check(parser, WL_PARSER_LEXER_TOK_RPAREN)) {
        for (;;) {
            uint32_t p_line = parser->current.line;
            uint32_t p_col = parser->current.col;

            if (!parser_consume(parser, WL_PARSER_LEXER_TOK_IDENT,
                "expected parameter name")) {
                wl_parser_ast_node_free(input);
                return NULL;
            }
            char *param_name = token_to_name(&parser->previous);

            if (!parser_consume(parser, WL_PARSER_LEXER_TOK_EQ,
                "expected '=' after parameter name")) {
                free(param_name);
                wl_parser_ast_node_free(input);
                return NULL;
            }

            if (!parser_consume(parser, WL_PARSER_LEXER_TOK_STRING,
                "expected string value")) {
                free(param_name);
                wl_parser_ast_node_free(input);
                return NULL;
            }
            char *param_value = token_to_str_value(&parser->previous);

            wl_parser_ast_node_t *param = wl_parser_ast_node_create(
                WL_PARSER_AST_NODE_INPUT_PARAM, p_line, p_col);
            param->name = param_name;
            param->str_value = param_value;
            wl_parser_ast_node_add_child(input, param);

            if (!parser_match(parser, WL_PARSER_LEXER_TOK_COMMA))
                break;
        }
    }

    if (!parser_consume(parser, WL_PARSER_LEXER_TOK_RPAREN,
        "expected ')' after .input params")) {
        wl_parser_ast_node_free(input);
        return NULL;
    }

    return input;
}

static wl_parser_ast_node_t *
parse_output_directive(wl_parser_t *parser)
{
    uint32_t line = parser->previous.line;
    uint32_t col = parser->previous.col;

    if (!parser_consume(parser, WL_PARSER_LEXER_TOK_IDENT,
        "expected relation name after .output")) {
        return NULL;
    }

    wl_parser_ast_node_t *output
        = wl_parser_ast_node_create(WL_PARSER_AST_NODE_OUTPUT, line, col);
    output->name = token_to_name(&parser->previous);

    /* Optional: (.output relation(key="value", ...)) */
    if (parser_match(parser, WL_PARSER_LEXER_TOK_LPAREN)) {
        if (!parser_check(parser, WL_PARSER_LEXER_TOK_RPAREN)) {
            for (;;) {
                uint32_t p_line = parser->current.line;
                uint32_t p_col = parser->current.col;

                if (!parser_consume(parser, WL_PARSER_LEXER_TOK_IDENT,
                    "expected parameter name")) {
                    wl_parser_ast_node_free(output);
                    return NULL;
                }
                char *param_name = token_to_name(&parser->previous);

                if (!parser_consume(parser, WL_PARSER_LEXER_TOK_EQ,
                    "expected '=' after parameter name")) {
                    free(param_name);
                    wl_parser_ast_node_free(output);
                    return NULL;
                }

                if (!parser_consume(parser, WL_PARSER_LEXER_TOK_STRING,
                    "expected string value")) {
                    free(param_name);
                    wl_parser_ast_node_free(output);
                    return NULL;
                }
                char *param_value = token_to_str_value(&parser->previous);

                wl_parser_ast_node_t *param = wl_parser_ast_node_create(
                    WL_PARSER_AST_NODE_OUTPUT_PARAM, p_line, p_col);
                param->name = param_name;
                param->str_value = param_value;
                wl_parser_ast_node_add_child(output, param);

                if (!parser_match(parser, WL_PARSER_LEXER_TOK_COMMA))
                    break;
            }
        }

        if (!parser_consume(parser, WL_PARSER_LEXER_TOK_RPAREN,
            "expected ')' after .output params")) {
            wl_parser_ast_node_free(output);
            return NULL;
        }
    }

    return output;
}

static wl_parser_ast_node_t *
parse_printsize_directive(wl_parser_t *parser)
{
    uint32_t line = parser->previous.line;
    uint32_t col = parser->previous.col;

    if (!parser_consume(parser, WL_PARSER_LEXER_TOK_IDENT,
        "expected relation name after .printsize")) {
        return NULL;
    }

    wl_parser_ast_node_t *ps
        = wl_parser_ast_node_create(WL_PARSER_AST_NODE_PRINTSIZE, line, col);
    ps->name = token_to_name(&parser->previous);
    return ps;
}

/* ======================================================================== */
/* Query Directive Parsing                                                  */
/* ======================================================================== */

static wl_parser_ast_node_t *
parse_query_directive(wl_parser_t *parser)
{
    /* .query already consumed */
    uint32_t line = parser->previous.line;
    uint32_t col = parser->previous.col;

    if (!parser_consume(parser, WL_PARSER_LEXER_TOK_IDENT,
        "expected relation name after .query")) {
        return NULL;
    }

    wl_parser_ast_node_t *query
        = wl_parser_ast_node_create(WL_PARSER_AST_NODE_QUERY, line, col);
    if (!query)
        return NULL;
    query->name = token_to_name(&parser->previous);

    if (!parser_consume(parser, WL_PARSER_LEXER_TOK_LPAREN,
        "expected '(' after .query relation name")) {
        wl_parser_ast_node_free(query);
        return NULL;
    }

    /* Parse comma-separated adornments: b (bound) or f (free) */
    uint64_t bound_mask = 0;
    uint32_t position = 0;

    if (!parser_check(parser, WL_PARSER_LEXER_TOK_RPAREN)) {
        for (;;) {
            if (!parser_consume(parser, WL_PARSER_LEXER_TOK_IDENT,
                "expected 'b' or 'f' adornment")) {
                wl_parser_ast_node_free(query);
                return NULL;
            }

            if (position >= 64) {
                parser_error(parser, "maximum 64 adornments supported");
                wl_parser_ast_node_free(query);
                return NULL;
            }

            const wl_parser_lexer_token_t *tok = &parser->previous;
            if (tok->length == 1 && tok->start[0] == 'b') {
                bound_mask |= ((uint64_t)1 << position);
            } else if (tok->length == 1 && tok->start[0] == 'f') {
                /* free position: bit stays 0 */
            } else {
                parser_error(parser,
                    "adornment must be 'b' (bound) or 'f' (free)");
                wl_parser_ast_node_free(query);
                return NULL;
            }

            position++;

            if (!parser_match(parser, WL_PARSER_LEXER_TOK_COMMA))
                break;
        }
    }

    if (!parser_consume(parser, WL_PARSER_LEXER_TOK_RPAREN,
        "expected ')' after .query adornments")) {
        wl_parser_ast_node_free(query);
        return NULL;
    }

    if (!parser_consume(parser, WL_PARSER_LEXER_TOK_DOT,
        "expected '.' after .query directive")) {
        wl_parser_ast_node_free(query);
        return NULL;
    }

    query->int_value = (int64_t)bound_mask;
    query->query_arity = position;
    return query;
}

/* ======================================================================== */
/* Program Parsing                                                          */
/* ======================================================================== */

static wl_parser_ast_node_t *
parse_program(wl_parser_t *parser)
{
    wl_parser_ast_node_t *program
        = wl_parser_ast_node_create(WL_PARSER_AST_NODE_PROGRAM, 1, 1);
    if (!program)
        return NULL;

    while (!parser_check(parser, WL_PARSER_LEXER_TOK_EOF)
        && !parser->had_error) {
        wl_parser_ast_node_t *node = NULL;

        if (parser_match(parser, WL_PARSER_LEXER_TOK_DECL)) {
            node = parse_declaration(parser);
        } else if (parser_match(parser, WL_PARSER_LEXER_TOK_INPUT)) {
            node = parse_input_directive(parser);
        } else if (parser_match(parser, WL_PARSER_LEXER_TOK_OUTPUT)) {
            node = parse_output_directive(parser);
        } else if (parser_match(parser, WL_PARSER_LEXER_TOK_PRINTSIZE)) {
            node = parse_printsize_directive(parser);
        } else if (parser_match(parser, WL_PARSER_LEXER_TOK_QUERY)) {
            node = parse_query_directive(parser);
        } else if (parser_check(parser, WL_PARSER_LEXER_TOK_IDENT)) {
            /* Fact or rule: both start with identifier */
            node = parse_rule_or_fact(parser);
        } else {
            parser_error(parser, "expected declaration, directive, or rule");
            break;
        }

        if (node) {
            wl_parser_ast_node_add_child(program, node);
        } else if (parser->had_error) {
            wl_parser_ast_node_free(program);
            return NULL;
        }
    }

    if (parser->had_error) {
        wl_parser_ast_node_free(program);
        return NULL;
    }

    return program;
}

/* ======================================================================== */
/* Public API                                                               */
/* ======================================================================== */

wl_parser_ast_node_t *
wl_parser_parse_string(const char *source, char *error_buf,
    size_t error_buf_size)
{
    wl_parser_t parser;
    memset(&parser, 0, sizeof(parser));

    wl_parser_lexer_init(&parser.lexer, source);
    parser.had_error = false;
    parser.error_msg[0] = '\0';

    /* Prime the parser with the first token */
    parser_advance(&parser);

    wl_parser_ast_node_t *result = parse_program(&parser);

    if (parser.had_error && error_buf && error_buf_size > 0) {
        snprintf(error_buf, error_buf_size, "%s", parser.error_msg);
    }

    return result;
}
