/*
 * lexer.h - wirelog Lexer Interface
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
 * Token design based on FlowLog grammar analysis.
 * See discussion/FlowLog_Grammar_Analysis.md for grammar reference.
 */

#ifndef WIRELOG_LEXER_H
#define WIRELOG_LEXER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

/* ======================================================================== */
/* Token Types                                                              */
/* ======================================================================== */

typedef enum {
    /* Literals */
    WL_TOK_IDENT,          /* identifier: _?[a-zA-Z]+[a-zA-Z0-9_]* */
    WL_TOK_INTEGER,        /* integer literal: [0-9]+ */
    WL_TOK_STRING,         /* "string literal" */

    /* Boolean literals */
    WL_TOK_TRUE,           /* True */
    WL_TOK_FALSE,          /* False */

    /* Placeholder */
    WL_TOK_UNDERSCORE,     /* _ (standalone wildcard) */

    /* Aggregate keywords (case-insensitive pairs) */
    WL_TOK_COUNT,          /* count / COUNT */
    WL_TOK_SUM,            /* sum / SUM */
    WL_TOK_MIN,            /* min / MIN */
    WL_TOK_MAX,            /* max / MAX */
    WL_TOK_AVG,            /* average / AVG */

    /* Type keywords */
    WL_TOK_INT32,          /* int32 */
    WL_TOK_INT64,          /* int64 */
    WL_TOK_STRING_TYPE,    /* string (as type name, not literal) */

    /* Punctuation */
    WL_TOK_LPAREN,         /* ( */
    WL_TOK_RPAREN,         /* ) */
    WL_TOK_COMMA,          /* , */
    WL_TOK_DOT,            /* . */
    WL_TOK_COLON,          /* : */
    WL_TOK_BANG,           /* ! */

    /* Operators */
    WL_TOK_HORN,           /* :- */
    WL_TOK_EQ,             /* = */
    WL_TOK_NEQ,            /* != */
    WL_TOK_LT,             /* < */
    WL_TOK_GT,             /* > */
    WL_TOK_LTE,            /* <= */
    WL_TOK_GTE,            /* >= */
    WL_TOK_PLUS,           /* + */
    WL_TOK_MINUS,          /* - */
    WL_TOK_STAR,           /* * */
    WL_TOK_SLASH,          /* / */
    WL_TOK_PERCENT,        /* % */

    /* Directives (dot-prefixed keywords) */
    WL_TOK_DECL,           /* .decl */
    WL_TOK_INPUT,          /* .input */
    WL_TOK_OUTPUT,         /* .output */
    WL_TOK_PRINTSIZE,      /* .printsize */
    WL_TOK_PLAN,           /* .plan */

    /* Special */
    WL_TOK_EOF,            /* end of input */
    WL_TOK_ERROR,          /* lexer error */
} wl_token_type_t;

/* ======================================================================== */
/* Token                                                                    */
/* ======================================================================== */

typedef struct {
    wl_token_type_t type;
    const char *start;      /* Pointer into source text */
    uint32_t length;        /* Length of token text */
    uint32_t line;
    uint32_t col;

    /* Parsed value (for integer literals) */
    int64_t int_value;
} wl_token_t;

/* ======================================================================== */
/* Lexer State                                                              */
/* ======================================================================== */

typedef struct {
    const char *source;     /* Source text */
    const char *current;    /* Current position */
    const char *start;      /* Start of current token */
    uint32_t line;
    uint32_t col;
    uint32_t start_line;
    uint32_t start_col;
    char error_msg[256];    /* Error message buffer */
} wl_lexer_t;

/* ======================================================================== */
/* Lexer API                                                                */
/* ======================================================================== */

void
wl_lexer_init(wl_lexer_t *lexer, const char *source);

wl_token_t
wl_lexer_next_token(wl_lexer_t *lexer);

wl_token_t
wl_lexer_peek_token(wl_lexer_t *lexer);

const char*
wl_token_type_str(wl_token_type_t type);

/* Extract token text as a new string (caller must free) */
char*
wl_token_to_string(const wl_token_t *token);

#ifdef __cplusplus
}
#endif

#endif /* WIRELOG_LEXER_H */
