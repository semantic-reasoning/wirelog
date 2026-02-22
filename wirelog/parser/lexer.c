/*
 * lexer.c - wirelog Lexer Implementation
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

#include "lexer.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Internal Helpers                                                         */
/* ======================================================================== */

static bool
is_at_end(const wl_lexer_t *lexer)
{
    return *lexer->current == '\0';
}

static char
peek(const wl_lexer_t *lexer)
{
    return *lexer->current;
}

static char
peek_next(const wl_lexer_t *lexer)
{
    if (is_at_end(lexer)) return '\0';
    return lexer->current[1];
}

static char
advance(wl_lexer_t *lexer)
{
    char c = *lexer->current++;
    if (c == '\n') {
        lexer->line++;
        lexer->col = 1;
    } else {
        lexer->col++;
    }
    return c;
}

static bool
match(wl_lexer_t *lexer, char expected)
{
    if (is_at_end(lexer)) return false;
    if (*lexer->current != expected) return false;
    advance(lexer);
    return true;
}

static wl_token_t
make_token(const wl_lexer_t *lexer, wl_token_type_t type)
{
    wl_token_t token;
    token.type = type;
    token.start = lexer->start;
    token.length = (uint32_t)(lexer->current - lexer->start);
    token.line = lexer->start_line;
    token.col = lexer->start_col;
    token.int_value = 0;
    return token;
}

static wl_token_t
make_error(wl_lexer_t *lexer, const char *message)
{
    wl_token_t token;
    token.type = WL_TOK_ERROR;
    token.start = lexer->start;
    token.length = (uint32_t)(lexer->current - lexer->start);
    token.line = lexer->start_line;
    token.col = lexer->start_col;
    token.int_value = 0;
    snprintf(lexer->error_msg, sizeof(lexer->error_msg), "%s", message);
    return token;
}

/* ======================================================================== */
/* Whitespace and Comments                                                  */
/* ======================================================================== */

static void
skip_whitespace_and_comments(wl_lexer_t *lexer)
{
    for (;;) {
        char c = peek(lexer);

        /* Whitespace */
        if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
            advance(lexer);
            continue;
        }

        /* Hash comment: # ... */
        if (c == '#') {
            while (!is_at_end(lexer) && peek(lexer) != '\n')
                advance(lexer);
            continue;
        }

        /* Double-slash comment: // ... */
        if (c == '/' && peek_next(lexer) == '/') {
            advance(lexer); /* skip first / */
            advance(lexer); /* skip second / */
            while (!is_at_end(lexer) && peek(lexer) != '\n')
                advance(lexer);
            continue;
        }

        break;
    }
}

/* ======================================================================== */
/* Token Scanning                                                           */
/* ======================================================================== */

static wl_token_t
scan_string(wl_lexer_t *lexer)
{
    /* Opening quote already consumed */
    while (!is_at_end(lexer) && peek(lexer) != '"') {
        advance(lexer);
    }

    if (is_at_end(lexer)) {
        return make_error(lexer, "unterminated string literal");
    }

    advance(lexer); /* closing quote */
    return make_token(lexer, WL_TOK_STRING);
}

static wl_token_t
scan_integer(wl_lexer_t *lexer)
{
    while (!is_at_end(lexer) && isdigit((unsigned char)peek(lexer))) {
        advance(lexer);
    }

    wl_token_t token = make_token(lexer, WL_TOK_INTEGER);

    /* Parse the integer value */
    int64_t value = 0;
    for (const char *p = token.start; p < lexer->current; p++) {
        value = value * 10 + (*p - '0');
    }
    token.int_value = value;

    return token;
}

static bool
check_keyword(const char *start, uint32_t length,
              const char *keyword, size_t kw_len)
{
    return length == (uint32_t)kw_len &&
           memcmp(start, keyword, kw_len) == 0;
}

#define IS_KW(s) check_keyword(start, length, s, sizeof(s) - 1)

static wl_token_type_t
identifier_type(const char *start, uint32_t length)
{
    /* Boolean literals */
    if (IS_KW("True"))    return WL_TOK_TRUE;
    if (IS_KW("False"))   return WL_TOK_FALSE;

    /* Aggregate keywords (lowercase) */
    if (IS_KW("count"))   return WL_TOK_COUNT;
    if (IS_KW("sum"))     return WL_TOK_SUM;
    if (IS_KW("min"))     return WL_TOK_MIN;
    if (IS_KW("max"))     return WL_TOK_MAX;
    if (IS_KW("average")) return WL_TOK_AVG;

    /* Aggregate keywords (uppercase) */
    if (IS_KW("COUNT"))   return WL_TOK_COUNT;
    if (IS_KW("SUM"))     return WL_TOK_SUM;
    if (IS_KW("MIN"))     return WL_TOK_MIN;
    if (IS_KW("MAX"))     return WL_TOK_MAX;
    if (IS_KW("AVG"))     return WL_TOK_AVG;

    /* Type keywords */
    if (IS_KW("int32"))   return WL_TOK_INT32;
    if (IS_KW("int64"))   return WL_TOK_INT64;
    if (IS_KW("string"))  return WL_TOK_STRING_TYPE;

    return WL_TOK_IDENT;
}

#undef IS_KW

static wl_token_t
scan_identifier(wl_lexer_t *lexer)
{
    /* First char already consumed (alpha or _alpha) */
    while (!is_at_end(lexer) &&
           (isalnum((unsigned char)peek(lexer)) ||
            peek(lexer) == '_')) {
        advance(lexer);
    }

    const char *start = lexer->start;
    uint32_t length = (uint32_t)(lexer->current - start);
    wl_token_type_t type = identifier_type(start, length);

    return make_token(lexer, type);
}

static wl_token_t
scan_directive(wl_lexer_t *lexer)
{
    /* The '.' has been consumed, and we see an alpha char next.
     * Read the directive word. */
    while (!is_at_end(lexer) &&
           isalpha((unsigned char)peek(lexer))) {
        advance(lexer);
    }

    const char *start = lexer->start;
    uint32_t length = (uint32_t)(lexer->current - start);

    /* Check for known directives (including the leading dot) */
    if (length == 5 && memcmp(start, ".decl", 5) == 0)
        return make_token(lexer, WL_TOK_DECL);
    if (length == 6 && memcmp(start, ".input", 6) == 0)
        return make_token(lexer, WL_TOK_INPUT);
    if (length == 7 && memcmp(start, ".output", 7) == 0)
        return make_token(lexer, WL_TOK_OUTPUT);
    if (length == 10 && memcmp(start, ".printsize", 10) == 0)
        return make_token(lexer, WL_TOK_PRINTSIZE);
    if (length == 5 && memcmp(start, ".plan", 5) == 0)
        return make_token(lexer, WL_TOK_PLAN);

    /* Not a known directive: back up to just after the dot,
     * return DOT token */
    lexer->current = lexer->start + 1;
    lexer->col = lexer->start_col + 1;
    return make_token(lexer, WL_TOK_DOT);
}

/* ======================================================================== */
/* Main Scanner                                                             */
/* ======================================================================== */

static wl_token_t
scan_token(wl_lexer_t *lexer)
{
    skip_whitespace_and_comments(lexer);

    lexer->start = lexer->current;
    lexer->start_line = lexer->line;
    lexer->start_col = lexer->col;

    if (is_at_end(lexer)) {
        return make_token(lexer, WL_TOK_EOF);
    }

    char c = advance(lexer);

    /* Identifiers and keywords */
    if (isalpha((unsigned char)c)) {
        return scan_identifier(lexer);
    }

    /* Underscore: could be wildcard or identifier start */
    if (c == '_') {
        if (!is_at_end(lexer) && isalpha((unsigned char)peek(lexer))) {
            /* _alpha... => identifier */
            return scan_identifier(lexer);
        }
        /* Standalone underscore => wildcard */
        return make_token(lexer, WL_TOK_UNDERSCORE);
    }

    /* Integer literals */
    if (isdigit((unsigned char)c)) {
        return scan_integer(lexer);
    }

    /* String literals */
    if (c == '"') {
        return scan_string(lexer);
    }

    /* Dot and directives */
    if (c == '.') {
        if (!is_at_end(lexer) && isalpha((unsigned char)peek(lexer))) {
            return scan_directive(lexer);
        }
        return make_token(lexer, WL_TOK_DOT);
    }

    /* Punctuation */
    switch (c) {
    case '(':  return make_token(lexer, WL_TOK_LPAREN);
    case ')':  return make_token(lexer, WL_TOK_RPAREN);
    case ',':  return make_token(lexer, WL_TOK_COMMA);

    case ':':
        if (match(lexer, '-'))
            return make_token(lexer, WL_TOK_HORN);
        return make_token(lexer, WL_TOK_COLON);

    case '!':
        if (match(lexer, '='))
            return make_token(lexer, WL_TOK_NEQ);
        return make_token(lexer, WL_TOK_BANG);

    case '=':
        return make_token(lexer, WL_TOK_EQ);

    case '<':
        if (match(lexer, '='))
            return make_token(lexer, WL_TOK_LTE);
        return make_token(lexer, WL_TOK_LT);

    case '>':
        if (match(lexer, '='))
            return make_token(lexer, WL_TOK_GTE);
        return make_token(lexer, WL_TOK_GT);

    case '+':  return make_token(lexer, WL_TOK_PLUS);
    case '-':  return make_token(lexer, WL_TOK_MINUS);
    case '*':  return make_token(lexer, WL_TOK_STAR);
    case '/':  return make_token(lexer, WL_TOK_SLASH);
    case '%':  return make_token(lexer, WL_TOK_PERCENT);
    }

    return make_error(lexer, "unexpected character");
}

/* ======================================================================== */
/* Public API                                                               */
/* ======================================================================== */

void
wl_lexer_init(wl_lexer_t *lexer, const char *source)
{
    lexer->source = source;
    lexer->current = source;
    lexer->start = source;
    lexer->line = 1;
    lexer->col = 1;
    lexer->start_line = 1;
    lexer->start_col = 1;
    lexer->error_msg[0] = '\0';
}

wl_token_t
wl_lexer_next_token(wl_lexer_t *lexer)
{
    return scan_token(lexer);
}

wl_token_t
wl_lexer_peek_token(wl_lexer_t *lexer)
{
    /* Save state */
    const char *saved_current = lexer->current;
    const char *saved_start = lexer->start;
    uint32_t saved_line = lexer->line;
    uint32_t saved_col = lexer->col;
    uint32_t saved_start_line = lexer->start_line;
    uint32_t saved_start_col = lexer->start_col;

    wl_token_t token = scan_token(lexer);

    /* Restore state */
    lexer->current = saved_current;
    lexer->start = saved_start;
    lexer->line = saved_line;
    lexer->col = saved_col;
    lexer->start_line = saved_start_line;
    lexer->start_col = saved_start_col;

    return token;
}

const char*
wl_token_type_str(wl_token_type_t type)
{
    switch (type) {
    case WL_TOK_IDENT:       return "IDENT";
    case WL_TOK_INTEGER:     return "INTEGER";
    case WL_TOK_STRING:      return "STRING";
    case WL_TOK_TRUE:        return "TRUE";
    case WL_TOK_FALSE:       return "FALSE";
    case WL_TOK_UNDERSCORE:  return "UNDERSCORE";
    case WL_TOK_COUNT:       return "COUNT";
    case WL_TOK_SUM:         return "SUM";
    case WL_TOK_MIN:         return "MIN";
    case WL_TOK_MAX:         return "MAX";
    case WL_TOK_AVG:         return "AVG";
    case WL_TOK_INT32:       return "INT32";
    case WL_TOK_INT64:       return "INT64";
    case WL_TOK_STRING_TYPE: return "STRING_TYPE";
    case WL_TOK_LPAREN:      return "LPAREN";
    case WL_TOK_RPAREN:      return "RPAREN";
    case WL_TOK_COMMA:       return "COMMA";
    case WL_TOK_DOT:         return "DOT";
    case WL_TOK_COLON:       return "COLON";
    case WL_TOK_BANG:        return "BANG";
    case WL_TOK_HORN:        return "HORN";
    case WL_TOK_EQ:          return "EQ";
    case WL_TOK_NEQ:         return "NEQ";
    case WL_TOK_LT:          return "LT";
    case WL_TOK_GT:          return "GT";
    case WL_TOK_LTE:         return "LTE";
    case WL_TOK_GTE:         return "GTE";
    case WL_TOK_PLUS:        return "PLUS";
    case WL_TOK_MINUS:       return "MINUS";
    case WL_TOK_STAR:        return "STAR";
    case WL_TOK_SLASH:       return "SLASH";
    case WL_TOK_PERCENT:     return "PERCENT";
    case WL_TOK_DECL:        return "DECL";
    case WL_TOK_INPUT:       return "INPUT";
    case WL_TOK_OUTPUT:      return "OUTPUT";
    case WL_TOK_PRINTSIZE:   return "PRINTSIZE";
    case WL_TOK_PLAN:        return "PLAN";
    case WL_TOK_EOF:         return "EOF";
    case WL_TOK_ERROR:       return "ERROR";
    }
    return "UNKNOWN";
}

char*
wl_token_to_string(const wl_token_t *token)
{
    if (token->type == WL_TOK_STRING) {
        /* Strip surrounding quotes */
        if (token->length >= 2) {
            uint32_t inner_len = token->length - 2;
            char *str = (char *)malloc(inner_len + 1);
            if (str) {
                memcpy(str, token->start + 1, inner_len);
                str[inner_len] = '\0';
            }
            return str;
        }
    }

    char *str = (char *)malloc(token->length + 1);
    if (str) {
        memcpy(str, token->start, token->length);
        str[token->length] = '\0';
    }
    return str;
}
