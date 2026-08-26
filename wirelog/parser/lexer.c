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

#ifndef _WIN32
#define _GNU_SOURCE 1
#endif

#include "lexer.h"

#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <locale.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Internal Helpers                                                         */
/* ======================================================================== */

static bool
is_at_end(const wl_parser_lexer_t *lexer)
{
    return *lexer->current == '\0';
}

static char
peek(const wl_parser_lexer_t *lexer)
{
    return *lexer->current;
}

static char
peek_next(const wl_parser_lexer_t *lexer)
{
    if (is_at_end(lexer))
        return '\0';
    return lexer->current[1];
}

static char
advance(wl_parser_lexer_t *lexer)
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
match(wl_parser_lexer_t *lexer, char expected)
{
    if (is_at_end(lexer))
        return false;
    if (*lexer->current != expected)
        return false;
    advance(lexer);
    return true;
}

static wl_parser_lexer_token_t
make_token(const wl_parser_lexer_t *lexer, wl_parser_lexer_token_type_t type)
{
    wl_parser_lexer_token_t token;
    token.type = type;
    token.start = lexer->start;
    token.length = (uint32_t)(lexer->current - lexer->start);
    token.line = lexer->start_line;
    token.col = lexer->start_col;
    token.int_value = 0;
    token.uint_value = 0;
    token.float_value = 0.0;
    return token;
}

static wl_parser_lexer_token_t
make_error(wl_parser_lexer_t *lexer, const char *message)
{
    wl_parser_lexer_token_t token;
    token.type = WL_PARSER_LEXER_TOK_ERROR;
    token.start = lexer->start;
    token.length = (uint32_t)(lexer->current - lexer->start);
    token.line = lexer->start_line;
    token.col = lexer->start_col;
    token.int_value = 0;
    token.uint_value = 0;
    token.float_value = 0.0;
    snprintf(lexer->error_msg, sizeof(lexer->error_msg), "%s", message);
    return token;
}

/* ======================================================================== */
/* Whitespace and Comments                                                  */
/* ======================================================================== */

static void
skip_whitespace_and_comments(wl_parser_lexer_t *lexer)
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

static wl_parser_lexer_token_t
scan_string(wl_parser_lexer_t *lexer)
{
    /* Opening quote already consumed */
    while (!is_at_end(lexer) && peek(lexer) != '"') {
        if (peek(lexer) == '\\'
            && (peek_next(lexer) == '"' || peek_next(lexer) == '\\')) {
            advance(lexer);
            advance(lexer);
            continue;
        }
        advance(lexer);
    }

    if (is_at_end(lexer)) {
        return make_error(lexer, "unterminated string literal");
    }

    advance(lexer); /* closing quote */
    return make_token(lexer, WL_PARSER_LEXER_TOK_STRING);
}

static wl_parser_lexer_token_t
scan_number(wl_parser_lexer_t *lexer)
{
    while (!is_at_end(lexer) && isdigit((unsigned char)peek(lexer))) {
        advance(lexer);
    }

    bool is_float = false;
    if (peek(lexer) == '.' && isdigit((unsigned char)peek_next(lexer))) {
        is_float = true;
        advance(lexer);
        while (!is_at_end(lexer) && isdigit((unsigned char)peek(lexer)))
            advance(lexer);
    }
    if (peek(lexer) == 'e' || peek(lexer) == 'E') {
        is_float = true;
        advance(lexer);
        if (peek(lexer) == '+' || peek(lexer) == '-')
            advance(lexer);
        if (!isdigit((unsigned char)peek(lexer)))
            return make_error(lexer, "malformed float exponent");
        while (!is_at_end(lexer) && isdigit((unsigned char)peek(lexer)))
            advance(lexer);
    }

    if (is_float) {
        wl_parser_lexer_token_t token
            = make_token(lexer, WL_PARSER_LEXER_TOK_FLOAT);
        char *text = wl_parser_lexer_token_to_string(&token);
        if (!text)
            return make_error(lexer, "out of memory parsing float literal");
        errno = 0;
        char *end = NULL;
        double value;
#ifdef _WIN32
        _locale_t c_locale = _create_locale(LC_NUMERIC, "C");
        value = c_locale ? _strtod_l(text, &end, c_locale) : 0.0;
        if (c_locale)
            _free_locale(c_locale);
#elif defined(__APPLE__)
        /* Apple's SDK does not expose strtod_l for the iOS deployment
         * target.  iOS applications use the process's invariant POSIX
         * numeric locale for this parser path; keep the locale-specific
         * implementation for hosted POSIX builds below. */
        value = strtod(text, &end);
#else
        locale_t c_locale = newlocale(LC_NUMERIC_MASK, "C", (locale_t)0);
        value = c_locale ? strtod_l(text, &end, c_locale) : 0.0;
        if (c_locale)
            freelocale(c_locale);
#endif
        int parse_errno = errno;
        /* ERANGE also reports gradual underflow.  A non-zero finite
         * subnormal (for example 5e-324) is a valid binary64 value; only an
         * underflow all the way to zero is rejected. */
        bool invalid = end == NULL || end == text
            || *end != '\0' || !isfinite(value)
            || (parse_errno == ERANGE && value == 0.0);
        free(text);
        if (invalid)
            return make_error(lexer,
                       "float literal is not a finite binary64 value");
        token.float_value = value == 0.0 ? 0.0 : value;
        return token;
    }

    wl_parser_lexer_token_t token
        = make_token(lexer, WL_PARSER_LEXER_TOK_INTEGER);

    /* Parse unsigned magnitude; the parser decides whether a preceding
     * adjacent '-' makes INT64_MAX + 1 valid as INT64_MIN. */
    const uint64_t max_signed_magnitude = (uint64_t)INT64_MAX + 1;
    uint64_t value = 0;
    if (lexer->current - token.start > 19)
        return make_error(lexer, "integer literal out of int64 range");
    for (const char *p = token.start; p < lexer->current; p++) {
        uint64_t digit = (uint64_t)(*p - '0');
        value = value * 10 + digit;
    }
    if (value > max_signed_magnitude)
        return make_error(lexer, "integer literal out of int64 range");
    token.uint_value = value;
    if (value <= (uint64_t)INT64_MAX)
        token.int_value = (int64_t)value;

    return token;
}

static bool
check_keyword(const char *start, uint32_t length, const char *keyword,
    size_t kw_len)
{
    return length == (uint32_t)kw_len && memcmp(start, keyword, kw_len) == 0;
}

#define IS_KW(s) check_keyword(start, length, s, sizeof(s) - 1)

static wl_parser_lexer_token_type_t
identifier_type(const char *start, uint32_t length)
{
    /* Boolean literals */
    if (IS_KW("True"))
        return WL_PARSER_LEXER_TOK_TRUE;
    if (IS_KW("False"))
        return WL_PARSER_LEXER_TOK_FALSE;

    /* Aggregate keywords (lowercase) */
    if (IS_KW("count"))
        return WL_PARSER_LEXER_TOK_COUNT;
    if (IS_KW("sum"))
        return WL_PARSER_LEXER_TOK_SUM;
    if (IS_KW("min"))
        return WL_PARSER_LEXER_TOK_MIN;
    if (IS_KW("max"))
        return WL_PARSER_LEXER_TOK_MAX;
    if (IS_KW("average"))
        return WL_PARSER_LEXER_TOK_AVG;

    /* Aggregate keywords (uppercase) */
    if (IS_KW("COUNT"))
        return WL_PARSER_LEXER_TOK_COUNT;
    if (IS_KW("SUM"))
        return WL_PARSER_LEXER_TOK_SUM;
    if (IS_KW("MIN"))
        return WL_PARSER_LEXER_TOK_MIN;
    if (IS_KW("MAX"))
        return WL_PARSER_LEXER_TOK_MAX;
    if (IS_KW("AVG"))
        return WL_PARSER_LEXER_TOK_AVG;

    /* Type keywords */
    if (IS_KW("int32"))
        return WL_PARSER_LEXER_TOK_INT32;
    if (IS_KW("int64"))
        return WL_PARSER_LEXER_TOK_INT64;
    if (IS_KW("string"))
        return WL_PARSER_LEXER_TOK_STRING_TYPE;
    if (IS_KW("symbol"))
        return WL_PARSER_LEXER_TOK_SYMBOL_TYPE;
    if (IS_KW("float"))
        return WL_PARSER_LEXER_TOK_FLOAT_TYPE;

    /* Bitwise operator keywords */
    if (IS_KW("band"))
        return WL_PARSER_LEXER_TOK_BAND;
    if (IS_KW("bor"))
        return WL_PARSER_LEXER_TOK_BOR;
    if (IS_KW("bxor"))
        return WL_PARSER_LEXER_TOK_BXOR;
    if (IS_KW("bnot"))
        return WL_PARSER_LEXER_TOK_BNOT;
    if (IS_KW("bshl"))
        return WL_PARSER_LEXER_TOK_BSHL;
    if (IS_KW("bshr"))
        return WL_PARSER_LEXER_TOK_BSHR;

    /* Hash function keyword */
    if (IS_KW("hash"))
        return WL_PARSER_LEXER_TOK_HASH;

    /* Cryptographic hash function keywords */
    if (IS_KW("md5"))
        return WL_PARSER_LEXER_TOK_MD5;
    if (IS_KW("sha1"))
        return WL_PARSER_LEXER_TOK_SHA1;
    if (IS_KW("sha256"))
        return WL_PARSER_LEXER_TOK_SHA256;
    if (IS_KW("sha512"))
        return WL_PARSER_LEXER_TOK_SHA512;
    if (IS_KW("hmac_sha256"))
        return WL_PARSER_LEXER_TOK_HMAC_SHA256;

    /* UUID function keywords */
    if (IS_KW("uuid4"))
        return WL_PARSER_LEXER_TOK_UUID4;
    if (IS_KW("uuid5"))
        return WL_PARSER_LEXER_TOK_UUID5;

    /* CRC-32 function keywords (Issue #884) */
    if (IS_KW("crc32_ethernet"))
        return WL_PARSER_LEXER_TOK_CRC32_ETH;
    if (IS_KW("crc32_castagnoli"))
        return WL_PARSER_LEXER_TOK_CRC32_CAST;

    /* String function keywords */
    if (IS_KW("strlen"))
        return WL_PARSER_LEXER_TOK_STRLEN;
    if (IS_KW("cat"))
        return WL_PARSER_LEXER_TOK_CAT;
    if (IS_KW("substr"))
        return WL_PARSER_LEXER_TOK_SUBSTR;
    if (IS_KW("contains"))
        return WL_PARSER_LEXER_TOK_CONTAINS;
    if (IS_KW("str_prefix"))
        return WL_PARSER_LEXER_TOK_STR_PREFIX;
    if (IS_KW("str_suffix"))
        return WL_PARSER_LEXER_TOK_STR_SUFFIX;
    if (IS_KW("str_ord"))
        return WL_PARSER_LEXER_TOK_STR_ORD;
    if (IS_KW("to_upper"))
        return WL_PARSER_LEXER_TOK_TO_UPPER;
    if (IS_KW("to_lower"))
        return WL_PARSER_LEXER_TOK_TO_LOWER;
    if (IS_KW("str_replace"))
        return WL_PARSER_LEXER_TOK_STR_REPLACE;
    if (IS_KW("trim"))
        return WL_PARSER_LEXER_TOK_TRIM;
    if (IS_KW("to_string"))
        return WL_PARSER_LEXER_TOK_TO_STRING;
    if (IS_KW("to_number"))
        return WL_PARSER_LEXER_TOK_TO_NUMBER;
    if (IS_KW("uuid5_rfc"))
        return WL_PARSER_LEXER_TOK_UUID5_RFC;

    return WL_PARSER_LEXER_TOK_IDENT;
}

#undef IS_KW

static wl_parser_lexer_token_t
scan_identifier(wl_parser_lexer_t *lexer)
{
    /* First char already consumed (alpha or _alpha) */
    while (!is_at_end(lexer)
        && (isalnum((unsigned char)peek(lexer)) || peek(lexer) == '_')) {
        advance(lexer);
    }

    const char *start = lexer->start;
    uint32_t length = (uint32_t)(lexer->current - start);
    wl_parser_lexer_token_type_t type = identifier_type(start, length);

    return make_token(lexer, type);
}

static wl_parser_lexer_token_t
scan_directive(wl_parser_lexer_t *lexer)
{
    /* The '.' has been consumed, and we see an alpha char next.
     * Read the directive word. */
    while (!is_at_end(lexer) && isalpha((unsigned char)peek(lexer))) {
        advance(lexer);
    }

    const char *start = lexer->start;
    uint32_t length = (uint32_t)(lexer->current - start);

    /* Check for known directives (including the leading dot) */
    if (length == 5 && memcmp(start, ".decl", 5) == 0)
        return make_token(lexer, WL_PARSER_LEXER_TOK_DECL);
    if (length == 6 && memcmp(start, ".input", 6) == 0)
        return make_token(lexer, WL_PARSER_LEXER_TOK_INPUT);
    if (length == 7 && memcmp(start, ".output", 7) == 0)
        return make_token(lexer, WL_PARSER_LEXER_TOK_OUTPUT);
    if (length == 10 && memcmp(start, ".printsize", 10) == 0)
        return make_token(lexer, WL_PARSER_LEXER_TOK_PRINTSIZE);
    if (length == 5 && memcmp(start, ".plan", 5) == 0)
        return make_token(lexer, WL_PARSER_LEXER_TOK_PLAN);
    if (length == 6 && memcmp(start, ".query", 6) == 0)
        return make_token(lexer, WL_PARSER_LEXER_TOK_QUERY);

    /* Not a known directive: back up to just after the dot,
     * return DOT token */
    lexer->current = lexer->start + 1;
    lexer->col = lexer->start_col + 1;
    return make_token(lexer, WL_PARSER_LEXER_TOK_DOT);
}

/* ======================================================================== */
/* Main Scanner                                                             */
/* ======================================================================== */

static wl_parser_lexer_token_t
scan_token(wl_parser_lexer_t *lexer)
{
    skip_whitespace_and_comments(lexer);

    lexer->start = lexer->current;
    lexer->start_line = lexer->line;
    lexer->start_col = lexer->col;

    if (is_at_end(lexer)) {
        return make_token(lexer, WL_PARSER_LEXER_TOK_EOF);
    }

    char c = advance(lexer);

    /* Identifiers and keywords */
    if (isalpha((unsigned char)c)) {
        return scan_identifier(lexer);
    }

    /* Underscore: could be wildcard or identifier start.
     * Accepted forms: _alpha..., __alpha..., __underscore_chain..._alpha...
     * Rejected: standalone _, __, ___ (no alpha anywhere after underscores). */
    if (c == '_') {
        if (!is_at_end(lexer)
            && (isalpha((unsigned char)peek(lexer))
            || (peek(lexer) == '_'
            && (isalpha((unsigned char)peek_next(lexer))
            || peek_next(lexer) == '_')))) {
            /* _alpha... or __<alpha-or-_>... => scan as identifier */
            return scan_identifier(lexer);
        }
        /* Standalone _ / __ / ___ with no alpha continuation => wildcard */
        return make_token(lexer, WL_PARSER_LEXER_TOK_UNDERSCORE);
    }

    /* Integer literals */
    if (isdigit((unsigned char)c)) {
        return scan_number(lexer);
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
        return make_token(lexer, WL_PARSER_LEXER_TOK_DOT);
    }

    /* Punctuation */
    switch (c) {
    case '(':
        return make_token(lexer, WL_PARSER_LEXER_TOK_LPAREN);
    case ')':
        return make_token(lexer, WL_PARSER_LEXER_TOK_RPAREN);
    case ',':
        return make_token(lexer, WL_PARSER_LEXER_TOK_COMMA);

    case ':':
        if (match(lexer, '-'))
            return make_token(lexer, WL_PARSER_LEXER_TOK_HORN);
        return make_token(lexer, WL_PARSER_LEXER_TOK_COLON);

    case '!':
        if (match(lexer, '='))
            return make_token(lexer, WL_PARSER_LEXER_TOK_NEQ);
        return make_token(lexer, WL_PARSER_LEXER_TOK_BANG);

    case '=':
        return make_token(lexer, WL_PARSER_LEXER_TOK_EQ);

    case '<':
        if (match(lexer, '='))
            return make_token(lexer, WL_PARSER_LEXER_TOK_LTE);
        return make_token(lexer, WL_PARSER_LEXER_TOK_LT);

    case '>':
        if (match(lexer, '='))
            return make_token(lexer, WL_PARSER_LEXER_TOK_GTE);
        return make_token(lexer, WL_PARSER_LEXER_TOK_GT);

    case '+':
        return make_token(lexer, WL_PARSER_LEXER_TOK_PLUS);
    case '-':
        return make_token(lexer, WL_PARSER_LEXER_TOK_MINUS);
    case '*':
        return make_token(lexer, WL_PARSER_LEXER_TOK_STAR);
    case '/':
        return make_token(lexer, WL_PARSER_LEXER_TOK_SLASH);
    case '%':
        return make_token(lexer, WL_PARSER_LEXER_TOK_PERCENT);
    }

    return make_error(lexer, "unexpected character");
}

/* ======================================================================== */
/* Public API                                                               */
/* ======================================================================== */

void
wl_parser_lexer_init(wl_parser_lexer_t *lexer, const char *source)
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

wl_parser_lexer_token_t
wl_parser_lexer_next_token(wl_parser_lexer_t *lexer)
{
    return scan_token(lexer);
}

wl_parser_lexer_token_t
wl_parser_lexer_peek_token(wl_parser_lexer_t *lexer)
{
    /* Save state */
    const char *saved_current = lexer->current;
    const char *saved_start = lexer->start;
    uint32_t saved_line = lexer->line;
    uint32_t saved_col = lexer->col;
    uint32_t saved_start_line = lexer->start_line;
    uint32_t saved_start_col = lexer->start_col;

    wl_parser_lexer_token_t token = scan_token(lexer);

    /* Restore state */
    lexer->current = saved_current;
    lexer->start = saved_start;
    lexer->line = saved_line;
    lexer->col = saved_col;
    lexer->start_line = saved_start_line;
    lexer->start_col = saved_start_col;

    return token;
}

const char *
wl_parser_lexer_token_type_str(wl_parser_lexer_token_type_t type)
{
    switch (type) {
    case WL_PARSER_LEXER_TOK_IDENT:
        return "IDENT";
    case WL_PARSER_LEXER_TOK_INTEGER:
        return "INTEGER";
    case WL_PARSER_LEXER_TOK_FLOAT:
        return "FLOAT";
    case WL_PARSER_LEXER_TOK_STRING:
        return "STRING";
    case WL_PARSER_LEXER_TOK_TRUE:
        return "TRUE";
    case WL_PARSER_LEXER_TOK_FALSE:
        return "FALSE";
    case WL_PARSER_LEXER_TOK_UNDERSCORE:
        return "UNDERSCORE";
    case WL_PARSER_LEXER_TOK_COUNT:
        return "COUNT";
    case WL_PARSER_LEXER_TOK_SUM:
        return "SUM";
    case WL_PARSER_LEXER_TOK_MIN:
        return "MIN";
    case WL_PARSER_LEXER_TOK_MAX:
        return "MAX";
    case WL_PARSER_LEXER_TOK_AVG:
        return "AVG";
    case WL_PARSER_LEXER_TOK_INT32:
        return "INT32";
    case WL_PARSER_LEXER_TOK_INT64:
        return "INT64";
    case WL_PARSER_LEXER_TOK_STRING_TYPE:
        return "STRING_TYPE";
    case WL_PARSER_LEXER_TOK_SYMBOL_TYPE:
        return "SYMBOL_TYPE";
    case WL_PARSER_LEXER_TOK_FLOAT_TYPE:
        return "FLOAT_TYPE";
    case WL_PARSER_LEXER_TOK_LPAREN:
        return "LPAREN";
    case WL_PARSER_LEXER_TOK_RPAREN:
        return "RPAREN";
    case WL_PARSER_LEXER_TOK_COMMA:
        return "COMMA";
    case WL_PARSER_LEXER_TOK_DOT:
        return "DOT";
    case WL_PARSER_LEXER_TOK_COLON:
        return "COLON";
    case WL_PARSER_LEXER_TOK_BANG:
        return "BANG";
    case WL_PARSER_LEXER_TOK_HORN:
        return "HORN";
    case WL_PARSER_LEXER_TOK_EQ:
        return "EQ";
    case WL_PARSER_LEXER_TOK_NEQ:
        return "NEQ";
    case WL_PARSER_LEXER_TOK_LT:
        return "LT";
    case WL_PARSER_LEXER_TOK_GT:
        return "GT";
    case WL_PARSER_LEXER_TOK_LTE:
        return "LTE";
    case WL_PARSER_LEXER_TOK_GTE:
        return "GTE";
    case WL_PARSER_LEXER_TOK_PLUS:
        return "PLUS";
    case WL_PARSER_LEXER_TOK_MINUS:
        return "MINUS";
    case WL_PARSER_LEXER_TOK_STAR:
        return "STAR";
    case WL_PARSER_LEXER_TOK_SLASH:
        return "SLASH";
    case WL_PARSER_LEXER_TOK_PERCENT:
        return "PERCENT";
    case WL_PARSER_LEXER_TOK_BAND:
        return "BAND";
    case WL_PARSER_LEXER_TOK_BOR:
        return "BOR";
    case WL_PARSER_LEXER_TOK_BXOR:
        return "BXOR";
    case WL_PARSER_LEXER_TOK_BNOT:
        return "BNOT";
    case WL_PARSER_LEXER_TOK_BSHL:
        return "BSHL";
    case WL_PARSER_LEXER_TOK_BSHR:
        return "BSHR";
    case WL_PARSER_LEXER_TOK_HASH:
        return "HASH";
    case WL_PARSER_LEXER_TOK_MD5:
        return "MD5";
    case WL_PARSER_LEXER_TOK_SHA1:
        return "SHA1";
    case WL_PARSER_LEXER_TOK_SHA256:
        return "SHA256";
    case WL_PARSER_LEXER_TOK_SHA512:
        return "SHA512";
    case WL_PARSER_LEXER_TOK_HMAC_SHA256:
        return "HMAC_SHA256";
    case WL_PARSER_LEXER_TOK_UUID4:
        return "UUID4";
    case WL_PARSER_LEXER_TOK_UUID5:
        return "UUID5";
    case WL_PARSER_LEXER_TOK_UUID5_RFC:
        return "UUID5_RFC";
    case WL_PARSER_LEXER_TOK_CRC32_ETH:
        return "CRC32_ETHERNET";
    case WL_PARSER_LEXER_TOK_CRC32_CAST:
        return "CRC32_CASTAGNOLI";
    case WL_PARSER_LEXER_TOK_STRLEN:
        return "STRLEN";
    case WL_PARSER_LEXER_TOK_CAT:
        return "CAT";
    case WL_PARSER_LEXER_TOK_SUBSTR:
        return "SUBSTR";
    case WL_PARSER_LEXER_TOK_CONTAINS:
        return "CONTAINS";
    case WL_PARSER_LEXER_TOK_STR_PREFIX:
        return "STR_PREFIX";
    case WL_PARSER_LEXER_TOK_STR_SUFFIX:
        return "STR_SUFFIX";
    case WL_PARSER_LEXER_TOK_STR_ORD:
        return "STR_ORD";
    case WL_PARSER_LEXER_TOK_TO_UPPER:
        return "TO_UPPER";
    case WL_PARSER_LEXER_TOK_TO_LOWER:
        return "TO_LOWER";
    case WL_PARSER_LEXER_TOK_STR_REPLACE:
        return "STR_REPLACE";
    case WL_PARSER_LEXER_TOK_TRIM:
        return "TRIM";
    case WL_PARSER_LEXER_TOK_TO_STRING:
        return "TO_STRING";
    case WL_PARSER_LEXER_TOK_TO_NUMBER:
        return "TO_NUMBER";
    case WL_PARSER_LEXER_TOK_DECL:
        return "DECL";
    case WL_PARSER_LEXER_TOK_INPUT:
        return "INPUT";
    case WL_PARSER_LEXER_TOK_OUTPUT:
        return "OUTPUT";
    case WL_PARSER_LEXER_TOK_PRINTSIZE:
        return "PRINTSIZE";
    case WL_PARSER_LEXER_TOK_PLAN:
        return "PLAN";
    case WL_PARSER_LEXER_TOK_QUERY:
        return "QUERY";
    case WL_PARSER_LEXER_TOK_EOF:
        return "EOF";
    case WL_PARSER_LEXER_TOK_ERROR:
        return "ERROR";
    }
    return "UNKNOWN";
}

char *
wl_parser_lexer_token_to_string(const wl_parser_lexer_token_t *token)
{
    if (token->type == WL_PARSER_LEXER_TOK_STRING) {
        /* Strip surrounding quotes and decode supported string escapes. */
        if (token->length >= 2) {
            uint32_t inner_len = token->length - 2;
            char *str = (char *)malloc(inner_len + 1);
            if (str) {
                const char *p = token->start + 1;
                const char *end = p + inner_len;
                char *out = str;
                while (p < end) {
                    if (*p == '\\' && p + 1 < end
                        && (p[1] == '"' || p[1] == '\\')) {
                        *out++ = p[1];
                        p += 2;
                        continue;
                    }
                    *out++ = *p++;
                }
                *out = '\0';
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
