/*
 * string_ops.c - String Operations Library
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * UTF-8-aware string operations that operate on interned string IDs.
 * Phase 1: ASCII case folding, codepoint-indexed slicing, byte-level search.
 */

#include "string_ops.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <inttypes.h>
#include <ctype.h>

/* ======================================================================== */
/* UTF-8 Utilities                                                          */
/* ======================================================================== */

/**
 * string_ops_utf8_next:
 * @str:       Pointer to current byte in a UTF-8 string (must not be NULL).
 * @codepoint: (out, nullable): Decoded codepoint, or -1 on invalid sequence.
 *
 * Decode one UTF-8 codepoint and return a pointer to the next byte.
 * Returns NULL if @str points to the NUL terminator (end of string).
 */
const char *
string_ops_utf8_next(const char *str, int64_t *codepoint)
{
    if (!str || !*str)
        return NULL;

    unsigned char c = (unsigned char)*str;
    int64_t cp;
    int len;

    if (c < 0x80) {
        cp = c;
        len = 1;
    } else if ((c & 0xE0) == 0xC0) {
        cp = c & 0x1F;
        len = 2;
    } else if ((c & 0xF0) == 0xE0) {
        cp = c & 0x0F;
        len = 3;
    } else if ((c & 0xF8) == 0xF0) {
        cp = c & 0x07;
        len = 4;
    } else {
        /* Invalid leading byte: skip one byte */
        if (codepoint)
            *codepoint = -1;
        return str + 1;
    }

    for (int i = 1; i < len; i++) {
        unsigned char b = (unsigned char)str[i];
        if ((b & 0xC0) != 0x80) {
            /* Invalid continuation byte: report error, advance past what we have */
            if (codepoint)
                *codepoint = -1;
            return str + i;
        }
        cp = (cp << 6) | (b & 0x3F);
    }

    if (codepoint)
        *codepoint = cp;
    return str + len;
}

/**
 * string_ops_utf8_strlen:
 * @str: UTF-8 string (NULL returns 0).
 *
 * Count the number of Unicode codepoints in @str (not bytes).
 */
size_t
string_ops_utf8_strlen(const char *str)
{
    if (!str)
        return 0;

    size_t count = 0;
    const char *p = str;
    while (*p) {
        const char *next = string_ops_utf8_next(p, NULL);
        if (!next)
            break;
        p = next;
        count++;
    }
    return count;
}

/* ======================================================================== */
/* Internal Helpers                                                         */
/* ======================================================================== */

/*
 * utf8_byte_offset:
 * Walk @str to find the byte offset of codepoint index @cp_index.
 * Returns the byte offset, or (size_t)-1 if out of bounds.
 */
static size_t
utf8_byte_offset(const char *str, int64_t cp_index)
{
    if (cp_index < 0)
        return (size_t)-1;

    const char *p = str;
    int64_t i = 0;
    while (*p && i < cp_index) {
        const char *next = string_ops_utf8_next(p, NULL);
        if (!next)
            break;
        p = next;
        i++;
    }
    if (i < cp_index)
        return (size_t)-1;
    return (size_t)(p - str);
}

/* ======================================================================== */
/* String Operations                                                        */
/* ======================================================================== */

/**
 * string_ops_strlen:
 * Returns the codepoint length of interned string @intern_id, or -1 on error.
 */
int64_t
string_ops_strlen(int64_t intern_id, wl_intern_t *intern)
{
    if (!intern)
        return -1;
    const char *str = wl_intern_reverse(intern, intern_id);
    if (!str)
        return -1;
    return (int64_t)string_ops_utf8_strlen(str);
}

/**
 * string_ops_cat:
 * Concatenate two interned strings and return a new intern ID.
 * Returns -1 on error.
 */
int64_t
string_ops_cat(int64_t id1, int64_t id2, wl_intern_t *intern)
{
    if (!intern)
        return -1;

    const char *s1 = wl_intern_reverse(intern, id1);
    const char *s2 = wl_intern_reverse(intern, id2);
    if (!s1 || !s2)
        return -1;

    size_t len1 = strlen(s1);
    size_t len2 = strlen(s2);
    char *buf = (char *)malloc(len1 + len2 + 1);
    if (!buf)
        return -1;

    memcpy(buf, s1, len1);
    memcpy(buf + len1, s2, len2);
    buf[len1 + len2] = '\0';

    int64_t result = wl_intern_put(intern, buf);
    free(buf);
    return result;
}

/**
 * string_ops_substr:
 * Extract a substring from interned string @id.
 * @start: codepoint index (0-based).
 * @len:   number of codepoints to extract.
 * Returns new intern ID, or -1 on error/out-of-bounds.
 */
int64_t
string_ops_substr(int64_t id, int64_t start, int64_t len, wl_intern_t *intern)
{
    if (!intern || start < 0 || len < 0)
        return -1;

    const char *str = wl_intern_reverse(intern, id);
    if (!str)
        return -1;

    size_t byte_start = utf8_byte_offset(str, start);
    if (byte_start == (size_t)-1)
        return -1;

    /* Walk len codepoints from the start position to find end byte offset */
    const char *p = str + byte_start;
    int64_t i = 0;
    while (*p && i < len) {
        const char *next = string_ops_utf8_next(p, NULL);
        if (!next)
            break;
        p = next;
        i++;
    }
    size_t byte_len = (size_t)(p - (str + byte_start));

    char *buf = (char *)malloc(byte_len + 1);
    if (!buf)
        return -1;

    memcpy(buf, str + byte_start, byte_len);
    buf[byte_len] = '\0';

    int64_t result = wl_intern_put(intern, buf);
    free(buf);
    return result;
}

/**
 * string_ops_contains:
 * Return true if interned string @id contains @sub_id (byte-level search).
 */
bool
string_ops_contains(int64_t id, int64_t sub_id, wl_intern_t *intern)
{
    if (!intern)
        return false;

    const char *str = wl_intern_reverse(intern, id);
    const char *sub = wl_intern_reverse(intern, sub_id);
    if (!str || !sub)
        return false;

    return strstr(str, sub) != NULL;
}

/**
 * string_ops_str_prefix:
 * Return true if interned string @id starts with @prefix_id.
 */
bool
string_ops_str_prefix(int64_t id, int64_t prefix_id, wl_intern_t *intern)
{
    if (!intern)
        return false;

    const char *str = wl_intern_reverse(intern, id);
    const char *prefix = wl_intern_reverse(intern, prefix_id);
    if (!str || !prefix)
        return false;

    size_t prefix_len = strlen(prefix);
    return strncmp(str, prefix, prefix_len) == 0;
}

/**
 * string_ops_str_suffix:
 * Return true if interned string @id ends with @suffix_id.
 */
bool
string_ops_str_suffix(int64_t id, int64_t suffix_id, wl_intern_t *intern)
{
    if (!intern)
        return false;

    const char *str = wl_intern_reverse(intern, id);
    const char *suffix = wl_intern_reverse(intern, suffix_id);
    if (!str || !suffix)
        return false;

    size_t str_len = strlen(str);
    size_t suffix_len = strlen(suffix);
    if (suffix_len > str_len)
        return false;

    return memcmp(str + str_len - suffix_len, suffix, suffix_len) == 0;
}

/**
 * string_ops_str_ord:
 * Return the codepoint value of the first character of interned string @id.
 * Returns -1 if the string is empty or an error occurs.
 */
int64_t
string_ops_str_ord(int64_t id, wl_intern_t *intern)
{
    if (!intern)
        return -1;

    const char *str = wl_intern_reverse(intern, id);
    if (!str || !*str)
        return -1;

    int64_t cp = 0;
    string_ops_utf8_next(str, &cp);
    return cp;
}

/**
 * string_ops_to_upper:
 * Return a new intern ID for the uppercased version of @id.
 * Phase 1: ASCII-only (a-z -> A-Z; non-ASCII bytes are copied unchanged).
 */
int64_t
string_ops_to_upper(int64_t id, wl_intern_t *intern)
{
    if (!intern)
        return -1;

    const char *str = wl_intern_reverse(intern, id);
    if (!str)
        return -1;

    size_t len = strlen(str);
    char *buf = (char *)malloc(len + 1);
    if (!buf)
        return -1;

    for (size_t i = 0; i <= len; i++) {
        unsigned char c = (unsigned char)str[i];
        buf[i] = (char)((c >= 'a' && c <= 'z') ? c - 32u : c);
    }

    int64_t result = wl_intern_put(intern, buf);
    free(buf);
    return result;
}

/**
 * string_ops_to_lower:
 * Return a new intern ID for the lowercased version of @id.
 * Phase 1: ASCII-only (A-Z -> a-z; non-ASCII bytes are copied unchanged).
 */
int64_t
string_ops_to_lower(int64_t id, wl_intern_t *intern)
{
    if (!intern)
        return -1;

    const char *str = wl_intern_reverse(intern, id);
    if (!str)
        return -1;

    size_t len = strlen(str);
    char *buf = (char *)malloc(len + 1);
    if (!buf)
        return -1;

    for (size_t i = 0; i <= len; i++) {
        unsigned char c = (unsigned char)str[i];
        buf[i] = (char)((c >= 'A' && c <= 'Z') ? c + 32u : c);
    }

    int64_t result = wl_intern_put(intern, buf);
    free(buf);
    return result;
}

/**
 * string_ops_str_replace:
 * Replace all occurrences of @old_id in @id with @new_id.
 * Returns a new intern ID, or -1 on error.
 * If @old_id is the empty string, returns @id unchanged.
 */
int64_t
string_ops_str_replace(int64_t id, int64_t old_id, int64_t new_id,
    wl_intern_t *intern)
{
    if (!intern)
        return -1;

    const char *str = wl_intern_reverse(intern, id);
    const char *old_str = wl_intern_reverse(intern, old_id);
    const char *new_str = wl_intern_reverse(intern, new_id);
    if (!str || !old_str || !new_str)
        return -1;

    size_t old_len = strlen(old_str);
    if (old_len == 0)
        return id; /* empty pattern: return original to avoid infinite loop */

    size_t new_len = strlen(new_str);
    size_t str_len = strlen(str);

    /* Count occurrences to compute exact result size */
    size_t count = 0;
    const char *p = str;
    while ((p = strstr(p, old_str)) != NULL) {
        count++;
        p += old_len;
    }

    if (count == 0)
        return id;

    /* Compute result buffer size (careful with signed arithmetic) */
    size_t result_size;
    if (new_len >= old_len) {
        result_size = str_len + count * (new_len - old_len) + 1;
    } else {
        result_size = str_len - count * (old_len - new_len) + 1;
    }

    char *buf = (char *)malloc(result_size);
    if (!buf)
        return -1;

    char *dst = buf;
    p = str;
    const char *match;
    while ((match = strstr(p, old_str)) != NULL) {
        size_t prefix_len = (size_t)(match - p);
        memcpy(dst, p, prefix_len);
        dst += prefix_len;
        memcpy(dst, new_str, new_len);
        dst += new_len;
        p = match + old_len;
    }
    size_t tail_len = strlen(p);
    memcpy(dst, p, tail_len);
    dst[tail_len] = '\0';

    int64_t result = wl_intern_put(intern, buf);
    free(buf);
    return result;
}

/**
 * string_ops_trim:
 * Return a new intern ID for @id with leading and trailing ASCII whitespace removed.
 */
int64_t
string_ops_trim(int64_t id, wl_intern_t *intern)
{
    if (!intern)
        return -1;

    const char *str = wl_intern_reverse(intern, id);
    if (!str)
        return -1;

    /* Find first non-whitespace byte */
    const char *start = str;
    while (*start && isspace((unsigned char)*start))
        start++;

    /* Find last non-whitespace byte */
    const char *end = str + strlen(str);
    while (end > start && isspace((unsigned char)*(end - 1)))
        end--;

    size_t len = (size_t)(end - start);
    char *buf = (char *)malloc(len + 1);
    if (!buf)
        return -1;

    memcpy(buf, start, len);
    buf[len] = '\0';

    int64_t result = wl_intern_put(intern, buf);
    free(buf);
    return result;
}

/**
 * string_ops_to_string:
 * Convert integer @num to its decimal string representation and intern it.
 * Returns new intern ID, or -1 on error.
 */
int64_t
string_ops_to_string(int64_t num, wl_intern_t *intern)
{
    if (!intern)
        return -1;

    /* INT64_MIN in decimal is at most 20 digits + sign + NUL */
    char buf[24];
    int written = snprintf(buf, sizeof(buf), "%" PRId64, num);
    if (written < 0 || (size_t)written >= sizeof(buf))
        return -1;

    return wl_intern_put(intern, buf);
}

/**
 * string_ops_to_number:
 * Parse interned string @id as a base-10 integer.
 * Parses a numeric prefix; returns 0 if the string has no valid numeric prefix.
 */
int64_t
string_ops_to_number(int64_t id, wl_intern_t *intern)
{
    if (!intern)
        return 0;

    const char *str = wl_intern_reverse(intern, id);
    if (!str || !*str)
        return 0;

    char *endptr = NULL;
    int64_t val = (int64_t)strtoll(str, &endptr, 10);
    if (endptr == str)
        return 0; /* no valid numeric prefix */

    return val;
}
