/*
 * string_ops.h - String Operations Library
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 * UTF-8-aware string operations that operate on interned string IDs.
 */

#ifndef WIRELOG_STRING_OPS_H
#define WIRELOG_STRING_OPS_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include "wirelog/intern.h"

/* UTF-8 utilities */
size_t string_ops_utf8_strlen(const char *str); /* codepoint count */
const char *string_ops_utf8_next(const char *str, int64_t *codepoint); /* next codepoint */

/* String functions */
int64_t string_ops_strlen(int64_t intern_id, wl_intern_t *intern);
int64_t string_ops_cat(int64_t id1, int64_t id2, wl_intern_t *intern);
int64_t string_ops_substr(int64_t id, int64_t start, int64_t len,
    wl_intern_t *intern);
bool string_ops_contains(int64_t id, int64_t sub_id, wl_intern_t *intern);
bool string_ops_str_prefix(int64_t id, int64_t prefix_id, wl_intern_t *intern);
bool string_ops_str_suffix(int64_t id, int64_t suffix_id, wl_intern_t *intern);
int64_t string_ops_str_ord(int64_t id, wl_intern_t *intern);
int64_t string_ops_to_upper(int64_t id, wl_intern_t *intern);
int64_t string_ops_to_lower(int64_t id, wl_intern_t *intern);
int64_t string_ops_str_replace(int64_t id, int64_t old_id, int64_t new_id,
    wl_intern_t *intern);
int64_t string_ops_trim(int64_t id, wl_intern_t *intern);
int64_t string_ops_to_string(int64_t num, wl_intern_t *intern);
int wl_string_ops_to_number_checked(int64_t id, wl_intern_t *intern,
    int64_t *out);

/* Checked variants of the producing builtins (Issue #1470).  They return 0
 * with the interned result in *out, EINVAL when the table is missing, an
 * operand is not interned or an argument is out of range, and ENOMEM when
 * the result buffer or its interning was refused (including a memory
 * governor denial of the table's growth).  The int64_t forms above return
 * -1 for every failure and stay unchanged for existing callers. */
int wl_string_ops_cat_checked(int64_t id1, int64_t id2, wl_intern_t *intern,
    int64_t *out);
int wl_string_ops_substr_checked(int64_t id, int64_t start, int64_t len,
    wl_intern_t *intern, int64_t *out);
int wl_string_ops_to_upper_checked(int64_t id, wl_intern_t *intern,
    int64_t *out);
int wl_string_ops_to_lower_checked(int64_t id, wl_intern_t *intern,
    int64_t *out);
int wl_string_ops_str_replace_checked(int64_t id, int64_t old_id,
    int64_t new_id, wl_intern_t *intern, int64_t *out);
int wl_string_ops_trim_checked(int64_t id, wl_intern_t *intern, int64_t *out);
int wl_string_ops_to_string_checked(int64_t num, wl_intern_t *intern,
    int64_t *out);
int64_t string_ops_to_number(int64_t id, wl_intern_t *intern);

#endif /* WIRELOG_STRING_OPS_H */
