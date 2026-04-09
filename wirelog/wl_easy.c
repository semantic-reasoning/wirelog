/*
 * wl_easy.c - wirelog convenience facade (Issue #441)
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
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.  If not, see
 * <https://www.gnu.org/licenses/lgpl-3.0.html>.
 *
 * Stub implementation: this file is intentionally a no-op skeleton in the
 * first commit so the public surface compiles and links.  The full
 * implementation lands in a follow-up commit on the same branch.
 */

#include "wirelog/wl_easy.h"

#include <stddef.h>

wirelog_error_t
wl_easy_open(const char *dl_src, wl_easy_session_t **out)
{
    (void)dl_src;
    if (out)
        *out = NULL;
    return WIRELOG_ERR_EXEC;
}

void
wl_easy_close(wl_easy_session_t *s)
{
    (void)s;
}

int64_t
wl_easy_intern(wl_easy_session_t *s, const char *sym)
{
    (void)s;
    (void)sym;
    return -1;
}

wirelog_error_t
wl_easy_insert(wl_easy_session_t *s, const char *relation, const int64_t *row,
    uint32_t ncols)
{
    (void)s;
    (void)relation;
    (void)row;
    (void)ncols;
    return WIRELOG_ERR_EXEC;
}

wirelog_error_t
wl_easy_remove(wl_easy_session_t *s, const char *relation, const int64_t *row,
    uint32_t ncols)
{
    (void)s;
    (void)relation;
    (void)row;
    (void)ncols;
    return WIRELOG_ERR_EXEC;
}

wirelog_error_t
wl_easy_insert_sym(wl_easy_session_t *s, const char *relation, ...)
{
    (void)s;
    (void)relation;
    return WIRELOG_ERR_EXEC;
}

wirelog_error_t
wl_easy_remove_sym(wl_easy_session_t *s, const char *relation, ...)
{
    (void)s;
    (void)relation;
    return WIRELOG_ERR_EXEC;
}

wirelog_error_t
wl_easy_step(wl_easy_session_t *s)
{
    (void)s;
    return WIRELOG_ERR_EXEC;
}

void
wl_easy_set_delta_cb(wl_easy_session_t *s, wl_on_delta_fn cb, void *user_data)
{
    (void)s;
    (void)cb;
    (void)user_data;
}

void
wl_easy_print_delta(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data)
{
    (void)relation;
    (void)row;
    (void)ncols;
    (void)diff;
    (void)user_data;
}

void
wl_easy_banner(const char *label)
{
    (void)label;
}

wirelog_error_t
wl_easy_snapshot(wl_easy_session_t *s, const char *relation, wl_on_tuple_fn cb,
    void *user_data)
{
    (void)s;
    (void)relation;
    (void)cb;
    (void)user_data;
    return WIRELOG_ERR_EXEC;
}
