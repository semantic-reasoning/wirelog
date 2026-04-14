/*
 * thread_c11.c - wirelog C11 Threading Backend (threads.h)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL - not installed, not part of public API.
 *
 * C11 standard threads implementation of the threading abstraction layer
 * defined in thread.h. This backend is preferred when the compiler and
 * platform support <threads.h> (e.g., GCC 10+/glibc 2.28+, musl).
 * On platforms without C11 threads support (Apple Clang, older MSVC),
 * the POSIX or MSVC backend is used instead.
 *
 * Issue #494: Migrate from POSIX pthreads to C11 <threads.h>
 */

#include "thread.h"

#include <threads.h>
#include <stdlib.h>

/* ======================================================================== */
/* Trampoline for C11 thrd_create                                           */
/* ======================================================================== */

/*
 * C11 thrd_create expects int(*)(void*), but wirelog's thread_create
 * uses void*(*)(void*) (matching the pthread signature).  We use a
 * thin trampoline to bridge the two.
 */
typedef struct {
    void *(*fn)(void *);
    void *arg;
} trampoline_ctx_t;

static int
trampoline(void *raw)
{
    trampoline_ctx_t ctx = *(trampoline_ctx_t *)raw;
    free(raw);
    ctx.fn(ctx.arg);
    return 0;
}

/* ======================================================================== */
/* Thread Creation and Joining                                              */
/* ======================================================================== */

int
thread_create(thread_t *tid, void *(*fn)(void *arg), void *arg)
{
    if (!tid || !fn)
        return -1;

    trampoline_ctx_t *ctx = malloc(sizeof(*ctx));
    if (!ctx)
        return -1;

    ctx->fn = fn;
    ctx->arg = arg;

    int result = thrd_create(&tid->tid, trampoline, ctx);
    if (result != thrd_success) {
        free(ctx);
        return -1;
    }
    return 0;
}

int
thread_join(thread_t *tid)
{
    if (!tid)
        return -1;

    int result = thrd_join(tid->tid, NULL);
    return (result == thrd_success) ? 0 : -1;
}

/* ======================================================================== */
/* Mutex (Mutual Exclusion Lock)                                            */
/* ======================================================================== */

int
mutex_init(mutex_t *m)
{
    if (!m)
        return -1;

    int result = mtx_init(&m->m, mtx_plain);
    return (result == thrd_success) ? 0 : -1;
}

int
mutex_lock(mutex_t *m)
{
    if (!m)
        return -1;

    int result = mtx_lock(&m->m);
    return (result == thrd_success) ? 0 : -1;
}

int
mutex_unlock(mutex_t *m)
{
    if (!m)
        return -1;

    int result = mtx_unlock(&m->m);
    return (result == thrd_success) ? 0 : -1;
}

void
mutex_destroy(mutex_t *m)
{
    if (!m)
        return;

    mtx_destroy(&m->m);
}

/* ======================================================================== */
/* Condition Variable                                                        */
/* ======================================================================== */

int
cond_init(cond_t *c)
{
    if (!c)
        return -1;

    int result = cnd_init(&c->c);
    return (result == thrd_success) ? 0 : -1;
}

int
cond_wait(cond_t *c, mutex_t *m)
{
    if (!c || !m)
        return -1;

    int result = cnd_wait(&c->c, &m->m);
    return (result == thrd_success) ? 0 : -1;
}

int
cond_signal(cond_t *c)
{
    if (!c)
        return -1;

    int result = cnd_signal(&c->c);
    return (result == thrd_success) ? 0 : -1;
}

int
cond_broadcast(cond_t *c)
{
    if (!c)
        return -1;

    int result = cnd_broadcast(&c->c);
    return (result == thrd_success) ? 0 : -1;
}

void
cond_destroy(cond_t *c)
{
    if (!c)
        return;

    cnd_destroy(&c->c);
}
