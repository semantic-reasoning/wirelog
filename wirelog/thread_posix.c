/*
 * thread_posix.c - wirelog POSIX Threading Backend (pthread)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL - not installed, not part of public API.
 *
 * POSIX (pthread) implementation of the threading abstraction layer
 * defined in thread.h. This backend is used on Unix/Linux/macOS platforms.
 */

#include "thread.h"

#include <pthread.h>
#include <stdlib.h>

/* ======================================================================== */
/* Thread Creation and Joining                                              */
/* ======================================================================== */

int
thread_create(thread_t *tid, void *(*fn)(void *arg), void *arg)
{
    if (!tid || !fn)
        return -1;

    tid->tid = 0; /* Clear in case of failure */

    int result = pthread_create(&tid->tid, NULL, fn, arg);
    return (result == 0) ? 0 : -1;
}

int
thread_join(thread_t *tid)
{
    if (!tid)
        return -1;

    int result = pthread_join(tid->tid, NULL);
    return (result == 0) ? 0 : -1;
}

/* ======================================================================== */
/* Mutex (Mutual Exclusion Lock)                                            */
/* ======================================================================== */

int
mutex_init(mutex_t *m)
{
    if (!m)
        return -1;

    int result = pthread_mutex_init(&m->m, NULL);
    return (result == 0) ? 0 : -1;
}

int
mutex_lock(mutex_t *m)
{
    if (!m)
        return -1;

    int result = pthread_mutex_lock(&m->m);
    return (result == 0) ? 0 : -1;
}

int
mutex_unlock(mutex_t *m)
{
    if (!m)
        return -1;

    int result = pthread_mutex_unlock(&m->m);
    return (result == 0) ? 0 : -1;
}

void
mutex_destroy(mutex_t *m)
{
    if (!m)
        return;

    pthread_mutex_destroy(&m->m);
}

/* ======================================================================== */
/* Condition Variable                                                        */
/* ======================================================================== */

int
cond_init(cond_t *c)
{
    if (!c)
        return -1;

    int result = pthread_cond_init(&c->c, NULL);
    return (result == 0) ? 0 : -1;
}

int
cond_wait(cond_t *c, mutex_t *m)
{
    if (!c || !m)
        return -1;

    int result = pthread_cond_wait(&c->c, &m->m);
    return (result == 0) ? 0 : -1;
}

int
cond_signal(cond_t *c)
{
    if (!c)
        return -1;

    int result = pthread_cond_signal(&c->c);
    return (result == 0) ? 0 : -1;
}

int
cond_broadcast(cond_t *c)
{
    if (!c)
        return -1;

    int result = pthread_cond_broadcast(&c->c);
    return (result == 0) ? 0 : -1;
}

void
cond_destroy(cond_t *c)
{
    if (!c)
        return;

    pthread_cond_destroy(&c->c);
}
