/*
 * thread_msvc.c - wirelog Windows Threading Backend (MSVC)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL - not installed, not part of public API.
 *
 * MSVC/Windows implementation of the threading abstraction layer
 * defined in thread.h. This backend is used on Windows platforms.
 *
 * Platform support:
 *   - Windows Vista and later (CONDITION_VARIABLE support)
 *   - MSVC, MinGW, Clang-cl, and compatible compilers
 */

#include "thread.h"

#if defined(_WIN32) || defined(_WIN64)
#include <windows.h>
#include <stdlib.h>

/* ======================================================================== */
/* Worker Wrapper                                                            */
/* ======================================================================== */

/**
 * thread_wrapper:
 *
 * Wrapper function to adapt from LPTHREAD_START_ROUTINE signature
 * (expected by CreateThread) to the thread.h signature (void *(*fn)(void *)).
 *
 * CreateThread expects: DWORD WINAPI ThreadFunc(LPVOID param)
 * thread.h expects:    void *(*fn)(void *arg)
 *
 * This wrapper bridges the difference.
 */
static DWORD WINAPI
thread_wrapper(LPVOID arg)
{
    typedef struct {
        void *(*fn)(void *);
        void *ctx;
    } wrapper_args_t;

    wrapper_args_t *args = (wrapper_args_t *)arg;
    if (!args)
        return 1; /* Error */

    void *(*fn)(void *) = args->fn;
    void *ctx = args->ctx;
    free(args);

    fn(ctx);
    return 0; /* Success */
}

/* ======================================================================== */
/* Thread Creation and Joining                                              */
/* ======================================================================== */

int
thread_create(thread_t *tid, void *(*fn)(void *arg), void *arg)
{
    if (!tid || !fn)
        return -1;

    /* Allocate wrapper args (freed by the worker thread) */
    typedef struct {
        void *(*fn)(void *);
        void *ctx;
    } wrapper_args_t;

    wrapper_args_t *args = (wrapper_args_t *)malloc(sizeof(wrapper_args_t));
    if (!args)
        return -1;

    args->fn = fn;
    args->ctx = arg;

    /* Create thread */
    tid->handle
        = CreateThread(NULL,           /* lpThreadAttributes: use defaults */
                       0,              /* dwStackSize: use default */
                       thread_wrapper, /* lpStartAddress: thread function */
                       (LPVOID)args,   /* lpParameter: wrapper args */
                       0,              /* dwCreationFlags: start immediately */
                       NULL            /* lpThreadId: don't need the ID */
        );

    if (!tid->handle) {
        free(args);
        return -1;
    }

    return 0;
}

int
thread_join(thread_t *tid)
{
    if (!tid || !tid->handle)
        return -1;

    DWORD result = WaitForSingleObject(tid->handle, INFINITE);
    if (result != WAIT_OBJECT_0)
        return -1;

    if (!CloseHandle(tid->handle))
        return -1;

    tid->handle = NULL;
    return 0;
}

/* ======================================================================== */
/* Mutex (Mutual Exclusion Lock)                                            */
/* ======================================================================== */

int
mutex_init(mutex_t *m)
{
    if (!m)
        return -1;

    if (!InitializeCriticalSection(&m->cs))
        return -1;

    return 0;
}

int
mutex_lock(mutex_t *m)
{
    if (!m)
        return -1;

    EnterCriticalSection(&m->cs);
    return 0;
}

int
mutex_unlock(mutex_t *m)
{
    if (!m)
        return -1;

    LeaveCriticalSection(&m->cs);
    return 0;
}

void
mutex_destroy(mutex_t *m)
{
    if (!m)
        return;

    DeleteCriticalSection(&m->cs);
}

/* ======================================================================== */
/* Condition Variable                                                        */
/* ======================================================================== */

int
cond_init(cond_t *c)
{
    if (!c)
        return -1;

    InitializeConditionVariable(&c->cv);
    return 0;
}

int
cond_wait(cond_t *c, mutex_t *m)
{
    if (!c || !m)
        return -1;

    if (!SleepConditionVariableCS(&c->cv, &m->cs, INFINITE))
        return -1;

    return 0;
}

int
cond_signal(cond_t *c)
{
    if (!c)
        return -1;

    WakeConditionVariable(&c->cv);
    return 0;
}

int
cond_broadcast(cond_t *c)
{
    if (!c)
        return -1;

    WakeAllConditionVariable(&c->cv);
    return 0;
}

void
cond_destroy(cond_t *c)
{
    if (!c)
        return;

    /* CONDITION_VARIABLE doesn't require explicit destruction */
    (void)c;
}

#else /* !_WIN32 && !_WIN64 */

/*
 * Stub implementations for non-Windows platforms.
 * This should not be compiled on non-Windows platforms; this is just a
 * safety measure.
 */

int
thread_create(thread_t *tid, void *(*fn)(void *arg), void *arg)
{
    (void)tid;
    (void)fn;
    (void)arg;
    return -1;
}

int
thread_join(thread_t *tid)
{
    (void)tid;
    return -1;
}

int
mutex_init(mutex_t *m)
{
    (void)m;
    return -1;
}

int
mutex_lock(mutex_t *m)
{
    (void)m;
    return -1;
}

int
mutex_unlock(mutex_t *m)
{
    (void)m;
    return -1;
}

void
mutex_destroy(mutex_t *m)
{
    (void)m;
}

int
cond_init(cond_t *c)
{
    (void)c;
    return -1;
}

int
cond_wait(cond_t *c, mutex_t *m)
{
    (void)c;
    (void)m;
    return -1;
}

int
cond_signal(cond_t *c)
{
    (void)c;
    return -1;
}

int
cond_broadcast(cond_t *c)
{
    (void)c;
    return -1;
}

void
cond_destroy(cond_t *c)
{
    (void)c;
}

#endif /* _WIN32 || _WIN64 */
