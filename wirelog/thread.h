/*
 * thread.h - wirelog Cross-Platform Thread Abstraction
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 *
 * ========================================================================
 * Overview
 * ========================================================================
 *
 * Platform-agnostic threading interface with three backends, selected
 * at build time by meson:
 *
 *   1. C11 <threads.h>  (preferred, Linux GCC 10+/glibc 2.28+, musl)
 *   2. POSIX pthreads    (fallback, macOS Apple Clang, older Linux)
 *   3. Windows MSVC      (CreateThread, CRITICAL_SECTION, etc.)
 *
 * Detection order: WL_HAVE_C11_THREADS > _WIN32 > POSIX (default).
 *
 * ========================================================================
 * Thread Safety Guarantees
 * ========================================================================
 *
 * All functions defined here provide the same synchronization semantics
 * as their pthread equivalents:
 *
 * - thread_create(): Creates a new thread; caller must thread_join() to wait
 * - mutex_lock/unlock: Mutual exclusion with acquisition/release semantics
 * - cond_wait/signal: Condition variable signaling with proper synchronization
 * - All operations are NOT recursive (standard mutex semantics)
 *
 * ========================================================================
 * Usage Pattern
 * ========================================================================
 *
 * Instead of:
 *   pthread_t tid;
 *   pthread_mutex_t lock;
 *   pthread_cond_t cond;
 *
 * Use:
 *   thread_t tid;
 *   mutex_t lock;
 *   cond_t cond;
 *
 * And call thread_create(), mutex_lock(), cond_wait() instead of
 * pthread_create(), pthread_mutex_lock(), pthread_cond_wait().
 */

#ifndef WL_THREAD_H
#define WL_THREAD_H

#include <stddef.h>

/* ======================================================================== */
/* Platform-Specific Includes                                               */
/* ======================================================================== */

#if defined(WL_HAVE_C11_THREADS)
#include <threads.h>
#elif defined(_WIN32) || defined(_WIN64)
#include <windows.h>
#else
#include <pthread.h>
#endif

/* ======================================================================== */
/* Type Definitions                                                          */
/* ======================================================================== */

/**
 * thread_t:
 *
 * Thread handle. Backend-specific:
 *   C11:   thrd_t
 *   POSIX: pthread_t
 *   Win32: HANDLE
 */
#if defined(WL_HAVE_C11_THREADS)
typedef struct thread_t {
    thrd_t tid;
} thread_t;
#elif defined(_WIN32) || defined(_WIN64)
typedef struct thread_t {
    HANDLE handle;
} thread_t;
#else
typedef struct thread_t {
    pthread_t tid;
} thread_t;
#endif

/**
 * mutex_t:
 *
 * Mutex (mutual exclusion lock). Backend-specific:
 *   C11:   mtx_t
 *   POSIX: pthread_mutex_t
 *   Win32: CRITICAL_SECTION
 *
 * Non-recursive semantics on all backends.
 *
 * NOTE: Windows CRITICAL_SECTION is reentrant/recursive by default, but
 * the public interface guarantees non-recursive semantics. Code must not
 * rely on self-reentrance.
 */
#if defined(WL_HAVE_C11_THREADS)
typedef struct mutex_t {
    mtx_t m;
} mutex_t;
#elif defined(_WIN32) || defined(_WIN64)
typedef struct mutex_t {
    CRITICAL_SECTION cs;
} mutex_t;
#else
typedef struct mutex_t {
    pthread_mutex_t m;
} mutex_t;
#endif

/**
 * cond_t:
 *
 * Condition variable. Backend-specific:
 *   C11:   cnd_t
 *   POSIX: pthread_cond_t
 *   Win32: CONDITION_VARIABLE
 */
#if defined(WL_HAVE_C11_THREADS)
typedef struct cond_t {
    cnd_t c;
} cond_t;
#elif defined(_WIN32) || defined(_WIN64)
typedef struct cond_t {
    CONDITION_VARIABLE cv;
} cond_t;
#else
typedef struct cond_t {
    pthread_cond_t c;
} cond_t;
#endif

/* ======================================================================== */
/* Thread Creation and Joining                                              */
/* ======================================================================== */

/**
 * thread_create:
 * @tid:     (out) Thread handle to fill in. Caller must pass valid pointer.
 * @fn:      Function to execute on the new thread. Must not be NULL.
 * @arg:     Argument passed to @fn. May be NULL.
 *
 * Create a new thread and execute @fn(arg) on it.
 *
 * Returns:
 *    0: Success. @tid is filled with a valid thread handle. The caller
 *       must call thread_join(@tid) to wait for thread completion.
 *   -1: Failure (allocation, system limit, invalid arguments).
 *       @tid is left in an undefined state; do NOT pass to thread_join().
 *
 * Thread safety: Safe to call from any thread.
 */
int
thread_create(thread_t *tid, void *(*fn)(void *arg), void *arg);

/**
 * thread_join:
 * @tid: Thread handle to join. Must be a valid handle from thread_create().
 *
 * Block the calling thread until the thread identified by @tid completes.
 * After this returns, the thread has exited and its handle is invalid.
 *
 * Returns:
 *    0: Success. The thread has exited.
 *   -1: Error (invalid handle, already joined, etc.).
 *
 * Thread safety: Do NOT call thread_join() from multiple threads on the
 * same @tid. Call once, and only once.
 */
int
thread_join(thread_t *tid);

/* ======================================================================== */
/* Mutex (Mutual Exclusion Lock)                                            */
/* ======================================================================== */

/**
 * mutex_init:
 * @m: (out) Mutex to initialize. Caller must pass valid pointer.
 *
 * Initialize a mutex to the unlocked state.
 *
 * Returns:
 *    0: Success. @m is ready for mutex_lock/unlock operations.
 *   -1: Failure (allocation, system limit). @m is left uninitialized.
 *       Do NOT pass @m to mutex_lock/unlock/destroy.
 *
 * Thread safety: Safe to call once. Do NOT call multiple times on the
 * same @m without calling mutex_destroy() first.
 */
int
mutex_init(mutex_t *m);

/**
 * mutex_lock:
 * @m: Mutex to lock. Must be a valid mutex from mutex_init().
 *
 * Acquire the mutex. If the mutex is already held by another thread,
 * the calling thread blocks until the mutex is released.
 *
 * This is a standard (non-recursive) mutex: if the current thread already
 * holds the mutex, the behavior is undefined (typically deadlock).
 *
 * Returns:
 *    0: Success. The mutex is now held by the calling thread.
 *   -1: Error (invalid handle, system error).
 *
 * Thread safety: Safe to call from any thread. Only one thread can hold
 * the mutex at a time.
 */
int
mutex_lock(mutex_t *m);

/**
 * mutex_unlock:
 * @m: Mutex to unlock. Must be held by the calling thread.
 *
 * Release the mutex. The calling thread must have called mutex_lock(@m)
 * and not yet called mutex_unlock(@m). If the calling thread does not
 * hold the mutex, behavior is undefined (likely error or hang).
 *
 * If one or more threads are blocked in mutex_lock(), one of them
 * is awakened and will proceed.
 *
 * Returns:
 *    0: Success. The mutex has been released.
 *   -1: Error (not held by calling thread, invalid handle, etc.).
 *
 * Thread safety: Must only be called by the thread that holds the mutex.
 */
int
mutex_unlock(mutex_t *m);

/**
 * mutex_destroy:
 * @m: (transfer full) Mutex to destroy. NULL-safe.
 *
 * Release all resources associated with the mutex. The mutex must not
 * be locked when this is called, and must not be used afterwards.
 *
 * If @m is NULL, this is a no-op.
 *
 * Thread safety: The caller must ensure no other thread is accessing
 * the mutex when this is called.
 */
void
mutex_destroy(mutex_t *m);

/* ======================================================================== */
/* Condition Variable                                                        */
/* ======================================================================== */

/**
 * cond_init:
 * @c: (out) Condition variable to initialize. Caller must pass valid pointer.
 *
 * Initialize a condition variable.
 *
 * Returns:
 *    0: Success. @c is ready for cond_wait/signal/broadcast operations.
 *   -1: Failure (allocation, system limit). @c is left uninitialized.
 *       Do NOT pass @c to cond_wait/signal/broadcast/destroy.
 *
 * Thread safety: Safe to call once. Do NOT call multiple times on the
 * same @c without calling cond_destroy() first.
 */
int
cond_init(cond_t *c);

/**
 * cond_wait:
 * @c:     Condition variable to wait on. Must be initialized.
 * @m:     Mutex to release/acquire. Must be held by the calling thread.
 *
 * Atomically release the mutex @m and wait for a signal on the condition
 * variable @c. When awakened (by cond_signal or cond_broadcast), the mutex
 * is re-acquired before this function returns.
 *
 * Spurious wakeups are possible: the caller may awaken without an explicit
 * signal. Therefore, the condition should be checked again after return.
 * Typical pattern:
 *
 *   while (!condition_is_true) {
 *       if (cond_wait(&c, &m) != 0) return error;
 *   }
 *
 * Returns:
 *    0: Awakened (either by signal or timeout if supported).
 *   -1: Error (invalid arguments, system error).
 *
 * Thread safety: Must be called with @m held by the calling thread.
 */
int
cond_wait(cond_t *c, mutex_t *m);

/**
 * cond_signal:
 * @c: Condition variable to signal. Must be initialized.
 *
 * Wake up one thread that is blocked in cond_wait() on @c.
 * If no threads are blocked, the signal is lost.
 *
 * Does NOT release any mutex. The calling thread must manually release
 * the associated mutex after signaling (or before, depending on the
 * synchronization pattern).
 *
 * Returns:
 *    0: Success. One waiting thread (if any) has been awakened.
 *   -1: Error (invalid handle, system error).
 *
 * Thread safety: Safe to call from any thread. Typically called while
 * holding the associated mutex to avoid race conditions.
 */
int
cond_signal(cond_t *c);

/**
 * cond_broadcast:
 * @c: Condition variable to broadcast to. Must be initialized.
 *
 * Wake up ALL threads that are blocked in cond_wait() on @c.
 * If no threads are blocked, the broadcast is a no-op.
 *
 * Does NOT release any mutex. The calling thread must manually release
 * the associated mutex after broadcasting.
 *
 * Returns:
 *    0: Success. All waiting threads (if any) have been awakened.
 *   -1: Error (invalid handle, system error).
 *
 * Thread safety: Safe to call from any thread. Typically called while
 * holding the associated mutex.
 */
int
cond_broadcast(cond_t *c);

/**
 * cond_destroy:
 * @c: (transfer full) Condition variable to destroy. NULL-safe.
 *
 * Release all resources associated with the condition variable.
 * The condition variable must not be in use (no threads blocked in
 * cond_wait), and must not be used afterwards.
 *
 * If @c is NULL, this is a no-op.
 *
 * Thread safety: The caller must ensure no other thread is accessing
 * the condition variable when this is called.
 */
void
cond_destroy(cond_t *c);

#endif /* WL_THREAD_H */
