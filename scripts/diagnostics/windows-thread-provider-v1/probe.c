/* Finite live-thread and ABI observations. Never compare an expired token. */
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#ifdef WL_DIAG_C11
#include <threads.h>
typedef thrd_t wl_diagnostic_token_t;
#else
typedef DWORD wl_diagnostic_token_t;
#endif

wl_diagnostic_token_t wl_diagnostic_thread_current(void);
int wl_diagnostic_thread_equal(wl_diagnostic_token_t, wl_diagnostic_token_t);

typedef struct {
    HANDLE ready;
    HANDLE release;
    wl_diagnostic_token_t token;
    int self_equal;
} wl_diagnostic_worker_t;

/* This kills the whole diagnostic process, never only a worker. In particular
* it cannot unwind/free an automatic payload under an unjoined live worker. */
__declspec(noreturn) static void
wl_diagnostic_fatal(unsigned code)
{
    printf("{\"kind\":\"fatal\",\"code\":%u}\n", code);
    fflush(stdout);
    ExitProcess(code);
}

static void
wl_diagnostic_json_string(const char *text)
{
    const unsigned char *p = (const unsigned char *)text;
    putchar('"');
    for (; *p; ++p) {
        if (*p == '"' || *p == '\\') {
            putchar('\\');
            putchar(*p);
        } else if (*p < 32) {
            printf("\\u%04x", (unsigned)*p);
        } else putchar(*p);
    }
    putchar('"');
}

/* No PE/IAT parsing or decoded-address execution. The compiler-linked address
 * may be an executable thunk. GetProcAddress is a separately labeled candidate,
 * never silently promoted to the actual compiled call's final provider. */
static void
wl_diagnostic_module(const char *symbol, uintptr_t address, int candidate)
{
    HMODULE module = NULL;
    wchar_t path[32768];
    char utf8[32768];
    DWORD length;
    MEMORY_BASIC_INFORMATION info;
    if (!VirtualQuery((const void *)address, &info, sizeof(info))
        || !GetModuleHandleExW(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS
        | GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT,
        (LPCWSTR)address, &module)) wl_diagnostic_fatal(30);
    length = GetModuleFileNameW(module, path,
            (DWORD)(sizeof(path)/sizeof(*path)));
    if (!length || length >= sizeof(path)/sizeof(*path)
        || !WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, path, -1,
        utf8, (int)sizeof(utf8), NULL, NULL)) wl_diagnostic_fatal(31);
    printf("{\"kind\":\"module\",\"symbol\":");
    wl_diagnostic_json_string(symbol);
    printf(",\"address_kind\":\"%s\",\"address\":\"0x%llx\","
        "\"base\":\"0x%llx\",\"offset\":\"0x%llx\",\"module_path\":",
        candidate ? "GetProcAddress_candidate" : "compiler_linked",
        (unsigned long long)address, (unsigned long long)(uintptr_t)module,
        (unsigned long long)(address - (uintptr_t)module));
    wl_diagnostic_json_string(utf8);
    printf(",\"provider_binding\":\"UNKNOWN_UNQUALIFIED\"}\n");
    if (!candidate) {
        FARPROC resolved = GetProcAddress(module, symbol);
        if (resolved) wl_diagnostic_module(symbol, (uintptr_t)resolved, 1);
        else {
            printf("{\"kind\":\"resolution\",\"symbol\":");
            wl_diagnostic_json_string(symbol);
            printf(",\"status\":\"UNRESOLVED_IMPORT_THUNK_OR_EXPORT\"}\n");
        }
    }
}

static int
wl_diagnostic_worker_body(void *argument)
{
    wl_diagnostic_worker_t *worker = argument;
    worker->token = wl_diagnostic_thread_current();
    worker->self_equal = wl_diagnostic_thread_equal(worker->token,
            wl_diagnostic_thread_current()) != 0;
    if (!SetEvent(worker->ready)) wl_diagnostic_fatal(40);
    if (WaitForSingleObject(worker->release, 30000) != WAIT_OBJECT_0)
        wl_diagnostic_fatal(41);
    return worker->self_equal ? 0 : 42;
}

#ifndef WL_DIAG_C11
static DWORD WINAPI
wl_diagnostic_native_worker(LPVOID argument)
{
    return (DWORD)wl_diagnostic_worker_body(argument);
}
#endif

int
main(void)
{
    wl_diagnostic_worker_t worker;
    wl_diagnostic_token_t first;
    wl_diagnostic_token_t second;
    int same, different_lr, different_rl, foreign_self;
#ifdef WL_DIAG_C11
    thrd_t thread;
    int joined;
    printf("{\"kind\":\"layout\",\"profile\":\"MSVC-C11\","
        "\"token_size\":%llu,\"token_align\":%llu,"
        "\"handle_offset\":%llu,\"handle_size\":%llu,"
        "\"tid_offset\":%llu,\"tid_size\":%llu,\"pointer_size\":%llu}\n",
        (unsigned long long)sizeof(thrd_t),
        (unsigned long long)__alignof(thrd_t),
        (unsigned long long)offsetof(thrd_t, _Handle),
        (unsigned long long)sizeof(((thrd_t *)0)->_Handle),
        (unsigned long long)offsetof(thrd_t, _Tid),
        (unsigned long long)sizeof(((thrd_t *)0)->_Tid),
        (unsigned long long)sizeof(void *));
    wl_diagnostic_module("thrd_current", (uintptr_t)&thrd_current, 0);
    wl_diagnostic_module("thrd_equal", (uintptr_t)&thrd_equal, 0);
#else
    HANDLE thread;
    DWORD joined;
    printf("{\"kind\":\"layout\",\"profile\":\"Windows-native\","
        "\"token_size\":%llu,\"token_align\":%llu,\"pointer_size\":%llu}\n",
        (unsigned long long)sizeof(DWORD),
        (unsigned long long)__alignof(DWORD),
        (unsigned long long)sizeof(void *));
    wl_diagnostic_module("GetCurrentThreadId", (uintptr_t)&GetCurrentThreadId,
        0);
#endif
    first = wl_diagnostic_thread_current();
    second = wl_diagnostic_thread_current();
    same = wl_diagnostic_thread_equal(first, second) != 0;
    worker.ready = CreateEventW(NULL, TRUE, FALSE, NULL);
    worker.release = CreateEventW(NULL, TRUE, FALSE, NULL);
    if (!worker.ready || !worker.release) wl_diagnostic_fatal(50);
#ifdef WL_DIAG_C11
    if (thrd_create(&thread, wl_diagnostic_worker_body,
        &worker) != thrd_success)
        wl_diagnostic_fatal(51);
#else
    thread = CreateThread(NULL, 0, wl_diagnostic_native_worker, &worker, 0,
            NULL);
    if (!thread) wl_diagnostic_fatal(51);
#endif
    /* Ready event publishes token/self_equal; release holds worker alive. */
    if (WaitForSingleObject(worker.ready, 10000) != WAIT_OBJECT_0)
        wl_diagnostic_fatal(52);
#ifdef WL_DIAG_C11
    printf("{\"kind\":\"live_token_members\",\"current_handle\":\"0x%llx\","
        "\"current_tid\":%u,\"worker_handle\":\"0x%llx\",\"worker_tid\":%u}\n",
        (unsigned long long)(uintptr_t)first._Handle, (unsigned)first._Tid,
        (unsigned long long)(uintptr_t)worker.token._Handle,
        (unsigned)worker.token._Tid);
#else
    printf(
        "{\"kind\":\"live_token_members\",\"current_tid\":%lu,\"worker_tid\":%lu}\n",
        (unsigned long)first, (unsigned long)worker.token);
#endif
    different_lr = wl_diagnostic_thread_equal(first, worker.token) == 0;
    different_rl = wl_diagnostic_thread_equal(worker.token, first) == 0;
    foreign_self = wl_diagnostic_thread_equal(worker.token, worker.token) != 0;
    printf("{\"kind\":\"live_checks\",\"same\":%d,\"different_lr\":%d,"
        "\"different_rl\":%d,\"foreign_self\":%d,\"worker_self\":%d}\n",
        same, different_lr, different_rl, foreign_self, worker.self_equal);
    /* No equality or token observation occurs after release/join. */
    if (!SetEvent(worker.release)) wl_diagnostic_fatal(53);
#ifdef WL_DIAG_C11
    /* Host bounds the whole process; no nonstandard thrd_t Handle is used. */
    if (thrd_join(thread, &joined) != thrd_success) wl_diagnostic_fatal(54);
#else
    if (WaitForSingleObject(thread, 10000) != WAIT_OBJECT_0
        || !GetExitCodeThread(thread, &joined)) wl_diagnostic_fatal(54);
    if (!CloseHandle(thread)) wl_diagnostic_fatal(55);
#endif
    if (!CloseHandle(worker.ready) || !CloseHandle(worker.release))
        wl_diagnostic_fatal(56);
    printf("{\"kind\":\"joined\",\"worker_result\":%d}\n", (int)joined);
    return same && different_lr && different_rl && foreign_self
           && worker.self_equal && !joined ? 0 : 57;
}
