/* Standalone ABI observations only; no engine headers or actor authority. */
#include <windows.h>
#ifdef WL_DIAG_C11
#include <threads.h>

__declspec(noinline) thrd_t
wl_diagnostic_thread_current(void)
{
    return thrd_current();
}

__declspec(noinline) int
wl_diagnostic_thread_equal(thrd_t left, thrd_t right)
{
    return thrd_equal(left, right);
}
#else
__declspec(noinline) DWORD
wl_diagnostic_thread_current(void)
{
    return GetCurrentThreadId();
}

__declspec(noinline) int
wl_diagnostic_thread_equal(DWORD left, DWORD right)
{
    return left == right;
}
#endif
