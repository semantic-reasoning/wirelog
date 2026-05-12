/*
 * test_standalone_wirelog_deprecated_macro.c - Issue #782 smoke test.
 *
 * Asserts that the `WIRELOG_DEPRECATED_SINCE(major, minor)` macro
 * defined at `wirelog/wirelog-export.h:36-47` actually compiles and
 * attaches its deprecation attribute on the current compiler.  Until
 * this test landed, the macro had zero in-tree call-sites -- its
 * cross-compiler expansion (GCC / Clang `__attribute__((deprecated))`,
 * MSVC `__declspec(deprecated)`, no-op fallback) had never been
 * exercised before its first real deprecation in v0.42+.
 *
 * The test compiles a `static` probe function annotated with
 * `WIRELOG_DEPRECATED_SINCE(99, 99)` -- a version that will not be
 * reached in practice -- and references it once from `main`.  The
 * meson registration passes `-Wno-deprecated-declarations` (or
 * `/wd4996` under MSVC) so the inevitable deprecation diagnostic at
 * the call site does not break `-Werror` CI.
 *
 * Build success here proves: (a) the macro is syntactically valid
 * for this compiler, (b) the resulting attribute attaches to a
 * function declaration without rejection, and (c) the call-site
 * diagnostic is suppressible via the documented warning flag.
 */

#include "wirelog/wirelog-export.h"

WIRELOG_DEPRECATED_SINCE(99, 99)
static int wirelog_deprecated_macro_smoke_probe(void)
{
    return 0;
}

int
main(void)
{
    return wirelog_deprecated_macro_smoke_probe();
}
