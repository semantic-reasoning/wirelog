/*
 * tests/clang_tidy_fixture.c - clang-tidy sensitivity fixture (Issue #1100).
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * This file is NOT compiled by any meson target and is NOT listed in
 * scripts/ci/clang-tidy-allowlist.txt or clang-tidy-backlog.txt.  It exists
 * only so scripts/ci/check-clang-tidy-ratchet.py --mode sensitivity can
 * prove that the ratchet can still see a diagnostic it is supposed to see.
 *
 * A ratchet that reports every file clean because it lost its config, or
 * because it silently analysed the wrong source, passes exactly as loudly
 * as one that works.  Issue #1079 -- a symlinked build directory quietly
 * changing what was analysed -- is that failure mode.  This is the control.
 *
 * The body below must produce EXACTLY ONE diagnostic under the repository
 * .clang-tidy: bugprone-integer-division, on the marked line.  Zero
 * diagnostics, a different check, a different line, or a compile error all
 * fail the check.  Do not "fix" the integer division below, and do not add
 * anything else to this file.
 */

double wl_clang_tidy_fixture_ratio(int a, int b);

double
wl_clang_tidy_fixture_ratio(int a, int b)
{
    double ratio = a / b; /* WL_TIDY_FIXTURE_EXPECT integer-division */

    return ratio;
}
