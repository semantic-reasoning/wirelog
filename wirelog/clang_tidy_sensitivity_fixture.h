/* Header-only clang-tidy sensitivity fixture; not part of any build target. */

static inline double
wl_clang_tidy_fixture_header_ratio(int a, int b)
{
    return a / b; /* WL_TIDY_HEADER_FIXTURE_EXPECT integer-division */
}
