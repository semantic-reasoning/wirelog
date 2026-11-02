/* Standalone-include compile gate for wirelog/wirelog-advanced.h.
 * Issue #689 (Blocker B2): every public installed header must
 * compile in isolation -- no hidden dependency on another header
 * being included first.  Each .c file includes ONE public header
 * and nothing else (modulo the stdlib that the header itself
 * pulls).  Failure = the header has an internal-only forward
 * declaration leaking into its visible API. */
#include "wirelog/wirelog-advanced.h"

/* Issue #2321: the source-inspection feature macro and declarations come
 * with this header alone.  sizeof does not evaluate its operand, so this
 * stays a compile-only check with no link dependency. */
#if !defined(WIRELOG_SOURCE_QUERY_VERSION) || WIRELOG_SOURCE_QUERY_VERSION < 1
#error "wirelog-advanced.h must declare WIRELOG_SOURCE_QUERY_VERSION >= 1"
#endif

int
main(void)
{
    wirelog_source_query_status_t st = WIRELOG_SOURCE_QUERY_OK;
    (void)sizeof(&wirelog_session_source_count);
    (void)sizeof(&wirelog_session_source_key_exists);
    (void)sizeof(&wirelog_session_source_validate);
    (void)sizeof(&wirelog_session_lookup_symbol);
    return (int)st;
}
