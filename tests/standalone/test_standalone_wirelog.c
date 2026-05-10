/* Standalone-include compile gate for wirelog/wirelog.h.
 * Issue #689 (Blocker B2): every public installed header must
 * compile in isolation -- no hidden dependency on another header
 * being included first.  Each .c file includes ONE public header
 * and nothing else (modulo the stdlib that the header itself
 * pulls).  Failure = the header has an internal-only forward
 * declaration leaking into its visible API. */
#include "wirelog/wirelog.h"
int
main(void)
{
    return 0;
}
