/* Standalone-include compile gate for wirelog/wirelog-extension.h. */
#include "wirelog/wirelog-extension.h"

int
main(void)
{
    return WIRELOG_EXTENSION_ABI_VERSION == 0u;
}
