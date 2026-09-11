/* Darwin namespace collision coverage: wirelog/thread.h before Mach headers. */
#include "../wirelog/thread.h"

#include <mach/mach.h>
#include <mach/mach_time.h>

int
main(void)
{
    wl_thread_t thread = { 0 };
    wl_mutex_t mutex = { 0 };
    wl_cond_t condition = { 0 };
    (void)thread;
    (void)mutex;
    (void)condition;
    return 0;
}
