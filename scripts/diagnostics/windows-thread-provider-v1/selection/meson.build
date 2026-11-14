# Configure-only reproduction of the frozen project's actual compiler check.
project('wirelog-thread-selection-diagnostic', 'c', version: '1',
  meson_version: '>=1.12.0',
  default_options: ['buildtype=release', 'optimization=s', 'b_lto=true'])
cc = meson.get_compiler('c')
if cc.get_id() != 'msvc'
  error('This probe requires actual MSVC cl; other compiler facts are separate')
endif
add_project_arguments(['/W3', '/D__STDC_NO_ATOMICS__',
  '-DCRC32_DEFAULT_VARIANT_ETHERNET'], language: 'c')
c11_threads_test = '''
#include <threads.h>
static int dummy(void *arg) { (void)arg; return 0; }
int main(void) {
    thrd_t t;
    if (thrd_create(&t, dummy, 0) == thrd_success)
        thrd_join(t, 0);
    return 0;
}
'''
have_c11_threads = cc.links(c11_threads_test, name: 'C11 <threads.h> support')
result = configuration_data()
result.set10('WL_DIAGNOSTIC_C11_SUPPORTED', have_c11_threads)
configure_file(output: 'selection-result.h', configuration: result)
message('DIAGNOSTIC: this is configuration evidence, not engine qualification')
