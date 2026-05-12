project(
  'wirelog',
  'c',
  version: '0.40.99',
  license: 'LGPL-3.0-or-later',
  meson_version: '>=0.62.0',
  default_options: [
    'buildtype=release',
    'optimization=s',
    'b_lto=true',
    'b_sanitize=none',
  ]
)

#Project metadata
project_name = meson.project_name()
project_version = meson.project_version()
project_license = 'LGPL-3.0-or-later'
project_url = 'https://github.com/semantic-reasoning/wirelog'

#Directories
wirelog_libdir = get_option('libdir')
wirelog_includedir = get_option('includedir')
wirelog_datadir = get_option('datadir') / project_name

#Build options (must be read before compiler settings for CRC-32 variant)
crc32_variant_option = get_option('crc32_variant')

#Compiler settings
cc = meson.get_compiler('c')

# CRC-32 variant define
crc32_defines = []
if crc32_variant_option == 'ethernet'
  crc32_defines = ['-DCRC32_DEFAULT_VARIANT_ETHERNET']
elif crc32_variant_option == 'castagnoli'
  crc32_defines = ['-DCRC32_DEFAULT_VARIANT_CASTAGNOLI']
endif

if cc.get_id() == 'msvc'
  # MSVC C11 atomics are incomplete
  # Define __STDC_NO_ATOMICS__ to skip C11 atomics declarations
  add_project_arguments(
    ['/W3', '/D__STDC_NO_ATOMICS__'] + crc32_defines,
    language: 'c'
  )
else
  # GCC/Clang: enforce C11 standard
  add_project_arguments(
    ['-std=c11'] + crc32_defines,
    language: 'c'
  )
  # GCC/Clang: add SIMD support flags for AVX2 and NEON optimization
  simd_flags = []
  if host_machine.cpu_family() == 'aarch64'
    simd_flags = ['-march=armv8-a+simd']  # ARM64: enable NEON
  elif host_machine.cpu_family() == 'x86_64'
    simd_flags = ['-mavx2']  # x86-64: enable AVX2 (portable base)
  endif

  add_project_arguments(
    [
      '-Wall',
      '-Wextra',
      '-Wno-unused-parameter',
      '-Wno-missing-field-initializers',
    ] + simd_flags,
    language: 'c'
  )
endif

# Issue #287: structured logger compile-time ceiling.
_log_level_map = {'error': 1, 'warn': 2, 'info': 3, 'debug': 4, 'trace': 5}
_log_opt = get_option('wirelog_log_max_level')
add_project_arguments(
  '-DWL_LOG_COMPILE_MAX_LEVEL=' + _log_level_map[_log_opt].to_string(),
  language: 'c',
)
if _log_opt == 'trace' and get_option('buildtype') == 'release'
  warning(
    'wirelog_log_max_level=trace in release build; pass -Dwirelog_log_max_level=error for zero-cost disabled log sites.'
  )
endif

#Platform detection
platform = host_machine.system()
is_embedded = get_option('embedded')
is_windows = platform == 'windows'
is_macos = platform == 'darwin'
is_linux = platform == 'linux'

#Build options
ipc_option = get_option('ipc')
threads_option = get_option('threads')
tests_option = get_option('tests')
docs_option = get_option('documentation')
# crc32_variant_option already defined earlier before compiler settings

io_plugin_dlopen_option = get_option('io_plugin_dlopen')

message('wirelog ' + project_version)
message('  Platform: ' + platform)
message('  Embedded: @0@'.format(is_embedded))
message('  IPC support: ' + ipc_option)
message('  Threading: ' + threads_option)
message('  CRC-32 variant: ' + crc32_variant_option)
message('  Plugin loader (dlopen): ' + io_plugin_dlopen_option)

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Dependencies
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

#pthreads for threading
threads_dep = []
_td = dependency('threads', required: false)
if not _td.found()
  _td = cc.find_library('pthread', required: false)
endif
if _td.found()
  threads_dep = [_td]
endif

#Optional : zlib for IPC compression(future)
zlib_dep = []
if ipc_option == 'enabled'
  _zd = dependency('zlib', required: false)
  if _zd.found()
    zlib_dep = [_zd]
  endif
endif

#Phase 2A : nanoarrow for columnar backend(via meson wrap)
nanoarrow_proj = subproject('nanoarrow', default_options: ['tests=disabled', 'apps=disabled', 'ipc=disabled', 'device=disabled', 'testing=disabled'])
nanoarrow_dep = nanoarrow_proj.get_variable('nanoarrow_dep')

#Built-in functions : xxHash3 for hash() function
xxhash_proj = subproject('xxhash', default_options: [])

#Optional : mbedTLS for cryptographic functions (Issue #73)
mbedtls_option = get_option('mbedTLS')
mbedtls_dep = []
if mbedtls_option == 'auto'
  _mbedtls = dependency('mbedtls', required: false)
  if _mbedtls.found()
    # mbedTLS has private dependencies on mbedx509 and tfpsacrypto for linking
    _mbedx509 = dependency('mbedx509', required: false)
    _tfpsacrypto = dependency('tfpsacrypto', required: false)
    mbedtls_dep = [_mbedtls]
    if _mbedx509.found()
      mbedtls_dep += [_mbedx509]
    endif
    if _tfpsacrypto.found()
      mbedtls_dep += [_tfpsacrypto]
    endif
    add_project_arguments('-DWL_MBEDTLS_ENABLED=1', language: 'c')
  endif
elif mbedtls_option == 'enabled'
  _mbedtls = dependency('mbedtls', required: true)
  # mbedTLS has private dependencies on mbedx509 and tfpsacrypto for linking
  _mbedx509 = dependency('mbedx509', required: false)
  _tfpsacrypto = dependency('tfpsacrypto', required: false)
  mbedtls_dep = [_mbedtls]
  if _mbedx509.found()
    mbedtls_dep += [_mbedx509]
  endif
  if _tfpsacrypto.found()
    mbedtls_dep += [_tfpsacrypto]
  endif
  add_project_arguments('-DWL_MBEDTLS_ENABLED=1', language: 'c')
endif

xxhash_dep = xxhash_proj.get_variable('xxhash_dep')

# Math library for col_compute_worker_cap (Issue #409)
# On most POSIX systems libm is separate; on some (musl, glibc 2.38+) it is
# part of libc and find_library may return not-found.  Fall back to a raw
# -lm link_args which is harmless when libm is folded into libc.
_math_lib = cc.find_library('m', required: false)
if _math_lib.found()
  math_dep = [_math_lib]
else
  math_dep = [declare_dependency(link_args: '-lm')]
endif

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Configuration
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

config_h = configuration_data()
config_h.set_quoted('WIRELOG_PLATFORM', platform)
config_h.set('WIRELOG_EMBEDDED', is_embedded)
config_h.set('WIRELOG_IPC', ipc_option == 'enabled')
config_h.set('WIRELOG_THREADS', true)

#Generate config header
config_h_file = configure_file(
  input: 'config.h.in',
  output: 'wirelog-config.h',
  configuration: config_h,
  install_dir: wirelog_includedir / 'wirelog'
)

#Version header (Issue #688 / #704)
#The integer triple (WIRELOG_VERSION_MAJOR/MINOR/PATCH) is parsed from
#project_version with any '-rcN' / '-dev' suffix stripped; the suffix is
#preserved verbatim in WIRELOG_VERSION_STRING.  The configure_file()
#call lives in wirelog/meson.build so the generated header lands in
#build/wirelog/wirelog-version.h, where include_directories('.') below
#picks it up under the canonical "wirelog/wirelog-version.h" path.
version_release = project_version.split('-')[0]
version_parts = version_release.split('.')
assert(version_parts.length() == 3,
  'project_version must be MAJOR.MINOR.PATCH (got: @0@)'.format(project_version))

version_h = configuration_data()
version_h.set('WIRELOG_VERSION_MAJOR', version_parts[0].to_int())
version_h.set('WIRELOG_VERSION_MINOR', version_parts[1].to_int())
version_h.set('WIRELOG_VERSION_PATCH', version_parts[2].to_int())
version_h.set('WIRELOG_VERSION_STRING', project_version)

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Include directories
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

wirelog_inc = include_directories('.')
wirelog_src_inc = include_directories('wirelog')

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Source files
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

#Collect source files for each module
subdir('wirelog')

#Combine all sources
wirelog_sources = []
wirelog_sources += wirelog_parser_src
wirelog_sources += wirelog_ir_src
wirelog_sources += wirelog_optimizer_src
wirelog_sources += wirelog_arena_src
wirelog_sources += wirelog_thread_src
wirelog_sources += wirelog_backend_src
wirelog_sources += wirelog_workqueue_src
wirelog_sources += wirelog_io_src
wirelog_sources += wirelog_string_ops_src
wirelog_sources += wirelog_wl_easy_src
wirelog_sources += wirelog_advanced_src
wirelog_sources += wirelog_util_log_src

#Public headers
wirelog_public_headers = [
  'wirelog/wirelog.h',
  'wirelog/wirelog-types.h',
  'wirelog/wirelog-parser.h',
  'wirelog/wirelog-ir.h',
  'wirelog/wirelog-optimizer.h',
  'wirelog/wirelog-export.h',
  'wirelog/wirelog-easy.h',
  'wirelog/wirelog-advanced.h',
]

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Build targets
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

#Shared library
wirelog_lib = library(
  'wirelog',
  wirelog_sources,
  config_h_file,
  include_directories: [wirelog_inc, wirelog_src_inc],
  c_args: ['-DWIRELOG_BUILDING'],
  dependencies: [threads_dep, zlib_dep, nanoarrow_dep, xxhash_dep, mbedtls_dep, math_dep],
  install: true,
  # SOVERSION ('soversion') is decoupled from project_version so the
  # 1.0 ABI commitment is fixed at SOVERSION=1 ahead of project_version
  # actually reaching 1.0.0.  Linux: libwirelog.so.1 -> libwirelog.so.1.0.0.
  # macOS: compatibility_version=1, current_version=1.0.0.
  # Windows: meson auto-derives wirelog-1.dll from version major.
  # See #733.
  version: project_version,
  soversion: '1',
  darwin_versions: ['1', '1.0.0'],
  # Symbol-visibility default: hidden.  Every public-API function on the
  # 9 installed headers carries WIRELOG_PUBLIC (= visibility("default")
  # / dllexport) per #733 K0.  Internal symbols (wl_*, col_*, arr_*)
  # are now hidden from the dynamic-symbol table; consumers who relied
  # on resolving them through libwirelog.so must rebuild against the
  # public surface.  See #733.
  gnu_symbol_visibility: 'hidden',
)

#Static library(optional)
wirelog_static_lib = static_library(
  'wirelog_static',
  wirelog_sources,
  config_h_file,
  include_directories: [wirelog_inc, wirelog_src_inc],
  c_args: ['-DWIRELOG_BUILDING'],
  dependencies: [threads_dep, zlib_dep, nanoarrow_dep, xxhash_dep, mbedtls_dep, math_dep],
  install: false,  # Not installed by default
)

#Install public headers
install_headers(
  wirelog_public_headers,
  subdir: 'wirelog'
)

install_headers(
  'wirelog/io/io_adapter.h',
  subdir: 'wirelog/io'
)

#pkg - config file
pkg = import('pkgconfig')
pkg.generate(
  wirelog_lib,
  name: project_name,
  description: 'Embedded-to-Enterprise Datalog Engine',
  version: project_version,
  filebase: project_name,
)

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#CLI Driver
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

# Optional plugin loader via dlopen (Issue #461)
# Excluded from Windows builds (no dlopen); Android/iOS excluded via option default.
cli_plugin_src = []
cli_plugin_deps = []
cli_plugin_args = []
if io_plugin_dlopen_option == 'enabled' and not is_windows
  _dl_dep = cc.find_library('dl', required: false)
  if _dl_dep.found()
    cli_plugin_deps += [_dl_dep]
  endif
  cli_plugin_src = files('wirelog/cli/plugin_loader.c')
  cli_plugin_args = ['-DWL_HAVE_PLUGIN_LOADER']
endif

#Build wirelog-cli executable (wirelog_cli_src defined in wirelog/meson.build)
#Include all wirelog sources to ensure proper linking with LTO
wirelog_cli = executable(
  'wirelog_cli',
  wirelog_cli_src,
  cli_plugin_src,
  wirelog_sources,
  config_h_file,
  include_directories: [wirelog_inc, wirelog_src_inc],
  c_args: cli_plugin_args,
  dependencies: [threads_dep, zlib_dep, nanoarrow_dep, xxhash_dep, mbedtls_dep, math_dep] + cli_plugin_deps,
  install: true,
)

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Benchmarks
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

subdir('bench')

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Tests
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

if tests_option
  subdir('tests')
endif

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Examples
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

subdir('examples/08-delta-queries')
subdir('examples/09-retraction-basics')
subdir('examples/10-recursive-under-update')
subdir('examples/11-time-evolution')
subdir('examples/12-snapshot-vs-delta')
subdir('examples/13-daemon-style')

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Documentation
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

if docs_option
#Documentation build(optional, not yet implemented)
  message('Documentation: enabled (not yet implemented)')
endif

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Git Hooks Setup
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

# Setup uncrustify pre-commit hook (optional, non-blocking)
python3 = find_program('python3', required: false)
if python3.found()
  run_command(
    python3,
    'scripts/setup_uncrustify_hook.py',
    check: false  # Don't fail build if hook setup has issues
  )
endif

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Summary
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

message('')
message('wirelog ' + project_version + ' Configuration:')
message('  Library: ' + wirelog_libdir)
message('  Headers: ' + wirelog_includedir / 'wirelog')
message('  Embedded mode: @0@'.format(is_embedded))
message('  IPC support: ' + ipc_option)
message('  Threading: ' + threads_option)
message('  Tests: @0@'.format(tests_option))
message('')
message('Git Hooks: uncrustify pre-commit hook configured')
