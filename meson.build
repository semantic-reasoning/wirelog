project(
  'wirelog',
  'c',
  version: '0.61.3',
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
is_android = get_option('android')

# Issue #464: Android packaging needs 16 KB ELF page alignment on aarch64
# so the same .so loads on both 4 KB and 16 KB kernels (Android 14+ on
# arm64).  Gated on cpu_family == 'aarch64' because 16 KB pages are an
# arm64-only kernel feature; the flag is meaningless on armv7 (deprecated)
# and harmless-but-irrelevant on x86_64 emulator builds.
#
# Note: on an aarch64 Linux host (e.g., Ampere / Apple Silicon Linux)
# invoking `-Dandroid=true` without a cross-file would also add the flag
# to a *Linux* .so.  Linux honors -Wl,-z,max-page-size as the maximum
# alignment a load segment may take, so the resulting .so is forward-
# compatible with hypothetical 16 KB Linux kernels and unchanged on
# normal 4 KB hosts.  Cross-compilation via cross/android-*.ini is the
# supported path; native-host -Dandroid=true is meant for option-flow
# testing, not shipping.
wirelog_android_link_args = []
if is_android and host_machine.cpu_family() == 'aarch64'
  wirelog_android_link_args += ['-Wl,-z,max-page-size=16384']
endif

#Build options
ipc_option = get_option('ipc')
threads_option = get_option('threads')
tests_option = get_option('tests')
docs_option = get_option('documentation')
# crc32_variant_option already defined earlier before compiler settings

io_plugin_dlopen_option = get_option('io_plugin_dlopen')
ios_option = get_option('ios')

message('wirelog ' + project_version)
message('  Platform: ' + platform)
message('  Embedded: @0@'.format(is_embedded))
message('  IPC support: ' + ipc_option)
message('  Threading: ' + threads_option)
message('  CRC-32 variant: ' + crc32_variant_option)
message('  Plugin loader (dlopen): ' + io_plugin_dlopen_option)
message('  iOS: @0@'.format(ios_option))

mobile_cross = is_android or ios_option
wirelog_ios_link_args = []

if ios_option
  ios_min_version = meson.get_external_property('wirelog_ios_min_version', '13.0')
  ios_platform = meson.get_external_property('wirelog_ios_platform', 'iphoneos')
  ios_arch = meson.get_external_property('wirelog_ios_arch',
    host_machine.cpu_family() == 'x86_64' ? 'x86_64' : 'arm64')

  if ios_platform == 'iphonesimulator'
    ios_target = ios_arch + '-apple-ios' + ios_min_version + '-simulator'
  else
    ios_target = ios_arch + '-apple-ios' + ios_min_version
  endif

  add_project_arguments(['-target', ios_target], language: 'c')
  wirelog_ios_link_args += ['-target', ios_target]
  message('  iOS target: ' + ios_target)
endif

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
fs = import('fs')

#Built-in functions : xxHash3 for hash() function
xxhash_proj = subproject('xxhash', default_options: [])

#Optional : mbedTLS for cryptographic functions (Issue #73)
mbedtls_option = get_option('mbedTLS')
mbedtls_dep = []
mbedtls_required_headers = [
  'psa/crypto.h',
]
mbedtls_psa_probe = '''
#include <psa/crypto.h>

int main(void) {
  unsigned char input[8] = {0};
  unsigned char output[64] = {0};
  size_t output_len = 0;
  psa_hash_operation_t hash_op = PSA_HASH_OPERATION_INIT;
  psa_key_attributes_t attributes = PSA_KEY_ATTRIBUTES_INIT;
  psa_key_id_t key = 0;
  psa_status_t status = psa_crypto_init();
  if (status != PSA_SUCCESS) return 1;
  (void)psa_hash_compute(PSA_ALG_MD5, input, sizeof(input),
                         output, sizeof(output), &output_len);
  (void)psa_hash_compute(PSA_ALG_SHA_1, input, sizeof(input),
                         output, sizeof(output), &output_len);
  (void)psa_hash_compute(PSA_ALG_SHA_256, input, sizeof(input),
                         output, sizeof(output), &output_len);
  (void)psa_hash_compute(PSA_ALG_SHA_512, input, sizeof(input),
                         output, sizeof(output), &output_len);
  (void)psa_hash_setup(&hash_op, PSA_ALG_SHA_1);
  (void)psa_hash_update(&hash_op, input, sizeof(input));
  (void)psa_hash_finish(&hash_op, output, sizeof(output), &output_len);
  psa_set_key_type(&attributes, PSA_KEY_TYPE_HMAC);
  psa_set_key_usage_flags(&attributes, PSA_KEY_USAGE_SIGN_MESSAGE);
  psa_set_key_algorithm(&attributes, PSA_ALG_HMAC(PSA_ALG_SHA_256));
  status = psa_import_key(&attributes, input, sizeof(input), &key);
  psa_reset_key_attributes(&attributes);
  if (status != PSA_SUCCESS) return 1;
  (void)psa_mac_compute(key, PSA_ALG_HMAC(PSA_ALG_SHA_256), input,
                        sizeof(input), output, sizeof(output), &output_len);
  (void)psa_destroy_key(key);
  (void)psa_generate_random(output, 16);
  return 0;
}
'''
mbedtls_psa_candidates = [
  'tfpsacrypto',
  'mbedcrypto',
  'mbedtls',
]
if mbedtls_option == 'auto' or mbedtls_option == 'enabled'
  mbedtls_psa_provider = ''
  mbedtls_psa_provider_version = ''
  mbedtls_psa_rejections = []
  foreach _mbedtls_candidate : mbedtls_psa_candidates
    if mbedtls_psa_provider != ''
      continue
    endif
    _mbedtls_crypto = dependency(_mbedtls_candidate, required: false)
    if not _mbedtls_crypto.found()
      mbedtls_psa_rejections += [_mbedtls_candidate + ': not found']
      continue
    endif

    _mbedtls_candidate_dep = [_mbedtls_crypto]
    _mbedtls_missing_headers = []
    foreach _mbedtls_header : mbedtls_required_headers
      if not cc.has_header(_mbedtls_header, dependencies: _mbedtls_candidate_dep)
        _mbedtls_missing_headers += [_mbedtls_header]
      endif
    endforeach

    if _mbedtls_missing_headers.length() > 0
      mbedtls_psa_rejections += [_mbedtls_candidate + ': missing headers <' + '>, <'.join(_mbedtls_missing_headers) + '>']
      continue
    endif

    if cc.links(mbedtls_psa_probe,
        dependencies: _mbedtls_candidate_dep,
        name: 'PSA Crypto hash, HMAC, and random APIs via ' + _mbedtls_candidate)
      mbedtls_dep = _mbedtls_candidate_dep
      mbedtls_psa_provider = _mbedtls_candidate
      mbedtls_psa_provider_version = _mbedtls_crypto.version()
    else
      mbedtls_psa_rejections += [_mbedtls_candidate + ': failed PSA Crypto link probe']
    endif
  endforeach

  if mbedtls_psa_provider == '' and get_option('mbedTLS_prefix') != ''
    if meson.is_cross_build()
      mbedtls_psa_rejections += ['metadata-free prefix: disabled for cross builds']
    else
      _mbedtls_prefix = get_option('mbedTLS_prefix')
      _mbedtls_realpath = find_program('realpath', required: false)
      if not _mbedtls_realpath.found()
        mbedtls_psa_rejections += ['metadata-free prefix: realpath is unavailable']
      else
        _mbedtls_prefix_realpath_result = run_command(_mbedtls_realpath,
          '-e', _mbedtls_prefix, check: false)
        _mbedtls_prefix_realpath = _mbedtls_prefix_realpath_result.stdout().strip()
        if _mbedtls_prefix_realpath_result.returncode() != 0 or _mbedtls_prefix_realpath == ''
          mbedtls_psa_rejections += ['metadata-free prefix: cannot resolve ' + _mbedtls_prefix]
        else
      _mbedtls_prefix_include = include_directories(_mbedtls_prefix / 'include')
      _mbedtls_lib_dirs = [_mbedtls_prefix / 'lib', _mbedtls_prefix / 'lib64']
      foreach _mbedtls_candidate : mbedtls_psa_candidates
        if mbedtls_psa_provider != ''
          continue
        endif
        _mbedtls_header_path = _mbedtls_prefix / 'include' / 'psa' / 'crypto.h'
        _mbedtls_header_realpath_result = run_command(_mbedtls_realpath,
          '-e', _mbedtls_header_path, check: false)
        _mbedtls_header_realpath = _mbedtls_header_realpath_result.stdout().strip()
        if _mbedtls_header_realpath_result.returncode() != 0 or not _mbedtls_header_realpath.startswith(_mbedtls_prefix_realpath + '/')
          mbedtls_psa_rejections += ['metadata-free prefix: missing <psa/crypto.h> under ' + _mbedtls_prefix]
          break
        endif

        _mbedtls_component_names = []
        if _mbedtls_candidate == 'tfpsacrypto'
          _mbedtls_component_names = ['tfpsacrypto', 'mbedcrypto']
        elif _mbedtls_candidate == 'mbedcrypto'
          _mbedtls_component_names = ['mbedcrypto']
        else
          _mbedtls_component_names = ['mbedtls', 'mbedx509', 'mbedcrypto']
        endif

        _mbedtls_fallback_link_args = []
        _mbedtls_missing_components = []
        foreach _mbedtls_component : _mbedtls_component_names
          _mbedtls_component_path = ''
            foreach _mbedtls_lib_dir : _mbedtls_lib_dirs
              foreach _mbedtls_suffix : ['.so', '.a', '.dylib', '.lib']
              if _mbedtls_component_path == '' and fs.exists(_mbedtls_lib_dir / ('lib' + _mbedtls_component + _mbedtls_suffix))
                _mbedtls_component_path = _mbedtls_lib_dir / ('lib' + _mbedtls_component + _mbedtls_suffix)
              elif _mbedtls_component_path == '' and fs.exists(_mbedtls_lib_dir / (_mbedtls_component + _mbedtls_suffix))
                _mbedtls_component_path = _mbedtls_lib_dir / (_mbedtls_component + _mbedtls_suffix)
              endif
            endforeach
          endforeach
          if _mbedtls_component_path == ''
            _mbedtls_missing_components += [_mbedtls_component]
            continue
          endif
          _mbedtls_component_realpath_result = run_command(_mbedtls_realpath,
            '-e', _mbedtls_component_path, check: false)
          _mbedtls_component_realpath = _mbedtls_component_realpath_result.stdout().strip()
          if _mbedtls_component_realpath_result.returncode() != 0 or not _mbedtls_component_realpath.startswith(_mbedtls_prefix_realpath + '/')
            _mbedtls_missing_components += [_mbedtls_component + ' (outside prefix)']
            continue
          endif
          _mbedtls_fallback_link_args += [_mbedtls_component_path]
        endforeach
        if _mbedtls_missing_components.length() > 0
          mbedtls_psa_rejections += ['metadata-free ' + _mbedtls_candidate + ': missing libraries ' + ', '.join(_mbedtls_missing_components)]
          continue
        endif

        _mbedtls_fallback_transitive_deps = [threads_dep]
        _mbedtls_dl_dep = cc.find_library('dl', required: false)
        if _mbedtls_dl_dep.found()
          _mbedtls_fallback_transitive_deps += [_mbedtls_dl_dep]
        endif
        _mbedtls_fallback_candidate_dep = declare_dependency(
          dependencies: _mbedtls_fallback_transitive_deps,
          link_args: _mbedtls_fallback_link_args,
          include_directories: _mbedtls_prefix_include)
        if cc.links(mbedtls_psa_probe,
            dependencies: [_mbedtls_fallback_candidate_dep],
            name: 'PSA Crypto hash, HMAC, and random APIs via metadata-free ' + _mbedtls_candidate)
          mbedtls_dep = [_mbedtls_fallback_candidate_dep]
          mbedtls_psa_provider = _mbedtls_candidate
          mbedtls_psa_provider_version = 'unknown'
          message('  mbedTLS discovery: metadata-free prefix')
          message('  mbedTLS prefix: ' + _mbedtls_prefix)
          message('  mbedTLS libraries: ' + ', '.join(_mbedtls_component_names))
        else
          mbedtls_psa_rejections += ['metadata-free ' + _mbedtls_candidate + ': failed PSA Crypto link probe']
        endif
      endforeach
        endif
      endif
    endif
  endif

  if mbedtls_psa_provider != ''
    message('  mbedTLS PSA provider: ' + mbedtls_psa_provider + ' ' + mbedtls_psa_provider_version)
    add_project_arguments('-DWL_MBEDTLS_ENABLED=1', language: 'c')
  elif mbedtls_option == 'auto'
    warning('mbedTLS auto-detection did not find a linkable PSA Crypto provider; crypto built-ins will remain disabled. Candidates: ' + '; '.join(mbedtls_psa_rejections))
  else
    error('mbedTLS enabled requires a linkable PSA Crypto provider exposing <psa/crypto.h>. Candidates: ' + '; '.join(mbedtls_psa_rejections))
  endif
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
wirelog_sources += wirelog_crc32_src
wirelog_sources += wirelog_workqueue_src
wirelog_sources += wirelog_io_src
wirelog_sources += wirelog_extension_src
wirelog_sources += wirelog_string_ops_src
wirelog_sources += wirelog_wl_easy_src
wirelog_sources += wirelog_advanced_src
wirelog_sources += wirelog_api_facade_src
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
  'wirelog/wirelog-extension.h',
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
  link_args: wirelog_android_link_args + wirelog_ios_link_args,
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
  # 9 installed headers carries WIRELOG_API (alias of WIRELOG_PUBLIC =
  # visibility("default") / dllexport) per #733 K0 + #782.  Internal
  # symbols (wl_*, col_*, arr_*) are now hidden from the dynamic-symbol
  # table; consumers who relied on resolving them through libwirelog.so
  # must rebuild against the public surface.  See #733.
  gnu_symbol_visibility: ios_option ? 'default' : 'hidden',
)

#Static library(optional)
# Note: `wirelog_android_link_args` is intentionally NOT threaded here.
# Static archives carry object code only, not link-time attributes; ELF
# page alignment is decided when the .a is finally linked into a .so
# or executable by the downstream consumer.
wirelog_static_lib = static_library(
  'wirelog_static',
  wirelog_sources,
  config_h_file,
  include_directories: [wirelog_inc, wirelog_src_inc],
  c_args: ['-DWIRELOG_BUILDING'],
  link_args: wirelog_ios_link_args,
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

install_data(
  'wirelog/module.modulemap',
  install_dir: wirelog_includedir / 'wirelog'
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
if io_plugin_dlopen_option == 'enabled' and not (is_windows or ios_option)
  _dl_dep = cc.find_library('dl', required: false)
  if _dl_dep.found()
    cli_plugin_deps += [_dl_dep]
  endif
  cli_plugin_src = files('wirelog/cli/plugin_loader.c')
  cli_plugin_args = ['-DWL_HAVE_PLUGIN_LOADER']
endif

#Build wirelog-cli executable (wirelog_cli_src defined in wirelog/meson.build)
#Include all wirelog sources to ensure proper linking with LTO
# Mobile builds do not ship the CLI: Android loads the library into a
# host process, and iOS uses the library/module surface rather than a
# command-line entry point.
if not mobile_cross
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
endif

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Benchmarks
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

if not mobile_cross
  subdir('bench')
endif

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Tests
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

if tests_option and not mobile_cross
  subdir('tests')
endif

#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==
#Examples
#== == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == == ==

if not mobile_cross
  # 04, 05 and 06 are golden-diff tests over wirelog_cli rather than their
  # own executables; they run the committed .dl programs in a temporary tree.
  subdir('examples/04-hash-functions')
  subdir('examples/05-crc32-checksum')
  subdir('examples/06-timestamp-lww')
  subdir('examples/08-delta-queries')
  subdir('examples/09-retraction-basics')
  subdir('examples/10-recursive-under-update')
  subdir('examples/11-time-evolution')
  subdir('examples/12-snapshot-vs-delta')
  subdir('examples/13-daemon-style')
  subdir('examples/14-arithmetic-operations')
endif

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
