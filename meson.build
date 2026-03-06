project(
  'wirelog',
  'c',
  version: '0.11.0',
  license: 'LGPL-3.0',
  meson_version: '>=0.62.0',
  default_options: [
    'c_std=c11',
    'buildtype=release',
    'optimization=s',
    'b_lto=true',
    'b_sanitize=none',
  ]
)

# Project metadata
project_name = meson.project_name()
project_version = meson.project_version()
project_license = 'LGPL-3.0'
project_url = 'https://github.com/justinjoy/wirelog'

# Directories
wirelog_libdir = get_option('libdir')
wirelog_includedir = get_option('includedir')
wirelog_datadir = get_option('datadir') / project_name

# Compiler settings
cc = meson.get_compiler('c')
if cc.get_id() == 'msvc'
  add_project_arguments(
    ['/W3'],
    language: 'c'
  )
else
  add_project_arguments(
    [
      '-Wall',
      '-Wextra',
      '-Wno-unused-parameter',
      '-Wno-missing-field-initializers',
    ],
    language: 'c'
  )
endif

# Platform detection
platform = host_machine.system()
is_embedded = get_option('embedded')
is_windows = platform == 'windows'
is_macos = platform == 'darwin'
is_linux = platform == 'linux'

# Build options
ipc_option = get_option('ipc')
threads_option = get_option('threads')
tests_option = get_option('tests')
docs_option = get_option('documentation')

message('wirelog ' + project_version)
message('  Platform: ' + platform)
message('  Embedded: @0@'.format(is_embedded))
message('  IPC support: ' + ipc_option)
message('  Threading: ' + threads_option)

# ============================================================================
# Dependencies
# ============================================================================

# Optional: pthreads for threading
threads_dep = []
if threads_option == 'enabled'
  _td = dependency('threads', required: false)
  if not _td.found()
    _td = cc.find_library('pthread', required: false)
  endif
  if _td.found()
    threads_dep = [_td]
  endif
endif

# Optional: zlib for IPC compression (future)
zlib_dep = []
if ipc_option == 'enabled'
  _zd = dependency('zlib', required: false)
  if _zd.found()
    zlib_dep = [_zd]
  endif
endif

# Phase 2A: nanoarrow for columnar backend (via git submodule)
nanoarrow_proj = subproject('nanoarrow', default_options: ['tests=disabled', 'apps=disabled', 'ipc=disabled', 'device=disabled', 'testing=disabled'])
nanoarrow_dep = nanoarrow_proj.get_variable('nanoarrow_dep')

# ============================================================================
# Configuration
# ============================================================================

config_h = configuration_data()
config_h.set_quoted('WIRELOG_VERSION', project_version)
config_h.set_quoted('WIRELOG_PLATFORM', platform)
config_h.set('WIRELOG_EMBEDDED', is_embedded)
config_h.set('WIRELOG_IPC', ipc_option == 'enabled')
config_h.set('WIRELOG_THREADS', threads_option == 'enabled')

# Generate config header
config_h_file = configure_file(
  input: 'config.h.in',
  output: 'wirelog-config.h',
  configuration: config_h,
  install_dir: wirelog_includedir / 'wirelog'
)

# ============================================================================
# Include directories
# ============================================================================

wirelog_inc = include_directories('.')
wirelog_src_inc = include_directories('wirelog')

# ============================================================================
# Source files
# ============================================================================

# Collect source files for each module
subdir('wirelog')

# Combine all sources
wirelog_sources = []
wirelog_sources += wirelog_parser_src
wirelog_sources += wirelog_ir_src
wirelog_sources += wirelog_optimizer_src
wirelog_sources += wirelog_dd_src
wirelog_sources += wirelog_ffi_src
wirelog_sources += wirelog_arena_src
wirelog_sources += wirelog_backend_src
wirelog_sources += wirelog_io_src

# Public headers
wirelog_public_headers = [
  'wirelog/wirelog.h',
  'wirelog/wirelog-types.h',
  'wirelog/wirelog-parser.h',
  'wirelog/wirelog-ir.h',
  'wirelog/wirelog-optimizer.h',
]

# ============================================================================
# Build targets
# ============================================================================

# Shared library
wirelog_lib = library(
  'wirelog',
  wirelog_sources,
  config_h_file,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [threads_dep, zlib_dep, nanoarrow_dep],
  install: true,
  version: project_version,
)

# Static library (optional)
wirelog_static_lib = static_library(
  'wirelog_static',
  wirelog_sources,
  config_h_file,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [threads_dep, zlib_dep, nanoarrow_dep],
  install: false,  # Not installed by default
)

# Install public headers
install_headers(
  wirelog_public_headers,
  subdir: 'wirelog'
)

# pkg-config file
pkg = import('pkgconfig')
pkg.generate(
  wirelog_lib,
  name: project_name,
  description: 'Embedded-to-Enterprise Datalog Engine',
  version: project_version,
  filebase: project_name,
)

# ============================================================================
# CLI Driver
# ============================================================================

# Note: CLI driver is deferred to Phase 2A (columnar backend implementation)

# ============================================================================
# Benchmarks
# ============================================================================

# Note: Benchmarks are deferred to Phase 2A (columnar backend implementation)

# ============================================================================
# Tests
# ============================================================================

if tests_option
  subdir('tests')
endif

# ============================================================================
# Documentation
# ============================================================================

if docs_option
  # Documentation build (optional, not yet implemented)
  message('Documentation: enabled (not yet implemented)')
endif

# ============================================================================
# Summary
# ============================================================================

message('')
message('wirelog ' + project_version + ' Configuration:')
message('  Library: ' + wirelog_libdir)
message('  Headers: ' + wirelog_includedir / 'wirelog')
message('  Embedded mode: @0@'.format(is_embedded))
message('  IPC support: ' + ipc_option)
message('  Threading: ' + threads_option)
message('  Tests: @0@'.format(tests_option))
message('')
