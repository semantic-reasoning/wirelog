project(
  'wirelog',
  'c',
  version: '0.10.0',
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
dd_option = get_option('dd')

message('wirelog ' + project_version)
message('  Platform: ' + platform)
message('  Embedded: @0@'.format(is_embedded))
message('  IPC support: ' + ipc_option)
message('  Threading: ' + threads_option)
message('  DD executor: @0@'.format(dd_option))

# ============================================================================
# Dependencies
# ============================================================================


# Rust FFI (Differential Dataflow)
if dd_option
  cargo = find_program('cargo', required: true)

  rust_dd_src = meson.project_source_root() / 'rust' / 'wirelog-dd'
  rust_dd_lib = rust_dd_src / 'target' / 'release' / 'libwirelog_dd.a'

  rust_dd_target = custom_target(
    'wirelog-dd',
    command: [cargo, 'build', '--release', '--manifest-path',
              rust_dd_src / 'Cargo.toml'],
    output: 'libwirelog_dd.stamp',
    build_by_default: true,
    console: true,
  )

  rust_ffi_dep = declare_dependency(
    link_args: [rust_dd_lib],
    sources: rust_dd_target,
  )

  # Rust lint targets (run manually: ninja rust-clippy / ninja rust-fmt-check)
  run_target('rust-clippy',
    command: [cargo, 'clippy', '--manifest-path', rust_dd_src / 'Cargo.toml',
              '--', '-D', 'warnings'],
  )
  run_target('rust-fmt-check',
    command: [cargo, 'fmt', '--manifest-path', rust_dd_src / 'Cargo.toml',
              '--check'],
  )
  run_target('rust-test',
    command: [cargo, 'test', '--manifest-path', rust_dd_src / 'Cargo.toml'],
  )
else
  rust_ffi_dep = disabler()
endif

# Optional: pthreads for threading
threads_dep = disabler()
if threads_option == 'enabled'
  threads_dep = dependency('threads', required: false)
  if not threads_dep.found()
    threads_dep = cc.find_library('pthread', required: false)
  endif
endif

# Optional: zlib for IPC compression (future)
zlib_dep = disabler()
if ipc_option == 'enabled'
  zlib_dep = dependency('zlib', required: false)
endif

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
  dependencies: [threads_dep, zlib_dep],
  install: true,
  version: project_version,
)

# Static library (optional)
wirelog_static_lib = static_library(
  'wirelog_static',
  wirelog_sources,
  config_h_file,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [threads_dep, zlib_dep],
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
# CLI Driver (requires DD executor)
# ============================================================================

if dd_option
  wirelog_cli_src = files(
    'wirelog/cli/driver.c',
    'wirelog/ffi/facts_loader.c',
  )

  wirelog_run_deps = [rust_ffi_dep]
  if threads_option == 'enabled'
    wirelog_run_deps += threads_dep
  endif

  # Build target is 'wirelog-cli' to avoid collision with the wirelog/
  # source subdirectory in the build tree. Installed as 'wirelog'.
  wirelog_run = executable(
    'wirelog-cli',
    files('wirelog/cli/main.c'),
    wirelog_sources,
    wirelog_cli_src,
    config_h_file,
    include_directories: [wirelog_inc, wirelog_src_inc],
    dependencies: wirelog_run_deps,
    install: true,
    install_dir: get_option('bindir'),
  )

  meson.add_install_script('sh', '-c',
    'mv "$DESTDIR$MESON_INSTALL_PREFIX/bin/wirelog-cli" "$DESTDIR$MESON_INSTALL_PREFIX/bin/wirelog"'
  )
endif

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
message('  DD executor: @0@'.format(dd_option))
message('  Tests: @0@'.format(tests_option))
message('')
