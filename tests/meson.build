# tests/meson.build
# Test configuration for wirelog

# ============================================================================
# Parser source files (compiled directly into test executables)
# ============================================================================

parser_src = files(
  '../wirelog/parser/lexer.c',
  '../wirelog/parser/ast.c',
  '../wirelog/parser/parser.c',
)

ir_src = files(
  '../wirelog/ir/ir.c',
  '../wirelog/ir/program.c',
  '../wirelog/ir/stratify.c',
  '../wirelog/ir/api.c',
  '../wirelog/intern.c',
  '../wirelog/session.c',
  '../wirelog/exec_plan_gen.c',
  '../wirelog/session_facts.c',
  '../wirelog/extension_registry.c',
  # program.c uses WL_LOG (Issue #539 observability); bundle the logger so
  # every test executable that links ir_src resolves the symbols.
  '../wirelog/util/log.c',
  '../wirelog/util/log_emit.c',
)

optimizer_src = files(
  '../wirelog/passes/fusion.c',
  '../wirelog/passes/jpp.c',
  '../wirelog/passes/sip.c',
  '../wirelog/passes/subsumption.c',
  '../wirelog/passes/magic_sets.c',
)

# intern.c takes the writer mutex from wirelog/thread.h (Issue #958), so
# every target that compiles it must also link the threading backend.
# Meson deduplicates against targets that already list thread_src.
intern_src = [
  files('../wirelog/intern.c'),
  wirelog_thread_src,
]

string_ops_src = files(
  '../wirelog/string_ops.c',
)

csv_src = files(
  '../wirelog/io/csv_reader.c',
)

io_src = files(
  '../wirelog/io/csv_reader.c',
  '../wirelog/io/io_adapter.c',
  '../wirelog/io/csv_adapter.c',
  '../wirelog/io/io_ctx.c',
)

# Select threading backend based on platform
# Reuse the same threading backend selected by wirelog/meson.build
# (C11 threads.h > POSIX pthreads > Windows MSVC).
# wirelog_thread_src is defined in wirelog/meson.build and propagated
# to subdir builds via the parent scope.
thread_src = wirelog_thread_src

# ============================================================================
# Test Executables
# ============================================================================

test_lexer_exe = executable(
  'test_lexer',
  files('test_lexer.c'),
  parser_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test_parser_exe = executable(
  'test_parser',
  files('test_parser.c'),
  parser_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test_ir_exe = executable(
  'test_ir',
  files('test_ir.c'),
  parser_src,
  ir_src,
  io_src,
  thread_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [threads_dep],
  link_args: host_machine.system() == 'linux' ? ['-Wl,--wrap=realloc'] : [],
  override_options: host_machine.system() == 'linux' ? ['b_lto=false'] : [],
)

test_program_exe = executable(
  'test_program',
  files('test_program.c'),
  parser_src,
  ir_src,
  io_src,
  thread_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [threads_dep],
)

if host_machine.system() == 'linux'
  test_column_names_oom_exe = executable(
    'test_column_names_oom',
    files('test_column_names_oom.c'),
    parser_src,
    ir_src,
    io_src,
    thread_src,
    include_directories: [wirelog_inc, wirelog_src_inc],
    dependencies: [threads_dep],
    link_args: ['-Wl,--wrap=malloc'],
    override_options: ['b_lto=false'],
  )

  test_program_oom_exe = executable(
    'test_program_oom',
    files('test_program_oom.c'),
    parser_src,
    ir_src,
    io_src,
    thread_src,
    include_directories: [wirelog_inc, wirelog_src_inc],
    dependencies: [threads_dep],
    link_args: ['-Wl,--wrap=malloc', '-Wl,--wrap=calloc', '-Wl,--wrap=realloc'],
    override_options: ['b_lto=false'],
  )
endif

test_stratify_exe = executable(
  'test_stratify',
  files('test_stratify.c'),
  parser_src,
  ir_src,
  io_src,
  thread_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [threads_dep],
  link_args: host_machine.system() == 'linux' ? ['-Wl,--wrap=realloc'] : [],
  override_options: host_machine.system() == 'linux' ? ['b_lto=false'] : [],
)

test_stratify_scc_bounds_exe = executable(
  'test_stratify_scc_bounds',
  files('test_stratify_scc_bounds.c'),
  parser_src,
  ir_src,
  io_src,
  thread_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [threads_dep],
)

test_parse_duplicate_decl_no_leak_exe = executable(
  'test_parse_duplicate_decl_no_leak',
  files('test_parse_duplicate_decl_no_leak.c'),
  parser_src,
  ir_src,
  io_src,
  thread_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [threads_dep],
)

# ============================================================================
# Register Tests
# ============================================================================

test('lexer', test_lexer_exe)
test('parser', test_parser_exe)
test('ir', test_ir_exe)
test('program', test_program_exe)
if host_machine.system() == 'linux'
  test('column_names_oom', test_column_names_oom_exe)
  test('program_oom', test_program_oom_exe)
endif
test('stratify', test_stratify_exe)
test('stratify_scc_bounds', test_stratify_scc_bounds_exe)
test('parse_duplicate_decl_no_leak', test_parse_duplicate_decl_no_leak_exe)

# ============================================================================
# Fuzz scaffolds (Issue #743). Build only with -Denable_fuzz=true.
# ============================================================================

if get_option('enable_fuzz')
  if cc.get_id() == 'clang'
    bash = find_program('bash', required: false)
    fuzz_smoke_helper = meson.current_source_dir() / 'fuzz/run_fuzz_smoke.sh'
    fuzz_work_root = meson.current_build_dir() / 'fuzz-smoke'

    if not bash.found()
      warning('enable_fuzz=true was requested, but bash was not found. '
        + 'Skipping parser_fuzz, csv_reader_fuzz, intern_fuzz, and '
        + 'compound_arena_fuzz smoke tests.')
    else
      parser_fuzz_smoke_workdir = fuzz_work_root / 'parser'
      csv_reader_fuzz_smoke_workdir = fuzz_work_root / 'csv_reader'
      intern_fuzz_smoke_workdir = fuzz_work_root / 'intern'
      compound_arena_fuzz_smoke_workdir = fuzz_work_root / 'compound_arena'

      parser_fuzz_corpus = meson.current_source_dir() / 'fuzz/corpus/parser'
      parser_fuzz = executable(
        'parser_fuzz',
        files('fuzz/parser_fuzz.c'),
        parser_src,
        c_args: ['-fsanitize=fuzzer'],
        link_args: ['-fsanitize=fuzzer'],
        include_directories: [wirelog_inc, wirelog_src_inc],
        install: false,
      )

      test(
        'parser_fuzz_smoke',
        bash,
        args: [
          fuzz_smoke_helper,
          parser_fuzz.full_path(),
          parser_fuzz_corpus,
          parser_fuzz_smoke_workdir,
          '-runs=1',
          '-max_total_time=1',
        ],
        suite: 'fuzz',
        timeout: 30,
      )

      csv_reader_fuzz_corpus = meson.current_source_dir() / 'fuzz/corpus/csv_reader'
      csv_reader_fuzz = executable(
        'csv_reader_fuzz',
        files('fuzz/csv_reader_fuzz.c'),
        files(
          '../wirelog/io/csv_reader.c',
          '../wirelog/intern.c',
        ),
        wirelog_thread_src,
        c_args: ['-fsanitize=fuzzer'],
        link_args: ['-fsanitize=fuzzer'],
        include_directories: [wirelog_inc, wirelog_src_inc],
        dependencies: [threads_dep],
        install: false,
      )

      test(
        'csv_reader_fuzz_smoke',
        bash,
        args: [
          fuzz_smoke_helper,
          csv_reader_fuzz.full_path(),
          csv_reader_fuzz_corpus,
          csv_reader_fuzz_smoke_workdir,
          '-runs=1',
          '-max_total_time=1',
        ],
        suite: 'fuzz',
        timeout: 30,
      )

      intern_fuzz_corpus = meson.current_source_dir() / 'fuzz/corpus/intern'
      intern_fuzz = executable(
        'intern_fuzz',
        files('fuzz/intern_fuzz.c',
          '../wirelog/intern.c'),
        wirelog_thread_src,
        include_directories: [wirelog_inc, wirelog_src_inc],
        c_args: ['-fsanitize=fuzzer'],
        link_args: ['-fsanitize=fuzzer'],
        dependencies: [threads_dep],
        install: false,
      )

      test(
        'intern_fuzz_smoke',
        bash,
        args: [
          fuzz_smoke_helper,
          intern_fuzz.full_path(),
          intern_fuzz_corpus,
          intern_fuzz_smoke_workdir,
          '-runs=1',
          '-max_total_time=1',
        ],
        suite: 'fuzz',
        timeout: 30,
      )

      compound_arena_fuzz_corpus = meson.current_source_dir() / 'fuzz/corpus/compound_arena'
      compound_arena_fuzz = executable(
        'compound_arena_fuzz',
        files('fuzz/compound_arena_fuzz.c',
          '../wirelog/arena/compound_arena.c',
          '../wirelog/util/log.c',
          '../wirelog/util/log_emit.c'),
        include_directories: [wirelog_inc, wirelog_src_inc],
        c_args: ['-fsanitize=fuzzer'],
        link_args: ['-fsanitize=fuzzer'],
        install: false,
      )

      test(
        'compound_arena_fuzz_smoke',
        bash,
        args: [
          fuzz_smoke_helper,
          compound_arena_fuzz.full_path(),
          compound_arena_fuzz_corpus,
          compound_arena_fuzz_smoke_workdir,
          '-runs=1',
          '-max_total_time=1',
        ],
        suite: 'fuzz',
        timeout: 30,
      )
    endif
  else
    warning('enable_fuzz=true was requested, but libFuzzer builds require clang. '
      + 'Skipping parser_fuzz, csv_reader_fuzz, intern_fuzz, and '
      + 'compound_arena_fuzz targets.')
  endif
endif

test_fusion_exe = executable(
  'test_fusion',
  files('test_fusion.c'),
  parser_src,
  ir_src,
  optimizer_src,
  io_src,
  thread_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [threads_dep],
)

test('fusion', test_fusion_exe)

test_jpp_exe = executable(
  'test_jpp',
  files('test_jpp.c'),
  parser_src,
  ir_src,
  optimizer_src,
  io_src,
  thread_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [threads_dep],
  # Issue #1111: the allocation-failure sweeps need calloc AND malloc
  # interposition -- jpp.c allocates its key arrays with calloc and the key
  # strings through strdup_safe(), which uses malloc.
  #
  # Issue #1119: realloc as well.  insert_projections() reaches realloc only
  # through wl_ir_node_add_child(), and that is the ONLY route to the
  # accumulator desynchronisation fixed in the same commit that added this
  # wrap -- neither the calloc nor the malloc sweep can reach it.
  #
  # b_lto=false is kept because it is the PORTABLE guarantee that --wrap is
  # applied before the linker can resolve these calls internally, and the
  # project default is b_lto=true.  Keep the two ternaries on the SAME
  # predicate so the pairing cannot drift apart.
  #
  # Do not read more into it than that.  Measured on gcc 16.2.1 with this
  # binutils, forcing b_lto=true on this target alone did NOT break the
  # interposition: the objects were genuinely LTO (gnu.lto ELF sections
  # present, zero calloc@plt), yet every allocation still routed through
  # __wrap_calloc, the baseline counts were unchanged, and with the
  # insert_projections() goto reverted the LTO build still reported
  # "realloc #0 ... zero keys (cross product)".  So --wrap surviving -flto is
  # toolchain-dependent, and on at least one mainstream toolchain it survives.
  #
  # Consequently the build does NOT enforce the pairing, and neither does the
  # test: run_jpp_oom_sweep()'s "allocator interposition inert" assertion
  # detects one specific condition, ZERO recorded wrapper calls in the unarmed
  # baseline.  Dropping override_options here is not guaranteed to reach that
  # condition -- done on gcc 16.2.1 it yields a green 30/30 with a fully live
  # sweep, not a failure.
  #
  # The error direction is at least safe: where LTO genuinely does defeat the
  # wrap the counts fall to zero and the assertion fires, and where it does
  # not the sweep is live.  Neither branch leaves a quietly vacuous sweep.
  #
  # Threading precondition: the wrap state is process-global and non-atomic.
  # It is sound because arming is scoped to the wl_jpp_apply() call and that
  # pass spawns nothing.  If program construction ever leaves an allocating
  # background thread live, the sweep goes flaky rather than wrong; TSan is
  # the guard, and the counters would then need to be _Atomic.
  link_args: host_machine.system() == 'linux' ? ['-Wl,--wrap=calloc', '-Wl,--wrap=malloc', '-Wl,--wrap=realloc'] : [],
  override_options: host_machine.system() == 'linux' ? ['b_lto=false'] : [],
)

test('jpp', test_jpp_exe)

test_sip_exe = executable(
  'test_sip',
  files('test_sip.c'),
  parser_src,
  ir_src,
  optimizer_src,
  io_src,
  thread_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [threads_dep],
)

test('sip', test_sip_exe)

# ============================================================================
# Rust FFI Tests (deferred to Phase 2A columnar backend)
# ============================================================================
#
# The following tests require the Rust Differential Dataflow executor:
# - test_dd_ffi
# - test_dd_execute
# - test_cli
# - test_session
# - test_recursive_agg_baseline
# - test_recursive_agg
# - test_recursive_agg_cc_min
# - test_recursive_agg_sssp_max
#
# These are deferred to Phase 2A when the columnar backend is implemented.

# ============================================================================
# Columnar Backend Session Tests (no Rust required)
# ============================================================================

# Columnar backend tests disabled in Phase 2C
# test_session_columnar and test_delta_callback depend on deleted DD backend
# These will be re-enabled when columnar backend provides equivalent functionality

test_csv_exe = executable(
  'test_csv',
  files('test_csv.c'),
  csv_src,
  intern_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [threads_dep],
)

test('csv', test_csv_exe)

# Issue #508: bench_argv shim unit tests. Header-only, portable by
# construction; runs on every platform including MSVC.
test_bench_argv_exe = executable(
  'test_bench_argv',
  files('test_bench_argv.c'),
  include_directories: [wirelog_inc, wirelog_src_inc],
)
test('bench_argv', test_bench_argv_exe,
     suite: 'bench')

test('bench_flowlog_tdd_bdx_smoke', bench_flowlog,
     args: [
       '--workload', 'tdd-bdx',
       '--data', meson.project_source_root() / 'bench/data/graph_10.csv',
       '--workers', '4',
       '--repeat', '1',
       '--format', 'json',
     ],
     suite: 'bench',
     timeout: 30)

bench_flowlog_json_py3 = find_program('python3', 'python', required: true)
bench_flowlog_tdd_json_validator = files('validate_bench_flowlog_tdd_json.py')
test('bench_flowlog_tdd_bdx_json', bench_flowlog_json_py3,
     args: [
       bench_flowlog_tdd_json_validator[0],
       bench_flowlog.full_path(),
       meson.project_source_root() / 'bench/data/graph_100.csv',
     ],
     depends: bench_flowlog,
     suite: 'bench',
     timeout: 30)

# Issue #287: logger env parser tests (pure function; no env mutation).
test_log_parse_exe = executable(
  'test_log_parse',
  files('test_log_parse.c'),
  files(
    '../wirelog/util/log.c',
    '../wirelog/util/log_emit.c',
  ),
  include_directories: [wirelog_inc, wirelog_src_inc],
)
test('log_parse', test_log_parse_exe,
     suite: 'log')

# Issue #287: logger gating tests (compile-time ceiling + runtime threshold).
# Links log.c + log_testhook.c (test emit capture); log_emit.c is deliberately
# omitted to avoid duplicate wl_log_emit definitions. The ceiling-override TU
# is built with its own -DWL_LOG_COMPILE_MAX_LEVEL=1 to exercise the
# compile-time short-circuit (Case A). Per-target c_args are appended after
# add_project_arguments, so the narrower ceiling wins in that TU.
test_log_gating_ceiling_lib = static_library(
  'test_log_gating_ceiling',
  files('test_log_gating_ceiling.c'),
  c_args: ['-UWL_LOG_COMPILE_MAX_LEVEL', '-DWL_LOG_COMPILE_MAX_LEVEL=1'],
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test_log_gating_exe = executable(
  'test_log_gating',
  files('test_log_gating.c'),
  files(
    '../wirelog/util/log.c',
    '../wirelog/util/log_testhook.c',
  ),
  link_with: test_log_gating_ceiling_lib,
  include_directories: [wirelog_inc, wirelog_src_inc],
)
test('log_gating', test_log_gating_exe,
     suite: 'log')

# Issue #287: TSan sanity test - readers spin on wl_log_thresholds while one
# writer flips it. No torn reads, TSan clean under the sanitizer build.
# Skipped on Windows/MSVC: uses <pthread.h> and __atomic_* GCC/Clang
# builtins; TSan is a Linux-only sanitizer, so the test has no reason to
# build on MSVC.
if not is_windows
  test_log_threshold_race_exe = executable(
    'test_log_threshold_race',
    files('test_log_threshold_race.c'),
    files(
      '../wirelog/util/log.c',
      '../wirelog/util/log_emit.c',
    ),
    include_directories: [wirelog_inc, wirelog_src_inc],
    dependencies: [threads_dep],
  )
  test('log_threshold_race', test_log_threshold_race_exe,
       suite: 'log',
       timeout: 30)
endif

# Issue #287: release-mode perf gate (suite 'perf').  Note suite membership
# does not exclude anything -- there is no add_test_setup in this project, so
# every perf entry runs in a default `meson test` and opts out at runtime via
# WIRELOG_PERF_GATE.
# Requires WIRELOG_PERF_GATE=1 and a stable timing host. The authoritative CI
# job uses separate trace and error build directories: log_perf_gate runs in
# the trace build, while the three evaluator timing gates run in the error
# build. Linux requires a 'performance' cpufreq governor; Windows uses thread affinity +
# HIGH_PRIORITY_CLASS via bench_stability_prep() (issue #508 Phase 2).
# A baseline coefficient-of-variation self-check converts noisy runs to SKIP
# instead of FAIL so shared CI runners never flag a false regression.
#   Linux:   env WIRELOG_PERF_GATE=1 taskset -c 0 meson test -C build --suite perf
#   Windows: set WIRELOG_PERF_GATE=1 & meson test -C build --suite perf
# math_dep supplies sqrt() for the variance self-check.
test_log_perf_gate_exe = executable(
  'test_log_perf_gate',
  files('test_log_perf_gate.c'),
  files(
    '../wirelog/util/log.c',
    '../wirelog/util/log_emit.c',
  ),
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [math_dep],
)
test('log_perf_gate', test_log_perf_gate_exe,
     suite: 'perf',
     timeout: 120)

# Issue #287: end-to-end integration - WL_LOG=JOIN:5 + WL_LOG_FILE captures
# one "[TRACE][JOIN] ... hello 42" line; unset produces zero output.
test_log_integration_exe = executable(
  'test_log_integration',
  files('test_log_integration.c'),
  files(
    '../wirelog/util/log.c',
    '../wirelog/util/log_emit.c',
  ),
  include_directories: [wirelog_inc, wirelog_src_inc],
)
test('log_integration', test_log_integration_exe,
     suite: 'log')

# Issue #287: legacy WL_DEBUG_JOIN/WL_CONSOLIDATION_LOG presence-check shim.
# Asserts WL_DEBUG_JOIN=0 still enables JOIN at TRACE; WL_LOG overrides shim.
test_log_legacy_shim_exe = executable(
  'test_log_legacy_shim',
  files('test_log_legacy_shim.c'),
  files(
    '../wirelog/util/log.c',
    '../wirelog/util/log_emit.c',
  ),
  include_directories: [wirelog_inc, wirelog_src_inc],
)
test('log_legacy_shim', test_log_legacy_shim_exe,
     suite: 'log')

# Issue #287: ABI lints for the logger. These are POSIX shell scripts that
# shell out to nm/objdump/awk; no Windows equivalent exists, and MSVC CI
# does not ship bash. Linux/macOS coverage is sufficient for the invariants
# they check (compile-erasure, header non-leakage, testhook symbol absence).
if not is_windows
  # Compile-erasure: rebuild libwirelog with -Dwirelog_log_max_level=error
  # in an isolated build dir; assert TRACE-level WL_LOG() sentinels are
  # absent from .rodata.
  test('log_abi_compile_erasure',
       find_program(meson.project_source_root() / 'scripts/ci/check-log-erasure.sh'),
       suite: 'abi',
       timeout: 180)

  # Header must not leak into public surface; testhook symbols must not
  # appear in libwirelog.
  test('log_abi_header_not_public',
       find_program(meson.project_source_root() / 'scripts/check_log_header_not_public.sh'),
       suite: 'abi')
  test('log_abi_no_testhook_in_libwirelog',
       find_program(meson.project_source_root() / 'scripts/ci/check-no-testhook-in-libwirelog.sh'),
       args: [meson.project_build_root()],
       suite: 'abi')
  test('wirelog_easy_abi_opts_symbol',
       find_program(meson.project_source_root() / 'scripts/ci/check-wl-easy-opts-symbol.sh'),
       args: [meson.project_build_root()],
       suite: 'abi')
  # Issue #717 / #703: wirelog/wirelog-advanced.h must not leak any
  # internal header (session.h, backend.h, exec_plan*.h, intern.h,
  # session_facts.h, wirelog-internal.h) into the installed surface.
  test('advanced_abi_header',
       find_program(meson.project_source_root() / 'scripts/ci/check-advanced-header.sh'),
       suite: 'abi')
endif

# Issue #688 / #704: meson.build version, the generated wirelog-version.h,
# and the template's @-placeholders must agree.  Python so it runs on
# Windows runners too.  Pass the build directory that actually contains
# the generated header (see wirelog/meson.build's configure_file()).
py3 = find_program('python3', 'python', required: true)
test('version_sync', py3,
     args: [meson.project_source_root() / 'scripts/ci/check-version-sync.py',
            meson.project_build_root() / 'wirelog'],
     suite: 'abi')

# Issue #705 / Blocker B2: every in-tree mirror of the public header
# surface must agree with `wirelog_public_headers` (plus the standalone
# `install_headers('wirelog/io/io_adapter.h', ...)` call) in
# meson.build. Static parse only — no build-tree input required.
test('public_header_surface', py3,
     args: [meson.project_source_root() / 'scripts/ci/check-public-header-surface.py',
            meson.project_source_root()],
     suite: 'abi')

# Issue #753: PSIRT response targets and the supported-version policy must
# remain present and internally linked. Static text scan; no build-tree input.
test('support_policy', py3,
     args: [meson.project_source_root() / 'scripts/ci/check-support-policy.py'],
     suite: 'abi')

# Issue #761: backstop CI lint preventing wl_*/WL_* prefixes from leaking
# back onto public installed headers after the v0.40 API audit closes its
# sibling rename issues (#762 / #756 / #757 / #758 / #759 / #760).
# Static text scan; no build-tree input required.
test('public_prefix', py3,
     args: [meson.project_source_root() / 'scripts/ci/check-public-prefix.py'],
     suite: 'abi')

# Issue #782 backstop: WIRELOG_PUBLIC must not re-appear on the 8
# prototype headers after the v0.40-cycle adoption sweep to WIRELOG_API.
# The macro itself lives in wirelog/wirelog-export.h (exempt).
test('public_api_macro', py3,
     args: [meson.project_source_root() / 'scripts/ci/check-public-api-macro.py'],
     suite: 'abi')

# Issue #841: every installed public WIRELOG_API function declaration
# must be present in the ABI dynamic-symbol allowlist.
test('public_api_exports', py3,
     args: [meson.project_source_root() / 'scripts/ci/check-public-api-exports.py'],
     suite: 'abi')

# Issue #785: cross-facade test parity.  Every test_* function in
# tests/test_wirelog_easy.c must either pair with a same-named
# function in tests/test_wirelog_advanced.c or carry a
# `/* PARITY: ... */` annotation on the line(s) above its
# declaration.  Static text scan.
test('test_parity', py3,
     args: [meson.project_source_root() / 'scripts/ci/check-test-parity.py'],
     suite: 'abi')

# Issue #780 / Epic #680 exit gate: every public header must carry
# `@file` and `@brief` inside a `/** ... */` Doxygen block.  Header
# list is sourced from `parse_meson_sot` in check-public-header-surface.py
# (which authoritatively parses meson.build), so the lint adapts
# automatically to surface additions/removals.  Static text scan; no
# build-tree input required.
test('public_doxygen_headers', py3,
     args: [meson.project_source_root() / 'scripts/ci/check-public-doxygen-headers.py'],
     suite: 'abi')

# Issue #471: CHANGELOG format gate.  Asserts the [Unreleased] section
# follows Keep-a-Changelog conventions documented in CONTRIBUTING.md
# (categories, PR/issue refs, dated versioned sections).  Static text
# scan; no build-tree input required.
test('changelog_format', py3,
     args: [meson.project_source_root() / 'scripts/ci/check-changelog-format.py',
            meson.project_source_root() / 'CHANGELOG.md'],
     suite: 'abi')

# Shell interpreter for the abi-suite shell scripts below.  Resolved
# once and reused; tests that depend on `sh.found()` are gated on
# the same handle so platforms without bash (e.g. some Windows
# runners) silently skip both gates instead of failing
# inconsistently.
sh = find_program('bash', required: false)

# Issue #772: release-template gate.  Asserts that the GitHub Releases
# body for the current tag matches the corresponding CHANGELOG section.
# SKIPs everywhere in CI today: on PR and main builds HEAD is not a tag,
# and release-tag.yml's Tag / ABI job both supplies no GH_TOKEN and runs
# before the release is published.  No open PR changes either; see the
# script header.  Until then this gate enforces nothing anywhere.
if sh.found()
  test('release_template', sh,
       args: [meson.project_source_root() / 'scripts/ci/check-release-template.sh'],
       suite: 'abi')
endif

# Issue #1288: the gate above exited 0 on all four of its skip paths, so meson
# reported it OK on every run while it asserted nothing.  Those paths now exit
# 77.  This self-test drives the real gate through all seven of its terminal
# outcomes from a fixture repository, including the mismatch it exists to catch
# -- which, because the gate enforces nowhere in CI today, had never run.
if sh.found()
  test('release_template_selftest', sh,
       args: [meson.project_source_root() /
              'scripts/ci/test-check-release-template.sh'],
       suite: 'abi')
endif

# Issue #747 B18: RC changelog selftest for changelog-freeze checker.
# Shell gate uses the same shared `sh` handle to keep skip behavior
# consistent on platforms without bash.
if sh.found()
  test('changelog_rc_selftest', sh,
       args: [meson.project_source_root() / 'scripts/ci/test-check-changelog-rc.sh'],
       suite: 'abi')
endif

# Issue #734/#1173: docs/THREADING.md atomics audit rows must resolve to
# logical atomic_* source lines, and the count must match production sources.
# Static shell checks; no build-tree input required.
if sh.found()
  test('threading_doc', sh,
       args: [meson.project_source_root() / 'scripts/ci/check-threading-doc.sh'],
       suite: 'abi')
  test('threading_doc_selftest', sh,
       args: [meson.project_source_root() / 'scripts/ci/test-threading-doc.sh'],
       suite: 'abi')
endif

# Issue #715: release dependency wraps must be reproducible.  Git wraps
# must pin immutable 40-hex commits; wrap-file archive URLs must have
# matching source_hash/patch_hash entries.
if sh.found()
  test('wrap_revisions', sh,
       args: [meson.project_source_root() / 'scripts/ci/check-wrap-revisions.sh'],
       suite: 'abi')
endif

# Issue #737: forbid new unanchored "Phase NX" labels in wirelog/ source.
# Each Phase NX label must either carry an explicit issue cross-reference
# on the same line ("#N", "Issue N", "US-N-NNN"), or be present in the
# historical allowlist at scripts/ci/phase-label-allowlist.txt.  Static
# text scan; no build-tree input required.
if sh.found()
  test('phase_labels', sh,
       args: [meson.project_source_root() / 'scripts/ci/check-phase-labels.sh'],
       suite: 'abi')
endif

# Issue #742: every `Status: Future` entry in docs/SEMANTICS.md must
# cite a milestone or issue link within an 8-line window of the
# heading.  Prevents the doc from accruing unbounded "Future" entries
# with no schedule.  Static text scan; no build-tree input required.
if sh.found()
  test('semantics_future', sh,
       args: [meson.project_source_root() / 'scripts/ci/check-semantics-future.sh'],
       suite: 'abi')
endif

# Issue #952 (follow-up to #950): the DOOP .facts catalogue is written down
# in four places -- download.sh, doop_edbs[], doop_fact_files[], and the
# fact-file count in run_doop_validation.sh.  They drifted apart silently
# once already, and the test that should have caught it skipped itself
# instead.  Static text scan; needs no dataset, so it runs on hosts that
# will never fetch the 740 MB archive.
if sh.found()
  test('doop_catalogue', sh,
       args: [meson.project_source_root() / 'scripts/ci/check-doop-catalogue.sh',
              meson.project_source_root()],
       suite: 'abi')
endif

# Issue #1163: the GA downstream matrix must contain exactly six pinned
# workloads, and the mutable DOOP source must fail its checksum negative case.
if sh.found()
  test('downstream_matrix_contract', sh,
       args: [meson.project_source_root() / 'scripts/ci/test-downstream-matrix.sh',
              meson.project_source_root()],
       suite: 'abi')
endif

# Issue #1285: the closure gate below can only verify libraries the
# owned-soname set names, and the derivation of that set only ever ran on a
# release tag.  Extracted to scripts/release/derive-owned-sonames.sh so its
# edge cases -- symlink chains, .p object directories, .symbols by-products,
# an empty tree -- are exercised on every PR.
# Linux-gated: the fixture builds a symlink chain with `ln -s`, which on
# MSYS/Git-Bash copies instead of linking, so the symlink assertions would pass
# for a different reason there rather than exercising what they name.
if host_machine.system() == 'linux' and sh.found()
  test('owned_sonames', sh,
       args: [meson.project_source_root() / 'scripts/ci/test-derive-owned-sonames.sh',
              meson.project_source_root()],
       suite: 'abi')
endif

# Issue #1272: the release upgrade matrix must prove the consumer loaded the
# libraries this release installs, not whatever the host had installed.  The
# matrix itself only runs on a release tag, so this self-test is the only
# pre-release coverage the closure parser gets.
if sh.found()
  test('shared_library_closure', sh,
       args: [meson.project_source_root() /
              'scripts/ci/test-check-shared-library-closure.sh',
              meson.project_source_root()],
       suite: 'abi')
endif

# Issue #913: keep the weighted/semiring design proposal explicit about its
# current Z-set baseline, future-only ABI, retraction contract, and links.
test('weighted_semiring_design', py3,
     args: [meson.project_source_root() /
            'scripts/ci/check-weighted-semiring-design.py'],
     suite: 'abi')

# Issue #912: keep the future scalar-function addon boundary, callback
# contract, and cross-document links explicit without adding runtime API.
test('scalar_function_addon_design', py3,
     args: [meson.project_source_root() /
            'scripts/ci/check-scalar-function-addon-design.py'],
     suite: 'abi')
test('scalar_function_addon_design_fixtures', py3,
     args: [meson.project_source_root() /
            'tests/test_scalar_function_addon_design.py'],
     suite: 'abi')

# Issue #733 K2 (cross-link #690 B3): pin the dynamic-symbol table of
# libwirelog.so against `abi/libwirelog-1.0.symbols`.  Any new public
# symbol must update the allowlist in the same PR; any accidental loss
# of an exported symbol fails the gate.  Linux/ELF-focused; SKIPs
# cleanly on platforms without `libwirelog.so` (Windows, static-only).
if sh.found()
  test('abi_symbols', sh,
       args: [meson.project_source_root() / 'scripts/ci/check-abi-symbols.sh',
              meson.project_build_root()],
       suite: 'abi')
endif

if host_machine.system() == 'linux' and sh.found()
  test('abi_symbols_locale', sh,
       args: [meson.project_source_root() / 'scripts/ci/check-abi-symbols-locale.sh'],
       suite: 'abi')
endif

# Issue #1291: sbom/snapshot.txt is a committed artifact, so its order must not
# depend on the locale of whoever regenerates it.  Same bug class as #886 and
# the same shape of guard: a pin without a regression test is a pin that gets
# deleted later.  SKIPs without an en_US locale or jq; needs no syft.
if host_machine.system() == 'linux' and sh.found()
  test('sbom_snapshot_locale', sh,
       args: [meson.project_source_root() / 'scripts/ci/check-sbom-snapshot-locale.sh'],
       suite: 'sbom')
endif

# Issue #788: warning-only platform ABI surface advisory checks.
# Linux remains the hard v1.0 ABI gate.  These checks run on every
# default `meson test` invocation but only emit GitHub warning
# annotations when macOS/Windows export surfaces drift.
if host_machine.system() == 'darwin' and sh.found()
  test('abi_symbols_macos', sh,
       args: [meson.project_source_root() / 'scripts/ci/check-abi-symbols-macos.sh',
              meson.project_build_root()],
       suite: 'abi_advisory')
endif

powershell = find_program('pwsh', 'powershell', required: false)
if host_machine.system() == 'windows' and powershell.found()
  test('abi_symbols_windows', powershell,
       args: ['-NoProfile',
              '-ExecutionPolicy', 'Bypass',
              '-File', meson.project_source_root() / 'scripts/ci/check-abi-symbols-windows.ps1',
              meson.project_build_root()],
       suite: 'abi_advisory')
endif

# Issue #786 (cross-link #690 libabigail half): libabigail manifest gate.
# Where abi_symbols only checks the dynamic-symbol set, abi_manifest uses
# `abidiff` to catch struct member offsets, function signature changes,
# and visibility/linkage drift that nm -D cannot see.  Linux/ELF-focused;
# SKIPs cleanly when libwirelog.so is missing (Windows, static-only),
# when abidiff is not on PATH, or when the baseline has not been seeded
# yet (run scripts/release/regenerate-abi-manifest.sh to seed).  Per
# issue #824, the committed v1.0 libabigail baseline is x86_64-only;
# Linux arm64 v1.0 ABI coverage is symbol-only via abi_symbols.
if sh.found()
  test('abi_manifest', sh,
       args: [meson.project_source_root() / 'scripts/ci/check-abi-manifest.sh',
              meson.project_build_root()],
       suite: 'abi')
endif

# Issue #744: SBOM snapshot gate.  Diffs the detected dependency list
# (packages + versions) against the committed baseline (sbom/snapshot.txt).
# SKIPs when syft is not on PATH or when the baseline has not been seeded yet
# (run scripts/release/generate-sbom.sh to seed).  Fails when dependencies
# change without a baseline update (enforces explicit review of dependency drift).
if sh.found()
  test('sbom_snapshot', sh,
       args: [meson.project_source_root() / 'scripts/ci/check-sbom-snapshot.sh'],
       suite: 'sbom',
       timeout: 120)
endif

# Issue #1294: two release gates hash a sorted file list and compare it against
# a pinned or committed value.  Unpinned collation made a non-C runner produce a
# different manifest, failing with a message that accuses the dataset.
if host_machine.system() == 'linux' and sh.found()
  test('manifest_collation', sh,
       args: [meson.project_source_root() / 'scripts/ci/check-manifest-collation.sh'],
       suite: 'abi')
endif

# Issue #1100: clang-tidy ratchet (suite 'tidy').  No CI job has run
# clang-tidy since efbde27 removed it, and wirelog/ir/program.c regressed
# after f1d2a2c with nothing failing.  The gate re-analyses every file in
# scripts/ci/clang-tidy-allowlist.txt on every run; the files in
# clang-tidy-backlog.txt are excluded, and the two lists must partition the
# libwirelog.so entries of compile_commands.json exactly.  Neither count is
# recorded here: promoting a file moves both.
#
# As with the perf gate above, suite membership excludes nothing (there is
# no add_test_setup in this project), so these run in a default
# `meson test`.  Opting out is a runtime decision instead: the ratchet SKIPs
# unless clang-tidy is present at a supported major
# (scripts/ci/clang-tidy-supported-majors.txt), the host is Linux on
# x86_64, and the database is free of -fsanitize= / MSVC command lines --
# so the sanitizer, tsan, macOS, MSVC and build-arm jobs skip it rather
# than reporting an uncalibrated verdict.  The arch guard is what covers
# build-arm: it is Linux, uninstrumented and not MSVC, so only the target
# check distinguishes it.  WIRELOG_TIDY_REQUIRED=1 (set by build-primary)
# turns those skips into failures; WIRELOG_TIDY_SKIP=1 forces a skip.
#
# Cost: ~12-16 s wall at 8 jobs, ~113 s summed CPU, on a suite whose
# baseline is ~20 s wall.  priority: 100 starts it early so it overlaps the
# rest rather than trailing them.  The 900 s timeout is generous against
# the 12.7 s worst single file (exec_plan_gen.c) on a loaded runner.
if not is_windows
  test('clang_tidy_ratchet', py3,
       args: [meson.project_source_root() / 'scripts/ci/check-clang-tidy-ratchet.py',
              '--build-dir', meson.project_build_root(),
              '--mode', 'ratchet'],
       suite: 'tidy',
       timeout: 900,
       priority: 100)

  # Sensitivity control: a fixture that no meson target compiles and that
  # appears in neither list, carrying one deliberate bugprone-integer-
  # division.  Without it a ratchet that silently analysed the wrong file
  # (issue #1079's symlinked build dir) would report everything clean.
  test('clang_tidy_sensitivity', py3,
       args: [meson.project_source_root() / 'scripts/ci/check-clang-tidy-ratchet.py',
              '--build-dir', meson.project_build_root(),
              '--mode', 'sensitivity'],
       suite: 'tidy',
       timeout: 300,
       priority: 100)

  test('clang_tidy_guard_unit', py3,
       args: [meson.project_source_root() / 'tests/test_clang_tidy_ratchet_guards.py'],
       suite: 'tidy')

  # Demotion guard: the partition assertion above catches a DELETED
  # allowlist line, but moving one to the backlog keeps the partition exact.
  # Compares against the merge base with the base branch, so a backlog line
  # that main removes while a branch is open is not misread as an addition.
  # SKIPs when no merge base is available (shallow clone, tarball).
  if sh.found()
    test('clang_tidy_backlog_monotonic', sh,
         args: [meson.project_source_root() / 'scripts/ci/check-clang-tidy-backlog-monotonic.sh'],
         suite: 'tidy')
  endif
endif

# Issue #689 / Blocker B2: standalone-include compile matrix for the
# public-headers SSoT.  One executable per public header, each
# compiled in isolation against tests/standalone/test_standalone_<H>.c
# whose body is `#include "wirelog/<H>.h"; int main(void) { return 0; }`.
# Failure = the header has a hidden dependency on another header being
# included first.  The cross-compiler matrix (GCC / Clang / MSVC) is
# realised at the GitHub Actions level: each platform compiles every
# stub via its native CC.
public_standalone_headers = [
  ['wirelog',           'test_standalone_wirelog.c'],
  ['wirelog_types',     'test_standalone_wirelog_types.c'],
  ['wirelog_ir',        'test_standalone_wirelog_ir.c'],
  ['wirelog_parser',    'test_standalone_wirelog_parser.c'],
  ['wirelog_optimizer', 'test_standalone_wirelog_optimizer.c'],
  ['wirelog_export',    'test_standalone_wirelog_export.c'],
  ['wirelog_easy',      'test_standalone_wirelog_easy.c'],
  ['wirelog_advanced',  'test_standalone_wirelog_advanced.c'],
  ['io_adapter',        'test_standalone_io_adapter.c'],
  ['wirelog_extension', 'test_standalone_wirelog_extension.c'],
]
foreach hdr : public_standalone_headers
  exe = executable(
    'test_standalone_' + hdr[0],
    files('standalone/' + hdr[1]),
    include_directories: [wirelog_inc, wirelog_src_inc],
  )
  test('standalone_include_' + hdr[0], exe, suite: 'abi')
endforeach

# Issue #782: compile-only smoke test for WIRELOG_DEPRECATED_SINCE.
# The macro is defined in wirelog/wirelog-export.h but has zero
# in-tree adopters at the time of v0.40.0 release; this stub
# annotates a static probe function with WIRELOG_DEPRECATED_SINCE(99,99)
# so the compiler exercises the attribute's expansion path
# (GCC/Clang __attribute__((deprecated)); MSVC __declspec(deprecated);
# no-op fallback elsewhere).  The deprecation diagnostic at the call
# site is suppressed per-target so it does not break -Werror CI.
deprecated_macro_smoke_warning_flags = cc.get_supported_arguments([
  '-Wno-deprecated-declarations',
  '/wd4996',
])
test_standalone_wirelog_deprecated_macro_exe = executable(
  'test_standalone_wirelog_deprecated_macro',
  files('standalone/test_standalone_wirelog_deprecated_macro.c'),
  include_directories: [wirelog_inc, wirelog_src_inc],
  c_args: deprecated_macro_smoke_warning_flags,
)
test('standalone_include_wirelog_deprecated_macro',
     test_standalone_wirelog_deprecated_macro_exe,
     suite: 'abi')

test_intern_exe = executable(
  'test_intern',
  files('test_intern.c'),
  intern_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [threads_dep],
)

test('intern', test_intern_exe)

# ============================================================================
# Plan Generator + Session Facts Tests (Phase 2C columnar backend)
# ============================================================================

backend_src = files(
  '../wirelog/columnar/relation.c',
  '../wirelog/columnar/cache.c',
  '../wirelog/columnar/ops.c',
  '../wirelog/columnar/arithmetic.c',
  '../wirelog/columnar/expr.c',
  '../wirelog/columnar/filter.c',
  '../wirelog/columnar/join.c',
  '../wirelog/columnar/merge.c',
  '../wirelog/columnar/kfusion.c',
  '../wirelog/columnar/kfusion_adaptive.c',
  '../wirelog/columnar/diff.c',
  '../wirelog/columnar/inline.c',
  '../wirelog/columnar/eval_dedup.c',
  '../wirelog/columnar/eval_delta.c',
  '../wirelog/columnar/eval_tdd_queue.c',
  '../wirelog/columnar/eval_tdd_plan.c',
  '../wirelog/columnar/eval_plan.c',
  '../wirelog/columnar/eval_stack.c',
  '../wirelog/columnar/eval_serial.c',
  '../wirelog/columnar/eval.c',
  '../wirelog/columnar/arrangement.c',
  '../wirelog/columnar/frontier.c',
  '../wirelog/columnar/frontier_epoch.c',
  '../wirelog/columnar/mobius.c',
  '../wirelog/columnar/session.c',
  '../wirelog/columnar/session_hash.c',
  '../wirelog/columnar/delta_pool.c',
  '../wirelog/columnar/lftj.c',
  '../wirelog/columnar/mem_ledger.c',
  '../wirelog/columnar/diff_trace.c',
  '../wirelog/columnar/diff_arrangement.c',
  '../wirelog/columnar/diff_vtable.c',
  '../wirelog/columnar/diff_bridge.c',
  '../wirelog/columnar/partition.c',
  '../wirelog/columnar/progress.c',
  '../wirelog/columnar/rotation_standard.c',
  '../wirelog/columnar/rotation_pinned.c',
  '../wirelog/columnar/compound_side.c',
  '../wirelog/columnar/handle_remap.c',
  '../wirelog/columnar/handle_remap_apply.c',
  '../wirelog/columnar/handle_remap_apply_side.c',
  '../wirelog/util/lockfree_queue.c',
  '../wirelog/util/log.c',
  '../wirelog/util/log_emit.c',
  '../wirelog/string_ops.c',
  # Issue #884: the columnar evaluator's WL_PLAN_EXPR_ARITH_CRC32_*
  # cases call ethernet_crc32()/castagnoli_crc32() from this TU.  Pull
  # the engine in so every backend-test binary has the symbols at
  # link time.
  '../wirelog/crc32.c',
  '../wirelog/extension_registry.c',
)

arena_src = files(
  '../wirelog/arena/arena.c',
  '../wirelog/arena/compound_arena.c',
)

workqueue_src = files(
  '../wirelog/workqueue.c',
)

test_optimizer_equivalence_exe = executable(
  'test_optimizer_equivalence',
  files('test_optimizer_equivalence.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('optimizer_equivalence', test_optimizer_equivalence_exe, suite: 'optimizer',
     timeout: 120)

test_plan_gen_exe = executable(
  'test_plan_gen',
  files('test_plan_gen.c', '../wirelog/api_facade.c',
        '../wirelog/extension_registry.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
  c_args: ['-DWIRELOG_TEST_DELTA_NAME',
           '-DWIRELOG_TEST_EXTENSION_SERIALIZATION'],
)

test('plan_gen', test_plan_gen_exe)

if host_machine.system() == 'linux'
  test_plan_gen_oom_exe = executable(
    'test_plan_gen_oom',
    files('test_plan_gen_oom.c'),
    parser_src,
    ir_src,
    optimizer_src,
    backend_src,
    io_src,
    arena_src,
    thread_src,
    workqueue_src,
    include_directories: [wirelog_inc, wirelog_src_inc],
    dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
    link_args: ['-Wl,--wrap=malloc', '-Wl,--wrap=calloc', '-Wl,--wrap=realloc'],
    override_options: ['b_lto=false'],
  )
  test('plan_gen_oom', test_plan_gen_oom_exe, suite: 'unit')
endif

test_recursive_agg_conformance_exe = executable(
  'test_recursive_agg_conformance',
  files('test_recursive_agg_conformance.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('recursive_agg_conformance', test_recursive_agg_conformance_exe,
     suite: 'semantics')

# ============================================================================
# Plan Exchange Insertion Tests (Issue #319)
# ============================================================================

test_plan_exchange_exe = executable(
  'test_plan_exchange',
  files('test_plan_exchange.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('plan_exchange', test_plan_exchange_exe)

# ============================================================================
# RDF Named-Graph Integration Tests (Issue #535)
# ============================================================================

test_rdf_named_graph_exe = executable(
  'test_rdf_named_graph',
  files('test_rdf_named_graph.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('rdf_named_graph', test_rdf_named_graph_exe)

# ============================================================================
# Workqueue Tests (Phase B-lite)
# ============================================================================

test_workqueue_exe = executable(
  'test_workqueue',
  files('test_workqueue.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('workqueue', test_workqueue_exe)

# ============================================================================
# Workqueue Capacity Tests (Issue #414)
# ============================================================================

test_workqueue_capacity_exe = executable(
  'test_workqueue_capacity',
  files('test_workqueue_capacity.c'),
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [threads_dep],
)

test('workqueue_capacity', test_workqueue_capacity_exe)

# ============================================================================
# FPGA Stub Backend Tests (Issue #496)
# ============================================================================

test_fpga_backend_exe = executable(
  'test_fpga_backend',
  files('test_fpga_backend.c', 'fpga_backend.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('fpga_backend', test_fpga_backend_exe)

# ============================================================================
# Lock-Free MPSC Queue Tests (Issue #401)
# ============================================================================

lockfree_queue_src = files(
  '../wirelog/util/lockfree_queue.c',
)

test_lockfree_queue_exe = executable(
  'test_lockfree_queue',
  files('test_lockfree_queue.c'),
  lockfree_queue_src,
  thread_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [threads_dep],
)

test('lockfree_queue', test_lockfree_queue_exe)

# ============================================================================
# Option 2 CSE Plan Rewriting Tests
# ============================================================================

test_option2_cse_exe = executable(
  'test_option2_cse',
  files('test_option2_cse.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
  c_args: ['-DENABLE_K_FUSION=1'],
)

test('option2_cse', test_option2_cse_exe)

# ============================================================================
# DOOP Validation Tests (US-009/US-010)
# ============================================================================
#
# Infrastructure tests (file checks, parse, synthetic 8-way join) run without
# the ~740MB DOOP dataset.  Data-dependent tests auto-skip when bench/data/doop
# is absent.  Full DOOP execution uses scripts/run_doop_validation.sh.

test_option2_doop_exe = executable(
  'test_option2_doop',
  files('test_option2_doop.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

# Issue #950: pins what each DOOP rule constant MEANS, on a hand-written
# miniature dataset.  The real 68 MB archive cannot be fetched in CI (and has
# already disappeared once), so these semantics have to be fixed independently
# of it.  Each case carries the negative that makes it non-vacuous.
test_doop_strings_exe = executable(
  'test_doop_strings',
  files('test_doop_strings.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('doop_strings', test_doop_strings_exe)

# Issue #962: `<`, `>`, `<=`, `>=` on symbol columns used to compare intern
# ids, so the answer depended on the order strings happened to be interned.
# Asserts exact tuples in both directions, across all four operators and both
# the `a op b` and `a op "literal"` forms, and pins that integer comparisons
# are untouched.
test_symbol_ordering_exe = executable(
  'test_symbol_ordering',
  files('test_symbol_ordering.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('symbol_ordering', test_symbol_ordering_exe)

# Issue #963: hash() and the CRC-32 built-ins digested the intern id of a
# symbol column rather than the string's bytes, so hash("abc") matched no
# external tool and the answer depended on what was interned first.  Every
# expected value here was derived outside wirelog (xxhsum -H3, zlib.crc32).
# The mbedTLS-backed half lives in test_cryptographic_hashes.c so that this
# binary runs in every build configuration.
test_symbol_digests_exe = executable(
  'test_symbol_digests',
  files('test_symbol_digests.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('symbol_digests', test_symbol_digests_exe)

# Issue #965: min()/max() over a symbol column reduced by intern id rather
# than lexicographically, so prepending one unrelated fact flipped the
# answer.  Every case is framed as intern-order independence.
test_symbol_aggregates_exe = executable(
  'test_symbol_aggregates',
  files('test_symbol_aggregates.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

# The mistyped-column case is recursive on purpose: a comparator whose
# verdicts depend on an intern table that grows during evaluation has no
# termination argument, so that failure mode is a hang, not a wrong answer.
# The whole binary runs in well under a second; the timeout bounds the hang.
test('symbol_aggregates', test_symbol_aggregates_exe, timeout: 60)

# Issue #975: recursive min()/max() canonicalisation never ran on a relation
# rewrite_multiway_delta() had fused, because the decision to run was taken
# by scanning the relation's operator list for a REDUCE and the rewrite had
# moved it into the K_FUSION operator's opaque_data.  Adding a rule that
# derives nothing changed the answer.
test_recursive_agg_kfusion_exe = executable(
  'test_recursive_agg_kfusion',
  files('test_recursive_agg_kfusion.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

# Every case runs at W = 1, 4, 8 and 16; the 16-worker recursive cases are
# the slow ones.  The timeout also bounds the failure mode where a relation
# reduced under a comparator that is not a total order never converges.
test('recursive_agg_kfusion', test_recursive_agg_kfusion_exe,
     suite: 'semantics', timeout: 120)

test_recursive_agg_kfusion_nofusion_exe = executable(
  'test_recursive_agg_kfusion_nofusion',
  files('test_recursive_agg_kfusion.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  c_args: ['-DENABLE_K_FUSION=0'],
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('recursive_agg_kfusion_nofusion', test_recursive_agg_kfusion_nofusion_exe,
     suite: 'semantics', timeout: 120)

# Issue #978: average() was never implemented -- col_op_reduce() had no AVG
# arm, so each group kept the first operand the scan happened to reach.  The
# legacy int64 executor has no float lane to return a mean in, so it is
# rejected at lowering instead.
test_average_rejected_exe = executable(
  'test_average_rejected',
  files('test_average_rejected.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

# One case evaluates a recursive stratum; the timeout bounds a non-terminating
# fixpoint rather than a slow one.
test('average_rejected', test_average_rejected_exe, timeout: 60)

test('option2_doop', test_option2_doop_exe,
  env: ['DOOP_DATA_DIR=' + meson.project_source_root() / 'bench/data/doop'],
)

# Differential test for the column-native filter kernel: the AVX2 body and
# the scalar body must select identical rows, including all 256 left-pack
# LUT entries.  Drives col_filter_select_rows() directly.
test_filter_select_rows_exe = executable(
  'test_filter_select_rows',
  files('test_filter_select_rows.c'),
  backend_src,
  files(subdir_wirelog / 'intern.c'),
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('filter_select_rows', test_filter_select_rows_exe)


# Differential test for the arrangement batch hash. The kernel runs the
# arrangement's 64-bit FNV chain in 32-bit arithmetic; the oracle here is the
# 64-bit form, so a mistake in that width argument surfaces as a mismatch.
test_arr_hash_rows_batch_exe = executable(
  'test_arr_hash_rows_batch',
  files('test_arr_hash_rows_batch.c'),
  backend_src,
  files(subdir_wirelog / 'intern.c'),
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('arr_hash_rows_batch', test_arr_hash_rows_batch_exe)

# Issue #807: validate the scalar kc <= 2 join-hash probe chain using the
# actual structured CRDT and CSPA fixtures.  This is a standalone model so
# candidate hash functions can be compared without changing production code.
test_join_hash_probe_exe = executable(
  'test_join_hash_probe',
  files('test_join_hash_probe.c'),
)
test('join_hash_probe', test_join_hash_probe_exe,
     timeout: 120,
     env: [
       'WIRELOG_JOIN_HASH_CRDT_DIR=' + meson.project_source_root() / 'bench/data/crdt',
       'WIRELOG_JOIN_HASH_CSPA_DIR=' + meson.project_source_root() / 'bench/data/cspa',
     ])

# ============================================================================
# CRDT W=1 perf-suite gate.
#
# Drives the same Datalog source as bench_flowlog --workload crdt and
# asserts the corrected result relation cardinality before enforcing the
# full-snapshot median wall-time target (currently 38,120 ms) documented
# in docs/CRDT_PERF_BASELINE.md.  Default-suite invocation skips this
# entry (suite is 'perf').  Registered here, after backend_src/arena_src/
# workqueue_src are defined, so it can link the full evaluator.
#
#   Linux:   echo performance | sudo tee /sys/.../scaling_governor
#            WIRELOG_PERF_GATE=1 WIRELOG_PERF_REQUIRE=1 \
#                meson test -C build --suite perf
#
# WIRELOG_PERF_GATE=1 must be set or the test SKIPs.  Without
# WIRELOG_PERF_REQUIRE=1 the test SKIPs on a non-performance cpufreq
# governor; with WIRELOG_PERF_REQUIRE=1 it FAILs loud (the merge
# runner asserts the host is configured for stable timing).
# ============================================================================

test_crdt_perf_gate_exe = executable(
  'test_crdt_perf_gate',
  files('test_crdt_perf_gate.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)
test('crdt_perf_gate', test_crdt_perf_gate_exe,
     suite: 'perf',
     timeout: 600,
     env: [
       'WIRELOG_CRDT_DATA_DIR=' + meson.project_source_root() / 'bench/data/crdt',
     ])

# Issue #948: the authoritative stable CI job uses two build directories
# because log_perf_gate measures the trace-enabled logger while the evaluator
# timing gates are measured with inner-loop log sites stripped at ERROR. Its
# scripts/ci/check-perf-gate-execution.sh check consumes both Meson logs and
# rejects missing, skipped, or correctness-only test records.
#
# Issue #947: the result-count sentinel inside the gate above never executed,
# because the perf-host skips (WIRELOG_PERF_GATE, log ceiling, cpufreq
# governor) are evaluated before it.  Output validation is deterministic and
# needs none of them, so it gets its own registrations.
#
# The default-suite entry uses the closure-derived subset
# (scripts/perf/make_crdt_subset.py); the full fixture takes ~23s, which is
# ~8x worse in the -O0 sanitizer legs and far too slow to run on every
# `meson test`.  The full-fixture entry stays in suite 'perf' but no longer
# requires a pinned governor or a stripped log ceiling, so the nightly and
# the perf PR job -- which set WIRELOG_PERF_GATE=1 and previously produced
# nothing but exit 77 -- now actually validate 104,851.
test('crdt_correctness_small', test_crdt_perf_gate_exe,
     timeout: 120,
     env: [
       'WIRELOG_GATE_CORRECTNESS_ONLY=1',
       'WIRELOG_CRDT_SMALL=1',
       'WIRELOG_CRDT_DATA_DIR=' + meson.project_source_root() / 'bench/data/crdt-small',
     ])

test('crdt_correctness_full', test_crdt_perf_gate_exe,
     suite: 'perf',
     timeout: 600,
     env: [
       'WIRELOG_GATE_CORRECTNESS_ONLY=1',
       'WIRELOG_CRDT_DATA_DIR=' + meson.project_source_root() / 'bench/data/crdt',
     ])

# ============================================================================
# CSPA W=1 perf-suite gate.
#
# Uses the same static-cspa-fast inline-fact assembly as
# `bench_flowlog --workload cspa-fast` and asserts median wall <= TARGET_MS
# (currently 2,050 ms = baseline 1,950 ms x 1.05).  Registered in suite 'perf'
# so a default invocation skips it at runtime via WIRELOG_PERF_GATE.
# ============================================================================

test_cspa_perf_gate_sources = files('test_cspa_perf_gate.c') + parser_src + ir_src + optimizer_src + backend_src + io_src + arena_src + thread_src + workqueue_src

test_cspa_perf_gate_exe = executable(
  'test_cspa_perf_gate',
  test_cspa_perf_gate_sources,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)
test_cspa_correctness_exe = executable(
  'test_cspa_correctness',
  test_cspa_perf_gate_sources,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
  override_options: ['optimization=3'],
)
test('cspa_w1_gate', test_cspa_perf_gate_exe,
     suite: 'perf',
     timeout: 180,
     env: [
       'WIRELOG_CSPA_DATA_DIR=' + meson.project_source_root() / 'bench/data/cspa',
     ])

# Issue #947: gold tuple and iteration counts, without a perf host.  Measured
# at ~1.35s release on the dev host, so it is affordable in the default suite
# even at the sanitizer legs' ~8x.
test('cspa_correctness', test_cspa_correctness_exe,
     timeout: 300,
     env: [
       'WIRELOG_GATE_CORRECTNESS_ONLY=1',
       'WIRELOG_CSPA_DATA_DIR=' + meson.project_source_root() / 'bench/data/cspa',
     ])

# DOOP W=8 perf gate (Issue #748).  The runner delegates execution to the
# already-built benchmark driver so its 35-file catalogue and rules remain a
# single source of truth.  It is intentionally opt-in: the zxing fixture is
# large and the gate is for a provisioned performance runner.
#
# Registered through the shared `sh` handle above rather than find_program()
# on the script itself.  Meson rewrites a `#!/usr/bin/env bash` shebang into a
# bare, unresolved `bash` argv[0] on Windows, and CreateProcess resolves a bare
# name from the system directory before PATH -- so it lands on
# System32\bash.exe, the WSL launcher, which exits 1 before the script can
# reach its own non-Linux skip.  The handle is resolved once at configure time
# instead, and on the CI images that resolves to Git for Windows' native MSYS2
# bash; no WSL is involved.  The script then self-skips on every non-Linux
# host, which is what keeps the Windows leg of perf-nightly.yml green when it
# runs this suite with WIRELOG_PERF_GATE=1.
if sh.found()
  test('doop_w8_gate', sh,
       args: [meson.project_source_root() / 'scripts/ci/run-doop-perf-gate.sh'],
       suite: 'perf',
       timeout: 3600,
       env: [
         'WIRELOG_DOOP_DATA_DIR=' + meson.project_source_root() / 'bench/data/doop',
         'WIRELOG_DOOP_BENCH_BIN=' + meson.project_build_root() / 'bench/bench_flowlog',
         'WIRELOG_PERF_LOG_COMPILE_MAX_LEVEL=' +
           _log_level_map[_log_opt].to_string(),
       ],
       depends: bench_flowlog)
endif

# ============================================================================
# Sub-ms graph W=1 perf-suite gate.
#
# Batches small graph workloads so the suite can enforce timing stability
# without yet setting absolute median regression targets.
# ============================================================================

test_sub_ms_graph_perf_gate_exe = executable(
  'test_sub_ms_graph_perf_gate',
  files('test_sub_ms_graph_perf_gate.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)
test('sub_ms_graph_perf_gate', test_sub_ms_graph_perf_gate_exe,
     suite: 'perf',
     timeout: 180,
     env: [
       'WIRELOG_GRAPH_WORKLOAD_DIR=' + meson.project_source_root() / 'bench/workloads',
       'WIRELOG_GRAPH_DATA_DIR=' + meson.project_source_root() / 'bench/data',
     ])

# Issue #947: gold tuple/iteration/per-relation counts, without a perf host.
test('sub_ms_graph_correctness', test_sub_ms_graph_perf_gate_exe,
     timeout: 120,
     env: [
       'WIRELOG_GATE_CORRECTNESS_ONLY=1',
       'WIRELOG_GRAPH_WORKLOAD_DIR=' + meson.project_source_root() / 'bench/workloads',
       'WIRELOG_GRAPH_DATA_DIR=' + meson.project_source_root() / 'bench/data',
     ])

# ============================================================================
# Consolidation Function Tests (US-003 TDD RED Phase)
# ============================================================================

test_consolidate_incremental_delta_exe = executable(
  'test_consolidate_incremental_delta',
  files('test_consolidate_incremental_delta.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('consolidate_incremental_delta', test_consolidate_incremental_delta_exe)

# ============================================================================
# CSE Cache Hit Rate Tests (US-001 TDD)
# ============================================================================

test_cse_cache_hit_rate_exe = executable(
  'test_cse_cache_hit_rate',
  files('test_cse_cache_hit_rate.c'),
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('cse_cache_hit_rate', test_cse_cache_hit_rate_exe)

# ============================================================================
# K-way Merge CONSOLIDATE Tests (US-002 TDD RED Phase)
# ============================================================================

test_consolidate_kway_merge_exe = executable(
  'test_consolidate_kway_merge',
  files('test_consolidate_kway_merge.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('consolidate_kway_merge', test_consolidate_kway_merge_exe)

# ============================================================================
# SIMD Row Comparison Tests (Issue #197)
# ============================================================================

test_simd_row_cmp_exe = executable(
  'test_simd_row_cmp',
  files('test_simd_row_cmp.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('simd_row_cmp', test_simd_row_cmp_exe)

# ============================================================================
# K-Fusion Merge Function Tests
# ============================================================================

test_k_fusion_merge_exe = executable(
  'test_k_fusion_merge',
  files('test_k_fusion_merge.c'),
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('k_fusion_merge', test_k_fusion_merge_exe)

# ============================================================================
# K-Fusion Dispatch Integration Tests
# ============================================================================

test_k_fusion_dispatch_exe = executable(
  'test_k_fusion_dispatch',
  files('test_k_fusion_dispatch.c'),
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('k_fusion_dispatch', test_k_fusion_dispatch_exe)

# ============================================================================
# R9 Self-Join Delta Permutation Tests
# ============================================================================

test_r9_self_join_delta_exe = executable(
  'test_r9_self_join_delta',
  files('test_r9_self_join_delta.c'),
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('r9_self_join_delta', test_r9_self_join_delta_exe)

# ============================================================================
# Empty-Delta Skip Tests (US-006/US-007)
# ============================================================================

test_empty_delta_skip_exe = executable(
  'test_empty_delta_skip',
  files('test_empty_delta_skip.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('empty_delta_skip', test_empty_delta_skip_exe)

# ============================================================================
# Delta Factoring E2E Tests (Issue #85)
# ============================================================================

test_delta_factoring_e2e_exe = executable(
  'test_delta_factoring_e2e',
  files('test_delta_factoring_e2e.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('delta_factoring_e2e', test_delta_factoring_e2e_exe)

# ============================================================================
# K-Fusion End-to-End Integration Tests (Phase 3A)
# ============================================================================

test_k_fusion_e2e_exe = executable(
  'test_k_fusion_e2e',
  files('test_k_fusion_e2e.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('k_fusion_e2e', test_k_fusion_e2e_exe)

test_k_fusion_adaptive_exe = executable(
  'test_k_fusion_adaptive',
  files('test_k_fusion_adaptive.c'),
  files('../wirelog/columnar/kfusion_adaptive.c'),
  include_directories: [wirelog_inc, wirelog_src_inc],
)
test('k_fusion_adaptive', test_k_fusion_adaptive_exe)

# ============================================================================
# K-Fusion Memory Isolation Tests (Issue #196)
# ============================================================================

test_k_fusion_memory_exe = executable(
  'test_k_fusion_memory',
  files('test_k_fusion_memory.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('k_fusion_memory', test_k_fusion_memory_exe)

test_k_fusion_memory_nofusion_exe = executable(
  'test_k_fusion_memory_nofusion',
  files('test_k_fusion_memory.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  c_args: ['-DENABLE_K_FUSION=0'],
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('k_fusion_memory_nofusion', test_k_fusion_memory_nofusion_exe)

# ============================================================================
# Delta Timestamp Tests (Phase 3B - 3B-001)
# ============================================================================

test_delta_timestamp_exe = executable(
  'test_delta_timestamp',
  files('test_delta_timestamp.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('delta_timestamp', test_delta_timestamp_exe)

# ============================================================================
# Arrangement Layer Tests (Phase 3C - 3C-001)
# ============================================================================

test_arrangement_exe = executable(
  'test_arrangement',
  files('test_arrangement.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('arrangement', test_arrangement_exe)

# ============================================================================
# Arrangement next-pow2 Overflow Tests (Issue #1074)
# ============================================================================

test_arrangement_pow2_overflow_exe = executable(
  'test_arrangement_pow2_overflow',
  files('test_arrangement_pow2_overflow.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('arrangement_pow2_overflow', test_arrangement_pow2_overflow_exe)

# ============================================================================
# Delta Arrangement Cache Tests (Phase 3C - 3C-001-Ext)
# ============================================================================

test_delta_arrangement_exe = executable(
  'test_delta_arrangement',
  files('test_delta_arrangement.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('delta_arrangement', test_delta_arrangement_exe)

# ============================================================================
# Join Arrangement Integration Tests (Phase 3C - 3C-002)
# ============================================================================

test_join_arrangement_exe = executable(
  'test_join_arrangement',
  files('test_join_arrangement.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('join_arrangement', test_join_arrangement_exe)

# ============================================================================
# Leapfrog Triejoin Tests (Issue #194)
# ============================================================================

test_lftj_exe = executable(
  'test_lftj',
  files('test_lftj.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('lftj', test_lftj_exe)

# ============================================================================
# Arrangement Cache Reuse Tests (Phase 3C - cache reuse across rules)
# ============================================================================

test_arrangement_cache_reuse_exe = executable(
  'test_arrangement_cache_reuse',
  files('test_arrangement_cache_reuse.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('arrangement_cache_reuse', test_arrangement_cache_reuse_exe)

# ============================================================================
# Arrangement LRU Eviction Tests (Issue #216)
# ============================================================================

test_arrangement_lru_eviction_exe = executable(
  'test_arrangement_lru_eviction',
  files('test_arrangement_lru_eviction.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('arrangement_lru_eviction', test_arrangement_lru_eviction_exe)

# ============================================================================
# Filtered Relation Cache Tests (Issue #386)
# ============================================================================

test_filt_cache_exe = executable(
  'test_filt_cache',
  files('test_filt_cache.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('filt_cache', test_filt_cache_exe)

# ============================================================================
# Frontier Tracking Tests (Phase 3B)
# ============================================================================

test_frontier_exe = executable(
  'test_frontier',
  files('test_frontier.c'),
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('frontier', test_frontier_exe)

# ============================================================================
# Frontier Vtable Tests (Issue #261)
# ============================================================================

test_frontier_vtable_exe = executable(
  'test_frontier_vtable',
  files('test_frontier_vtable.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('frontier_vtable', test_frontier_vtable_exe)

# ============================================================================
# Differential Frontier Vtable Tests (Issue #262)
# ============================================================================

test_diff_vtable_exe = executable(
  'test_diff_vtable',
  files('test_diff_vtable.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('diff_vtable', test_diff_vtable_exe)

# ============================================================================
# Frontier Integration Tests (Phase 3B-005)
# ============================================================================

test_frontier_integration_exe = executable(
  'test_frontier_integration',
  files('test_frontier_integration.c'),
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('frontier_integration', test_frontier_integration_exe)

# ============================================================================
# Frontier Filtering Tests (Phase 3D - 3D-001)
# ============================================================================

test_frontier_filtering_exe = executable(
  'test_frontier_filtering',
  files('test_frontier_filtering.c'),
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('frontier_filtering', test_frontier_filtering_exe)

# ============================================================================
# Frontier Skip Tests (Phase 3D-Ext - 3D-Ext-001)
# ============================================================================

test_frontier_skip_exe = executable(
  'test_frontier_skip',
  files('test_frontier_skip.c'),
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('frontier_skip', test_frontier_skip_exe)

# ============================================================================
# Frontier Skip Integration Tests (Phase 3D-Ext-FU - 3D-Ext-FU-002)
# ============================================================================

test_frontier_skip_integration_exe = executable(
  'test_frontier_skip_integration',
  files('test_frontier_skip_integration.c'),
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('frontier_skip_integration', test_frontier_skip_integration_exe)

# ============================================================================
# Phase 4: Per-Stratum Frontier Array Tests (US-4-001)
# ============================================================================

test_phase4_frontier_array_exe = executable(
  'test_phase4_frontier_array',
  files('test_phase4_frontier_array.c'),
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('phase4_frontier_array', test_phase4_frontier_array_exe)

# ============================================================================
# Phase 4: Incremental Fact Insertion Tests (US-4-002)
# ============================================================================

test_incremental_insertion_exe = executable(
  'incremental_insertion',
  files('test_incremental_insertion.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('incremental_insertion', test_incremental_insertion_exe)

bench_incremental_frontier_exe = executable(
  'bench_incremental_frontier',
  files('bench_incremental_frontier.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)


# ============================================================================
# Phase 4: Affected Strata Detection Tests (US-4-affected_strata)
# ============================================================================

test_affected_strata_exe = executable(
  'test_affected_strata',
  files('test_affected_strata.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('affected_strata', test_affected_strata_exe)

# ============================================================================
# Phase 4: Affected Rule Detection Tests (US-4-003)
# ============================================================================

test_affected_rules_exe = executable(
  'test_affected_rules',
  files('test_affected_rules.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('affected_rules', test_affected_rules_exe)

# ============================================================================
# Issue #1140: Base-Case Skip NULL Template
# ============================================================================

test_base_skip_null_template_exe = executable(
  'test_base_skip_null_template',
  files('test_base_skip_null_template.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('base_skip_null_template', test_base_skip_null_template_exe)

# ============================================================================
# Phase 4: Per-Stratum Frontier Integration Tests (US-4-005)
# ============================================================================

test_phase4_frontier_integration_exe = executable(
  'test_phase4_frontier_integration',
  files('test_phase4_frontier_integration.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('phase4_frontier_integration', test_phase4_frontier_integration_exe)

# ============================================================================
# Rule-Level Frontier Integration Tests (Phase 4, US-4-006)
# ============================================================================

test_rule_level_frontier_exe = executable(
  'test_rule_level_frontier',
  files('test_rule_level_frontier.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('rule_level_frontier', test_rule_level_frontier_exe)

# ============================================================================
# Mobius COUNT Signed Multiplicity Tests (US-3B-002 TDD RED Phase)
# ============================================================================

test_mobius_count_signed_exe = executable(
  'test_mobius_count_signed',
  files('test_mobius_count_signed.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('mobius_count_signed', test_mobius_count_signed_exe)

# ============================================================================
# Mobius JOIN Weighted Multiplicity Tests (US-3B-004 TDD RED Phase)
# ============================================================================

test_mobius_join_weighted_exe = executable(
  'test_mobius_join_weighted',
  files('test_mobius_join_weighted.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('mobius_join_weighted', test_mobius_join_weighted_exe)

# ============================================================================
# Mobius Delta Formula Tests (US-3B-006 TDD RED Phase)
# ============================================================================

test_mobius_delta_formula_exe = executable(
  'test_mobius_delta_formula',
  files('test_mobius_delta_formula.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('mobius_delta_formula', test_mobius_delta_formula_exe)

# ============================================================================
# Phase 3B End-to-End Integration Tests (US-3B-008)
# ============================================================================

test_phase3b_integration_exe = executable(
  'test_phase3b_integration',
  files('test_phase3b_integration.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('phase3b_integration', test_phase3b_integration_exe)

# ============================================================================
# Phase 3C JOIN Integration Tests (US-3C-001 TDD RED)
# ============================================================================

test_phase3c_join_integration_exe = executable(
  'test_phase3c_join_integration',
  files('test_phase3c_join_integration.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('phase3c_join_integration', test_phase3c_join_integration_exe)

# ============================================================================
# Phase 3C REDUCE Integration Tests (US-3C-003 TDD)
# ============================================================================
#
# Tests 1 and 2 (COUNT with multiplicities) call col_op_reduce_weighted directly
# and pass.  Test 3 (SUM weighted) declares col_op_reduce_weighted_sum which is
# not yet implemented; the executable is expected to fail at link time until the
# function is added to columnar_nanoarrow.c.

test_phase3c_reduce_integration_exe = executable(
  'test_phase3c_reduce_integration',
  files('test_phase3c_reduce_integration.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('phase3c_reduce_integration', test_phase3c_reduce_integration_exe)

# ============================================================================
# Phase 3C Full Integration Tests (US-3C-005)
# ============================================================================

test_phase3c_full_integration_exe = executable(
  'test_phase3c_full_integration',
  files('test_phase3c_full_integration.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('phase3c_full_integration', test_phase3c_full_integration_exe)

# ============================================================================
# Phase 3D Frontier Skip Tests (US-3D-001 TDD)
# ============================================================================

test_frontier_skip_mobius_exe = executable(
  'test_frontier_skip_mobius',
  files('test_frontier_skip_mobius.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('frontier_skip_mobius', test_frontier_skip_mobius_exe)

# ============================================================================
# Phase 3D Frontier Skip Integration Tests (US-3D-003)
# ============================================================================

test_phase3d_frontier_skip_integration_exe = executable(
  'test_phase3d_frontier_skip_integration',
  files('test_phase3d_frontier_skip_integration.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('phase3d_frontier_skip_integration', test_phase3d_frontier_skip_integration_exe)

# ============================================================================
# Consolidation CoW Tests (Issue #94)
# ============================================================================

test_consolidation_cow_exe = executable(
  'test_consolidation_cow',
  files('test_consolidation_cow.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('consolidation_cow', test_consolidation_cow_exe)

# ============================================================================
# Arrangement Incremental Invalidation Tests (Issue #92)
# ============================================================================

test_arrangement_incr_invalidation_exe = executable(
  'test_arrangement_incremental_invalidation',
  files('test_arrangement_incremental_invalidation.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('arrangement_incr_invalidation', test_arrangement_incr_invalidation_exe)

# ============================================================================
# Delta Propagation Tests (Issue #83)
# ============================================================================

test_delta_propagation_exe = executable(
  'test_delta_propagation',
  files('test_delta_propagation.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('delta_propagation', test_delta_propagation_exe)

# Issue #914: non-recursive stratum after a recursive one
test_issue914_nonrec_exe = executable(
  'test_issue914_nonrec_after_recursive',
  files('test_issue914_nonrec_after_recursive.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('issue914_nonrec_after_recursive', test_issue914_nonrec_exe)

# Issue #105: Monotone stratum detection
test_monotone_detection_exe = executable(
  'test_monotone_detection',
  files('test_monotone_detection.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('monotone_detection', test_monotone_detection_exe)

# Issue #99: Multi-worker task dispatch
test_multi_worker_exe = executable(
  'test_multi_worker',
  files('test_multi_worker.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('multi_worker', test_multi_worker_exe)

# Issue #103: 2D Frontier Epoch Tracking Tests
test_2d_frontier_init_exe = executable(
  'test_2d_frontier_init',
  files('test_2d_frontier_init.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('2d_frontier_init', test_2d_frontier_init_exe)

# Issue #104: 2D Frontier Skip Condition Tests
test_2d_frontier_skip_exe = executable(
  'test_2d_frontier_skip',
  files('test_2d_frontier_skip.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('2d_frontier_skip', test_2d_frontier_skip_exe)

test_selective_frontier_reset_exe = executable(
  'test_selective_frontier_reset',
  files('test_selective_frontier_reset.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('selective_frontier_reset', test_selective_frontier_reset_exe)

# Issue #106: Multi-Stratum Rule Frontier Tracking Tests
test_multi_stratum_rule_frontier_exe = executable(
  'test_multi_stratum_rule_frontier',
  files('test_multi_stratum_rule_frontier.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('multi_stratum_rule_frontier', test_multi_stratum_rule_frontier_exe)

# ============================================================================
# CLI Driver Tests (Issue #137 - .output filename parameter support)
# ============================================================================

driver_src = files(
  '../wirelog/cli/driver.c',
  '../wirelog/api_facade.c',
)

test_cli_exe = executable(
  'test_cli',
  files('test_cli.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  driver_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('cli', test_cli_exe)

test_cli_watch_exe = executable(
  'test_cli_watch',
  files('test_cli_watch.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  driver_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('cli_watch', test_cli_watch_exe)

# ============================================================================
# Affected Strata Across Plan Rewrites (Issue #1019)
# ============================================================================

test_affected_strata_rewrites_exe = executable(
  'test_affected_strata_rewrites',
  files('test_affected_strata_rewrites.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  driver_src,
  '../wirelog/wirelog-easy.c',
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

# Issue #1019: allocation-heavy -- ten plan builds plus four end-to-end
# incremental runs at W in {1,2,4,8}, each stepping the session once per
# inserted edge.  Measured under `meson test` on the dev host at 1.67s
# (release) and 0.33s (address,undefined); the release figure is dominated by
# MALLOC_PERTURB_, which meson sets for every test and which makes glibc
# scrub every one of those allocations.  That leaves little margin on a
# loaded or slower CI runner, so take the same explicit override the other
# allocation-heavy cases here use rather than riding the 30s default.
test('affected_strata_rewrites', test_affected_strata_rewrites_exe,
     timeout: 120)

# ============================================================================
# Mixed insert/remove affected-strata mask (Issue #1031)
# ============================================================================

test_mixed_insert_remove_mask_exe = executable(
  'test_mixed_insert_remove_mask',
  files('test_mixed_insert_remove_mask.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  '../wirelog/wirelog-easy.c',
  '../wirelog/api_facade.c',
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('mixed_insert_remove_mask', test_mixed_insert_remove_mask_exe)

test_cli_delta_exe = executable(
  'test_cli_delta',
  files('test_cli_delta.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  driver_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('cli_delta', test_cli_delta_exe)

# ============================================================================
# Bitwise Operator Tests (Issue #72)
# ============================================================================

test_bitwise_types_exe = executable(
  'test_bitwise_types',
  files('test_bitwise_types.c'),
  parser_src,
  ir_src,
  io_src,
  thread_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep],
)

test('bitwise_types', test_bitwise_types_exe)

test_bitwise_parser_exe = executable(
  'test_bitwise_parser',
  files('test_bitwise_parser.c'),
  parser_src,
  ir_src,
  io_src,
  thread_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep],
)

test('bitwise_parser', test_bitwise_parser_exe)

test_bitwise_eval_exe = executable(
  'test_bitwise_eval',
  files('test_bitwise_eval.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('bitwise_eval', test_bitwise_eval_exe)

test_bitwise_integration_exe = executable(
  'test_bitwise_integration',
  files('test_bitwise_integration.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('bitwise_integration', test_bitwise_integration_exe)

# ============================================================================
# Hash Function Tests (Issue #144)
# ============================================================================

test_hash_types_exe = executable(
  'test_hash_types',
  files('test_hash_types.c'),
  parser_src,
  ir_src,
  io_src,
  thread_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep],
)

test('hash_types', test_hash_types_exe)

test_hash_parser_exe = executable(
  'test_hash_parser',
  files('test_hash_parser.c'),
  parser_src,
  ir_src,
  io_src,
  thread_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep],
)

test('hash_parser', test_hash_parser_exe)

# CRC-32 parser tests (Issue #884): verify the lexer/parser accept
# crc32_ethernet(...) and crc32_castagnoli(...) as built-in keywords.
test_crc32_parser_exe = executable(
  'test_crc32_parser',
  files('test_crc32_parser.c'),
  parser_src,
  ir_src,
  io_src,
  thread_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep],
)

test('crc32_parser', test_crc32_parser_exe)

test_hash_eval_exe = executable(
  'test_hash_eval',
  files('test_hash_eval.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('hash_eval', test_hash_eval_exe)

test_hash_integration_exe = executable(
  'test_hash_integration',
  files('test_hash_integration.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('hash_integration', test_hash_integration_exe)

# ============================================================================
# Cryptographic Hash Function Tests (Issue #73)
# ============================================================================
#
# Tests for md5(), sha1(), sha256(), sha512(), hmac_sha256(), uuid4(), and
# uuid5() built-in functions.  When mbedTLS is enabled
# (-DWL_MBEDTLS_ENABLED=1), runs determinism, sensitivity, UUID, and
# integration tests.  When disabled, runs fallback tests verifying that each
# function fails gracefully at runtime.

test_cryptographic_hashes_exe = executable(
  'test_cryptographic_hashes',
  files('test_cryptographic_hashes.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('cryptographic_hashes', test_cryptographic_hashes_exe, suite: 'crypto')

# ============================================================================
# CRC-32 Tests (Issue #145)
# ============================================================================

crc32_src = files(
  '../wirelog/crc32.c',
)

# Architecture-specific flags to enable hardware CRC32 instructions.
# Add GNU-style target feature flags only for compilers using GCC/Clang
# argument syntax. MSVC-syntax builds, including clang-cl, rely on
# crc32.c's compiler-specific paths and portable fallback.
crc32_arch_c_args = []
if cc.get_argument_syntax() != 'msvc'
  if host_machine.cpu_family() == 'x86_64'
    crc32_arch_c_args = ['-msse4.2']
  elif host_machine.cpu_family() == 'aarch64'
    crc32_arch_c_args = ['-march=armv8-a+crc']
  endif
endif

test_crc32_windows_fallback_exe = executable(
  'test_crc32_windows_fallback',
  files('test_crc32_windows_fallback.c'),
  crc32_src,
  c_args: crc32_arch_c_args,
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('crc32_windows_fallback', test_crc32_windows_fallback_exe)

# CRC-32 tests (Issue #145)
# These broader legacy CRC tests are still skipped under MSVC, but the
# smoke test above keeps known-vector coverage on MSVC/clang-cl paths.
if cc.get_id() != 'msvc'
  test_crc32_ethernet_exe = executable(
    'test_crc32_ethernet',
    files('test_crc32_ethernet.c'),
    crc32_src,
    c_args: crc32_arch_c_args,
    include_directories: [wirelog_inc, wirelog_src_inc],
  )

  test('crc32_ethernet', test_crc32_ethernet_exe)

  test_crc32_castagnoli_exe = executable(
    'test_crc32_castagnoli',
    files('test_crc32_castagnoli.c'),
    crc32_src,
    c_args: crc32_arch_c_args,
    include_directories: [wirelog_inc, wirelog_src_inc],
  )

  test('crc32_castagnoli', test_crc32_castagnoli_exe)

  # CRC-32 hardware/software equivalence test (Issue #145, US-145-004)
  # Reuses crc32_arch_c_args defined above for the same architecture flags.
  test_crc32_hw_equivalence_exe = executable(
    'test_crc32_hw_equivalence',
    files('test_crc32_hw_equivalence.c'),
    crc32_src,
    c_args: crc32_arch_c_args,
    include_directories: [wirelog_inc, wirelog_src_inc],
  )

  test('crc32_hw_equivalence', test_crc32_hw_equivalence_exe)

  # CRC-32 evaluator tests (Issue #884): drive the columnar evaluator end
  # to end and verify it matches the engine functions in wirelog/crc32.c.
  # Reuses crc32_arch_c_args / crc32_src defined above.
  test_crc32_eval_exe = executable(
    'test_crc32_eval',
    files('test_crc32_eval.c'),
    parser_src,
    ir_src,
    optimizer_src,
    backend_src,
    io_src,
    arena_src,
    thread_src,
    workqueue_src,
    crc32_src,
    c_args: crc32_arch_c_args,
    include_directories: [wirelog_inc, wirelog_src_inc],
    dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
  )

  test('crc32_eval', test_crc32_eval_exe)
endif

# ============================================================================
# Delta Callback Tests (Issue #153 - Re-enable session tests)
# ============================================================================

test_delta_callback_exe = executable(
  'test_delta_callback',
  files('test_delta_callback.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  csv_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('delta_callback', test_delta_callback_exe)

test_side_compound_delta_exe = executable(
  'test_side_compound_delta',
  files('test_side_compound_delta.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  csv_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('side_compound_delta', test_side_compound_delta_exe)

# ============================================================================
# Delta Retraction Tests (Issue #158)
# ============================================================================

test_delta_retraction_exe = executable(
  'test_delta_retraction',
  files('test_delta_retraction.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  csv_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('delta_retraction', test_delta_retraction_exe)

# ============================================================================
# Session Tests (Issue #153 - Persistent columnar session deltas)
# ============================================================================

test_session_exe = executable(
  'test_session',
  files('test_session.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('session', test_session_exe)

# ============================================================================
# Rotation Strategy Vtable (Issue #600)
# ============================================================================

test_rotation_strategy_exe = executable(
  'test_rotation_strategy',
  files('test_rotation_strategy.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('rotation_strategy', test_rotation_strategy_exe)

# ============================================================================
# Session Compound Arena Lifecycle (Issue #559) - Phase 0 gateway
# ============================================================================

test_session_compound_arena_lifecycle_exe = executable(
  'test_session_compound_arena_lifecycle',
  files('test_session_compound_arena_lifecycle.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('session_compound_arena_lifecycle',
     test_session_compound_arena_lifecycle_exe)

# ============================================================================
# GC Epoch Boundary Integration (Issue #560)
# ============================================================================

test_gc_epoch_boundary_exe = executable(
  'test_gc_epoch_boundary',
  files('test_gc_epoch_boundary.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('gc_epoch_boundary', test_gc_epoch_boundary_exe)

# ============================================================================
# GC Freeze Guard Unit Tests (Issue #561)
# ============================================================================

test_gc_freeze_guard_exe = executable(
  'test_gc_freeze_guard',
  files(
    'test_gc_freeze_guard.c',
    '../wirelog/util/log.c',
    '../wirelog/util/log_emit.c',
  ),
  arena_src,
  thread_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('gc_freeze_guard', test_gc_freeze_guard_exe)

# ============================================================================
# Recursive Aggregation Plan Tests - DISABLED in Phase 2C
# ============================================================================
#
# test_recursive_agg_plan depends on deleted DD backend (dd_plan.h)
# This test is disabled until columnar backend provides equivalent plan generation

# ============================================================================
# Magic Sets Optimization Pass Tests (Issue #206)
# ============================================================================

test_magic_sets_exe = executable(
  'test_magic_sets',
  files('test_magic_sets.c'),
  parser_src,
  ir_src,
  optimizer_src,
  # Issue #989: the guard-order tests evaluate magic-rewritten programs, so
  # this target needs the plan generator's backend (columnar) and its arena
  # and work-queue dependencies, not just the IR passes.
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('magic_sets', test_magic_sets_exe)

# ============================================================================
# LFTJ Integration Tests (Issue #195)
# ============================================================================

test_lftj_integration_exe = executable(
  'test_lftj_integration',
  files('test_lftj_integration.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('lftj_integration', test_lftj_integration_exe)

# ============================================================================
# SIMD Filter Fast-Path Tests (Issue #214)
# ============================================================================

test_simd_filter_exe = executable(
  'test_simd_filter',
  files('test_simd_filter.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('simd_filter', test_simd_filter_exe)

# ============================================================================
# Relation Compaction Tests (Issue #217)
# ============================================================================

test_compaction_exe = executable(
  'test_compaction',
  files('test_compaction.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('compaction', test_compaction_exe)

# ============================================================================
# Dynamic Join Output Limit Tests (Issue #221)
# ============================================================================

test_join_limit_exe = executable(
  'test_join_limit',
  files('test_join_limit.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('join_limit', test_join_limit_exe)

test_join_overflow_exe = executable(
  'test_join_overflow',
  files('test_join_overflow.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('join_overflow', test_join_overflow_exe)

# ============================================================================
# WL_MAX_WORKERS Clamping Tests (Issue #398)
# ============================================================================

test_max_workers_exe = executable(
  'test_max_workers',
  files('test_max_workers.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('max_workers', test_max_workers_exe)

# ============================================================================
# SIMD JOIN Hash and Key-Match Tests (Issue #231)
# ============================================================================

test_simd_join_exe = executable(
  'test_simd_join',
  files('test_simd_join.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('simd_join', test_simd_join_exe)

# ============================================================================
# ARM NEON SIMD Hash and Key-Match Tests
# ============================================================================

test_simd_neon_exe = executable(
  'test_simd_neon',
  files('test_simd_neon.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('simd_neon', test_simd_neon_exe)

# ============================================================================
# Differential Trace & Arrangement Tests (Issue #259)
# ============================================================================

test_diff_trace_exe = executable(
  'test_diff_trace',
  files('test_diff_trace.c'),
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('diff_trace', test_diff_trace_exe)

test_diff_arrangement_exe = executable(
  'test_diff_arrangement',
  files('test_diff_arrangement.c'),
  backend_src,
  parser_src,
  ir_src,
  optimizer_src,
  arena_src,
  io_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('diff_arrangement', test_diff_arrangement_exe)

test_diff_arrangement_deep_copy_buckets_exe = executable(
  'test_diff_arrangement_deep_copy_buckets',
  files('test_diff_arrangement_deep_copy_buckets.c'),
  '../wirelog/columnar/diff_arrangement.c',
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('diff_arrangement_deep_copy_buckets', test_diff_arrangement_deep_copy_buckets_exe)

# ============================================================================
# Multi-Worker K-Fusion Validation Tests (Issue #260)
# ============================================================================

test_diff_multiworker_exe = executable(
  'test_diff_multiworker',
  files('test_diff_multiworker.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('diff_multiworker', test_diff_multiworker_exe)

# ============================================================================
# Differential Bridge Tests (Issue #262)
# ============================================================================

test_diff_bridge_exe = executable(
  'test_diff_bridge',
  files('test_diff_bridge.c'),
  backend_src,
  parser_src,
  ir_src,
  optimizer_src,
  arena_src,
  io_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('diff_bridge', test_diff_bridge_exe)

# ============================================================================
# Differential Join Operator Tests (Issue #263)
# ============================================================================

test_diff_join_exe = executable(
  'test_diff_join',
  files('test_diff_join.c'),
  backend_src,
  parser_src,
  ir_src,
  optimizer_src,
  arena_src,
  io_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('diff_join', test_diff_join_exe)

# ============================================================================
# Differential Consolidate Operator Tests (Issue #263)
# ============================================================================

test_diff_consolidate_exe = executable(
  'test_diff_consolidate',
  files('test_diff_consolidate.c'),
  backend_src,
  parser_src,
  ir_src,
  optimizer_src,
  arena_src,
  io_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('diff_consolidate', test_diff_consolidate_exe)

# ============================================================================
# Differential Integration Tests (Issue #264)
# ============================================================================

test_diff_integration_exe = executable(
  'test_diff_integration',
  files('test_diff_integration.c'),
  backend_src,
  parser_src,
  ir_src,
  optimizer_src,
  arena_src,
  io_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('diff_integration', test_diff_integration_exe, timeout: 120)

test_diff_recursive_arrangement_exe = executable(
  'test_diff_recursive_arrangement',
  files('test_diff_recursive_arrangement.c'),
  backend_src,
  parser_src,
  ir_src,
  optimizer_src,
  arena_src,
  io_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('diff_recursive_arrangement', test_diff_recursive_arrangement_exe)

# ============================================================================
# Pre-compiled Expression Evaluator Tests (Issue #298)
# ============================================================================

test_expr_compile_exe = executable(
  'test_expr_compile',
  files('test_expr_compile.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('expr_compile', test_expr_compile_exe)

test_arithmetic_overflow_exe = executable(
  'test_arithmetic_overflow',
  files('test_arithmetic_overflow.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('arithmetic_overflow', test_arithmetic_overflow_exe)

# ============================================================================
# Pointer Swap Tests (Issue #300)
# ============================================================================

test_pointer_swap_exe = executable(
  'test_pointer_swap',
  files('test_pointer_swap.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('pointer_swap', test_pointer_swap_exe)

# ============================================================================
# Malloc Optimization Tests (Arena Allocation)
# ============================================================================

test_malloc_optimization_exe = executable(
  'test_malloc_optimization',
  files('test_malloc_optimization.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('malloc_optimization', test_malloc_optimization_exe)

# ============================================================================
# Radix Sort Tests (Issue #308)
# ============================================================================

test_radix_sort_exe = executable(
  'test_radix_sort',
  files('test_radix_sort.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('radix_sort', test_radix_sort_exe)

# ============================================================================
# Hash-Partitioned Relation Storage (Issue #314)
# ============================================================================

test_partition_exe = executable(
  'test_partition',
  files('test_partition.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('partition', test_partition_exe)

# ============================================================================
# col_rel_t compound metadata fields (Issue #532 Task 1)
# ============================================================================

test_col_rel_compound_schema_exe = executable(
  'test_col_rel_compound_schema',
  files('test_col_rel_compound_schema.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('col_rel_compound_schema', test_col_rel_compound_schema_exe)

# ============================================================================
# col_rel_deep_copy (Issue #553)
# ============================================================================

test_col_rel_deep_copy_exe = executable(
  'test_col_rel_deep_copy',
  files('test_col_rel_deep_copy.c', 'col_rel_deep_copy_fixture.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('col_rel_deep_copy', test_col_rel_deep_copy_exe)

# ============================================================================
# Inline compound store/retrieve/retract ops (Issue #532 Task 3)
# ============================================================================

test_col_rel_inline_storage_exe = executable(
  'test_col_rel_inline_storage',
  files('test_col_rel_inline_storage.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('col_rel_inline_storage', test_col_rel_inline_storage_exe)

# ============================================================================
# Compound logging + error observability (Issue #532 Task 6)
# ============================================================================

test_compound_logging_exe = executable(
  'test_compound_logging',
  files('test_compound_logging.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('compound_logging', test_compound_logging_exe, suite: 'log')

# ============================================================================
# Arena observability (Issue #558)
# ============================================================================

test_arena_observability_exe = executable(
  'test_arena_observability',
  files('test_arena_observability.c'),
  arena_src,
  files('../wirelog/arena/compound_arena.c'),
  files(
    '../wirelog/util/log.c',
    '../wirelog/util/log_emit.c',
  ),
  include_directories: [wirelog_inc, wirelog_src_inc],
)

test('arena_observability', test_arena_observability_exe, suite: 'log')

# ============================================================================
# Inline-tier integration battery (Issue #532 Task 5)
# ============================================================================

test_columnar_inline_exe = executable(
  'test_columnar_inline',
  files('test_columnar_inline.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('columnar_inline', test_columnar_inline_exe)

# ============================================================================
# FILTER/PROJECT/LFTJ inline wiring unit tests (Issue #534 Task #1)
# ============================================================================

test_inline_compound_wiring_exe = executable(
  'test_inline_compound_wiring',
  files('test_inline_compound_wiring.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('inline_compound_wiring', test_inline_compound_wiring_exe)

# ============================================================================
# Side-relation auto-creation (Issue #533, with #580 arena precondition)
# ============================================================================

test_compound_side_relation_exe = executable(
  'test_compound_side_relation',
  files('test_compound_side_relation.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('compound_side_relation', test_compound_side_relation_exe)

# ============================================================================
# Compound arena freeze-cycle stress (Issue #582)
# Concurrent workers + many freeze/unfreeze cycles on a borrowed arena.
# Heavy under TSan; timeout extended.  Note: this is the in-arena freeze
# cycle, not public daemon rotation. #550 Option C was declined; public
# daemon rotation is covered by the caller-owned replay example.
# ============================================================================

test_compound_arena_freeze_cycle_stress_exe = executable(
  'test_compound_arena_freeze_cycle_stress',
  files('test_compound_arena_freeze_cycle_stress.c'),
  arena_src,
  thread_src,
  workqueue_src,
  files(
    '../wirelog/util/log.c',
    '../wirelog/util/log_emit.c',
  ),
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [threads_dep, math_dep],
)

test('compound_arena_freeze_cycle_stress',
     test_compound_arena_freeze_cycle_stress_exe,
     timeout: 120)

# ============================================================================
# Compound arena GC freeze-guard alloc race (Issue #584)
# W=2 TSan: alloc and gc_epoch_boundary concurrently observe arena->frozen.
# ============================================================================

test_gc_freeze_alloc_race_exe = executable(
  'test_gc_freeze_alloc_race',
  files('test_gc_freeze_alloc_race.c'),
  arena_src,
  thread_src,
  workqueue_src,
  files(
    '../wirelog/util/log.c',
    '../wirelog/util/log_emit.c',
  ),
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [threads_dep, math_dep],
)

test('gc_freeze_alloc_race', test_gc_freeze_alloc_race_exe, timeout: 60)

# ============================================================================
# Handle remap EDB apply pass (Issue #589)
# 10K rows x 3 compound columns acceptance harness, plus negative paths.
# ============================================================================

test_handle_remap_apply_exe = executable(
  'test_handle_remap_apply',
  files('test_handle_remap_apply.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('handle_remap_apply', test_handle_remap_apply_exe, timeout: 60)

# ============================================================================
# Side-relation handle remap pass (Issue #590)
# 100 side-relations x 2-level nested acceptance harness.
# ============================================================================

test_handle_remap_side_apply_exe = executable(
  'test_handle_remap_side_apply',
  files('test_handle_remap_side_apply.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('handle_remap_side_apply', test_handle_remap_side_apply_exe, timeout: 60)

# ============================================================================
# Post-remap cache invalidation (Issue #591)
# Arrangements + dedup_slots reset for __compound_* relations.
# ============================================================================

test_post_remap_invalidate_exe = executable(
  'test_post_remap_invalidate',
  files('test_post_remap_invalidate.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('post_remap_invalidate', test_post_remap_invalidate_exe, timeout: 60)

# ============================================================================
# Worker borrow + W=2 + TSan (Issue #592)
# Pins the R-5 borrow path under explicit two-thread workqueue concurrency,
# closing a gap that #581 (single-threaded) and #582/#584 (standalone arena
# at W=4/W=8/W=2) only cover via race-surface-equivalence reasoning.
# ============================================================================

test_worker_borrow_w2_tsan_exe = executable(
  'test_worker_borrow_w2_tsan',
  files('test_worker_borrow_w2_tsan.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('worker_borrow_w2_tsan', test_worker_borrow_w2_tsan_exe, timeout: 60)

# ============================================================================
# Parameterizable freeze-cycle stress harness (Issue #594)
# Same binary registered three times with WL_STRESS_W / WL_STRESS_R env
# overrides; suite tags route W=4 to nightly and W=8 to release.
# ============================================================================

test_stress_harness_exe = executable(
  'test_stress_harness',
  files('test_stress_harness.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

# Freeze-cycle workload (default).
# PR tier (default suite -> picked up by ci-pr.yml's meson test).
test('stress_harness_w2', test_stress_harness_exe,
     timeout: 60,
     env: {'WL_STRESS_W': '2', 'WL_STRESS_R': '200',
           'WL_STRESS_WORKLOAD': 'freeze-cycle'})

# Nightly tier (suite: stress-nightly).  Wired into perf-nightly.yml.
test('stress_harness_w4', test_stress_harness_exe,
     suite: 'stress-nightly',
     timeout: 120,
     env: {'WL_STRESS_W': '4', 'WL_STRESS_R': '500',
           'WL_STRESS_WORKLOAD': 'freeze-cycle'})

# Release tier (suite: stress-release).  Run manually per
# docs/STRESS_BASELINE.md; no release.yml workflow exists yet.
test('stress_harness_w8', test_stress_harness_exe,
     suite: 'stress-release',
     timeout: 300,
     env: {'WL_STRESS_W': '8', 'WL_STRESS_R': '1000',
           'WL_STRESS_WORKLOAD': 'freeze-cycle'})

# Apply-roundtrip workload (#594 acceptance: pre-rotation handles
# valid post-apply).  Single-mutator by contract -- W is accepted
# but ignored by the workload; PR/nightly/release tiers vary R.
test('stress_harness_apply_roundtrip_pr', test_stress_harness_exe,
     timeout: 60,
     env: {'WL_STRESS_W': '2', 'WL_STRESS_R': '500',
           'WL_STRESS_WORKLOAD': 'apply-roundtrip'})

test('stress_harness_apply_roundtrip_nightly', test_stress_harness_exe,
     suite: 'stress-nightly',
     timeout: 120,
     env: {'WL_STRESS_W': '4', 'WL_STRESS_R': '5000',
           'WL_STRESS_WORKLOAD': 'apply-roundtrip'})

test('stress_harness_apply_roundtrip_release', test_stress_harness_exe,
     suite: 'stress-release',
     timeout: 300,
     env: {'WL_STRESS_W': '8', 'WL_STRESS_R': '50000',
           'WL_STRESS_WORKLOAD': 'apply-roundtrip'})

# Nested-asan workload (#595): session-level register/apply/invalidate/GC
# churn over 100 __compound_ side-relations.  Suite 'asan' so it runs
# every PR via builddir-san (ci-pr.yml:158).  Single-mutator -- W is
# accepted but ignored by the workload; R=50 default keeps wall time
# under 1s without ASan and well under the 120s timeout under ASan.
test('stress_harness_nested_asan', test_stress_harness_exe,
     suite: 'asan',
     timeout: 120,
     env: {'WL_STRESS_R': '50',
           'WL_STRESS_WORKLOAD': 'nested-asan'})

# Rotation-vtable workload (#596): drives the #600 rotation strategy
# vtable (sess->rotation_ops->rotate_eval_arena and ->gc_epoch_boundary)
# under W/R stress.  Both strategy variants (WIRELOG_ROTATION=standard
# and =pinned) must be exercised because #596's contract is the
# dispatch path, not the strategy semantics; PR-tier registers both at
# small R to keep cost down, nightly/release tiers scale R for soak
# coverage.  Single-mutator -- W is accepted but ignored by the workload.
test('stress_harness_rotation_pr', test_stress_harness_exe,
     timeout: 60,
     env: {'WL_STRESS_W': '2', 'WL_STRESS_R': '50',
           'WL_STRESS_WORKLOAD': 'rotation-vtable',
           'WIRELOG_ROTATION': 'standard'})

test('stress_harness_rotation_pr_pinned', test_stress_harness_exe,
     timeout: 60,
     env: {'WL_STRESS_W': '2', 'WL_STRESS_R': '50',
           'WL_STRESS_WORKLOAD': 'rotation-vtable',
           'WIRELOG_ROTATION': 'pinned',
           'WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER': '1'})

test('stress_harness_rotation_nightly', test_stress_harness_exe,
     suite: 'stress-nightly',
     timeout: 240,
     env: {'WL_STRESS_W': '4', 'WL_STRESS_R': '500',
           'WL_STRESS_WORKLOAD': 'rotation-vtable',
           'WIRELOG_ROTATION': 'standard'})

test('stress_harness_rotation_nightly_pinned', test_stress_harness_exe,
     suite: 'stress-nightly',
     timeout: 240,
     env: {'WL_STRESS_W': '4', 'WL_STRESS_R': '500',
           'WL_STRESS_WORKLOAD': 'rotation-vtable',
           'WIRELOG_ROTATION': 'pinned',
           'WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER': '1'})

test('stress_harness_rotation_release', test_stress_harness_exe,
     suite: 'stress-release',
     timeout: 600,
     env: {'WL_STRESS_W': '8', 'WL_STRESS_R': '1000',
           'WL_STRESS_WORKLOAD': 'rotation-vtable',
           'WIRELOG_ROTATION': 'standard'})

test('stress_harness_rotation_release_pinned', test_stress_harness_exe,
     suite: 'stress-release',
     timeout: 600,
     env: {'WL_STRESS_W': '8', 'WL_STRESS_R': '1000',
           'WL_STRESS_WORKLOAD': 'rotation-vtable',
           'WIRELOG_ROTATION': 'pinned',
           'WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER': '1'})

# Daemon-soak workload (#597): long-running soak proving bounded-RSS
# behavior under saturation-driven rotation cadence + cumulative
# handle survival across many gc_epoch_boundary calls within a
# rotation window.  Both strategy variants (WIRELOG_ROTATION=standard
# and =pinned) are registered at PR/nightly/release tiers because #596's
# contract is the dispatch path; the ASan tier is single-strategy to
# keep cost down (both ASan slowdown and a long soak per run).
# WL_STRESS_R for this workload is the step count (not cycle count),
# K=1000 handles per step is fixed, hard-cap is 10000 steps.  Single-
# mutator -- W is accepted but ignored.
test('stress_harness_daemon_pr', test_stress_harness_exe,
     timeout: 60,
     env: {'WL_STRESS_W': '2', 'WL_STRESS_R': '100',
           'WL_STRESS_WORKLOAD': 'daemon-soak',
           'WIRELOG_ROTATION': 'standard'})

test('stress_harness_daemon_pr_pinned', test_stress_harness_exe,
     timeout: 60,
     env: {'WL_STRESS_W': '2', 'WL_STRESS_R': '100',
           'WL_STRESS_WORKLOAD': 'daemon-soak',
           'WIRELOG_ROTATION': 'pinned',
           'WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER': '1'})

test('stress_harness_daemon_asan', test_stress_harness_exe,
     suite: 'asan',
     timeout: 240,
     env: {'WL_STRESS_W': '2', 'WL_STRESS_R': '200',
           'WL_STRESS_WORKLOAD': 'daemon-soak',
           'WIRELOG_ROTATION': 'standard'})

test('stress_harness_daemon_nightly', test_stress_harness_exe,
     suite: 'stress-nightly',
     timeout: 600,
     env: {'WL_STRESS_W': '4', 'WL_STRESS_R': '2000',
           'WL_STRESS_WORKLOAD': 'daemon-soak',
           'WIRELOG_ROTATION': 'standard'})

test('stress_harness_daemon_nightly_pinned', test_stress_harness_exe,
     suite: 'stress-nightly',
     timeout: 600,
     env: {'WL_STRESS_W': '4', 'WL_STRESS_R': '2000',
           'WL_STRESS_WORKLOAD': 'daemon-soak',
           'WIRELOG_ROTATION': 'pinned',
           'WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER': '1'})

test('stress_harness_daemon_release', test_stress_harness_exe,
     suite: 'stress-release',
     timeout: 1800,
     env: {'WL_STRESS_W': '8', 'WL_STRESS_R': '10000',
           'WL_STRESS_WORKLOAD': 'daemon-soak',
           'WIRELOG_ROTATION': 'standard'})

test('stress_harness_daemon_release_pinned', test_stress_harness_exe,
     suite: 'stress-release',
     timeout: 1800,
     env: {'WL_STRESS_W': '8', 'WL_STRESS_R': '10000',
           'WL_STRESS_WORKLOAD': 'daemon-soak',
           'WIRELOG_ROTATION': 'pinned',
           'WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER': '1'})

# ============================================================================
# Per-rotation RSS-bounded + ledger-stability gates (Issue #598)
# ============================================================================
# Residual scope on top of #597's daemon-soak end-of-run gate:
#   - per-rotation VmRSS sampling (not monotonic VmHWM peak)
#   - cumulative cap with no 4 MB floor (10% or 1 MB, whichever larger)
#   - ledger.current_bytes stability across each rotation
# Strict per-rotation delta gate is opt-in via WL_RSS_BOUNDED_STRICT=1.

test_rss_bounded_exe = executable(
  'test_rss_bounded',
  files('test_rss_bounded.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('rss_bounded_pr', test_rss_bounded_exe, timeout: 60)

# Nightly tier: strict per-rotation delta gate at R=500.  Skipped via
# meson exit-77 if warmup CoV exceeds 0.05 on a noisy localhost.
test('rss_bounded_nightly', test_rss_bounded_exe,
     suite: 'stress-nightly',
     timeout: 600,
     env: {'WL_RSS_BOUNDED_STRICT': '1', 'WL_RSS_BOUNDED_R': '500'})

# Release tier: strict per-rotation delta gate at R=1000.
test('rss_bounded_release', test_rss_bounded_exe,
     suite: 'stress-release',
     timeout: 1200,
     env: {'WL_RSS_BOUNDED_STRICT': '1', 'WL_RSS_BOUNDED_R': '1000'})

# ASan tier: RSS portion is compile-time skipped (ASan instrumentation
# confounds VmRSS/VmHWM signal).  Ledger and rotation assertions still
# run under ASan to surface UAF/heap-overflow on the rotation hot path.
test('rss_bounded_asan', test_rss_bounded_exe,
     suite: 'asan',
     timeout: 240)

# ============================================================================
# Issue #599: rotate-latency p50/p99/p999 gate.  Companion to #598's
# RSS-bounded gate -- same workload (K=64 handles, 24-byte payload,
# compound_arena), same rotation_ops vtable, but gates wall-clock latency
# of the rotate_eval_arena + gc_epoch_boundary pair.
#
# Suite: 'perf' (mirrors test_log_perf_gate.c).  Default-suite invocation
# SKIPs unless WIRELOG_PERF_GATE=1.  No 'asan' variant: the test is
# compile-time-skipped under sanitizers (wall-clock timing is invalidated
# by ASan/TSan instrumentation).
#
#   Linux:   env WIRELOG_PERF_GATE=1 taskset -c 0 meson test -C build --suite perf
#   Other:   no shipped stability design today -- test SKIPs at runtime.
# ============================================================================

test_rotate_latency_exe = executable(
  'test_rotate_latency',
  files('test_rotate_latency.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

# PR tier: N=1000.  Opt-in via WIRELOG_PERF_GATE=1; default invocation SKIPs.
test('rotate_latency_pr', test_rotate_latency_exe,
     suite: 'perf',
     timeout: 60,
     env: {'WIRELOG_PERF_GATE': '1', 'WL_ROTATE_LATENCY_N': '1000',
           'WIRELOG_ROTATION': 'standard'})

test('rotate_latency_pr_pinned', test_rotate_latency_exe,
     suite: 'perf',
     timeout: 60,
     env: {'WIRELOG_PERF_GATE': '1', 'WL_ROTATE_LATENCY_N': '1000',
           'WIRELOG_ROTATION': 'pinned',
           'WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER': '1'})

# Nightly tier: strict (p999 gated) at N=10000.
test('rotate_latency_nightly', test_rotate_latency_exe,
     suite: 'perf',
     timeout: 600,
     env: {'WIRELOG_PERF_GATE': '1', 'WL_ROTATE_LATENCY_STRICT': '1',
           'WL_ROTATE_LATENCY_N': '10000',
           'WIRELOG_ROTATION': 'standard'})

test('rotate_latency_nightly_pinned', test_rotate_latency_exe,
     suite: 'perf',
     timeout: 600,
     env: {'WIRELOG_PERF_GATE': '1', 'WL_ROTATE_LATENCY_STRICT': '1',
           'WL_ROTATE_LATENCY_N': '10000',
           'WIRELOG_ROTATION': 'pinned',
           'WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER': '1'})

# Release tier: strict at N=100000.
test('rotate_latency_release', test_rotate_latency_exe,
     suite: 'stress-release',
     timeout: 1800,
     env: {'WIRELOG_PERF_GATE': '1', 'WL_ROTATE_LATENCY_STRICT': '1',
           'WL_ROTATE_LATENCY_N': '100000',
           'WIRELOG_ROTATION': 'standard'})

test('rotate_latency_release_pinned', test_rotate_latency_exe,
     suite: 'stress-release',
     timeout: 1800,
     env: {'WIRELOG_PERF_GATE': '1', 'WL_ROTATE_LATENCY_STRICT': '1',
           'WL_ROTATE_LATENCY_N': '100000',
           'WIRELOG_ROTATION': 'pinned',
           'WIRELOG_ROTATION_ACKNOWLEDGE_PLACEHOLDER': '1'})

# ----------------------------------------------------------------------------
# deep_copy ledger-charge canary (Issue #598)
# Vacuous today (col_rel_deep_copy has no production rotation callers);
# regression canary for the deep-copy ledger contract.
# ----------------------------------------------------------------------------

test_deep_copy_ledger_canary_exe = executable(
  'test_deep_copy_ledger_canary',
  files('test_deep_copy_ledger_canary.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('deep_copy_ledger_canary', test_deep_copy_ledger_canary_exe)

# ============================================================================
# Compound arena lifecycle observability (Issue #583)
# WL_LOG=COMPOUND:5 must emit alloc/freeze/unfreeze tags.
# ============================================================================

test_compound_arena_trace_exe = executable(
  'test_compound_arena_trace',
  files('test_compound_arena_trace.c'),
  arena_src,
  thread_src,
  files(
    '../wirelog/util/log.c',
    '../wirelog/util/log_emit.c',
  ),
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [threads_dep, math_dep],
)

test('compound_arena_trace', test_compound_arena_trace_exe, suite: 'log')

# ============================================================================
# K-Fusion deep-copy + inline compound visibility (Issue #532 Task 4)
# ============================================================================

test_k_fusion_inline_shadow_exe = executable(
  'test_k_fusion_inline_shadow',
  files('test_k_fusion_inline_shadow.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('k_fusion_inline_shadow', test_k_fusion_inline_shadow_exe, timeout: 60)

# ============================================================================
# K-Fusion Correctness Tests (Issue #534 Task 5)
# Verifies K=1/K=2/K=4/K=8 byte-for-byte output equivalence via fingerprint
# ============================================================================

test_k_fusion_correctness_exe = executable(
  'test_k_fusion_correctness',
  files('test_k_fusion_correctness.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  parser_src,
  ir_src,
  optimizer_src,
  io_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('k_fusion_correctness', test_k_fusion_correctness_exe, timeout: 60)

# ============================================================================
# K=4 Inline Compound Authorization E2E (Issue #534 Task 5 - Auth Use Case)
# ============================================================================

test_e2e_kfusion_k4_inline_exe = executable(
  'test_e2e_kfusion_k4_inline',
  files('test_e2e_kfusion_k4_inline.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('e2e_kfusion_k4_inline', test_e2e_kfusion_k4_inline_exe, timeout: 60)

# ============================================================================
# TSan stress: K=4 concurrent readers over nested inline compound (Issue #534 Task 3)
# K=4 workers x 10k iterations over scope(metadata(t,ts,loc,risk)) inline/4.
# + 100 insert/delete lifecycle cycles with K=4 concurrent readers per epoch.
# TSan clean == 0 data races on inline column buffers.
# ============================================================================

test_e2e_tsan_inline_kfusion_exe = executable(
  'test_e2e_tsan_inline_kfusion',
  files('test_e2e_tsan_inline_kfusion.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('e2e_tsan_inline_kfusion', test_e2e_tsan_inline_kfusion_exe,
     suite: 'tsan',
     timeout: 180)

# ============================================================================
# ASan stress: K=4 workers, nested inline + side-relation arena GC (Issue #534 Task 4)
# K=4 independent workers x 2500 cycles: arena alloc/retain/gc + inline store/retrieve.
# + arena freeze/unfreeze correctness under K-Fusion epoch model.
# ASan clean == 0 leaks, 0 use-after-free.
# ============================================================================

test_e2e_asan_side_relation_nested_exe = executable(
  'test_e2e_asan_side_relation_nested',
  files('test_e2e_asan_side_relation_nested.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('e2e_asan_side_relation_nested', test_e2e_asan_side_relation_nested_exe,
     suite: 'asan',
     timeout: 120)

# ============================================================================
# Per-Worker Session State (Issue #315)
# ============================================================================

test_worker_session_exe = executable(
  'test_worker_session',
  files('test_worker_session.c'),
  files('../wirelog/extension_registry.c'),
  backend_src,
  parser_src,
  ir_src,
  optimizer_src,
  arena_src,
  io_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('worker_session', test_worker_session_exe)

# ============================================================================
# Worker Arena Borrow (Issue #579 / R-5)
# ============================================================================

test_worker_arena_borrow_exe = executable(
  'test_worker_arena_borrow',
  files('test_worker_arena_borrow.c'),
  backend_src,
  parser_src,
  ir_src,
  optimizer_src,
  arena_src,
  io_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('worker_arena_borrow', test_worker_arena_borrow_exe)

# ============================================================================
# Exchange Operator (Issue #316)
# ============================================================================

test_td_exchange_exe = executable(
  'test_td_exchange',
  files('test_td_exchange.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('td_exchange', test_td_exchange_exe)

# ============================================================================
# Distributed Stratum Evaluator Non-Recursive Path (Issue #318)

test_tdd_nonrecursive_exe = executable(
  'test_tdd_nonrecursive',
  files('test_tdd_nonrecursive.c'),
  backend_src,
  parser_src,
  ir_src,
  optimizer_src,
  arena_src,
  io_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('tdd_nonrecursive', test_tdd_nonrecursive_exe)

test_tdd_recursive_exe = executable(
  'test_tdd_recursive',
  files('test_tdd_recursive.c'),
  backend_src,
  parser_src,
  ir_src,
  optimizer_src,
  arena_src,
  io_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('tdd_recursive', test_tdd_recursive_exe)

# ============================================================================
# TDD Exchange Operator Integration (Issue #318 Phase 3)
# ============================================================================

test_tdd_exchange_exe = executable(
  'test_tdd_exchange',
  files('test_tdd_exchange.c'),
  backend_src,
  parser_src,
  ir_src,
  optimizer_src,
  arena_src,
  io_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('tdd_exchange', test_tdd_exchange_exe)

test_tdd_convergence_exe = executable(
  'test_tdd_convergence',
  files('test_tdd_convergence.c'),
  backend_src,
  parser_src,
  ir_src,
  optimizer_src,
  arena_src,
  io_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('tdd_convergence', test_tdd_convergence_exe)

# ============================================================================
# TDD Integration Tests (Issue #321)
# ============================================================================

test_tdd_integration_exe = executable(
  'test_tdd_integration',
  files('test_tdd_integration.c'),
  backend_src,
  parser_src,
  ir_src,
  optimizer_src,
  arena_src,
  io_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('tdd_integration', test_tdd_integration_exe)

# ============================================================================
# Per-Worker Frontier Progress Protocol (Issue #317)
# ============================================================================

test_frontier_progress_exe = executable(
  'test_frontier_progress',
  files('test_frontier_progress.c'),
  backend_src,
  intern_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('frontier_progress', test_frontier_progress_exe)

# ============================================================================
# Multiworker Integration Tests (Issue #321)
# ============================================================================

test_td_multiworker_exe = executable(
  'test_td_multiworker',
  files('test_td_multiworker.c'),
  backend_src,
  parser_src,
  ir_src,
  optimizer_src,
  arena_src,
  io_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('td_multiworker', test_td_multiworker_exe)

# ============================================================================
# TDD Multi-Worker Regression Tests (Issue #404)
# ============================================================================

test_tdd_multiworker_exe = executable(
  'test_tdd_multiworker',
  files('test_tdd_multiworker.c'),
  backend_src,
  parser_src,
  ir_src,
  optimizer_src,
  arena_src,
  io_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('tdd_multiworker', test_tdd_multiworker_exe)

test_relation_append_exe = executable(
  'test_relation_append',
  files('test_relation_append.c'),
  backend_src,
  parser_src,
  ir_src,
  optimizer_src,
  arena_src,
  io_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('relation_append', test_relation_append_exe)

test_tdd_decision_stats_exe = executable(
  'test_tdd_decision_stats',
  files('test_tdd_decision_stats.c'),
  backend_src,
  parser_src,
  ir_src,
  optimizer_src,
  arena_src,
  io_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('tdd_decision_stats', test_tdd_decision_stats_exe)

test_tdd_single_key_owner_exe = executable(
  'test_tdd_single_key_owner',
  files('test_tdd_single_key_owner.c'),
  backend_src,
  parser_src,
  ir_src,
  optimizer_src,
  arena_src,
  io_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('tdd_single_key_owner', test_tdd_single_key_owner_exe)

test_constant_filter_pushdown_exe = executable(
  'test_constant_filter_pushdown',
  files('test_constant_filter_pushdown.c'),
  backend_src,
  parser_src,
  ir_src,
  optimizer_src,
  arena_src,
  io_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('constant_filter_pushdown', test_constant_filter_pushdown_exe)

# ============================================================================
# Safe Worker Scaling Tests (Issue #409)
# ============================================================================

test_safe_worker_scaling_exe = executable(
  'test_safe_worker_scaling',
  files('test_safe_worker_scaling.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('safe_worker_scaling', test_safe_worker_scaling_exe)

test_workers_as_cap_exe = executable(
  'test_workers_as_cap',
  files('test_workers_as_cap.c'),
  backend_src,
  parser_src,
  ir_src,
  optimizer_src,
  arena_src,
  io_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('workers_as_cap', test_workers_as_cap_exe)

# ============================================================================
# Queue Transport Equivalence and Stress Tests (Issue #410)
# ============================================================================

test_queue_transport_exe = executable(
  'test_queue_transport',
  files('test_queue_transport.c'),
  backend_src,
  parser_src,
  ir_src,
  optimizer_src,
  arena_src,
  io_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('queue_transport', test_queue_transport_exe)

# ============================================================================
# DOOP Multi-Worker Stability Tests (Issue #416)
# ============================================================================

# Issue #958: the intern table is shared by every parallel worker.  The
# K-fusion path (recursive rule, 4 IDB body atoms, cat() in the head) is
# the cheapest way to reach it -- no row-count gate, 30 facts suffice.
test_intern_parallel_determinism_exe = executable(
  'test_intern_parallel_determinism',
  files('test_intern_parallel_determinism.c'),
  backend_src,
  parser_src,
  ir_src,
  optimizer_src,
  arena_src,
  io_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('intern_parallel_determinism', test_intern_parallel_determinism_exe)

test_doop_multiworker_exe = executable(
  'test_doop_multiworker',
  files('test_doop_multiworker.c'),
  backend_src,
  parser_src,
  ir_src,
  optimizer_src,
  arena_src,
  io_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('doop_multiworker', test_doop_multiworker_exe,
  env: ['DOOP_DATA_DIR=' + meson.project_source_root() / 'bench/data/doop'],
)

# ============================================================================
# String Operations Tests (Issue #143)
# ============================================================================

test_string_ops_exe = executable(
  'test_string_ops',
  files('test_string_ops.c'),
  intern_src,
  string_ops_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [threads_dep],
)

test('string_ops', test_string_ops_exe)

test_string_parser_exe = executable(
  'test_string_parser',
  files('test_string_parser.c'),
  parser_src,
  ir_src,
  optimizer_src,
  io_src,
  thread_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [threads_dep],
)

test('string_parser', test_string_parser_exe)

test_string_eval_exe = executable(
  'test_string_eval',
  files('test_string_eval.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('string_eval', test_string_eval_exe)

test_wirelog_easy_exe = executable(
  'test_wirelog_easy',
  files('test_wirelog_easy.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  '../wirelog/wirelog-easy.c',
  '../wirelog/api_facade.c',
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('wirelog_easy', test_wirelog_easy_exe)

# Issue #718: inline .dl static-fact materialization regression.
test_wirelog_easy_inline_facts_exe = executable(
  'test_wirelog_easy_inline_facts',
  files('test_wirelog_easy_inline_facts.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  '../wirelog/wirelog-easy.c',
  '../wirelog/api_facade.c',
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('wirelog_easy_inline_facts', test_wirelog_easy_inline_facts_exe)

test_host_insert_width_exe = executable(
  'test_host_insert_width',
  files('test_host_insert_width.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  '../wirelog/wirelog-easy.c',
  '../wirelog/api_facade.c',
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('host_insert_width', test_host_insert_width_exe)

# Issue #717 / #703: behavioral-parity test for the advanced session API.
test_wirelog_advanced_exe = executable(
  'test_wirelog_advanced',
  files('test_wirelog_advanced.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  '../wirelog/wirelog-easy.c',
  '../wirelog/api_facade.c',
  '../wirelog/wirelog_advanced.c',
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('wirelog_advanced', test_wirelog_advanced_exe)

# Issue #841: umbrella public API symbols are real linkable/runtime
# entry points, not only header declarations.
test_wirelog_public_api_exe = executable(
  'test_wirelog_public_api',
  files('test_wirelog_public_api.c'),
  include_directories: [wirelog_inc, wirelog_src_inc],
  link_with: wirelog_lib,
)

test('wirelog_public_api', test_wirelog_public_api_exe)

# ============================================================================
# Baseline .input Fixture Snapshot Tests (Issue #448)
# ============================================================================

baseline_py3 = find_program('python3', 'python', required: true)
baseline_diff_script = files('golden_diff_baseline.py')
baseline_fixture_dir = meson.project_source_root() / 'tests/fixtures/baseline_input'
baseline_snapshot_dir = meson.project_source_root() / 'tests/baseline_snapshots'

# Issue #464: CLI-dependent tests skip on Android (wirelog_cli is not
# built when -Dandroid=true; see meson.build CLI Driver section).  The
# guard mirrors the cli executable() guard in the project meson.build.
if not is_android
  test('baseline_int_edges',
    baseline_py3,
    args: [
      baseline_diff_script[0],
      wirelog_cli.full_path(),
      baseline_fixture_dir / 'int_edges.dl',
      baseline_snapshot_dir / 'int_edges_stdout.txt',
    ],
    depends: wirelog_cli,
    workdir: baseline_fixture_dir,
    suite: 'baseline',
  )

  test('baseline_sym_family',
    baseline_py3,
    args: [
      baseline_diff_script[0],
      wirelog_cli.full_path(),
      baseline_fixture_dir / 'sym_family.dl',
      baseline_snapshot_dir / 'sym_family_stdout.txt',
    ],
    depends: wirelog_cli,
    workdir: baseline_fixture_dir,
    suite: 'baseline',
  )

  test('baseline_tab_nodes',
    baseline_py3,
    args: [
      baseline_diff_script[0],
      wirelog_cli.full_path(),
      baseline_fixture_dir / 'tab_nodes.dl',
      baseline_snapshot_dir / 'tab_nodes_stdout.txt',
    ],
    depends: wirelog_cli,
    workdir: baseline_fixture_dir,
    suite: 'baseline',
  )

  # ============================================================================
  # CLI Version / Help Surface Test (Issue #801)
  # Reuses baseline_py3 (already resolved above) to invoke the executable.
  # ============================================================================

  test('cli_parse_diagnostics',
    baseline_py3,
    args: [
      files('test_cli_parse_diagnostics.py')[0],
      wirelog_cli.full_path(),
    ],
    depends: wirelog_cli,
    suite: 'cli',
  )

  test('string_compare_ops',
    baseline_py3,
    args: [
      files('test_string_compare_ops.py')[0],
      wirelog_cli.full_path(),
    ],
    depends: wirelog_cli,
    suite: 'cli',
  )

  test('inline_slot_types',
    baseline_py3,
    args: [
      files('test_inline_slot_types.py')[0],
      wirelog_cli.full_path(),
    ],
    depends: wirelog_cli,
    suite: 'cli',
  )

  test('cli_version',
    baseline_py3,
    args: [
      files('test_cli_version.py')[0],
      wirelog_cli.full_path(),
      meson.project_version(),
    ],
    depends: wirelog_cli,
    suite: 'cli',
  )
endif

# ============================================================================
# I/O Adapter Registry Tests (#450)
# ============================================================================

test_io_adapter_exe = executable(
  'test_io_adapter',
  files('test_io_adapter.c', '../wirelog/io/io_adapter.c', '../wirelog/io/csv_adapter.c', '../wirelog/io/csv_reader.c', '../wirelog/io/io_ctx.c', '../wirelog/intern.c'),
  thread_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  c_args: ['-DWIRELOG_BUILDING'],
  dependencies: [threads_dep],
  install: false,
)
test('io_adapter', test_io_adapter_exe)

# Scalar addon registry ABI/lifecycle core (Issue #1245, first atomic unit).
test_extension_registry_exe = executable(
  'test_extension_registry',
  files('test_extension_registry.c', '../wirelog/extension_registry.c'),
  thread_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  c_args: ['-DWIRELOG_BUILDING'],
  dependencies: [threads_dep],
  install: false,
)
test('extension_registry', test_extension_registry_exe)

# Session-owned extension snapshot lifetime (Issue #1250 atomic unit).
test_session_extension_snapshot_exe = executable(
  'test_session_extension_snapshot',
  files('test_session_extension_snapshot.c',
        '../wirelog/session.c', '../wirelog/extension_registry.c'),
  thread_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  c_args: ['-DWIRELOG_BUILDING'],
  dependencies: [threads_dep],
  install: false,
)
test('session_extension_snapshot', test_session_extension_snapshot_exe)

test_extension_filter_exe = executable(
  'test_extension_filter',
  files('test_extension_filter.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  workqueue_src,
  thread_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
)

test('extension_filter', test_extension_filter_exe)

# ============================================================================
# I/O Context Accessor Tests (#452)
# ============================================================================

test_io_ctx_exe = executable(
  'test_io_ctx',
  files('test_io_ctx.c', '../wirelog/io/io_ctx.c'),
  intern_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  c_args: ['-DWIRELOG_BUILDING'],
  dependencies: [threads_dep],
  install: false,
)
test('io_ctx', test_io_ctx_exe)

# ============================================================================
# I/O Dispatch Tests (#457)
# ============================================================================

test_io_dispatch_exe = executable(
  'test_io_dispatch',
  files('test_io_dispatch.c'),
  parser_src,
  ir_src,
  io_src,
  thread_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  c_args: ['-DWIRELOG_BUILDING', '-DTEST_DISPATCH_PRESENT'],
  dependencies: [threads_dep],
  install: false,
)
test('io_dispatch', test_io_dispatch_exe)

# ============================================================================
# .input CSV Arity Validation Tests (#977)
# Drives the full pipeline (parse -> plan -> load .input -> evaluate) so the
# columnar insert path sees the loaded rows; a width mismatch between the CSV
# and the .decl must fail the load instead of re-striding (or over-reading).
# ============================================================================

test_input_arity_exe = executable(
  'test_input_arity',
  files('test_input_arity.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  driver_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
  install: false,
)
test('input_arity', test_input_arity_exe)

# ============================================================================
# CLI physical column rendering (#1036)
# ============================================================================
test_cli_physical_columns_exe = executable(
  'test_cli_physical_columns',
  files('test_cli_physical_columns.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  driver_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
  install: false,
)
test('cli_physical_columns', test_cli_physical_columns_exe)

# ============================================================================
# CSV Reader Line/Field Limit Tests (#953)
# The reader used to pull lines through a fixed 4096-byte fgets buffer, so a
# longer physical line was delivered in chunks and every chunk became its own
# tuple -- truncation that fabricated rows.  #977's width check cannot cover
# the class because the reader never assembles the whole line.  Drives the
# full pipeline, plus one direct wl_csv_parse_line_ex() case for the field
# buffer that is only reachable off the file path, and a >256-column case
# pinning that #997's row-write capacity bound survives the rewrite.
# ============================================================================

test_csv_limits_exe = executable(
  'test_csv_limits',
  files('test_csv_limits.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  driver_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
  install: false,
)
test('csv_limits', test_csv_limits_exe)

# ============================================================================
# Wide-relation CSV Loading Tests (Issue #997)
# The string-aware CSV parser wrote one int64_t per declared column into a
# fixed row_values[256] frame buffer with no capacity check, so a relation
# with 257+ columns and at least one symbol overflowed the caller's stack.
# Drives the pipeline, wl_csv_read_file_via_ctx(), and the public
# wirelog_load_facts_from_csv() entry an embedder reaches the parser through.
# ============================================================================

test_csv_wide_columns_exe = executable(
  'test_csv_wide_columns',
  files('test_csv_wide_columns.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  driver_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
  install: false,
)

# ============================================================================
# Wide-Relation Row Buffer Tests (#1000)
# Every columnar operator copies rows through an int64_t[COL_STACK_MAX]
# buffer but fills it at the relation's width, so relations wider than 32
# columns smash the stack (SIGABRT in release, not just an ASan report).
# One rule shape per operator kind at widths 32 / 33 / 200.
# ============================================================================

test_wide_relation_exe = executable(
  'test_wide_relation',
  files('test_wide_relation.c'),
  parser_src,
  ir_src,
  optimizer_src,
  backend_src,
  io_src,
  arena_src,
  thread_src,
  workqueue_src,
  driver_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  dependencies: [nanoarrow_dep, threads_dep, xxhash_dep, mbedtls_dep, math_dep],
  install: false,
)
test('csv_wide_columns', test_csv_wide_columns_exe)
test('wide_relation', test_wide_relation_exe, timeout: 120)

# ============================================================================
# Concurrent I/O Adapter Registry Stress Test (#459)
# Exercises mutex safety under TSan: concurrent find, register/unregister,
# and mixed readers+writers.  Linux/macOS only (pthreads; skipped on Windows).
# ============================================================================

test_io_adapter_concurrent_exe = executable(
  'test_io_adapter_concurrent',
  files('test_io_adapter_concurrent.c',
        '../wirelog/io/io_adapter.c',
        '../wirelog/io/csv_adapter.c',
        '../wirelog/io/csv_reader.c',
        '../wirelog/io/io_ctx.c',
        '../wirelog/intern.c'),
  thread_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  c_args: ['-DWIRELOG_BUILDING'],
  dependencies: [threads_dep],
  install: false,
)
test('io_adapter_concurrent', test_io_adapter_concurrent_exe)

# ============================================================================
# Mock Adapter Error-Path Tests for ASan (#459)
# Exercises NULL inputs, boundary scheme lengths, registry-full, and
# error-string clearing.  ASan catches out-of-bounds writes on scheme copy.
# ============================================================================

test_io_adapter_asan_exe = executable(
  'test_io_adapter_asan',
  files('test_io_adapter_asan.c',
        '../wirelog/io/io_adapter.c',
        '../wirelog/io/csv_adapter.c',
        '../wirelog/io/csv_reader.c',
        '../wirelog/io/io_ctx.c',
        '../wirelog/intern.c'),
  thread_src,
  include_directories: [wirelog_inc, wirelog_src_inc],
  c_args: ['-DWIRELOG_BUILDING'],
  dependencies: [threads_dep],
  install: false,
)
test('io_adapter_asan', test_io_adapter_asan_exe)

# ======================================================================
# Plugin loader tests (Issue #461)
# Only built when io_plugin_dlopen is enabled and not on Windows
# ======================================================================
if io_plugin_dlopen_option == 'enabled' and not is_windows

  # Mock plugin shared library (valid adapter)
  mock_plugin_lib = shared_library(
    'mock_plugin_adapter',
    files('mock_plugin_adapter.c'),
    include_directories: [wirelog_inc, wirelog_src_inc],
    install: false,
  )

  # Mock plugin shared library (ABI mismatch — always returns NULL)
  mock_plugin_bad_abi_lib = shared_library(
    'mock_plugin_adapter_bad_abi',
    files('mock_plugin_adapter_bad_abi.c'),
    include_directories: [wirelog_inc, wirelog_src_inc],
    install: false,
  )

  _dl_test_dep = cc.find_library('dl', required: false)
  test_plugin_loader_exe = executable(
    'test_plugin_loader',
    files('test_plugin_loader.c',
          '../wirelog/cli/plugin_loader.c',
          '../wirelog/io/io_adapter.c',
          '../wirelog/io/csv_adapter.c',
          '../wirelog/io/csv_reader.c',
          '../wirelog/io/io_ctx.c',
          '../wirelog/intern.c'),
    thread_src,
    include_directories: [wirelog_inc, wirelog_src_inc],
    c_args: ['-DWIRELOG_BUILDING', '-DWL_HAVE_PLUGIN_LOADER'],
    dependencies: [threads_dep] + (_dl_test_dep.found() ? [_dl_test_dep] : []),
    install: false,
  )
  test('plugin_loader', test_plugin_loader_exe,
    args: [mock_plugin_lib.full_path(), mock_plugin_bad_abi_lib.full_path()],
    depends: [mock_plugin_lib, mock_plugin_bad_abi_lib],
  )

endif

# ======================================================================
# bench_compound smoke test (Issue #536 TDD red step)
# Execs bench/bench_compound --mode parser --iters 10 and asserts
# stdout contains p50=, p95=, p99=.  bench_compound_exe is defined in
# bench/meson.build (processed before this subdir); depends: ensures
# the binary is built before the test runs.
# ======================================================================

test_bench_compound_smoke_exe = executable(
  'test_bench_compound_smoke',
  files('test_bench_compound.c'),
  include_directories: [],
  install: false,
)

# bench_compound_exe is defined in bench/meson.build (processed before
# this subdir).  depends: ensures the binary is built before the test runs;
# args: passes the absolute path so the test does not rely on CWD.
test('bench_compound_smoke', test_bench_compound_smoke_exe,
  args: [bench_compound_exe.full_path()],
  depends: [bench_compound_exe],
)
