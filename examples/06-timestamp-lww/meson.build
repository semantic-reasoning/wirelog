# examples/06-timestamp-lww/meson.build
# Golden-output regression test for the last-write-wins example (#965)
#
# The example addresses its CSVs with repository-root-relative paths, so the
# runner copies it into a temporary tree, runs the CLI there, and diffs the
# produced files against the committed ones.  Nothing under the source tree
# is written, so a regression cannot update its own golden.
#
# lww_resolution.dl declares update(id: symbol, ts: int64, value: string) and
# reduces `max(Ts)` over it: the one in-tree fixture where an aggregate's
# operand is an integer column sitting between two symbol ones, so an
# off-by-one in operand-type inference lands on it.  latest_out.csv then
# exercises the join back to the full record, where a wrong winner shows up
# as the wrong `value`.
#
# What this catches is the aggregate answer end to end through the CLI, not
# operand typing as such: these timestamps are far past the end of the
# intern table, so a `max(Ts)` mistyped as a string still falls back to the
# numeric comparison and still answers correctly.  The discriminating case
# needs values small enough to name real intern ids and lives in
# tests/test_symbol_aggregates.c (test_integer_aggregates_unchanged).

py3 = find_program('python3', 'python', required: true)

test(
  'example_06_timestamp_lww_golden',
  py3,
  args: [
    meson.project_source_root() / 'examples' / 'golden_example_diff.py',
    '--cli', wirelog_cli.full_path(),
    '--root', meson.project_source_root(),
    '--dir', 'examples/06-timestamp-lww',
    '--program', 'lww_resolution.dl',
    '--golden', 'latest_ts_out.csv',
    '--golden', 'latest_out.csv',
    # The check the example's README documents: the winning timestamps are
    # the committed expected_latest.csv.
    '--same', 'latest_ts_out.csv:expected_latest.csv',
  ],
  depends: wirelog_cli,
  suite: 'examples',
)
