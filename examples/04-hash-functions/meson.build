# examples/04-hash-functions/meson.build
# Golden-output regression test for the hash() example (#963)
#
# The example addresses its CSVs with repository-root-relative paths, so the
# runner copies it into a temporary tree, runs the CLI there, and diffs the
# produced files against the committed ones.  Nothing under the source tree
# is written, so a regression cannot update its own golden.
#
# fingerprints_output.csv is the file #963 moved: before the fix it held
# XXH3 of the intern ids 1/3/5/8 rather than of the e-mail addresses.  The
# other three outputs are the integer path and must not move.

py3 = find_program('python3', 'python', required: true)

test(
  'example_04_hash_functions_golden',
  py3,
  args: [
    meson.project_source_root() / 'examples' / 'golden_example_diff.py',
    '--cli', wirelog_cli.full_path(),
    '--root', meson.project_source_root(),
    '--dir', 'examples/04-hash-functions',
    '--program', 'hash_dedup.dl',
    '--golden', 'fingerprints_output.csv',
    '--golden', 'unique_records_output.csv',
    '--golden', 'valid_records_output.csv',
    '--golden', 'corrupted_records_output.csv',
    # The deduplication check the example's README tells the reader to run.
    '--same', 'unique_records_output.csv:expected_deduplicated.csv',
  ],
  depends: wirelog_cli,
  suite: 'examples',
)
