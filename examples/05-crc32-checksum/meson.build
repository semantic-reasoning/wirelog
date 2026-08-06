# examples/05-crc32-checksum/meson.build
# Golden-output regression test for the CRC-32 example (#963)
#
# This example is the regression test for #963: its committed checksums are
# crc32 over the payload bytes, so while crc32_ethernet() digested the
# payload's intern id instead, valid_frames_output.csv came out empty against
# a four-row golden, all six frames were reported corrupt, and the `diff`
# step the README documents failed.  Both rules are covered -- the filter
# `crc32_ethernet(Payload) = Stored` and the head projection
# `corrupt_frame(Id, Stored, crc32_ethernet(Payload))`.

py3 = find_program('python3', 'python', required: true)

test(
  'example_05_crc32_checksum_golden',
  py3,
  args: [
    meson.project_source_root() / 'examples' / 'golden_example_diff.py',
    '--cli', wirelog_cli.full_path(),
    '--root', meson.project_source_root(),
    '--dir', 'examples/05-crc32-checksum',
    '--program', 'crc32_validate.dl',
    '--golden', 'valid_frames_output.csv',
    '--golden', 'corrupt_frames_output.csv',
    # The check the example's README tells the reader to run.
    '--same', 'valid_frames_output.csv:expected_valid_frames.csv',
  ],
  depends: wirelog_cli,
  suite: 'examples',
)
