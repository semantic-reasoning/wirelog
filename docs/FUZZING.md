# Fuzzing

Current `#743` scaffolding includes parser, CSV reader, and intern targets.

- `parser_fuzz` exercises `wl_parser_parse_string()` via `LLVMFuzzerTestOneInput`.
- `csv_reader_fuzz` exercises `wl_csv_parse_line()` and `wl_csv_parse_line_ex()`
  at the line level.
- `intern_fuzz` exercises `wl_intern_*()` APIs with randomized symbol tokens.

## Build + run (parser only)

```sh
CC=clang meson setup build-fuzz-parser -Denable_fuzz=true -Db_sanitize=address --wipe
meson compile -C build-fuzz-parser parser_fuzz
meson test -C build-fuzz-parser --suite fuzz --print-errorlogs
```

The default profile remains unchanged. `-Denable_fuzz=true` gates the extra targets
and requires a Clang + libFuzzer-capable toolchain.

## Follow-up targets (Issue #743)

`#743` also tracks compound arena fuzzers as follow-up scaffolds after
parser, CSV reader, and intern line-level coverage lands.
