# Fuzzing

Current `#743` scaffolding includes parser, CSV reader, intern, and compound
arena targets.

- `parser_fuzz` exercises `wl_parser_parse_string()` via `LLVMFuzzerTestOneInput`.
- `csv_reader_fuzz` exercises `wl_csv_parse_line()` and `wl_csv_parse_line_ex()`
  at the line level.
- `intern_fuzz` exercises `wl_intern_*()` APIs with randomized symbol tokens.
- `compound_arena_fuzz` exercises bounded `wl_compound_arena_*()` allocation,
  lookup, retain, freeze/unfreeze, GC-boundary, and live-handle paths.

## Build + run

```sh
CC=clang meson setup build-fuzz -Denable_fuzz=true -Db_sanitize=address --wipe
meson compile -C build-fuzz parser_fuzz csv_reader_fuzz intern_fuzz compound_arena_fuzz
meson test -C build-fuzz --suite fuzz --print-errorlogs
```

The default profile remains unchanged. `-Denable_fuzz=true` gates the extra targets
and requires a Clang + libFuzzer-capable toolchain.
