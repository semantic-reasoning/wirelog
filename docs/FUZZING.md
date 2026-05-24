# Parser Fuzzing

The `#743` fuzzing scaffold currently includes only a parser-target reference
implementation. It exercises `wl_parser_parse_string()` via a libFuzzer entrypoint
(`LLVMFuzzerTestOneInput`) and runs a tiny seeded smoke check during `meson test
--suite fuzz`.

## Build + run (parser only)

```sh
CC=clang meson setup build-fuzz-parser -Denable_fuzz=true -Db_sanitize=address --wipe
meson compile -C build-fuzz-parser parser_fuzz
meson test -C build-fuzz-parser --suite fuzz --print-errorlogs
```

The default profile remains unchanged. `-Denable_fuzz=true` gates the extra target
and requires a Clang + libFuzzer-capable toolchain.

## Follow-up targets (Issue #743)

`#743` also tracks CSV/intern/compound fuzzers as follow-up scaffolds after parser
coverage lands.
