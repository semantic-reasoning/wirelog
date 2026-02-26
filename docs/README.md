# wirelog 문서 가이드

wirelog 프로젝트의 아키텍처 및 설계 문서.

---

## 핵심 문서

### ARCHITECTURE.md (영문)

현재 wirelog 설계 현황 및 개발 로드맵.

- 핵심 요구사항 (Multi-target, FPGA 준비, C11 기반)
- 아키텍처 레이어 설계 (Logic Layer → DD Translator → DD)
- Phase별 개발 로드맵 (Phase 0-4)
- 기술 스택 및 설계 결정 (Allocator ADR 포함)
- 미결정 설계 사항

### ARCHITECTURE.ko.md (한국어)

ARCHITECTURE.md의 한국어 버전. 동일한 내용.

---

## 구현 현황 (Phase 0 완료, Phase 1 진행 중)

| 모듈 | 테스트 | 상태 |
|------|--------|------|
| Parser (hand-written RDP) | 91/91 (47 lexer + 44 parser) | ✅ 완료 |
| IR Representation (8 node types) | 60/60 (19 IR + 41 program) | ✅ 완료 |
| Stratification & SCC (Tarjan's) | 20/20 | ✅ 완료 |
| DD Plan Translator (IR → DD ops) | 19/19 | ✅ 완료 |
| Logic Fusion (FILTER+PROJECT → FLATMAP) | 14/14 | ✅ 완료 |
| FFI Marshalling (C ↔ Rust boundary) | 27/27 | ✅ 완료 |
| DD Execute (종단간 C→Rust, 인라인 사실) | 11/11 | ✅ 완료 |
| CLI Driver (`wirelog` 실행 파일) | 8/8 | ✅ 완료 |
| **C 합계** | **250 tests passing** | |
| Rust DD Executor (wirelog-dd 크레이트) | 90/90 | ✅ 완료 |
| **전체 합계** | **340 tests passing** | |

---

## 주요 모듈 설명

### Logic Layer (C11)

- `parser/`: Datalog → AST (hand-written RDP, 47 lexer + 44 parser 테스트)
- `ir/`: IR 표현 (8가지 노드 타입), 프로그램 메타데이터, Stratification
- `passes/`: 최적화 패스 (Logic Fusion 완료; JPP, SIP, Subplan Sharing 계획)

### DD Translator & FFI Layer (C11 ↔ Rust)

- `ffi/dd_plan.c`: IR → DD operator graph 변환 (19 테스트)
- `ffi/dd_marshal.c`: FFI-safe 타입으로 plan 마샬링 (27 테스트)
- `ffi/facts_loader.c`: Rust FFI를 통한 EDB 사실 일괄 로딩

### CLI Driver

- `cli/driver.c`: `wl_read_file()`, `wl_print_tuple()`, `wl_run_pipeline()`
- `cli/main.c`: 진입점 (`--workers N`, `--help` 플래그)
- `wirelog-cli`로 빌드, `wirelog`로 설치

### Rust DD Executor (`rust/wirelog-dd`)

- FFI 진입점: `wl_dd_worker_create/destroy`, `wl_dd_load_edb`, `wl_dd_execute_cb`
- 인터프리터 기반 실행: 비재귀 + 재귀(고정점) stratum
- 90/90 Rust 테스트 통과 (clippy clean, rustfmt clean)

---

## 참고 자료

**wirelog 프로젝트**:
- 프로젝트 URL: https://github.com/justinjoy/wirelog
- FlowLog 논문: `discussion/papers/2511.00865v4.pdf`

**외부 프로젝트**:
- [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow)
- [nanoarrow](https://github.com/apache/arrow-nanoarrow) (추후 사용)
- [Meson Build System](https://mesonbuild.com/)

---

## 문서 업데이트 이력

| 날짜 | 변경 |
|------|------|
| 2026-02-22 | 초안 작성 |
| 2026-02-24 | Phase 0 구현 완료 현황 반영 (186 tests) |
| 2026-02-27 | 340 tests 반영 (250 C + 90 Rust); CLI driver, 인라인 사실 추출, 종단간 실행 추가 |

---

**Last Updated**: 2026-02-27
