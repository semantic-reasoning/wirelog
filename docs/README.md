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

## 구현 현황 (Phase 0)

| 모듈 | 테스트 | 상태 |
|------|--------|------|
| Parser (hand-written RDP) | 91/91 (47 lexer + 44 parser) | ✅ 완료 |
| IR Representation (8 node types) | 56/56 (19 IR + 37 program) | ✅ 완료 |
| Stratification & SCC (Tarjan's) | 20/20 | ✅ 완료 |
| DD Plan Translator (IR → DD ops) | 19/19 | ✅ 완료 |
| **합계** | **186 tests passing** | |
| Rust FFI 통합 | — | 🔄 다음 단계 |
| 통합 테스트 | — | 🔄 계획 |

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

---

**Last Updated**: 2026-02-24
