# Wirelog 아키텍처 - 영구적 설계

**마지막 업데이트:** 2026-04-13  
**검증:** 아키텍트 & 비평가 리뷰 완료

---

## 개요

Wirelog는 **순수 C11**로 구현된 **Timely-Differential 개념** 기반의 Datalog 엔진으로, **Columnar 저장소**와 **K-Fusion 병렬화**, 그리고 **플러그인 가능한 Backend 추상화**를 사용합니다.

이 문서는 모든 단계와 구현에서 보존되어야 하는 **불변 아키텍처 원칙**을 설명합니다. 단계별 구현 세부사항(별도 로드맵 문서에 있을 수 있음)과 의도적으로 분리되어 있습니다.

---

## 핵심 설계 철학

1. **임베디드-엔터프라이즈 이중성**: 단일 코드베이스가 자원 제약이 있는 임베디드 시스템과 고성능 엔터프라이즈 서버를 모두 지원합니다.
2. **FPGA 준비 완료된 경량 설계**: 무거운 의존성 없음 (LLVM, CUDA, MPI 불필요); Arrow IPC가 미래 하드웨어 가속을 가능하게 합니다.
3. **Backend 플러그인 가능성**: Compute backend는 추상화를 통해 교체 가능; 새로운 backend (FPGA, GPU)를 핵심 로직 수정 없이 추가할 수 있습니다.
4. **순수 C11 기반**: 런타임 없음, GC 없음, 최소한의 외부 의존성

---

## 5가지 불변 원칙

### 1. Timely-Differential 기반

**원칙**: Wirelog는 Timely-Differential 데이터플로우의 핵심 개념을 C11에서 네이티브로 구현합니다.

**의미**:
- **Lattice 타임스탐프**: 각 반복/에포크는 부분 순서 격자 `(outer_epoch, iteration, worker)`로 표현되며 semilattice 조인 연산을 지원합니다.
- **Z-set 의미론**: 사실은 부호있는 중복도를 가집니다 (+1 삽입, -1 제거) 정확한 증분 계산을 가능하게 합니다.
- **Differential arrangement**: 관계는 증분 스킵 최적화를 위한 델타 추적으로 인덱싱됩니다.
- **Frontier 추적**: Stratum별 및 규칙별 진행도 frontier는 증분 평가를 가능하게 합니다 (불필요한 반복 스킵).
- **Möbius 델타 공식**: 가중된 조인은 중복도 곱셈과 Möbius 역변환을 사용합니다.

**이유**: Timely-Differential은 증분 datalog 평가에 대한 검증된 정확성 보장을 제공합니다. C에서 이러한 개념을 다시 구현하면 임베디드 호환성을 보장하면서 계산 모델을 유지합니다.

**코드 위치**:
- Lattice 타임스탐프: `wirelog/columnar/diff_trace.h` (col_diff_trace_t)
- Z-set 중복도: `wirelog/columnar/columnar_nanoarrow.h:162-174` (col_delta_timestamp_t)
- Differential arrangement: `wirelog/columnar/diff_arrangement.h`
- Frontier 추적: `wirelog/columnar/frontier.h`, `wirelog/columnar/progress.h`
- Möbius 공식: `wirelog/columnar/mobius.c`

---

### 2. 순수 C11 언어

**원칙**: 모든 구현은 표준 C11 (ISO/IEC 9899:2011)로 작성되어야 합니다.

**의미**:
- Rust, C++, Python 또는 다른 언어가 핵심 코드베이스에 없어야 합니다.
- FFI 경계(외부 함수 인터페이스)가 없어야 합니다.
- C11 표준 기능은 허용됩니다: `_Static_assert`, `stdatomic.h`, designated initializers 등

**현재 상태 (2026-04-13)**:
- ✅ 핵심 엔진: 순수 C11
- ⚠️ POSIX 의존성: `pthread` (C11 `<threads.h>` 아님), `qsort_r` (표준 `qsort` 아님)
- ⚠️ 플랫폼 인트린직: `__builtin_ctzll`, `__builtin_prefetch` (GCC/Clang 특정)

**계획된 제거**:
- POSIX pthread → C11 `<threads.h>`로 마이그레이션 (컴파일러 지원 성숙 시)
- `_GNU_SOURCE` / `qsort_r` → 표준 C11 포터블 정렬로 구현
- `__builtin_*` → 포터블 C11 동등물로 폴백

**코드 위치**:
- 빌드 강제: `meson.build:49` (`-std=c11`)
- POSIX 추상화: `wirelog/thread.h`, `wirelog/thread_posix.c`, `wirelog/thread_msvc.c`
- qsort 호환성: `wirelog/columnar/internal.h:999-1115`

---

### 3. Columnar 저장소 기반

**원칙**: 모든 관계 데이터는 열 우선(columnar) 형식으로 저장됩니다.

**의미**:
- 관계는 `col_rel_t`로 표현됩니다: 열 배열, 각 열은 `int64_t[]` 배열입니다.
- 레이아웃: `columns[col_index][row_index]` — 열 우선 접근 패턴
- Arrow 스키마 메타데이터: 각 관계는 타입 정보를 위한 `ArrowSchema`를 가집니다 (nanoarrow 경유).
- 핫 경로에서는 행 우선 저장소가 없습니다 (정렬된 복사본이나 조인 캐시 같은 보조 인덱스에만 행 우선 구조가 존재합니다).

**이유**:
- **SIMD 친화적**: 열 우선은 동질 데이터 타입에 대한 벡터화 연산을 가능하게 합니다.
- **캐시 효율성**: 순차적 열 접근은 L1/L2 히트율을 개선합니다.
- **Arrow 생태계**: Apache Arrow columnar 형식으로 직접 매핑되어 미래 IPC 통합을 가능하게 합니다 (FPGA, 외부 시스템).

**코드 위치**:
- 관계 구조: `wirelog/columnar/internal.h:183-266` (col_rel_t with columns[col][row])
- ArrowSchema 통합: `wirelog/columnar/relation.c:213-227`
- 연산자 구현: `wirelog/columnar/ops.c` (VARIABLE/MAP/FILTER/JOIN이 columnar 버퍼에서 동작)

---

### 4. K-Fusion 병렬화

**원칙**: 다중 방향 semi-naive 평가는 K-Fusion을 통해 병렬화됩니다. 재귀 관계가 규칙 본문에 K ≥ 2번 나타날 때, backend는 K개의 독립적인 병렬 평가 경로를 생성합니다.

**의미**:
- `Path(x,y) :- Edge(x,z), Path(z,y)` 규칙의 경우 (K=1 자기 루프): 표준 semi-naive
- `Result(x,y) :- Path1(x,z), Path2(z,y)` 규칙의 경우 (K=2 다중 방향): 2개의 독립적 연산자 시퀀스를 생성하고 워크큐에 디스패치합니다.
- 각 워커는 differential arrangement의 격리된 복사본을 가집니다 (워커 간 동기화 없음).
- 모든 K개 워커가 완료된 후 결과를 병합합니다.

**이유**:
- 전역 동기화 없이 stratum 수준의 병렬성을 노출합니다.
- 깊은 복사 격리는 정확성을 단순화합니다 (K-fusion 워커에 락이 필요 없음).
- 워크큐 기반 병렬 실행에 자연스러운 적합성

**코드 위치**:
- 계획 수준: `wirelog/exec_plan.h:259` (WL_PLAN_OP_K_FUSION 연산자 타입)
- 계획 생성: `wirelog/exec_plan_gen.c:1574-1746` (expand_multiway_k_fusion)
- 실행: `wirelog/columnar/internal.h:1257` (col_op_k_fusion)
- 워커 격리: `wirelog/columnar/diff_arrangement.h:88-98` (col_diff_arrangement_deep_copy)

---

### 5. Backend 추상화 (플러그인 가능)

**원칙**: Compute backend는 깔끔한 vtable 추상화를 통해 교체 가능합니다. 핵심 엔진 (파서, 옵티마이저, 계획 생성)은 backend에 독립적입니다.

**의미**:
- `wirelog/backend.h:85-107`의 `wl_compute_backend_t` vtable은 7개의 연산을 정의합니다: `session_create`, `session_destroy`, `session_insert`, `session_remove`, `session_step`, `session_set_delta_cb`, `session_snapshot`
- 각 backend는 이 인터페이스의 구현을 제공합니다.
- `wl_session_t`는 backend vtable에 대한 포인터만 포함하며; 모든 디스패치는 다형적입니다.
- 미래 backend (FPGA, GPU, 분산)는 핵심 엔진 수정 없이 추가할 수 있습니다.

**현재 상태**:
- ✅ Columnar backend 구현됨 (`wl_backend_columnar()`)
- ⚠️ 하나의 backend만 존재; 추상화가 여러 backend에서 미검증
- ⚠️ Backend 특정 연산자 (K_FUSION, LFTJ, EXCHANGE)가 shared `exec_plan.h` enum에 노출됨

**계획된 개선**:
- 보편적 연산자 (0-8)를 backend 특정 연산자 (9+)로부터 분리
- Arrow IPC를 사용하는 FPGA backend 추가

**코드 위치**:
- Vtable: `wirelog/backend.h:85-107` (wl_compute_backend_t)
- 디스패치 계층: `wirelog/session.c` (순수 vtable 위임)
- 임베딩 계약: `wirelog/columnar/columnar_nanoarrow.h:140-149` (C11 section 6.7.2.1 캐스팅)
- 싱글톤: `wirelog/session.c:1921-1936` (wl_backend_columnar)

---

## 아키텍처 계층

```
┌─────────────────────────────────────────┐
│ Application API (wirelog.h)             │
│ - wirelog_program_load()                │
│ - wirelog_program_eval()                │
│ - wirelog_get_facts()                   │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│ Logic Layer (C11)                       │
│ - Parser (hand-written RDP)             │
│ - IR (Intermediate Representation)      │
│ - Stratification (SCC detection)        │
│ - Symbol interning (string dedup)       │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│ Optimizer (C11)                         │
│ - Fusion (FILTER+PROJECT → FLATMAP)    │
│ - JPP (join reordering)                │
│ - SIP (semijoin pre-filtering)         │
│ - Magic Sets, Subsumption              │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│ Plan Generation (C11)                   │
│ - IR → Execution plan translation      │
│ - K-Fusion expansion                   │
│ - Multi-way delta expansion            │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│ Backend Abstraction Vtable              │
│ - wl_compute_backend_t interface       │
└──────────────┬──────────────────────────┘
               │
      ┌────────┴────────────┐
      │                     │
┌─────▼──────────┐  ┌──────▼──────────┐
│ Columnar       │  │ FPGA Backend    │
│ Backend (C11)  │  │ (Planned)       │
│ + nanoarrow    │  │ Arrow IPC       │
└────────────────┘  └─────────────────┘
```

---

## 설계 결정사항

### Timely-Differential을 선택한 이유 (semi-naive 대신)?

Timely-Differential은 다음을 제공합니다:
- 파생 순서의 임의 우선순위에 대한 정확한 증분 평가
- Frontier 기반 스킵 최적화 (불필요한 반복 회피)
- 부정 및 집계 정확성을 위한 중복도 추적
- 분산 평가로의 자연스러운 확장

### 순수 C11을 선택한 이유 (Rust 대신)?

- **임베디드 호환성**: C11은 베어 메탈, RTOS, 마이크로컨트롤러에서 실행됩니다.
- **런타임 없음**: 가비지 수집기 없음, 예측 가능한 메모리 동작
- **단일 툴체인**: GCC, Clang, MSVC 모두 FFI 없이 지원
- **최소 의존성**: nanoarrow와 xxHash 모두 C입니다.

### Columnar를 선택한 이유 (행 우선 대신)?

- **SIMD 가속화**: 동질 데이터 타입에 대한 벡터화 비교, 해시, 조인 연산
- **캐시 효율성**: 순차적 열 접근은 L1/L2 히트율을 개선합니다.
- **Arrow 생태계**: Apache Arrow columnar 형식으로 직접 매핑

### K-Fusion을 선택한 이유?

- **병렬성 노출**: 다중 방향 조인은 여러 독립적 실행 경로를 생성합니다.
- **동기화 회피**: 깊은 복사 격리는 워커 간 락을 제거합니다.
- **확장성**: 알고리즘 변경 없이 임의의 K에서 작동합니다.

### Backend 추상화를 선택한 이유?

- **확장성**: FPGA, GPU, 분산 backend를 나중에 추가할 수 있습니다.
- **관심사의 분리**: 엔진이 계산 대상을 알 필요가 없습니다.
- **테스팅**: 단위 테스트를 위해 모의 backend를 추가할 수 있습니다.

---

## 참고자료

- **검증 보고서**: 아키텍트 & 비평가 분석 (2026-04-13)
- **Timely-Differential 논문**: McCLeland et al., SIGMOD 2013
- **Apache Arrow 형식**: https://arrow.apache.org/docs/format/
- **nanoarrow**: https://github.com/apache/arrow-nanoarrow
