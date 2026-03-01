# wirelog 아키텍처 설계 문서

**프로젝트**: wirelog - Embedded-to-Enterprise Datalog Engine
**Copyright**: Copyright (C) CleverPlant
**작성일**: 2026-03-01
**상태**: Phase 0 완료, Phase 1 완료

---

## 핵심 요구사항

1. **멀티 타겟 (동일 시작)**: 임베디드 + 엔터프라이즈 **모두 DD와 연결된 상태에서 시작**
2. **FPGA 가속 준비**: 무거운 라이브러리 없이 미래에 FPGA 가속으로 연산 오프로드 가능
3. **철저한 레이어링**: 계층 분리로 향후 최적화 유연성 확보
4. **nanoarrow 후순위**: 초기에는 불필요하며, 임베디드 최적화 단계에서 추가
5. **C11 기반**: C11 사용 (넓은 호환성 + _Static_assert, stdatomic 등 현대적 기능)

---

## 1. 핵심 설계 원칙

### 1.1 멀티 타겟 아키텍처 (Embedded ↔ Enterprise)

**Phase 0-1 (현재): 모두 DD 기반**
```
wirelog 코어 (C11)
├─ Parser (Datalog → IR)
├─ Optimizer (Fusion, JPP, SIP)
└─ DD Executor (Rust FFI)
    │
    ├─ [Embedded Target]
    │   ├─ ARM/RISC-V CPU 타겟
    │   ├─ 싱글 워커 또는 로컬 멀티 스레드
    │   └─ 메모리 제약 (<256MB)
    │
    └─ [Enterprise Target]
        ├─ x86-64 서버
        ├─ 멀티 워커, 분산 처리
        └─ 메모리 풍부 (GB 단위)
```

**중기 (Phase 4+): 선택적 최적화**
```
wirelog 코어 (C11)
    └─ Backend Abstraction (선택)
        │
        ├─ [Embedded Path]
        │   ├─ nanoarrow 메모리 (columnar, 선택적)
        │   ├─ Semi-naive executor (C11)
        │   └─ 500KB-2MB 독립 바이너리
        │
        ├─ [Enterprise Path]
        │   └─ DD 유지 (변경 없음)
        │
        └─ [FPGA Path] (미래)
            ├─ 추상화된 연산 커널
            ├─ Hardware 오프로드
            └─ Arrow IPC data transfer
```

### 1.2 FPGA 가속 준비 원칙

**무거운 라이브러리를 피하는 이유**:
- LLVM (30M LOC) → FFI 비용 증가, FPGA 연동 복잡
- CUDA/OpenCL → 특정 하드웨어 종속성
- MPI → 분산 처리는 DD에 위임

**대신 경량 설계**:
- 추상화된 연산 인터페이스 (ComputeBackend)
- Arrow IPC를 통한 데이터 전송
- Backend 구현체는 선택적 (CPU, FPGA, GPU)

### 1.3 철저한 레이어링

```
[Application Layer]
  wirelog public API (.h)
    │
[Logic Layer]
  Parser → IR → Optimizer
    │
[Execution Interface]
  Backend abstraction (backend.h)
    │
    ├─ [DD Backend]       ├─ [CPU Backend]    ├─ [FPGA Backend]
    │  Rust FFI           │  nanoarrow        │  Arrow IPC
    │  (초기)             │  (중기)           │  (후기)
    │
[Memory Layer]
  ArrowBuffer / malloc / custom allocator
    │
[I/O Layer]
  CSV, Arrow IPC, network sockets
```

### 1.4 Differential Dataflow 연결

**Phase 0-1: DD 기반 구현**
```
wirelog (C11 parser/optimizer)
    ↓ (IR → DD operator graph 변환)
Differential Dataflow (Rust executor, 독립)
    ↓
Result
```

**이점**:
- 입증된 성능 (Differential Dataflow의 증분 처리)
- DD의 멀티 워커, 분산 처리 즉시 활용
- wirelog는 파서/최적화만 C11로 구현
- 임베디드 및 엔터프라이즈가 동일한 기반에서 시작
- 추후 임베디드만 선택적으로 nanoarrow로 마이그레이션 가능

**실행 경로** (모든 환경):
```
wirelog (C11 parser/optimizer)
    ↓
IR → Fusion → JPP → SIP → DD operator graph
    ↓
Differential Dataflow (Rust executor)
    ↓
Result

• 임베디드 환경: DD 단일 워커 모드, 로컬 메모리
• 엔터프라이즈: DD 멀티 워커, 분산 처리
• 동일한 코드베이스, 타겟별로만 빌드 설정 다름
```

**선택적 최적화 경로** (Phase 4+):
```
임베디드 환경만 (선택사항):
  wirelog (C11 parser/optimizer)
      ↓
  nanoarrow executor (C11, 완전 독립)
      ↓
  Result (500KB-2MB 바이너리)

엔터프라이즈 환경:
  (DD 경로 유지, 변경 없음)

FPGA 가속 (미래):
  wirelog (C11 parser/optimizer)
      ↓
  ComputeBackend abstraction
      ↓
  [CPU executor] 또는 [FPGA via Arrow IPC]
```

---

## 2. 아키텍처 레이어 설계

### 2.1 계층 구조 (Phase 0-1: 모두 DD 기반)

```
┌─────────────────────────────────────────────────────┐
│ Application API (wirelog.h)                         │
│ - wirelog_parse()                                   │
│ - wirelog_optimize()                                │
│ - wirelog_evaluate()                                │
│ - wirelog_get_result()                              │
└──────────────────┬──────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────┐
│ Logic Layer (wirelog core) - C11                    │
│ - Parser (hand-written RDP, Datalog → AST)         │
│ - IR Representation (backend-agnostic structs)      │
│ - Optimizer (Fusion, JPP, SIP)                     │
│ - Stratifier (SCC detection, topological sort)     │
└──────────────────┬──────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────┐
│ DD Translator (C11 ↔ Rust FFI)                      │
│ - IR → DD operator graph conversion                 │
│ - Result collection from DD runtime                 │
│ - Data marshalling                                  │
└──────────────────┬──────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────┐
│ Differential Dataflow (Rust, Timely)                │
│ - Multi-worker execution                            │
│ - Incremental computation                           │
│ - Distributed processing (enterprise)               │
│ - Single-worker mode (embedded)                     │
└──────────────────────────────────────────────────────┘

[I/O Layer]
  CSV, JSON, Arrow IPC (추후)
```

### 2.1b 계층 구조 (Phase 3+: 선택적 임베디드 최적화)

```
wirelog core (C11)
    ├─ [Enterprise: DD 유지]
    │   └─ Differential Dataflow (변경 없음)
    │
    └─ [Embedded: 선택적 마이그레이션]
        └─ ComputeBackend abstraction
            ├─ nanoarrow executor (C11)
            └─ (미래) FPGA backend via Arrow IPC
```

### 2.2 각 레이어의 책임

#### Logic Layer (wirelog 핵심, C11)

**책임**:
- Datalog 프로그램을 파싱하여 AST 생성
- AST → IR 변환 (backend-agnostic)
- IR 레벨 최적화 passes (Fusion, JPP, SIP)
- Tarjan's iterative SCC 검출을 통한 stratification
- DD와 독립적 설계

**현재 상태**:
- Parser (hand-written RDP, FlowLog-compatible grammar, 96 tests)
- IR 표현 (9가지 노드 타입: FLATMAP, SEMIJOIN 포함, AST→IR 변환, UNION merge, 61 tests)
- Stratification & SCC 감지 (Tarjan's iterative, negation 검증, 20 tests)
- 심볼 인터닝 (`wl_intern_t`, 문자열 → 순차 i64 ID, 9 tests)
- CSV 입력 지원 (`.input` 지시자, 구분자 설정, 17 tests)
- Logic Fusion: FILTER+PROJECT → FLATMAP (in-place mutation, 14 tests)
- JPP: Join-Project Plan (3+ 원자 체인에 대한 greedy 조인 재배치, 13 tests)
- SIP: Semijoin Information Passing (조인 체인에 사전 필터 삽입, 9 tests)
- CLI driver (`wirelog` 실행 파일, .dl 파일 실행, `--workers` 플래그, 15 tests)

#### DD Translator & FFI 레이어 (C11 ↔ Rust FFI)

**책임**:
- IR → DD 실행 계획 변환 (전체 9가지 IR 노드 타입)
- FFI-safe 플랫 구조체로 plan 마샬링
- RPN 수식 직렬화 (IR expr 트리 → 바이트 버퍼)
- Rust FFI를 통한 EDB 사실 일괄 로딩
- 메모리 소유권: C가 할당/해제, Rust는 const 포인터로 빌림

**현재 상태**:
- DD 실행 계획 데이터 구조 (`wl_dd_plan_t`, `wl_dd_stratum_plan_t`, `wl_dd_relation_plan_t`, `wl_dd_op_t`)
- 9가지 DD 연산자 타입: VARIABLE, MAP, FILTER, JOIN, ANTIJOIN, REDUCE, CONCAT, CONSOLIDATE, SEMIJOIN
- Stratum 기반 plan 생성 (재귀 stratum 감지 포함)
- FFI-safe 타입 정의 및 RPN 수식 직렬화
- 상수가 포함된 ANTIJOIN 지원 (우측 필터, 키 인덱스)
- 22 DD plan tests + 31 FFI marshalling tests

#### Rust DD Executor (`wirelog-dd` 크레이트)

**현재 상태**:
- `staticlib` 크레이트: `#[no_mangle] extern "C"` FFI 진입점
- `repr(C)` 타입 미러 — `dd_ffi.h` 레이아웃 일치
- FFI 진입점: `wl_dd_worker_create/destroy`, `wl_dd_load_edb`, `wl_dd_execute_cb`
- RPN 수식 역직렬화 + 스택 기반 평가기 (i64 전용)
- FFI plan 리더: unsafe C 포인터 → safe owned Rust 타입
- 비재귀 stratum 실행: `timely::execute()` 사용
- 재귀 stratum 실행: DD `iterate()` + `distinct()` 사용
- 전체 9가지 연산자 타입: Variable, Map, Filter, Join, Antijoin, Reduce, Concat, Consolidate, Semijoin
- Meson-Cargo 빌드 통합 (`-Ddd=true`, clippy/fmt/test 타겟)
- 85 Rust tests passing (clippy clean, rustfmt clean)

**변환 규칙** (IR 노드 → DD 연산자):
```
SCAN      → WL_DD_VARIABLE   (입력 컬렉션 참조)
PROJECT   → WL_DD_MAP        (컬럼 프로젝션)
FILTER    → WL_DD_FILTER     (predicate 필터, deep-copy된 expr)
JOIN      → WL_DD_JOIN       (키 컬럼 기반 equijoin)
ANTIJOIN  → WL_DD_ANTIJOIN   (negation, right relation + 선택적 필터)
SEMIJOIN  → WL_DD_SEMIJOIN   (semijoin 사전 필터)
AGGREGATE → WL_DD_REDUCE     (group-by + 집계 함수)
UNION     → WL_DD_CONCAT + WL_DD_CONSOLIDATE (union + 중복 제거)
FLATMAP   → WL_DD_FILTER + WL_DD_MAP  (fused filter+project)
```

**설계 결정 사항**:
- DD op의 모든 포인터 필드는 소유(deep copy), `wl_dd_plan_free()`로 해제
- 에러 반환: `int` (0 = 성공, -1 = 메모리, -2 = 잘못된 입력) + out-parameter
- FFI 바운더리: 복사 기반 마샬링, C가 모든 메모리 소유, Rust는 const 포인터로 빌림
- 수식 트리를 RPN 바이트 버퍼로 직렬화 (FFI를 통한 포인터 트리 전달 방지)
- FFI 타입은 고정 폭 정수와 명시적 enum 값 사용 (ABI 안정성)

#### I/O Layer

**책임**:
- .dl 파일 읽기 → 파싱 → 최적화 → 전체 파이프라인 실행
- `.input` 지시자를 통한 CSV 입력 (쉼표/탭 구분자)
- `.output` 지시자를 통한 선택적 결과 출력 필터링
- 결과를 튜플 형태로 출력 (예: `tc(1, 2)`, `tc(2, 3)`)
- `wirelog-cli`로 빌드 (빌드 디렉터리 충돌 방지), `wirelog`로 설치

---

### 2.3 향후 계층 구조 (Phase 3+: 선택적 임베디드 최적화)

**이때 추가되는 레이어** (계획):

#### ComputeBackend Abstraction (C11)

```c
typedef struct {
    void (*join)(...);
    void (*project)(...);
    void (*filter)(...);
    void (*union_rel)(...);
    void (*dedup)(...);
    // ...
} ComputeBackend;
```

#### nanoarrow Executor (C11, 선택)

- Sort-merge join on columnar data
- Semi-naive delta propagation
- 메모리 최적화
- 참고: nanoarrow 마이그레이션 시 DD에서 wontfix로 결정된 C 레벨 최적화 passes (Subplan Sharing, Boolean Specialization)를 재검토해야 함 — [Discussion #63](https://github.com/justinjoy/wirelog/discussions/63) 참조

#### FPGA Backend (미래)

- Arrow IPC를 통한 데이터 전송
- 하드웨어 연산 오프로드

---

## 3. 개발 로드맵

### Phase 0: 기초 — 완료

**목표**: C11 파서/최적화 + DD 변환기 + 종단간 실행

**주요 산출물**:
- Hand-written RDP 파서 (FlowLog-compatible grammar)
- 트리 기반 IR (9가지 노드 타입) 및 AST→IR 변환
- Tarjan's iterative SCC 검출을 통한 stratification
- IR → DD operator graph 변환 (9가지 연산자 타입)
- FFI 마샬링 레이어 (RPN 수식 직렬화)
- Rust DD executor 크레이트 (Differential Dataflow dogs3 v0.19.1)
- CLI driver: .dl 파일 실행, `--workers` 플래그
- `.input` 지시자를 위한 CSV 입력 지원

### Phase 1: 최적화 — 완료

**목표**: IR 수준 최적화 passes + 포괄적 벤치마크 스위트

**최적화 passes**:
- Logic Fusion (FILTER+PROJECT → FLATMAP, in-place mutation)
- JPP — Join-Project Plan (3+ 원자 체인에 대한 greedy 조인 재배치)
- SIP — Semijoin Information Passing (조인 체인에 사전 필터 삽입)
- ~~Subplan Sharing~~ — wontfix로 종료 ([#61](https://github.com/justinjoy/wirelog/issues/61)): DOOP 프로파일링 결과 +1.9% 느려짐; DD가 이미 경량 Variable 핸들로 컬렉션을 공유
- ~~Boolean Specialization~~ — wontfix로 종료 ([#62](https://github.com/justinjoy/wirelog/issues/62)): DD의 `join_map`과 `semijoin`이 단항 관계에 동일한 `join_core` 사용; 비용 차이 거의 없음

**벤치마크 스위트** (15개 워크로드):

| 카테고리 | 워크로드 |
|----------|----------|
| 그래프 | TC, Reach, CC, SSSP, SG, Bipartite |
| 포인터 분석 | Andersen, CSPA, CSDA, Dyck-2 |
| 고급 | Galen (8 규칙), Polonius (37 규칙, 1487 이터레이션), CRDT (23 규칙), DDISASM (28 규칙), DOOP (136 규칙, 8-way 조인) |

**테스트 수**: C 325개 (14 스위트) + Rust 85개 = **총 410개 테스트 통과**

### Phase 2: 문서화 (계획)

**목표**: 사용자 대상 문서 작성 (도입 및 온보딩)

- **Language Reference** — 지원 문법, 타입, 연산자, 지시자 레퍼런스
- **Tutorial** — 첫 프로그램부터 재귀, 부정, 집계까지 단계별 가이드
- **Examples** — 벤치마크가 아닌 학습용 예제 (`examples/` 디렉토리)
- **CLI Usage** — `wirelog --help`, man page 수준의 CLI 문서

### Phase 3: nanoarrow 백엔드 (계획)

**목표**: 임베디드 타겟용 DD 대체 C11 네이티브 실행 엔진

- DD 기반 성능 프로파일링 (15개 벤치마크: 실행 시간, 메모리, 병목)
- Backend 추상화 인터페이스 설계
- nanoarrow executor 구현 (sort-merge join, semi-naive delta propagation)
- DD vs nanoarrow 동일 벤치마크 비교
- Subplan Sharing ([#61](https://github.com/justinjoy/wirelog/issues/61)) 및 Boolean Specialization ([#62](https://github.com/justinjoy/wirelog/issues/62)) 재검토 — [Discussion #63](https://github.com/justinjoy/wirelog/discussions/63) 참조
- 바이너리 최소화 (LTO, -Os, strip)

### FPGA 지원 (Phase 3 완료 후 재평가)

FPGA 가속은 nanoarrow executor가 전제 조건이다. DD의 내부 arrangement 공유와 증분 연산은 하드웨어에 직접 매핑할 수 없는 소프트웨어 수준 최적화다. Phase 3 완료 후 실현 가능성을 재평가한다.

---

## 4. 기술 스택

| 계층 | 선택 | 상태 | 근거 |
|------|------|------|------|
| **언어** | C11 | 확정 | 최소 의존성, 임베디드 친화, 호환성 |
| **빌드** | Meson + Ninja | 확정 | Cross-compile 우수, 경량 |
| **Parser** | Hand-written RDP | 구현됨 | Zero deps, FlowLog-compatible grammar |
| **IR** | Tree-based (9 node types) | 구현됨 | AST→IR, UNION merge, FLATMAP, SEMIJOIN |
| **Stratification** | Tarjan's SCC | 구현됨 | O(V+E), iterative |
| **Optimizer** | Fusion + JPP + SIP | 구현됨 | 3 passes, in-place IR mutation |
| **DD Plan** | IR → DD op graph | 구현됨 | 9 op types, stratum-aware |
| **FFI 마샬링** | DD plan → FFI-safe 타입 | 구현됨 | RPN 수식 직렬화 |
| **Rust DD Executor** | wirelog-dd 크레이트 | 구현됨 | DD dogs3 v0.19.1, 85 Rust tests |
| **빌드 통합** | Meson + Cargo | 구현됨 | `-Ddd=true`, clippy/fmt/test 타겟 |
| **CLI Driver** | wirelog-cli 바이너리 | 구현됨 | .dl 실행, `--workers` 플래그 |
| **벤치마크** | 15 워크로드 | 구현됨 | 그래프, 포인터 분석, 프로그램 분석 |
| **메모리** | nanoarrow (중기) | 계획 | Columnar, Arrow interop |
| **Allocator** | Region/Arena + system malloc | 계획 | [Discussion #58](https://github.com/justinjoy/wirelog/discussions/58) 참조 |
| **I/O** | CSV + Arrow IPC | CSV 구현됨, Arrow 계획 | 표준 포맷 |

---

## 5. 미결정 설계 사항

### IR 및 최적화
- [x] 최적화 pass 순서 (Fusion → JPP → SIP)
- [x] 조인 순서 전략 (greedy heuristic, 공유 변수 최대화)
- [ ] Cost model 정확도 vs 성능 trade-off
- [ ] IR 표현 형식 탐구 (tree vs DAG vs SSA)

### Memory 관리
- [ ] Region/Arena allocator 설계 (할당 패턴 안정화 후)
- [ ] 할당 카테고리 분리: `WL_ALLOC_INTERNAL` (AST/IR) vs `WL_ALLOC_FFI_TRANSFER` (DD 경계)
- [ ] 메모리 누수 감지 전략

### Backend 추상화
- [ ] RelationBuffer와 Arrow schema의 관계
- [ ] Backend 간 데이터 변환 비용
- [ ] 에러 처리 방식

### 성능 목표
- [ ] 임베디드 vs 엔터프라이즈별 성능 목표
- [ ] 메모리 사용량 제약
- [ ] 배포 바이너리 크기 목표

### nanoarrow 마이그레이션 (Phase 3)
- [ ] Subplan Sharing (#61) 및 Boolean Specialization (#62) 재검토 — [Discussion #63](https://github.com/justinjoy/wirelog/discussions/63) 참조
- [ ] Arrow 컬럼형 비용 모델은 DD와 다름: CSE 및 set-membership 필터가 가치 있어짐
- [ ] FPGA 실현 가능성 평가 (nanoarrow executor 완성 후 재평가)

---

## 6. 참고 자료

**wirelog 프로젝트 문서**:
- 프로젝트 URL: https://github.com/justinjoy/wirelog
- FlowLog 논문 (참고용): PVLDB 2025, "FlowLog: Efficient and Extensible Datalog via Incrementality"
- nanoarrow 마이그레이션 분석: [Discussion #63](https://github.com/justinjoy/wirelog/discussions/63)
- Allocator ADR: [Discussion #58](https://github.com/justinjoy/wirelog/discussions/58)

**외부 프로젝트**:
- Differential Dataflow: https://github.com/TimelyDataflow/differential-dataflow
- nanoarrow: https://github.com/apache/arrow-nanoarrow (추후 사용)
- Arrow format: https://arrow.apache.org/docs/format/ (추후 사용)

---

## 7. 문서 업데이트 이력

| 날짜 | 버전 | 변경 |
|------|------|------|
| 2026-02-22 | 0.1 | 초안 작성, 레이어링 정의 |
| 2026-02-22 | 0.2 | Phase 0 parser 구현 상태 업데이트 |
| 2026-02-23 | 0.3 | Allocator ADR을 Discussion #58로 이동 |
| 2026-02-24 | 0.4 | IR 표현 완료; Stratification & SCC 완료 |
| 2026-02-24 | 0.5 | DD Plan Translator 완료 |
| 2026-02-24 | 0.6 | Phase 1 Logic Fusion 완료 |
| 2026-02-26 | 0.7 | FFI 마샬링 레이어 완료 |
| 2026-02-26 | 0.8 | Rust DD executor 크레이트 완료 |
| 2026-02-27 | 0.9 | 인라인 팩트 추출; CLI driver; 종단간 파이프라인 완료 |
| 2026-02-27 | 0.10 | 실제 DD 통합 (dogs3 v0.19.1) |
| 2026-02-28 | 0.11 | Rust 코드 최소화; CSV 입력 지원; 벤치마크 스위트 시작 |
| 2026-03-01 | 0.12 | **Phase 1 완료.** JPP, SIP 최적화 passes; 15개 벤치마크 (TC ~ DOOP); 상수 포함 ANTIJOIN 수정; Subplan Sharing (#61) 및 Boolean Specialization (#62) 프로파일링 후 wontfix 결정. 총 410개 테스트 (C 325 + Rust 85). 디렉토리 구조 목록 제거. |

