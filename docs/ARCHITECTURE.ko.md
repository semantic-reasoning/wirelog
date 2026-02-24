# wirelog 아키텍처 설계 문서

**프로젝트**: wirelog - Embedded-to-Enterprise Datalog Engine
**Copyright**: Copyright (C) CleverPlant
**작성일**: 2026-02-22
**상태**: 🔄 설계 진행 중 (Phase 0 구현 중)

⚠️ **이 문서는 초안입니다.** 지속적으로 업데이트됩니다.

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

**초기 (Phase 0-3): 모두 DD 기반**
```
wirelog 코어 (C11)
├─ Parser (Datalog → IR)
├─ Optimizer (Logic Fusion, JPP, SIP, etc.)
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

### 1.4 초기 시작: Differential Dataflow 연결

**Phase 0-3: DD 기반 구현**
```
wirelog (C11 parser/optimizer)
    ↓ (IR를 DD operator graph로 변환)
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
초기 (Phase 0-3, Month 1-5):
  wirelog (C11 파서/최적화)
      ↓
  IR → DD operator graph (변환)
      ↓
  Differential Dataflow (Rust executor, FlowLog 기반)
      ↓
  Result

  • 임베디드 환경: DD 단일 워커 모드, 로컬 메모리
  • 엔터프라이즈: DD 멀티 워커, 분산 처리
  • 동일한 코드베이스, 타겟별로만 빌드 설정 다름
```

**선택적 최적화 경로** (Phase 4+):
```
임베디드 환경만 (선택사항):
  wirelog (C11 파서/최적화)
      ↓
  nanoarrow executor (C11, 완전 독립)
      ↓
  Result (500KB-2MB 바이너리)

엔터프라이즈 환경:
  (DD 경로 유지, 변경 없음)

FPGA 가속 (미래):
  wirelog (C11 파서/최적화)
      ↓
  ComputeBackend abstraction
      ↓
  [CPU executor] 또는 [FPGA via Arrow IPC]
```

---

## 2. 아키텍처 레이어 설계

### 2.1 계층 구조 (Phase 0-3: 모두 DD 기반)

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
│ - Optimizer (Logic Fusion, JPP, SIP, Subplan)      │
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

### 2.2 각 레이어의 책임 (Phase 0-3)

#### Logic Layer (wirelog 핵심, C11)

**파일 구조**:
```
wirelog/
  parser/
    lexer.c         # Tokenization
    parser.c        # Datalog → AST (hand-written RDP)
    ast.c           # AST node 관리
  ir/
    ir.c            # IR node 구성, expression clone
    program.c       # 프로그램 메타데이터, AST→IR 변환, UNION merge
    stratify.c      # Stratification, dependency graph, Tarjan's SCC
    dd_plan.h       # DD 실행 계획 타입 및 내부 API
    dd_plan.c       # IR → DD operator graph 변환
    api.c           # Public API 구현
  optimizer.c       # Optimizer orchestrator (계획)
  passes/
    fusion.h        # Logic Fusion 헤더 (내부 API)
    fusion.c        # Logic Fusion (FILTER+PROJECT → FLATMAP)
    jpp.c           # Join-Project Plan (계획)
    sip.c           # Semijoin Information Passing (계획)
    sharing.c       # Subplan Sharing (계획)
```

**책임**:
- Datalog 프로그램을 파싱하여 AST 생성
- AST → IR 변환 (backend-agnostic)
- IR 레벨 최적화 (알고리즘)
- DD와 독립적 설계

**Phase 0 구현 상태**:
- ✅ Parser 구현 (hand-written RDP, C11)
- ✅ 파서 테스트: 91/91 passing (47 lexer + 44 parser)
- ✅ Grammar: FlowLog-compatible (declarations, rules, negation, aggregation, arithmetic, comparisons, booleans, .plan marker)
- ✅ IR 표현 (8가지 노드 타입, AST→IR 변환, UNION merge)
- ✅ IR 테스트: 56/56 passing (19 IR + 37 program)
- ✅ Stratification & SCC 감지 (Tarjan's iterative, negation 검증)
- ✅ Stratification 테스트: 20/20 passing
- ✅ DD Plan Translator (IR → DD operator graph, 전체 8가지 IR 노드 타입)
- ✅ DD Plan 테스트: 19/19 passing
- 🔄 최적화 passes (Phase 1 진행 중)
- ✅ Logic Fusion: FILTER+PROJECT → FLATMAP (in-place mutation, 14/14 tests)

#### DD Translator (C11 ↔ Rust FFI)

**파일**:
```
wirelog/ir/
  dd_plan.h         # DD 실행 계획 타입 및 내부 API
  dd_plan.c         # IR → DD operator graph 변환
  (계획)
  dd_ffi.h          # FFI 정의 (Rust 경계)
  dd_marshal.c      # 데이터 변환 C ↔ Rust
```

**Phase 0 상태** (DD Plan — C 측 완료):
- ✅ DD 실행 계획 데이터 구조 (`wl_dd_plan_t`, `wl_dd_stratum_plan_t`, `wl_dd_relation_plan_t`, `wl_dd_op_t`)
- ✅ 8가지 DD 연산자 타입: VARIABLE, MAP, FILTER, JOIN, ANTIJOIN, REDUCE, CONCAT, CONSOLIDATE
- ✅ 전체 8가지 IR 노드 → DD 변환 (SCAN, PROJECT, FILTER, JOIN, ANTIJOIN, AGGREGATE, UNION, FLATMAP 보류)
- ✅ Stratum 기반 plan 생성 (EDB 수집, stratum별 relation plan)
- ✅ 재귀 stratum 감지 (`is_recursive` 플래그, DD `iterate()` 래핑용)
- ✅ Deep-copy 소유 의미론 (`wl_ir_expr_clone()`, filter expression용)
- ✅ 19/19 테스트 passing

**변환 규칙** (IR 노드 → DD 연산자):
```
SCAN      → WL_DD_VARIABLE   (입력 컬렉션 참조)
PROJECT   → WL_DD_MAP        (컬럼 프로젝션)
FILTER    → WL_DD_FILTER     (predicate 필터, deep-copy된 expr)
JOIN      → WL_DD_JOIN       (키 컬럼 기반 equijoin)
ANTIJOIN  → WL_DD_ANTIJOIN   (negation, right relation 포함)
AGGREGATE → WL_DD_REDUCE     (group-by + 집계 함수)
UNION     → WL_DD_CONCAT + WL_DD_CONSOLIDATE (union + 중복 제거)
FLATMAP   → WL_DD_FILTER + WL_DD_MAP  (fused filter+project)
```

**책임**:
- ✅ wirelog IR을 DD operator graph로 변환 (C 측 plan)
- 🔄 C ↔ Rust 데이터 마샬링 (계획)
- 🔄 DD 워커 관리 (계획)
- 🔄 결과 수집 및 변환 (계획)

**설계 결정 사항**:
- ✅ DD op의 모든 포인터 필드는 소유(deep copy), `wl_dd_plan_free()`로 해제
- ✅ 에러 반환: `int` (0 = 성공, -1 = 메모리, -2 = 잘못된 입력) + out-parameter
- ✅ FLATMAP 보류: 현재 IR이 별도의 FILTER/PROJECT/JOIN 노드를 생성
- [ ] FFI 바운더리 명확히 (Rust 경계에서 메모리 소유권)
- [ ] Data marshalling 전략 (zero-copy vs copy)
- [ ] Context 전달 방식

#### I/O Layer (Phase 0: 기본)

**파일** (계획):
```
src/
  io/
    csv.c           # CSV 입력 → DD collection
    output.c        # DD 결과 → 출력 (stdout, file)
```

**책임**:
- CSV 파일 읽기 → Datalog 사실(facts)
- 프로그램 실행 후 결과 출력
- (추후 Arrow IPC 추가)

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

#### FPGA Backend (미래)

- Arrow IPC를 통한 데이터 전송
- 하드웨어 연산 오프로드

---

## 3. 개발 로드맵

### Phase 0: 기초 (Weeks 1-4) - 모든 환경 DD 기반

**목표**: C11 파서/최적화 + DD 변환기로 동작하는 초기 버전

**구현 항목**:
- ✅ C11 파서 (Datalog → AST, hand-written RDP)
- ✅ 파서 테스트 (91/91 passing)
- ✅ FlowLog-compatible grammar 구현
- ✅ 빌드 시스템 (Meson, C11)
- ✅ IR 표현 (8가지 노드 타입, AST→IR, UNION merge)
- ✅ IR 테스트 (56/56 passing: 19 IR 노드 + 37 program)
- ✅ Stratification & SCC 감지 (iterative Tarjan's, O(V+E))
- ✅ Stratification 테스트 (20/20 passing)
- ✅ IR → DD operator graph 변환기 (전체 8가지 IR 노드 타입, 19/19 tests)
- 🔄 Rust FFI 통합
- 🔄 기본 통합 테스트

**검증**:
- [ ] 임베디드 타겟 (ARM cross-compile) 빌드 성공
- [ ] 엔터프라이즈 타겟 (x86-64) 빌드 성공
- [ ] 기본 Datalog 프로그램 실행 확인

**현재 상태**: Parser (91/91), IR (56/56), Stratification (20/20), DD Plan (19/19), Logic Fusion (14/14) 완료 — 200 tests passing. Phase 1 최적화 진행 중.

### Phase 1: 최적화 (Weeks 5-10) - 모든 환경 공통

**목표**: 논문 기반 최적화 기법을 IR 수준에서 구현 (FlowLog/Soufflé 참고)

**구현 항목** (계획):
- ✅ Logic Fusion (FILTER+PROJECT → FLATMAP, in-place mutation, 14/14 tests)
- [ ] Join-Project Plan (structural cost model, JST enumeration)
- [ ] SIP (Semijoin Information Passing)
- [ ] Subplan Sharing (hash-based CTE detection)
- [ ] Boolean Specialization (diff encoding)

**검증**:
- [ ] 최적화 비교 (최적화 on vs off)
- [ ] 자체 벤치마크 작성: Reach, CC, SSSP, TC 등
- [ ] 성능 측정

**추정**: 2500-3000 LOC, 6주

### Phase 2: 성능 기준선 (Weeks 11-14)

**목표**: 임베디드 vs 엔터프라이즈 성능 및 메모리 비교

**구현 항목** (계획):
- [ ] 광범위 벤치마킹 (모든 환경)
- [ ] 메모리 프로파일링 (임베디드 vs 엔터프라이즈)
- [ ] 병목 분석
- [ ] nanoarrow 마이그레이션 필요성 판단
- [ ] 문서화

**추정**: 4주

### Phase 3: 선택적 임베디드 최적화 (Month 4+)

**목표**: 임베디드 환경만 nanoarrow로 마이그레이션 (선택)

**구현 항목** (계획):
- [ ] Backend 추상화 인터페이스 설계
- [ ] nanoarrow executor 구현
- [ ] ComputeBackend 인터페이스 적응
- [ ] 리팩토링 & 테스트
- [ ] 바이너리 최소화 (LTO, -Os, strip)

**추정**: 1500-2000 LOC + 리팩토링, 4-6주

**결정 시점**: Phase 2 벤치마크 결과 후 필요 여부 판단

### Phase 4: FPGA 지원 (Month 6+)

**목표**: 무거운 연산을 FPGA로 오프로드 (선택)

**구현 항목** (계획):
- [ ] ComputeBackend를 FPGA로 확장
- [ ] Arrow IPC FPGA 통신
- [ ] 작업 스케줄링 & 오프로드
- [ ] 결과 수집

**추정**: TBD (FPGA 하드웨어 가용성에 따라 결정)

---

## 4. 기술 스택

| 계층 | 선택 | 상태 | 근거 |
|------|------|------|------|
| **언어** | C11 | ✅ 확정 | 최소 의존성, 임베디드 친화, 호환성 |
| **빌드** | Meson | ✅ 확정 | Cross-compile 우수, 경량 |
| **Parser** | Hand-written RDP | ✅ 구현됨 | Zero deps, 91/91 tests passing |
| **IR** | Tree-based (8 node types) | ✅ 구현됨 | AST→IR, UNION merge, 56/56 tests |
| **Stratification** | Tarjan's SCC | ✅ 구현됨 | O(V+E), iterative, 20/20 tests |
| **DD Plan** | IR → DD op graph | ✅ 구현됨 | 8 op types, stratum-aware, 19/19 tests |
| **메모리** | nanoarrow (중기) | 계획 | Columnar, Arrow interop |
| **Allocator** | Region/Arena + system malloc | 계획 (Phase 2) | jemalloc 검토 후 보류; §4.1 ADR 참조 |
| **Threading** | Optional pthreads | 계획 | Single-threaded 기본 |
| **I/O** | CSV + Arrow IPC | 계획 | 표준 포맷 |

---

## 5. 미결정 설계 사항 (TODO)

### Parser & 전처리
- [x] Datalog 확장 기능 범위 (negation, aggregation, constraints, etc.) - FlowLog grammar 구현됨
- [ ] 에러 메시지 전략
- [ ] Incremental 파싱 필요 여부

### IR 및 최적화
- [ ] IR 표현 형식 (tree vs DAG vs SSA)
- [ ] 최적화 pass 순서
- [ ] Cost model 정확도 vs 성능 trade-off
- [ ] Join ordering search space 크기 제한

### Memory 관리
- [ ] Region/Arena allocator 설계 (Phase 1 후반 ~ Phase 2, 할당 패턴 안정화 후)
- [ ] 할당 카테고리 분리: `WL_ALLOC_INTERNAL` (AST/IR) vs `WL_ALLOC_FFI_TRANSFER` (DD 경계)
- [ ] 동적 할당 vs 고정 할당
- [ ] 메모리 누수 감지 전략
- [ ] jemalloc 재검토 조건: Phase 2 벤치마크에서 시스템 malloc이 엔터프라이즈 경로의 병목으로 확인될 경우

### Backend 추상화
- [ ] RelationBuffer와 Arrow schema의 관계
- [ ] Backend 간 데이터 변환 비용
- [ ] 에러 처리 방식

### 성능 목표
- [ ] 임베디드 vs 엔터프라이즈별 성능 목표
- [ ] 메모리 사용량 제약
- [ ] 배포 바이너리 크기 목표

### FPGA 통합
- [ ] Hardware/Software boundary 정의
- [ ] Arrow IPC 통신 프로토콜 상세
- [ ] 작업 스케줄링 전략

---

### 4.1 Allocator 결정 기록 (ADR): jemalloc 검토

**날짜**: 2026-02-23
**상태**: 결정됨 — Phase 0-1에서 jemalloc 비도입
**참여자**: Planner, Architect, Critic (합의 기반 계획)

**배경**:
wirelog는 임베디드(ARM/RISC-V, <256MB)와 엔터프라이즈(x86-64, GB 규모) 환경을 모두 지원합니다.
현재 C11 코드베이스는 5개 파일(parser, AST, IR, program)에서 약 35회의 할당 호출(malloc/calloc/realloc)을 수행합니다.
메모리 집약적 실행은 FFI를 통해 Differential Dataflow(Rust)에 위임됩니다.

**결정**: Phase 0-1에서 jemalloc을 도입하지 않습니다. Phase 2 벤치마크 이후 Region/Arena allocator를 설계합니다.

**근거**:

1. **C11 측은 쿼리 규모 할당만 담당**: wirelog C11은 파서/옵티마이저 메모리만 관리합니다.
   데이터 규모(GB)의 메모리는 DD의 Rust allocator가 관리하므로, jemalloc은 C11 레이어에서
   실질적 이점이 없습니다.

2. **임베디드 타겟과 충돌**: jemalloc의 ~2MB 메타데이터 오버헤드는 임베디드 배포의
   500KB-2MB 독립 바이너리 목표와 직접 충돌합니다.

3. **Arena/Region이 더 적합**: AST와 IR은 명확한 "생성 → 사용 → 일괄 해제" 생애주기를
   따릅니다(파싱, IR 변환, 프로그램 메타데이터의 3단계). 이 패턴은 범용 allocator 교체가
   아닌 Region 기반 할당에 이상적입니다.

4. **시기상조 최적화**: Phase 0의 35회 할당 호출은 병목이 아닙니다. 옵티마이저 패스(Phase 1)가
   새로운 할당 패턴을 도입할 것이며, allocator 설계 전에 이 패턴이 안정화되어야 합니다.

**검토된 대안**:

| 대안 | 판정 | 이유 |
|------|------|------|
| jemalloc | 보류 | ~2MB 오버헤드, 쿼리 규모 할당에 이점 없음 |
| mimalloc | 보류 | jemalloc보다 작지만 근본적 부적합은 동일 |
| 자체 Arena 구현 | **선호** (Phase 2) | AST/IR 생애주기에 부합; 에러 경로 정리 단순화 |
| Region 기반 allocator | **선호** (Phase 2) | 계층적 Region이 파싱/IR/프로그램 단계에 대응 |
| 시스템 malloc (현재) | **유지** (Phase 0-1) | 현재 규모에 충분; 병목 증거 없음 |
| `wl_allocator_t` 인터페이스 | Phase 1 후반 | 옵티마이저 할당 패턴 안정화 후 정의 |
| Meson 빌드 시점 선택 | Phase 2+ | 기존 `embedded`/`threads` 옵션 패턴을 따르는 `option('allocator', ...)` |

**재검토 조건**: Phase 2 벤치마크에서 시스템 malloc이 엔터프라이즈 경로에서 측정 가능한
병목으로 확인되면, 해당 타겟에 한해 jemalloc 또는 mimalloc을 재검토합니다.

**이번 리뷰에서 도출된 미결 항목**:
- DD FFI 메모리 소유권(copy vs transfer vs shared buffer)이 allocator 카테고리 설계에 영향
- `strdup_safe`가 3곳에 독립적으로 정의되어 있음 — 공유 내부 유틸리티로 통합 필요
- `WIRELOG_EMBEDDED` 빌드 매크로가 정의되어 있으나 C 소스의 `#ifdef` 가드에서 아직 미사용

---

## 6. 참고 자료

**wirelog 프로젝트 문서**:
- 프로젝트 URL: https://github.com/justinjoy/wirelog
- FlowLog 논문 (참고용): `discussion/papers/2511.00865v4.pdf`
- 이전 분석: `discussion/FlowLog_C_Implementation_Analysis.md`
- Build system 분석: `discussion/build_system_analysis.md`

**외부 프로젝트**:
- Differential Dataflow: https://github.com/TimelyDataflow/differential-dataflow
- nanoarrow: https://github.com/apache/arrow-nanoarrow (추후 사용)
- Arrow format: https://arrow.apache.org/docs/format/ (추후 사용)

---

## 7. 문서 업데이트 이력

| 날짜 | 버전 | 변경 |
|------|------|------|
| 2026-02-22 | 0.1 | 초안 작성, 레이어링 정의 |
| 2026-02-22 | 0.2 | Phase 0 parser 구현 상태 업데이트 (91/91 tests passing) |
| 2026-02-23 | 0.3 | Allocator 결정 기록(§4.1 ADR) 추가: jemalloc 검토 후 보류 결정 |
| 2026-02-24 | 0.4 | IR 표현 완료 (56 tests); Stratification & SCC 완료 (20 tests); 167 total |
| 2026-02-24 | 0.5 | DD Plan Translator 완료 (19 tests); 전체 8가지 IR→DD 변환; 186 total |
| 2026-02-24 | 0.6 | Phase 1 Logic Fusion 완료 (14 tests); in-place FILTER+PROJECT→FLATMAP; 200 total |

---

**다음 단계**:
1. [x] Parser 구현 완료 (91/91 tests)
2. [x] IR 표현 정의 및 구현 (56/56 tests)
3. [x] Stratification & SCC 감지 (20/20 tests)
4. [x] DD Plan Translator (IR → DD operator graph, 19/19 tests)
5. [x] Logic Fusion 최적화 패스 (FILTER+PROJECT → FLATMAP, 14/14 tests)
6. [ ] 나머지 Phase 1 최적화 패스 (JPP, SIP, Subplan Sharing)
7. [ ] Rust FFI 통합 (DD plan → Rust executor)
8. [ ] 통합 테스트 작성
