# wirelog 아키텍처 설계 문서

**프로젝트**: wirelog - Embedded-to-Enterprise Datalog Engine
**Copyright**: Copyright (C) CleverPlant
**작성일**: 2026-02-22
**상태**: 🔄 설계 진행 중 (Phase 0 구현 중)

⚠️ **이 문서는 초안입니다.** 지속적으로 업데이트됩니다.

---

## 핵심 요구사항

1. **멀티 타겟 (동일 시작)**: 임베디드 + 엔터프라이즈 **모두 DD와 연결한 상태에서 시작**
2. **FPGA 가속 준비**: 무거운 라이브러리 없이 미래에 FPGA 가속으로 연산 오프로드 가능
3. **철저한 레이어링**: 계층 분리로 향후 최적화 유연성 확보
4. **nanoarrow 후순위**: 초기에는 불필요, 임베디드 최적화 단계에서 추가
5. **C99 기반**: C11 대신 C99 사용 (더 넓은 호환성)

---

## 1. 핵심 설계 원칙

### 1.1 멀티 타겟 아키텍처 (Embedded ↔ Enterprise)

**초기 (Phase 0-3): 모두 DD 기반**
```
wirelog 코어 (C99)
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
wirelog 코어 (C99)
    └─ Backend Abstraction (선택)
        │
        ├─ [Embedded Path]
        │   ├─ nanoarrow 메모리 (columnar, 선택적)
        │   ├─ Semi-naive executor (C99)
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
- MPI → 분산 처리는 DD에게 위임

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
wirelog (C99 parser/optimizer)
    ↓ (IR를 DD operator graph로 변환)
Differential Dataflow (Rust executor, 독립)
    ↓
Result
```

**이점**:
- 입증된 성능 (Differential Dataflow의 증분 처리)
- DD의 멀티 워커, 분산 처리 즉시 활용
- wirelog는 파서/최적화만 C99로 구현
- 임베디드 + 엔터프라이즈 동일 기반에서 시작
- 나중에 임베디드만 선택적으로 nanoarrow로 마이그레이션 가능

**실행 경로** (모든 환경):
```
초기 (Phase 0-3, Month 1-5):
  wirelog (C99 파서/최적화)
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
  wirelog (C99 파서/최적화)
      ↓
  nanoarrow executor (C99, 완전 독립)
      ↓
  Result (500KB-2MB 바이너리)

엔터프라이즈 환경:
  (DD 경로 유지, 변경 없음)

FPGA 가속 (미래):
  wirelog (C99 파서/최적화)
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
│ Logic Layer (wirelog core) - C99                    │
│ - Parser (hand-written RDP, Datalog → AST)         │
│ - IR Representation (backend-agnostic structs)      │
│ - Optimizer (Logic Fusion, JPP, SIP, Subplan)      │
│ - Stratifier (SCC detection, topological sort)     │
└──────────────────┬──────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────┐
│ DD Translator (C99 ↔ Rust FFI)                      │
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
  CSV, JSON, Arrow IPC (나중에)
```

### 2.1b 계층 구조 (Phase 3+: 선택적 임베디드 최적화)

```
wirelog core (C99)
    ├─ [Enterprise: DD 유지]
    │   └─ Differential Dataflow (변경 없음)
    │
    └─ [Embedded: 선택적 마이그레이션]
        └─ ComputeBackend abstraction
            ├─ nanoarrow executor (C99)
            └─ (미래) FPGA backend via Arrow IPC
```

### 2.2 각 레이어의 책임 (Phase 0-3)

#### Logic Layer (wirelog 핵심, C99)

**파일 구조**:
```
wirelog/
  lexer.c         # Tokenization
  parser.c        # Datalog → AST (hand-written RDP)
  ast.c           # AST node 관리
  ir.c            # IR node 관리
  stratify.c      # Stratification, SCC 감지
  optimizer.c     # Optimizer orchestrator
  passes/
    fusion.c      # Logic Fusion
    jpp.c         # Join-Project Plan
    sip.c         # Semijoin Information Passing
    sharing.c     # Subplan Sharing
```

**책임**:
- Datalog 프로그램을 파싱하여 AST 생성
- AST → IR 변환 (backend-agnostic)
- IR 레벨 최적화 (알고리즘)
- DD와 독립적 설계

**Phase 0 구현 상태**:
- ✅ Parser 구현 (hand-written RDP, C99)
- ✅ 파서 테스트: 91/91 passing (47 lexer + 44 parser)
- ✅ Grammar: FlowLog-compatible (declarations, rules, negation, aggregation, arithmetic, comparisons, booleans, .plan marker)
- 🔄 IR 표현 정의 (진행 중)
- 🔄 Stratification & SCC 감지 (계획됨)
- 🔄 최적화 passes (계획됨)

#### DD Translator (C99 ↔ Rust FFI)

**파일** (계획):
```
src/
  dd/
    translator.c    # IR → DD operator graph
    ffi.h           # FFI definitions
    data_marshal.c  # Data conversion C ↔ Rust
```

**책임**:
- wirelog IR을 DD operator graph로 변환
- C ↔ Rust 데이터 마샬링
- DD 워커 관리 (single vs multi)
- 결과 수집 및 변환

**설계 결정 사항** (TODO):
- [ ] FFI 바운더리 명확히 (메모리 소유권)
- [ ] Data marshalling 전략 (zero-copy vs copy)
- [ ] Error handling 방식
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
- (나중에 Arrow IPC 추가)

---

### 2.3 향후 계층 구조 (Phase 3+: 선택적 임베디드 최적화)

**이 때 추가될 레이어** (계획):

#### ComputeBackend Abstraction (C99)

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

#### nanoarrow Executor (C99, 선택)

- Sort-merge join on columnar data
- Semi-naive delta propagation
- 메모리 최적화

#### FPGA Backend (미래)

- Arrow IPC를 통한 데이터 전송
- 하드웨어 연산 오프로드

---

## 3. 개발 로드맵

### Phase 0: 기초 (Weeks 1-4) - 모든 환경 DD 기반

**목표**: C99 파서/최적화 + DD 변환기로 동작하는 초기 버전

**구현 항목**:
- ✅ C99 파서 (Datalog → AST, hand-written RDP)
- ✅ 파서 테스트 (91/91 passing)
- ✅ FlowLog-compatible grammar 구현
- ✅ 빌드 시스템 (Meson, C99)
- 🔄 IR 표현 정의 (backend-agnostic)
- 🔄 Stratification & SCC 감지
- 🔄 IR → DD operator graph 변환기
- 🔄 기본 통합 테스트

**검증**:
- [ ] 임베디드 타겟 (ARM cross-compile) 빌드 성공
- [ ] 엔터프라이즈 타겟 (x86-64) 빌드 성공
- [ ] 기본 Datalog 프로그램 실행 확인

**현재 상태**: Parser 완료 (91/91 tests passing), IR/DD translator 진행 중

### Phase 1: 최적화 (Weeks 5-10) - 모든 환경 공통

**목표**: 논문 기반 최적화 기법을 IR 수준에서 구현 (FlowLog/Soufflé 참고)

**구현 항목** (계획):
- [ ] Logic Fusion (Join+Map+Filter → FlatMap)
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

**추정**: TBD (FPGA 하드웨어 가용성 의존)

---

## 4. 기술 스택

| 계층 | 선택 | 상태 | 근거 |
|------|------|------|------|
| **언어** | C99 | ✅ 확정 | 최소 의존성, 임베디드 친화, 호환성 |
| **빌드** | Meson | ✅ 확정 | Cross-compile 우수, 경량 |
| **Parser** | Hand-written RDP | ✅ 구현됨 | Zero deps, 91/91 tests passing |
| **메모리** | nanoarrow (중기) | 계획 | Columnar, Arrow interop |
| **Allocator** | Arena + malloc | 계획 | 상세 설계 필요 |
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
- [ ] Arena allocator 세밀 설계
- [ ] 동적 할당 vs 고정 할당
- [ ] 메모리 누수 감지 전략

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

## 6. 참고 자료

**wirelog 프로젝트 문서**:
- 프로젝트 URL: https://github.com/justinjoy/wirelog
- FlowLog 논문 (참고용): `discussion/papers/2511.00865v4.pdf`
- 이전 분석: `discussion/FlowLog_C_Implementation_Analysis.md`
- Build system 분석: `discussion/build_system_analysis.md`

**외부 프로젝트**:
- Differential Dataflow: https://github.com/TimelyDataflow/differential-dataflow
- nanoarrow: https://github.com/apache/arrow-nanoarrow (나중에 사용)
- Arrow format: https://arrow.apache.org/docs/format/ (나중에 사용)

---

## 7. 문서 업데이트 이력

| 날짜 | 버전 | 변경 |
|------|------|------|
| 2026-02-22 | 0.1 | 초안 작성, 레이어링 정의 |
| 2026-02-22 | 0.2 | Phase 0 parser 구현 상태 업데이트 (91/91 tests passing) |

---

**다음 단계**:
1. [x] Parser 구현 완료 (91/91 tests)
2. [ ] IR 표현 정의 및 구현
3. [ ] DD Translator FFI 설계
4. [ ] Stratification 구현
5. [ ] 통합 테스트 작성
