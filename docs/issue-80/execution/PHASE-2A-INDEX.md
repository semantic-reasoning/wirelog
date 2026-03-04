# Phase 2A Documentation Index

**Status**: ✅ READY FOR EXECUTION
**Date**: 2026-03-04
**Decision**: Option B (Complete Columnar Backend in C11)
**Timeline**: 35 days with 2 C engineers
**Strategic Goal**: Rust removal + full recursive delta support

---

## Quick Start (For Busy Engineers)

**Start here**: `docs/PHASE-2A-CHECKLIST.md` (daily task breakdown)
**Need background**: `docs/ARCHITECTURE.md` (system design)
**Need decision rationale**: `docs/delta-query.md` (RALPLAN consensus)

---

## Complete Documentation Suite

### 1. **delta-query.md** (Decision Document)
📄 **453 lines** | Decision, drivers, options, pre-mortem, timeline, acceptance criteria

**What**: RALPLAN deliberate consensus discussion
**Why**: Documents why Option B (columnar) was chosen over Option A (extend DD)
**Key Decision**: Rust elimination is non-negotiable; columnar is the fastest path to Rust-free

**Sections**:
- Executive summary (constraint shift: Rust removal is primary goal)
- Decision framework (5 principles)
- Option comparison (A vs B vs C)
- Architecture overview (semi-naive evaluation, operators, memory)
- Pre-mortem scenarios (4 failure modes + mitigations)
- Phase 2A acceptance criteria (MUST/SHOULD/NICE)
- Decision points & fallback protocol
- Test strategy (unit/integration/E2E)
- References to issues, decision rationale

**Read if**: You need to understand "why columnar?" or defend the decision to stakeholders

---

### 2. **ARCHITECTURE.md** (System Design)
📄 **492 lines** | Vision, layers, Phase 1 design, Phase 2A changes, technology choices

**What**: Comprehensive architecture documentation (5-layer system)
**Why**: Reference for understanding how the system works end-to-end

**Sections**:
- Vision & phase roadmap (Phase 0→1→2A→2B→3)
- 5-layer architecture:
  1. Parser & IR (lexer, parser, AST, rules, stratification)
  2. Optimizer (join push-down, semi-naive planning)
  3. Plan layer (type definitions, marshaling)
  4. Backend layer (vtable, pluggable executors)
  5. Session layer (state management, snapshots)
- Phase 1 architecture & data flow (current, with Rust DD)
- Phase 2A architecture changes (columnar backend, no Rust)
- Phase 2B & 3 roadmap (multi-worker, nanoarrow, FPGA)
- 15 benchmarks (TC, Reach, CC, SSSP, SG, Bipartite, Polonius, DOOP, FlowLog)
- Technology choices (C11, Meson, hand-written parser)
- Code organization (full directory structure with annotations)
- Development guidelines

**Read if**: You're new to wirelog, want to understand the big picture, or planning Phase 2B/3

---

### 3. **PHASE-2A-EXECUTION.md** (Execution Roadmap)
📄 **382 lines** | 35-day task breakdown, daily activities, checkpoints, success criteria

**What**: Day-by-day execution plan for Phase 2A
**Why**: Engineers use this to know exactly what to do each day

**Sections**:
- 5 clarifications resolved (with implementation guidance)
- 35-day task breakdown:
  - Days 0-1: Design phase (headers, memory, evaluator, operators)
  - Days 2-10: Core infrastructure (E1: backend, E2: operators)
  - Days 11-16: CRITICAL recursion (E1: deltas, E2: joins)
  - Days 17-22: Features (E1: sessions, E2: remaining ops)
  - Days 23-30: Validation & Day 25 checkpoint
  - Days 31-35: Rust removal & final validation
- Memory management architecture (arena per epoch)
- Critical issues (header refactoring, oracle tests, LOC accounting)
- Risk mitigation by phase
- Success criteria (MUST/SHOULD/NICE)
- Phase 2B plan (if Yellow path on Day 25)
- Execution checklist

**Read if**: You're assigned to Phase 2A and need to know your daily tasks

---

### 4. **PHASE-2A-CLARIFICATIONS.md** (Decision Details)
📄 **727 lines** | 5 required clarifications, each with concrete resolution

**What**: Detailed explanation of 5 blocking decisions with implementation guidance
**Why**: Engineers need to understand design decisions before starting

**Sections** (one per clarification):

1. **Header Split for `dd_ffi.h`**
   - Create `exec_plan.h` with backend-agnostic types
   - Delete Rust FFI declarations
   - Timing: Days 2-3, E1
   - Code example provided

2. **Memory Management**
   - Decision: Arena per epoch (256MB per iteration)
   - Rationale: Simpler than reference-counting
   - Polonius: ~15GB total manageable
   - Profiling goals table

3. **`dd_marshal.c` Fate**
   - Keep generic unmarshaling (440 LOC)
   - Delete Rust helpers (345 LOC)
   - Rename to `exec_marshal.c`
   - Timing: Days 1-3, E1

4. **Hand-Computed Oracle Tests**
   - Test 1: Aggregation (count with GROUP BY)
   - Test 2: Transitive closure (manual enumeration)
   - Test 3: Stratified negation (fixed-point)
   - Timing: Days 9-10, E2

5. **Day 25 Scope Decision Protocol**
   - GREEN: All 15 pass → full Phase 2A
   - YELLOW: Recursion broken → non-recursive only, Phase 2B deferred
   - RED: Critical failure → escalate
   - Decision authority defined
   - Meeting agenda provided

**Read if**: You're designing Phase 2A (Architect/Team Lead) or implementing it (Engineers)

---

### 5. **PHASE-2A-CHECKLIST.md** (Daily Reference)
📄 **415 lines** | Pre-kickoff checklist, day-by-day tasks, success metrics, communication

**What**: Tactical execution checklist for engineers
**Why**: Day-to-day reference for staying on track

**Sections**:
- Pre-kickoff checklist (environment, architecture review, baseline verification)
- Day 0 morning (E1 & E2 task assignments)
- Days 2-10: Core infrastructure (parallel work breakdown)
- Days 11-16: CRITICAL recursion (E1 & E2 tracks)
- Days 17-22: Features
- Days 23-30: Validation
- Days 31-35: Rust removal
- Success metrics (MUST/SHOULD/NICE)
- Daily standup format
- Blocker escalation (who to contact, response time)
- Weekly checkpoints
- Commit message convention
- Code review process

**Read if**: You're executing Phase 2A (daily reference) or managing the team

---

### 6. **MONOTONE_AGGREGATION.md** (Previous Work, Phase 1)
📄 **488 lines** | Formal semantics of MIN/MAX aggregation in recursion

**What**: Phase 1 innovation documentation (recursive aggregation with MIN/MAX only)
**Why**: Context for why COUNT/SUM are rejected in recursive strata (Issue #69)

**Sections**:
- Formal definitions (lattice theory, monotone functions)
- Monotonicity proofs (MIN/MAX are monotone)
- Non-monotonicity proofs (COUNT/SUM/AVG are non-monotone)
- Implementation details
- Use cases (CC with MIN, SSSP with MAX)
- Design rationale
- Safety guarantees

**Read if**: You need to understand why aggregation validation works or implement new aggregations

---

## Document Dependencies

```
Phase 2A Documentation Dependency Graph:

delta-query.md (DECISION)
    ↓
    ├→ ARCHITECTURE.md (SYSTEM DESIGN)
    │      ↓
    │      ├→ PHASE-2A-EXECUTION.md (ROADMAP)
    │      │      ↓
    │      │      ├→ PHASE-2A-CHECKLIST.md (DAILY REFERENCE)
    │      │      │
    │      │      └→ PHASE-2A-CLARIFICATIONS.md (DETAIL)
    │      │
    │      └→ MONOTONE_AGGREGATION.md (CONTEXT: Issue #69)
    │
    └→ PHASE-2A-CLARIFICATIONS.md (5 DECISIONS)
           ↓
           └→ PHASE-2A-EXECUTION.md (IMPLEMENTATION)

Reading Order:
1. delta-query.md (understand decision)
2. ARCHITECTURE.md (understand system)
3. PHASE-2A-CLARIFICATIONS.md (understand design choices)
4. PHASE-2A-EXECUTION.md (understand timeline)
5. PHASE-2A-CHECKLIST.md (execute day-by-day)
```

---

## Stakeholder Quick Links

### For Architects/Tech Leads
1. **Understand the decision**: `delta-query.md` (Executive Summary + Decision Framework)
2. **Review design**: `PHASE-2A-CLARIFICATIONS.md` (all 5 clarifications)
3. **Sign off on architecture**: `ARCHITECTURE.md` (Phase 2A section)

### For Engineers (E1 & E2)
1. **Understand the system**: `ARCHITECTURE.md` (all layers)
2. **Get your tasks**: `PHASE-2A-CHECKLIST.md` (Day 0 morning assignment)
3. **Reference clarifications**: `PHASE-2A-CLARIFICATIONS.md` (design decisions)
4. **Follow the roadmap**: `PHASE-2A-EXECUTION.md` (timeline + deliverables)

### For Product/Management
1. **Understand the decision**: `delta-query.md` (Executive Summary + Strategic Value)
2. **See the timeline**: `PHASE-2A-EXECUTION.md` (35 days with 2 engineers)
3. **View success criteria**: `PHASE-2A-EXECUTION.md` or `delta-query.md` (gates)

### For Future Phases (2B/3)
1. **Understand current state**: `ARCHITECTURE.md` (Phase 1 + 2A sections)
2. **See roadmap**: `ARCHITECTURE.md` (Phase 2B & 3 section)

---

## Validation Checklist

Before Phase 2A begins (Day 0), verify:

- [ ] **Decision Approved**: RALPLAN consensus documented in `delta-query.md`
- [ ] **Architecture Reviewed**: Architect signed off on `ARCHITECTURE.md` + `PHASE-2A-CLARIFICATIONS.md`
- [ ] **Engineers Assigned**: 2 C engineers confirmed for full 35 days
- [ ] **Environment Ready**: Meson, Ninja, clang-format 18, Valgrind, ASan installed
- [ ] **Repository Access**: Both engineers can push to feature branches
- [ ] **Baseline Passes**: `meson test -C build` = 20/20 tests ✅
- [ ] **Build Clean**: `meson compile -C build` <5 sec ✅
- [ ] **Documentation Read**: Engineers completed architecture review
- [ ] **GitHub Issue Created**: Issue #X (Phase 2A) with link to docs
- [ ] **Day 0 Tasks Assigned**: E1 and E2 have clear morning assignments

---

## Key Metrics & Success Criteria

### Phase 2A Gates (Day 35)

| Gate | Requirement | Status |
|------|-------------|--------|
| **Correctness** | All 15 benchmarks identical to Phase 1 (oracle validation) | ⏳ |
| **Rust Removal** | Zero Rust LOC remaining | ⏳ |
| **Build Speed** | Meson-only compile <5 seconds | ⏳ |
| **Memory Safety** | Zero ASan/Valgrind errors | ⏳ |
| **Performance** | Execution time <3x DD baseline | ⏳ |

### Timeline Tracking

| Phase | Days | Status | Owner |
|-------|------|--------|-------|
| Design | 0-1 | ⏳ Pending E1+E2 | E1, E2 |
| Core Infrastructure | 2-10 | ⏳ Pending design | E1, E2 |
| CRITICAL Recursion | 11-16 | ⏳ Pending core | E1, E2 |
| Features | 17-22 | ⏳ Pending recursion | E1, E2 |
| Validation | 23-30 | ⏳ Pending features | E1, E2 |
| Rust Removal | 31-35 | ⏳ Pending validation | E1, E2 |

---

## Questions & Answers

### Q: What if we hit a blocker?
**A**: See `PHASE-2A-CHECKLIST.md` → "Blocker Escalation" section. Tag architect or team lead; expect 2-hour response.

### Q: What if Day 25 recursion fails?
**A**: See `PHASE-2A-CLARIFICATIONS.md` → "Clarification #5" → "YELLOW PATH". Ship non-recursive backend; defer recursion to Phase 2B.

### Q: How do we validate correctness?
**A**: See `PHASE-2A-EXECUTION.md` → "Test Strategy" section. Oracle tests (DD vs columnar) + 3 hand-computed tests.

### Q: What if Polonius goes over 20GB memory?
**A**: See `PHASE-2A-CLARIFICATIONS.md` → "Clarification #2" → Memory Management. If arena approach insufficient, escalate for alternative design.

### Q: Can we skip the design phase (Days 0-1)?
**A**: No. Design phase prevents Days 2-10 rework and unblocks both E1 and E2 parallel work.

### Q: What's the fallback if Option B fails?
**A**: See `delta-query.md` → "Fallback Protocol" section. If recursion broken by Day 25 (Yellow path), ship non-recursive backend and plan Phase 2B.

---

## File Inventory

| File | Lines | Purpose | Owner | Phase |
|------|-------|---------|-------|-------|
| delta-query.md | 453 | Decision document | Arch | Approved |
| ARCHITECTURE.md | 492 | System design | Arch | Phase 1+2A |
| PHASE-2A-EXECUTION.md | 382 | Roadmap | Lead | Execution |
| PHASE-2A-CLARIFICATIONS.md | 727 | 5 decisions | Arch | Design |
| PHASE-2A-CHECKLIST.md | 415 | Daily reference | E1, E2 | Execution |
| MONOTONE_AGGREGATION.md | 488 | Phase 1 innovation | Arch | Context |

**Total**: ~2,957 lines of Phase 2A documentation

---

## Next Steps

### Immediate (Today, Before Day 0)

1. ✅ Document Phase 2A decision and roadmap (DONE)
2. ⏳ Architect reviews all documents
3. ⏳ Architect signs off on design
4. ⏳ Assign 2 C engineers
5. ⏳ Create GitHub Issue #X (Phase 2A) with links

### Day 0 Morning

1. ⏳ Team kickoff (review design, clarifications, checklist)
2. ⏳ E1 starts header refactoring design
3. ⏳ E2 starts evaluator loop design
4. ⏳ Daily standup (30 min)

### Days 1-35

1. ⏳ Execute per PHASE-2A-CHECKLIST.md
2. ⏳ Daily standup (15 min)
3. ⏳ Day 25 checkpoint (critical decision)
4. ⏳ Weekly reviews (Monday, Wednesday, Friday)

### Day 35 (Completion)

1. ⏳ Phase 2A ships (Rust-free columnar backend ± recursion)
2. ⏳ Celebrate 🎉
3. ⏳ Plan Phase 2B (if Yellow path)

---

## Contact & Escalation

**Phase Lead**: [Team Lead Name] (overall timeline, risk management)
**Architect**: [Architect Name] (design decisions, sign-off)
**Engineer 1**: [E1 Name] (backend, memory, delta propagation)
**Engineer 2**: [E2 Name] (operators, joins, aggregation)

**Escalation Path**:
1. Daily blocker → Tag architect (response: 2h)
2. Design review → Tag team lead (response: 4h)
3. Critical issue → Full team (response: immediate)

---

**Prepared by**: wirelog architecture team
**Date**: 2026-03-04
**Status**: ✅ All documentation complete and ready for execution
**Next**: Await architect sign-off and engineer assignment (Day 0 kickoff)
