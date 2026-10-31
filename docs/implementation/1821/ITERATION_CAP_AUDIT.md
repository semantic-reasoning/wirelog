# Serial fixed-point iteration-cap audit

This inventory covers every production fixed-point `MAX_ITERATIONS` site and
the cap-free recursive hosts reachable from the columnar session API. The
production outer limit remains4096 and `EVAL_STRIDE` remains8. No public cap
option or worker-wide evaluation-control guarantee is introduced.

| Actual route | Allowance and exit | Publication and retry |
| --- | --- | --- |
| `eval_serial.c:col_eval_stratum` |4096 outer iterations ×8 effective subpasses. A real unchanged subpass ends convergence. Physically empty frontiers and rules whose forced deltas are all empty establish convergence. An unproven frontier-skipped or nonempty signed net-zero retry can exhaust the allowance. | Historical raw status0 remains. Exhaustion sets `eval_stratum_iteration_capped`, leaves heads nonfinal and suppresses rule/stratum/progress convergence records. Delta removal, pools, arena rotation and cache cleanup still run; their errors retain precedence. |
| `session.c:wl_columnar_session_observed_action`, recursive RETIRE | One controlled round equals one effective subpass, so the allowance is32768 rounds, including the unchanged round that proves convergence. A changing final allowed round returns `EOVERFLOW`. | Poll the active control at the cap decision before synthesizing overflow. A pending configured stop wins. Private heads/events are discarded; pending input and published heads stay intact. Reset and retry the same host with sufficient allowance. |
| `eval.c:col_eval_stratum_tdd_recursive` |4096 ×8. Parallel-only after actual worker selection; single-worker fallback delegates to serial. Adaptive/overflow replay takes the serial evaluator's actual outcome. | Raw status compatibility and worker/queue cleanup remain. Exhaustion does not record recursive convergence. Multiworker aggregation ORs cap outcomes after its barrier and suppresses global convergence if any worker capped. This is outcome bookkeeping, not new worker control support. |
| `session.c:col_controlled_step_recursive_drive` | No legacy iteration cap. | The controlled binary host is bounded by configured work/cancellation and resumable cursor ownership. |
| `eval_tdd_insert.c:session_snapshot_insert` recursive loop | No `MAX_ITERATIONS` cap; actual delta exhaustion ends its loop. | This incremental insertion route is raw; controlled evaluation uses the private transaction host. |

`col_eval_stratum`, `col_eval_stratum_tdd` and
`col_eval_stratum_multiworker` reject attached control before raw dispatch.
`col_eval_stratum_tdd` with one worker delegates to the audited serial loop.
Parser compound depth, worker selection, relation capacity, the scheduling
quantum and the arena comment mentioning the legacy constant are not
fixed-point termination caps.

Raw STEP and evaluating SNAPSHOT consume the last-stratum cap outcome before
another stratum or correctness replay can overwrite it. A capped pass leaves
`pending_input_change` and `pending_full_input_eval` true, snapshot stability
false, and does not advance input `base_nrows`, clear retractions or claim a
successful first evaluation. The derived prefix is recorded only as the extent
to clear on retry. Plain completion tokens cannot send that retry straight to
compaction. A SNAPSHOT draining a pending plain STEP checks the cap immediately.
Raw SNAPSHOT may still deliver its historical partial image with status0, but
the next call must evaluate again. Observed STEP retains its original
unevaluated notification baseline and emits no partial completed difference.

`evaluation_iteration_cap` runs a generated two-head SCC over a13-edge chain,
checks its independent complete value set, and reaches boundaries through real
evaluation. Four reduced outer strides allow32 effective passes; the controlled
host needs29 and succeeds under the same allowance. Exact-convergence allowance
succeeds, one fewer round fails privately, and cancellation/work exhaustion
injected at the real cap decision wins. STEP/SNAPSHOT and observed/callback-free
hosts check frozen reports, unchanged input generations, private publication,
same-host reset/retry and duplicate-free notifications. Raw plain/observed STEP
and SNAPSHOT exercise cap status0 followed by a raised-limit complete retry.
Checked teardown releases session/control storage; the explicitly program-owned
intern-table reservation is detached separately and governor credit reaches0.

The test-only override/checkpoint is absent from production builds. The inventory
is an iteration-cap disposition audit, not a claim that raw weighted dispatch,
parallel global budgeting or all other lifecycle obligations of #1953 are done.

The empty-frontier audit also fixes a masked legacy exit: fully empty serial
frontiers/forced-delta bodies and TDD all-workers-empty bodies terminate as
converged instead of burning the remaining cap. A nonempty signed net-zero
frontier is not evidence of unchanged tuple content. Full observed head
reconstruction invalidates old stratum/rule convergence before running; a
frontier for detached heads cannot skip their replacement. The existing
`easy_delta_head_input` removal-with-callback-disabled regression qualifies the
signed deletion after the callback is re-enabled, with one and four workers.
