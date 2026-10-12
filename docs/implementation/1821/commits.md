# Prior implementation commits

This manifest covers the 51 implementation commits before the unfinished checkpoint.

## c0cb41760fe9de24cb8d635d9c81e590f74789ee — feat(columnar): bound controlled preallocated bulk materialization

- `tests/meson.build`
- `tests/test_evaluation_work.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/relation.c`

## 56ff8b54e21db3c146f436b83b84db854001bab7 — feat(columnar): grow controlled private storage transactionally

- `tests/test_evaluation_work.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/relation.c`

## 4f49a5365dbd03b04d7f7a1b38a3a96b0739a2c0 — feat(columnar): stage controlled type and schema installation

- `tests/test_evaluation_work.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/relation.c`

## 7cba279380d32dd472dbd2132998c30c2521f83e — feat(columnar): meter fresh governed relation construction

- `tests/test_evaluation_work.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/relation.c`

## 90972bea0b37ee64b0ceff9d15dcb6a1be1ea48d — feat(columnar): meter empty checked relation construction

- `tests/test_evaluation_work.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/relation.c`

## e9242a0252fc5b77625ff2557684761ab4ecfa6b — feat(columnar): meter governed logical relation copies

- `tests/test_evaluation_work.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/relation.c`

## d4dfcc5810a15e9651d67fd2ca128575b502422e — feat(columnar): meter checked pool relation construction

- `tests/test_evaluation_work.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/relation.c`

## 2efbbd3e90a94abc7f541ec710daef9f7bbf20d5 — feat(columnar): meter exact owned capacity reservation

- `tests/test_evaluation_work.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/relation.c`

## 69db6db06db6c2e14ff1264cb7ab027d18318e14 — test: fix arena cleanup in controlled reserve fixture

- `tests/test_evaluation_work.c`

## 46af80f55805bf0dd39fa799481967b6a3f3329f — feat: bound controlled relation source and mutation admission

- `tests/test_evaluation_work.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/relation.c`
- `wirelog/columnar/source_access.h`

## edcc8e8de72f6b62a19aa7b66ecfc358b382258e — feat(columnar): meter governor admission and commit work

- `tests/meson.build`
- `tests/test_evaluation_work.c`
- `tests/test_memory_governor.c`
- `wirelog/columnar/memory_governor.c`
- `wirelog/columnar/memory_governor.h`
- `wirelog/columnar/relation.c`

## 0bb562fe4e1dcc40f07c0a45030d9382894b79c6 — fix(columnar): bound MSVC atomic fetch arithmetic

- `tests/test_evaluation_work.c`
- `tests/test_mem_ledger.c`
- `tests/test_relation_generations.c`
- `tests/test_worker_session.c`
- `wirelog/columnar/eval.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/kfusion.c`
- `wirelog/columnar/mem_ledger.h`
- `wirelog/columnar/memory_governor.c`
- `wirelog/evaluation_control.c`

## d59e1f6a278f5d249fb30d6d56fc2623d4ec0705 — feat(columnar): bound exclusive ledger publication work

- `tests/meson.build`
- `tests/test_evaluation_work.c`
- `tests/test_mem_ledger.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/mem_ledger.c`
- `wirelog/columnar/mem_ledger.h`
- `wirelog/columnar/relation.c`

## b84880eb55325852af058fa40a2df61199b9720f — feat(columnar): bound row append and held batch work

- `tests/test_evaluation_work.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/relation.c`

## c19baa4cbb20455ff9e91c2ed6d148316a4a2edf — feat(columnar): bound ordinary JOIN work and interruption

- `tests/test_evaluation_work.c`
- `wirelog/columnar/eval_plan.c`
- `wirelog/columnar/eval_stack.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/join.c`
- `wirelog/columnar/relation.c`

## fd22332669a3792613745ce71f44e1b6132ed709 — feat(columnar): meter serial VARIABLE and simple FILTER work

- `tests/test_evaluation_work.c`
- `wirelog/columnar/eval_plan.c`
- `wirelog/columnar/filter.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/join.c`
- `wirelog/columnar/ops.c`
- `wirelog/columnar/relation.c`

## 2e8a9ee2e7e6c31c82e9b49441b0ca7d108d4032 — feat(columnar): meter compiled FILTER and MAP evaluation

- `tests/test_evaluation_work.c`
- `wirelog/columnar/eval_plan.c`
- `wirelog/columnar/expr.c`
- `wirelog/columnar/filter.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/ops.c`

## 28cf74f7def435387f5d00b58fadabdb9bd92306 — feat(columnar): meter FLOAT and numeric interpreter work

- `tests/test_evaluation_work.c`
- `wirelog/columnar/eval_plan.c`
- `wirelog/columnar/expr.c`
- `wirelog/columnar/filter.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/ops.c`

## 8cf89c357dea8b073c160f13b5ef9837fc357e2c — feat(columnar): meter string expressions and intern insertion

- `tests/test_evaluation_work.c`
- `wirelog/columnar/expr.c`
- `wirelog/columnar/filter.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/ops.c`
- `wirelog/intern.c`
- `wirelog/intern.h`

## 1b059784651169e858db1840540682134add0ec0 — feat(columnar): meter string builtins and checked parsing

- `tests/test_evaluation_work.c`
- `wirelog/columnar/expr.c`

## 7028a19093f71ca63c489e689088f07625692287 — feat(columnar): meter CRC expression work

- `tests/test_evaluation_work.c`
- `wirelog/columnar/expr.c`

## 4f6f2bae638ac6e0c14e4985a81dfef52d8c2f50 — feat(columnar): meter HASH expressions with private XXH3 streaming

- `tests/meson.build`
- `tests/test_evaluation_work.c`
- `wirelog/columnar/expr.c`
- `wirelog/columnar/expr_hash.c`
- `wirelog/columnar/internal.h`
- `wirelog/meson.build`

## 27bfd8832321208abbd70678683143add6aeb83a — feat(columnar): meter unary PSA digest expressions

- `meson.build`
- `tests/meson.build`
- `tests/test_evaluation_work.c`
- `wirelog/columnar/expr.c`
- `wirelog/columnar/expr_digest.c`
- `wirelog/columnar/internal.h`
- `wirelog/meson.build`

## 101d8c1d937adb4b1dbbd9970ad10b0c4e304baf — feat(columnar): meter UUID5 pair expressions

- `tests/test_evaluation_work.c`
- `wirelog/columnar/expr.c`
- `wirelog/columnar/expr_digest.c`
- `wirelog/columnar/internal.h`

## 436086debc0e79487be324560ab8c39ac3a1141d — feat(columnar): meter RFC UUID5 expressions

- `tests/test_evaluation_work.c`
- `wirelog/columnar/expr.c`
- `wirelog/columnar/expr_digest.c`
- `wirelog/columnar/internal.h`

## eb5121b5ce3c1c91623935766503cf8ac03f83cb — feat(columnar): meter UUID4 expressions

- `tests/test_evaluation_work.c`
- `wirelog/columnar/expr.c`
- `wirelog/columnar/expr_digest.c`
- `wirelog/columnar/internal.h`

## c15ba9c842a9760bcd7cde26d842627482155def — fix(ci): verify PSA prefix header and library isolation

- `scripts/ci/test-mbedtls-prefix.sh`

## 13175bf178848e706225df15e8f8199622692630 — feat(columnar): meter HMAC-SHA256 expressions

- `meson.build`
- `tests/meson.build`
- `tests/test_evaluation_work.c`
- `wirelog/columnar/expr.c`
- `wirelog/columnar/expr_digest.c`
- `wirelog/columnar/expr_hmac.c`
- `wirelog/columnar/internal.h`
- `wirelog/meson.build`

## c986d8c74f141c86ea6ecf21593af988cc93fa1a — feat(columnar): meter scalar extension callbacks

- `tests/meson.build`
- `tests/test_evaluation_work.c`
- `wirelog/columnar/expr.c`
- `wirelog/columnar/filter.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/ops.c`
- `wirelog/extension_registry.c`
- `wirelog/wirelog-internal.h`

## 8c499f8f04f78ffbd51ea0340eb50c8ea8acfc1b — feat(columnar): meter ordinary REDUCE evaluation

- `tests/test_evaluation_work.c`
- `wirelog/columnar/eval_plan.c`
- `wirelog/columnar/eval_stack.c`
- `wirelog/columnar/expr.c`
- `wirelog/columnar/filter.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/ops.c`
- `wirelog/columnar/relation.c`

## b3c08f7204238f4a9ba7ed6fb15f7a529661f5cf — fix(columnar): retain controlled MAP and FILTER writers

- `tests/test_evaluation_work.c`
- `wirelog/columnar/eval_stack.c`
- `wirelog/columnar/filter.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/ops.c`
- `wirelog/columnar/relation.c`

## c58b54b18f84c1484ac12c58860561c750c976f3 — fix(columnar): retain controlled JOIN output writers

- `tests/test_evaluation_work.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/join.c`

## d273dbf7f51b93b64db84993a0868d0fec8bb6e3 — feat(columnar): meter borrowed CONSOLIDATE evaluation

- `docs/SEMANTICS.md`
- `docs/THREADING.md`
- `scripts/ci/check-threading-doc.sh`
- `tests/test_evaluation_work.c`
- `wirelog/columnar/eval_plan.c`
- `wirelog/columnar/eval_stack.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/merge.c`
- `wirelog/columnar/relation.c`

## 965bcd032aa290af5223c56b4e7f45ac76b85e7c — fix(bench): link controlled expression helpers

- `bench/meson.build`

## b1d0750baae45c3a2d0c70a2a86fa35fdf76ebce — feat(columnar): meter consolidation of owned MAP output

- `tests/test_evaluation_work.c`
- `wirelog/columnar/eval_plan.c`
- `wirelog/columnar/merge.c`

## 25c39dd7899846e06db5c3bae4050794479b9430 — feat(columnar): meter serial result publication

- `docs/SEMANTICS.md`
- `docs/THREADING.md`
- `scripts/ci/check-threading-doc.sh`
- `tests/test_evaluation_work.c`
- `wirelog/columnar/eval_plan.c`
- `wirelog/columnar/eval_serial.c`
- `wirelog/columnar/eval_stack.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/ops.c`
- `wirelog/columnar/relation.c`

## c05f6c16a7cdc8e4f00eb30ca4c2d95fece654bd — feat(columnar): meter initial serial session attempts

- `docs/SEMANTICS.md`
- `docs/THREADING.md`
- `scripts/ci/check-threading-doc.sh`
- `scripts/ci/test-check-state-map-anchors.py`
- `tests/test_evaluation_work.c`
- `wirelog/columnar/eval_serial.c`
- `wirelog/columnar/eval_stack.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/relation.c`
- `wirelog/columnar/session.c`
- `wirelog/columnar/session_hash.c`

## bc689b3e44e3f056c32f8cdbeffccb1d5a2c00df — feat(columnar): meter ordinary full-input reevaluation

- `docs/SEMANTICS.md`
- `docs/THREADING.md`
- `scripts/ci/check-threading-doc.sh`
- `tests/test_evaluation_work.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/relation.c`
- `wirelog/columnar/session.c`

## d3483e92269e41261b9d9cdf99f64de65ee9e46f — feat(columnar): meter generated filtered serial rules

- `docs/SEMANTICS.md`
- `docs/THREADING.md`
- `tests/test_evaluation_work.c`
- `wirelog/columnar/eval_plan.c`
- `wirelog/columnar/eval_serial.c`
- `wirelog/columnar/eval_stack.c`
- `wirelog/columnar/expr.c`
- `wirelog/columnar/filter.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/merge.c`
- `wirelog/columnar/ops.c`
- `wirelog/columnar/session.c`

## 7bc639464007fe2ac9f76c90bccdf232250ca1ab — feat(columnar): meter generated ordinary joins

- `docs/SEMANTICS.md`
- `docs/THREADING.md`
- `tests/test_evaluation_work.c`
- `wirelog/columnar/eval_plan.c`
- `wirelog/columnar/eval_stack.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/join.c`
- `wirelog/columnar/relation.c`
- `wirelog/columnar/session.c`

## 9e129e9bcb2c5539388b253b342106fe2e705455 — feat(columnar): meter large flat set consolidation

- `docs/SEMANTICS.md`
- `docs/THREADING.md`
- `scripts/ci/check-threading-doc.sh`
- `tests/test_evaluation_work.c`
- `wirelog/columnar/eval_stack.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/merge.c`
- `wirelog/columnar/relation.c`

## b448b3933803065e9c639b0aac0d9ef206f61c5d — feat(columnar): meter segmented set evaluation

- `docs/SEMANTICS.md`
- `docs/THREADING.md`
- `scripts/ci/check-threading-doc.sh`
- `tests/meson.build`
- `tests/test_evaluation_work.c`
- `wirelog/columnar/eval_plan.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/merge.c`
- `wirelog/columnar/relation.c`
- `wirelog/columnar/session.c`

## d3ab625b8a34a17eccc95ef85def795c83cf80e7 — feat(columnar): meter ordinary aggregate evaluation

- `docs/SEMANTICS.md`
- `docs/THREADING.md`
- `scripts/ci/check-threading-doc.sh`
- `tests/test_evaluation_work.c`
- `wirelog/columnar/eval_plan.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/ops.c`
- `wirelog/columnar/relation.c`
- `wirelog/columnar/session.c`

## 22cbd329d85dec9aaee4396353364b86a6211641 — feat(columnar): compose controlled aggregate routes

- `docs/SEMANTICS.md`
- `tests/test_evaluation_work.c`
- `wirelog/columnar/eval_plan.c`
- `wirelog/columnar/merge.c`
- `wirelog/columnar/relation.c`
- `wirelog/columnar/session.c`

## 19f8d29bf6036a786edab83216bc633b5eb87487 — feat(columnar): journal controlled output gate counts

- `docs/SEMANTICS.md`
- `docs/THREADING.md`
- `scripts/ci/check-threading-doc.sh`
- `scripts/ci/test-check-state-map-anchors.py`
- `tests/test_evaluation_work.c`
- `wirelog/columnar/eval_stack.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/relation.c`
- `wirelog/columnar/session.c`

## fd3fcf91d6646eade87679058b25e515f9a90d69 — feat(columnar): bound ordinary batch join production

- `docs/SEMANTICS.md`
- `docs/THREADING.md`
- `scripts/ci/check-threading-doc.sh`
- `tests/test_evaluation_work.c`
- `tests/test_join_batch_resume.c`
- `wirelog/columnar/arrangement.c`
- `wirelog/columnar/continuation.c`
- `wirelog/columnar/continuation.h`
- `wirelog/columnar/eval_plan.c`
- `wirelog/columnar/eval_stack.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/join.c`
- `wirelog/columnar/join_batch.c`
- `wirelog/columnar/join_batch.h`
- `wirelog/columnar/merge.c`
- `wirelog/columnar/relation.c`
- `wirelog/columnar/session.c`

## 9af33ae348c641b3362d30659f5abb5eb05d9565 — feat(columnar): bound ordinary filtered batch pipelines

- `docs/SEMANTICS.md`
- `tests/test_evaluation_work.c`
- `wirelog/columnar/eval_plan.c`
- `wirelog/columnar/eval_stack.c`
- `wirelog/columnar/filter.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/join.c`
- `wirelog/columnar/join_batch.c`
- `wirelog/columnar/join_batch.h`
- `wirelog/columnar/join_pipeline.c`
- `wirelog/columnar/join_pipeline.h`
- `wirelog/columnar/ops.c`
- `wirelog/columnar/relation.c`
- `wirelog/columnar/session.c`

## ae6b32784ba9d6504fce49ff23f78f15374cd780 — feat(columnar): bound ordinary membership joins

- `docs/SEMANTICS.md`
- `docs/THREADING.md`
- `scripts/ci/check-threading-doc.sh`
- `tests/meson.build`
- `tests/test_evaluation_membership.c`
- `wirelog/columnar/eval_plan.c`
- `wirelog/columnar/eval_stack.c`
- `wirelog/columnar/filter.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/join.c`

## 819d8f6ac4e77fc0b28862df1c634faf7c7e4563 — feat(columnar): bound ordinary LFTJ evaluation

- `docs/SEMANTICS.md`
- `docs/THREADING.md`
- `scripts/ci/check-threading-doc.sh`
- `tests/meson.build`
- `tests/test_evaluation_lftj.c`
- `wirelog/columnar/arrangement.c`
- `wirelog/columnar/eval_plan.c`
- `wirelog/columnar/eval_stack.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/lftj.c`
- `wirelog/columnar/lftj.h`
- `wirelog/columnar/ops.c`
- `wirelog/columnar/relation.c`
- `wirelog/columnar/session.c`

## 30b228c9c01da65cf58853045defdf6da8701c5c — feat(columnar): bound ordinary recursive evaluation

- `docs/SEMANTICS.md`
- `docs/THREADING.md`
- `scripts/ci/check-threading-doc.sh`
- `tests/meson.build`
- `tests/test_evaluation_recursive.c`
- `wirelog/arena/compound_arena.c`
- `wirelog/arena/compound_arena.h`
- `wirelog/columnar/arrangement.c`
- `wirelog/columnar/cache.c`
- `wirelog/columnar/diff.c`
- `wirelog/columnar/eval_plan.c`
- `wirelog/columnar/eval_serial.c`
- `wirelog/columnar/eval_stack.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/join.c`
- `wirelog/columnar/memory_governor.c`
- `wirelog/columnar/memory_governor.h`
- `wirelog/columnar/merge.c`
- `wirelog/columnar/ops.c`
- `wirelog/columnar/relation.c`
- `wirelog/columnar/session.c`
- `wirelog/columnar/session_hash.c`

## 4e2e1272189921bd4b3d8ab167517c6d52ecc81c — feat(columnar): bound final recursive MIN/MAX canonicalization

- `docs/SEMANTICS.md`
- `docs/THREADING.md`
- `scripts/ci/check-threading-doc.sh`
- `tests/test_evaluation_recursive.c`
- `wirelog/columnar/eval_dedup.c`
- `wirelog/columnar/eval_plan.c`
- `wirelog/columnar/eval_serial.c`
- `wirelog/columnar/eval_stack.c`
- `wirelog/columnar/internal.h`
- `wirelog/columnar/merge.c`
- `wirelog/columnar/ops.c`
- `wirelog/columnar/session.c`
