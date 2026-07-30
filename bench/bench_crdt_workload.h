/*
 * bench_crdt_workload.h - Shared CRDT workload Datalog template.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * The CRDT verification workload (Kleppmann sequence CRDT, 23 rules
 * across 8 strata) is consumed by both bench_flowlog and the perf-suite
 * gate test (tests/test_crdt_perf_gate.c).  The template lives here so
 * those two consumers cannot drift; either consumer is expected to
 * snprintf() it once with the data directory before parsing.
 *
 * The template carries two %s placeholders, both substituted with the
 * directory containing Insert_input.csv and Remove_input.csv.
 *
 * static linkage: each translation unit gets its own private copy.
 * The string is ~5 KB, so the duplication cost is negligible and there
 * is no extra .c source file to wire into the build.
 */

#ifndef WL_BENCH_CRDT_WORKLOAD_H
#define WL_BENCH_CRDT_WORKLOAD_H

#include <stddef.h>

/* Buffer size for snprintf() expansion of the template.  The body is
 * roughly 5 KB; the data-dir path occupies a few hundred bytes; 8 KB
 * leaves headroom and matches the historical CRDT_SRC_BUFSZ. */
#define WL_BENCH_CRDT_SRC_BUFSZ ((size_t)8 * 1024)

/* Expected cardinality of the CRDT result relation on the shipped
 * bench/data/crdt fixtures.  This is not the aggregate number of rows
 * emitted for all relations by a full-session snapshot. */
#define WL_BENCH_CRDT_RESULT_TUPLES ((int64_t)104851)

static const char wl_bench_crdt_template[]
    = ".decl Insert_input(ctr: int32, node: int32, parent_ctr: int32, "
    "parent_node: int32)\n"
    ".input Insert_input(filename=\"%s/Insert_input.csv\", delimiter=\",\")\n"
    ".decl Remove_input(ctr: int32, node: int32)\n"
    ".input Remove_input(filename=\"%s/Remove_input.csv\", delimiter=\",\")\n"
    ".decl insert(ctr: int32, node: int32, parent_ctr: int32, parent_node: "
    "int32)\n"
    "insert(a, b, c, d) :- Insert_input(a, b, c, d).\n"
    ".decl remove(ctr: int32, node: int32)\n"
    "remove(a, b) :- Remove_input(a, b).\n"
    ".decl assign(id_ctr: int32, id_node: int32, elem_ctr: int32, "
    "elem_node: int32, value: int32)\n"
    "assign(ctr, n, ctr, n, n) :- insert(ctr, n, _, _).\n"
    ".decl hasChild(parent_ctr: int32, parent_node: int32)\n"
    "hasChild(pc, pn) :- insert(_, _, pc, pn).\n"
    ".decl laterChild(parent_ctr: int32, parent_node: int32, child_ctr: "
    "int32, child_node: int32)\n"
    "laterChild(pc, pn, c2, n2) :- insert(c1, n1, pc, pn), insert(c2, n2, "
    "pc, pn), c1 * 10 + n1 > c2 * 10 + n2.\n"
    ".decl firstChild(parent_ctr: int32, parent_node: int32, child_ctr: "
    "int32, child_node: int32)\n"
    "firstChild(pc, pn, cc, cn) :- insert(cc, cn, pc, pn), !laterChild(pc, "
    "pn, cc, cn).\n"
    ".decl sibling(c1: int32, n1: int32, c2: int32, n2: int32)\n"
    "sibling(c1, n1, c2, n2) :- insert(c1, n1, pc, pn), insert(c2, n2, pc, "
    "pn).\n"
    ".decl laterSibling(c1: int32, n1: int32, c2: int32, n2: int32)\n"
    "laterSibling(c1, n1, c2, n2) :- sibling(c1, n1, c2, n2), c1 * 10 + n1 "
    "> c2 * 10 + n2.\n"
    ".decl laterSibling2(c1: int32, n1: int32, c3: int32, n3: int32)\n"
    "laterSibling2(c1, n1, c3, n3) :- sibling(c1, n1, c2, n2), sibling(c1, "
    "n1, c3, n3), c1 * 10 + n1 > c2 * 10 + n2, c2 * 10 + n2 > c3 * 10 + "
    "n3.\n"
    ".decl nextSibling(c1: int32, n1: int32, c2: int32, n2: int32)\n"
    "nextSibling(c1, n1, c2, n2) :- laterSibling(c1, n1, c2, n2), "
    "!laterSibling2(c1, n1, c2, n2).\n"
    ".decl hasNextSibling(c: int32, n: int32)\n"
    "hasNextSibling(c, n) :- laterSibling(c, n, _, _).\n"
    ".decl nextSiblingAnc(start_ctr: int32, start_node: int32, next_ctr: "
    "int32, next_node: int32)\n"
    "nextSiblingAnc(sc, sn, nc, nn) :- nextSibling(sc, sn, nc, nn).\n"
    "nextSiblingAnc(sc, sn, nc, nn) :- !hasNextSibling(sc, sn), insert(sc, "
    "sn, pc, pn), nextSiblingAnc(pc, pn, nc, nn).\n"
    ".decl nextElem(prev_ctr: int32, prev_node: int32, next_ctr: int32, "
    "next_node: int32)\n"
    "nextElem(pc, pn, nc, nn) :- firstChild(pc, pn, nc, nn).\n"
    "nextElem(pc, pn, nc, nn) :- !hasChild(pc, pn), nextSiblingAnc(pc, pn, "
    "nc, nn).\n"
    ".decl currentValue(elem_ctr: int32, elem_node: int32, value: int32)\n"
    "currentValue(ec, en, v) :- assign(ic, in, ec, en, v), !remove(ic, "
    "in).\n"
    ".decl hasValue(elem_ctr: int32, elem_node: int32)\n"
    "hasValue(ec, en) :- currentValue(ec, en, _).\n"
    ".decl valueStep(from_ctr: int32, from_node: int32, to_ctr: int32, "
    "to_node: int32)\n"
    "valueStep(fc, fn, tc, tn) :- hasValue(fc, fn), nextElem(fc, fn, tc, "
    "tn).\n"
    ".decl blankStep(from_ctr: int32, from_node: int32, to_ctr: int32, "
    "to_node: int32)\n"
    "blankStep(fc, fn, tc, tn) :- !valueStep(fc, fn, tc, tn), nextElem(fc, "
    "fn, tc, tn).\n"
    ".decl value_blank_star(from_ctr: int32, from_node: int32, to_ctr: "
    "int32, to_node: int32)\n"
    "value_blank_star(fc, fn, tc, tn) :- valueStep(fc, fn, tc, tn).\n"
    "value_blank_star(fc, fn, tc, tn) :- value_blank_star(fc, fn, vc, vn), "
    "blankStep(vc, vn, tc, tn).\n"
    ".decl nextVisible(prev_ctr: int32, prev_node: int32, next_ctr: int32, "
    "next_node: int32)\n"
    "nextVisible(pc, pn, nc, nn) :- value_blank_star(pc, pn, nc, nn), "
    "hasValue(nc, nn).\n"
    ".decl result(ctr1: int32, ctr2: int32, value: int32)\n"
    "result(c1, c2, v) :- nextVisible(c1, _, c2, n2), currentValue(c2, n2, "
    "v).\n";

#endif /* WL_BENCH_CRDT_WORKLOAD_H */
