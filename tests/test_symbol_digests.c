/*
 * tests/test_symbol_digests.c - digests over symbol columns (Issue #963)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * hash() and the CRC-32 built-ins used to digest the *intern id* of a
 * symbol column rather than the string that id stands for.  Two
 * consequences, both observable in-tree before the fix:
 *
 *   - hash("abc") matched no external tool, because it was really
 *     XXH3_64bits of an int64 counter.
 *   - The same string digested differently depending on what had been
 *     interned first, so prepending an unrelated row to an input file
 *     changed every fingerprint below it.
 *
 * The plan generator now emits the WL_PLAN_EXPR_ARITH_*_S opcodes when the
 * operand's declared type is `symbol`/`string`, and those digest the
 * string's own bytes -- strlen many, no NUL terminator.
 *
 * Every expected value here was produced outside wirelog before the code
 * was written, so the tests pin the external convention rather than
 * whatever the implementation happens to do:
 *
 *     printf 'alice@example.com' | xxhsum -H3     -> 76eb895512bf35ff
 *     python3 -c 'import zlib; print(zlib.crc32(b"aa"))'
 *
 * The mbedTLS-backed digests (md5, sha1/256/512, hmac, uuid5) get the same
 * treatment and are covered in tests/test_cryptographic_hashes.c, the test
 * the mbedtls-enabled CI job runs.  This file deliberately contains only
 * the half that is available in every build configuration.
 */

#include "../wirelog/backend.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/intern.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int test_count;
static int pass_count;
static int fail_count;

#define TEST(name)                                      \
        do {                                                \
            test_count++;                                   \
            printf("TEST %d: %s ... ", test_count, (name)); \
        } while (0)

#define PASS()            \
        do {                  \
            pass_count++;     \
            printf("PASS\n"); \
        } while (0)

#define FAIL(msg)                    \
        do {                             \
            fail_count++;                \
            printf("FAIL: %s\n", (msg)); \
            return;                      \
        } while (0)

#define ASSERT(cond, msg) \
        do {                  \
            if (!(cond))      \
            FAIL(msg);    \
        } while (0)

/*
 * Externally derived expected values.  Digest results are compared as raw
 * int64 -- never rendered through the intern table -- because a digest is a
 * value, and a collector that reversed ids would hide exactly the confusion
 * between the two domains that this file is about.
 */
#define XXH3_ALICE  8569093714482247167LL  /* "alice@example.com"        */
#define XXH3_BOB    8153412963705046890LL  /* "bob@example.com"          */
#define XXH3_CAROL  1513278612195422706LL  /* "carol@example.com"        */
#define XXH3_DAVE   (-6155219185708674874LL) /* "dave@example.com"       */
#define XXH3_AA     3932278706564862771LL  /* "aa"                       */
#define XXH3_ZZ     (-6040919500630484495LL) /* "zz"                     */
#define XXH3_UPPER_AA (-8874864312778924372LL) /* "AA"                   */
#define XXH3_I64_0  (-4072596861322023719LL) /* int64 0                  */
#define XXH3_I64_1  3439722301264460078LL  /* int64 1, little-endian     */
#define XXH3_I64_2  2343778756980564547LL  /* int64 2                    */
#define XXH3_I64_3  5589565451239960189LL  /* int64 3                    */
#define XXH3_I64_1000000 3181700808144611680LL
#define XXH3_I64_1000001 (-1343336171446265251LL)
/* XXH3 of the int64 XXH3_I64_1: the outer digest of hash(hash(x)). */
#define XXH3_OF_XXH3_I64_1 (-3387566715524744054LL)
#define CRC32E_AA   126491095LL            /* zlib.crc32(b"aa")          */
#define CRC32E_ZZ   618208161LL
#define CRC32C_AA   4059224770LL           /* CRC-32C of "aa"            */

#define MAX_SEEN 32
#define MAX_COLS 8

typedef struct {
    const char *rel;
    uint32_t count;
    uint32_t ncols[MAX_SEEN];
    int64_t rows[MAX_SEEN][MAX_COLS];
} collect_t;

/* Rows the caller wants inserted directly (physical layout), used by the
 * compound case where the fact cannot be written in source syntax. */
typedef struct {
    const char *relation;
    const int64_t *rows;
    uint32_t num_rows;
    uint32_t ncols;
} insert_spec_t;

static void
collect_cb(const char *relation, const int64_t *row, uint32_t ncols, void *user)
{
    collect_t *c = (collect_t *)user;
    if (!c->rel || !relation || strcmp(relation, c->rel) != 0)
        return;
    if (c->count < MAX_SEEN) {
        uint32_t n = ncols < MAX_COLS ? ncols : MAX_COLS;
        c->ncols[c->count] = n;
        for (uint32_t i = 0; i < n; i++)
            c->rows[c->count][i] = row[i];
    }
    c->count++;
}

/* True if any collected row equals the @n values at @want. */
static bool
saw(const collect_t *c, const int64_t *want, uint32_t n)
{
    uint32_t rows = c->count < MAX_SEEN ? c->count : MAX_SEEN;
    for (uint32_t i = 0; i < rows; i++) {
        if (c->ncols[i] != n)
            continue;
        bool match = true;
        for (uint32_t j = 0; j < n; j++) {
            if (c->rows[i][j] != want[j]) {
                match = false;
                break;
            }
        }
        if (match)
            return true;
    }
    return false;
}

/* True if any collected row has @v in column @col. */
static bool
saw_value(const collect_t *c, uint32_t col, int64_t v)
{
    uint32_t rows = c->count < MAX_SEEN ? c->count : MAX_SEEN;
    for (uint32_t i = 0; i < rows; i++) {
        if (col < c->ncols[i] && c->rows[i][col] == v)
            return true;
    }
    return false;
}

/*
 * Evaluate @src, filling @out with @relation's tuples.
 *
 * @prepare may be NULL.  When present it runs after parsing -- so the intern
 * table already holds the source facts' symbols -- and describes rows to
 * insert directly, which is how the mistyped-column and compound cases
 * supply facts that have no source syntax.  Returns 0 on success.
 */
static int
eval_relation_ex(const char *src, const char *relation, collect_t *out,
    int (*prepare)(const wirelog_program_t *prog, insert_spec_t *ins))
{
    memset(out, 0, sizeof(*out));
    out->rel = relation;

    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog) {
        fprintf(stderr, "  parse error: %d\n", (int)err);
        return -1;
    }

    insert_spec_t ins;
    memset(&ins, 0, sizeof(ins));
    if (prepare && prepare(prog, &ins) != 0) {
        wirelog_program_free(prog);
        return -1;
    }

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    if (wl_plan_from_program(prog, &plan) != 0) {
        wirelog_program_free(prog);
        return -1;
    }

    wl_session_t *sess = NULL;
    if (wl_session_create(wl_backend_columnar(), plan, 1, &sess) != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    int result = -1;
    if (wl_session_load_facts(sess, prog) == 0
        && (ins.relation == NULL
        || wl_session_insert(sess, ins.relation, ins.rows, ins.num_rows,
        ins.ncols) == 0)
        && wl_session_snapshot(sess, collect_cb, out) == 0)
        result = 0;

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return result;
}

static int
eval_relation(const char *src, const char *relation, collect_t *out)
{
    return eval_relation_ex(src, relation, out, NULL);
}

/* ---------------------------------------------------------------------- */

/*
 * The defect report, in its original setting: examples/04-hash-functions
 * fingerprints four e-mail addresses.  Before the fix column 4 held
 * XXH3_64bits of the int64 ids 1, 3, 5 and 8, so the same file produced
 * different fingerprints if a row was inserted above it.
 */
static void
test_hash_of_symbol_in_head_position(void)
{
    TEST("hash(sym) in a head projection digests the string's bytes");

    const char *src =
        ".decl record(id: int64, email: symbol)\n"
        "record(1, \"alice@example.com\").\n"
        "record(2, \"bob@example.com\").\n"
        "record(3, \"carol@example.com\").\n"
        "record(4, \"dave@example.com\").\n"
        ".decl fp(id: int64, h: int64)\n"
        "fp(id, hash(email)) :- record(id, email).\n";

    collect_t c;
    ASSERT(eval_relation(src, "fp", &c) == 0, "evaluation failed");
    ASSERT(c.count == 4, "expected one fingerprint per record");

    const int64_t want[4][2] = {
        { 1, XXH3_ALICE }, { 2, XXH3_BOB },
        { 3, XXH3_CAROL }, { 4, XXH3_DAVE },
    };
    for (int i = 0; i < 4; i++)
        ASSERT(saw(&c, want[i], 2), "fingerprint does not match xxhsum -H3");

    /* The pre-fix values were digests of the ids 1..4.  If any survives,
     * the string opcode was not emitted for this position. */
    ASSERT(!saw_value(&c, 1, XXH3_I64_1) && !saw_value(&c, 1, XXH3_I64_2),
        "a fingerprint is still the digest of an intern id");
    PASS();
}

static void
test_hash_of_symbol_in_filter_position(void)
{
    TEST("hash(sym) in a body filter digests the string's bytes");

    /* The literal is the value xxhsum prints for "alice@example.com"; only
     * a byte-wise digest can match it. */
    const char *src =
        ".decl record(id: int64, email: symbol)\n"
        "record(1, \"alice@example.com\").\n"
        "record(2, \"bob@example.com\").\n"
        ".decl found(id: int64)\n"
        "found(id) :- record(id, email), "
        "hash(email) = 8569093714482247167.\n";

    collect_t c;
    ASSERT(eval_relation(src, "found", &c) == 0, "evaluation failed");
    ASSERT(c.count == 1, "expected exactly the alice row");
    const int64_t want[1] = { 1 };
    ASSERT(saw(&c, want, 1), "expected found(1)");
    PASS();
}

static void
test_hash_is_independent_of_intern_order(void)
{
    TEST("the fingerprint of a symbol does not depend on intern order");

    /*
     * Identical rule and identical S facts.  The only difference is an
     * unrelated relation whose facts are parsed first and therefore claim
     * the lower ids.  Before the fix this changed every fingerprint.
     */
    const char *without =
        ".decl S(s: symbol)\n"
        "S(\"aa\").\n"
        "S(\"zz\").\n"
        ".decl H(h: int64)\n"
        "H(hash(s)) :- S(s).\n";

    const char *with_shift =
        ".decl Z(s: symbol)\n"
        "Z(\"pad0\").\n"
        "Z(\"pad1\").\n"
        "Z(\"pad2\").\n"
        ".decl S(s: symbol)\n"
        "S(\"aa\").\n"
        "S(\"zz\").\n"
        ".decl H(h: int64)\n"
        "H(hash(s)) :- S(s).\n";

    collect_t a, b;
    ASSERT(eval_relation(without, "H", &a) == 0, "evaluation failed (a)");
    ASSERT(eval_relation(with_shift, "H", &b) == 0, "evaluation failed (b)");

    const int64_t aa[1] = { XXH3_AA }, zz[1] = { XXH3_ZZ };
    ASSERT(a.count == 2 && b.count == 2, "expected two fingerprints each");
    ASSERT(saw(&a, aa, 1) && saw(&a, zz, 1), "wrong fingerprints (a)");
    ASSERT(saw(&b, aa, 1) && saw(&b, zz, 1), "wrong fingerprints (b)");
    PASS();
}

/*
 * The integer path is the half that must not move.  These four values are
 * the ones tests/test_hash_eval.c has always pinned; a blanket conversion
 * to the string opcodes would reverse a genuine integer and digest whatever
 * string happened to hold that id.
 */
static void
test_hash_of_integer_is_unchanged(void)
{
    TEST("hash(int64) still digests the 8-byte int64 representation");

    const char *src =
        ".decl N(x: int64)\n"
        "N(1).\n"
        "N(2).\n"
        ".decl H(x: int64, h: int64)\n"
        "H(x, hash(x)) :- N(x).\n";

    collect_t c;
    ASSERT(eval_relation(src, "H", &c) == 0, "evaluation failed");
    ASSERT(c.count == 2, "expected two rows");
    const int64_t one[2] = { 1, XXH3_I64_1 };
    const int64_t two[2] = { 2, XXH3_I64_2 };
    ASSERT(saw(&c, one, 2), "hash(1) moved");
    ASSERT(saw(&c, two, 2), "hash(2) moved");
    PASS();
}

/*
 * The sharp edge of the integer path.  Because a symbol-typed operand whose
 * reverse lookup fails falls back to the int64 digest, an emitter that
 * converted *everything* would still look correct on integers -- right up
 * to the point where an integer value happens to equal a live intern id,
 * and the digest silently becomes a digest of some unrelated string.  This
 * program interns four symbols first so that ids 0..3 all resolve, then
 * digests the integers 0..3.
 */
static int
require_low_ids_are_interned(const wirelog_program_t *prog, insert_spec_t *ins)
{
    const wirelog_intern_t *in = wirelog_program_get_intern(prog);
    (void)ins;
    if (!in)
        return -1;
    /* Without this the test is vacuous: the fallback would make a blanket
     * string opcode indistinguishable from the integer one. */
    for (int64_t id = 0; id < 4; id++) {
        if (wl_intern_reverse(in, id) == NULL)
            return -1;
    }
    return 0;
}

static void
test_integer_values_that_collide_with_intern_ids(void)
{
    TEST("hash(int64) is not diverted by an id that names a string");

    const char *src =
        ".decl S(s: symbol)\n"
        "S(\"aa\").\n"
        "S(\"zz\").\n"
        "S(\"mm\").\n"
        "S(\"qq\").\n"
        ".decl N(x: int64)\n"
        "N(0).\n"
        "N(1).\n"
        "N(2).\n"
        "N(3).\n"
        ".decl H(x: int64, h: int64)\n"
        "H(x, hash(x)) :- N(x).\n";

    collect_t c;
    ASSERT(eval_relation_ex(src, "H", &c, require_low_ids_are_interned) == 0,
        "evaluation failed, or ids 0..3 name no strings (test is vacuous)");
    ASSERT(c.count == 4, "expected four rows");

    const int64_t want[4][2] = {
        { 0, XXH3_I64_0 }, { 1, XXH3_I64_1 },
        { 2, XXH3_I64_2 }, { 3, XXH3_I64_3 },
    };
    for (int i = 0; i < 4; i++)
        ASSERT(saw(&c, want[i], 2), "an integer was digested as a symbol");
    /* The values a blanket string opcode would have produced. */
    ASSERT(!saw_value(&c, 1, XXH3_AA) && !saw_value(&c, 1, XXH3_ZZ),
        "digested the string an intern id happened to name");
    PASS();
}

static void
test_crc32_of_symbol_covers_the_string_bytes(void)
{
    TEST("crc32_ethernet/castagnoli(sym) cover strlen bytes, no NUL");

    /* examples/05-crc32-checksum's committed checksums are crc32 over the
     * payload with no NUL; before the fix every frame in that example was
     * reported corrupt.  "aa" is the same convention in miniature -- the
     * with-NUL answers differ from these. */
    const char *src =
        ".decl S(s: symbol)\n"
        "S(\"aa\").\n"
        "S(\"zz\").\n"
        ".decl C(e: int64, k: int64)\n"
        "C(crc32_ethernet(s), crc32_castagnoli(s)) :- S(s).\n";

    collect_t c;
    ASSERT(eval_relation(src, "C", &c) == 0, "evaluation failed");
    ASSERT(c.count == 2, "expected one row per symbol");
    const int64_t aa[2] = { CRC32E_AA, CRC32C_AA };
    ASSERT(saw(&c, aa, 2), "crc32 of \"aa\" does not match zlib/CRC-32C");
    ASSERT(saw_value(&c, 0, CRC32E_ZZ), "crc32_ethernet of \"zz\" is wrong");
    PASS();
}

static void
test_crc32_of_integer_is_unchanged(void)
{
    TEST("crc32_ethernet(int64) still covers the 8-byte representation");

    const char *src =
        ".decl N(x: int64)\n"
        "N(1).\n"
        ".decl C(k: int64)\n"
        "C(crc32_ethernet(x)) :- N(x).\n";

    collect_t c;
    ASSERT(eval_relation(src, "C", &c) == 0, "evaluation failed");
    ASSERT(c.count == 1, "expected one row");
    const int64_t want[1] = { 2844319735LL }; /* zlib.crc32(pack('<q', 1)) */
    ASSERT(saw(&c, want, 1), "crc32_ethernet(1) moved");
    PASS();
}

static void
test_string_function_results_are_digested_as_strings(void)
{
    TEST("hash(to_upper(sym)) digests \"AA\", not the runtime intern id");

    /* to_upper() interns its result at evaluation time, so its id depends
     * on row visit order -- there is no source ordering to appeal to and
     * the pre-fix answer was not even stable run to run. */
    const char *src =
        ".decl S(s: symbol)\n"
        "S(\"aa\").\n"
        ".decl H(h: int64)\n"
        "H(hash(to_upper(s))) :- S(s).\n";

    collect_t c;
    ASSERT(eval_relation(src, "H", &c) == 0, "evaluation failed");
    ASSERT(c.count == 1, "expected one row");
    const int64_t want[1] = { XXH3_UPPER_AA };
    ASSERT(saw(&c, want, 1), "expected xxhsum -H3 of \"AA\"");
    PASS();
}

static void
test_nested_digest_takes_the_integer_path(void)
{
    TEST("hash(hash(sym)) digests the inner result as the integer it is");

    /* expr_result_type() reports SCALAR for an arith expression, which is
     * right: no arith op produces a string, so the outer hash must not try
     * to reverse the inner one's output through the intern table. */
    const char *src =
        ".decl N(x: int64)\n"
        "N(1).\n"
        ".decl H(h: int64)\n"
        "H(hash(hash(x))) :- N(x).\n";

    collect_t c;
    ASSERT(eval_relation(src, "H", &c) == 0, "evaluation failed");
    ASSERT(c.count == 1, "expected one row");
    const int64_t want[1] = { XXH3_OF_XXH3_I64_1 };
    ASSERT(saw(&c, want, 1), "nested hash is not the integer digest");
    PASS();
}

/* ---------------------------------------------------------------------- */

/*
 * A `symbol` column holding values that were never interned.  `.decl` types
 * are not enforced, so this is reachable, and the emitter has no way to see
 * it -- the declaration is all it has.
 *
 * The engine digests the int64 representation of such a value, i.e. exactly
 * what the integer opcode would have produced, and the query still runs.
 * The alternative considered was to fail the row; in MAP position that is
 * not a dropped row but an ERANGE that aborts the whole PROJECT operator,
 * so a query that runs today would become `error: execution failed` with no
 * output at all.  This test pins the softer choice: both the values and,
 * just as importantly, that there *are* values.
 */
static int64_t mistyped_rows[2];

static int
mistyped_prepare(const wirelog_program_t *prog, insert_spec_t *ins)
{
    const wirelog_intern_t *in = wirelog_program_get_intern(prog);
    if (!in)
        return -1;
    /* Far above any id this program assigns, so the reverse lookup fails
     * rather than finding some unrelated string. */
    mistyped_rows[0] = 1000000;
    mistyped_rows[1] = 1000001;
    ins->relation = "S";
    ins->rows = mistyped_rows;
    ins->num_rows = 2;
    ins->ncols = 1;
    return 0;
}

static void
test_mistyped_declaration_digests_the_integer(void)
{
    TEST("a symbol column holding non-interned values still evaluates");

    const char *src =
        ".decl S(s: symbol)\n"
        ".decl H(h: int64)\n"
        "H(hash(s)) :- S(s).\n";

    collect_t c;
    ASSERT(eval_relation_ex(src, "H", &c, mistyped_prepare) == 0,
        "evaluation failed: the mistyped column aborted the query");
    ASSERT(c.count == 2, "expected one row per inserted value");
    const int64_t a[1] = { XXH3_I64_1000000 };
    const int64_t b[1] = { XXH3_I64_1000001 };
    ASSERT(saw(&c, a, 1) && saw(&c, b, 1),
        "expected the int64 digest of each un-interned value");
    ASSERT(!saw_value(&c, 0, XXH3_I64_0),
        "digested something other than the value itself");
    PASS();
}

/* ---------------------------------------------------------------------- */

/*
 * ev(id: int64, lbl: pair/2 inline, s: symbol) occupies four *physical*
 * columns -- [id][lbl_0][lbl_1][s] -- behind three logical ones.  A type
 * array indexed by physical position gives `s` the compound's type and runs
 * off the end, which #962 records as the plumbing bug it nearly shipped.
 * #963 reads the same array from a new position (arith operands rather than
 * comparison operands), so it gets its own case: if `s` loses its type here
 * the digest silently returns to hashing the id.
 */
#define COMPOUND_SRC                                                    \
        ".decl seed(s: symbol)\n"                                           \
        "seed(\"aa\").\n"                                                   \
        ".decl ev(id: int64, lbl: pair/2 inline, s: symbol)\n"              \
        ".decl H(h: int64)\n"                                               \
        "H(hash(s)) :- ev(i, pair(p, q), s).\n"

static int64_t compound_rows[4];
static int64_t compound_aa_id;

static int
compound_prepare(const wirelog_program_t *prog, insert_spec_t *ins)
{
    const wirelog_intern_t *in = wirelog_program_get_intern(prog);
    if (!in)
        return -1;
    int64_t aa = wl_intern_get(in, "aa");
    if (aa < 0)
        return -1;
    compound_aa_id = aa;

    compound_rows[0] = 1;  /* id     */
    compound_rows[1] = 7;  /* lbl_0  */
    compound_rows[2] = 8;  /* lbl_1  */
    compound_rows[3] = aa; /* s      */

    ins->relation = "ev";
    ins->rows = compound_rows;
    ins->num_rows = 1;
    ins->ncols = 4;
    return 0;
}

static void
test_inline_compound_relation(void)
{
    TEST("a symbol column after an inline compound keeps its type");

    collect_t c;
    ASSERT(eval_relation_ex(COMPOUND_SRC, "H", &c, compound_prepare) == 0,
        "evaluation failed");
    ASSERT(c.count == 1, "expected one row");
    const int64_t want[1] = { XXH3_AA };
    ASSERT(saw(&c, want, 1),
        "expected xxhsum -H3 of \"aa\"; the column's type was lost");
    PASS();
}

int
main(void)
{
    printf("=== digests over symbol columns (Issue #963) ===\n");

    test_hash_of_symbol_in_head_position();
    test_hash_of_symbol_in_filter_position();
    test_hash_is_independent_of_intern_order();
    test_hash_of_integer_is_unchanged();
    test_integer_values_that_collide_with_intern_ids();
    test_crc32_of_symbol_covers_the_string_bytes();
    test_crc32_of_integer_is_unchanged();
    test_string_function_results_are_digested_as_strings();
    test_nested_digest_takes_the_integer_path();
    test_mistyped_declaration_digests_the_integer();
    test_inline_compound_relation();

    printf("\n--- Results: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf(" ---\n");
    return (fail_count > 0) ? 1 : 0;
}
