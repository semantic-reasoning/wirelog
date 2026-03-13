/*
 * test_bitwise_integration.c - Integration tests for bitwise operators (Issue #72)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * End-to-end integration test demonstrating all 6 bitwise operators in a
 * realistic protocol packet analysis scenario:
 *
 *   band(x, mask)  - bitwise AND  (extract nibble from byte)
 *   bor(x, y)      - bitwise OR   (reconstruct 16-bit word from bytes)
 *   bxor(x, y)     - bitwise XOR  (toggle flags)
 *   bnot(x)        - bitwise NOT  (invert byte)
 *   bshl(x, n)     - shift left   (shift high byte into position)
 *   bshr(x, n)     - shift right  (extract high nibble)
 *
 * Packet format: byte_val encodes [proto_version:4 | msg_type:4]
 * Flags format:  bit 7 = error flag (0x80 = 128)
 *
 * Test data:
 *   packet(1, 165, 128)  -- byte_val=0xA5, flags=0x80 (error set)
 *   packet(2, 115, 0)    -- byte_val=0x73, flags=0x00 (no error)
 *   hi_byte(1, 202)      -- 0xCA for 16-bit word reconstruction
 *   lo_byte(1, 254)      -- 0xFE for 16-bit word reconstruction
 *
 * Expected derived results:
 *   msg_type(1, 5)       -- 165 & 15  = 0xA5 & 0x0F = 5
 *   msg_type(2, 3)       -- 115 & 15  = 0x73 & 0x0F = 3
 *   proto_ver(1, 10)     -- 165 >> 4  = 0xA5 >> 4 = 10 (0xA)
 *   proto_ver(2, 7)      -- 115 >> 4  = 0x73 >> 4 = 7
 *   has_error(1)         -- 128 & 128 = 128 != 0  -> present
 *   (no has_error(2))    -- 0 & 128   = 0    == 0 -> absent
 *   word16(1, 51966)     -- bor(bshl(202,8), 254) = bor(51712, 254) = 0xCAFE
 *   flags_xor(1, 127)    -- 128 ^ 255 = 0x7F = 127
 *   flags_xor(2, 255)    -- 0   ^ 255 = 0xFF = 255
 *   inv_byte(1, -166)    -- ~165 (two's complement int64)
 *   inv_byte(2, -116)    -- ~115 (two's complement int64)
 */

#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Test Framework                                                           */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                                        \
    do {                                                  \
        tests_run++;                                      \
        printf("  [TEST] %-60s", (name));                 \
        fflush(stdout);                                   \
    } while (0)

#define PASS()              \
    do {                    \
        tests_passed++;     \
        printf(" PASS\n");  \
    } while (0)

#define FAIL(msg)                    \
    do {                             \
        tests_failed++;              \
        printf(" FAIL: %s\n", (msg)); \
        return;                      \
    } while (0)

#define ASSERT(cond, msg) \
    do {                  \
        if (!(cond))      \
            FAIL(msg);    \
    } while (0)

/* ======================================================================== */
/* Tuple Collector                                                          */
/* ======================================================================== */

#define MAX_TUPLES 512
#define MAX_COLS   8

typedef struct {
    int count;
    char relations[MAX_TUPLES][64];
    int64_t rows[MAX_TUPLES][MAX_COLS];
    uint32_t ncols[MAX_TUPLES];
} tuple_collector_t;

static void
collect_tuple(const char *relation, const int64_t *row, uint32_t ncols,
              void *user_data)
{
    tuple_collector_t *c = (tuple_collector_t *)user_data;
    if (c->count >= MAX_TUPLES)
        return;
    int idx = c->count++;
    strncpy(c->relations[idx], relation, 63);
    c->relations[idx][63] = '\0';
    c->ncols[idx] = ncols < MAX_COLS ? ncols : MAX_COLS;
    for (uint32_t i = 0; i < c->ncols[idx]; i++)
        c->rows[idx][i] = row[i];
}

static bool
has_tuple(const tuple_collector_t *c, const char *relation,
          const int64_t *expected, uint32_t ncols)
{
    for (int i = 0; i < c->count; i++) {
        if (strcmp(c->relations[i], relation) != 0)
            continue;
        if (c->ncols[i] != ncols)
            continue;
        bool match = true;
        for (uint32_t j = 0; j < ncols; j++) {
            if (c->rows[i][j] != expected[j]) {
                match = false;
                break;
            }
        }
        if (match)
            return true;
    }
    return false;
}

/* ======================================================================== */
/* Pipeline Helper                                                          */
/* ======================================================================== */

/*
 * Compile Datalog source, run optimizers, generate plan, create columnar
 * session, load inline facts, snapshot into collector.
 * Returns 0 on success, -1 on error.
 */
static int
run_program(const char *src, tuple_collector_t *out)
{
    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog) {
        fprintf(stderr, "parse error: %d\n", (int)err);
        return -1;
    }

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0) {
        wirelog_program_free(prog);
        fprintf(stderr, "plan generation failed\n");
        return -1;
    }

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    if (rc != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        fprintf(stderr, "session create failed\n");
        return -1;
    }

    rc = wl_session_load_facts(sess, prog);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        fprintf(stderr, "load facts failed\n");
        return -1;
    }

    memset(out, 0, sizeof(*out));
    rc = wl_session_snapshot(sess, collect_tuple, out);

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return rc;
}

/* ======================================================================== */
/* Test: band + bshr — extract nibbles from byte                           */
/* ======================================================================== */

static void
test_nibble_extraction(void)
{
    TEST("band(x,15) extracts low nibble, bshr(x,4) extracts high nibble");

    const char *src =
        ".decl packet(id: int64, byte_val: int64, flags: int64)\n"
        "packet(1, 165, 128).\n" /* 0xA5, error set */
        "packet(2, 115, 0).\n"   /* 0x73, no error */
        ".decl msg_type(id: int64, mtype: int64)\n"
        "msg_type(Id, band(ByteVal, 15)) :- packet(Id, ByteVal, Flags).\n"
        ".decl proto_ver(id: int64, version: int64)\n"
        "proto_ver(Id, bshr(ByteVal, 4)) :- packet(Id, ByteVal, Flags).\n";

    tuple_collector_t tuples;
    ASSERT(run_program(src, &tuples) == 0, "program execution failed");

    /* band(165, 15) = 0xA5 & 0x0F = 5 */
    int64_t mt1[] = { 1, 5 };
    ASSERT(has_tuple(&tuples, "msg_type", mt1, 2),
           "expected msg_type(1,5): band(165,15)=5");

    /* band(115, 15) = 0x73 & 0x0F = 3 */
    int64_t mt2[] = { 2, 3 };
    ASSERT(has_tuple(&tuples, "msg_type", mt2, 2),
           "expected msg_type(2,3): band(115,15)=3");

    /* bshr(165, 4) = 0xA5 >> 4 = 10 */
    int64_t pv1[] = { 1, 10 };
    ASSERT(has_tuple(&tuples, "proto_ver", pv1, 2),
           "expected proto_ver(1,10): bshr(165,4)=10");

    /* bshr(115, 4) = 0x73 >> 4 = 7 */
    int64_t pv2[] = { 2, 7 };
    ASSERT(has_tuple(&tuples, "proto_ver", pv2, 2),
           "expected proto_ver(2,7): bshr(115,4)=7");

    PASS();
}

/* ======================================================================== */
/* Test: band in comparison — check error flag                             */
/* ======================================================================== */

static void
test_error_flag_check(void)
{
    TEST("band(Flags,128) != 0 detects error flag in bit 7");

    const char *src =
        ".decl packet(id: int64, byte_val: int64, flags: int64)\n"
        "packet(1, 165, 128).\n" /* flags=0x80: error set */
        "packet(2, 115, 0).\n"   /* flags=0x00: no error */
        ".decl has_error(id: int64)\n"
        "has_error(Id) :- packet(Id, ByteVal, Flags), band(Flags, 128) != 0.\n";

    tuple_collector_t tuples;
    ASSERT(run_program(src, &tuples) == 0, "program execution failed");

    /* packet 1: band(128, 128) = 128 != 0 -> has_error(1) */
    int64_t he1[] = { 1 };
    ASSERT(has_tuple(&tuples, "has_error", he1, 1),
           "expected has_error(1): band(128,128)=128 != 0");

    /* packet 2: band(0, 128) = 0 == 0 -> no has_error(2) */
    int64_t he2[] = { 2 };
    ASSERT(!has_tuple(&tuples, "has_error", he2, 1),
           "unexpected has_error(2): band(0,128)=0 should not trigger");

    PASS();
}

/* ======================================================================== */
/* Test: bshl + bor — reconstruct 16-bit word from two bytes              */
/* ======================================================================== */

static void
test_word_reconstruction(void)
{
    TEST("bor(bshl(Hi,8),Lo) reconstructs 16-bit word from two bytes");

    const char *src =
        ".decl hi_byte(id: int64, val: int64)\n"
        ".decl lo_byte(id: int64, val: int64)\n"
        "hi_byte(1, 202).\n" /* 0xCA */
        "lo_byte(1, 254).\n" /* 0xFE */
        ".decl word16(id: int64, val: int64)\n"
        "word16(Id, bor(bshl(Hi, 8), Lo)) :- hi_byte(Id, Hi), lo_byte(Id, Lo).\n";

    tuple_collector_t tuples;
    ASSERT(run_program(src, &tuples) == 0, "program execution failed");

    /* bshl(202, 8) = 202 * 256 = 51712 = 0xCA00
     * bor(51712, 254) = 0xCA00 | 0xFE = 0xCAFE = 51966 */
    int64_t w[] = { 1, 51966 };
    ASSERT(has_tuple(&tuples, "word16", w, 2),
           "expected word16(1,51966): bor(bshl(202,8),254)=0xCAFE=51966");

    PASS();
}

/* ======================================================================== */
/* Test: bxor — toggle flag bits                                           */
/* ======================================================================== */

static void
test_bxor_flag_toggle(void)
{
    TEST("bxor(Flags,255) toggles all flag bits");

    const char *src =
        ".decl packet(id: int64, byte_val: int64, flags: int64)\n"
        "packet(1, 165, 128).\n" /* flags=0x80 */
        "packet(2, 115, 0).\n"   /* flags=0x00 */
        ".decl flags_xor(id: int64, val: int64)\n"
        "flags_xor(Id, bxor(Flags, 255)) :- packet(Id, ByteVal, Flags).\n";

    tuple_collector_t tuples;
    ASSERT(run_program(src, &tuples) == 0, "program execution failed");

    /* bxor(128, 255) = 0x80 ^ 0xFF = 0x7F = 127 */
    int64_t fx1[] = { 1, 127 };
    ASSERT(has_tuple(&tuples, "flags_xor", fx1, 2),
           "expected flags_xor(1,127): bxor(128,255)=127");

    /* bxor(0, 255) = 0x00 ^ 0xFF = 0xFF = 255 */
    int64_t fx2[] = { 2, 255 };
    ASSERT(has_tuple(&tuples, "flags_xor", fx2, 2),
           "expected flags_xor(2,255): bxor(0,255)=255");

    PASS();
}

/* ======================================================================== */
/* Test: bnot — bitwise complement                                         */
/* ======================================================================== */

static void
test_bnot_complement(void)
{
    TEST("bnot(x) computes bitwise NOT (two's complement int64)");

    const char *src =
        ".decl byte_val(id: int64, val: int64)\n"
        "byte_val(1, 165).\n" /* 0xA5 */
        "byte_val(2, 115).\n" /* 0x73 */
        ".decl inv_byte(id: int64, val: int64)\n"
        "inv_byte(Id, bnot(Val)) :- byte_val(Id, Val).\n";

    tuple_collector_t tuples;
    ASSERT(run_program(src, &tuples) == 0, "program execution failed");

    /* bnot(165) = ~165 = -166 (int64 two's complement) */
    int64_t ib1[] = { 1, -166 };
    ASSERT(has_tuple(&tuples, "inv_byte", ib1, 2),
           "expected inv_byte(1,-166): bnot(165)=~165=-166");

    /* bnot(115) = ~115 = -116 */
    int64_t ib2[] = { 2, -116 };
    ASSERT(has_tuple(&tuples, "inv_byte", ib2, 2),
           "expected inv_byte(2,-116): bnot(115)=~115=-116");

    PASS();
}

/* ======================================================================== */
/* Test: full protocol analysis — all 6 operators together                */
/* ======================================================================== */

static void
test_protocol_analysis_end_to_end(void)
{
    TEST("full protocol analysis: all 6 bitwise operators together");

    const char *src =
        /* Raw packet data: (id, byte_val, flags) */
        ".decl packet(id: int64, byte_val: int64, flags: int64)\n"
        "packet(1, 165, 128).\n" /* id=1: byte=0xA5, flags=0x80 (error) */
        "packet(2, 115, 0).\n"   /* id=2: byte=0x73, flags=0x00 (clean) */

        /* Bytes for 16-bit word reconstruction */
        ".decl hi_byte(id: int64, val: int64)\n"
        ".decl lo_byte(id: int64, val: int64)\n"
        "hi_byte(1, 202).\n" /* 0xCA */
        "lo_byte(1, 254).\n" /* 0xFE */

        /* band: extract low nibble (message type) */
        ".decl msg_type(id: int64, mtype: int64)\n"
        "msg_type(Id, band(ByteVal, 15)) :- packet(Id, ByteVal, Flags).\n"

        /* bshr: extract high nibble (protocol version) */
        ".decl proto_ver(id: int64, version: int64)\n"
        "proto_ver(Id, bshr(ByteVal, 4)) :- packet(Id, ByteVal, Flags).\n"

        /* band in comparison: detect error flag (bit 7) */
        ".decl has_error(id: int64)\n"
        "has_error(Id) :- packet(Id, ByteVal, Flags), band(Flags, 128) != 0.\n"

        /* bshl + bor: reconstruct 16-bit word */
        ".decl word16(id: int64, val: int64)\n"
        "word16(Id, bor(bshl(Hi, 8), Lo)) :- hi_byte(Id, Hi), lo_byte(Id, Lo).\n"

        /* bxor: toggle all flag bits */
        ".decl flags_xor(id: int64, val: int64)\n"
        "flags_xor(Id, bxor(Flags, 255)) :- packet(Id, ByteVal, Flags).\n"

        /* bnot: bitwise complement of byte */
        ".decl inv_byte(id: int64, val: int64)\n"
        "inv_byte(Id, bnot(ByteVal)) :- packet(Id, ByteVal, Flags).\n";

    tuple_collector_t tuples;
    ASSERT(run_program(src, &tuples) == 0, "program execution failed");

    char msg[128];

    /* --- band: low nibble extraction --- */
    int64_t mt1[] = { 1, 5 };   /* 0xA5 & 0x0F = 5 */
    int64_t mt2[] = { 2, 3 };   /* 0x73 & 0x0F = 3 */
    snprintf(msg, sizeof(msg), "msg_type(1,5): band(165,15)=%d", (int)(165 & 15));
    ASSERT(has_tuple(&tuples, "msg_type", mt1, 2), msg);
    snprintf(msg, sizeof(msg), "msg_type(2,3): band(115,15)=%d", (int)(115 & 15));
    ASSERT(has_tuple(&tuples, "msg_type", mt2, 2), msg);

    /* --- bshr: high nibble extraction --- */
    int64_t pv1[] = { 1, 10 };  /* 0xA5 >> 4 = 10 */
    int64_t pv2[] = { 2, 7 };   /* 0x73 >> 4 = 7 */
    ASSERT(has_tuple(&tuples, "proto_ver", pv1, 2),
           "proto_ver(1,10): bshr(165,4)=10");
    ASSERT(has_tuple(&tuples, "proto_ver", pv2, 2),
           "proto_ver(2,7): bshr(115,4)=7");

    /* --- band in comparison: error flag --- */
    int64_t he1[] = { 1 };
    int64_t he2[] = { 2 };
    ASSERT(has_tuple(&tuples, "has_error", he1, 1),
           "has_error(1): band(128,128)=128 != 0");
    ASSERT(!has_tuple(&tuples, "has_error", he2, 1),
           "no has_error(2): band(0,128)=0");

    /* --- bshl + bor: 16-bit word reconstruction --- */
    int64_t w[] = { 1, 51966 }; /* bor(bshl(0xCA,8), 0xFE) = 0xCAFE = 51966 */
    ASSERT(has_tuple(&tuples, "word16", w, 2),
           "word16(1,51966): bor(bshl(202,8),254)=0xCAFE");

    /* --- bxor: flag bit toggle --- */
    int64_t fx1[] = { 1, 127 }; /* 0x80 ^ 0xFF = 0x7F = 127 */
    int64_t fx2[] = { 2, 255 }; /* 0x00 ^ 0xFF = 0xFF = 255 */
    ASSERT(has_tuple(&tuples, "flags_xor", fx1, 2),
           "flags_xor(1,127): bxor(128,255)=127");
    ASSERT(has_tuple(&tuples, "flags_xor", fx2, 2),
           "flags_xor(2,255): bxor(0,255)=255");

    /* --- bnot: bitwise complement --- */
    int64_t ib1[] = { 1, -166 }; /* ~165 = -166 */
    int64_t ib2[] = { 2, -116 }; /* ~115 = -116 */
    ASSERT(has_tuple(&tuples, "inv_byte", ib1, 2),
           "inv_byte(1,-166): bnot(165)=-166");
    ASSERT(has_tuple(&tuples, "inv_byte", ib2, 2),
           "inv_byte(2,-116): bnot(115)=-116");

    PASS();
}

/* ======================================================================== */
/* Main                                                                    */
/* ======================================================================== */

int
main(void)
{
    printf("=== Bitwise Operators Integration Tests (Issue #72) ===\n\n");
    printf("Protocol analysis scenario:\n");
    printf("  packet(1, 0xA5=165, 0x80=128): error set, byte=[ver=A,type=5]\n");
    printf("  packet(2, 0x73=115, 0x00=0):   clean,    byte=[ver=7,type=3]\n");
    printf("  hi_byte(1, 0xCA=202) + lo_byte(1, 0xFE=254) -> 0xCAFE\n\n");

    printf("--- Operator tests ---\n");
    test_nibble_extraction();
    test_error_flag_check();
    test_word_reconstruction();
    test_bxor_flag_toggle();
    test_bnot_complement();

    printf("\n--- End-to-end protocol analysis ---\n");
    test_protocol_analysis_end_to_end();

    printf("\n  %d tests: %d passed, %d failed\n",
           tests_run, tests_passed, tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
