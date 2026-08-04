/*
 * tests/test_doop_strings.c - string-typed DOOP rule semantics (Issue #950)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * The DOOP workload's rules used to carry thirteen bare integer IDs from the
 * dataset's original encoding, e.g.
 *
 *     Method_Descriptor(m, 2671384), Method_Modifier(760051, m)
 *     SubtypeOf(s, t) :- isInterfaceType(s), isType(t), t = 613907.
 *
 * Those IDs only meant anything relative to one encoder run, so the
 * benchmark could not be reproduced from a re-downloaded dataset: any
 * re-encoding assigns different numbers and silently changes the result.
 * They are now string constants, which are legible in review and stable
 * across any encoding.
 *
 * This test pins what each constant MEANS, on a miniature hand-written
 * dataset, so the meanings are fixed independently of the 68 MB archive --
 * which has already disappeared once and cannot be re-fetched in CI.
 *
 * Ten of the thirteen are covered here.  The other three appear only as
 * identity exclusions in MainMethodDeclaration:
 *
 *     m != 536048, m != 1057660, m != 796639
 *
 * Those are opaque method IDs, not names or types, so unlike the rest they
 * carry no recoverable meaning -- there is nothing to pin.  They exist to
 * keep MainMethodDeclaration a singleton, and that property is asserted
 * directly here and enforced against the real dataset, which is the
 * guarantee they were standing in for.  They are dropped rather than
 * translated; see Issue #950.
 *
 * Each case includes the negative that makes it non-vacuous. Asserting only
 * "the expected tuple appears" would pass against a rule that matches
 * everything, which is exactly how a constant could be wrong and unnoticed.
 *
 *   test_method_implemented_excludes_abstract
 *       !Method_Modifier("abstract", m) -- a method with a body is
 *       implemented; an abstract one is not.
 *
 *   test_main_method_declaration
 *       The conjunction of MainClass, simple name "main", descriptor
 *       "void(java.lang.String[])", and modifiers "public" and "static".
 *       Five negatives: wrong class, wrong name, wrong descriptor,
 *       missing static, missing public.  Both modifier negatives are needed --
 *       with only one, the other conjunct could be dropped or duplicated
 *       and nothing would notice.  The two conjuncts are symmetric, so
 *       this pins the SET {public, static} and not which literal was
 *       which; nothing can distinguish that, and nothing needs to.
 *
 *   test_class_initializer
 *       MethodImplemented("<clinit>", "void()", t, m) -- the class
 *       initializer is matched by simple name AND descriptor.  Negative: a
 *       same-named method with a different descriptor must not match.
 *
 *   test_subtype_of_object_and_array_interfaces
 *       Every interface and every array type is a subtype of
 *       "java.lang.Object"; array types are also subtypes of
 *       "java.lang.Cloneable" and "java.io.Serializable".  Those last two
 *       rules are structurally identical, so again this pins the set.
 */

#include "../wirelog/backend.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/intern.h"
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

#define MAX_SEEN 32

typedef struct {
    const char *rel;
    uint32_t count;
    const wirelog_intern_t *intern;
    /* Interned values of the row, joined with '|', for the first MAX_SEEN
     * rows.  Counting alone is not enough: swapping one rule constant for
     * another that happens to select a different-but-equally-sized set
     * leaves the count identical, so the test must look at what was
     * actually selected. */
    char seen[MAX_SEEN][256];
} collect_t;

static void
collect_cb(const char *relation, const int64_t *row, uint32_t ncols, void *user)
{
    collect_t *c = (collect_t *)user;
    if (!c->rel || !relation || strcmp(relation, c->rel) != 0)
        return;
    if (c->count < MAX_SEEN) {
        char *dst = c->seen[c->count];
        size_t pos = 0;
        dst[0] = '\0';
        for (uint32_t i = 0; i < ncols && pos + 1 < sizeof(c->seen[0]); i++) {
            const char *v = c->intern
                ? wl_intern_reverse(c->intern, row[i]) : NULL;
            int n = snprintf(dst + pos, sizeof(c->seen[0]) - pos, "%s%s",
                    i ? "|" : "", v ? v : "?");
            if (n < 0)
                break;
            pos += (size_t)n;
        }
    }
    c->count++;
}

/* True if any collected row equals @want. */
static bool
saw(const collect_t *c, const char *want)
{
    uint32_t n = c->count < MAX_SEEN ? c->count : MAX_SEEN;
    for (uint32_t i = 0; i < n; i++) {
        if (strcmp(c->seen[i], want) == 0)
            return true;
    }
    return false;
}

/* Evaluate @src, filling @out with @relation's tuples.  Returns 0 on
 * success. */
static int
eval_relation(const char *src, const char *relation, collect_t *out)
{
    memset(out, 0, sizeof(*out));
    out->rel = relation;

    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog) {
        fprintf(stderr, "  parse error: %d\n", (int)err);
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

    out->intern = wirelog_program_get_intern(prog);

    int result = -1;
    if (wl_session_load_facts(sess, prog) == 0
        && wl_session_snapshot(sess, collect_cb, out) == 0)
        result = 0;

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return result;
}

/*
 * MethodImplemented is "declared, and not abstract".  The abstract method
 * must be excluded; without the negation both would appear.
 */
static void
test_method_implemented_excludes_abstract(void)
{
    TEST("MethodImplemented excludes abstract methods");

    const char *src =
        ".decl Method_SimpleName(m: string, sn: string)\n"
        ".decl Method_Descriptor(m: string, d: string)\n"
        ".decl Method_DeclaringType(m: string, t: string)\n"
        ".decl Method_Modifier(mod: string, m: string)\n"
        "Method_SimpleName(\"<A: void f()>\", \"f\").\n"
        "Method_SimpleName(\"<A: void g()>\", \"g\").\n"
        "Method_Descriptor(\"<A: void f()>\", \"void()\").\n"
        "Method_Descriptor(\"<A: void g()>\", \"void()\").\n"
        "Method_DeclaringType(\"<A: void f()>\", \"A\").\n"
        "Method_DeclaringType(\"<A: void g()>\", \"A\").\n"
        "Method_Modifier(\"abstract\", \"<A: void g()>\").\n"
        ".decl MethodImplemented(sn: string, d: string, t: string, m: string)\n"
        "MethodImplemented(sn, d, t, m) :- Method_SimpleName(m, sn), "
        "Method_Descriptor(m, d), Method_DeclaringType(m, t), "
        "!Method_Modifier(\"abstract\", m).\n";

    collect_t c;
    ASSERT(eval_relation(src, "MethodImplemented", &c) == 0,
        "evaluation failed");
    ASSERT(c.count == 1, "expected exactly the non-abstract method");
    ASSERT(saw(&c, "f|void()|A|<A: void f()>"),
        "expected f(), the method with a body");
    PASS();
}

/* Shared prelude for the MainMethodDeclaration cases. */
#define MAIN_DECLS                                                       \
        ".decl MainClass(t: string)\n"                                       \
        ".decl Method_DeclaringType(m: string, t: string)\n"                 \
        ".decl Method_SimpleName(m: string, sn: string)\n"                   \
        ".decl Method_Descriptor(m: string, d: string)\n"                    \
        ".decl Method_Modifier(mod: string, m: string)\n"

#define MAIN_RULE                                                        \
        ".decl MainMethodDeclaration(m: string)\n"                           \
        "MainMethodDeclaration(m) :- MainClass(t), "                         \
        "Method_DeclaringType(m, t), Method_SimpleName(m, \"main\"), "       \
        "Method_Descriptor(m, \"void(java.lang.String[])\"), "              \
        "Method_Modifier(\"public\", m), Method_Modifier(\"static\", m).\n"

static void
test_main_method_declaration(void)
{
    TEST("MainMethodDeclaration pins name, descriptor and modifier set");

    /* The real entry point, plus four near-misses that must all be rejected:
     * wrong declaring class, wrong simple name, wrong descriptor, and no
     * static modifier. */
    const char *src =
        MAIN_DECLS
        "MainClass(\"App\").\n"
        "Method_DeclaringType(\"<App: void main(String[])>\", \"App\").\n"
        "Method_SimpleName(\"<App: void main(String[])>\", \"main\").\n"
        "Method_Descriptor(\"<App: void main(String[])>\", "
        "\"void(java.lang.String[])\").\n"
        "Method_Modifier(\"public\", \"<App: void main(String[])>\").\n"
        "Method_Modifier(\"static\", \"<App: void main(String[])>\").\n"
        /* not the main class */
        "Method_DeclaringType(\"<Other: void main(String[])>\", \"Other\").\n"
        "Method_SimpleName(\"<Other: void main(String[])>\", \"main\").\n"
        "Method_Descriptor(\"<Other: void main(String[])>\", "
        "\"void(java.lang.String[])\").\n"
        "Method_Modifier(\"public\", \"<Other: void main(String[])>\").\n"
        "Method_Modifier(\"static\", \"<Other: void main(String[])>\").\n"
        /* right class, wrong simple name */
        "Method_DeclaringType(\"<App: void run(String[])>\", \"App\").\n"
        "Method_SimpleName(\"<App: void run(String[])>\", \"run\").\n"
        "Method_Descriptor(\"<App: void run(String[])>\", "
        "\"void(java.lang.String[])\").\n"
        "Method_Modifier(\"public\", \"<App: void run(String[])>\").\n"
        "Method_Modifier(\"static\", \"<App: void run(String[])>\").\n"
        /* right name, wrong descriptor */
        "Method_DeclaringType(\"<App: void main(int)>\", \"App\").\n"
        "Method_SimpleName(\"<App: void main(int)>\", \"main\").\n"
        "Method_Descriptor(\"<App: void main(int)>\", \"void(int)\").\n"
        "Method_Modifier(\"public\", \"<App: void main(int)>\").\n"
        "Method_Modifier(\"static\", \"<App: void main(int)>\").\n"
        /* right name and descriptor and static, but not public: this is
         * what constrains the "public" literal.  Without it every method
         * carrying static also carried public, so dropping or swapping the
         * public conjunct changed nothing. */
        "Method_DeclaringType(\"<App: void main3(String[])>\", \"App\").\n"
        "Method_SimpleName(\"<App: void main3(String[])>\", \"main\").\n"
        "Method_Descriptor(\"<App: void main3(String[])>\", "
        "\"void(java.lang.String[])\").\n"
        "Method_Modifier(\"static\", \"<App: void main3(String[])>\").\n"
        /* right name and descriptor, but not static */
        "Method_DeclaringType(\"<App: void main2(String[])>\", \"App\").\n"
        "Method_SimpleName(\"<App: void main2(String[])>\", \"main\").\n"
        "Method_Descriptor(\"<App: void main2(String[])>\", "
        "\"void(java.lang.String[])\").\n"
        "Method_Modifier(\"public\", \"<App: void main2(String[])>\").\n"
        MAIN_RULE;

    collect_t c;
    ASSERT(eval_relation(src, "MainMethodDeclaration", &c) == 0,
        "evaluation failed");
    ASSERT(c.count == 1, "expected exactly one entry point");
    /* Naming the method is what makes each constant load-bearing: swapping
     * "main" for "run", or the descriptor for "int", selects a different
     * near-miss and keeps the count at 1. */
    ASSERT(saw(&c, "<App: void main(String[])>"),
        "expected the real entry point, not a near-miss");
    PASS();
}

/*
 * The class initializer is matched by simple name AND descriptor.  A
 * same-named method taking arguments must not match, which is what makes the
 * descriptor constant load-bearing rather than decorative.
 */
static void
test_class_initializer(void)
{
    TEST("ClassInitializer matches <clinit> with the void() descriptor");

    const char *src =
        ".decl MethodImplemented(sn: string, d: string, t: string, m: string)\n"
        "MethodImplemented(\"<clinit>\", \"void()\", \"A\", "
        "\"<A: void clinit()>\").\n"
        "MethodImplemented(\"<clinit>\", \"void(int)\", \"A\", "
        "\"<A: void clinit(int)>\").\n"
        "MethodImplemented(\"<init>\", \"void()\", \"A\", "
        "\"<A: void init()>\").\n"
        ".decl ClassInitializer(t: string, m: string)\n"
        "ClassInitializer(t, m) :- "
        "MethodImplemented(\"<clinit>\", \"void()\", t, m).\n";

    collect_t c;
    ASSERT(eval_relation(src, "ClassInitializer", &c) == 0,
        "evaluation failed");
    ASSERT(c.count == 1, "expected only the zero-argument <clinit>");
    ASSERT(saw(&c, "A|<A: void clinit()>"),
        "expected the no-argument <clinit>, not the int overload");
    PASS();
}

/*
 * Every interface and every array type is a subtype of java.lang.Object, and
 * array types are additionally subtypes of the two interfaces arrays
 * implement.  The two array-interface rules are structurally identical, so
 * this pins the set {java.lang.Cloneable, java.io.Serializable}.
 */
static void
test_subtype_of_object_and_array_interfaces(void)
{
    TEST("SubtypeOf relates interfaces and arrays to Object and array ifaces");

    const char *src =
        ".decl isInterfaceType(t: string)\n"
        ".decl isArrayType(t: string)\n"
        ".decl isType(t: string)\n"
        "isInterfaceType(\"I\").\n"
        "isArrayType(\"A[]\").\n"
        "isType(\"java.lang.Object\").\n"
        "isType(\"java.lang.Cloneable\").\n"
        "isType(\"java.io.Serializable\").\n"
        "isType(\"Unrelated\").\n"
        "isInterfaceType(\"java.lang.Cloneable\").\n"
        "isInterfaceType(\"java.io.Serializable\").\n"
        ".decl SubtypeOf(s: string, t: string)\n"
        "SubtypeOf(s, t) :- isInterfaceType(s), isType(t), "
        "t = \"java.lang.Object\".\n"
        "SubtypeOf(s, t) :- isArrayType(s), isType(t), "
        "t = \"java.lang.Object\".\n"
        "SubtypeOf(s, t) :- isArrayType(s), isInterfaceType(t), isType(t), "
        "t = \"java.lang.Cloneable\".\n"
        "SubtypeOf(s, t) :- isArrayType(s), isInterfaceType(t), isType(t), "
        "t = \"java.io.Serializable\".\n";

    /* isInterfaceType = {I, Cloneable, Serializable}, isArrayType = {A[]}.
     * Object rules give 3 + 1 = 4; the two array-interface rules give 1 each.
     * "Unrelated" is in isType but must never appear as a supertype. */
    collect_t c;
    ASSERT(eval_relation(src, "SubtypeOf", &c) == 0, "evaluation failed");
    ASSERT(c.count == 6,
        "expected 4 Object pairs plus 2 array-interface pairs");
    ASSERT(saw(&c, "I|java.lang.Object"), "interface must subtype Object");
    ASSERT(saw(&c, "A[]|java.lang.Object"), "array must subtype Object");
    ASSERT(saw(&c, "A[]|java.lang.Cloneable"), "array must subtype Cloneable");
    ASSERT(saw(&c, "A[]|java.io.Serializable"),
        "array must subtype Serializable");
    ASSERT(!saw(&c, "I|Unrelated"), "Unrelated must never be a supertype");
    PASS();
}

/*
 * Issue #956.  Every other test here supplies Method_Descriptor as EDB
 * facts, so none of them reaches the rule that DERIVES it from _Method --
 * which is exactly how the wrong column survived review.  This one derives
 * it.  The rule text is DUPLICATED from bench/bench_flowlog.c by hand and
 * kept in step by hand -- reverting the benchmark's rule alone breaks
 * nothing here.  Only scripts/run_doop_validation.sh would catch that,
 * and it is not in CI.
 *
 * Two things must be pinned, and a count alone pins neither:
 *
 *   1. The FORMAT.  Asserting only cardinality passes under cat(rt, p)
 *      with no parens, and under the arguments transposed, because every
 *      descriptor stays distinct either way.  So the exact strings are
 *      asserted, including one with NON-EMPTY params -- with only
 *      empty-parameter methods, "void()" and "()void" are the same string
 *      reversed and transposition survives.
 *
 *   2. The CONSEQUENCE.  The reason the column matters is that params
 *      alone collapses covariant overrides: LongPipeline declares two
 *      iterator() methods differing only in return type, so under the
 *      params reading the subclass entry shadows the inherited one and
 *      the !MethodImplemented antijoin deletes it.  Under the correct
 *      reading the descriptors differ and both survive.  The grounding
 *      pair is real -- 456 such superclass-spanning pairs exist in
 *      Method.facts.
 */
static void
test_method_descriptor_is_return_type_and_params(void)
{
    TEST("Method_Descriptor derives returnType(params), not params alone");

    /* _Method columns, per doop_edbs[] in bench/bench_flowlog.c:
     * (method, simplename, params, declaringType, returnType,
     *  jvmDescriptor, arity) */
    const char *src =
        ".decl _Method(m: string, sn: string, p: string, t: string, "
        "rt: string, jvm: string, ar: string)\n"
        "_Method(\"<App: void main(java.lang.String[])>\", \"main\", "
        "\"java.lang.String[]\", \"App\", \"void\", "
        "\"([Ljava/lang/String;)V\", \"1\").\n"
        "_Method(\"<App: void <clinit>()>\", \"<clinit>\", \"\", \"App\", "
        "\"void\", \"()V\", \"0\").\n"
        ".decl Method_Descriptor(method: string, descriptor: string)\n"
        "Method_Descriptor(m, cat(cat(cat(rt, \"(\"), p), \")\")) :- "
        "_Method(m, _, p, _, rt, _, _).\n";

    collect_t c;
    ASSERT(eval_relation(src, "Method_Descriptor", &c) == 0,
        "evaluation failed");
    ASSERT(c.count == 2, "expected one descriptor per method");
    /* Non-empty params: kills transposition and the missing-paren forms. */
    ASSERT(saw(&c, "<App: void main(java.lang.String[])>|"
        "void(java.lang.String[])"),
        "expected returnType(params), not params alone");
    /* Empty params: pins that the parens are emitted even when empty, so
     * <clinit> still matches the "void()" constant the rules use. */
    ASSERT(saw(&c, "<App: void <clinit>()>|void()"),
        "expected void() for a no-argument method, not the empty string");
    PASS();
}

/*
 * The consequence of getting that column wrong, in miniature: a covariant
 * override across a DirectSuperclass edge.  Under the params reading both
 * iterator() methods carry the same descriptor, the subclass entry shadows
 * the superclass one, and the antijoin drops the inherited tuple.
 */
static void
test_covariant_override_survives_lookup(void)
{
    TEST("MethodLookup keeps covariant overrides distinct");

    const char *src =
        ".decl Method_SimpleName(m: string, sn: string)\n"
        ".decl Method_DeclaringType(m: string, t: string)\n"
        ".decl Method_Modifier(mod: string, m: string)\n"
        ".decl DirectSuperclass(t: string, st: string)\n"
        ".decl _Method(m: string, sn: string, p: string, t: string, "
        "rt: string, jvm: string, ar: string)\n"
        /* Superclass declares the covariant pair; the subclass declares
         * only the erased one.  Return types differ, params do not. */
        "_Method(\"<Sup: I it()>\", \"it\", \"\", \"Sup\", \"I\", "
        "\"()LI;\", \"0\").\n"
        "_Method(\"<Sup: OfLong it()>\", \"it\", \"\", \"Sup\", "
        "\"OfLong\", \"()LOfLong;\", \"0\").\n"
        "_Method(\"<Sub: I it()>\", \"it\", \"\", \"Sub\", \"I\", "
        "\"()LI;\", \"0\").\n"
        "Method_SimpleName(\"<Sup: I it()>\", \"it\").\n"
        "Method_SimpleName(\"<Sup: OfLong it()>\", \"it\").\n"
        "Method_SimpleName(\"<Sub: I it()>\", \"it\").\n"
        "Method_DeclaringType(\"<Sup: I it()>\", \"Sup\").\n"
        "Method_DeclaringType(\"<Sup: OfLong it()>\", \"Sup\").\n"
        "Method_DeclaringType(\"<Sub: I it()>\", \"Sub\").\n"
        "DirectSuperclass(\"Sub\", \"Sup\").\n"
        ".decl Method_Descriptor(method: string, descriptor: string)\n"
        "Method_Descriptor(m, cat(cat(cat(rt, \"(\"), p), \")\")) :- "
        "_Method(m, _, p, _, rt, _, _).\n"
        ".decl MethodImplemented(sn: string, d: string, t: string, "
        "m: string)\n"
        "MethodImplemented(sn, d, t, m) :- Method_SimpleName(m, sn), "
        "Method_Descriptor(m, d), Method_DeclaringType(m, t), "
        "!Method_Modifier(\"abstract\", m).\n"
        ".decl MethodLookup(sn: string, d: string, t: string, m: string)\n"
        "MethodLookup(sn, d, t, m) :- MethodImplemented(sn, d, t, m).\n"
        "MethodLookup(sn, d, t, m) :- DirectSuperclass(t, st), "
        "MethodLookup(sn, d, st, m), !MethodImplemented(sn, d, t, _).\n";

    collect_t c;
    ASSERT(eval_relation(src, "MethodLookup", &c) == 0,
        "evaluation failed");
    /* 3 declared + the OfLong variant inherited by Sub.  Under the params
     * reading the inherited tuple is shadowed and this is 3. */
    ASSERT(c.count == 4, "expected the inherited covariant variant to "
        "survive; params-only descriptors shadow it");
    ASSERT(saw(&c, "it|OfLong()|Sub|<Sup: OfLong it()>"),
        "Sub must inherit the covariant OfLong variant from Sup");
    PASS();
}

int
main(void)
{
    printf("=== DOOP string-constant rule semantics (Issue #950) ===\n");

    test_method_descriptor_is_return_type_and_params();
    test_covariant_override_survives_lookup();
    test_method_implemented_excludes_abstract();
    test_main_method_declaration();
    test_class_initializer();
    test_subtype_of_object_and_array_interfaces();

    printf("\n--- Results: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf(" ---\n");
    return (fail_count > 0) ? 1 : 0;
}
