# Hash Functions: Deduplication and Checksum Validation

## Overview

This example demonstrates the `hash()` built-in function for three practical tasks:

1. **Email fingerprinting** - collapse a string field to a single integer for fast comparison
2. **Deduplication** - identify and remove duplicate registrations using content hashes
3. **Checksum validation** - detect records corrupted or tampered with after ingest

**Real-world use cases:**
- Deduplicating user registrations from multiple sign-up sources
- Detecting data corruption in stored records
- Building compact content-addressable identifiers

## Files

- **records.csv** - User records `(id, name, email)` from multiple registration sources.
  Records with ids 1/4 and 2/6 share an email address — they represent the same person
  registered twice.

- **checksums.csv** - Per-record integrity checksums `(id, stored_checksum)` captured at
  ingest time. Record id=5 has a tampered checksum to demonstrate corruption detection.

- **hash_dedup.dl** - Datalog program implementing all three hash() use cases.

- **expected_deduplicated.csv** - Expected output of the deduplication step (4 unique users).

- **unique_records_output.csv** - Generated: deduplicated user records (one per email address).
- **fingerprints_output.csv** - Generated: each record annotated with its email hash.
- **valid_records_output.csv** - Generated: records whose stored checksum matches `hash(id)`.
- **corrupted_records_output.csv** - Generated: records whose stored checksum does not match.

## How It Works

### Part 1: Email Fingerprints

```datalog
email_fp(id, name, email, hash(email)) :- record(id, name, email).
```

`hash(email)` maps the email string to an `int64`. Two records with the same email produce
the same fingerprint, making duplicate detection a simple integer comparison.

Example output (fingerprints_output.csv):
```
1,alice,alice@example.com,8569093714482247167
4,alice_dup,alice@example.com,8569093714482247167   <- same fp as id=1
2,bob,bob@example.com,8153412963705046890
6,bob_dup,bob@example.com,8153412963705046890       <- same fp as id=2
```

The fingerprint is a property of the address, nothing else. It is xxHash3 over the
address's own bytes, so it reproduces outside wirelog:

```
$ printf 'alice@example.com' | xxhsum -H3
XXH3_76eb895512bf35ff  stdin        # = 8569093714482247167 as int64
```

That also means the values do not move when the input file changes around them.
Until #963, `hash()` digested the *interned id* of the string rather than the string,
so inserting one row at the top of `records.csv` renumbered the ids and gave every
record below it a different fingerprint. It also put the fingerprints in the same
value domain as the Part 3 checksums, which hash genuine integers — `hash("carol@example.com")`
and `hash(5)` were the same number.

This depends on `email` being declared `symbol` in `.decl record(...)`. Column types
are not enforced, so an undeclared column still digests its id — see
`docs/SYNTAX.md`, "What the digest functions cover".

### Part 2: Deduplication

A record is considered superseded when another record shares its email fingerprint and
carries a smaller id (registered earlier). Keeping only the earliest registration per
email gives a canonical set of users.

```datalog
// A record is superseded if an earlier record has the same email hash.
superseded(id) :-
    email_fp(id,       _, _, fp),
    email_fp(other_id, _, _, fp),
    other_id < id.

// Canonical records: those not superseded by any earlier registration.
unique_record(id, name, email) :-
    record(id, name, email),
    !superseded(id).
```

Result (unique_records_output.csv):
```
1,alice,alice@example.com
2,bob,bob@example.com
3,carol,carol@example.com
5,dave,dave@example.com
```

Records id=4 (alice_dup) and id=6 (bob_dup) are eliminated because ids 1 and 2 already
cover those email addresses.

### Part 3: Checksum Validation

At ingest, `hash(id)` is stored as the row checksum. Re-computing it later and comparing
against the stored value reveals any record that was modified after ingest.

```datalog
// Valid: stored checksum matches the freshly computed hash(id).
valid_record(id, name) :-
    checksum(id, stored),
    record(id, name, _),
    hash(id) = stored.

// Corrupted: stored checksum does not match hash(id).
corrupted_record(id, name, stored, hash(id)) :-
    checksum(id, stored),
    record(id, name, _),
    hash(id) != stored.
```

In checksums.csv, record id=5 has a tampered value (`1234567890123456789` instead of
the correct `-8213464378072455284`). The corrupted_record output surfaces this:

```
5,dave,1234567890123456789,-8213464378072455284
         ^stored              ^computed by hash(id)
```

## Running the Example

From the project root:

```bash
./build/wirelog_cli examples/04-hash-functions/hash_dedup.dl
```

Verify the deduplication output matches the expected file:

```bash
diff examples/04-hash-functions/unique_records_output.csv \
     examples/04-hash-functions/expected_deduplicated.csv
```

No output means the files match.

## Key Concepts

### hash() Function

- Accepts `int64` or `symbol` arguments and returns `int64`.
- The bytes it digests come from the operand's **declared type**: a `symbol` column
  digests the string's own bytes (`strlen()` many, no NUL terminator), a numeric column
  digests the 8-byte `int64`. This example uses both — `hash(email)` in Part 1 and
  `hash(id)` in Part 3 — and they no longer share a value domain, which they did before
  #963.
- Deterministic across runs and across machines: the same input always produces the same
  output, and it is the output `xxhsum -H3` produces for the same bytes.
- Built on xxHash3, a fast non-cryptographic hash suitable for fingerprinting and
  deduplication. Do not use it for security-sensitive purposes (passwords, signatures).

### Deduplication via Negation

Wirelog does not silently merge duplicate CSV rows — each row in the input file produces
a tuple in the relation. True deduplication requires explicit logic. The pattern used here
is:

1. Compute a content fingerprint (`hash(email)`).
2. Mark any record superseded when an earlier record shares its fingerprint.
3. Use negation (`!superseded(id)`) to keep only canonical records.

### Checksum Strategy

Using `hash(id)` as a row checksum is a lightweight integrity check. A stronger approach
would hash all fields together; see example `05-crc32-checksum` for CRC-32 based
checksumming.

## Learning Points

1. `hash()` works on both `int64` and `symbol` types.
2. Equal inputs always produce equal hashes — this is the foundation of content-addressed
   deduplication.
3. Negation (`!relation(...)`) combined with a derived predicate is the standard Datalog
   pattern for set-difference and deduplication.
4. Hash-based checksums can detect corruption but are not cryptographically secure.
