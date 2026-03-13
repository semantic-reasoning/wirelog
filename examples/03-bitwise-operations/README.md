# Bitwise Operations: Permission Checking with band, bor, bxor

## Overview

This example demonstrates **bitwise operations** in wirelog Datalog for Unix-style permission checking. Each user holds a bitmask of granted permissions; rules use `band`, `bor`, and `bxor` to derive which permissions are active, how two permission sets combine, and where they differ.

**Real-world use cases:**
- Unix file permissions (rwx bitmasks)
- OAuth scope flags
- Feature flag systems
- Role-based access control (RBAC) bitmask encoding

## Files

- **users_sample.csv** — user IDs and their permission bitmasks
- **permissions.csv** — mapping of permission names to bit masks
- **bitwise_perms.dl** — Datalog rules using `band`, `bor`, `bxor`
- **expected_access.csv** — expected `has_access` output for verification
- **access_output.csv** — generated output: which users hold which permissions
- **combined_output.csv** — generated output: merged permission bits for each user pair
- **exclusive_output.csv** — generated output: XOR difference bits for each user pair

## Permission Bit Layout

```
bit 0  (mask  1 = 0b0001)  read
bit 1  (mask  2 = 0b0010)  write
bit 2  (mask  4 = 0b0100)  execute
bit 3  (mask  8 = 0b1000)  admin
```

## Input Data

`users_sample.csv` — five users with varying permission bitmasks:

| user_id | permission_bits | binary   | permissions granted      |
|---------|----------------|----------|--------------------------|
| 1       | 13             | 0b1101   | read, execute, admin     |
| 2       | 3              | 0b0011   | read, write              |
| 3       | 7              | 0b0111   | read, write, execute     |
| 4       | 15             | 0b1111   | read, write, execute, admin |
| 5       | 8              | 0b1000   | admin only               |

`permissions.csv` — four named permissions:

| name    | mask |
|---------|------|
| read    | 1    |
| write   | 2    |
| execute | 4    |
| admin   | 8    |

## Datalog Rules

### Rule 1: has_access — which users hold which permissions

```datalog
has_access(U, P) :-
    user_perm(U, Bits),
    permission_def(P, Mask),
    band(Bits, Mask) != 0.
```

`band(Bits, Mask) != 0` evaluates to true when the bit for permission `P` is set in the user's bitmask. This is the standard bitmask test.

### Rule 2: combined_perm — merged permissions for pairs of users

```datalog
combined_perm(A, B, bor(BitsA, BitsB)) :-
    user_perm(A, BitsA),
    user_perm(B, BitsB),
    A != B.
```

`bor` computes the union of two permission sets. Useful for asking: "if we merge these two roles, what can the combined role do?"

### Rule 3: exclusive_perm_bits — permissions held by exactly one user

```datalog
exclusive_perm_bits(A, B, bxor(BitsA, BitsB)) :-
    user_perm(A, BitsA),
    user_perm(B, BitsB),
    A != B.
```

`bxor` returns bits that are set in one operand but not both. Useful for auditing: "which permissions differ between these two accounts?"

## Expected Output

`expected_access.csv` (same as `access_output.csv` after running):

```
1,admin
1,execute
1,read
2,read
2,write
3,execute
3,read
3,write
4,admin
4,execute
4,read
4,write
5,admin
```

User 5 has bits=8 (admin only), so only `band(8, 8) = 8 != 0` passes. Users 1 and 4 both have admin; only user 4 has all four permissions.

## How to Run

From the project root:

```bash
./build/wirelog_cli examples/03-bitwise-operations/bitwise_perms.dl
```

The program writes three output files:
- `examples/03-bitwise-operations/access_output.csv`
- `examples/03-bitwise-operations/combined_output.csv`
- `examples/03-bitwise-operations/exclusive_output.csv`

## Verification

```bash
diff examples/03-bitwise-operations/access_output.csv \
     examples/03-bitwise-operations/expected_access.csv
```

No output means the result is correct.

## Worked Example: band

User 1 has `permission_bits = 13` (binary `1101`).

```
permission_bits:  1 1 0 1
                  │ │ │ └─ bit 0: read    (mask 1)  → band(13,1) = 1  ✓
                  │ │ └─── bit 1: write   (mask 2)  → band(13,2) = 0  ✗
                  │ └───── bit 2: execute (mask 4)  → band(13,4) = 4  ✓
                  └─────── bit 3: admin   (mask 8)  → band(13,8) = 8  ✓
```

So `has_access(1, "read")`, `has_access(1, "execute")`, `has_access(1, "admin")` are derived; `has_access(1, "write")` is not.

## Worked Example: bxor (permission diff)

Users 1 (bits=13, `1101`) and 2 (bits=3, `0011`):

```
user 1:  1 1 0 1  (13)
user 2:  0 0 1 1   (3)
XOR:     1 1 1 0  (14)
```

`exclusive_perm_bits(1, 2, 14)` — bits 1 (write), 2 (execute), 3 (admin) differ between the two users. The DBA can inspect this number to audit divergence without expanding into individual permission rows.

## Key Concepts

### Bitwise Operators in wirelog

| operator     | meaning                        | example            |
|-------------|--------------------------------|--------------------|
| `band(a, b)` | bitwise AND — test if bit set  | `band(13, 4) = 4`  |
| `bor(a, b)`  | bitwise OR — union of bits     | `bor(3, 8) = 11`   |
| `bxor(a, b)` | bitwise XOR — symmetric diff   | `bxor(13, 3) = 14` |

### Why use bitmasks in Datalog?

Bitmask encoding compresses a set of boolean flags into a single integer, enabling:
- **Single-row joins** instead of one row per flag
- **Arithmetic comparisons** to test multiple flags at once
- **Compact storage** for dense permission tables

The trade-off is readability: `band(Bits, Mask) != 0` requires knowing the mask layout, whereas a normalized table (`user_has_perm(user_id, perm_name)`) is self-documenting. This example shows both: the bitmask input and the named-permission output derived from it.

## Next Steps

- `04-hash-functions`: Using the `hash()` built-in for deduplication and fingerprinting
- `05-crc32-checksum`: CRC-32 integrity checking for data frames
