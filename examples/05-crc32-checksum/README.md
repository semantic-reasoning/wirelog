# Example 05-crc32-checksum: Frame Integrity Validation with CRC-32

## Overview

This example demonstrates **CRC-32 checksum validation** for network frame integrity
checking — a fundamental technique used in Ethernet, iSCSI, SCTP, and storage protocols
to detect corrupted or tampered data.

Each frame carries a stored CRC-32 checksum alongside its payload. wirelog recomputes
the checksum from the payload bytes and flags any frame where the computed value does not
match the stored value.

**Real-world use cases:**
- Ethernet frame integrity checking (IEEE 802.3 / ISO-HDLC)
- iSCSI/SCTP packet validation (Castagnoli variant)
- Storage block checksums (ext4, Btrfs use CRC-32C)
- Network protocol debugging: isolating corrupted frames in packet captures
- Data pipeline validation: detecting bit-rot or transmission errors in bulk data

## Files

- **frames.csv**: Six sample frames with payloads and stored CRC-32 checksums
  - Format: `frame_id,payload,stored_crc32`
  - F001-F003, F005: valid frames (stored checksum matches recomputed value)
  - F004, F006: corrupted frames (stored checksum does not match)

- **crc32_validate.dl**: Datalog rules using `crc32_ethernet()` for validation
  - Derives `valid_frame` for frames where computed == stored
  - Derives `corrupt_frame` for frames where computed != stored, showing both values

- **expected_valid_frames.csv**: Expected valid frame output for verification

- **valid_frames_output.csv** / **corrupt_frames_output.csv**: Generated outputs (after running)

## CRC-32 Variants in wirelog

wirelog exposes two CRC-32 variants as built-in functions on `symbol`-typed columns:

| Function | Polynomial | Standard | Use case |
|----------|------------|----------|----------|
| `crc32_ethernet(x)` | 0x04C11DB7 (reflected: 0xEDB88320) | IEEE 802.3, ISO-HDLC | Ethernet frames, ZIP, zlib |
| `crc32_castagnoli(x)` | 0x1EDC6F41 (reflected: 0x82F63B78) | RFC 3720 (iSCSI) | iSCSI, SCTP, ext4, Btrfs |

Both variants use `init=0xFFFFFFFF` and `finalXOR=0xFFFFFFFF`. An empty string produces
`0x00000000` (0xFFFFFFFF XOR 0xFFFFFFFF).

The Castagnoli variant (`crc32_castagnoli`) is hardware-accelerated on modern CPUs:
- Intel/AMD x86-64: uses the `CRC32` instruction (SSE4.2)
- Apple Silicon / ARM64: uses the `__crc32cb` instruction (ARMv8 CRC extension)

The Ethernet variant always uses a software lookup table because Intel/ARM hardware
implements only the Castagnoli polynomial.

## How It Works

### Validation Rule

```datalog
valid_frame(Id, Payload, Stored) :-
    frame(Id, Payload, Stored),
    crc32_ethernet(Payload) = Stored.
```

`crc32_ethernet(Payload)` computes the CRC-32 Ethernet checksum over the raw bytes of
the `Payload` string. The comparison `= Stored` filters to only tuples where the
recomputed checksum matches the stored value.

### Corruption Detection

```datalog
corrupt_frame(Id, Stored, crc32_ethernet(Payload)) :-
    frame(Id, Payload, Stored),
    crc32_ethernet(Payload) != Stored.
```

Corrupt frames surface both the stored (wrong) checksum and the recomputed (correct)
value, allowing downstream rules to identify the expected checksum.

### Example Trace

Input (`frames.csv`):

```
F001, DEADBEEF0102030405060708, 3838819244  <- stored == computed, VALID
F002, CAFEBABE0A0B0C0D0E0F1011, 250819451   <- stored == computed, VALID
F003, AABBCCDD1213141516171819, 1661565857  <- stored == computed, VALID
F004, 001122334455667788990011, 9999999999  <- stored != computed (2056678491), CORRUPT
F005, FFEEDDCCBBAA998877665544, 2259087609  <- stored == computed, VALID
F006, 0102030405060708090A0B0C, 1234567890  <- stored != computed (1992588221), CORRUPT
```

Valid frame output (`valid_frames_output.csv`):

```
F001,DEADBEEF0102030405060708,3838819244
F002,CAFEBABE0A0B0C0D0E0F1011,250819451
F003,AABBCCDD1213141516171819,1661565857
F005,FFEEDDCCBBAA998877665544,2259087609
```

Corrupt frame output (`corrupt_frames_output.csv`):

```
F004,9999999999,2056678491
F006,1234567890,1992588221
```

## Running the Example

From the project root:

```bash
./build/wirelog_cli examples/05-crc32-checksum/crc32_validate.dl
```

Outputs are written to:
- `examples/05-crc32-checksum/valid_frames_output.csv`
- `examples/05-crc32-checksum/corrupt_frames_output.csv`

Verify the valid frame output matches expectations:

```bash
diff examples/05-crc32-checksum/valid_frames_output.csv \
     examples/05-crc32-checksum/expected_valid_frames.csv
```

## Switching to Castagnoli

To validate iSCSI or storage frames using the Castagnoli variant, substitute
`crc32_castagnoli` for `crc32_ethernet` in the rules and recompute checksums
with the Castagnoli polynomial.

The Castagnoli checksums for the same payloads differ from the Ethernet values:

| Frame | Ethernet CRC-32 | Castagnoli CRC-32C |
|-------|----------------|--------------------|
| F001 (DEADBEEF...) | 3838819244 | 1638441351 |
| F002 (CAFEBABE...) | 250819451  | 2001331688 |
| F003 (AABBCCDD...) | 1661565857 | 3001799908 |
| F005 (FFEEDDCC...) | 2259087609 | 3978851243 |

The two polynomials are not interchangeable: a frame valid under Ethernet CRC-32
will compute a different (incorrect) value under Castagnoli, and vice versa.

## Key Concepts

### CRC-32 as a Unary Function on Symbols

wirelog's `crc32_ethernet(x)` and `crc32_castagnoli(x)` accept a `symbol`-typed
argument and return `int64`. The function hashes the raw UTF-8 bytes of the symbol
string, treating the string as an opaque byte sequence. This matches the wire-level
behaviour: a frame payload is a sequence of bytes, not a structured value.

### Comparison in Body vs. Head

Both usages are valid:

```datalog
// Body filter: keep only tuples where checksum matches
valid_frame(Id) :- frame(Id, P, S), crc32_ethernet(P) = S.

// Head projection: derive the computed checksum as a new column
with_crc(Id, P, crc32_ethernet(P)) :- frame(Id, P, _).
```

### CRC-32 is Not a Cryptographic Hash

CRC-32 detects accidental corruption (bit flips, truncation) but is NOT
collision-resistant. An adversary can craft inputs with any desired CRC-32 value.
For tamper detection, use a cryptographic hash such as SHA-256. For fast
deduplication and non-adversarial integrity, CRC-32 is sufficient and hardware-fast.

## Next Steps

- Add a `retransmit_request(Id)` rule to flag corrupt frames for retransmission
- Combine with `02-graph-reachability` to propagate integrity status across a
  multi-hop network path
- Switch to `crc32_castagnoli` and update checksums for iSCSI workload simulation
