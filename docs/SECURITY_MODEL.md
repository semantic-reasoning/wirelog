# wirelog Security Model

This document records wirelog's threat model, the cryptographic and
export-control posture of its build options, and the third-party
dependency license stack that downstream packagers and integrators
need to be aware of.

> **Not legal advice.** The export-classification statements below are
> the project's **good-faith self-classification** under U.S. EAR
> guidance and the upstream mbedTLS project's published posture.
> Downstream redistributors must perform their own classification under
> the laws of every jurisdiction in which they distribute wirelog.

Companion documents:

- `SECURITY.md` — vulnerability reporting and disclosure channel.
- `docs/SEMANTICS.md` — engine semantic-model decisions.
- `docs/THREADING.md` (planned) — concurrency model.

---

## 1. Threat model

wirelog is an embedded Datalog engine. It is intended to run as a
**library inside a host application**, on the same machine as the
host, with the host owning the I/O boundary. Specifically:

- **Trusted**: the program text fed to `wirelog_parse` /
  `wirelog_parse_string`, the row data passed to
  `wirelog_session_insert` and `wirelog_easy_insert`, the I/O adapter
  callbacks the host registers via `wirelog_io_register_adapter`.
- **Untrusted**: nothing in the default surface. wirelog does not
  open sockets, accept network connections, or fork helper processes.
- **Optional crypto**: when built with `-DmbedTLS=enabled` (default
  is `disabled`), wirelog links against system mbedTLS to provide
  cryptographic hash, HMAC, and UUID built-ins. The crypto surface
  is described in §3.

Out of scope for the default build: side-channel attacks, malicious
peer authentication, transport-layer security (no transport exists),
secure-multi-party computation, hardware-attested execution.

## 2. Default build (no cryptographic code)

Build option `mbedTLS=disabled` (the default) produces a wirelog
library that contains **no cryptographic primitives** beyond:

- `xxHash3` (non-cryptographic hash; BSD-2-Clause).
- `CRC-32` (data-integrity check, not a security primitive;
  ethernet and Castagnoli variants both BSD-style royalty-free).

Neither is a cryptographic mechanism in the export-control sense.

**Self-classification (default build)**: **EAR99** (no encryption
controls). License exception not required. This is the posture for
all CI artifacts the project ships unless a release explicitly
advertises mbedTLS support.

## 3. mbedTLS-enabled build

When the build is configured with `-DmbedTLS=enabled` (or
`-DmbedTLS=auto` and a system PSA Crypto provider is detected),
wirelog exposes the following mbedTLS-backed Datalog built-ins:

- Digest built-ins: `md5(x)`, `sha1(x)`, `sha256(x)`, `sha512(x)`.
- HMAC built-in: `hmac_sha256(msg, key)`.
- UUID built-ins: `uuid4()`, `uuid5(namespace, name)`.

wirelog does not currently expose broader digest-family or generic
HMAC-family built-ins as callable Datalog functions.
Digest and HMAC built-ins take their input bytes from the operand's
declared type (#963): the string's own bytes for a `symbol`/`string`
column, `strlen()` many with no NUL terminator, and the 8-byte `int64`
representation for a numeric one. `.decl` types are not enforced, so a
`symbol` column holding values that were never interned falls back to
the `int64` form — the guarantee is only as strong as the declaration.
They compute mbedTLS digest/HMAC bytes and fold those bytes with
`XXH3_64bits()` into the single `int64` value stored by wirelog;
they do not return hex strings or byte arrays. **The fold means these
are 64-bit fingerprints, not full-width digests**, and must not be
relied on for collision resistance at scale. `uuid4()` and
`uuid5(namespace, name)` return the first 8 UUID bytes as an `int64`;
`uuid5()` is not RFC 4122 (see `docs/SYNTAX.md`).

**The digest built-ins do not separate operand domains.** A `symbol`
operand contributes its own bytes and an `int64` operand contributes its
8-byte little-endian representation, with nothing recording which of the
two it was. A symbol whose bytes coincide with an `int64`'s therefore
digests identically to that integer:

```
hash("abcdefgh")  ==  hash(7523094288207667809)
```

This is the documented behaviour, not a defect, and it is not being
fixed. It falls out of the byte-transparency contract from #963 — the
whole point of digesting a symbol's own bytes is that
`printf '%s' abcdefgh | xxhsum -H3` reproduces `hash("abcdefgh")`, which
leaves no room for a type tag in the digest input. It applies to every
single-operand digest — `hash()`, `crc32_ethernet()`,
`crc32_castagnoli()`, `md5()`, `sha1()`, `sha256()`, `sha512()`
(the first three are available in every build) — and to both operands of
`hmac_sha256(msg, key)`. If a relation mixes typed operands and the
distinction matters, digest a value that carries the type, or key the
relation on the type as well.

Note that this is a *separate* caveat from the 64-bit fold above. That
one is a birthday bound: collisions exist but cost work to find. This
one is constructible at zero cost by anyone who can choose an operand.

`uuid5()` is the one exception. Its symbol-bearing opcodes prefix each
operand with a one-byte domain tag and its length (#968), so
`uuid5("abcdefgh", x)` and `uuid5(7523094288207667809, x)` differ. That
buys an unambiguous *digest input* and nothing more: the return value is
the first 8 digest bytes with 4 bits overwritten by the version nibble,
so `uuid5()` has at most 2^60 distinct outputs and is subject to the
same birthday bound as everything else here. Injective encoding is not
collision-free output.

These functions become callable as Datalog built-ins; the host has no
control over whether a `.dl` program calls them once mbedTLS is
linked in.

### 3.1 License stack

mbedTLS upstream ships under a **dual-license**: Apache-2.0 OR
GPL-2.0-or-later (the user picks at distribution time). wirelog uses
mbedTLS as a dynamically linked dependency — the user takes the
Apache-2.0 path by default.

| Layer | Component | License |
|---|---|---|
| wirelog itself | this repository | LGPL-3.0-or-later (or commercial) |
| Hash built-in | xxHash3 | BSD-2-Clause |
| CRC-32 | in-tree | LGPL-3.0-or-later |
| Optional crypto | system PSA Crypto provider (`tfpsacrypto`, `mbedcrypto`, or PSA-capable `mbedtls`) | Apache-2.0 (default) or GPL-2.0-or-later |

LGPL-3.0-or-later is compatible with Apache-2.0 in the *consumer*
direction (consumer of mbedTLS may be LGPL-licensed). Downstream
redistributors of an `mbedTLS=enabled` build must include the
upstream Apache-2.0 NOTICE for the selected PSA Crypto provider.

### 3.1.1 SBOM artifacts

Each release publishes two SBOM (Software Bill of Materials) formats in
the `sbom/` directory (Issue #744):

- `wirelog-<version>.spdx.json` — SPDX 2.3 format (generated by syft)
- `wirelog-<version>.cdx.json` — CycloneDX 1.5 format (generated by syft)

Both formats enumerate the detected packages, versions, and license
declarations covering wirelog source, all `subprojects/*` dependencies
(xxHash, nanoarrow, optional mbedTLS), and system dependencies.

The committed `sbom/snapshot.txt` captures the deterministic `name@version:license`
baseline for all detected packages. The CI gate `meson test --suite sbom`
diffs the current dependency graph against this snapshot and fails if
dependencies have changed without an explicit baseline update.

To regenerate after a dependency change (e.g., updating a wrap-file version,
adding a system library, changing `meson.build` include rules):

```bash
scripts/release/generate-sbom.sh <build_root>
git add sbom/snapshot.txt && git commit -m "sbom: update snapshot"
```

### 3.2 Export-control self-classification

**Self-classification (mbedTLS-enabled build)**: **ECCN 5D002.c.1**
("information security" software providing encryption functions),
matching the international **Wassenaar Arrangement 5.A.2 / 5.D.2**
control list. wirelog has **not** requested a CCATS (Commodity
Classification Automated Tracking System) determination from BIS;
the classification above is the project's own good-faith assessment.

Two U.S. EAR pathways apply to publicly-available source code:

- **§740.13(e) TSU** (Technology and Software — Unrestricted) covers
  publicly-available encryption source code with **no notification
  requirement**. This is the safest baseline for a downstream
  redistributor that has not separately filed the §740.17 notice.
- **§740.17(b)(1) ENC** (Encryption commodities, software, and
  technology) covers publicly-available encryption source code under
  the ENC license exception, but is conditioned on a **one-time
  email notification** to BIS and the ENC Encryption Request
  Coordinator. A redistributor that has filed that notification may
  cite §740.17(b)(1); otherwise §740.13(e) TSU is the cleaner cite.

Either pathway permits export to most destinations; both exclude
EAR-embargoed countries.

This mirrors the upstream mbedTLS project's published stance: mbedTLS
is freely available source code, generally exportable subject to the
above EAR pathways and not requiring an export license for the
default redistribution channel.

**What this means for downstream redistributors:**

1. If you redistribute a wirelog binary that was built with
   `mbedTLS=enabled` from the United States, you should be aware that
   the artifact carries cryptographic functionality and falls under
   ECCN 5D002.c.1. Most commercial mass-market redistribution paths
   are covered by License Exception ENC notification (a one-time
   email to BIS and NSA per 15 CFR 740.17(b)(1)).
2. From other jurisdictions, determine the equivalent classification
   under the local export regime:
   - **EU**: Regulation 2021/821 Annex I Category 5 Part 2, with
     similar publicly-available-source and mass-market exemptions.
   - **UK** (post-Brexit): Strategic Export Control regime under the
     Export Control Order 2008 (as amended); UK Strategic Export
     Control Lists Annex 1 Category 5 Part 2 mirrors the Wassenaar
     5.A.2 / 5.D.2 controls.
   - **Japan**: METI Foreign Exchange and Foreign Trade Act, with
     publicly-available-software exemptions paralleling the EAR's.
   - All Wassenaar Arrangement participating states have closely
     analogous controls in their own dual-use frameworks.
3. **Do not redistribute to embargoed destinations** without checking
   the current sanctions lists (OFAC SDN, EU consolidated list, etc.).
4. The project does not file Annual Self-Classification Reports
   (ASRs) on behalf of downstream redistributors. Each redistributor
   is responsible for its own filings.

The project ships **all artifacts as `mbedTLS=disabled` by default**.
Distros and packagers that turn mbedTLS on must update their own
classification records accordingly.

### 3.3 User responsibility

You are responsible for:

- Choosing the correct build option for your distribution channel
  (`mbedTLS=disabled` if you do not need the crypto built-ins).
- Performing the export-control classification under **your**
  jurisdiction.
- Filing any required notifications (US ENC notification, EU dual-use
  reporting, Japanese METI, etc.) for the artifact you ship.
- Including the upstream NOTICE files for the selected PSA Crypto
  provider when you redistribute an `mbedTLS=enabled` build.

The project provides this self-classification in good faith and as a
starting point; it is not a substitute for review by qualified export
counsel for your specific distribution model.

---

## 4. References

- **mbedTLS upstream** — <https://github.com/Mbed-TLS/mbedtls>
  (license, ECCN, Annual Self-Classification posture).
- **U.S. EAR §740.13(e) TSU** — unrestricted publicly-available
  encryption source code, no notification required.
- **U.S. EAR §740.17(b)(1) ENC** — License Exception ENC pathway for
  publicly-available encryption source code, conditioned on the
  one-time BIS / ENC-Coordinator notification.
- **U.S. CCL 5D002** — Encryption software classification.
- **Wassenaar Arrangement 5.A.2 / 5.D.2** — international control
  list parent of the EAR / EU / UK / JP cryptography controls.
- **EU Regulation 2021/821** — Dual-use export-control regime,
  Annex I Category 5 Part 2.
- **UK Export Control Order 2008** — UK Strategic Export Control
  Lists Annex 1 Category 5 Part 2 (post-Brexit).
- **Japan FEFTA** — METI Foreign Exchange and Foreign Trade Act,
  publicly-available-software exemptions.
- **Apache License 2.0 NOTICE** — required attribution for
  redistributions of the selected PSA Crypto provider.
- **`meson_options.txt`** — the `mbedTLS` build option entry repeats
  the headline disclosure inline.
- **`SECURITY.md`** — vulnerability reporting channel.

---

## 5. Status

This document is **Status: Current**. The threat-model section is
expected to be promoted to Stable at 1.0 GA. The export-classification
self-statements track upstream mbedTLS posture; they will be reviewed
each release cycle and updated if the upstream classification changes.

Refs: `#701`, `stable-release-plan.md` §10, §11.
