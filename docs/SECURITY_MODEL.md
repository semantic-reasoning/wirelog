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
  `wirelog_session_insert` and `wl_easy_insert`, the I/O adapter
  callbacks the host registers via `wl_io_register_adapter`.
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
`-DmbedTLS=auto` and the system mbedTLS is detected), wirelog adds:

- Cryptographic hash families: SHA-1, SHA-2 (224/256/384/512), SHA-3.
- HMAC over the same hash families.
- UUIDv4 generation backed by a CSPRNG.

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
| Optional crypto | system mbedTLS | Apache-2.0 (default) or GPL-2.0-or-later |
| Optional crypto | mbedTLS sub-deps (`mbedx509`, `tfpsacrypto`) | Apache-2.0 |

LGPL-3.0-or-later is compatible with Apache-2.0 in the *consumer*
direction (consumer of mbedTLS may be LGPL-licensed). Downstream
redistributors of an `mbedTLS=enabled` build must include the
upstream Apache-2.0 NOTICE for mbedTLS plus the mbedx509/tfpsacrypto
notices.

### 3.2 Export-control self-classification

**Self-classification (mbedTLS-enabled build)**: **ECCN 5D002.c.1**
("information security" software providing encryption functions),
qualifying for **License Exception ENC** under U.S. 15 CFR 740.17,
sub-paragraph (b)(1) (publicly available encryption source code,
mass-market notification path).

This mirrors the upstream mbedTLS project's published stance: mbedTLS
is freely available source code, subject to ENC mass-market notice
filing rather than a license requirement, and is generally exportable
to all destinations except those embargoed under the EAR.

**What this means for downstream redistributors:**

1. If you redistribute a wirelog binary that was built with
   `mbedTLS=enabled` from the United States, you should be aware that
   the artifact carries cryptographic functionality and falls under
   ECCN 5D002.c.1. Most commercial mass-market redistribution paths
   are covered by License Exception ENC notification (a one-time
   email to BIS and NSA per 15 CFR 740.17(b)(1)).
2. From other jurisdictions (EU, UK, Japan, etc.), determine the
   equivalent classification under the local export regime. The EU
   counterpart is the EU Dual-Use Regulation Annex I Category 5
   Part 2, with similar mass-market exemptions.
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
- Including the upstream mbedTLS NOTICE files when you redistribute
  an `mbedTLS=enabled` build.

The project provides this self-classification in good faith and as a
starting point; it is not a substitute for review by qualified export
counsel for your specific distribution model.

---

## 4. References

- **mbedTLS upstream** — <https://github.com/Mbed-TLS/mbedtls>
  (license, ECCN, Annual Self-Classification posture).
- **U.S. EAR §740.17(b)(1)** — License Exception ENC for
  publicly-available encryption source code.
- **U.S. CCL 5D002** — Encryption software classification.
- **EU Regulation 2021/821** — Dual-use export-control regime,
  Annex I Category 5 Part 2.
- **Apache License 2.0 NOTICE** — required attribution for mbedTLS
  redistributions.
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
