# Release signing and verification

Status of each mechanism, so nothing here is mistaken for a guarantee it does
not make:

| Mechanism | State |
|---|---|
| Sigstore keyless signing of the source archive | Implemented in the tag workflow; **not yet run on a real tag** |
| Publication of signatures as Release assets | Implemented; same |
| GPG-signed tags | **Not established.** Tracked in [#1154](https://github.com/semantic-reasoning/wirelog/issues/1154) |
| SBOM and ABI manifest attached to the Release | Not automated; open exit condition on [#687](https://github.com/semantic-reasoning/wirelog/issues/687) |

No maintainer GPG key is published **for release signing**, and no release tag
is signed. If you need tag signatures, follow #1154 rather than treating
anything below as covering them.

`SECURITY.md` does publish a maintainer GPG fingerprint. That key is for
encrypting vulnerability reports. It does not sign releases, and a release
cannot be verified against it.

No release has been signed by this pipeline yet: the job that signs is gated
behind a verification gate that has not passed, so it has never executed. Until
a tag reaches it, everything below describes what the workflow will do, not
what it has done. "What has and has not been exercised" at the end of this file
is the full account.

## What is signed, and what is not

The tag workflow signs **only the source archive**, `wirelog-X.Y.Z.tar.gz`,
producing three files beside it:

| File | Contents |
|---|---|
| `wirelog-X.Y.Z.tar.gz.sig` | detached base64 signature |
| `wirelog-X.Y.Z.tar.gz.pem` | the ephemeral Fulcio certificate |
| `wirelog-X.Y.Z.tar.gz.cosign.bundle` | signature, certificate and Rekor entry together |

`wirelog-X.Y.Z.tar.gz.sha256` and `.blake3` are published **unsigned**. They
carry no authority on their own: their integrity comes from
`scripts/release/verify-release.sh` recomputing both digests over the signed
archive, not from a signature of their own. Do not treat a checksum file
fetched by itself as evidence of anything.

`wirelog-X.Y.Z.intoto.jsonl` is a SLSA provenance attestation. It is a
self-authenticating DSSE envelope, verified by `gh attestation verify` rather
than by cosign, so it is not separately signed.

## Verifying a release

### Getting the assets

```bash
gh release download vX.Y.Z --repo semantic-reasoning/wirelog
```

That fetches all seven: the archive, `.sha256`, `.blake3`, `.sig`, `.pem`,
`.cosign.bundle`, and the `.intoto.jsonl` attestation.

Download all of them even if you only mean to check a signature.
`verify-release.sh` always verifies checksums first and exits with
`archive and both checksum files are required` if `.sha256` or `.blake3` is
absent — including on the signature and attestation recipes. The raw `cosign`
forms below have no such requirement.

### What you need installed

| Tool | Needed for |
|---|---|
| `sha256sum` and `b3sum` | checksum verification, and every `verify-release.sh` recipe |
| `cosign` (2.x, or 3.x for the bundle) | signature verification |
| `gh` | downloading the assets; attestation verification |

`b3sum` is packaged separately on most systems and `sha256sum` is GNU
coreutils, so on macOS both usually need installing (`brew install coreutils
b3sum`).

Three of the recipes below use `scripts/release/verify-release.sh`, which is
itself something you need. Take it from a git checkout at the tag, or from
`raw.githubusercontent.com` at that tag — **not** from the archive you are
about to verify, which would be circular. If you would rather not run it at
all, every check it performs has a direct equivalent below.

### Checksums only

```bash
scripts/release/verify-release.sh wirelog-X.Y.Z.tar.gz
```

Recomputes SHA256 and BLAKE3 over the archive and compares them to the
published manifests. It prints `checksum verification complete; signed inputs
were not supplied` — a reminder that this alone proves the archive matches its
manifests, not who produced it.

Without the script, the manifests are in the standard format both tools read:

```bash
sha256sum -c wirelog-X.Y.Z.tar.gz.sha256
b3sum -c wirelog-X.Y.Z.tar.gz.blake3
```

Each prints `wirelog-X.Y.Z.tar.gz: OK`.

### With the Sigstore signature

```bash
scripts/release/verify-release.sh wirelog-X.Y.Z.tar.gz \
    --signature wirelog-X.Y.Z.tar.gz.sig \
    --certificate wirelog-X.Y.Z.tar.gz.pem
```

Requires `cosign` on `PATH`. Under the hood this runs:

```bash
cosign verify-blob \
    --certificate wirelog-X.Y.Z.tar.gz.pem \
    --certificate-identity-regexp '^https://github\.com/semantic-reasoning/wirelog/\.github/workflows/release-tag\.yml@refs/(heads/main$|tags/)' \
    --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
    --signature wirelog-X.Y.Z.tar.gz.sig \
    wirelog-X.Y.Z.tar.gz
```

Use the raw form if you do not have the repository checked out, or do not have
the checksum files.

Note the identity pattern is anchored and names the release workflow, because
cosign matches `--certificate-identity-regexp` unanchored: a bare prefix would
accept a certificate minted by *any* workflow in this repository granted
`id-token: write`. It also constrains the ref, because a workflow run mints a
certificate for the workflow file **as it exists at the ref being run** — so an
unconstrained ref would let anyone with write access run a modified
`release-tag.yml` and mint a certificate this recipe accepts. Both `refs/tags/`
and `refs/heads/main` are admitted deliberately: the manual immutable-rerun path
dispatches from `main` while checking out the tag, so requiring a tag ref would
reject it ([#1287](https://github.com/semantic-reasoning/wirelog/issues/1287)).

Two things the pattern deliberately does **not** bound, so that you can judge
what this signature is worth rather than assume:

- **Which job minted it.** The certificate identifies the workflow file, not the
  job — there is no job discriminator in the SAN or in the GitHub OID extensions
  — so no identity pattern can separate one job in `release-tag.yml` from
  another. This is bounded by the workflow's `permissions:` block instead:
  `id-token: write` is declared on the signing job alone, so no other job holds
  a token that can mint this identity
  ([#1319](https://github.com/semantic-reasoning/wirelog/issues/1319)).
- **Which tag.** `refs/tags/` accepts any tag name, and the attacker picks the
  name, so no regex closes this. It needs tag protection or a protected Actions
  environment ([#1318](https://github.com/semantic-reasoning/wirelog/issues/1318)).

Both require repository write access, and neither affects any published artifact
today: signing landed after `v0.60.0`, so no release certificate has been minted
in this repository yet. The first is now bounded by the workflow's permissions
block; the second remains open.

### With the bundle — the recipe that keeps working

```bash
cosign verify-blob \
    --bundle wirelog-X.Y.Z.tar.gz.cosign.bundle \
    --certificate-identity-regexp '^https://github\.com/semantic-reasoning/wirelog/\.github/workflows/release-tag\.yml@refs/(heads/main$|tags/)' \
    --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
    wirelog-X.Y.Z.tar.gz
```

**Verification is never offline, in either form.** The Fulcio certificate is
valid for about ten minutes, and cosign establishes that the signature was made
while it was valid from the Rekor transparency-log entry — so
`--insecure-ignore-tlog` on an expired ephemeral certificate fails outright.

The difference between the two forms is *which* remote calls are made, not
whether any are:

- **Both** need the Sigstore trust root — the Fulcio root and Rekor public keys
  — fetched from `tuf-repo-cdn.sigstore.dev` unless already cached locally.
- **The detached pair additionally** performs a live Rekor *search* against
  `rekor.sigstore.dev` to find the log entry.

The bundle carries that entry inline, removing the search but not the trust-root
fetch. That is why it is the form to prefer months after a release, and why
allowlisting only `rekor.sigstore.dev` in a restricted network is not enough.

`.cosign.bundle` is cosign's **legacy** bundle format. cosign 3.x reads it by
auto-detection, so either major works. Non-cosign Sigstore SDKs — `sigstore-go`,
`sigstore-python` — expect the newer protobuf bundle and are not expected to
parse this file; use `cosign` with it, or the detached pair with them. That
last point is inferred from the format difference and has not been tested
here.

A successful verification prints `Verified OK`.

### With the provenance attestation

```bash
scripts/release/verify-release.sh wirelog-X.Y.Z.tar.gz \
    --attestation wirelog-X.Y.Z.intoto.jsonl \
    --repo semantic-reasoning/wirelog
```

Requires the `gh` CLI. Under the hood this runs:

```bash
gh attestation verify wirelog-X.Y.Z.tar.gz \
    --repo semantic-reasoning/wirelog \
    --bundle wirelog-X.Y.Z.intoto.jsonl \
    --predicate-type https://slsa.dev/provenance/v1 \
    --cert-identity-regex '^https://github\.com/semantic-reasoning/wirelog/\.github/workflows/release-tag\.yml@refs/(heads/main$|tags/)' \
    --cert-oidc-issuer 'https://token.actions.githubusercontent.com'
```

Note `gh` spells these flags differently from cosign: `--cert-identity-regex`
and `--cert-oidc-issuer`, not `--certificate-identity-regexp` and
`--certificate-oidc-issuer`.

### `--tag` is not available yet

`verify-release.sh` accepts `--tag`, which runs `git verify-tag`. That needs a
maintainer GPG key that does not exist (#1154), so the flag will fail today.

Separately: the workflow passes `--verify-tag` to `gh release create`. That flag
only checks that the git tag **exists** in the remote. It verifies nothing
signed, and must not be read as a signature or provenance check.

## What the maintainer does at tag time

Nothing, for signing. There is no key to hold and no secret to configure. The
`release-artifacts` job runs after the verification gate, signs the archive,
verifies its own output, and attaches everything to a draft GitHub Release. The
maintainer reviews that draft and publishes it. `docs/RELEASE_PROCESS.md` has
the full procedure, including what to do if an upload is interrupted.

## Key rotation

Keyless signing has no long-lived key to rotate. Each run obtains a fresh
certificate from Fulcio, valid for minutes, bound to the workflow's OIDC
identity. There is nothing to leak and nothing to revoke.

What can change, and must be changed in both places at once:

- **`identity_regexp`** in `scripts/release/verify-release.sh` and
  `scripts/release/sign-artifacts.sh` — if the repository or the release
  workflow is renamed. The `release_signing_contract` test asserts the two files
  agree and that the pattern names the repository the verifier expects, so a
  half-done rename fails at PR time rather than at the next release.
- **The Sigstore trust root** is rotated upstream through TUF and picked up by
  cosign automatically.

Consumers who pinned a certificate or a Rekor log index should note that the
workflow never overwrites a published asset: a re-run mints fresh material but
leaves what was published in place.

There is one documented exception, and it is manual. If an upload is
interrupted, a release can be left holding some of the three signing assets but
not all — and because they share a single ephemeral key and Rekor entry, mixing
them across runs produces a certificate and a signature that do not verify
against each other. The repair, in `docs/RELEASE_PROCESS.md`, is for a
maintainer to **delete those three assets** and re-run, which republishes
different material. If you fetched signing assets from a release that was
briefly in that state, re-fetch them. The Rekor log itself is append-only and
unaffected; what changes is the published asset set.

## When Sigstore is unreachable

If Fulcio or Rekor is unavailable, `sign-artifacts.sh` retries three times, at
5 and 15 seconds, for conditions a retry can fix — 5xx and 429 responses,
connection failures, timeouts, and TUF errors that cosign reports as failures.
Anything else fails immediately rather than sleeping over a permanent error.

On final failure the job fails and **nothing is published**. The archive,
checksums and attestation are still uploaded as a
`release-artifacts-partial-<tag>` evidence artifact, distinctly named so an
incomplete set cannot be mistaken for a complete one. Recovery is to re-run the
job once Sigstore is healthy. A release must not be published unsigned.

One TUF failure mode the retry cannot see, because cosign does not report it as
a failure: when the trusted-root fetch fails, cosign warns and continues with
individual targets rather than erroring. That path never reaches the retry
logic, so a TUF outage can degrade silently rather than retrying or failing.
This behaviour is read from cosign's source, not observed here.

## cosign version

The pipeline pins cosign **2.x** (currently 2.6.5). cosign 3 defaults
`--new-bundle-format` to true and then requires `--bundle` as the sole output,
deprecating `--output-signature` and `--output-certificate` — the flags this
repository publishes to consumers through `verify-release.sh`. Moving to cosign
3 means changing that published contract, so the pin is deliberate;
`sign-artifacts.sh` refuses to run under cosign 3 with a message saying why.

**Verifying** from the bundle works under either major; that was checked
against 2.6.5 and 3.1.3. Detached `.sig`/`.pem` verification under cosign 3 has
not been exercised here.

## What has and has not been exercised

Stated plainly, because the rest of this file is claims and a reader downstream
cannot check them:

- **Designed to run on every release, but not yet run once.** The
  `release-artifacts` job is gated behind a verification gate that has not
  passed, so it has never executed and **no release has been signed by this
  pipeline yet**. The first tag that reaches it will be its first end-to-end
  exercise: signing under cosign 2.6.5, and verification from the bundle with
  real Fulcio and Rekor material, via `sign-artifacts.sh` round-tripping its
  own output before anything is published.
- **Checked by hand:** a bundle written by cosign 2.6.5 verifies under both
  cosign 2.6.5 and 3.1.3. That check used a *keyed* signature, because keyless
  signing needs an OIDC token unavailable outside the workflow. It establishes
  that cosign 3 reads the legacy bundle format; it does not exercise a keyless
  bundle end to end.
- **Not exercised by anyone here:** a keyless bundle under cosign 3.x. The
  reasoning that it works is drawn from cosign's source — format detection
  happens before any certificate content is read, so the keyless bundle takes
  the same path — not from a run.

## If verification fails

Route on *what* failed, not on how far you got:

| Failure | What it means | Where it goes |
|---|---|---|
| `sha256sum`, `b3sum`, `cosign` or `gh` not found; `archive and both checksum files are required`; `tuf-repo-cdn.sigstore.dev` or `rekor.sigstore.dev` unreachable | environmental — your machine or network, not the artifact | fix locally; nothing to report |
| `SHA256 mismatch`, `BLAKE3 mismatch`, certificate identity or issuer mismatch, signature does not verify, attestation does not verify | cryptographic — the artifact does not match what was published | **`SECURITY.md`**, not a public issue |
| cosign 3 cannot *parse* the bundle | format compatibility | public issue; it would be new information |

A checksum mismatch is reported before any signature work and is as strong a
substitution signal as a signature failure. Treat it the same way.
