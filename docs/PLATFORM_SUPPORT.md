# Platform Support and Release Artifacts Policy

This policy defines what **Android** and **iOS** support means for the
`1.x` series while release blockers remain scoped to buildability and test
coverage.

## Current status for 1.x

For wirelog `1.x`, Android and iOS source cross-builds and basic smoke checks
exist and remain supported. Release packaging for those platforms is explicitly
**Tier-2** and deferred.

## What is intentionally not required in 1.0 GA

- No Android AAR/Prefab artifact is required.
- No published iOS `wirelog.xcframework` is required.
- Neither item is a release-blocking requirement for `1.0` GA.
- #697 does not require these artifacts to ship in GA.

## Required work before promotion

Before any promotion of Android and iOS packaging beyond this policy, the
following work is required:

- Android: AAR/Prefab packaging in release artifacts.
- iOS: XCFramework release artifact production.
- Smoke-tested downstream consumer projects for Android/iOS.
- Release automation for those package pipelines.

## Known follow-ups (not #697 blockers)

Issues #466, #470, #728, and #729 remain scheduled follow-up work and are
not blockers for #697 completion.
