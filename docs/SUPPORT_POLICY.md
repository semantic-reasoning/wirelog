# wirelog Support Policy

This is the canonical lifecycle policy for supported wirelog versions. It
defines the support promise referenced by [`SECURITY.md`](../SECURITY.md) and
the version summary in [`README.md`](../README.md).

## Current status

wirelog is currently pre-1.0. Security fixes are made on `main`. The 1.x
window below begins when `1.0.0` is declared GA; it does not imply that a GA
date has already been announced.

## 1.x support window

The maintained 1.x line is supported until the later of:

1. 18 months after the `1.0.0` GA date; or
2. 12 months after the `2.0.0` GA date, once `2.0.0` exists.

If `2.0.0` has not shipped, the first condition remains the applicable
minimum. The end of the window is the EOL date for the maintained 1.x line;
the project will announce that date with the relevant release notice.

There is no separate LTS designation for `1.0.x` at this time. A future LTS
designation must name its maintenance line, end date, and backport scope in a
release announcement and in this document.

## What supported means

During the support window, the maintained 1.x line receives:

- fixes for confirmed security vulnerabilities and applicable CVE backports;
- critical correctness, crash, and data-loss fixes when they can be applied
  safely; and
- documentation or build fixes needed to consume supported releases.

New features are not added to a maintained 1.x line. A fix that cannot be
backported safely is handled through an advisory with the affected versions,
mitigation, and supported upgrade path.

Only the maintained 1.x line is promised routine backports. Older 1.x lines
may receive a security backport when the maintainer judges it safe and
necessary, but they are not supported after their announced EOL. Unmaintained
0.x tags and old branches receive no routine fixes.

## End of life

After EOL, users should upgrade to the current supported release. Security
reports for an EOL line are still triaged, but the normal response is an
advisory and an upgrade or mitigation recommendation rather than a guaranteed
backport.
