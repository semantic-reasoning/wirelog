# Contributing to wirelog

First off, thank you for considering contributing to wirelog! It's people like you that make wirelog such a great tool for the community.

## Code of Conduct

This project and everyone participating in it is governed by the [wirelog Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to inquiry@cleverplant.com.

## Dual Licensing and CLA

wirelog uses a dual-licensing model:
1. **Open Source**: GNU Lesser General Public License v3.0 (LGPL-3.0)
2. **Commercial**: Proprietary license by CleverPlant

Because of this dual licensing, **all contributors must agree to the [Contributor License Agreement (CLA)](CLA.md)**. By submitting a pull request or patch to this project, you indicate your agreement to the terms in the CLA.

## How Can I Contribute?

### Reporting Bugs

Before creating bug reports, please check the existing issues as you might find out that you don't need to create one. When you are creating a bug report, please include as many details as possible:
* Use a clear and descriptive title.
* Describe the exact steps which reproduce the problem in as many details as possible.
* Provide specific examples to demonstrate the steps.
* Describe the behavior you observed after following the steps and point out what exactly is the problem with that behavior.
* Explain which behavior you expected to see instead and why.

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. When creating an enhancement suggestion, please provide the following information:
* Use a clear and descriptive title.
* Provide a step-by-step description of the suggested enhancement.
* Provide specific examples to demonstrate the steps.
* Describe the current behavior and explain which behavior you expected to see instead.
* Explain why this enhancement would be useful to most users.

### Pull Requests

1. Fork the repo and create your branch from `main`.
2. Ensure you have the `meson` build system installed, along with a C99 compiler and Rust (for Differential Dataflow).
3. Build the project:
   ```bash
   meson setup builddir
   cd builddir
   meson compile
   ```
4. If you've added code that should be tested, add tests.
5. Ensure the test suite passes:
   ```bash
   meson test
   ```
6. Issue that pull request!

## Styleguides

* Use C99 standard features for C code.
* Write clear, self-documenting code.
* Add comments for complex logic or design decisions.
* When adding documentation or updating existing texts, adhere to Markdown formatting.
