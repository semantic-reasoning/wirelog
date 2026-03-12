# ARM64 CI Runner Setup

## Overview

This document describes how to configure a self-hosted ARM64 runner for GitHub Actions CI/CD testing of NEON optimizations in wirelog.

## GitHub Actions ARM64 Runner

The wirelog CI pipeline (`.github/workflows/ci-main.yml`) includes support for ARM64 testing via a self-hosted runner. This enables automated compilation and testing of NEON-optimized code on ARM64 architecture.

### Configuration

**Runner Labels:** `self-hosted`, `arm64`

**Supported Platforms:**
- Ubuntu 20.04 LTS / 22.04 LTS (ARM64)
- Raspberry Pi 4/5 (64-bit OS)
- AWS Graviton instances
- Azure Ampere A1 Compute instances
- Oracle Cloud Ampere A1 instances

### Prerequisites

1. **Physical/Virtual ARM64 System**
   - Minimum: ARMv8-A CPU with NEON support
   - Recommended: 4GB RAM, 10GB storage

2. **Software Requirements**
   ```bash
   # Ubuntu/Debian
   sudo apt-get install -y \
     git \
     python3 python3-pip \
     build-essential \
     meson ninja-build \
     gcc clang

   # Verify NEON support
   grep "neon" /proc/cpuinfo
   ```

3. **GitHub Runner Setup**
   ```bash
   # Create runner directory
   mkdir -p ~/github-runner && cd ~/github-runner

   # Download latest runner (replace VERSION with latest)
   curl -o actions-runner-linux-arm64.tar.gz \
     -L https://github.com/actions/runner/releases/download/vVERSION/actions-runner-linux-arm64.tar.gz
   tar xzf actions-runner-linux-arm64.tar.gz

   # Configure runner
   ./config.sh --url https://github.com/justinjoy/wirelog \
     --token YOUR_RUNNER_TOKEN \
     --labels self-hosted,arm64 \
     --name arm64-runner-1 \
     --work _work

   # Run as service
   sudo ./svc.sh install && sudo ./svc.sh start
   ```

### CI Workflow Details

**Matrix Entry (ci-main.yml):**
```yaml
# ARM64 - GCC (self-hosted runner)
# Configure a self-hosted ARM64 runner with -march=armv8-a+simd support
# This is optional and only runs if a self-hosted ARM64 runner is available
- os: arm64
  os_display: arm64-self-hosted
  os_runs_on: [self-hosted, arm64]
  compiler: gcc
  cc: gcc
```

**Steps Executed:**
1. **Checkout** (`actions/checkout@v5`)
2. **Install Dependencies** (Linux)
   - Updates package manager
   - Installs meson, ninja-build, gcc
   - Verifies NEON support via `/proc/cpuinfo`
3. **Configure** (`meson setup builddir -Dtests=true -Dthreads=enabled`)
   - Meson detects `-march=armv8-a+simd` for NEON
4. **Build** (`meson compile -C builddir`)
   - Compiles with NEON intrinsics enabled
5. **Test** (`meson test -C builddir`)
   - Runs all 56 unit tests with NEON optimization

### Compilation Flags

The meson.build automatically enables NEON for ARM64:

```meson
if host_machine.cpu_family() == 'aarch64'
  simd_flags = ['-march=armv8-a+simd']  # ARM64: enable NEON
else
  simd_flags = ['-march=native']  # x86/x64: enable AVX2/SSE
endif
```

### NEON Verification

**In CI Output:**
```
ARM64 runner detected. Verifying NEON support...
NEON support verified
```

**Manual Verification (on ARM64 system):**
```bash
# Check CPU flags
grep -o "neon[^ ]*" /proc/cpuinfo | sort | uniq

# Expected: neon asimd (both present)

# Compile test
meson setup builddir -Dthreads=enabled
meson compile -C builddir

# Run tests
meson test -C builddir
# All 56 tests should pass
```

### Troubleshooting

**Problem: "NEON support not detected"**
- Verify CPU supports NEON: `grep neon /proc/cpuinfo`
- Upgrade to modern ARM64 CPU (ARMv8.1+ recommended)

**Problem: Runner timeouts**
- Increase timeout in GitHub Actions workflow
- Consider using faster ARM64 instance type

**Problem: Compilation failures**
- Ensure GCC 9+ or Clang 10+ is installed
- Verify `-march=armv8-a+simd` flag is present: `meson compile -C builddir -v`

### Performance Baseline

Expected test execution time on ARM64:
- **Raspberry Pi 4 (1.5 GHz):** ~180-200 seconds
- **AWS Graviton2 (2.3 GHz):** ~90-110 seconds
- **Oracle Ampere A1 (3.0 GHz):** ~60-80 seconds

### References

- [NEON Intrinsics Guide](https://developer.arm.com/architectures/instruction-sets/intrinsics/)
- [GitHub Actions Self-Hosted Runners](https://docs.github.com/en/actions/hosting-your-own-runners)
- [wirelog Compilation Guide](./ARCHITECTURE.md)
