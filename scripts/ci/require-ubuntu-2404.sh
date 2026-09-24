#!/usr/bin/env bash
# The default path is the runner-owned OS release file. The optional argument
# exists so the workflow contract tests can use isolated fixtures.
set -euo pipefail

if [[ $# -gt 1 ]]; then
  echo "usage: $0 [os-release-file]" >&2
  exit 2
fi

os_release_file="${1:-/etc/os-release}"
if [[ ! -r "$os_release_file" ]]; then
  echo "error: cannot read OS release file: $os_release_file" >&2
  exit 1
fi

read_os_release_field() {
  local key="$1"
  local line raw value="" matches=0

  while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ "$line" == "$key" ]] || \
       [[ "$line" =~ ^[[:space:]]*${key}[[:space:]] ]] || \
       [[ "$line" =~ ^[[:space:]]+${key}= ]]; then
      echo "error: malformed $key field in OS release file: $os_release_file" >&2
      return 1
    fi

    case "$line" in
      "$key="*)
        matches=$((matches + 1))
        if (( matches > 1 )); then
          echo "error: duplicate $key field in OS release file: $os_release_file" >&2
          return 1
        fi

        raw="${line#*=}"
        if [[ "$raw" == \"* ]]; then
          if [[ "$raw" != *\" || ${#raw} -lt 2 ]]; then
            echo "error: malformed $key field in OS release file: $os_release_file" >&2
            return 1
          fi
          value="${raw:1:${#raw}-2}"
        else
          if [[ "$raw" == *\"* ]]; then
            echo "error: malformed $key field in OS release file: $os_release_file" >&2
            return 1
          fi
          value="$raw"
        fi

        if [[ ! "$value" =~ ^[A-Za-z0-9._-]+$ ]]; then
          echo "error: malformed $key field in OS release file: $os_release_file" >&2
          return 1
        fi
        ;;
    esac
  done < "$os_release_file"

  if (( matches != 1 )); then
    echo "error: missing $key field in OS release file: $os_release_file" >&2
    return 1
  fi

  printf '%s' "$value"
}

# Parse the two identity fields as data. Never source/evaluate the OS release
# file: even malformed or fixture input must not be able to execute commands.
if ! os_id="$(read_os_release_field ID)"; then
  exit 1
fi
if ! os_version="$(read_os_release_field VERSION_ID)"; then
  exit 1
fi

if [[ "$os_id" != "ubuntu" || "$os_version" != "24.04" ]]; then
  echo "error: runner must be Ubuntu 24.04 (found ID=$os_id, VERSION_ID=$os_version)" >&2
  exit 1
fi
