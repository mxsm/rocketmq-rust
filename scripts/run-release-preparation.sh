#!/usr/bin/env bash
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

set -euo pipefail

# The shared engine owns release_candidate_command.py, no_remote_publication_guard.py,
# and release_evidence_guard.py so both platform wrappers have identical semantics.
arguments=(scripts/release_preparation.py)
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode|--candidate-manifest|--common-inputs-bundle-output|--build-source-bundle-output|--build-control-bundle-output|--common-inputs-bundle|--build-source-bundle|--build-control-bundle|--target|--target-bundle-output|--target-bundles-root|--candidate-source-bundle-output|--source-root)
      [[ $# -ge 2 ]] || { echo "missing value for $1" >&2; exit 2; }
      arguments+=("$1" "$2")
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

python_command="${PYTHON:-python}"
"$python_command" "${arguments[@]}"
