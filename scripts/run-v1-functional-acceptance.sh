#!/usr/bin/env bash
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

set -euo pipefail

candidate_manifest=""
matrix="scripts/v1-functional-test-matrix.json"
target=""
selection=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --candidate-manifest) candidate_manifest="$2"; shift 2 ;;
    --matrix) matrix="$2"; shift 2 ;;
    --target) target="$2"; shift 2 ;;
    --profile) selection=(--profile "$2"); shift 2 ;;
    --scenario) selection=(--scenario "$2"); shift 2 ;;
    --all-scenarios) selection=(--all-scenarios); shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "$candidate_manifest" || ${#selection[@]} -eq 0 ]]; then
  echo "--candidate-manifest and one selection mode are required" >&2
  exit 2
fi
arguments=(scripts/v1_functional_acceptance.py --candidate-manifest "$candidate_manifest" --matrix "$matrix")
if [[ -n "$target" ]]; then arguments+=(--target "$target"); fi
python "${arguments[@]}" "${selection[@]}"
