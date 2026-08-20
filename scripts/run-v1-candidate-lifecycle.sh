#!/usr/bin/env bash
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

set -euo pipefail

mode=""
candidate_manifest=""
stage_outcomes_index=""
parent_manifest=""
source_root=""
rejection_reason=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) mode="$2"; shift 2 ;;
    --candidate-manifest) candidate_manifest="$2"; shift 2 ;;
    --stage-outcomes-index) stage_outcomes_index="$2"; shift 2 ;;
    --parent-manifest) parent_manifest="$2"; shift 2 ;;
    --source-root) source_root="$2"; shift 2 ;;
    --rejection-reason) rejection_reason="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
case "$mode" in
  StageRc|FinalizeRc|FinalizeFinalFunctional|RejectFinalHandoff) ;;
  *) echo "--mode is required" >&2; exit 2 ;;
esac
[[ -f "$candidate_manifest" ]] || { echo "--candidate-manifest is required" >&2; exit 2; }

python_command="${PYTHON:-python}"
arguments=(
  scripts/v1_candidate_lifecycle.py
  --mode "$mode"
  --candidate-manifest "$candidate_manifest"
)
[[ -z "$stage_outcomes_index" ]] || arguments+=(--stage-outcomes-index "$stage_outcomes_index")
[[ -z "$parent_manifest" ]] || arguments+=(--parent-manifest "$parent_manifest")
[[ -z "$source_root" ]] || arguments+=(--source-root "$source_root")
[[ -z "$rejection_reason" ]] || arguments+=(--rejection-reason "$rejection_reason")

"$python_command" "${arguments[@]}"
