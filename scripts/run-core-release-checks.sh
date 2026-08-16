#!/usr/bin/env bash
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0 (the "License");

set -uo pipefail

phase=""
candidate_manifest=""
gate_stage=""
result_root=""
required_result_ids=""
evidence_output=""
arguments=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --phase) phase="$2"; shift 2 ;;
    --version|--run-id|--attempt) arguments+=("$1" "$2"); shift 2 ;;
    --candidate-manifest) candidate_manifest="$2"; shift 2 ;;
    --gate-stage) gate_stage="$2"; shift 2 ;;
    --result-root) result_root="$2"; shift 2 ;;
    --require-result-ids) required_result_ids="$2"; shift 2 ;;
    --evidence-output) evidence_output="$2"; shift 2 ;;
    --include-repo-global|--list) arguments+=("$1"); shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
if [[ ! "$phase" =~ ^[0-6]$ ]]; then
  echo "--phase 0..6 is required" >&2
  exit 2
fi
python_command="${PYTHON:-}"
if [[ -z "$python_command" ]]; then
  if command -v python >/dev/null 2>&1; then
    python_command="python"
  else
    python_command="python3"
  fi
fi
"$python_command" scripts/core_release_checks.py --phase "$phase" "${arguments[@]}"
core_exit=$?
if (( core_exit != 0 )); then
  exit "$core_exit"
fi
if [[ -n "$candidate_manifest" ]]; then
  if (( phase < 5 )); then
    echo "--candidate-manifest is only supported by Phase 5/6 checks" >&2
    exit 2
  fi
  "$python_command" distribution/candidate_run.py validate --candidate-manifest "$candidate_manifest"
  selector_count=0
  for selector in "$gate_stage" "$result_root" "$required_result_ids" "$evidence_output"; do
    [[ -z "$selector" ]] || selector_count=$((selector_count + 1))
  done
  if (( selector_count == 0 )); then
    exit 0
  fi
  if (( selector_count != 4 )); then
    echo "Evidence validation requires --gate-stage, --result-root, --require-result-ids, and --evidence-output together" >&2
    exit 2
  fi
  "$python_command" scripts/release_evidence_guard.py \
    --candidate-manifest "$candidate_manifest" \
    --result-root "$result_root" \
    --phase "$phase" \
    --gate-stage "$gate_stage" \
    --require-result-ids "$required_result_ids" \
    --output "$evidence_output"
fi
