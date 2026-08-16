#!/usr/bin/env bash
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

set -euo pipefail

mode=""
candidate_manifest=""
phase="5"
gate_stage="release-preparation"
required_result_ids="R01-RELEASE-VERSION,R01-CANDIDATE-LIFECYCLE,R01-CORE-IMAGE-WORKFLOW"
result_root=""
output_root=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) mode="$2"; shift 2 ;;
    --candidate-manifest) candidate_manifest="$2"; shift 2 ;;
    --phase) phase="$2"; shift 2 ;;
    --gate-stage) gate_stage="$2"; shift 2 ;;
    --require-result-ids) required_result_ids="$2"; shift 2 ;;
    --result-root) result_root="$2"; shift 2 ;;
    --output-root) output_root="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
case "$mode" in PrepareCommon|Target|Aggregate) ;; *) echo "--mode PrepareCommon|Target|Aggregate is required" >&2; exit 2 ;; esac
[[ -f "$candidate_manifest" ]] || { echo "--candidate-manifest is required" >&2; exit 2; }
[[ "$phase" == "5" || "$phase" == "6" ]] || { echo "--phase must be 5 or 6" >&2; exit 2; }
python_command="${PYTHON:-python}"
candidate_manifest="$($python_command -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).resolve())' "$candidate_manifest")"
candidate_root="$(dirname "$candidate_manifest")"
result_root="${result_root:-$candidate_root/results}"
output_root="${output_root:-$candidate_root/evidence}"
mkdir -p "$output_root"
mode_lower="$(printf '%s' "$mode" | tr '[:upper:]' '[:lower:]')"
worker="phase${phase}-${mode_lower}"
context_root="$candidate_root/contexts"
event_root="$candidate_root/events"
"$python_command" scripts/capture_candidate_execution_context.py --candidate-manifest "$candidate_manifest" --worker-id "$worker" --output-root "$context_root"
context="$context_root/$worker.json"
"$python_command" scripts/release_candidate_command.py run --candidate-manifest "$candidate_manifest" --route-id "R11-${mode_lower}-validate" --worker-id "$worker" --context "$context" --event-root "$event_root" -- "$python_command" distribution/candidate_run.py validate --candidate-manifest "$candidate_manifest"
[[ "$mode" == "Aggregate" ]] || exit 0
no_remote="$output_root/NO_REMOTE_PUBLICATION.json"
"$python_command" scripts/release_candidate_command.py run --candidate-manifest "$candidate_manifest" --route-id R11-no-remote --worker-id "$worker" --context "$context" --event-root "$event_root" -- "$python_command" scripts/no_remote_publication_guard.py --candidate-manifest "$candidate_manifest" --phase "$phase" --context-root "$context_root" --event-root "$event_root" --output "$no_remote"
"$python_command" scripts/release_candidate_command.py run --candidate-manifest "$candidate_manifest" --route-id R11-evidence --worker-id "$worker" --context "$context" --event-root "$event_root" -- "$python_command" scripts/release_evidence_guard.py --candidate-manifest "$candidate_manifest" --result-root "$result_root" --phase "$phase" --gate-stage "$gate_stage" --require-result-ids "$required_result_ids" --no-remote-evidence "$no_remote" --output "$output_root/EVIDENCE_INDEX.json"
