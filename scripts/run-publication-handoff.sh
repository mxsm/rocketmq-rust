#!/usr/bin/env bash
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

set -euo pipefail

mode=""
candidate_source_bundle=""
candidate_control_bundle=""
output_root="target/v1-publication-handoff"
draft_bundle_output=""
draft_bundle=""
platform=""
result_id=""
platform_bundle_output=""
platform_bundles_root=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) mode="$2"; shift 2 ;;
    --candidate-source-bundle) candidate_source_bundle="$2"; shift 2 ;;
    --candidate-control-bundle) candidate_control_bundle="$2"; shift 2 ;;
    --output-root) output_root="$2"; shift 2 ;;
    --draft-bundle-output) draft_bundle_output="$2"; shift 2 ;;
    --draft-bundle) draft_bundle="$2"; shift 2 ;;
    --platform) platform="$2"; shift 2 ;;
    --result-id) result_id="$2"; shift 2 ;;
    --platform-bundle-output) platform_bundle_output="$2"; shift 2 ;;
    --platform-bundles-root) platform_bundles_root="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
case "$mode" in PrepareDraft|Platform|Finalize) ;; *) echo "--mode PrepareDraft|Platform|Finalize is required" >&2; exit 2 ;; esac
[[ -f "$candidate_source_bundle" && -f "$candidate_control_bundle" ]] || { echo "candidate source/control bundles are required" >&2; exit 2; }
python_command="${PYTHON:-python}"
operation_root="$output_root/.handoff-${mode,,}-$$"
source_import="$operation_root/candidate-source"
control_import="$operation_root/candidate-control"
"$python_command" distribution/transfer_candidate.py import --bundle "$candidate_source_bundle" --output "$source_import"
candidate_selector="$operation_root/candidate-selector.json"
"$python_command" distribution/transfer_candidate.py import-build-control --bundle "$candidate_control_bundle" --output "$control_import" --selector-output "$candidate_selector"
candidate="$($python_command -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["candidate_manifest"])' "$candidate_selector")"
repository_source="$source_import/repository-source"
read_candidate() { "$python_command" -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))[sys.argv[2]])' "$candidate" "$1"; }
version="$(read_candidate version)"
run_id="$(read_candidate run_id)"
attempt="$(read_candidate attempt)"
context_root="$operation_root/contexts"
event_root="$operation_root/events"
run_route() {
  local route="$1" worker="$2" context="$3" events="$4"; shift 4
  "$python_command" scripts/release_candidate_command.py run --candidate-manifest "$candidate" --route-id "$route" --worker-id "$worker" --context "$context" --event-root "$events" -- "$@"
}
if [[ "$mode" == "PrepareDraft" ]]; then
  [[ -n "$draft_bundle_output" ]] || { echo "PrepareDraft requires --draft-bundle-output" >&2; exit 2; }
  worker="handoff-prepare"
  "$python_command" scripts/capture_candidate_execution_context.py --candidate-manifest "$candidate" --worker-id "$worker" --output-root "$context_root"
  context="$context_root/$worker.json"
  run_route H00-PREPARE-DRAFT "$worker" "$context" "$event_root" "$python_command" distribution/build_publication_handoff.py --draft --candidate-manifest "$candidate" --candidate-root "$source_import" --source-root "$repository_source" --output-root "$output_root"
  draft="$output_root/$version/$run_id/.attempt-$attempt.staging"
  run_route H00-EXPORT-DRAFT "$worker" "$context" "$event_root" "$python_command" distribution/transfer_handoff_draft.py export --draft "$draft" --candidate-manifest "$candidate" --output "$draft_bundle_output"
  run_route H00-DISCARD-EXPORTED-DRAFT "$worker" "$context" "$event_root" "$python_command" distribution/build_publication_handoff.py --discard-draft "$draft"
  exit 0
fi
[[ -f "$draft_bundle" ]] || { echo "$mode requires --draft-bundle" >&2; exit 2; }
if [[ "$mode" == "Platform" ]]; then
  [[ -n "$platform" && -n "$result_id" && -n "$platform_bundle_output" ]] || { echo "Platform arguments are incomplete" >&2; exit 2; }
  case "$platform:$result_id" in linux:H01-LINUX|windows:H01-WINDOWS|macos:H01-MACOS) ;; *) echo "platform and result ID disagree" >&2; exit 2 ;; esac
  [[ ! -e "$platform_bundle_output" ]] || { echo "platform output already exists" >&2; exit 2; }
  mkdir -p "$platform_bundle_output"
  draft="$operation_root/draft"
  "$python_command" distribution/transfer_handoff_draft.py import --bundle "$draft_bundle" --candidate-manifest "$candidate" --output "$draft"
  worker="handoff-$platform"
  context_root="$platform_bundle_output/contexts"
  event_root="$platform_bundle_output/events"
  "$python_command" scripts/capture_candidate_execution_context.py --candidate-manifest "$candidate" --worker-id "$worker" --output-root "$context_root"
  context="$context_root/$worker.json"
  "$python_command" scripts/release_candidate_command.py run --candidate-manifest "$candidate" --route-id "$result_id" --worker-id "$worker" --context "$context" --event-root "$event_root" --portable-root "$platform_bundle_output" -- "$python_command" distribution/verify_publication_handoff.py --handoff "$draft" --candidate-manifest "$candidate" --candidate-root "$source_import" --source-root "$repository_source" --draft-pre-ready --platform "$platform" --worker-id "$worker" --result-id "$result_id" --output "$platform_bundle_output/$result_id.json"
  exit 0
fi
[[ -d "$platform_bundles_root" ]] || { echo "Finalize requires --platform-bundles-root" >&2; exit 2; }
staging="$output_root/$version/$run_id/.attempt-$attempt.staging"
"$python_command" distribution/transfer_handoff_draft.py import --bundle "$draft_bundle" --candidate-manifest "$candidate" --output "$staging"
worker="handoff-finalize"
context_root="$source_import/contexts"
event_root="$source_import/events"
"$python_command" scripts/capture_candidate_execution_context.py --candidate-manifest "$candidate" --worker-id "$worker" --output-root "$context_root"
context="$context_root/$worker.json"
run_route H01-MERGE "$worker" "$context" "$event_root" "$python_command" distribution/merge_handoff_platform_results.py --candidate-manifest "$candidate" --bundles-root "$platform_bundles_root" --evidence-root "$source_import/evidence" --event-root "$event_root" --context-root "$context_root" --base-evidence-index "$source_import/EVIDENCE_INDEX.json"
run_route H01-REFRESH "$worker" "$context" "$event_root" "$python_command" distribution/build_publication_handoff.py --refresh-evidence "$staging" --candidate-manifest "$candidate" --candidate-root "$source_import" --evidence-index "$source_import/evidence/EVIDENCE_INDEX.json" --no-remote-evidence "$source_import/NO_REMOTE_PUBLICATION.json"
run_route H02-DRAFT-SEMANTIC "$worker" "$context" "$event_root" "$python_command" distribution/verify_publication_handoff.py --handoff "$staging" --candidate-manifest "$candidate" --candidate-root "$source_import" --source-root "$repository_source" --draft-pre-ready --result-id H02-DRAFT-SEMANTIC --output "$source_import/evidence/H02-DRAFT-SEMANTIC.json"
run_route H03-DRAFT-NO-REMOTE "$worker" "$context" "$event_root" "$python_command" scripts/no_remote_publication_guard.py --candidate-manifest "$candidate" --phase 6 --audit-point handoff-draft --result-id H03-DRAFT-NO-REMOTE --gate-stage final-handoff --context-root "$context_root" --event-root "$event_root" --handoff "$staging" --output "$source_import/evidence/H03-DRAFT-NO-REMOTE.json"
run_route H00-FINALIZE "$worker" "$context" "$event_root" "$python_command" distribution/build_publication_handoff.py --finalize "$staging"
final="$output_root/$version/$run_id/attempt-$attempt"
run_route H04-FINAL-SEMANTIC "$worker" "$context" "$event_root" "$python_command" distribution/verify_publication_handoff.py --handoff "$final" --candidate-manifest "$candidate" --candidate-root "$source_import" --source-root "$repository_source" --final-pre-ready --final-read-only --result-id H04-FINAL-SEMANTIC --output "$source_import/evidence/H04-FINAL-SEMANTIC.json"
run_route H05-FINAL-NO-REMOTE "$worker" "$context" "$event_root" "$python_command" scripts/no_remote_publication_guard.py --candidate-manifest "$candidate" --phase 6 --audit-point handoff-final --result-id H05-FINAL-NO-REMOTE --gate-stage final-handoff --context-root "$context_root" --event-root "$event_root" --handoff "$final" --output "$source_import/evidence/H05-FINAL-NO-REMOTE.json"
final_evidence="$source_import/evidence/FINAL_HANDOFF_EVIDENCE.json"
run_route H06-FINAL-EVIDENCE "$worker" "$context" "$event_root" "$python_command" scripts/release_evidence_guard.py --candidate-manifest "$candidate" --result-root "$source_import/evidence" --phase 6 --gate-stage final-handoff --require-result-ids H01-LINUX,H01-WINDOWS,H01-MACOS,H02-DRAFT-SEMANTIC,H03-DRAFT-NO-REMOTE,H04-FINAL-SEMANTIC,H05-FINAL-NO-REMOTE --event-root "$event_root" --context-root "$context_root" --no-remote-evidence "$source_import/evidence/H05-FINAL-NO-REMOTE.json" --output "$final_evidence"
retained_root="$(read_candidate candidate_root)"
retained_candidate="$retained_root/CANDIDATE_RUN.json"
retained_context_root="$retained_root/contexts"
retained_event_root="$retained_root/events"
"$python_command" scripts/capture_candidate_execution_context.py --candidate-manifest "$retained_candidate" --worker-id "$worker" --output-root "$retained_context_root"
retained_context="$retained_context_root/$worker.json"
"$python_command" scripts/release_candidate_command.py run --candidate-manifest "$retained_candidate" --route-id H06-PUBLICATION-READY --worker-id "$worker" --context "$retained_context" --event-root "$retained_event_root" -- "$python_command" scripts/release_lifecycle_guard.py --candidate-manifest "$retained_candidate" --transition publication-ready --phase 6 --gate-evidence "$final_evidence" --handoff-ready --handoff-evidence-root "$source_import/evidence" --current-route-id H06-PUBLICATION-READY --publication-marker "$final/PUBLICATION_READY.json"
candidate_control_output="$retained_root/transfer/CANDIDATE_CONTROL_BUNDLE.tar"
"$python_command" distribution/transfer_candidate.py export-build-control --candidate-manifest "$retained_candidate" --output "$candidate_control_output"
series_manifest="$(read_candidate series_manifest)"
series_control_output="$(dirname "$series_manifest")/RELEASE_SERIES_CONTROL_BUNDLE.tar"
"$python_command" distribution/release_series.py export-control --series-manifest "$series_manifest" --output "$series_control_output"
portable_control_root="$output_root/terminal-control"
mkdir -p "$portable_control_root/candidate" "$portable_control_root/series"
cp "$candidate_control_output" "$portable_control_root/candidate/CANDIDATE_CONTROL_BUNDLE.tar"
cp "$series_control_output" "$portable_control_root/series/RELEASE_SERIES_CONTROL_BUNDLE.tar"
