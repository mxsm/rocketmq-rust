#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <namesrv|broker|controller|proxy> <executable> [common args...]" >&2
  exit 2
fi

service="$1"
executable="$2"
shift 2
case "$service" in
  namesrv|broker|controller|proxy) ;;
  *) echo "unsupported service: $service" >&2; exit 2 ;;
esac
[[ -x "$executable" ]] || { echo "executable not found: $executable" >&2; exit 2; }

sample_seconds="${LOG_FILTER_SAMPLE_SECONDS:-60}"
evidence_root="${LOG_FILTER_EVIDENCE_DIR:-target/log-filter-evidence}"
target_name="${LOG_FILTER_TARGET_NAME:-rocketmq_${service}}"
target_filter="${LOG_FILTER_TARGET_FILTER:-info,${target_name}=debug}"
mkdir -p "$evidence_root"

run_probe() {
  local name="$1"
  local rust_log="$2"
  local expect_failure="$3"
  shift 3
  local stdout_path="$evidence_root/$service-$name.stdout.log"
  local stderr_path="$evidence_root/$service-$name.stderr.log"
  if [[ -n "$rust_log" ]]; then
    RUST_LOG="$rust_log" "$executable" "$@" >"$stdout_path" 2>"$stderr_path" &
  else
    env -u RUST_LOG "$executable" "$@" >"$stdout_path" 2>"$stderr_path" &
  fi
  local pid=$!
  local exited=0
  for ((i=0; i<sample_seconds*5; i++)); do
    if ! kill -0 "$pid" 2>/dev/null; then exited=1; break; fi
    sleep 0.2
  done
  local exit_code=0
  if [[ $exited -eq 0 ]]; then
    kill -TERM "$pid" 2>/dev/null || true
    wait "$pid" || true
  else
    wait "$pid" || exit_code=$?
  fi
  local debug_count info_count non_target_debug_count
  debug_count=$( (grep -Eh '\bDEBUG\b' "$stdout_path" "$stderr_path" || true) | awk 'END { print NR }')
  info_count=$( (grep -Eh '\bINFO\b' "$stdout_path" "$stderr_path" || true) | awk 'END { print NR }')
  non_target_debug_count=$( (grep -Eh '\bDEBUG\b' "$stdout_path" "$stderr_path" || true) | grep -Fvc -- "$target_name" || true)

  if [[ "$expect_failure" == "true" ]]; then
    [[ $exited -eq 1 && $exit_code -ne 0 ]] || { echo "$service invalid filter did not fail closed" >&2; exit 1; }
  else
    [[ $exited -eq 0 ]] || { echo "$service exited before sampling completed" >&2; exit 1; }
  fi
  printf '%s,%s,%s,%s,%s\n' "$name" "$debug_count" "$info_count" "$non_target_debug_count" "$exit_code"
}

summary="$evidence_root/$service-summary.csv"
echo "probe,debug_count,info_count,non_target_debug_count,exit_code" >"$summary"
default_result=$(run_probe default-info "" false "$@")
echo "$default_result" >>"$summary"
IFS=, read -r _ default_debug default_info _ _ <<<"$default_result"
[[ $default_debug -eq 0 && $default_info -gt 0 ]] || { echo "$service default INFO gate failed" >&2; exit 1; }

target_result=$(run_probe target-debug "$target_filter" false "$@")
echo "$target_result" >>"$summary"
IFS=, read -r _ target_debug _ non_target_debug _ <<<"$target_result"
[[ $target_debug -gt 0 && $non_target_debug -eq 0 ]] || { echo "$service target DEBUG gate failed" >&2; exit 1; }

invalid_result=$(run_probe invalid-filter "" true "$@" --log-filter rocketmq==debug)
echo "$invalid_result" >>"$summary"
echo "Log-filter governance gates passed for $service. Evidence: $summary"
