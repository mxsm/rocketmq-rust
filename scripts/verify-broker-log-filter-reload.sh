#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "usage: $0 <admin-executable> <broker-address> <audit-jsonl> [namesrv-address]" >&2
  exit 2
fi

admin_executable="$1"
broker_address="$2"
audit_path="$3"
namesrv_address="${4:-}"
filter="${LOG_FILTER_FILTER:-info,rocketmq_broker=debug}"
reason="${LOG_FILTER_REASON:-production log-filter verification}"
request_id="${LOG_FILTER_REQUEST_ID:-log-filter-$(date +%s)-$$}"
ttl_seconds="${LOG_FILTER_TTL_SECONDS:-60}"
restore_mode="${LOG_FILTER_RESTORE_MODE:-ttl}"
command_timeout="${LOG_FILTER_COMMAND_TIMEOUT_SECONDS:-30}"
max_reload_latency_ms="${LOG_FILTER_MAX_RELOAD_LATENCY_MS:-100}"
evidence_root="${LOG_FILTER_EVIDENCE_DIR:-target/log-filter-evidence}"

[[ -x "$admin_executable" ]] || { echo "admin executable not found: $admin_executable" >&2; exit 2; }
[[ -n "${ROCKETMQ_ACL_ACCESS_KEY:-}" && -n "${ROCKETMQ_ACL_SECRET_KEY:-}" ]] || {
  echo "ROCKETMQ_ACL_ACCESS_KEY and ROCKETMQ_ACL_SECRET_KEY must be set" >&2
  exit 2
}
[[ "$ttl_seconds" =~ ^[0-9]+$ && $ttl_seconds -ge 60 && $ttl_seconds -le 7200 ]] || {
  echo "LOG_FILTER_TTL_SECONDS must be between 60 and 7200" >&2
  exit 2
}
[[ "$restore_mode" == "ttl" || "$restore_mode" == "restore" ]] || {
  echo "LOG_FILTER_RESTORE_MODE must be ttl or restore" >&2
  exit 2
}
command -v python3 >/dev/null 2>&1 || { echo "python3 is required to validate JSONL evidence" >&2; exit 2; }
mkdir -p "$evidence_root"

run_update() {
  local label="$1"
  shift
  local args=()
  if [[ -n "$namesrv_address" ]]; then args+=("-n" "$namesrv_address"); fi
  args+=("broker" "updateBrokerConfig" "-b" "$broker_address")
  while [[ $# -gt 0 ]]; do args+=("-p" "$1"); shift; done
  args+=("--yes")
  local started ended
  started=$(python3 -c 'import time; print(time.time_ns() // 1000000)')
  if ! timeout "$command_timeout" "$admin_executable" "${args[@]}" \
      >"$evidence_root/$label.stdout.log" 2>"$evidence_root/$label.stderr.log"; then
    echo "admin CLI failed during $label; inspect evidence logs" >&2
    exit 1
  fi
  ended=$(python3 -c 'import time; print(time.time_ns() // 1000000)')
  echo $((ended - started))
}

audit_record() {
  local expected_request_id="$1"
  local phase="$2"
  python3 - "$audit_path" "$expected_request_id" "$phase" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
if not path.is_file():
    raise SystemExit(1)
match = None
for line in path.read_text(encoding="utf-8").splitlines():
    if not line.strip():
        continue
    record = json.loads(line)
    if record.get("request_id") == sys.argv[2] and record.get("phase") == sys.argv[3]:
        match = record
if match is None:
    raise SystemExit(1)
print(json.dumps(match, separators=(",", ":")))
PY
}

wait_audit() {
  local expected_request_id="$1"
  local phase="$2"
  local timeout_seconds="$3"
  local deadline=$((SECONDS + timeout_seconds))
  local record
  while (( SECONDS <= deadline )); do
    if record=$(audit_record "$expected_request_id" "$phase" 2>/dev/null); then
      echo "$record"
      return 0
    fi
    sleep 0.2
  done
  echo "timed out waiting for audit phase '$phase' for request '$expected_request_id'" >&2
  return 1
}

apply_round_trip=$(run_update "$request_id-apply" \
  "logFilter=$filter" \
  "logFilterReason=$reason" \
  "logFilterRequestId=$request_id" \
  "logFilterTtlSeconds=$ttl_seconds")
intent=$(wait_audit "$request_id" intent "$command_timeout")
success=$(wait_audit "$request_id" success "$command_timeout")
reload_latency=$(python3 - "$intent" "$success" <<'PY'
import json
import sys
success = json.loads(sys.argv[2])
if success.get("result") != "success":
    raise SystemExit("reload audit result is not success")
print(int(success["reload_duration_millis"]))
PY
)
(( reload_latency >= 0 && reload_latency <= max_reload_latency_ms )) || {
  echo "broker reload latency gate failed: ${reload_latency}ms (limit ${max_reload_latency_ms}ms)" >&2
  exit 1
}

restore_request_id=""
restore_round_trip="null"
if [[ "$restore_mode" == "restore" ]]; then
  restore_request_id="$request_id-restore"
  restore_round_trip=$(run_update "$restore_request_id-apply" \
    "logFilterReason=early restore after $request_id" \
    "logFilterRequestId=$restore_request_id" \
    "logFilterRestore=true")
  restore_audit=$(wait_audit "$restore_request_id" success "$command_timeout")
else
  restore_audit=$(wait_audit "$request_id" ttl_restore "$((ttl_seconds + command_timeout))")
fi
python3 - "$restore_audit" <<'PY'
import json
import sys
if json.loads(sys.argv[1]).get("result") != "success":
    raise SystemExit("broker did not restore the startup log-filter baseline successfully")
PY

summary="$evidence_root/$request_id-summary.json"
python3 - "$summary" "$broker_address" "$request_id" "$filter" "$ttl_seconds" "$restore_mode" \
  "$restore_request_id" "$reload_latency" "$max_reload_latency_ms" "$apply_round_trip" "$restore_round_trip" "$audit_path" <<'PY'
import datetime
import json
import pathlib
import sys
payload = {
    "broker_address": sys.argv[2],
    "request_id": sys.argv[3],
    "filter": sys.argv[4],
    "ttl_seconds": int(sys.argv[5]),
    "restore_mode": sys.argv[6],
    "restore_request_id": sys.argv[7] or None,
    "broker_reload_latency_milliseconds": int(sys.argv[8]),
    "max_reload_latency_milliseconds": int(sys.argv[9]),
    "apply_client_round_trip_milliseconds": int(sys.argv[10]),
    "restore_client_round_trip_milliseconds": None if sys.argv[11] == "null" else int(sys.argv[11]),
    "audit_path": str(pathlib.Path(sys.argv[12]).resolve()),
    "verified_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
}
pathlib.Path(sys.argv[1]).write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
PY
echo "Broker log-filter reload and restore gates passed. Evidence: $summary"
