#!/usr/bin/env bash
set -euo pipefail

OUT="${1:-target/runtime-audit}"
BASELINE="${2:-target/runtime-baseline/before}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AUDIT_ROOT="$ROOT/$OUT"
BASELINE_ROOT="$ROOT/$BASELINE"

mkdir -p "$AUDIT_ROOT" "$BASELINE_ROOT"

scan() {
  local name="$1"
  local pattern="$2"
  local file="$AUDIT_ROOT/${name}.md"

  {
    echo "# ${name//-/ }"
    echo
    echo "Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo
    echo "| File | Line | Code |"
    echo "|---|---:|---|"
    rg -n --glob "*.rs" --glob "!target/**" "$pattern" "$ROOT" \
      | sed "s#^$ROOT/##" \
      | awk -F: '{ code=$0; sub($1 ":" $2 ":", "", code); gsub(/\|/, "\\\\|", code); printf("| `%s` | %s | `%s` |\n", $1, $2, code) }' || true
  } > "$file"
}

scan "runtime-spawn-sites" "tokio::spawn|std::thread::spawn|thread::Builder::new|JoinHandle|JoinSet"
scan "runtime-creation-sites" "Runtime::new|RocketMQRuntime::new|Builder::new_multi_thread|Builder::new_current_thread|Handle::try_current|Handle::current|block_on|block_in_place"
scan "blocking-sites" "spawn_blocking|blocking_recv|blocking_send|std::fs::|RocksDB|rocksdb|thread::sleep"
scan "scheduler-sites" "schedule_at_fixed_rate|ScheduledTaskManager|FixedRate|FixedDelay|FixedRateNoOverlap|tokio::time::interval|tokio::time::sleep"
scan "shutdown-sites" "abort\(|shutdown_timeout|shutdown_background|CancellationToken|cancelled\(|Notify|wait_for_server_task|shutdown\("

cat > "$AUDIT_ROOT/task-classification.md" <<'MARKDOWN'
# Runtime Task Classification

| Classification | Migration target | Audit source |
|---|---|---|
| short async task | RuntimeHandle::spawn + tracing span | runtime-spawn-sites.md |
| long service task | TaskGroup::spawn_service | runtime-spawn-sites.md, shutdown-sites.md |
| scheduled task | ScheduledTaskGroup | scheduler-sites.md |
| connection task | remoting connection TaskGroup | runtime-spawn-sites.md |
| blocking task | BlockingExecutor | blocking-sites.md |
| dedicated loop | dedicated thread + CancellationToken | runtime-creation-sites.md, shutdown-sites.md |
| detached task | DetachedTaskPolicy or eliminate | runtime-spawn-sites.md |

Manual review is still required before migrating each site.
MARKDOWN

count_pattern() {
  local crate="$1"
  local pattern="$2"
  rg -n --glob "*.rs" --glob "!target/**" "$pattern" "$ROOT/$crate" 2>/dev/null | wc -l | tr -d ' '
}

write_json() {
  local file="$1"
  local body="$2"
  cat > "$BASELINE_ROOT/$file" <<JSON
{
  "generated_at": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "phase": "before",
  "source": "scripts/runtime-audit.sh",
  "metrics": $body
}
JSON
}

write_json "client-runtime-spawn.json" "{\"spawn_sites\": $(count_pattern rocketmq-client 'tokio::spawn|std::thread::spawn|thread::Builder::new'), \"runtime_creation_sites\": $(count_pattern rocketmq-client 'Runtime::new|Builder::new_current_thread|Builder::new_multi_thread|Handle::try_current|block_on')}"
write_json "namesrv-shutdown.json" "{\"shutdown_sites\": $(count_pattern rocketmq-namesrv 'abort\\(|shutdown_timeout|CancellationToken|cancelled\\(|Notify|shutdown\\('), \"scheduler_sites\": $(count_pattern rocketmq-namesrv 'ScheduledTaskManager|FixedRate|FixedDelay|tokio::time::interval')}"
write_json "broker-runtime-lifecycle.json" "{\"spawn_sites\": $(count_pattern rocketmq-broker 'tokio::spawn|std::thread::spawn|thread::Builder::new'), \"runtime_creation_sites\": $(count_pattern rocketmq-broker 'Runtime::new|RocketMQRuntime::new|Builder::new_current_thread|Builder::new_multi_thread|Handle::try_current|block_on'), \"shutdown_sites\": $(count_pattern rocketmq-broker 'abort\\(|shutdown_timeout|CancellationToken|cancelled\\(|Notify|shutdown\\(')}"
write_json "remoting-connection-lifecycle.json" "{\"spawn_sites\": $(count_pattern rocketmq-remoting 'tokio::spawn|std::thread::spawn|thread::Builder::new'), \"shutdown_sites\": $(count_pattern rocketmq-remoting 'abort\\(|shutdown_timeout|CancellationToken|cancelled\\(|Notify|shutdown\\(')}"
write_json "scheduler.json" "{\"rocketmq_scheduler_sites\": $(count_pattern rocketmq 'ScheduledTaskManager|FixedRate|FixedDelay|FixedRateNoOverlap|tokio::time::interval'), \"namesrv_scheduler_sites\": $(count_pattern rocketmq-namesrv 'ScheduledTaskManager|FixedRate|FixedDelay|FixedRateNoOverlap|tokio::time::interval'), \"broker_scheduler_sites\": $(count_pattern rocketmq-broker 'ScheduledTaskManager|FixedRate|FixedDelay|FixedRateNoOverlap|tokio::time::interval')}"
write_json "store-blocking.json" "{\"blocking_sites\": $(count_pattern rocketmq-store 'spawn_blocking|blocking_recv|blocking_send|RocksDB|rocksdb'), \"shutdown_sites\": $(count_pattern rocketmq-store 'abort\\(|shutdown_timeout|CancellationToken|cancelled\\(|Notify|shutdown\\(')}"

cat > "$AUDIT_ROOT/summary.json" <<JSON
{
  "generated_at": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "audit_directory": "$OUT",
  "baseline_directory": "$BASELINE"
}
JSON

echo "Runtime audit written to $AUDIT_ROOT"
echo "Baseline JSON written to $BASELINE_ROOT"
