#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 4 ]]; then
  echo "usage: $0 <prometheus-url> <throughput-promql> <p99-promql> <baseline|candidate> [baseline-summary]" >&2
  exit 2
fi

prometheus_url="${1%/}"
throughput_query="$2"
p99_query="$3"
phase="$4"
baseline_summary="${5:-}"
duration_seconds="${LOG_FILTER_CANARY_DURATION_SECONDS:-900}"
sample_interval="${LOG_FILTER_CANARY_SAMPLE_INTERVAL_SECONDS:-15}"
max_throughput_decrease="${LOG_FILTER_MAX_THROUGHPUT_DECREASE_PERCENT:-1}"
max_p99_increase="${LOG_FILTER_MAX_P99_INCREASE_PERCENT:-2}"
evidence_root="${LOG_FILTER_EVIDENCE_DIR:-target/log-filter-evidence}"

[[ "$phase" == "baseline" || "$phase" == "candidate" ]] || { echo "phase must be baseline or candidate" >&2; exit 2; }
[[ "$duration_seconds" =~ ^[0-9]+$ && $duration_seconds -ge 60 ]] || { echo "duration must be at least 60 seconds" >&2; exit 2; }
[[ "$sample_interval" =~ ^[0-9]+$ && $sample_interval -ge 1 ]] || { echo "sample interval must be positive" >&2; exit 2; }
[[ "$phase" != "candidate" || -f "$baseline_summary" ]] || { echo "candidate phase requires a baseline summary" >&2; exit 2; }
command -v curl >/dev/null 2>&1 || { echo "curl is required" >&2; exit 2; }
command -v python3 >/dev/null 2>&1 || { echo "python3 is required" >&2; exit 2; }
mkdir -p "$evidence_root"

query_scalar() {
  local query="$1"
  curl --fail --silent --show-error --get --data-urlencode "query=$query" "$prometheus_url/api/v1/query" |
    python3 -c 'import json, math, sys
payload=json.load(sys.stdin)
result=payload.get("data", {}).get("result", [])
if payload.get("status") != "success" or len(result) != 1: raise SystemExit("query must return exactly one series")
value=float(result[0]["value"][1])
if not math.isfinite(value): raise SystemExit("query returned a non-finite value")
print(value)'
}

stamp=$(date -u +%Y%m%d-%H%M%S)
samples="$evidence_root/log-filter-canary-$phase-$stamp.csv"
summary="$evidence_root/log-filter-canary-$phase-$stamp.json"
echo "timestamp,throughput,p99" >"$samples"
started=$(date +%s)
sample_count=0
while true; do
  throughput=$(query_scalar "$throughput_query")
  p99=$(query_scalar "$p99_query")
  printf '%s,%s,%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$throughput" "$p99" >>"$samples"
  sample_count=$((sample_count + 1))
  elapsed=$(($(date +%s) - started))
  (( elapsed >= duration_seconds )) && break
  sleep "$sample_interval"
done
(( sample_count >= 2 )) || { echo "at least two samples are required" >&2; exit 1; }

python3 - "$samples" "$summary" "$phase" "$throughput_query" "$p99_query" "$baseline_summary" \
  "$max_throughput_decrease" "$max_p99_increase" "$sample_interval" <<'PY'
import csv
import datetime
import json
import pathlib
import sys

sample_path = pathlib.Path(sys.argv[1])
rows = list(csv.DictReader(sample_path.open(encoding="utf-8")))
throughput = sum(float(row["throughput"]) for row in rows) / len(rows)
p99 = sum(float(row["p99"]) for row in rows) / len(rows)
summary = {
    "phase": sys.argv[3],
    "started_at": rows[0]["timestamp"],
    "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "sample_interval_seconds": int(sys.argv[9]),
    "sample_count": len(rows),
    "throughput_query": sys.argv[4],
    "p99_query": sys.argv[5],
    "throughput_average": throughput,
    "p99_average": p99,
    "throughput_change_percent": None,
    "p99_change_percent": None,
    "gate_passed": True,
}
if sys.argv[3] == "candidate":
    baseline = json.loads(pathlib.Path(sys.argv[6]).read_text(encoding="utf-8-sig"))
    baseline_throughput = float(baseline["throughput_average"])
    baseline_p99 = float(baseline["p99_average"])
    if baseline_throughput <= 0 or baseline_p99 <= 0:
        raise SystemExit("baseline averages must both be greater than zero")
    throughput_change = (throughput / baseline_throughput - 1.0) * 100.0
    p99_change = (p99 / baseline_p99 - 1.0) * 100.0
    summary["throughput_change_percent"] = throughput_change
    summary["p99_change_percent"] = p99_change
    summary["gate_passed"] = throughput_change >= -float(sys.argv[7]) and p99_change <= float(sys.argv[8])
pathlib.Path(sys.argv[2]).write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
if not summary["gate_passed"]:
    raise SystemExit("canary performance gate failed")
PY
echo "Canary performance evidence collected. Summary: $summary"
