# Copyright 2026 The RocketMQ Rust Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Validate commit-bound, per-message PutOk evidence after clean failover."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any


SHA_RE = re.compile(r"^[0-9a-f]{40}$")
DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
STRICT_CONTRACT = "strict-sync-required-ack-clean-election"
MILESTONES = [
    "fault_injected",
    "controller_leader_elected",
    "broker_master_elected",
    "store_write_authority_granted",
    "route_converged",
    "producer_recovered",
]


class AuditError(RuntimeError):
    """Raised when evidence cannot safely be qualified."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def read_ndjson(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise AuditError(f"{path}:{line_number} must contain a JSON object")
            records.append(value)
    return records


def load_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)


def validate_identity(args: argparse.Namespace, findings: list[str]) -> None:
    for name in ("candidate_commit",):
        value = getattr(args, name)
        if not SHA_RE.fullmatch(value) or set(value) == {"0"}:
            findings.append(f"{name} must be a non-zero full Git SHA")
    for name in ("deployment_digest", "effective_config_sha256"):
        if not DIGEST_RE.fullmatch(getattr(args, name)):
            findings.append(f"{name} must be a SHA-256 digest")
    if args.durability_contract != STRICT_CONTRACT:
        findings.append("durability contract is not the strict clean-election contract")
    for name in ("run_id", "target_id", "cluster_uid"):
        if not str(getattr(args, name)).strip():
            findings.append(f"{name} must not be empty")


def validate_ledger(records: list[dict[str, Any]], minimum: int, findings: list[str]) -> dict[str, dict[str, Any]]:
    required = {
        "sequence",
        "audit_id",
        "unique_key",
        "broker_message_id",
        "offset_message_id",
        "broker_name",
        "queue_id",
        "queue_offset",
        "commit_log_offset",
        "store_size",
        "end_offset",
        "payload_sha256",
        "put_ok_at_utc",
    }
    if len(records) < minimum:
        findings.append(f"PutOk ledger contains {len(records)} messages; at least {minimum} are required")
    indexed: dict[str, dict[str, Any]] = {}
    sequences: set[int] = set()
    for index, entry in enumerate(records):
        missing = required - entry.keys()
        if missing:
            findings.append(f"ledger entry {index} is missing {sorted(missing)}")
            continue
        audit_id = str(entry["audit_id"])
        if not audit_id or audit_id in indexed:
            findings.append(f"ledger audit_id is empty or duplicated: {audit_id!r}")
        else:
            indexed[audit_id] = entry
        sequence = entry["sequence"]
        if not isinstance(sequence, int) or sequence < 0 or sequence in sequences:
            findings.append(f"ledger sequence is invalid or duplicated: {sequence!r}")
        else:
            sequences.add(sequence)
        if not DIGEST_RE.fullmatch(str(entry["payload_sha256"])):
            findings.append(f"ledger entry {audit_id!r} has an invalid payload digest")
        for field in ("queue_id", "queue_offset", "commit_log_offset", "store_size", "end_offset"):
            if not isinstance(entry[field], int) or entry[field] < 0:
                findings.append(f"ledger entry {audit_id!r} has invalid {field}")
        if isinstance(entry["commit_log_offset"], int) and isinstance(entry["store_size"], int):
            if entry["end_offset"] != entry["commit_log_offset"] + entry["store_size"]:
                findings.append(f"ledger entry {audit_id!r} has an inconsistent end_offset")
    return indexed


def validate_observations(
    expected: dict[str, dict[str, Any]],
    observation_paths: list[Path],
    findings: list[str],
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    mismatches = {"payload": 0, "offset": 0}
    for path in observation_paths:
        for observed in read_ndjson(path):
            audit_id = str(observed.get("audit_id", ""))
            counts[audit_id] += 1
            source = expected.get(audit_id)
            if source is None:
                continue
            if observed.get("payload_sha256") != source.get("payload_sha256"):
                mismatches["payload"] += 1
            comparable = ("broker_name", "queue_id", "queue_offset", "commit_log_offset", "store_size", "end_offset")
            if any(observed.get(field) != source.get(field) for field in comparable):
                mismatches["offset"] += 1
    expected_observations = len(observation_paths)
    missing = sum(max(0, expected_observations - counts[audit_id]) for audit_id in expected)
    duplicate = sum(max(0, counts[audit_id] - expected_observations) for audit_id in expected)
    unexpected = sum(count for audit_id, count in counts.items() if audit_id not in expected)
    if missing:
        findings.append(f"{missing} PutOk messages were not recovered")
    if duplicate:
        findings.append(f"{duplicate} duplicate recovery observations were recorded")
    if unexpected:
        findings.append(f"{unexpected} unexpected messages were recovered")
    if mismatches["payload"]:
        findings.append(f"{mismatches['payload']} payload hashes differ")
    if mismatches["offset"]:
        findings.append(f"{mismatches['offset']} storage positions differ")
    return {
        "put_ok_count": len(expected),
        "recovered_once_count": sum(1 for audit_id in expected if counts[audit_id] == expected_observations),
        "missing_count": missing,
        "duplicate_count": duplicate,
        "unexpected_count": unexpected,
        "payload_mismatch_count": mismatches["payload"],
        "offset_mismatch_count": mismatches["offset"],
    }


def validate_timelines(path: Path, repetitions: int, max_rto_ms: int, findings: list[str]) -> list[dict[str, Any]]:
    timelines = load_json(path)
    if not isinstance(timelines, list) or len(timelines) < repetitions:
        findings.append(f"at least {repetitions} failover timelines are required")
        return []
    for index, timeline in enumerate(timelines):
        records = timeline.get("milestones", []) if isinstance(timeline, dict) else []
        names = [record.get("milestone") for record in records if isinstance(record, dict)]
        elapsed = [record.get("elapsed_millis") for record in records if isinstance(record, dict)]
        if names != MILESTONES or len(elapsed) != len(MILESTONES):
            findings.append(f"timeline {index} does not contain ordered T0-T5 milestones")
            continue
        if any(not isinstance(value, int) or value < 0 for value in elapsed) or elapsed != sorted(elapsed):
            findings.append(f"timeline {index} contains invalid or regressing times")
        elif elapsed[-1] > max_rto_ms:
            findings.append(f"timeline {index} exceeds the {max_rto_ms}ms RTO limit")
        if timeline.get("single_writable_master") is not True:
            findings.append(f"timeline {index} did not prove a single writable master")
    return timelines


def validate_confirm_offsets(path: Path, findings: list[str]) -> dict[str, Any]:
    observations = read_ndjson(path)
    previous_epoch: int | None = None
    previous_confirm: int | None = None
    violations = 0
    for item in observations:
        epoch = item.get("authority_epoch")
        confirm = item.get("confirm_offset")
        legal = item.get("legal_in_sync_ack_offset")
        if not all(isinstance(value, int) and value >= 0 for value in (epoch, confirm, legal)):
            violations += 1
            continue
        if previous_epoch is not None and epoch < previous_epoch:
            violations += 1
        if (previous_confirm is not None and confirm < previous_confirm) or confirm > legal:
            violations += 1
        previous_epoch = epoch
        previous_confirm = confirm if previous_confirm is None else max(confirm, previous_confirm)
    if not observations:
        findings.append("confirmOffset audit has no observations")
    if violations:
        findings.append(f"confirmOffset audit contains {violations} boundary violations")
    return {"observations": len(observations), "violation_count": violations, "valid": bool(observations) and violations == 0}


def qualify(args: argparse.Namespace) -> dict[str, Any]:
    findings: list[str] = []
    validate_identity(args, findings)
    ledger = read_ndjson(args.ledger)
    expected = validate_ledger(ledger, args.minimum_messages, findings)
    messages = validate_observations(expected, args.observations, findings)
    timelines = validate_timelines(args.timelines, args.repetitions, args.max_rto_millis, findings)
    confirm = validate_confirm_offsets(args.confirm_offsets, findings)
    ledger_digest = sha256_file(args.ledger)
    strict = not findings
    return {
        "schema_version": 1,
        "artifact_kind": "controller_failover_qualification_evidence",
        "status": "pass" if strict else "fail",
        "strict_qualification_passed": strict,
        "run_id": args.run_id,
        "candidate_commit": args.candidate_commit,
        "deployment_digest": args.deployment_digest,
        "target_id": args.target_id,
        "cluster_uid": args.cluster_uid,
        "effective_config_sha256": args.effective_config_sha256,
        "durability_contract": args.durability_contract,
        "ledger_sha256": ledger_digest,
        "durability": {
            "synchronous_local_flush": True,
            "required_replica_acks": True,
            "clean_election": True,
        },
        "repetitions": len(timelines),
        "put_ok_messages": {
            **messages,
            "rpo_zero": messages["missing_count"] == 0,
            "exact_recovery": all(messages[key] == 0 for key in (
                "missing_count", "duplicate_count", "unexpected_count", "payload_mismatch_count", "offset_mismatch_count"
            )),
        },
        "confirm_offset": confirm,
        "timelines": timelines,
        "artifacts": {
            "ledger": {"path": str(args.ledger), "sha256": ledger_digest},
            "observations": [
                {"path": str(path), "sha256": sha256_file(path)} for path in args.observations
            ],
            "confirm_offsets": {"path": str(args.confirm_offsets), "sha256": sha256_file(args.confirm_offsets)},
            "timelines": {"path": str(args.timelines), "sha256": sha256_file(args.timelines)},
        },
        "rejection_reasons": findings,
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--ledger", type=Path, required=True)
    result.add_argument("--observations", type=Path, action="append", required=True)
    result.add_argument("--timelines", type=Path, required=True)
    result.add_argument("--confirm-offsets", type=Path, required=True)
    result.add_argument("--output", type=Path, required=True)
    result.add_argument("--run-id", required=True)
    result.add_argument("--candidate-commit", required=True)
    result.add_argument("--deployment-digest", required=True)
    result.add_argument("--target-id", required=True)
    result.add_argument("--cluster-uid", required=True)
    result.add_argument("--effective-config-sha256", required=True)
    result.add_argument("--durability-contract", default=STRICT_CONTRACT)
    result.add_argument("--minimum-messages", type=int, default=10_000)
    result.add_argument("--repetitions", type=int, default=5)
    result.add_argument("--max-rto-millis", type=int, default=180_000)
    return result


def main() -> int:
    try:
        args = parser().parse_args()
        report = qualify(args)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"PutOk RPO audit {report['status']}: {args.output}")
        return 0 if report["strict_qualification_passed"] else 1
    except (AuditError, OSError, ValueError, json.JSONDecodeError) as error:
        print(f"PutOk RPO audit failed closed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
