#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

"""Build a closed, semantic release-evidence index for one candidate."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from pathlib import PurePosixPath
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DISTRIBUTION = ROOT / "distribution"
if str(DISTRIBUTION) not in sys.path:
    sys.path.insert(0, str(DISTRIBUTION))

from release_state import (
    ReleaseStateError,
    atomic_write_json,
    ensure_no_digest_fields,
    read_json,
    resolve_existing_file,
    utc_now,
    validate_candidate,
)


class EvidenceError(ReleaseStateError):
    """Raised when the selected evidence denominator is not closed."""


RESULT_FIELDS = {
    "schema_version",
    "candidate_id",
    "version",
    "run_id",
    "attempt",
    "phase",
    "gate_stage",
    "result_id",
    "result_kind",
    "status",
    "command",
    "exit_code",
    "matched_test_count",
    "executed_test_count",
    "passed_test_count",
    "failed_test_count",
    "ignored_test_count",
    "capability_ids",
    "result_path",
}
RESULT_OUTPUT_FIELDS = (
    "result_id",
    "result_kind",
    "status",
    "command",
    "exit_code",
    "matched_test_count",
    "executed_test_count",
    "passed_test_count",
    "failed_test_count",
    "ignored_test_count",
    "capability_ids",
    "result_path",
)


def _validate_waivers(candidate: dict[str, Any]) -> None:
    today = date.today()
    for issue in candidate["known_issues"]:
        if issue.get("resolution_status") == "closed":
            continue
        if issue.get("severity") in {"Critical", "High"}:
            raise EvidenceError(f"unresolved blocking known issue: {issue.get('issue_id')}")
        expiry = issue.get("waiver_expiry")
        try:
            expiration = date.fromisoformat(expiry) if isinstance(expiry, str) else None
        except ValueError:
            expiration = None
        if issue.get("approval_status") != "approved" or expiration is None or expiration < today:
            raise EvidenceError(f"known issue waiver is missing or expired: {issue.get('issue_id')}")


def _validate_result(value: dict[str, Any], candidate: dict[str, Any]) -> None:
    ensure_no_digest_fields(value)
    missing = sorted(RESULT_FIELDS - value.keys())
    extra = sorted(value.keys() - RESULT_FIELDS)
    if missing or extra:
        raise EvidenceError(f"result fields are not closed; missing={missing}, extra={extra}")
    identity = (value["candidate_id"], value["version"], value["run_id"], value["attempt"])
    expected = (candidate["candidate_id"], candidate["version"], candidate["run_id"], candidate["attempt"])
    if identity != expected:
        raise EvidenceError(f"result {value.get('result_id')} belongs to another candidate")
    if value["result_kind"] not in {"test", "check", "artifact", "smoke"}:
        raise EvidenceError(f"result {value['result_id']} has an unknown kind")
    if value["schema_version"] != 1 or not isinstance(value["exit_code"], int) or isinstance(value["exit_code"], bool):
        raise EvidenceError(f"result {value['result_id']} has an invalid schema or exit code")
    if not isinstance(value["command"], list) or not all(isinstance(item, str) for item in value["command"]):
        raise EvidenceError(f"result {value['result_id']} has no semantic command record")
    counts = [
        value[field]
        for field in (
            "matched_test_count",
            "executed_test_count",
            "passed_test_count",
            "failed_test_count",
            "ignored_test_count",
        )
    ]
    if not all(isinstance(count, int) and not isinstance(count, bool) and count >= 0 for count in counts):
        raise EvidenceError(f"result {value['result_id']} has invalid test counts")
    if value["result_kind"] == "test" and (value["matched_test_count"] == 0 or value["executed_test_count"] == 0):
        raise EvidenceError(f"test result {value['result_id']} executed zero tests")
    if value["status"] != "passed" or value["exit_code"] != 0 or value["failed_test_count"] != 0:
        raise EvidenceError(f"required result {value['result_id']} did not pass")
    if value["passed_test_count"] + value["failed_test_count"] != value["executed_test_count"]:
        raise EvidenceError(f"result {value['result_id']} test counts are inconsistent")
    if value["matched_test_count"] != value["executed_test_count"] + value["ignored_test_count"]:
        raise EvidenceError(f"result {value['result_id']} matched/ignored counts are inconsistent")
    if not isinstance(value["capability_ids"], list) or len(set(value["capability_ids"])) != len(value["capability_ids"]):
        raise EvidenceError(f"result {value['result_id']} capability IDs are invalid")
    result_path = PurePosixPath(value["result_path"])
    if result_path.is_absolute() or not result_path.parts or any(part in {"", ".", ".."} for part in result_path.parts):
        raise EvidenceError(f"result {value['result_id']} has an unsafe result path")


def build_evidence(
    candidate_manifest: Path,
    result_root: Path,
    *,
    phase: int,
    gate_stage: str,
    required_result_ids: list[str],
    output: Path,
    required_capability_ids: list[str] | None = None,
    no_remote_evidence: Path | None = None,
) -> dict[str, Any]:
    if phase not in {5, 6} or gate_stage not in {"release-preparation", "full-matrix", "final-handoff"}:
        raise EvidenceError("phase/gate_stage is outside the 1.0 candidate evidence contract")
    if not required_result_ids or len(set(required_result_ids)) != len(required_result_ids):
        raise EvidenceError("required result IDs must be a non-empty unique denominator")
    manifest = resolve_existing_file(candidate_manifest, "candidate_manifest")
    candidate = read_json(manifest)
    validate_candidate(candidate)
    _validate_waivers(candidate)
    if not result_root.is_dir():
        raise EvidenceError(f"result root does not exist: {result_root}")
    selected: dict[str, list[dict[str, Any]]] = {}
    for path in sorted(result_root.rglob("*.json")):
        value = read_json(path)
        if value.get("phase") != phase or value.get("gate_stage") != gate_stage:
            continue
        result_id = value.get("result_id")
        if not isinstance(result_id, str) or not result_id:
            raise EvidenceError(f"result has no ID: {path}")
        selected.setdefault(result_id, []).append(value)
    expected = set(required_result_ids)
    actual = set(selected)
    if actual != expected:
        raise EvidenceError(
            f"result denominator mismatch; missing={sorted(expected - actual)}, unknown={sorted(actual - expected)}"
        )
    duplicates = sorted(result_id for result_id, values in selected.items() if len(values) != 1)
    if duplicates:
        raise EvidenceError(f"duplicate result IDs: {', '.join(duplicates)}")
    results: list[dict[str, Any]] = []
    capability_results: dict[str, str] = {}
    for result_id in required_result_ids:
        value = selected[result_id][0]
        _validate_result(value, candidate)
        results.append({field: value[field] for field in RESULT_OUTPUT_FIELDS})
        for capability_id in value["capability_ids"]:
            capability_results[capability_id] = "passed"
    if required_capability_ids is not None and set(capability_results) != set(required_capability_ids):
        raise EvidenceError(
            "capability denominator mismatch; "
            f"missing={sorted(set(required_capability_ids) - set(capability_results))}, "
            f"unknown={sorted(set(capability_results) - set(required_capability_ids))}"
        )
    remote_status = "not-executed"
    if no_remote_evidence is not None:
        remote = read_json(resolve_existing_file(no_remote_evidence, "no_remote_evidence"))
        remote_status = remote.get("remote_publication", {}).get("status")
        if remote.get("candidate_id") != candidate["candidate_id"] or remote_status != "not-executed":
            raise EvidenceError("remote publication evidence is not a current-candidate success")
    evidence = {
        "schema_version": 1,
        "candidate_id": candidate["candidate_id"],
        "version": candidate["version"],
        "run_id": candidate["run_id"],
        "attempt": candidate["attempt"],
        "phase": phase,
        "gate_stage": gate_stage,
        "required_result_ids": required_result_ids,
        "results": results,
        "capability_results": capability_results,
        "release_result_ids": {result_id: "passed" for result_id in required_result_ids},
        "failed_result_ids": [],
        "all_required_passed": True,
        "remote_publication": {"status": remote_status},
        "generated_at": utc_now(),
    }
    ensure_no_digest_fields(evidence)
    atomic_write_json(output, evidence)
    return evidence


def _csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--result-root", type=Path, required=True)
    parser.add_argument("--phase", type=int, choices=(5, 6), required=True)
    parser.add_argument("--gate-stage", choices=("release-preparation", "full-matrix", "final-handoff"), required=True)
    parser.add_argument("--require-result-ids", required=True)
    parser.add_argument("--require-capability-ids")
    parser.add_argument("--no-remote-evidence", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        evidence = build_evidence(
            args.candidate_manifest,
            args.result_root,
            phase=args.phase,
            gate_stage=args.gate_stage,
            required_result_ids=_csv(args.require_result_ids),
            required_capability_ids=_csv(args.require_capability_ids) if args.require_capability_ids else None,
            no_remote_evidence=args.no_remote_evidence,
            output=args.output,
        )
    except ReleaseStateError as error:
        print(f"RELEASE_EVIDENCE_FAILED detail={error}", file=sys.stderr)
        return 1
    print(f"RELEASE_EVIDENCE_OK results={len(evidence['results'])} output={args.output.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
