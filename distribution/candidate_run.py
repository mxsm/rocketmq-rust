#!/usr/bin/env python3
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

"""Create and update one explicit local release-candidate manifest."""

from __future__ import annotations

import argparse
from copy import deepcopy
from pathlib import Path
import re
import sys
from typing import Any
import uuid


DISTRIBUTION = Path(__file__).resolve().parent
ROUTE_DENOMINATOR = DISTRIBUTION / "candidate-route-denominator.json"
if str(DISTRIBUTION) not in sys.path:
    sys.path.insert(0, str(DISTRIBUTION))

from release_state import (
    ReleaseStateError,
    atomic_write_json,
    exclusive_lock,
    read_json,
    require_safe_id,
    resolve_existing_file,
    series_lock_path,
    utc_now,
    validate_candidate,
    validate_series,
)


class CandidateError(ReleaseStateError):
    """Raised when a candidate would fork or corrupt its release series."""


def _parse_version(version: str, release_line: str) -> tuple[str, int | None]:
    base = f"{release_line}.0"
    if version == base:
        return "final", None
    match = re.fullmatch(re.escape(base) + r"-rc\.([1-9]\d*)", version)
    if match is None:
        raise CandidateError(f"version {version!r} is outside release line {release_line}")
    return "rc", int(match.group(1))


def _parent_issues(parent_manifest: str | None) -> list[dict[str, Any]]:
    if parent_manifest is None:
        return []
    parent = read_json(Path(parent_manifest))
    validate_candidate(parent)
    inherited: list[dict[str, Any]] = []
    for issue in parent["known_issues"]:
        if issue.get("resolution_status") == "closed":
            continue
        copied = deepcopy(issue)
        copied["approval_status"] = "inherited-pending-approval"
        copied["approver"] = None
        copied["waiver_expiry"] = None
        inherited.append(copied)
    return inherited


def create_candidate(
    root: Path,
    version: str,
    run_id: str,
    attempt: int,
    series_manifest: Path,
    *,
    parent_manifest: Path | None = None,
    fail_after: str | None = None,
) -> Path:
    require_safe_id(run_id, "run_id")
    if not isinstance(attempt, int) or attempt < 1:
        raise CandidateError("attempt must be a positive integer")
    series_manifest = resolve_existing_file(series_manifest, "series_manifest")
    with exclusive_lock(series_lock_path(series_manifest)):
        series = read_json(series_manifest)
        validate_series(series)
        if series["pending_operation"] is not None:
            raise CandidateError("release series has an unresolved pending operation")
        kind, rc_suffix = _parse_version(version, series["release_line"])
        head = series["head"]
        if head is None:
            if parent_manifest is not None:
                raise CandidateError("the first candidate cannot declare a parent")
            parent = None
        else:
            if not head.get("sealed"):
                raise CandidateError("the current series head must be sealed before creating a successor")
            expected_parent = Path(head["candidate_manifest"]).resolve()
            if parent_manifest is not None and Path(parent_manifest).resolve() != expected_parent:
                raise CandidateError("candidate parent is not the current series head")
            parent = str(expected_parent)
        if kind == "rc":
            if rc_suffix != series["next_rc_suffix"]:
                raise CandidateError(
                    f"RC suffix must be {series['next_rc_suffix']}, found {rc_suffix}"
                )
        else:
            if head is None or head.get("candidate_kind") != "rc" or head.get("state") != "rc-candidate-ready":
                raise CandidateError("a final candidate must directly follow a successful sealed RC")
            if series["consecutive_successful_rcs"] < 2:
                raise CandidateError("a final candidate requires two consecutive successful sealed RCs")

        candidate_root = (root / version / run_id / f"attempt-{attempt}").resolve()
        manifest = candidate_root / "CANDIDATE_RUN.json"
        if manifest.exists() or candidate_root.exists():
            raise CandidateError(f"candidate run already exists: {candidate_root}")
        ordinal = series["next_ordinal"]
        operation_id = str(uuid.uuid4())
        candidate_id = f"{version}-run{run_id}-attempt{attempt}-ordinal{ordinal}"
        value: dict[str, Any] = {
            "schema_version": 1,
            "candidate_id": candidate_id,
            "candidate_kind": kind,
            "version": version,
            "run_id": run_id,
            "attempt": attempt,
            "ordinal": ordinal,
            "candidate_root": str(candidate_root),
            "series_manifest": str(series_manifest),
            "series_id": series["series_id"],
            "series_generation": series["generation"] + 1,
            "parent_manifest": parent,
            "state": "development",
            "sealed": False,
            "outcome": None,
            "rejection_reason": None,
            "known_issues": _parent_issues(parent),
            "generation": 0,
            "build_source_bundle": None,
            "source_snapshot": None,
            "artifact_index": None,
            "evidence_index": None,
            "event_index": None,
            "execution_context_index": None,
            "route_denominator": read_json(ROUTE_DENOMINATOR),
            "creation_operation_id": operation_id,
            "created_at": utc_now(),
            "updated_at": utc_now(),
        }
        validate_candidate(value)
        series["pending_operation"] = {
            "operation_id": operation_id,
            "kind": "create-candidate",
            "ordinal": ordinal,
            "candidate_manifest": str(manifest),
            "target_generation": series["generation"] + 1,
            "candidate_kind": kind,
            "rc_suffix": rc_suffix,
        }
        series["updated_at"] = utc_now()
        atomic_write_json(series_manifest, series)
        if fail_after == "series-pending":
            raise CandidateError("simulated interruption after series pending reservation")
        atomic_write_json(manifest, value)
        if fail_after == "candidate-write":
            raise CandidateError("simulated interruption after candidate manifest write")
        entry = {
            "ordinal": ordinal,
            "version": version,
            "candidate_kind": kind,
            "run_id": run_id,
            "attempt": attempt,
            "candidate_manifest": str(manifest),
            "parent_manifest": parent,
            "state": "development",
            "outcome": None,
            "sealed": False,
            "operation_id": operation_id,
        }
        series["entries"].append(entry)
        series["head"] = deepcopy(entry)
        series["generation"] += 1
        series["next_ordinal"] += 1
        if kind == "rc":
            series["next_rc_suffix"] += 1
        series["pending_operation"] = None
        series["updated_at"] = utc_now()
        atomic_write_json(series_manifest, series)
    from release_series import default_control_bundle, export_control_bundle

    export_control_bundle(series_manifest, default_control_bundle(series_manifest, series["generation"]))
    return manifest


def recover_pending_creation(series_manifest: Path) -> str:
    series_manifest = resolve_existing_file(series_manifest, "series_manifest")
    with exclusive_lock(series_lock_path(series_manifest)):
        series = read_json(series_manifest)
        validate_series(series)
        pending = series.get("pending_operation")
        if not isinstance(pending, dict) or pending.get("kind") != "create-candidate":
            raise CandidateError("release series has no pending candidate creation")
        manifest = Path(pending["candidate_manifest"]).resolve()
        if not manifest.is_file():
            series["pending_operation"] = None
            series["updated_at"] = utc_now()
            atomic_write_json(series_manifest, series)
            return "abandoned"
        candidate = read_json(manifest)
        validate_candidate(candidate)
        if (
            candidate.get("creation_operation_id") != pending.get("operation_id")
            or candidate.get("ordinal") != pending.get("ordinal")
            or candidate.get("series_generation") != pending.get("target_generation")
        ):
            raise CandidateError("pending candidate does not match the reserved series operation")
        if any(entry.get("ordinal") == candidate["ordinal"] for entry in series["entries"]):
            raise CandidateError("pending candidate ordinal is already committed")
        entry = {
            "ordinal": candidate["ordinal"],
            "version": candidate["version"],
            "candidate_kind": candidate["candidate_kind"],
            "run_id": candidate["run_id"],
            "attempt": candidate["attempt"],
            "candidate_manifest": str(manifest),
            "parent_manifest": candidate["parent_manifest"],
            "state": candidate["state"],
            "outcome": candidate["outcome"],
            "sealed": candidate["sealed"],
            "operation_id": pending["operation_id"],
        }
        series["entries"].append(entry)
        series["head"] = deepcopy(entry)
        series["generation"] = pending["target_generation"]
        series["next_ordinal"] = candidate["ordinal"] + 1
        if candidate["candidate_kind"] == "rc":
            series["next_rc_suffix"] = int(pending["rc_suffix"]) + 1
        series["pending_operation"] = None
        series["updated_at"] = utc_now()
        atomic_write_json(series_manifest, series)
        generation = series["generation"]
    from release_series import default_control_bundle, export_control_bundle

    export_control_bundle(series_manifest, default_control_bundle(series_manifest, generation))
    return "committed"


def _update_unsealed(manifest: Path, update) -> None:
    manifest = resolve_existing_file(manifest, "candidate_manifest")
    candidate = read_json(manifest)
    validate_candidate(candidate)
    series_manifest = resolve_existing_file(Path(candidate["series_manifest"]), "series_manifest")
    with exclusive_lock(series_lock_path(series_manifest)):
        candidate = read_json(manifest)
        series = read_json(series_manifest)
        try:
            validate_candidate(candidate)
            validate_series(series)
        except ReleaseStateError as error:
            raise CandidateError(f"candidate and release series are inconsistent: {error}") from error
        if candidate["sealed"]:
            raise CandidateError("sealed candidates are immutable")
        if series["pending_operation"] is not None:
            raise CandidateError("release series has an unresolved pending operation")
        head = series.get("head")
        if (
            not isinstance(head, dict)
            or head.get("ordinal") != candidate["ordinal"]
            or Path(head.get("candidate_manifest", "")).resolve() != manifest
            or candidate["series_generation"] != series["generation"]
        ):
            raise CandidateError("only the consistent current release-series head may be updated")
        update(candidate)
        validate_candidate(candidate)
        candidate["generation"] += 1
        candidate["updated_at"] = utc_now()
        atomic_write_json(manifest, candidate)


def record_build_source_bundle(manifest: Path, bundle: Path) -> None:
    bundle = resolve_existing_file(bundle, "build_source_bundle")

    def update(candidate: dict[str, Any]) -> None:
        if candidate.get("build_source_bundle") is not None:
            raise CandidateError("build source bundle is already registered")
        candidate["build_source_bundle"] = str(bundle)

    _update_unsealed(manifest, update)


def record_known_issue(
    manifest: Path,
    *,
    issue_id: str,
    severity: str,
    impact: str,
    workaround: str,
    owner: str,
    target_version: str,
    approver: str | None = None,
    waiver_expiry: str | None = None,
) -> None:
    require_safe_id(issue_id, "issue_id")
    if severity not in {"Critical", "High", "Medium", "Low"}:
        raise CandidateError("severity must be Critical, High, Medium, or Low")
    if severity in {"Critical", "High"} and (approver is not None or waiver_expiry is not None):
        raise CandidateError("Critical and High issues cannot be waived")

    def update(candidate: dict[str, Any]) -> None:
        if any(issue.get("issue_id") == issue_id for issue in candidate["known_issues"]):
            raise CandidateError(f"known issue already exists: {issue_id}")
        candidate["known_issues"].append(
            {
                "issue_id": issue_id,
                "severity": severity,
                "impact": impact,
                "workaround": workaround,
                "owner": owner,
                "target_version": target_version,
                "approval_status": "approved" if approver and waiver_expiry else "pending-approval",
                "approver": approver,
                "waiver_expiry": waiver_expiry,
                "resolution_status": "open",
            }
        )

    _update_unsealed(manifest, update)


def approve_known_issue(
    manifest: Path,
    *,
    issue_id: str,
    approver: str,
    waiver_expiry: str,
) -> None:
    require_safe_id(issue_id, "issue_id")
    require_safe_id(approver, "approver")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", waiver_expiry) is None:
        raise CandidateError("waiver_expiry must use YYYY-MM-DD")

    def update(candidate: dict[str, Any]) -> None:
        matches = [issue for issue in candidate["known_issues"] if issue.get("issue_id") == issue_id]
        if len(matches) != 1:
            raise CandidateError(f"known issue does not exist exactly once: {issue_id}")
        issue = matches[0]
        if issue.get("resolution_status") != "open":
            raise CandidateError(f"closed known issue cannot be approved: {issue_id}")
        if issue.get("severity") not in {"Medium", "Low"}:
            raise CandidateError("only Medium and Low known issues may be waived")
        issue["approval_status"] = "approved"
        issue["approver"] = approver
        issue["waiver_expiry"] = waiver_expiry

    _update_unsealed(manifest, update)


def close_known_issue(
    manifest: Path,
    *,
    issue_id: str,
    resolved_by: str,
    resolution: str,
) -> None:
    require_safe_id(issue_id, "issue_id")
    require_safe_id(resolved_by, "resolved_by")
    if not resolution.strip():
        raise CandidateError("known issue resolution must be non-empty")

    def update(candidate: dict[str, Any]) -> None:
        matches = [issue for issue in candidate["known_issues"] if issue.get("issue_id") == issue_id]
        if len(matches) != 1:
            raise CandidateError(f"known issue does not exist exactly once: {issue_id}")
        issue = matches[0]
        if issue.get("resolution_status") != "open":
            raise CandidateError(f"known issue is already closed: {issue_id}")
        issue["resolution_status"] = "closed"
        issue["resolved_by"] = resolved_by
        issue["resolution"] = resolution.strip()
        issue["resolved_at"] = utc_now()

    _update_unsealed(manifest, update)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    create = subcommands.add_parser("create")
    create.add_argument("--version", required=True)
    create.add_argument("--run-id", required=True)
    create.add_argument("--attempt", type=int, required=True)
    create.add_argument("--root", type=Path, required=True)
    create.add_argument("--series-manifest", type=Path, required=True)
    create.add_argument("--parent-manifest", type=Path)
    validate = subcommands.add_parser("validate")
    validate.add_argument("--candidate-manifest", type=Path, required=True)
    source = subcommands.add_parser("record-build-source")
    source.add_argument("--candidate-manifest", type=Path, required=True)
    source.add_argument("--bundle", type=Path, required=True)
    issue = subcommands.add_parser("record-known-issue")
    issue.add_argument("--candidate-manifest", type=Path, required=True)
    issue.add_argument("--issue-id", required=True)
    issue.add_argument("--severity", required=True)
    issue.add_argument("--impact", required=True)
    issue.add_argument("--workaround", required=True)
    issue.add_argument("--owner", required=True)
    issue.add_argument("--target-version", required=True)
    issue.add_argument("--approver")
    issue.add_argument("--waiver-expiry")
    approve = subcommands.add_parser("approve-known-issue")
    approve.add_argument("--candidate-manifest", type=Path, required=True)
    approve.add_argument("--issue-id", required=True)
    approve.add_argument("--approver", required=True)
    approve.add_argument("--waiver-expiry", required=True)
    close = subcommands.add_parser("close-known-issue")
    close.add_argument("--candidate-manifest", type=Path, required=True)
    close.add_argument("--issue-id", required=True)
    close.add_argument("--resolved-by", required=True)
    close.add_argument("--resolution", required=True)
    recover = subcommands.add_parser("recover")
    recover.add_argument("--series-manifest", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "create":
            output = create_candidate(
                args.root,
                args.version,
                args.run_id,
                args.attempt,
                args.series_manifest,
                parent_manifest=args.parent_manifest,
            )
        elif args.command == "validate":
            validate_candidate(read_json(resolve_existing_file(args.candidate_manifest, "candidate_manifest")))
            output = args.candidate_manifest.resolve()
        elif args.command == "record-build-source":
            record_build_source_bundle(args.candidate_manifest, args.bundle)
            output = args.candidate_manifest.resolve()
        elif args.command == "record-known-issue":
            record_known_issue(
                args.candidate_manifest,
                issue_id=args.issue_id,
                severity=args.severity,
                impact=args.impact,
                workaround=args.workaround,
                owner=args.owner,
                target_version=args.target_version,
                approver=args.approver,
                waiver_expiry=args.waiver_expiry,
            )
            output = args.candidate_manifest.resolve()
        elif args.command == "approve-known-issue":
            approve_known_issue(
                args.candidate_manifest,
                issue_id=args.issue_id,
                approver=args.approver,
                waiver_expiry=args.waiver_expiry,
            )
            output = args.candidate_manifest.resolve()
        elif args.command == "close-known-issue":
            close_known_issue(
                args.candidate_manifest,
                issue_id=args.issue_id,
                resolved_by=args.resolved_by,
                resolution=args.resolution,
            )
            output = args.candidate_manifest.resolve()
        else:
            result = recover_pending_creation(args.series_manifest)
            output = args.series_manifest.resolve()
    except ReleaseStateError as error:
        print(f"CANDIDATE_RUN_FAILED detail={error}", file=sys.stderr)
        return 1
    suffix = f" recovery={result}" if args.command == "recover" else ""
    print(f"CANDIDATE_RUN_OK command={args.command} manifest={output}{suffix}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
