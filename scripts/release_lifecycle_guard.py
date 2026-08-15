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

"""Validate and atomically transition one local release candidate and its series."""

from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import date
import os
from pathlib import Path
from pathlib import PurePosixPath
import stat
import sys
from typing import Any
import uuid


ROOT = Path(__file__).resolve().parents[1]
DISTRIBUTION = ROOT / "distribution"
if str(DISTRIBUTION) not in sys.path:
    sys.path.insert(0, str(DISTRIBUTION))

from release_state import (
    ReleaseStateError,
    atomic_write_json,
    ensure_no_digest_fields,
    exclusive_lock,
    read_json,
    resolve_existing_file,
    series_lock_path,
    utc_now,
    validate_candidate,
    validate_series,
)
from create_candidate_source_snapshot import verify_snapshot_content


class LifecycleError(ReleaseStateError):
    """Raised when a lifecycle transition violates the candidate contract."""


REMOTE_STATES = {"publishing", "released"}
LIFECYCLE_CONFIG = ROOT / "distribution/config/release-lifecycle.json"


def _lifecycle_config() -> dict[str, Any]:
    value = read_json(LIFECYCLE_CONFIG)
    ensure_no_digest_fields(value)
    if value.get("schema_version") != 1 or value.get("scope") != "core-release":
        raise LifecycleError("release lifecycle configuration is invalid")
    return value


def _entry(series: dict[str, Any], candidate: dict[str, Any], manifest: Path) -> dict[str, Any]:
    matches = [entry for entry in series["entries"] if entry.get("ordinal") == candidate["ordinal"]]
    if len(matches) != 1:
        raise LifecycleError("candidate ordinal is not unique in the release series")
    entry = matches[0]
    if Path(entry["candidate_manifest"]).resolve() != manifest.resolve():
        raise LifecycleError("candidate and series manifest paths disagree")
    head = series.get("head")
    if head is None or head.get("ordinal") != candidate["ordinal"]:
        raise LifecycleError("only the current release-series head may transition")
    return entry


def _validate_gate_evidence(path: Path | None, candidate: dict[str, Any]) -> None:
    if path is None:
        raise LifecycleError("the transition requires explicit gate evidence")
    value = read_json(resolve_existing_file(path, "gate_evidence"))
    ensure_no_digest_fields(value)
    if value.get("schema_version") != 1 or value.get("candidate_id") != candidate["candidate_id"]:
        raise LifecycleError("gate evidence does not identify this candidate")
    if value.get("all_required_passed") is not True or value.get("failed_result_ids") != []:
        raise LifecycleError("not all required capability and short-check results passed")
    config = _lifecycle_config()
    expected_capabilities = set(config.get("required_capabilities", []))
    capability_results = value.get("capability_results")
    if not isinstance(capability_results, dict) or set(capability_results) != expected_capabilities:
        raise LifecycleError("gate evidence capability denominator is incomplete or contains extras")
    if any(status != "passed" for status in capability_results.values()):
        raise LifecycleError("one or more required capabilities did not pass")
    expected_release_results = set(config.get("phase5_required_result_ids", []))
    release_results = value.get("release_result_ids")
    if not isinstance(release_results, dict) or set(release_results) != expected_release_results:
        raise LifecycleError("gate evidence release-result denominator is incomplete or contains extras")
    if any(status != "passed" for status in release_results.values()):
        raise LifecycleError("one or more phase 5 release results did not pass")


def _validate_source_snapshot(candidate: dict[str, Any]) -> None:
    raw = candidate.get("source_snapshot")
    if not isinstance(raw, str):
        raise LifecycleError("a successful RC requires a source snapshot")
    value = read_json(resolve_existing_file(Path(raw), "source_snapshot"))
    ensure_no_digest_fields(value)
    if (
        value.get("schema_version") != 1
        or value.get("candidate_id") != candidate["candidate_id"]
        or value.get("version") != candidate["version"]
        or value.get("run_id") != candidate["run_id"]
        or value.get("attempt") != candidate["attempt"]
        or value.get("sealed") is not True
        or not isinstance(value.get("files"), list)
        or not value["files"]
    ):
        raise LifecycleError("source snapshot is not sealed for this candidate")
    source_root = Path(raw).resolve().parent / "source"
    expected: dict[str, int] = {}
    for item in value["files"]:
        if not isinstance(item, dict) or item.get("type") != "file":
            raise LifecycleError("source snapshot contains a non-file entry")
        path = PurePosixPath(item.get("path", ""))
        if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
            raise LifecycleError(f"source snapshot has an unsafe path: {path}")
        relative = path.as_posix()
        if relative in expected or not isinstance(item.get("size"), int):
            raise LifecycleError(f"source snapshot has a duplicate or invalid entry: {relative}")
        expected[relative] = item["size"]
    actual = {
        path.relative_to(source_root).as_posix(): path
        for path in source_root.rglob("*")
        if path.is_file()
    }
    if set(actual) != set(expected):
        raise LifecycleError("source snapshot file denominator does not match its manifest")
    for relative, path in actual.items():
        if path.stat().st_size != expected[relative] or path.stat().st_mode & stat.S_IWUSR:
            raise LifecycleError(f"source snapshot file is changed or writable: {relative}")
    bundle_raw = candidate.get("build_source_bundle")
    if not isinstance(bundle_raw, str) or value.get("source_bundle") != bundle_raw:
        raise LifecycleError("source snapshot does not identify the candidate build source bundle")
    try:
        verify_snapshot_content(
            resolve_existing_file(Path(bundle_raw), "build_source_bundle"),
            candidate,
            source_root,
            expected,
        )
    except ReleaseStateError as error:
        raise LifecycleError(f"source snapshot content drift: {error}") from error


def _validate_known_issues(candidate: dict[str, Any]) -> None:
    today = date.today()
    for issue in candidate["known_issues"]:
        if issue.get("resolution_status") == "closed":
            continue
        severity = issue.get("severity")
        if severity in {"Critical", "High"}:
            raise LifecycleError(f"unresolved {severity} issue blocks readiness: {issue.get('issue_id')}")
        if issue.get("approval_status") != "approved" or not issue.get("approver"):
            raise LifecycleError(f"known issue lacks current-candidate approval: {issue.get('issue_id')}")
        expiry = issue.get("waiver_expiry")
        try:
            expiration = date.fromisoformat(expiry) if isinstance(expiry, str) else None
        except ValueError:
            expiration = None
        if expiration is None or expiration < today:
            raise LifecycleError(f"known issue waiver is missing or expired: {issue.get('issue_id')}")


def _completion_event(
    current_route_id: str | None,
    candidate: dict[str, Any],
) -> tuple[Path, dict[str, Any]] | None:
    if current_route_id is None:
        return None
    started_raw = os.environ.get("RELEASE_CANDIDATE_STARTED_EVENT")
    completed_raw = os.environ.get("RELEASE_CANDIDATE_COMPLETED_EVENT")
    if not started_raw or not completed_raw:
        raise LifecycleError("lifecycle route requires wrapper event reservations")
    started_path = resolve_existing_file(Path(started_raw), "started_event")
    started = read_json(started_path)
    if (
        started.get("route_id") != current_route_id
        or started.get("candidate_id") != candidate["candidate_id"]
        or started.get("status") != "started"
    ):
        raise LifecycleError("lifecycle started event does not match the current route")
    completed_path = Path(completed_raw).resolve()
    if completed_path.exists():
        raise LifecycleError("lifecycle completed event already exists")
    completed = {
        "schema_version": 1,
        "candidate_id": candidate["candidate_id"],
        "route_id": current_route_id,
        "worker_id": started.get("worker_id"),
        "context_path": started.get("context_path"),
        "status": "passed",
        "exit_code": 0,
        "started_at": started.get("started_at"),
        "completed_at": utc_now(),
        "lifecycle_atomic_completion": True,
    }
    return completed_path, completed


def validate_only(candidate_manifest: Path) -> None:
    manifest = resolve_existing_file(candidate_manifest, "candidate_manifest")
    candidate = read_json(manifest)
    validate_candidate(candidate)
    ensure_no_digest_fields(candidate)
    series_manifest = resolve_existing_file(Path(candidate["series_manifest"]), "series_manifest")
    series = read_json(series_manifest)
    validate_series(series)
    ensure_no_digest_fields(series)
    _entry(series, candidate, manifest)
    if series["pending_operation"] is not None:
        raise LifecycleError("release series has an unresolved pending operation")
    if candidate["series_generation"] != series["generation"]:
        raise LifecycleError("candidate and release-series generations disagree")


def transition_candidate(
    candidate_manifest: Path,
    transition: str,
    *,
    phase: int,
    gate_evidence: Path | None = None,
    rejection_reason: str | None = None,
    handoff_ready: bool = False,
    current_route_id: str | None = None,
    fail_after: str | None = None,
    publication_marker: Path | None = None,
) -> dict[str, Any]:
    config = _lifecycle_config()
    configured_remote = set(config.get("remote_publication_states", []))
    if transition in REMOTE_STATES or transition in configured_remote:
        raise LifecycleError(f"state {transition} is reserved for a separate remote-publication task")
    manifest = resolve_existing_file(candidate_manifest, "candidate_manifest")
    initial = read_json(manifest)
    validate_candidate(initial)
    series_manifest = resolve_existing_file(Path(initial["series_manifest"]), "series_manifest")
    with exclusive_lock(series_lock_path(series_manifest)):
        candidate = read_json(manifest)
        series = read_json(series_manifest)
        validate_candidate(candidate)
        validate_series(series)
        if series["pending_operation"] is not None:
            raise LifecycleError("release series has an unresolved pending operation")
        entry = _entry(series, candidate, manifest)
        if candidate["sealed"] or entry.get("sealed"):
            raise LifecycleError("sealed candidates are immutable")
        kind = candidate["candidate_kind"]
        current = candidate["state"]
        allowed = config.get("candidate_kinds", {}).get(kind, {}).get("transitions", {}).get(current, [])
        if transition not in allowed:
            raise LifecycleError(f"transition {current} -> {transition} is not allowed for {kind}")
        target_outcome: str | None = candidate["outcome"]
        seal = False
        if transition == "staged-rc":
            if kind != "rc" or current != "development" or phase not in {5, 6}:
                raise LifecycleError("staged-rc requires a development RC in the local preparation phases")
        elif transition == "rc-candidate-ready":
            if kind != "rc" or current != "staged-rc" or phase != 6:
                raise LifecycleError("rc-candidate-ready requires a staged RC in phase 6")
            _validate_gate_evidence(gate_evidence, candidate)
            _validate_source_snapshot(candidate)
            _validate_known_issues(candidate)
            target_outcome = "success"
            seal = True
        elif transition == "ga-candidate-ready":
            if kind != "final" or current != "development" or phase != 6:
                raise LifecycleError("ga-candidate-ready requires a development final candidate in phase 6")
            if series["consecutive_successful_rcs"] < 2:
                raise LifecycleError("ga-candidate-ready requires two consecutive successful RCs")
            _validate_gate_evidence(gate_evidence, candidate)
            _validate_known_issues(candidate)
            target_outcome = "success"
        elif transition == "publication-ready":
            if (
                kind != "final"
                or current != "ga-candidate-ready"
                or phase != 6
                or not handoff_ready
                or publication_marker is None
            ):
                raise LifecycleError("publication-ready requires a verified final handoff in phase 6")
            publication_marker = publication_marker.resolve()
            if publication_marker.exists():
                raise LifecycleError("publication-ready marker already exists")
            _validate_known_issues(candidate)
            target_outcome = "success"
            seal = True
        elif transition == "rejected":
            if phase not in {5, 6} or not rejection_reason:
                raise LifecycleError("rejected candidates require a non-empty failure reason")
            target_outcome = "rejected"
            seal = True
        else:
            raise LifecycleError(f"unsupported lifecycle transition: {transition}")

        operation_id = str(uuid.uuid4())
        target_generation = series["generation"] + 1
        completion = _completion_event(current_route_id, candidate)
        marker_temp: Path | None = None
        marker_value: dict[str, Any] | None = None
        if transition == "publication-ready":
            marker_temp = publication_marker.with_name(f".{publication_marker.name}.pending")
            if marker_temp.exists():
                raise LifecycleError("publication-ready marker has an unresolved pending file")
            marker_value = {
                "schema_version": 1,
                "state": "publication-ready",
                "candidate_id": candidate["candidate_id"],
                "version": candidate["version"],
                "run_id": candidate["run_id"],
                "attempt": candidate["attempt"],
                "series_id": series["series_id"],
                "series_generation": target_generation,
                "operation_id": operation_id,
                "remote_publication": "not-executed",
                "created_at": utc_now(),
            }
            ensure_no_digest_fields(marker_value)
        series["pending_operation"] = {
            "operation_id": operation_id,
            "kind": "lifecycle-transition",
            "ordinal": candidate["ordinal"],
            "from_state": current,
            "to_state": transition,
            "target_generation": target_generation,
            "candidate_manifest": str(manifest),
            "target_outcome": target_outcome,
            "seal": seal,
            "completion_path": str(completion[0]) if completion is not None else None,
            "completion_event": completion[1] if completion is not None else None,
            "marker_path": str(publication_marker) if marker_temp is not None else None,
            "marker_temp": str(marker_temp) if marker_temp is not None else None,
            "marker_value": marker_value,
            "state_committed": False,
        }
        series["updated_at"] = utc_now()
        atomic_write_json(series_manifest, series)
        if fail_after == "series-pending":
            raise LifecycleError("simulated interruption after lifecycle pending reservation")
        if marker_temp is not None:
            atomic_write_json(marker_temp, marker_value)
            if fail_after == "marker-temp":
                raise LifecycleError("simulated interruption after publication marker preparation")

        candidate["state"] = transition
        candidate["outcome"] = target_outcome
        candidate["sealed"] = seal
        candidate["rejection_reason"] = rejection_reason if transition == "rejected" else None
        candidate["series_generation"] = target_generation
        candidate["generation"] += 1
        candidate["updated_at"] = utc_now()
        atomic_write_json(manifest, candidate)
        if fail_after == "candidate-write":
            raise LifecycleError("simulated interruption after lifecycle candidate write")
        if completion is not None:
            atomic_write_json(*completion)

        entry["state"] = transition
        entry["outcome"] = target_outcome
        entry["sealed"] = seal
        entry["operation_id"] = operation_id
        series["head"] = deepcopy(entry)
        if transition == "rc-candidate-ready":
            series["consecutive_successful_rcs"] += 1
        elif transition == "rejected":
            series["consecutive_successful_rcs"] = 0
        series["generation"] = target_generation
        if marker_temp is None:
            series["pending_operation"] = None
        else:
            series["pending_operation"]["state_committed"] = True
        series["updated_at"] = utc_now()
        atomic_write_json(series_manifest, series)
        if fail_after == "series-commit":
            raise LifecycleError("simulated interruption after lifecycle state commit")
        if marker_temp is not None:
            publication_marker.parent.mkdir(parents=True, exist_ok=True)
            os.replace(marker_temp, publication_marker)
            series["pending_operation"] = None
            series["updated_at"] = utc_now()
            atomic_write_json(series_manifest, series)
        generation = series["generation"]
    from release_series import default_control_bundle, export_control_bundle

    export_control_bundle(series_manifest, default_control_bundle(series_manifest, generation))
    return candidate


def recover_pending_transition(series_manifest: Path) -> str:
    series_manifest = resolve_existing_file(series_manifest, "series_manifest")
    with exclusive_lock(series_lock_path(series_manifest)):
        series = read_json(series_manifest)
        validate_series(series)
        pending = series.get("pending_operation")
        if not isinstance(pending, dict) or pending.get("kind") != "lifecycle-transition":
            raise LifecycleError("release series has no pending lifecycle transition")
        manifest = resolve_existing_file(Path(pending["candidate_manifest"]), "candidate_manifest")
        candidate = read_json(manifest)
        validate_candidate(candidate)
        if (
            candidate.get("state") != pending.get("to_state")
            or candidate.get("series_generation") != pending.get("target_generation")
        ):
            marker_temp = pending.get("marker_temp")
            if marker_temp:
                Path(marker_temp).unlink(missing_ok=True)
            series["pending_operation"] = None
            series["updated_at"] = utc_now()
            atomic_write_json(series_manifest, series)
            return "abandoned"
        entry = _entry(series, candidate, manifest)
        entry["state"] = candidate["state"]
        entry["outcome"] = candidate["outcome"]
        entry["sealed"] = candidate["sealed"]
        entry["operation_id"] = pending["operation_id"]
        series["head"] = deepcopy(entry)
        if candidate["state"] == "rc-candidate-ready":
            series["consecutive_successful_rcs"] += 1
        elif candidate["state"] == "rejected":
            series["consecutive_successful_rcs"] = 0
        completion_path = pending.get("completion_path")
        completion_event = pending.get("completion_event")
        if completion_path is not None:
            path = Path(completion_path)
            if not path.exists():
                if not isinstance(completion_event, dict):
                    raise LifecycleError("pending lifecycle completion event is invalid")
                atomic_write_json(path, completion_event)
        series["generation"] = pending["target_generation"]
        marker_path = pending.get("marker_path")
        marker_temp = pending.get("marker_temp")
        marker_value = pending.get("marker_value")
        if marker_path is not None:
            if not isinstance(marker_temp, str) or not isinstance(marker_value, dict):
                raise LifecycleError("pending publication marker metadata is invalid")
            temporary = Path(marker_temp)
            final = Path(marker_path)
            if not temporary.exists() and not final.exists():
                atomic_write_json(temporary, marker_value)
            if not final.exists():
                final.parent.mkdir(parents=True, exist_ok=True)
                os.replace(temporary, final)
            elif temporary.exists():
                temporary.unlink()
        series["pending_operation"] = None
        series["updated_at"] = utc_now()
        atomic_write_json(series_manifest, series)
        generation = series["generation"]
    from release_series import default_control_bundle, export_control_bundle

    export_control_bundle(series_manifest, default_control_bundle(series_manifest, generation))
    return "committed"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-manifest", type=Path)
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--transition")
    parser.add_argument("--phase", type=int, choices=(5, 6), default=5)
    parser.add_argument("--gate-evidence", type=Path)
    parser.add_argument("--rejection-reason")
    parser.add_argument("--handoff-ready", action="store_true")
    parser.add_argument("--current-route-id")
    parser.add_argument("--publication-marker", type=Path)
    parser.add_argument("--recover-series", type=Path)
    args = parser.parse_args(argv)
    modes = int(args.validate_only) + int(args.transition is not None) + int(args.recover_series is not None)
    if modes != 1:
        parser.error("choose exactly one of --validate-only, --transition, or --recover-series")
    if args.recover_series is None and args.candidate_manifest is None:
        parser.error("candidate validation and transitions require --candidate-manifest")
    try:
        if args.recover_series is not None:
            recovery = recover_pending_transition(args.recover_series)
            state = f"recovered-{recovery}"
            output = args.recover_series.resolve()
        elif args.validate_only:
            validate_only(args.candidate_manifest)
            state = read_json(args.candidate_manifest)["state"]
            output = args.candidate_manifest.resolve()
        else:
            value = transition_candidate(
                args.candidate_manifest,
                args.transition,
                phase=args.phase,
                gate_evidence=args.gate_evidence,
                rejection_reason=args.rejection_reason,
                handoff_ready=args.handoff_ready,
                current_route_id=args.current_route_id,
                publication_marker=args.publication_marker,
            )
            state = value["state"]
            output = args.candidate_manifest.resolve()
    except ReleaseStateError as error:
        print(f"RELEASE_LIFECYCLE_FAILED detail={error}", file=sys.stderr)
        return 1
    print(f"RELEASE_LIFECYCLE_OK state={state} candidate={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
