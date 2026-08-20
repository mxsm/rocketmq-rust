#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

"""Close candidate job outcomes into one atomic lifecycle input."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path, PurePosixPath
import shutil
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
    require_safe_id,
    resolve_existing_file,
    validate_candidate,
)


DEFAULT_POLICY = DISTRIBUTION / "candidate-stage-outcome-policy.json"
OUTCOME_NAME = "CANDIDATE_STAGE_OUTCOME.json"
INDEX_NAME = "CANDIDATE_STAGE_OUTCOMES.json"
WORKFLOW_RESULTS = {"success", "failure", "cancelled", "skipped"}
OUTCOME_STATUS_BY_WORKFLOW_RESULT = {
    "success": "success",
    "failure": "failed",
    "cancelled": "cancelled",
    "skipped": "cancelled",
}


class OutcomeError(ReleaseStateError):
    """Raised when job outcomes are ambiguous, incomplete, or unsafe."""


def _identity(value: dict[str, Any]) -> tuple[Any, ...]:
    return value.get("candidate_id"), value.get("version"), value.get("run_id"), value.get("attempt")


def _safe_relative(value: Any, label: str) -> PurePosixPath:
    if not isinstance(value, str) or not value or "\\" in value:
        raise OutcomeError(f"{label} must be a safe POSIX relative path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise OutcomeError(f"{label} must be a safe POSIX relative path")
    return path


def _load_policy(path: Path, profile: str) -> list[dict[str, Any]]:
    require_safe_id(profile, "profile")
    value = read_json(resolve_existing_file(path, "candidate outcome policy"))
    ensure_no_digest_fields(value)
    if value.get("schema_version") != 1 or set(value) != {"schema_version", "profiles"}:
        raise OutcomeError("candidate outcome policy fields are invalid")
    profiles = value.get("profiles")
    if not isinstance(profiles, dict):
        raise OutcomeError("candidate outcome policy profiles are invalid")
    entries = profiles.get(profile)
    if not isinstance(entries, list) or not entries:
        raise OutcomeError(f"candidate outcome policy profile is missing: {profile}")
    job_ids: list[str] = []
    normalized: list[dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {"job_id", "target", "result_ids", "route_ids"}:
            raise OutcomeError("candidate outcome policy entry is invalid")
        job_id = require_safe_id(entry.get("job_id"), "job_id")
        target = entry.get("target")
        if target is not None:
            require_safe_id(target, "target")
        result_ids = entry.get("result_ids")
        if (
            not isinstance(result_ids, list)
            or not result_ids
            or any(not isinstance(result_id, str) for result_id in result_ids)
        ):
            raise OutcomeError(f"candidate outcome result denominator is invalid: {job_id}")
        normalized_result_ids = [require_safe_id(result_id, "result_id") for result_id in result_ids]
        if len(normalized_result_ids) != len(set(normalized_result_ids)):
            raise OutcomeError(f"candidate outcome result denominator contains duplicates: {job_id}")
        route_ids = entry.get("route_ids")
        if (
            not isinstance(route_ids, list)
            or not route_ids
            or any(not isinstance(route_id, str) for route_id in route_ids)
        ):
            raise OutcomeError(f"candidate outcome route denominator is invalid: {job_id}")
        normalized_route_ids = [require_safe_id(route_id, "route_id") for route_id in route_ids]
        if len(normalized_route_ids) != len(set(normalized_route_ids)):
            raise OutcomeError(f"candidate outcome route denominator contains duplicates: {job_id}")
        job_ids.append(job_id)
        normalized.append(
            {
                "job_id": job_id,
                "target": target,
                "result_ids": normalized_result_ids,
                "route_ids": normalized_route_ids,
            }
        )
    if len(job_ids) != len(set(job_ids)):
        raise OutcomeError("candidate outcome policy contains duplicate jobs")
    return normalized


def _load_workflow_results(path: Path, candidate: dict[str, Any], expected: set[str]) -> dict[str, str]:
    value = read_json(resolve_existing_file(path, "workflow results"))
    ensure_no_digest_fields(value)
    allowed = {"schema_version", "candidate_id", "version", "run_id", "attempt", "jobs"}
    if set(value) != allowed or value.get("schema_version") != 1 or _identity(value) != _identity(candidate):
        raise OutcomeError("workflow results belong to another candidate")
    jobs = value.get("jobs")
    if not isinstance(jobs, dict) or set(jobs) != expected:
        raise OutcomeError("workflow job result denominator is incomplete or contains extras")
    if any(not isinstance(result, str) or result not in WORKFLOW_RESULTS for result in jobs.values()):
        raise OutcomeError("workflow job result contains an invalid status")
    return jobs


def _bundle_files(bundle: Path) -> set[str]:
    files: set[str] = set()
    for path in sorted(bundle.rglob("*")):
        if path.is_symlink():
            raise OutcomeError(f"candidate outcome bundle contains a symbolic link: {path}")
        if path.is_file():
            files.add(path.relative_to(bundle).as_posix())
        elif not path.is_dir():
            raise OutcomeError(f"candidate outcome bundle contains an unsupported file: {path}")
    return files


def _bundle_path(bundle: Path, relative: str, label: str) -> Path:
    path = _safe_relative(relative, label)
    resolved = bundle.joinpath(*path.parts).resolve()
    try:
        resolved.relative_to(bundle.resolve())
    except ValueError as error:
        raise OutcomeError(f"{label} escapes its bundle") from error
    return resolve_existing_file(resolved, label)


def _categorized_relative(value: Any, category: str, label: str) -> str:
    path = _safe_relative(value, label)
    if len(path.parts) < 2 or path.parts[0] != category:
        raise OutcomeError(f"{label} must be below {category}/")
    return path.as_posix()


def _validate_payload_identity(value: dict[str, Any], candidate: dict[str, Any], label: str) -> None:
    ensure_no_digest_fields(value)
    if value.get("schema_version") != 1 or _identity(value) != _identity(candidate):
        raise OutcomeError(f"{label} belongs to another candidate")


def _validate_bundle(
    bundle: Path,
    candidate: dict[str, Any],
    expected: dict[str, Any],
    workflow_result: str,
) -> tuple[dict[str, Any], list[tuple[str, Path]], list[str], list[str]]:
    outcome_path = resolve_existing_file(bundle / OUTCOME_NAME, "candidate job outcome")
    outcome = read_json(outcome_path)
    _validate_payload_identity(outcome, candidate, "candidate job outcome")
    allowed = {
        "schema_version", "candidate_id", "version", "run_id", "attempt", "job_id",
        "worker_id", "target", "status", "workflow_result", "sealed", "result_files",
        "event_pairs", "context_files",
    }
    if set(outcome) != allowed:
        raise OutcomeError(f"candidate job outcome fields are not closed: {expected['job_id']}")
    worker_id = require_safe_id(outcome.get("worker_id"), "worker_id")
    if (
        outcome.get("job_id") != expected["job_id"]
        or outcome.get("target") != expected["target"]
        or outcome.get("workflow_result") != workflow_result
        or outcome.get("sealed") is not True
        or outcome.get("status") not in {"success", "failed", "cancelled"}
        or outcome.get("status") != OUTCOME_STATUS_BY_WORKFLOW_RESULT[workflow_result]
    ):
        raise OutcomeError(f"candidate job outcome disagrees with workflow result: {expected['job_id']}")
    result_files = outcome.get("result_files")
    event_pairs = outcome.get("event_pairs")
    context_files = outcome.get("context_files")
    if not all(isinstance(value, list) for value in (result_files, event_pairs, context_files)):
        raise OutcomeError(f"candidate job outcome payload lists are invalid: {expected['job_id']}")
    if outcome["status"] == "success" and (not result_files or not event_pairs or not context_files):
        raise OutcomeError(f"successful candidate job outcome is incomplete: {expected['job_id']}")
    payloads: list[tuple[str, Path]] = []
    declared = {OUTCOME_NAME}
    result_ids: list[str] = []
    for relative in result_files:
        normalized = _categorized_relative(relative, "results", "candidate result")
        path = _bundle_path(bundle, normalized, "candidate result")
        value = read_json(path)
        _validate_payload_identity(value, candidate, "candidate result")
        result_id = require_safe_id(value.get("result_id"), "result_id")
        result_status = value.get("status")
        if result_id in result_ids:
            raise OutcomeError(f"candidate result ID is duplicated: {result_id}")
        if result_id not in expected["result_ids"]:
            raise OutcomeError(f"candidate result ID is outside the job denominator: {result_id}")
        if result_status not in {"passed", "failed", "cancelled"}:
            raise OutcomeError(f"candidate result status is invalid: {result_id}")
        if outcome["status"] == "success" and result_status != "passed":
            raise OutcomeError(f"successful job has a non-passed result: {expected['job_id']}")
        result_ids.append(result_id)
        declared.add(normalized)
        payloads.append((normalized, path))
    routes: set[str] = set()
    for pair in event_pairs:
        if not isinstance(pair, dict) or set(pair) != {"route_id", "started", "completed"}:
            raise OutcomeError(f"candidate event pair is invalid: {expected['job_id']}")
        route_id = require_safe_id(pair.get("route_id"), "route_id")
        if route_id in routes:
            raise OutcomeError(f"candidate event route is duplicated: {route_id}")
        routes.add(route_id)
        started_relative = _categorized_relative(pair["started"], "events", "started event")
        completed_relative = _categorized_relative(pair["completed"], "events", "completed event")
        started_path = _bundle_path(bundle, started_relative, "started event")
        completed_path = _bundle_path(bundle, completed_relative, "completed event")
        started = read_json(started_path)
        completed = read_json(completed_path)
        for value, label in ((started, "started event"), (completed, "completed event")):
            _validate_payload_identity(value, candidate, label)
            if value.get("route_id") != route_id or value.get("worker_id") != worker_id:
                raise OutcomeError(f"candidate event identity is invalid: {route_id}")
        completed_status = completed.get("status")
        exit_code = completed.get("exit_code")
        if started.get("status") != "started":
            raise OutcomeError(f"candidate started event status is invalid: {route_id}")
        if (
            completed_status not in {"passed", "failed", "cancelled"}
            or not isinstance(exit_code, int)
            or isinstance(exit_code, bool)
            or (completed_status == "passed") != (exit_code == 0)
        ):
            raise OutcomeError(f"candidate completed event status is invalid: {route_id}")
        if outcome["status"] == "success" and (
            completed_status != "passed" or exit_code != 0
        ):
            raise OutcomeError(f"successful job has a failed event: {route_id}")
        for normalized, path in ((started_relative, started_path), (completed_relative, completed_path)):
            declared.add(normalized)
            payloads.append((normalized, path))
    contexts: set[str] = set()
    for relative in context_files:
        normalized = _categorized_relative(relative, "contexts", "execution context")
        path = _bundle_path(bundle, normalized, "execution context")
        value = read_json(path)
        _validate_payload_identity(value, candidate, "execution context")
        context_worker = require_safe_id(value.get("worker_id"), "context worker_id")
        if context_worker in contexts or context_worker != worker_id:
            raise OutcomeError(f"candidate execution context is invalid: {expected['job_id']}")
        contexts.add(context_worker)
        if value.get("publish_input") is not False or value.get("publishing_credentials_provided") is not False:
            raise OutcomeError(f"candidate execution context is publication-enabled: {expected['job_id']}")
        declared.add(normalized)
        payloads.append((normalized, path))
    if _bundle_files(bundle) != declared:
        raise OutcomeError(f"candidate job outcome bundle has undeclared or missing files: {expected['job_id']}")
    missing_result_ids = sorted(set(expected["result_ids"]) - set(result_ids))
    if outcome["status"] == "success" and missing_result_ids:
        raise OutcomeError(f"successful job is missing required results: {expected['job_id']}")
    unknown_route_ids = sorted(routes - set(expected["route_ids"]))
    if unknown_route_ids:
        raise OutcomeError(f"candidate event route is outside the job denominator: {unknown_route_ids[0]}")
    missing_route_ids = sorted(set(expected["route_ids"]) - routes)
    if outcome["status"] == "success" and missing_route_ids:
        raise OutcomeError(f"successful job is missing required routes: {expected['job_id']}")
    return outcome, payloads, result_ids, sorted(routes)


def _copy_payload(source: Path, destination: Path) -> None:
    if destination.exists():
        raise OutcomeError(f"candidate outcome destination collides: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    with source.open("rb") as source_file, destination.open("rb") as destination_file:
        while True:
            source_chunk = source_file.read(1024 * 1024)
            destination_chunk = destination_file.read(1024 * 1024)
            if source_chunk != destination_chunk:
                raise OutcomeError(f"candidate outcome copy changed bytes: {destination}")
            if not source_chunk:
                break


def collect_outcomes(
    candidate_manifest: Path,
    bundles_root: Path,
    workflow_results: Path,
    policy: Path,
    profile: str,
    output_root: Path,
) -> Path:
    candidate = read_json(resolve_existing_file(candidate_manifest, "candidate manifest"))
    validate_candidate(candidate)
    ensure_no_digest_fields(candidate)
    expected = _load_policy(policy, profile)
    expected_ids = [entry["job_id"] for entry in expected]
    workflow = _load_workflow_results(workflow_results, candidate, set(expected_ids))
    bundles_root = bundles_root.resolve()
    if not bundles_root.is_dir():
        raise OutcomeError("candidate job bundle root is missing")
    bundle_jobs: set[str] = set()
    for path in bundles_root.iterdir():
        if path.is_symlink() or not path.is_dir():
            raise OutcomeError(f"candidate job bundle root contains an unsupported entry: {path.name}")
        bundle_jobs.add(path.name)
    unknown_jobs = bundle_jobs - set(expected_ids)
    if unknown_jobs:
        raise OutcomeError(f"unknown job bundle: {', '.join(sorted(unknown_jobs))}")
    output_root = output_root.resolve()
    if output_root.exists():
        raise OutcomeError(f"candidate outcome output already exists: {output_root}")
    staging = output_root.with_name(f".{output_root.name}.staging")
    if staging.exists():
        raise OutcomeError(f"stale candidate outcome staging directory exists: {staging}")
    jobs: list[dict[str, Any]] = []
    failed: list[str] = []
    try:
        staging.mkdir(parents=True)
        for entry in expected:
            job_id = entry["job_id"]
            bundle = bundles_root / job_id
            if not bundle.is_dir():
                failed.append(job_id)
                jobs.append(
                    {
                        "job_id": job_id,
                        "worker_id": None,
                        "target": entry["target"],
                        "status": "missing-worker",
                        "workflow_result": workflow[job_id],
                        "expected_result_ids": entry["result_ids"],
                        "result_ids": [],
                        "missing_result_ids": entry["result_ids"],
                        "expected_route_ids": entry["route_ids"],
                        "route_ids": [],
                        "missing_route_ids": entry["route_ids"],
                        "result_files": [],
                        "event_files": [],
                        "context_files": [],
                    }
                )
                continue
            outcome, payloads, result_ids, route_ids = _validate_bundle(
                bundle, candidate, entry, workflow[job_id]
            )
            copied = {"results": [], "events": [], "contexts": []}
            for relative, source in payloads:
                category = PurePosixPath(relative).parts[0]
                if category not in copied:
                    raise OutcomeError(f"candidate payload has an unsupported category: {relative}")
                destination_relative = f"{category}/{job_id}/" + "/".join(PurePosixPath(relative).parts[1:])
                _copy_payload(source, staging.joinpath(*PurePosixPath(destination_relative).parts))
                copied[category].append(destination_relative)
            if outcome["status"] != "success":
                failed.append(job_id)
            jobs.append(
                {
                    "job_id": job_id,
                    "worker_id": outcome["worker_id"],
                    "target": entry["target"],
                    "status": outcome["status"],
                    "workflow_result": workflow[job_id],
                    "expected_result_ids": entry["result_ids"],
                    "result_ids": result_ids,
                    "missing_result_ids": sorted(set(entry["result_ids"]) - set(result_ids)),
                    "expected_route_ids": entry["route_ids"],
                    "route_ids": route_ids,
                    "missing_route_ids": sorted(set(entry["route_ids"]) - set(route_ids)),
                    "result_files": copied["results"],
                    "event_files": copied["events"],
                    "context_files": copied["contexts"],
                }
            )
        index = {
            "schema_version": 1,
            "candidate_id": candidate["candidate_id"],
            "version": candidate["version"],
            "run_id": candidate["run_id"],
            "attempt": candidate["attempt"],
            "profile": profile,
            "expected_job_ids": expected_ids,
            "jobs": jobs,
            "failed_job_ids": failed,
            "all_required_passed": not failed,
            "remote_publication": {"status": "not-executed"},
        }
        ensure_no_digest_fields(index)
        atomic_write_json(staging / INDEX_NAME, index)
        os.replace(staging, output_root)
    except Exception:
        if staging.exists():
            shutil.rmtree(staging)
        raise
    return output_root / INDEX_NAME


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--bundles-root", type=Path, required=True)
    parser.add_argument("--workflow-results", type=Path, required=True)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        output = collect_outcomes(
            args.candidate_manifest,
            args.bundles_root,
            args.workflow_results,
            args.policy,
            args.profile,
            args.output_root,
        )
    except (OutcomeError, OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        print(f"CANDIDATE_STAGE_OUTCOMES_FAILED detail={error}", file=sys.stderr)
        return 1
    value = read_json(output)
    print(
        "CANDIDATE_STAGE_OUTCOMES_OK "
        f"jobs={len(value['jobs'])} failed={len(value['failed_job_ids'])} output={output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
