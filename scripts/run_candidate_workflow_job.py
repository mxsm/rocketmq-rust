#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

"""Run one candidate workflow job and seal its portable outcome bundle."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import tempfile
from typing import Any, Sequence


ROOT = Path(__file__).resolve().parents[1]
for module_root in (ROOT / "distribution", ROOT / "scripts"):
    if str(module_root) not in sys.path:
        sys.path.insert(0, str(module_root))

import capture_candidate_execution_context
import release_candidate_command
from release_state import (
    ReleaseStateError,
    atomic_write_json,
    read_json,
    require_safe_id,
    resolve_existing_file,
    validate_candidate,
)


class WorkflowJobError(ReleaseStateError):
    """Raised when a workflow job cannot produce an attributable outcome."""


def _identity(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": candidate["candidate_id"],
        "version": candidate["version"],
        "run_id": candidate["run_id"],
        "attempt": candidate["attempt"],
    }


def _result_record(
    candidate: dict[str, Any], result_id: str, status: str, command: Sequence[str]
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        **_identity(candidate),
        "phase": 6,
        "gate_stage": "full-matrix",
        "result_id": result_id,
        "result_kind": "check",
        "status": status,
        "command": list(command),
        "exit_code": 0 if status == "passed" else 1,
        "matched_test_count": 0,
        "executed_test_count": 0,
        "passed_test_count": 0,
        "failed_test_count": 0 if status == "passed" else 1,
        "ignored_test_count": 0,
        "capability_ids": [],
        "result_path": f"results/{result_id}.json",
    }


def _copy_or_record_result(
    candidate: dict[str, Any],
    candidate_root: Path,
    staging: Path,
    result_id: str,
    status: str,
    command: Sequence[str],
) -> str:
    require_safe_id(result_id, "result_id")
    relative = f"results/{result_id}.json"
    source = candidate_root / relative
    destination = staging / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    if source.is_file():
        value = read_json(source)
        if any(value.get(key) != expected for key, expected in _identity(candidate).items()):
            raise WorkflowJobError(f"candidate result belongs to another candidate: {result_id}")
        if value.get("result_id") != result_id:
            raise WorkflowJobError(f"candidate result ID is inconsistent: {result_id}")
        value["phase"] = 6
        value["gate_stage"] = "full-matrix"
        value["result_path"] = relative
        atomic_write_json(destination, value)
    else:
        atomic_write_json(destination, _result_record(candidate, result_id, status, command))
    return relative


def _publish_bundle(staging: Path, output: Path) -> None:
    output = output.resolve()
    if output.exists():
        raise WorkflowJobError(f"candidate workflow outcome already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    staging.replace(output)


def run_job(
    candidate_manifest: Path,
    *,
    job_id: str,
    worker_id: str,
    target: str | None,
    result_ids: Sequence[str],
    output: Path,
    command: Sequence[str],
) -> int:
    require_safe_id(job_id, "job_id")
    require_safe_id(worker_id, "worker_id")
    if target is not None:
        require_safe_id(target, "target")
    if not result_ids or len(result_ids) != len(set(result_ids)):
        raise WorkflowJobError("result ID denominator must be non-empty and unique")
    if not command:
        raise WorkflowJobError("candidate workflow command cannot be empty")

    candidate_manifest = resolve_existing_file(candidate_manifest, "candidate manifest")
    candidate = read_json(candidate_manifest)
    validate_candidate(candidate)
    candidate_root = Path(candidate["candidate_root"]).resolve()
    if candidate_root != candidate_manifest.parent.resolve():
        raise WorkflowJobError("candidate manifest and root disagree")

    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output.parent) as temporary:
        staging = Path(temporary) / job_id
        context = capture_candidate_execution_context.capture_context(
            candidate_manifest, worker_id, staging / "contexts"
        )
        event_root = staging / "events"
        exit_code = release_candidate_command.run_command(
            candidate_manifest,
            route_id=job_id,
            worker_id=worker_id,
            context_path=context,
            event_root=event_root,
            command=command,
            portable_root=staging,
            cwd=ROOT,
        )
        workflow_result = "success" if exit_code == 0 else "failure"
        result_status = "passed" if exit_code == 0 else "failed"
        result_files = [
            _copy_or_record_result(
                candidate,
                candidate_root,
                staging,
                result_id,
                result_status,
                command,
            )
            for result_id in result_ids
        ]
        started = f"events/{job_id}.started.json"
        completed = f"events/{job_id}.completed.json"
        context_relative = f"contexts/{worker_id}.json"
        for relative in (started, completed, context_relative):
            resolve_existing_file(staging / relative, relative)
        atomic_write_json(
            staging / "CANDIDATE_STAGE_OUTCOME.json",
            {
                "schema_version": 1,
                **_identity(candidate),
                "job_id": job_id,
                "worker_id": worker_id,
                "target": target,
                "status": result_status.replace("passed", "success"),
                "workflow_result": workflow_result,
                "sealed": True,
                "result_files": result_files,
                "event_pairs": [
                    {"route_id": job_id, "started": started, "completed": completed}
                ],
                "context_files": [context_relative],
            },
        )
        _publish_bundle(staging, output)
    return exit_code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--target")
    parser.add_argument("--result-ids", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command and args.command[0] == "--" else args.command
    try:
        return run_job(
            args.candidate_manifest,
            job_id=args.job_id,
            worker_id=args.worker_id,
            target=args.target,
            result_ids=tuple(item for item in args.result_ids.split(",") if item),
            output=args.output,
            command=command,
        )
    except (ReleaseStateError, OSError, json.JSONDecodeError) as error:
        print(f"CANDIDATE_WORKFLOW_JOB_FAILED detail={error}", file=sys.stderr)
        return 125


if __name__ == "__main__":
    raise SystemExit(main())
