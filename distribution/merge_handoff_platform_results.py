#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

"""Merge the three attributable platform handoff results into candidate evidence."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "distribution") not in sys.path:
    sys.path.insert(0, str(ROOT / "distribution"))

from release_state import ReleaseStateError, atomic_write_json, ensure_no_digest_fields, read_json, validate_candidate


EXPECTED = {
    "H01-LINUX": ("linux", "x86_64-unknown-linux-gnu"),
    "H01-WINDOWS": ("windows", "x86_64-pc-windows-msvc"),
    "H01-MACOS": ("macos", "x86_64-apple-darwin"),
}


class PlatformMergeError(ReleaseStateError):
    """Raised when a platform bundle is incomplete, mixed, or unattributable."""


def _identity(value: dict[str, Any]) -> tuple[Any, ...]:
    return value.get("candidate_id"), value.get("version"), value.get("run_id"), value.get("attempt")


def _load_one(root: Path, result_id: str, candidate: dict[str, Any]) -> dict[str, Any]:
    matches = [path for path in root.rglob(f"{result_id}.json") if path.parent.name != "events"]
    if len(matches) != 1:
        raise PlatformMergeError(f"expected exactly one bundle result for {result_id}, found {len(matches)}")
    result_path = matches[0]
    bundle = result_path.parent
    result = read_json(result_path)
    platform, target = EXPECTED[result_id]
    expected_identity = _identity(candidate)
    if _identity(result) != expected_identity:
        raise PlatformMergeError(f"platform result belongs to another candidate: {result_id}")
    if (
        result.get("schema_version") != 1
        or result.get("result_id") != result_id
        or result.get("platform") != platform
        or result.get("target") != target
        or result.get("status") != "passed"
        or result.get("skipped") is not False
        or not isinstance(result.get("assertions"), list)
        or not result["assertions"]
        or any(item.get("status") != "passed" for item in result["assertions"] if isinstance(item, dict))
    ):
        raise PlatformMergeError(f"platform result is incomplete or not passed: {result_id}")
    worker = result.get("worker_id")
    if not isinstance(worker, str) or not worker:
        raise PlatformMergeError(f"platform result has no worker identity: {result_id}")
    started_path = bundle / "events" / f"{result_id}.started.json"
    completed_path = bundle / "events" / f"{result_id}.completed.json"
    context_path = bundle / "contexts" / f"{worker}.json"
    if not all(path.is_file() and not path.is_symlink() for path in (started_path, completed_path, context_path)):
        raise PlatformMergeError(f"platform result event/context bundle is incomplete: {result_id}")
    started = read_json(started_path)
    completed = read_json(completed_path)
    context = read_json(context_path)
    event_identity = expected_identity + (worker, result_id)
    if _identity(started) + (started.get("worker_id"), started.get("route_id")) != event_identity:
        raise PlatformMergeError(f"started event worker identity mismatch: {result_id}")
    if _identity(completed) + (completed.get("worker_id"), completed.get("route_id")) != event_identity:
        raise PlatformMergeError(f"completed event worker identity mismatch: {result_id}")
    if completed.get("exit_code") != 0:
        raise PlatformMergeError(f"platform route did not complete successfully: {result_id}")
    if _identity(context) + (context.get("worker_id"),) != expected_identity + (worker,):
        raise PlatformMergeError(f"execution context worker identity mismatch: {result_id}")
    if context.get("publish_input") is not False or context.get("publishing_credentials_provided") is not False:
        raise PlatformMergeError(f"platform worker was publication-enabled: {result_id}")
    if started.get("context_path") != f"contexts/{worker}.json" or completed.get("context_path") != started.get(
        "context_path"
    ):
        raise PlatformMergeError(f"event context binding mismatch: {result_id}")
    for value in (result, started, completed, context):
        ensure_no_digest_fields(value)
    return {
        "result_id": result_id,
        "worker_id": worker,
        "result": result_path,
        "started": started_path,
        "completed": completed_path,
        "context": context_path,
        "archive_id": result.get("archive_id"),
        "target": target,
    }


def _copy_new(source: Path, destination: Path) -> None:
    if destination.exists():
        raise PlatformMergeError(f"canonical handoff evidence already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.staging")
    if temporary.exists():
        raise PlatformMergeError(f"stale platform merge staging file exists: {temporary}")
    try:
        shutil.copyfile(source, temporary)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def merge_platform_results(
    candidate_manifest: Path,
    bundles_root: Path,
    evidence_root: Path,
    event_root: Path,
    context_root: Path,
    base_evidence_index: Path | None = None,
) -> Path:
    """Validate and atomically import the closed H01 platform denominator."""

    candidate = read_json(candidate_manifest.resolve(strict=True))
    validate_candidate(candidate)
    if candidate["candidate_kind"] != "final" or candidate["version"] != "1.0.0":
        raise PlatformMergeError("handoff platform evidence requires a 1.0.0 final candidate")
    bundles_root = bundles_root.resolve()
    if not bundles_root.is_dir():
        raise PlatformMergeError("platform bundle root does not exist")
    records = [_load_one(bundles_root, result_id, candidate) for result_id in EXPECTED]
    workers = [record["worker_id"] for record in records]
    if len(workers) != len(set(workers)):
        raise PlatformMergeError("platform worker identities must be unique")
    base: dict[str, Any] | None = None
    if base_evidence_index is not None:
        base = read_json(base_evidence_index.resolve(strict=True))
        if _identity(base) != _identity(candidate) or base.get("status") != "passed":
            raise PlatformMergeError("base candidate evidence index is invalid")
        result_ids = base.get("result_ids")
        if not isinstance(result_ids, list) or len(result_ids) != len(set(result_ids)):
            raise PlatformMergeError("base candidate evidence result denominator is invalid")
        for result_id in EXPECTED:
            if result_id in result_ids:
                raise PlatformMergeError(f"base evidence already contains handoff result: {result_id}")
            result_ids.append(result_id)
        ensure_no_digest_fields(base)
    destinations: list[tuple[Path, Path]] = []
    for record in records:
        result_id = record["result_id"]
        destinations.extend(
            [
                (record["result"], evidence_root / f"{result_id}.json"),
                (record["started"], event_root / f"{result_id}.started.json"),
                (record["completed"], event_root / f"{result_id}.completed.json"),
                (record["context"], context_root / f"{record['worker_id']}.json"),
            ]
        )
    if any(destination.exists() for _source, destination in destinations):
        raise PlatformMergeError("canonical platform merge destinations must all be absent")
    for source, destination in destinations:
        _copy_new(source, destination)
    index = {
        "schema_version": 1,
        "candidate_id": candidate["candidate_id"],
        "version": candidate["version"],
        "run_id": candidate["run_id"],
        "attempt": candidate["attempt"],
        "result_ids": list(EXPECTED),
        "workers": workers,
        "targets": [record["target"] for record in records],
        "archive_ids": [record["archive_id"] for record in records],
        "remote_publication": {"status": "not-executed"},
    }
    ensure_no_digest_fields(index)
    output = evidence_root / "HANDOFF_PLATFORM_INDEX.json"
    atomic_write_json(output, index)
    if base is not None:
        atomic_write_json(evidence_root / "EVIDENCE_INDEX.json", base)
    return output


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--bundles-root", type=Path, required=True)
    parser.add_argument("--evidence-root", type=Path, required=True)
    parser.add_argument("--event-root", type=Path, required=True)
    parser.add_argument("--context-root", type=Path, required=True)
    parser.add_argument("--base-evidence-index", type=Path)
    args = parser.parse_args(argv)
    try:
        output = merge_platform_results(
            args.candidate_manifest,
            args.bundles_root,
            args.evidence_root,
            args.event_root,
            args.context_root,
            args.base_evidence_index,
        )
    except (PlatformMergeError, OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        print(f"HANDOFF_PLATFORM_MERGE_FAILED detail={error}", file=sys.stderr)
        return 1
    print(f"HANDOFF_PLATFORM_MERGE_OK results=3 output={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
