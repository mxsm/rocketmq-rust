#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

"""Drive one candidate to a verified lifecycle state or a sealed rejection."""

from __future__ import annotations

import argparse
import json
from pathlib import Path, PurePosixPath
import subprocess
import sys
import tarfile
from typing import Any, Sequence


ROOT = Path(__file__).resolve().parents[1]
DISTRIBUTION = ROOT / "distribution"
for import_root in (ROOT, DISTRIBUTION):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

from release_state import (
    ReleaseStateError,
    ensure_no_digest_fields,
    read_json,
    resolve_existing_file,
    validate_candidate,
    validate_series,
)
from capture_candidate_execution_context import capture_context
from collect_candidate_stage_outcomes import _load_policy


class CandidateLifecycleError(ReleaseStateError):
    """Raised when the high-level candidate lifecycle cannot close safely."""


MODES = {"StageRc", "FinalizeRc", "FinalizeFinalFunctional", "RejectFinalHandoff"}
LIFECYCLE_WORKER = "candidate-lifecycle-finalizer"
REJECTION_WORKER = "candidate-lifecycle-rejector"
OUTCOME_POLICY = ROOT / "distribution/candidate-stage-outcome-policy.json"
LIFECYCLE_CONFIG = ROOT / "distribution/config/release-lifecycle.json"
OUTCOME_INDEX_FIELDS = {
    "schema_version",
    "candidate_id",
    "version",
    "run_id",
    "attempt",
    "profile",
    "expected_job_ids",
    "jobs",
    "failed_job_ids",
    "all_required_passed",
    "remote_publication",
}
OUTCOME_JOB_FIELDS = {
    "job_id",
    "worker_id",
    "target",
    "status",
    "workflow_result",
    "expected_result_ids",
    "result_ids",
    "missing_result_ids",
    "expected_route_ids",
    "route_ids",
    "missing_route_ids",
    "result_files",
    "event_files",
    "context_files",
}


def _candidate(path: Path) -> tuple[Path, dict]:
    manifest = resolve_existing_file(path, "candidate manifest")
    value = read_json(manifest)
    validate_candidate(value)
    ensure_no_digest_fields(value)
    if Path(value["candidate_root"]).resolve() != manifest.parent:
        raise CandidateLifecycleError("candidate manifest and candidate root disagree")
    return manifest, value


def _run(command: Sequence[str]) -> None:
    completed = subprocess.run(list(command), cwd=ROOT, check=False)
    if completed.returncode != 0:
        raise CandidateLifecycleError(
            f"candidate lifecycle child failed with exit code {completed.returncode}: {command[0]}"
        )


def _wrapped(
    candidate_manifest: Path,
    *,
    route_id: str,
    worker_id: str,
    context: Path,
    event_root: Path,
    command: Sequence[str],
) -> None:
    _run(
        [
            sys.executable,
            str(ROOT / "scripts/release_candidate_command.py"),
            "run",
            "--candidate-manifest",
            str(candidate_manifest),
            "--route-id",
            route_id,
            "--worker-id",
            worker_id,
            "--context",
            str(context),
            "--event-root",
            str(event_root),
            "--",
            *command,
        ]
    )


def _context(
    candidate_manifest: Path,
    root: Path,
    worker_id: str = LIFECYCLE_WORKER,
) -> tuple[Path, Path]:
    context_root = root / "contexts"
    event_root = root / "events"
    context = context_root / f"{worker_id}.json"
    if not context.is_file():
        context = capture_context(candidate_manifest, worker_id, context_root)
    return context, event_root


def _export_candidate_control(candidate_manifest: Path) -> Path:
    _manifest, candidate = _candidate(candidate_manifest)
    output = (
        Path(candidate["candidate_root"])
        / "transfer"
        / f"CANDIDATE_CONTROL_BUNDLE.g{candidate['generation']}.tar"
    )
    if not output.is_file():
        _run(
            [
                sys.executable,
                str(ROOT / "distribution/transfer_candidate.py"),
                "export-build-control",
                "--candidate-manifest",
                str(candidate_manifest),
                "--output",
                str(output),
            ]
        )
    _validate_candidate_control(output, candidate_manifest, Path(candidate["series_manifest"]))
    series = read_json(resolve_existing_file(Path(candidate["series_manifest"]), "release series"))
    series_bundle = (
        Path(candidate["series_manifest"]).resolve().parent
        / f"RELEASE_SERIES_CONTROL_BUNDLE.g{series['generation']}.tar"
    )
    if not series_bundle.is_file():
        raise CandidateLifecycleError("release-series control bundle was not created")
    return output


def _validate_candidate_control(bundle: Path, candidate_manifest: Path, series_manifest: Path) -> None:
    candidate = read_json(candidate_manifest)
    series = read_json(resolve_existing_file(series_manifest, "release series"))
    validate_candidate(candidate)
    validate_series(series)
    try:
        with tarfile.open(resolve_existing_file(bundle, "candidate control bundle"), "r") as archive:
            members = archive.getmembers()
            names = [member.name for member in members]
            expected_names = {
                "CANDIDATE_TRANSFER.json",
                "payload/CANDIDATE_RUN.json",
                "payload/RELEASE_SERIES.json",
            }
            if set(names) != expected_names or len(names) != len(expected_names):
                raise CandidateLifecycleError("candidate control bundle members are not closed")
            for member in members:
                if not member.isfile() or member.issym() or member.islnk():
                    raise CandidateLifecycleError("candidate control bundle contains an unsafe member")
            control_file = archive.extractfile("CANDIDATE_TRANSFER.json")
            candidate_file = archive.extractfile("payload/CANDIDATE_RUN.json")
            series_file = archive.extractfile("payload/RELEASE_SERIES.json")
            if control_file is None or candidate_file is None or series_file is None:
                raise CandidateLifecycleError("candidate control bundle is unreadable")
            control = json.loads(control_file.read())
            archived_candidate = json.loads(candidate_file.read())
            archived_series = json.loads(series_file.read())
    except (tarfile.TarError, json.JSONDecodeError, UnicodeDecodeError) as error:
        raise CandidateLifecycleError(f"candidate control bundle is invalid: {error}") from error
    identity = (
        control.get("candidate_id"),
        control.get("version"),
        control.get("run_id"),
        control.get("attempt"),
    )
    expected_identity = (
        candidate["candidate_id"],
        candidate["version"],
        candidate["run_id"],
        candidate["attempt"],
    )
    files = control.get("files")
    expected_files = {
        "CANDIDATE_RUN.json": candidate_manifest.stat().st_size,
        "RELEASE_SERIES.json": series_manifest.stat().st_size,
    }
    actual_files = (
        {item.get("path"): item.get("size") for item in files if isinstance(item, dict)}
        if isinstance(files, list)
        else {}
    )
    if (
        control.get("schema_version") != 1
        or control.get("bundle_kind") != "build-control"
        or identity != expected_identity
        or not isinstance(files, list)
        or len(files) != len(expected_files)
        or actual_files != expected_files
        or archived_candidate != candidate
        or archived_series != series
    ):
        raise CandidateLifecycleError("candidate control bundle does not match committed state")
    for value in (control, archived_candidate, archived_series):
        ensure_no_digest_fields(value)


def _safe_index_file(root: Path, relative: Any, prefix: str) -> Path:
    if not isinstance(relative, str) or "\\" in relative:
        raise CandidateLifecycleError("candidate stage outcome contains an unsafe path")
    path = PurePosixPath(relative)
    if path.is_absolute() or len(path.parts) < 3 or path.parts[0] != prefix:
        raise CandidateLifecycleError("candidate stage outcome path has the wrong canonical root")
    if any(part in {"", ".", ".."} for part in path.parts):
        raise CandidateLifecycleError("candidate stage outcome contains an unsafe path")
    resolved = root.joinpath(*path.parts).resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as error:
        raise CandidateLifecycleError("candidate stage outcome path escapes its root") from error
    return resolve_existing_file(resolved, "candidate stage outcome payload")


def _stage_outcomes(
    candidate_manifest: Path,
    index_path: Path,
) -> tuple[Path, list[str]]:
    _manifest, candidate = _candidate(candidate_manifest)
    if index_path.is_symlink() or index_path.name != "CANDIDATE_STAGE_OUTCOMES.json":
        raise CandidateLifecycleError("candidate stage outcome index path is invalid")
    index_path = resolve_existing_file(index_path, "candidate stage outcome index")
    root = index_path.parent.resolve()
    index = read_json(index_path)
    ensure_no_digest_fields(index)
    identity = (
        index.get("candidate_id"),
        index.get("version"),
        index.get("run_id"),
        index.get("attempt"),
    )
    expected_identity = (
        candidate["candidate_id"],
        candidate["version"],
        candidate["run_id"],
        candidate["attempt"],
    )
    entries = _load_policy(OUTCOME_POLICY, "release-candidate")
    expected_job_ids = [entry["job_id"] for entry in entries]
    jobs = index.get("jobs")
    if (
        index.get("schema_version") != 1
        or set(index) != OUTCOME_INDEX_FIELDS
        or identity != expected_identity
        or index.get("profile") != "release-candidate"
        or index.get("expected_job_ids") != expected_job_ids
        or not isinstance(jobs, list)
        or len(jobs) != len(entries)
        or index.get("remote_publication") != {"status": "not-executed"}
    ):
        raise CandidateLifecycleError("candidate stage outcome index identity or denominator is invalid")
    required_results: list[str] = []
    required_routes: list[str] = []
    failed_jobs: list[str] = []
    declared_files = {index_path.name}
    for job, expected in zip(jobs, entries, strict=True):
        if not isinstance(job, dict) or set(job) != OUTCOME_JOB_FIELDS:
            raise CandidateLifecycleError("candidate stage outcome job fields are not closed")
        if (
            job.get("job_id") != expected["job_id"]
            or job.get("target") != expected["target"]
            or job.get("expected_result_ids") != expected["result_ids"]
            or job.get("expected_route_ids") != expected["route_ids"]
        ):
            raise CandidateLifecycleError(f"candidate stage outcome job denominator is invalid: {expected['job_id']}")
        required_results.extend(expected["result_ids"])
        required_routes.extend(expected["route_ids"])
        success = job.get("status") == "success" and job.get("workflow_result") == "success"
        if success and (
            job.get("result_ids") != expected["result_ids"]
            or job.get("missing_result_ids") != []
            or job.get("route_ids") != expected["route_ids"]
            or job.get("missing_route_ids") != []
        ):
            raise CandidateLifecycleError(f"successful candidate job is incomplete: {expected['job_id']}")
        if not success:
            failed_jobs.append(expected["job_id"])
        files = {
            "results": job.get("result_files"),
            "events": job.get("event_files"),
            "contexts": job.get("context_files"),
        }
        if any(not isinstance(paths, list) for paths in files.values()):
            raise CandidateLifecycleError(f"candidate job payload index is invalid: {expected['job_id']}")
        for prefix, paths in files.items():
            for relative in paths:
                path = _safe_index_file(root, relative, prefix)
                if path.is_symlink():
                    raise CandidateLifecycleError("candidate stage outcome payload is a symbolic link")
                declared_files.add(path.relative_to(root).as_posix())
    if len(required_results) != len(set(required_results)) or len(required_routes) != len(set(required_routes)):
        raise CandidateLifecycleError("candidate result or route denominator contains duplicates")
    declared_routes = candidate.get("route_denominator", {}).get("audit_points", {}).get(
        "full-matrix-finalizer"
    )
    if declared_routes != required_routes:
        raise CandidateLifecycleError("candidate route denominator does not match stage outcomes")
    if index.get("failed_job_ids") != failed_jobs or index.get("all_required_passed") != (not failed_jobs):
        raise CandidateLifecycleError("candidate stage outcome aggregate status is inconsistent")
    actual_files: set[str] = set()
    for path in root.rglob("*"):
        if path.is_symlink():
            raise CandidateLifecycleError("candidate stage outcome root contains a symbolic link")
        if path.is_file():
            actual_files.add(path.relative_to(root).as_posix())
    if actual_files != declared_files:
        raise CandidateLifecycleError("candidate stage outcome root has undeclared or missing files")
    if failed_jobs:
        raise CandidateLifecycleError("candidate stage outcomes contain failed or missing workers")
    return root, required_results


def _finalizer_paths(candidate_manifest: Path, outcome_root: Path) -> tuple[Path, Path, Path, Path]:
    manifest, _candidate_value = _candidate(candidate_manifest)
    context, event_root = _context(manifest, outcome_root)
    evidence_root = manifest.parent / "evidence"
    evidence_root.mkdir(parents=True, exist_ok=True)
    return context, event_root, evidence_root, outcome_root / "results"


def _run_candidate_validation(
    candidate_manifest: Path,
    context: Path,
    event_root: Path,
) -> None:
    _wrapped(
        candidate_manifest,
        route_id="candidate-finalize-validate",
        worker_id=LIFECYCLE_WORKER,
        context=context,
        event_root=event_root,
        command=[
            sys.executable,
            str(ROOT / "distribution/candidate_run.py"),
            "validate",
            "--candidate-manifest",
            str(candidate_manifest),
        ],
    )


def _run_no_remote(
    candidate_manifest: Path,
    context: Path,
    event_root: Path,
    evidence_root: Path,
) -> Path:
    output = evidence_root / "NO_REMOTE_PUBLICATION.json"
    route_id = "candidate-finalize-no-remote"
    _wrapped(
        candidate_manifest,
        route_id=route_id,
        worker_id=LIFECYCLE_WORKER,
        context=context,
        event_root=event_root,
        command=[
            sys.executable,
            str(ROOT / "scripts/no_remote_publication_guard.py"),
            "--candidate-manifest",
            str(candidate_manifest),
            "--phase",
            "6",
            "--audit-point",
            "full-matrix-finalizer",
            "--current-route-id",
            route_id,
            "--context-root",
            str(context.parent),
            "--event-root",
            str(event_root),
            "--output",
            str(output),
        ],
    )
    return output


def _run_full_matrix_evidence(
    candidate_manifest: Path,
    context: Path,
    event_root: Path,
    evidence_root: Path,
    result_root: Path,
    required_results: list[str],
    no_remote: Path,
) -> Path:
    config = read_json(LIFECYCLE_CONFIG)
    capabilities = config.get("required_capabilities")
    release_results = config.get("phase5_required_result_ids")
    if not isinstance(capabilities, list) or not isinstance(release_results, list):
        raise CandidateLifecycleError("release lifecycle result denominator is invalid")
    output = evidence_root / "FULL_MATRIX_EVIDENCE.json"
    _wrapped(
        candidate_manifest,
        route_id="candidate-finalize-evidence",
        worker_id=LIFECYCLE_WORKER,
        context=context,
        event_root=event_root,
        command=[
            sys.executable,
            str(ROOT / "scripts/release_evidence_guard.py"),
            "--candidate-manifest",
            str(candidate_manifest),
            "--result-root",
            str(result_root),
            "--phase",
            "6",
            "--gate-stage",
            "full-matrix",
            "--require-result-ids",
            ",".join(required_results),
            "--release-result-ids",
            ",".join(release_results),
            "--require-capability-ids",
            ",".join(capabilities),
            "--no-remote-evidence",
            str(no_remote),
            "--output",
            str(output),
        ],
    )
    return output


def _run_source_snapshot(
    candidate_manifest: Path,
    context: Path,
    event_root: Path,
) -> None:
    _wrapped(
        candidate_manifest,
        route_id="candidate-finalize-source-snapshot",
        worker_id=LIFECYCLE_WORKER,
        context=context,
        event_root=event_root,
        command=[
            sys.executable,
            str(ROOT / "distribution/create_candidate_source_snapshot.py"),
            "--candidate-manifest",
            str(candidate_manifest),
        ],
    )


def _run_final_delta(
    candidate_manifest: Path,
    context: Path,
    event_root: Path,
    evidence_root: Path,
    parent_manifest: Path,
    source_root: Path,
) -> None:
    _wrapped(
        candidate_manifest,
        route_id="candidate-finalize-delta",
        worker_id=LIFECYCLE_WORKER,
        context=context,
        event_root=event_root,
        command=[
            sys.executable,
            str(ROOT / "scripts/final_candidate_delta_guard.py"),
            "--candidate-manifest",
            str(candidate_manifest),
            "--parent-manifest",
            str(parent_manifest),
            "--source-root",
            str(source_root),
            "--output",
            str(evidence_root / "FINAL_CANDIDATE_DELTA.json"),
        ],
    )


def _transition_ready(
    candidate_manifest: Path,
    context: Path,
    event_root: Path,
    gate_evidence: Path,
    transition: str,
) -> None:
    route_id = "candidate-finalize-ready"
    _wrapped(
        candidate_manifest,
        route_id=route_id,
        worker_id=LIFECYCLE_WORKER,
        context=context,
        event_root=event_root,
        command=[
            sys.executable,
            str(ROOT / "scripts/release_lifecycle_guard.py"),
            "--candidate-manifest",
            str(candidate_manifest),
            "--transition",
            transition,
            "--phase",
            "6",
            "--gate-evidence",
            str(gate_evidence),
            "--current-route-id",
            route_id,
        ],
    )


def _reject(
    candidate_manifest: Path,
    *,
    root: Path,
    reason: str,
) -> None:
    manifest, candidate = _candidate(candidate_manifest)
    if candidate["sealed"]:
        _export_candidate_control(manifest)
        return
    context, event_root = _context(manifest, root, REJECTION_WORKER)
    route_id = "candidate-finalize-reject"
    _wrapped(
        manifest,
        route_id=route_id,
        worker_id=REJECTION_WORKER,
        context=context,
        event_root=event_root,
        command=[
            sys.executable,
            str(ROOT / "scripts/release_lifecycle_guard.py"),
            "--candidate-manifest",
            str(manifest),
            "--transition",
            "rejected",
            "--phase",
            "6",
            "--rejection-reason",
            reason,
            "--current-route-id",
            route_id,
        ],
    )
    _export_candidate_control(manifest)


def finalize(
    mode: str,
    candidate_manifest: Path,
    stage_outcomes_index: Path | None,
    parent_manifest: Path | None,
    source_root: Path | None,
) -> None:
    manifest, candidate = _candidate(candidate_manifest)
    target_state = "rc-candidate-ready" if mode == "FinalizeRc" else "ga-candidate-ready"
    expected_kind = "rc" if mode == "FinalizeRc" else "final"
    if candidate["candidate_kind"] != expected_kind:
        raise CandidateLifecycleError(f"{mode} requires a {expected_kind} candidate")
    if candidate["state"] == target_state:
        _export_candidate_control(manifest)
        return
    if stage_outcomes_index is None:
        try:
            _reject(manifest, root=manifest.parent / "lifecycle", reason=f"{mode} has no stage outcome index")
        finally:
            raise CandidateLifecycleError(f"{mode} requires --stage-outcomes-index")
    outcome_root = stage_outcomes_index.resolve().parent
    rejection_root = manifest.parent / "lifecycle"
    try:
        outcome_root, required_results = _stage_outcomes(manifest, stage_outcomes_index)
        rejection_root = outcome_root
        context, event_root, evidence_root, result_root = _finalizer_paths(manifest, outcome_root)
        _run_candidate_validation(manifest, context, event_root)
        no_remote = _run_no_remote(manifest, context, event_root, evidence_root)
        gate = _run_full_matrix_evidence(
            manifest,
            context,
            event_root,
            evidence_root,
            result_root,
            required_results,
            no_remote,
        )
        if mode == "FinalizeRc":
            _run_source_snapshot(manifest, context, event_root)
            transition = "rc-candidate-ready"
        else:
            if parent_manifest is None or source_root is None:
                raise CandidateLifecycleError(
                    "FinalizeFinalFunctional requires --parent-manifest and --source-root"
                )
            _run_final_delta(
                manifest,
                context,
                event_root,
                evidence_root,
                parent_manifest,
                source_root,
            )
            transition = "ga-candidate-ready"
        _transition_ready(manifest, context, event_root, gate, transition)
        _export_candidate_control(manifest)
    except (ReleaseStateError, OSError) as error:
        current = read_json(manifest)
        if not current["sealed"]:
            _reject(
                manifest,
                root=rejection_root,
                reason=f"{mode} gate failure",
            )
        raise CandidateLifecycleError(str(error)) from error


def reject_final_handoff(candidate_manifest: Path, reason: str | None) -> None:
    manifest, candidate = _candidate(candidate_manifest)
    if candidate["candidate_kind"] != "final":
        raise CandidateLifecycleError("RejectFinalHandoff requires a final candidate")
    if candidate["state"] == "rejected" and candidate["sealed"]:
        _export_candidate_control(manifest)
        return
    if candidate["state"] != "ga-candidate-ready" or candidate["sealed"]:
        raise CandidateLifecycleError("RejectFinalHandoff requires an open ga-candidate-ready final")
    _reject(
        manifest,
        root=manifest.parent / "lifecycle",
        reason=reason or "final publication handoff failed",
    )


def stage_rc(candidate_manifest: Path) -> None:
    manifest, candidate = _candidate(candidate_manifest)
    if candidate["candidate_kind"] != "rc":
        raise CandidateLifecycleError("StageRc requires an RC candidate")
    if candidate["state"] == "staged-rc" and not candidate["sealed"]:
        _export_candidate_control(manifest)
        return
    if candidate["state"] != "development" or candidate["sealed"]:
        raise CandidateLifecycleError("StageRc requires an unsealed development RC")
    context, event_root = _context(manifest, manifest.parent / "lifecycle")
    route_id = "candidate-stage-rc"
    _wrapped(
        manifest,
        route_id=route_id,
        worker_id=LIFECYCLE_WORKER,
        context=context,
        event_root=event_root,
        command=[
            sys.executable,
            str(ROOT / "scripts/release_lifecycle_guard.py"),
            "--candidate-manifest",
            str(manifest),
            "--transition",
            "staged-rc",
            "--phase",
            "6",
            "--current-route-id",
            route_id,
        ],
    )
    _export_candidate_control(manifest)


def execute(
    mode: str,
    candidate_manifest: Path,
    *,
    stage_outcomes_index: Path | None,
    parent_manifest: Path | None,
    source_root: Path | None,
    rejection_reason: str | None,
) -> None:
    if mode == "StageRc":
        try:
            stage_rc(candidate_manifest)
        except (ReleaseStateError, OSError) as error:
            manifest, candidate = _candidate(candidate_manifest)
            if not candidate["sealed"]:
                _reject(
                    manifest,
                    root=manifest.parent / "lifecycle",
                    reason="StageRc transition failed",
                )
            raise CandidateLifecycleError(str(error)) from error
        return
    if mode in {"FinalizeRc", "FinalizeFinalFunctional"}:
        finalize(mode, candidate_manifest, stage_outcomes_index, parent_manifest, source_root)
        return
    reject_final_handoff(candidate_manifest, rejection_reason)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=sorted(MODES), required=True)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--stage-outcomes-index", type=Path)
    parser.add_argument("--parent-manifest", type=Path)
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--rejection-reason")
    args = parser.parse_args(argv)
    try:
        execute(
            args.mode,
            args.candidate_manifest,
            stage_outcomes_index=args.stage_outcomes_index,
            parent_manifest=args.parent_manifest,
            source_root=args.source_root,
            rejection_reason=args.rejection_reason,
        )
    except (ReleaseStateError, OSError) as error:
        print(f"V1_CANDIDATE_LIFECYCLE_FAILED mode={args.mode} detail={error}", file=sys.stderr)
        return 1
    value = read_json(args.candidate_manifest)
    print(
        "V1_CANDIDATE_LIFECYCLE_OK "
        f"mode={args.mode} state={value['state']} sealed={str(value['sealed']).lower()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
