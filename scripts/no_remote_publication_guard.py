#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

"""Prove from workflow, context, and event evidence that publication did not run."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
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


class NoRemotePublicationError(ReleaseStateError):
    """Raised when remote publication is detected or cannot be disproved."""

    def __init__(self, message: str, status: str) -> None:
        super().__init__(message)
        self.status = status


PHASE_WORKFLOWS = {
    5: ["release-candidate.yml", "core-service-image-publish.yml", "core-kubernetes-assets-ci.yml"],
    6: [
        "release-candidate.yml",
        "core-service-image-publish.yml",
        "core-kubernetes-assets-ci.yml",
        "v1-functional-acceptance.yml",
    ],
}
REMOTE_COMMAND = re.compile(
    r"(?:cargo\s+publish|docker\s+(?:login|push)|(?:oras|helm)\s+push|gh\s+(?:release|workflow\s+run)|git\s+(?:push|tag))",
    re.IGNORECASE,
)
WRITE_PERMISSION = re.compile(r"^\s*(?:contents|packages|actions|id-token|attestations)\s*:\s*write\s*$", re.MULTILINE)
SECRET_ROUTE = re.compile(r"\$\{\{\s*secrets\.", re.IGNORECASE)
AUTOMATIC_RELEASE = re.compile(r"^\s*(?:release|registry_package)\s*:\s*$|^\s*tags\s*:", re.MULTILINE)
HANDOFF_SCRIPT_SUFFIXES = {".sh", ".ps1", ".bat", ".cmd"}
HANDOFF_FORBIDDEN_KEYS = {"command", "args", "shell", "workflow_dispatch_payload", "token", "secret"}


def audit_workflow_files(workflow_root: Path, workflow_names: list[str]) -> list[str]:
    findings: list[str] = []
    for name in workflow_names:
        path = workflow_root / name
        if not path.is_file():
            findings.append(f"missing workflow: {name}")
            continue
        text = path.read_text(encoding="utf-8")
        if WRITE_PERMISSION.search(text):
            findings.append(f"write permission in {name}")
        if SECRET_ROUTE.search(text):
            findings.append(f"publishing secret route in {name}")
        if REMOTE_COMMAND.search(text):
            findings.append(f"remote publication command in {name}")
        if AUTOMATIC_RELEASE.search(text):
            findings.append(f"automatic publication trigger in {name}")
    return findings


def audit_handoff(handoff_root: Path) -> list[str]:
    """Inspect handoff structure and executable surfaces for publication payloads."""

    findings: list[str] = []
    if not handoff_root.is_dir():
        return ["publication handoff root is missing"]
    manifest_path = handoff_root / "PUBLICATION_HANDOFF.json"
    if not manifest_path.is_file():
        return ["publication handoff manifest is missing"]
    manifest = read_json(manifest_path)
    if manifest.get("remote_publication", {}).get("status") != "not-executed":
        findings.append("publication handoff remote status is not not-executed")
    if manifest.get("future_publication", {}).get("executed") is not False:
        findings.append("publication handoff marks future publication as executed")

    def inspect_json(value: Any, relative: str) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if str(key).lower() in HANDOFF_FORBIDDEN_KEYS:
                    findings.append(f"executable or secret-bearing field in handoff JSON: {relative}:{key}")
                inspect_json(child, relative)
        elif isinstance(value, list):
            for child in value:
                inspect_json(child, relative)
        elif isinstance(value, str) and REMOTE_COMMAND.search(value):
            findings.append(f"remote publication payload in handoff JSON: {relative}")

    for path in sorted(handoff_root.rglob("*")):
        if path.is_symlink():
            findings.append(f"symbolic link in publication handoff: {path.relative_to(handoff_root)}")
            continue
        if not path.is_file():
            continue
        relative = path.relative_to(handoff_root).as_posix()
        if path.suffix.lower() in HANDOFF_SCRIPT_SUFFIXES:
            try:
                text = path.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError):
                findings.append(f"unreadable publication handoff script: {relative}")
            else:
                if REMOTE_COMMAND.search(text):
                    findings.append(f"remote publication command in handoff script: {relative}")
        elif path.suffix.lower() == ".json":
            try:
                inspect_json(read_json(path), relative)
            except ReleaseStateError as error:
                findings.append(f"invalid handoff JSON {relative}: {error}")
    return findings


def _runtime_evidence(
    candidate: dict[str, Any],
    context_root: Path,
    event_root: Path,
    current_route_id: str | None,
    required_route_ids: list[str],
) -> tuple[list[str], list[str], list[str], list[str]]:
    violations: list[str] = []
    indeterminate: list[str] = []
    dispatches: list[str] = []
    credential_names: list[str] = []
    contexts: dict[str, dict[str, Any]] = {}
    if not context_root.is_dir():
        indeterminate.append("worker execution-context root is missing")
    else:
        for path in sorted(context_root.rglob("*.json")):
            value = read_json(path)
            worker_id = value.get("worker_id")
            if not isinstance(worker_id, str) or worker_id in contexts:
                indeterminate.append(f"duplicate or invalid worker context: {path}")
                continue
            identity = (value.get("candidate_id"), value.get("version"), value.get("run_id"), value.get("attempt"))
            expected = (candidate["candidate_id"], candidate["version"], candidate["run_id"], candidate["attempt"])
            if identity != expected:
                indeterminate.append(f"worker context belongs to another candidate: {path}")
            contexts[worker_id] = value
            if value.get("publish_input") is True:
                dispatches.append(worker_id)
                violations.append(f"publishing input enabled for worker {worker_id}")
            if value.get("publishing_credentials_provided") is True:
                names = value.get("publishing_credential_names")
                if isinstance(names, list):
                    credential_names.extend(str(name) for name in names)
                violations.append(f"publishing credentials provided to worker {worker_id}")
    started: dict[str, tuple[Path, dict[str, Any]]] = {}
    completed: dict[str, tuple[Path, dict[str, Any]]] = {}
    if not event_root.is_dir():
        indeterminate.append("candidate event root is missing")
    else:
        for path in sorted(event_root.rglob("*.started.json")):
            value = read_json(path)
            route_id = value.get("route_id")
            if not isinstance(route_id, str) or route_id in started:
                indeterminate.append(f"duplicate or invalid started event: {path}")
                continue
            started[route_id] = (path, value)
        for path in sorted(event_root.rglob("*.completed.json")):
            value = read_json(path)
            route_id = value.get("route_id")
            if not isinstance(route_id, str) or route_id in completed:
                indeterminate.append(f"duplicate or invalid completed event: {path}")
                continue
            completed[route_id] = (path, value)
    if current_route_id is not None:
        current = started.pop(current_route_id, None)
        if current is None:
            indeterminate.append(f"current guard route has no started reservation: {current_route_id}")
        elif current_route_id in completed:
            indeterminate.append(f"current guard route completed before its audit: {current_route_id}")
        else:
            start = current[1]
            if start.get("candidate_id") != candidate["candidate_id"] or start.get("worker_id") not in contexts:
                indeterminate.append(f"current guard route is not attributable: {current_route_id}")
    if set(started) != set(completed):
        indeterminate.append(
            f"event reservations are not paired; missing_completed={sorted(set(started)-set(completed))}, orphan_completed={sorted(set(completed)-set(started))}"
        )
    for route_id in required_route_ids:
        if route_id not in started or route_id not in completed:
            indeterminate.append(f"required route is missing or incomplete: {route_id}")
            continue
        finish = completed[route_id][1]
        if finish.get("status") != "passed" or finish.get("exit_code") != 0:
            indeterminate.append(f"required route did not complete successfully: {route_id}")
    for route_id in sorted(set(started) & set(completed)):
        start = started[route_id][1]
        finish = completed[route_id][1]
        identity = (
            start.get("candidate_id"),
            start.get("version"),
            start.get("run_id"),
            start.get("attempt"),
            start.get("worker_id"),
            start.get("context_path"),
        )
        if identity != (
            finish.get("candidate_id"),
            finish.get("version"),
            finish.get("run_id"),
            finish.get("attempt"),
            finish.get("worker_id"),
            finish.get("context_path"),
        ):
            indeterminate.append(f"event pair identity mismatch for route {route_id}")
        event_candidate = (
            start.get("candidate_id"),
            start.get("version"),
            start.get("run_id"),
            start.get("attempt"),
        )
        expected_candidate = (
            candidate["candidate_id"],
            candidate["version"],
            candidate["run_id"],
            candidate["attempt"],
        )
        if event_candidate != expected_candidate or start.get("worker_id") not in contexts:
            indeterminate.append(f"route {route_id} has no current-candidate worker context")
        command = start.get("command")
        if isinstance(command, list) and REMOTE_COMMAND.search(" ".join(str(item) for item in command)):
            violations.append(f"remote publication command executed by route {route_id}")
    return violations, indeterminate, sorted(set(dispatches)), sorted(set(credential_names))


def _required_route_ids(candidate: dict[str, Any], audit_point: str | None) -> tuple[str, list[str]]:
    denominator = candidate.get("route_denominator")
    if not isinstance(denominator, dict) or denominator.get("schema_version") != 1:
        raise NoRemotePublicationError("candidate route denominator is missing", "indeterminate")
    audit_points = denominator.get("audit_points")
    if not isinstance(audit_points, dict) or not audit_points:
        raise NoRemotePublicationError("candidate route denominator has no audit points", "indeterminate")
    if audit_point is None:
        if len(audit_points) != 1:
            raise NoRemotePublicationError("no-remote audit point is required", "indeterminate")
        audit_point = next(iter(audit_points))
    route_ids = audit_points.get(audit_point)
    if not isinstance(route_ids, list) or not route_ids:
        raise NoRemotePublicationError(f"candidate has no route denominator for {audit_point}", "indeterminate")
    return audit_point, route_ids


def audit_no_remote_publication(
    candidate_manifest: Path,
    *,
    phase: int,
    context_root: Path,
    event_root: Path,
    workflow_root: Path,
    output: Path,
    audit_point: str | None = None,
    current_route_id: str | None = None,
    handoff_root: Path | None = None,
) -> dict[str, Any]:
    if phase not in PHASE_WORKFLOWS:
        raise NoRemotePublicationError("phase must be 5 or 6", "indeterminate")
    manifest = resolve_existing_file(candidate_manifest, "candidate_manifest")
    candidate = read_json(manifest)
    validate_candidate(candidate)
    audit_point, required_route_ids = _required_route_ids(candidate, audit_point)
    workflow_findings = audit_workflow_files(workflow_root, PHASE_WORKFLOWS[phase])
    violations, indeterminate, dispatches, credential_names = _runtime_evidence(
        candidate, context_root, event_root, current_route_id, required_route_ids
    )
    violations.extend(finding for finding in workflow_findings if not finding.startswith("missing workflow"))
    indeterminate.extend(finding for finding in workflow_findings if finding.startswith("missing workflow"))
    if handoff_root is not None:
        violations.extend(audit_handoff(handoff_root))
    status = "violation-detected" if violations else "indeterminate" if indeterminate else "not-executed"
    value = {
        "schema_version": 1,
        "candidate_id": candidate["candidate_id"],
        "version": candidate["version"],
        "run_id": candidate["run_id"],
        "attempt": candidate["attempt"],
        "phase": phase,
        "audit_point": audit_point,
        "required_route_ids": required_route_ids,
        "remote_publication": {"status": status},
        "remote_publication_workflow_dispatches": dispatches,
        "publishing_credentials_provided": bool(credential_names),
        "publishing_credential_names": credential_names,
        "workflow_files": PHASE_WORKFLOWS[phase],
        "handoff_scanned": handoff_root is not None,
        "violations": sorted(set(violations)),
        "indeterminate_reasons": sorted(set(indeterminate)),
        "generated_at": utc_now(),
    }
    ensure_no_digest_fields(value)
    atomic_write_json(output, value)
    if status != "not-executed":
        details = value["violations"] if violations else value["indeterminate_reasons"]
        raise NoRemotePublicationError("; ".join(details), status)
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-manifest", type=Path)
    parser.add_argument("--phase", type=int, choices=(5, 6), required=True)
    parser.add_argument("--context-root", type=Path)
    parser.add_argument("--event-root", type=Path)
    parser.add_argument("--workflow-root", type=Path, default=ROOT / ".github" / "workflows")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--static-only", action="store_true")
    parser.add_argument("--current-route-id")
    parser.add_argument("--audit-point")
    parser.add_argument("--handoff", type=Path)
    args = parser.parse_args(argv)
    if args.static_only:
        findings = audit_workflow_files(args.workflow_root, PHASE_WORKFLOWS[args.phase])
        if findings:
            print("NO_REMOTE_PUBLICATION_FAILED detail=" + "; ".join(findings), file=sys.stderr)
            return 1
        print(f"NO_REMOTE_PUBLICATION_STATIC_OK workflows={len(PHASE_WORKFLOWS[args.phase])}")
        return 0
    if not all((args.candidate_manifest, args.context_root, args.event_root, args.output)):
        parser.error("runtime audit requires --candidate-manifest, --context-root, --event-root, and --output")
    try:
        value = audit_no_remote_publication(
            args.candidate_manifest,
            phase=args.phase,
            context_root=args.context_root,
            event_root=args.event_root,
            workflow_root=args.workflow_root,
            output=args.output,
            audit_point=args.audit_point,
            current_route_id=args.current_route_id or os.environ.get("RELEASE_CANDIDATE_ROUTE_ID"),
            handoff_root=args.handoff,
        )
    except NoRemotePublicationError as error:
        print(f"NO_REMOTE_PUBLICATION_FAILED status={error.status} detail={error}", file=sys.stderr)
        return 1
    print(f"NO_REMOTE_PUBLICATION_OK status={value['remote_publication']['status']} output={args.output.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
