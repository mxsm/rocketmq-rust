#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

"""Assemble a local-only publication handoff from an existing final candidate."""

from __future__ import annotations

import argparse
from datetime import date
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "distribution") not in sys.path:
    sys.path.insert(0, str(ROOT / "distribution"))

from release_state import (
    ReleaseStateError,
    atomic_write_json,
    ensure_no_digest_fields,
    read_json,
    resolve_existing_file,
    validate_candidate,
)


EXCLUDED_CAPABILITIES = ["BrokerContainer", "Dashboard", "DLedger CommitLog", "MCP", "OpenMessaging", "SRE"]
TARGETS = ["x86_64-unknown-linux-gnu", "x86_64-pc-windows-msvc", "x86_64-apple-darwin"]


class HandoffBuildError(ReleaseStateError):
    """Raised when the publication handoff cannot be assembled without ambiguity."""


def _safe_relative(value: str, label: str) -> PurePosixPath:
    if not isinstance(value, str) or not value or "\\" in value:
        raise HandoffBuildError(f"{label} must be a safe POSIX relative path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise HandoffBuildError(f"{label} must be a safe POSIX relative path")
    return path


def _identity(value: dict[str, Any]) -> tuple[Any, ...]:
    return value.get("candidate_id"), value.get("version"), value.get("run_id"), value.get("attempt")


def _load_candidate(manifest: Path, candidate_root: Path) -> tuple[Path, dict[str, Any], Path]:
    manifest = resolve_existing_file(manifest, "candidate manifest")
    candidate = read_json(manifest)
    validate_candidate(candidate)
    candidate_root = candidate_root.resolve(strict=True)
    if not candidate_root.is_dir():
        raise HandoffBuildError("candidate root must be a directory")
    if (
        candidate["candidate_kind"] != "final"
        or candidate["version"] != "1.0.0"
        or candidate["state"] != "ga-candidate-ready"
        or candidate["sealed"]
        or candidate["outcome"] != "success"
    ):
        raise HandoffBuildError("handoff requires an unsealed successful ga-candidate-ready final candidate")
    ensure_no_digest_fields(candidate)
    return manifest, candidate, candidate_root


def _load_identity_file(path: Path, candidate: dict[str, Any], label: str) -> dict[str, Any]:
    value = read_json(resolve_existing_file(path, label))
    if _identity(value) != _identity(candidate):
        raise HandoffBuildError(f"{label} belongs to another candidate run")
    ensure_no_digest_fields(value)
    return value


def _validate_known_issues(issues: Any) -> list[dict[str, Any]]:
    if not isinstance(issues, list):
        raise HandoffBuildError("candidate known issues must be a list")
    identifiers: set[str] = set()
    normalized: list[dict[str, Any]] = []
    for issue in issues:
        if not isinstance(issue, dict):
            raise HandoffBuildError("candidate known issue must be an object")
        if issue.get("resolution_status") == "closed":
            continue
        identifier = issue.get("issue_id")
        if not isinstance(identifier, str) or not identifier or identifier in identifiers:
            raise HandoffBuildError("candidate known issue identifiers must be unique")
        identifiers.add(identifier)
        if issue.get("severity") not in {"Medium", "Low"}:
            raise HandoffBuildError("critical/high or invalid known issues block handoff")
        for field in ("impact", "workaround", "owner", "target_version"):
            if not isinstance(issue.get(field), str) or not issue[field]:
                raise HandoffBuildError(f"known issue {identifier} is missing {field}")
        if issue.get("approval_status") != "approved" or issue.get("approver") != "mxsm":
            raise HandoffBuildError(f"known issue {identifier} has no release approver waiver")
        try:
            expires = date.fromisoformat(issue.get("waiver_expiry"))
        except (TypeError, ValueError) as error:
            raise HandoffBuildError(f"known issue {identifier} waiver expiry is invalid") from error
        if expires < date.today():
            raise HandoffBuildError(f"known issue {identifier} waiver has expired")
        normalized.append(issue)
    return normalized


def _copy_file(
    source: Path,
    destination: Path,
    *,
    source_domain: str,
    source_relative: str,
    destination_relative: str,
    bindings: list[dict[str, Any]],
) -> None:
    source = resolve_existing_file(source, "handoff source")
    if source.is_symlink():
        raise HandoffBuildError(f"handoff source cannot be a symbolic link: {source}")
    if destination.exists():
        raise HandoffBuildError(f"handoff destination collides: {destination_relative}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.copying")
    try:
        shutil.copyfile(source, temporary)
        if source.read_bytes() != temporary.read_bytes():
            raise HandoffBuildError(f"handoff copy byte content differs: {destination_relative}")
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    bindings.append(
        {
            "source_domain": source_domain,
            "source": source_relative,
            "destination": destination_relative,
            "type": "file",
            "size": source.stat().st_size,
        }
    )


def _copy_directory(
    source_root: Path,
    destination_root: Path,
    *,
    source_domain: str,
    source_prefix: str,
    destination_prefix: str,
    bindings: list[dict[str, Any]],
) -> None:
    if not source_root.is_dir() or source_root.is_symlink():
        raise HandoffBuildError(f"required handoff source directory is missing or unsafe: {source_prefix}")
    files = [path for path in sorted(source_root.rglob("*")) if path.is_file()]
    if not files:
        raise HandoffBuildError(f"required handoff source directory is empty: {source_prefix}")
    for source in files:
        if source.is_symlink():
            raise HandoffBuildError(f"handoff source contains a symbolic link: {source}")
        relative = source.relative_to(source_root).as_posix()
        source_relative = f"{source_prefix}/{relative}"
        destination_relative = f"{destination_prefix}/{relative}"
        _copy_file(
            source,
            destination_root.joinpath(*PurePosixPath(relative).parts),
            source_domain=source_domain,
            source_relative=source_relative,
            destination_relative=destination_relative,
            bindings=bindings,
        )


def _render_known_issues(issues: list[dict[str, Any]]) -> str:
    lines = ["# Known issues", ""]
    if not issues:
        return "\n".join(lines + ["No approved known issues remain.", ""])
    for issue in issues:
        lines.extend(
            [
                f"## {issue['issue_id']} ({issue['severity']})",
                "",
                f"- Impact: {issue['impact']}",
                f"- Workaround: {issue['workaround']}",
                f"- Owner: {issue['owner']}",
                f"- Target version: {issue['target_version']}",
                f"- Waiver: {issue['approver']} through {issue['waiver_expiry']}",
                "",
            ]
        )
    return "\n".join(lines)


def _file_inventory(root: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise HandoffBuildError(f"handoff contains a symbolic link: {path}")
        if path.is_file() and path.name != "PUBLICATION_HANDOFF.json":
            records.append({"path": path.relative_to(root).as_posix(), "type": "file", "size": path.stat().st_size})
        elif not path.is_file() and not path.is_dir():
            raise HandoffBuildError(f"handoff contains an unsupported file type: {path}")
    return records


def build_draft(
    candidate_manifest: Path,
    candidate_root: Path,
    source_root: Path,
    output_root: Path,
    source_map_path: Path,
) -> Path:
    """Build one closed staging handoff without rebuilding any candidate artifact."""

    _manifest, candidate, candidate_root = _load_candidate(candidate_manifest, candidate_root)
    source_root = source_root.resolve(strict=True)
    if not source_root.is_dir():
        raise HandoffBuildError("candidate source root must be a directory")
    source_map = read_json(resolve_existing_file(source_map_path, "publication handoff source map"))
    ensure_no_digest_fields(source_map)
    if source_map.get("schema_version") != 1:
        raise HandoffBuildError("unsupported publication handoff source map")
    artifact = _load_identity_file(candidate_root / "ARTIFACT_INDEX.json", candidate, "artifact index")
    evidence = _load_identity_file(candidate_root / "EVIDENCE_INDEX.json", candidate, "evidence index")
    no_remote = _load_identity_file(candidate_root / "NO_REMOTE_PUBLICATION.json", candidate, "no-remote evidence")
    if no_remote.get("remote_publication", {}).get("status") != "not-executed":
        raise HandoffBuildError("remote publication status must be not-executed")
    if no_remote.get("publishing_credentials_provided") is not False or no_remote.get(
        "remote_publication_workflow_dispatches"
    ) != []:
        raise HandoffBuildError("publication credentials or workflow dispatches are forbidden")
    if artifact.get("remote_publication") != "not-executed" or evidence.get("status") != "passed":
        raise HandoffBuildError("candidate artifact/evidence indexes are not publication-handoff ready")
    if sorted(artifact.get("targets", [])) != sorted(TARGETS):
        raise HandoffBuildError("candidate artifact target denominator is incomplete")
    result_ids = evidence.get("result_ids")
    if not isinstance(result_ids, list) or not result_ids or len(result_ids) != len(set(result_ids)):
        raise HandoffBuildError("candidate evidence result denominator is invalid")
    issues = _validate_known_issues(candidate["known_issues"])

    output_root = output_root.resolve()
    parent = output_root / candidate["version"] / candidate["run_id"]
    staging = parent / f".attempt-{candidate['attempt']}.staging"
    final = parent / f"attempt-{candidate['attempt']}"
    if staging.exists() or final.exists():
        raise HandoffBuildError("publication handoff staging or final path already exists")
    bindings: list[dict[str, Any]] = []
    try:
        staging.mkdir(parents=True)
        roots = {"candidate": candidate_root, "repository": source_root}
        for entry in source_map.get("directories", []):
            if not isinstance(entry, dict) or entry.get("source_domain") not in roots:
                raise HandoffBuildError("publication handoff directory mapping is invalid")
            source_relative = _safe_relative(entry.get("source"), "handoff directory source").as_posix()
            destination_relative = _safe_relative(entry.get("destination"), "handoff directory destination").as_posix()
            _copy_directory(
                roots[entry["source_domain"]].joinpath(*PurePosixPath(source_relative).parts),
                staging.joinpath(*PurePosixPath(destination_relative).parts),
                source_domain=entry["source_domain"],
                source_prefix=source_relative,
                destination_prefix=destination_relative,
                bindings=bindings,
            )
        for entry in source_map.get("files", []):
            if not isinstance(entry, dict) or entry.get("source_domain") not in roots:
                raise HandoffBuildError("publication handoff file mapping is invalid")
            source_relative = _safe_relative(entry.get("source"), "handoff file source").as_posix()
            destination_relative = _safe_relative(entry.get("destination"), "handoff file destination").as_posix()
            _copy_file(
                roots[entry["source_domain"]].joinpath(*PurePosixPath(source_relative).parts),
                staging.joinpath(*PurePosixPath(destination_relative).parts),
                source_domain=entry["source_domain"],
                source_relative=source_relative,
                destination_relative=destination_relative,
                bindings=bindings,
            )
        (staging / "docs" / "KNOWN_ISSUES.md").write_text(
            _render_known_issues(issues), encoding="utf-8", newline="\n"
        )
        (staging / "REMOTE_PUBLICATION_NOT_EXECUTED.md").write_text(
            "# Remote publication not executed\n\n"
            "This candidate is an unofficial community distribution prepared for a separate future publication task. "
            "No crate registry publication, remote Git tag or release, OCI registry promotion, or Chart repository publication was executed.\n",
            encoding="utf-8",
            newline="\n",
        )
        handoff = {
            "schema_version": 1,
            "candidate_id": candidate["candidate_id"],
            "version": candidate["version"],
            "run_id": candidate["run_id"],
            "attempt": candidate["attempt"],
            "candidate_state": candidate["state"],
            "candidate_generation": candidate["generation"],
            "series_id": candidate["series_id"],
            "series_generation": candidate["series_generation"],
            "distribution_identity": "unofficial-community",
            "distribution_name": "RocketMQ Rust Community Distribution",
            "release_approver": "mxsm",
            "official_apache_release": False,
            "remote_publication": {
                "status": "not-executed",
                "publishing_credentials_provided": False,
                "workflow_dispatches": [],
            },
            "future_publication": {
                "executed": False,
                "crate_registry": "crates.io",
                "git_tag": "v1.0.0",
                "release_title": "RocketMQ Rust Community Distribution 1.0.0",
                "oci_namespace": "ghcr.io/mxsm/rocketmq-rust",
                "helm_chart": "rocketmq-rust-core",
                "requires_separate_authorization": True,
            },
            "targets": TARGETS,
            "evidence_result_ids": result_ids,
            "excluded_capabilities": EXCLUDED_CAPABILITIES,
            "controller_boundary": "Rust-native controller functional parity; no Java controller wire or quorum interoperability.",
            "java_data_migration_profile": "not-declared",
            "deferred_validation": ["long-running RC soak", "remote publication", "post-publication smoke"],
            "known_issue_ids": [issue["issue_id"] for issue in issues],
            "source_bindings": bindings,
            "files": _file_inventory(staging),
        }
        ensure_no_digest_fields(handoff)
        atomic_write_json(staging / "PUBLICATION_HANDOFF.json", handoff)
    except Exception:
        if staging.exists():
            shutil.rmtree(staging)
        raise
    return staging


def refresh_evidence(
    staging: Path,
    candidate_manifest: Path,
    candidate_root: Path,
    evidence_index: Path,
    no_remote_evidence: Path,
) -> Path:
    """Apply the final H01 evidence cut, scan it, and close the mutable draft."""

    _manifest, candidate, candidate_root = _load_candidate(candidate_manifest, candidate_root)
    staging = staging.resolve(strict=True)
    handoff_path = resolve_existing_file(staging / "PUBLICATION_HANDOFF.json", "publication handoff manifest")
    handoff = read_json(handoff_path)
    if _identity(handoff) != _identity(candidate) or (staging / "PUBLICATION_READY.json").exists():
        raise HandoffBuildError("handoff evidence refresh candidate identity or readiness state is invalid")
    evidence = _load_identity_file(evidence_index, candidate, "refreshed evidence index")
    no_remote = _load_identity_file(no_remote_evidence, candidate, "refreshed no-remote evidence")
    if evidence.get("status") != "passed" or not {
        "H01-LINUX",
        "H01-WINDOWS",
        "H01-MACOS",
    }.issubset(set(evidence.get("result_ids", []))):
        raise HandoffBuildError("refreshed evidence index does not contain the closed H01 platform denominator")
    if no_remote.get("remote_publication", {}).get("status") != "not-executed":
        raise HandoffBuildError("refreshed no-remote evidence is not not-executed")
    replacements = (
        (resolve_existing_file(evidence_index, "refreshed evidence index"), "evidence/EVIDENCE_INDEX.json"),
        (resolve_existing_file(no_remote_evidence, "refreshed no-remote evidence"), "evidence/NO_REMOTE_PUBLICATION.json"),
    )
    for source, relative in replacements:
        try:
            source_relative = source.relative_to(candidate_root).as_posix()
        except ValueError as error:
            raise HandoffBuildError(f"refreshed evidence must live within the candidate root: {source}") from error
        destination = staging.joinpath(*PurePosixPath(relative).parts)
        temporary = destination.with_name(f".{destination.name}.refreshing")
        try:
            shutil.copyfile(source, temporary)
            if source.read_bytes() != temporary.read_bytes():
                raise HandoffBuildError(f"refreshed evidence byte content differs: {relative}")
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
        bindings = [item for item in handoff["source_bindings"] if item.get("destination") != relative]
        bindings.append(
            {
                "source_domain": "candidate",
                "source": source_relative,
                "destination": relative,
                "type": "file",
                "size": source.stat().st_size,
            }
        )
        handoff["source_bindings"] = bindings
    from verify_publication_handoff import _scan_file

    for path in sorted(staging.rglob("*")):
        if path.is_file() and path.name not in {"PUBLICATION_HANDOFF.json", "SECRET_SCAN.json"}:
            _scan_file(path, path.relative_to(staging).as_posix())
    secret_scan = {
        "schema_version": 1,
        "candidate_id": candidate["candidate_id"],
        "version": candidate["version"],
        "run_id": candidate["run_id"],
        "attempt": candidate["attempt"],
        "status": "passed",
        "findings": [],
    }
    ensure_no_digest_fields(secret_scan)
    atomic_write_json(staging / "evidence" / "SECRET_SCAN.json", secret_scan)
    handoff["evidence_result_ids"] = evidence["result_ids"]
    handoff["files"] = _file_inventory(staging)
    atomic_write_json(handoff_path, handoff)
    return staging


def finalize_draft(staging: Path) -> Path:
    """Atomically move a closed staging handoff to its pre-ready final path."""

    staging = staging.resolve()
    name = staging.name
    if not name.startswith(".attempt-") or not name.endswith(".staging"):
        raise HandoffBuildError("handoff draft path must use .attempt-N.staging")
    final = staging.with_name(name[1:-8])
    if final.exists():
        raise HandoffBuildError(f"publication handoff final already exists: {final}")
    if not staging.is_dir():
        raise HandoffBuildError(f"publication handoff draft does not exist: {staging}")
    if (staging / "PUBLICATION_READY.json").exists():
        raise HandoffBuildError("draft must not contain a publication-ready marker")
    os.replace(staging, final)
    return final


def discard_exported_draft(staging: Path) -> None:
    """Remove only an exported staging draft so later jobs must import its bundle."""

    staging = staging.resolve(strict=True)
    if not staging.is_dir() or not staging.name.startswith(".attempt-") or not staging.name.endswith(".staging"):
        raise HandoffBuildError("discard target is not a publication handoff staging directory")
    if not (staging / "PUBLICATION_HANDOFF.json").is_file() or (staging / "PUBLICATION_READY.json").exists():
        raise HandoffBuildError("discard target is not an exported pre-ready handoff draft")
    shutil.rmtree(staging)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--draft", action="store_true")
    mode.add_argument("--refresh-evidence", type=Path)
    mode.add_argument("--finalize", type=Path)
    mode.add_argument("--discard-draft", type=Path)
    parser.add_argument("--candidate-manifest", type=Path)
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--evidence-index", type=Path)
    parser.add_argument("--no-remote-evidence", type=Path)
    parser.add_argument(
        "--source-map", type=Path, default=ROOT / "distribution" / "publication-handoff-source-map.json"
    )
    args = parser.parse_args(argv)
    try:
        if args.draft:
            if not all((args.candidate_manifest, args.candidate_root, args.source_root, args.output_root)):
                parser.error("--draft requires candidate manifest/root, source root, and output root")
            output = build_draft(
                args.candidate_manifest, args.candidate_root, args.source_root, args.output_root, args.source_map
            )
        elif args.refresh_evidence:
            if not all((args.candidate_manifest, args.candidate_root, args.evidence_index, args.no_remote_evidence)):
                parser.error("--refresh-evidence requires candidate manifest/root and both evidence inputs")
            output = refresh_evidence(
                args.refresh_evidence,
                args.candidate_manifest,
                args.candidate_root,
                args.evidence_index,
                args.no_remote_evidence,
            )
        elif args.finalize:
            output = finalize_draft(args.finalize)
        else:
            discard_exported_draft(args.discard_draft)
            output = args.discard_draft.resolve()
    except (HandoffBuildError, OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        print(f"PUBLICATION_HANDOFF_BUILD_FAILED detail={error}", file=sys.stderr)
        return 1
    print(f"PUBLICATION_HANDOFF_BUILD_OK output={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
