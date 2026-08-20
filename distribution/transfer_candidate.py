#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import argparse
import io
import json
from pathlib import Path, PurePosixPath
import subprocess
import sys
import tarfile
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "distribution") not in sys.path:
    sys.path.insert(0, str(ROOT / "distribution"))

from release_archive_common import ArchiveError, load_candidate, require_relative_path
from release_state import (
    ReleaseStateError,
    ensure_no_digest_fields,
    read_json,
    resolve_existing_file,
    resolve_within,
    validate_candidate,
    validate_series,
)


EXCLUDED_PREFIXES = (
    ".git/",
    ".worktrees/",
    "rocketmq-dashboard/",
    "rocketmq-sre/",
    "rocketmq-tools/rocketmq-mcp/",
    "rocketmq-website/",
    "target/",
)


def _candidate_evidence_file(root: Path, candidate: dict[str, Any], name: str) -> Path:
    """Resolve one evidence input and normalize it into the transfer root."""

    declared = candidate.get("evidence_index") if name == "EVIDENCE_INDEX.json" else None
    candidates: list[Path] = []
    if isinstance(declared, str) and declared:
        candidates.append(root / declared)
    candidates.extend((root / "evidence" / name, root / name))
    seen: set[Path] = set()
    for path in candidates:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        if path.is_file():
            return resolve_existing_file(path, f"candidate {name}")
    raise ArchiveError(f"candidate {name} is missing")


def _tracked_source_files(source_root: Path) -> list[Path]:
    completed = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=source_root,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise ArchiveError("cannot enumerate canonical source files with git ls-files")
    values = completed.stdout.decode("utf-8").split("\0")
    selected: list[Path] = []
    for value in values:
        if not value or value.startswith(EXCLUDED_PREFIXES):
            continue
        path = source_root / value
        if path.is_file() and not path.is_symlink():
            selected.append(path)
    return sorted(selected)


def _add_bytes(archive: tarfile.TarFile, name: str, content: bytes) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(content)
    info.mode = 0o644
    info.mtime = 0
    archive.addfile(info, io.BytesIO(content))


def _write_bundle(
    candidate: dict[str, Any],
    *,
    bundle_kind: str,
    output: Path,
    records: list[tuple[str, Path]],
) -> Path:
    normalized: list[tuple[str, bytes]] = []
    seen: set[str] = set()
    manifest_records: list[dict[str, Any]] = []
    for relative, path in records:
        require_relative_path(relative, "candidate transfer path")
        if relative in seen:
            raise ArchiveError(f"candidate transfer path is duplicated: {relative}")
        seen.add(relative)
        content = resolve_existing_file(path, "candidate transfer input").read_bytes()
        manifest_records.append({"path": relative, "type": "file", "size": len(content)})
        normalized.append((relative, content))
    manifest = {
        "schema_version": 1,
        "bundle_kind": bundle_kind,
        "candidate_id": candidate["candidate_id"],
        "version": candidate["version"],
        "run_id": candidate["run_id"],
        "attempt": candidate["attempt"],
        "files": manifest_records,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(output, "w") as archive:
        _add_bytes(
            archive,
            "CANDIDATE_TRANSFER.json",
            (json.dumps(manifest, indent=2) + "\n").encode(),
        )
        for relative, content in normalized:
            _add_bytes(archive, f"payload/{relative}", content)
    return output


def export_bundle(
    candidate_manifest: Path,
    *,
    bundle_kind: str,
    output: Path,
    source_root: Path,
    files: list[Path],
) -> Path:
    _manifest, candidate, candidate_root = load_candidate(candidate_manifest)
    output = resolve_within(candidate_root, output, "candidate transfer output")
    if output.exists():
        raise ArchiveError(f"candidate transfer bundle already exists: {output}")
    source_root = source_root.resolve()
    normalized: list[tuple[str, Path]] = []
    for path in files:
        path = resolve_existing_file(path, "candidate transfer input")
        try:
            relative = path.relative_to(source_root).as_posix()
        except ValueError as error:
            raise ArchiveError(f"candidate transfer input escapes source root: {path}") from error
        require_relative_path(relative, "candidate transfer path")
        normalized.append((relative, path))
    return _write_bundle(candidate, bundle_kind=bundle_kind, output=output, records=normalized)


def export_build_source(candidate_manifest: Path, output: Path, source_root: Path) -> Path:
    source_root = source_root.resolve()
    return export_bundle(
        candidate_manifest,
        bundle_kind="build-source",
        output=output,
        source_root=source_root,
        files=_tracked_source_files(source_root),
    )


def export_common_inputs(candidate_manifest: Path, input_root: Path, output: Path) -> Path:
    """Export the closed, candidate-scoped common release input tree."""

    input_root = input_root.resolve(strict=True)
    if not input_root.is_dir() or not (input_root / "COMMON_RELEASE_INPUTS.json").is_file():
        raise ArchiveError("common release inputs are incomplete")
    files = sorted(
        path
        for path in input_root.rglob("*")
        if path.is_file() and not path.is_symlink()
    )
    if not files:
        raise ArchiveError("common release inputs are empty")
    return export_bundle(
        candidate_manifest,
        bundle_kind="common-inputs",
        output=output,
        source_root=input_root,
        files=files,
    )


def export_build_control(candidate_manifest: Path, output: Path) -> Path:
    """Export current control state without reopening sealed artifact mutation."""

    manifest = resolve_existing_file(candidate_manifest, "candidate manifest")
    candidate = read_json(manifest)
    validate_candidate(candidate)
    ensure_no_digest_fields(candidate)
    root = Path(candidate["candidate_root"]).resolve()
    if root != manifest.parent:
        raise ArchiveError("candidate manifest and candidate root disagree")
    output = resolve_within(root, output, "candidate transfer output")
    if output.exists():
        raise ArchiveError(f"candidate transfer bundle already exists: {output}")
    records = [("CANDIDATE_RUN.json", manifest)]
    series = candidate.get("series_manifest")
    if not isinstance(series, str):
        raise ArchiveError("candidate has no release-series manifest")
    series_path = resolve_existing_file(Path(series), "release series")
    series_value = read_json(series_path)
    validate_series(series_value)
    ensure_no_digest_fields(series_value)
    head = series_value.get("head")
    if (
        series_value["pending_operation"] is not None
        or candidate["series_generation"] != series_value["generation"]
        or not isinstance(head, dict)
        or head.get("ordinal") != candidate["ordinal"]
    ):
        raise ArchiveError("candidate and release series are not at one committed generation")
    records.append(("RELEASE_SERIES.json", series_path))
    return _write_bundle(candidate, bundle_kind="build-control", output=output, records=records)


def _safe_member(member: tarfile.TarInfo) -> PurePosixPath:
    path = PurePosixPath(member.name)
    if path.is_absolute() or ".." in path.parts or member.issym() or member.islnk():
        raise ArchiveError(f"unsafe candidate transfer member: {member.name}")
    return path


def import_bundle(bundle: Path, output: Path, *, include_manifest: bool = True) -> Path:
    bundle = resolve_existing_file(bundle, "candidate transfer bundle")
    output = output.resolve()
    if output.exists():
        raise ArchiveError(f"candidate transfer import already exists: {output}")
    with tarfile.open(bundle, "r") as archive:
        members = archive.getmembers()
        names = [member.name for member in members]
        if len(names) != len(set(names)):
            raise ArchiveError("candidate transfer archive has duplicate members")
        for member in members:
            _safe_member(member)
        manifest_member = archive.getmember("CANDIDATE_TRANSFER.json")
        manifest_file = archive.extractfile(manifest_member)
        if manifest_file is None:
            raise ArchiveError("candidate transfer manifest is unreadable")
        manifest = json.loads(manifest_file.read())
        expected = {
            entry["path"]: entry["size"]
            for entry in manifest.get("files", [])
            if isinstance(entry, dict)
        }
        if len(expected) != len(manifest.get("files", [])):
            raise ArchiveError("candidate transfer manifest has duplicate or invalid paths")
        expected_members = {"CANDIDATE_TRANSFER.json", *(f"payload/{path}" for path in expected)}
        if set(names) != expected_members:
            raise ArchiveError("candidate transfer members do not match the closed manifest")
        output.mkdir(parents=True)
        for relative, size in expected.items():
            require_relative_path(relative, "candidate transfer path")
            member = archive.getmember(f"payload/{relative}")
            source = archive.extractfile(member)
            if source is None:
                raise ArchiveError(f"candidate transfer file is unreadable: {relative}")
            content = source.read()
            if len(content) != size:
                raise ArchiveError(f"candidate transfer size mismatch: {relative}")
            destination = output.joinpath(*PurePosixPath(relative).parts)
            resolve_within(output, destination, "candidate transfer destination")
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(content)
        if include_manifest:
            (output / "CANDIDATE_TRANSFER.json").write_text(
                json.dumps(manifest, indent=2) + "\n", encoding="utf-8", newline="\n"
            )
    return output


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    source = subcommands.add_parser("export-build-source")
    source.add_argument("--candidate-manifest", type=Path, required=True)
    source.add_argument("--source-root", type=Path, default=ROOT)
    source.add_argument("--output", type=Path, required=True)
    common = subcommands.add_parser("export-common-inputs")
    common.add_argument("--candidate-manifest", type=Path, required=True)
    common.add_argument("--input-root", type=Path, required=True)
    common.add_argument("--output", type=Path, required=True)
    control = subcommands.add_parser("export-build-control")
    control.add_argument("--candidate-manifest", type=Path, required=True)
    control.add_argument("--output", type=Path, required=True)
    artifacts = subcommands.add_parser("export-artifacts")
    artifacts.add_argument("--candidate-manifest", type=Path, required=True)
    artifacts.add_argument("--output", type=Path, required=True)
    artifacts.add_argument("--repository-source-root", type=Path, default=ROOT)
    target = subcommands.add_parser("export-target")
    target.add_argument("--candidate-manifest", type=Path, required=True)
    target.add_argument("--target", required=True)
    target.add_argument("--output", type=Path, required=True)
    imported = subcommands.add_parser("import")
    imported.add_argument("--bundle", type=Path, required=True)
    imported.add_argument("--output", type=Path, required=True)
    imported.add_argument("--payload-only", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.command == "export-build-source":
            output = export_build_source(args.candidate_manifest, args.output, args.source_root)
        elif args.command == "export-common-inputs":
            output = export_common_inputs(args.candidate_manifest, args.input_root, args.output)
        elif args.command == "export-build-control":
            output = export_build_control(args.candidate_manifest, args.output)
        elif args.command == "import":
            output = import_bundle(args.bundle, args.output, include_manifest=not args.payload_only)
        else:
            manifest, candidate, root = load_candidate(args.candidate_manifest)
            if args.command == "export-target":
                from release_archive_common import sealed_partial_path, resolve_candidate_path

                partial_path = sealed_partial_path(root, args.target)
                partial = read_json(resolve_existing_file(partial_path, "sealed target partial"))
                files = [partial_path]
                for artifact in partial.get("artifacts", []):
                    artifact_path = resolve_candidate_path(root, artifact.get("path"), "target artifact")
                    if artifact_path.is_dir():
                        files.extend(path for path in artifact_path.rglob("*") if path.is_file())
                    else:
                        files.append(artifact_path)
                for event in partial.get("events", []):
                    for key in ("started", "completed"):
                        files.append(resolve_candidate_path(root, event.get(key), f"target event {key}"))
                for context in partial.get("execution_contexts", []):
                    files.append(resolve_candidate_path(root, context.get("path"), "target context"))
            else:
                allowed_roots = [
                    "archives",
                    "common-input-source",
                    "crate-packages",
                    "evidence",
                    "helm",
                    "legal",
                    "manifests",
                    "oci-layout",
                    "provenance",
                    "sbom",
                ]
                records = [
                    (path.relative_to(root).as_posix(), path)
                    for name in allowed_roots
                    for path in (root / name).rglob("*")
                    if path.is_file()
                ]
                artifact_index = resolve_existing_file(root / "ARTIFACT_INDEX.json", "candidate artifact index")
                records.append(("ARTIFACT_INDEX.json", artifact_index))
                for name in ("EVIDENCE_INDEX.json", "NO_REMOTE_PUBLICATION.json"):
                    records.append((name, _candidate_evidence_file(root, candidate, name)))
                repository_root = args.repository_source_root.resolve(strict=True)
                for relative in (
                    "LICENSE-APACHE",
                    "NOTICE",
                    "rocketmq-doc/en/release/1.0/upgrade-and-rollback.md",
                    "rocketmq-doc/en/release/1.0/publication-handoff.md",
                ):
                    path = resolve_existing_file(repository_root / relative, "publication handoff documentation")
                    records.append((f"repository-source/{relative}", path))
                output_path = resolve_within(root, args.output, "candidate transfer output")
                if output_path.exists():
                    raise ArchiveError(f"candidate transfer bundle already exists: {output_path}")
                output = _write_bundle(
                    read_json(manifest),
                    bundle_kind="artifacts",
                    output=output_path,
                    records=records,
                )
                print(f"CANDIDATE_TRANSFER_OK command={args.command} output={output}")
                return 0
            output = export_bundle(
                manifest,
                bundle_kind="target" if args.command == "export-target" else "artifacts",
                output=args.output,
                source_root=root,
                files=files,
            )
        print(f"CANDIDATE_TRANSFER_OK command={args.command} output={output}")
        return 0
    except (ReleaseStateError, OSError, KeyError, json.JSONDecodeError) as error:
        print(f"CANDIDATE_TRANSFER_FAILED detail={error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
