#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import argparse
import io
import json
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys
import tarfile
import tempfile
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "distribution") not in sys.path:
    sys.path.insert(0, str(ROOT / "distribution"))

from release_archive_common import ArchiveError, load_candidate, require_relative_path
from release_series import export_control_bundle, import_control_bundle
from release_state import (
    atomic_write_json,
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
        "series_generation": candidate["series_generation"],
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
    with tempfile.TemporaryDirectory() as temporary:
        portable_series = Path(temporary) / "RELEASE_SERIES_CONTROL_BUNDLE.tar"
        export_control_bundle(series_path, portable_series)
        return _write_bundle(
            candidate,
            bundle_kind="build-control",
            output=output,
            records=[("RELEASE_SERIES_CONTROL_BUNDLE.tar", portable_series)],
        )


def import_build_control(
    bundle: Path, output: Path, *, build_source_bundle: Path | None = None
) -> Path:
    """Import one committed candidate generation into a worker-local root."""

    bundle = resolve_existing_file(bundle, "build control bundle")
    output = output.resolve()
    if output.exists():
        raise ArchiveError(f"build control import already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output.parent) as temporary:
        payload_root = import_bundle(bundle, Path(temporary) / "payload")
        transfer = read_json(payload_root / "CANDIDATE_TRANSFER.json")
        if transfer.get("bundle_kind") != "build-control":
            raise ArchiveError("candidate transfer bundle is not build-control state")
        records = transfer.get("files")
        if records != [
            {
                "path": "RELEASE_SERIES_CONTROL_BUNDLE.tar",
                "type": "file",
                "size": (payload_root / "RELEASE_SERIES_CONTROL_BUNDLE.tar").stat().st_size,
            }
        ]:
            raise ArchiveError("build control bundle has an invalid payload denominator")
        generation = transfer.get("series_generation")
        if not isinstance(generation, int) or isinstance(generation, bool) or generation < 0:
            raise ArchiveError("build control bundle has an invalid series generation")
        series_manifest = import_control_bundle(
            payload_root / "RELEASE_SERIES_CONTROL_BUNDLE.tar",
            output,
            expected_generation=generation,
        )
    series = read_json(series_manifest)
    head = series.get("head")
    if not isinstance(head, dict):
        raise ArchiveError("build control release series has no current candidate")
    candidate_manifest = resolve_existing_file(
        Path(head.get("candidate_manifest", "")), "build control candidate manifest"
    )
    candidate = read_json(candidate_manifest)
    validate_candidate(candidate)
    identity = (
        candidate.get("candidate_id"),
        candidate.get("version"),
        candidate.get("run_id"),
        candidate.get("attempt"),
        candidate.get("series_generation"),
    )
    expected = (
        transfer.get("candidate_id"),
        transfer.get("version"),
        transfer.get("run_id"),
        transfer.get("attempt"),
        generation,
    )
    if identity != expected:
        raise ArchiveError("imported build control candidate identity does not match")
    if (
        Path(candidate["candidate_root"]).resolve() != candidate_manifest.parent.resolve()
        or Path(candidate["series_manifest"]).resolve() != series_manifest.resolve()
        or candidate["ordinal"] != head.get("ordinal")
    ):
        raise ArchiveError("imported build control ownership is inconsistent")
    if build_source_bundle is not None:
        source = resolve_existing_file(build_source_bundle, "build source bundle")
        if candidate.get("build_source_bundle") is None:
            raise ArchiveError("build control candidate has no canonical source bundle")
        candidate["build_source_bundle"] = str(source)
        atomic_write_json(candidate_manifest, candidate)
    return candidate_manifest


def import_artifacts(bundle: Path, candidate_manifest: Path) -> Path:
    """Restore one artifact bundle into its relocated candidate root."""

    candidate_manifest = resolve_existing_file(candidate_manifest, "candidate manifest")
    candidate = read_json(candidate_manifest)
    validate_candidate(candidate)
    candidate_root = Path(candidate["candidate_root"]).resolve()
    if candidate_root != candidate_manifest.parent.resolve():
        raise ArchiveError("candidate manifest and candidate root disagree")
    bundle = resolve_existing_file(bundle, "candidate artifact bundle")
    with tempfile.TemporaryDirectory(dir=candidate_root.parent) as temporary:
        payload_root = import_bundle(bundle, Path(temporary) / "payload")
        transfer = read_json(payload_root / "CANDIDATE_TRANSFER.json")
        identity = (
            transfer.get("candidate_id"),
            transfer.get("version"),
            transfer.get("run_id"),
            transfer.get("attempt"),
            transfer.get("series_generation"),
        )
        expected = (
            candidate.get("candidate_id"),
            candidate.get("version"),
            candidate.get("run_id"),
            candidate.get("attempt"),
            candidate.get("series_generation"),
        )
        if transfer.get("bundle_kind") != "artifacts" or identity != expected:
            raise ArchiveError("candidate artifact bundle identity does not match")
        records = transfer.get("files")
        if not isinstance(records, list) or not records:
            raise ArchiveError("candidate artifact bundle has no closed file denominator")
        copies: list[tuple[Path, Path]] = []
        for record in records:
            if not isinstance(record, dict) or set(record) != {"path", "type", "size"}:
                raise ArchiveError("candidate artifact bundle contains an invalid file record")
            relative = require_relative_path(record.get("path"), "candidate artifact path")
            source = resolve_existing_file(payload_root / relative, "candidate artifact payload")
            if record.get("type") != "file" or record.get("size") != source.stat().st_size:
                raise ArchiveError(f"candidate artifact record is inconsistent: {relative}")
            destination = resolve_within(
                candidate_root, candidate_root / relative, "candidate artifact destination"
            )
            if destination.exists():
                if not destination.is_file() or destination.read_bytes() != source.read_bytes():
                    raise ArchiveError(f"candidate artifact destination collides: {relative}")
                continue
            copies.append((source, destination))
        for source, destination in copies:
            destination.parent.mkdir(parents=True, exist_ok=True)
            temporary_destination = destination.with_name(destination.name + ".candidate-import.tmp")
            if temporary_destination.exists():
                raise ArchiveError(f"candidate artifact temporary destination exists: {destination}")
            shutil.copyfile(source, temporary_destination)
            temporary_destination.replace(destination)
    return candidate_manifest


def write_build_control_selector(candidate_manifest: Path, output: Path) -> Path:
    """Write the worker-local candidate selector produced by a control import."""

    candidate_manifest = resolve_existing_file(candidate_manifest, "candidate manifest")
    candidate = read_json(candidate_manifest)
    validate_candidate(candidate)
    output = output.resolve()
    atomic_write_json(
        output,
        {
            "schema_version": 1,
            "candidate_id": candidate["candidate_id"],
            "version": candidate["version"],
            "run_id": candidate["run_id"],
            "attempt": candidate["attempt"],
            "series_generation": candidate["series_generation"],
            "candidate_manifest": str(candidate_manifest),
            "candidate_root": str(candidate_manifest.parent),
        },
    )
    return output


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
    control_import = subcommands.add_parser("import-build-control")
    control_import.add_argument("--bundle", type=Path, required=True)
    control_import.add_argument("--output", type=Path, required=True)
    control_import.add_argument("--selector-output", type=Path, required=True)
    control_import.add_argument("--build-source-bundle", type=Path)
    artifact_import = subcommands.add_parser("import-artifacts")
    artifact_import.add_argument("--bundle", type=Path, required=True)
    artifact_import.add_argument("--candidate-manifest", type=Path, required=True)
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
        elif args.command == "import-build-control":
            candidate_manifest = import_build_control(
                args.bundle,
                args.output,
                build_source_bundle=args.build_source_bundle,
            )
            output = write_build_control_selector(candidate_manifest, args.selector_output)
        elif args.command == "import-artifacts":
            output = import_artifacts(args.bundle, args.candidate_manifest)
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
