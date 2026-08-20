#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

"""Semantically verify a local publication handoff without content digests."""

from __future__ import annotations

import argparse
import io
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import sys
import tarfile
import tempfile
from typing import Any, BinaryIO
import zipfile


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "distribution") not in sys.path:
    sys.path.insert(0, str(ROOT / "distribution"))

from release_archive_common import ArchiveError, load_layout
from release_state import ReleaseStateError, atomic_write_json, ensure_no_digest_fields, read_json, validate_candidate
from verify_release_archive import inspect_archive


COPY_BUFFER_SIZE = 1024 * 1024
MAX_NESTED_DEPTH = 4
SECRET_PATTERNS = (
    re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    re.compile(rb"github_pat_[A-Za-z0-9_]{20,}"),
    re.compile(rb"ghp_[A-Za-z0-9]{20,}"),
    re.compile(rb"AKIA[0-9A-Z]{16}"),
)
REMOTE_COMMAND = re.compile(
    r"(?:cargo\s+publish|docker\s+(?:login|push)|(?:oras|helm)\s+push|gh\s+release\s+(?:create|upload)|git\s+push)",
    re.IGNORECASE,
)
SCRIPT_SUFFIXES = {".sh", ".ps1", ".bat", ".cmd"}
REQUIRED_TOP_LEVEL = {
    "PUBLICATION_HANDOFF.json",
    "REMOTE_PUBLICATION_NOT_EXECUTED.md",
    "crate-packages",
    "archives",
    "oci-layout",
    "helm",
    "manifests",
    "legal",
    "sbom",
    "provenance",
    "evidence",
    "docs",
}
PLATFORM_TARGETS = {
    "linux": ("H01-LINUX", "x86_64-unknown-linux-gnu"),
    "windows": ("H01-WINDOWS", "x86_64-pc-windows-msvc"),
    "macos": ("H01-MACOS", "x86_64-apple-darwin"),
}


class HandoffVerifyError(ReleaseStateError):
    """Raised when handoff contents do not match their retained trusted sources."""


def _safe_relative(value: str, label: str) -> PurePosixPath:
    if not isinstance(value, str) or not value or "\\" in value:
        raise HandoffVerifyError(f"{label} is not a safe relative path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise HandoffVerifyError(f"{label} is not a safe relative path")
    return path


def _identity(value: dict[str, Any]) -> tuple[Any, ...]:
    return value.get("candidate_id"), value.get("version"), value.get("run_id"), value.get("attempt")


def _stream_equal(first: Path, second: Path) -> bool:
    with first.open("rb") as left, second.open("rb") as right:
        while True:
            left_chunk = left.read(COPY_BUFFER_SIZE)
            right_chunk = right.read(COPY_BUFFER_SIZE)
            if left_chunk != right_chunk:
                return False
            if not left_chunk:
                return True


def _inventory(root: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise HandoffVerifyError(f"handoff contains a symbolic link: {path}")
        if path.is_file() and path.name not in {"PUBLICATION_HANDOFF.json", "PUBLICATION_READY.json"}:
            records.append({"path": path.relative_to(root).as_posix(), "type": "file", "size": path.stat().st_size})
        elif not path.is_file() and not path.is_dir():
            raise HandoffVerifyError(f"handoff contains an unsupported file type: {path}")
    return records


def _closed_file_inventory(root: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise HandoffVerifyError(f"handoff contains a symbolic link: {path}")
        if path.is_file():
            records.append(
                {
                    "path": path.relative_to(root).as_posix(),
                    "type": "file",
                    "size": path.stat().st_size,
                }
            )
        elif not path.is_dir():
            raise HandoffVerifyError(f"handoff contains an unsupported file type: {path}")
    return records


def _copy_read_only_snapshot(root: Path, snapshot: Path) -> list[dict[str, Any]]:
    before = _closed_file_inventory(root)
    for record in before:
        relative = PurePosixPath(record["path"])
        source = root.joinpath(*relative.parts)
        destination = snapshot.joinpath(*relative.parts)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
    if _closed_file_inventory(root) != before:
        raise HandoffVerifyError("final handoff changed while creating the read-only snapshot")
    for record in before:
        relative = PurePosixPath(record["path"])
        if not _stream_equal(
            root.joinpath(*relative.parts),
            snapshot.joinpath(*relative.parts),
        ):
            raise HandoffVerifyError(f"final handoff changed during snapshot: {relative}")
    return before


def _verify_read_only_snapshot(
    root: Path,
    snapshot: Path,
    expected: list[dict[str, Any]],
) -> None:
    if _closed_file_inventory(root) != expected:
        raise HandoffVerifyError("final handoff inventory changed during verification")
    for record in expected:
        relative = PurePosixPath(record["path"])
        if not _stream_equal(
            root.joinpath(*relative.parts),
            snapshot.joinpath(*relative.parts),
        ):
            raise HandoffVerifyError(f"final handoff content changed during verification: {relative}")


def _scan_bytes(payload: bytes, label: str) -> None:
    if any(pattern.search(payload) for pattern in SECRET_PATTERNS):
        raise HandoffVerifyError(f"secret material detected in {label}")


def _safe_archive_name(name: str, label: str) -> PurePosixPath:
    path = PurePosixPath(name)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts) or "\\" in name:
        raise HandoffVerifyError(f"unsafe nested archive path in {label}: {name}")
    return path


def _scan_nested_payload(name: str, payload: bytes, label: str, depth: int) -> None:
    _scan_bytes(payload, f"{label}:{name}")
    if depth >= MAX_NESTED_DEPTH:
        return
    lower = name.lower()
    stream = io.BytesIO(payload)
    if lower.endswith((".zip",)):
        try:
            with zipfile.ZipFile(stream) as archive:
                names = archive.namelist()
                if len(names) != len(set(names)):
                    raise HandoffVerifyError(f"duplicate nested zip member in {label}")
                for member in names:
                    _safe_archive_name(member, label)
                    info = archive.getinfo(member)
                    if info.is_dir():
                        continue
                    _scan_nested_payload(member, archive.read(member), label, depth + 1)
        except zipfile.BadZipFile as error:
            raise HandoffVerifyError(f"invalid nested zip in {label}: {error}") from error
    elif lower.endswith((".tar", ".tar.gz", ".tgz", ".crate")):
        try:
            with tarfile.open(fileobj=stream, mode="r:*") as archive:
                names: set[str] = set()
                for member in archive.getmembers():
                    _safe_archive_name(member.name, label)
                    if member.name in names or member.issym() or member.islnk():
                        raise HandoffVerifyError(f"unsafe or duplicate nested tar member in {label}: {member.name}")
                    names.add(member.name)
                    if member.isfile():
                        source = archive.extractfile(member)
                        if source is None:
                            raise HandoffVerifyError(f"unreadable nested tar member in {label}: {member.name}")
                        _scan_nested_payload(member.name, source.read(), label, depth + 1)
        except tarfile.TarError as error:
            raise HandoffVerifyError(f"invalid nested tar in {label}: {error}") from error


def _scan_file(path: Path, relative: str) -> None:
    payload = path.read_bytes()
    _scan_nested_payload(relative, payload, relative, 0)
    if path.suffix.lower() in SCRIPT_SUFFIXES:
        try:
            text = payload.decode("utf-8")
        except UnicodeDecodeError as error:
            raise HandoffVerifyError(f"publication handoff script is not UTF-8: {relative}") from error
        if REMOTE_COMMAND.search(text):
            raise HandoffVerifyError(f"remote publication command is embedded in handoff: {relative}")
    if path.suffix.lower() == ".json":
        try:
            value = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise HandoffVerifyError(f"handoff JSON is invalid: {relative}: {error}") from error
        ensure_no_digest_fields(value)
        forbidden_keys = {"command", "args", "shell", "workflow_dispatch_payload", "token", "secret"}

        def visit(item: Any) -> None:
            if isinstance(item, dict):
                for key, child in item.items():
                    if str(key).lower() in forbidden_keys:
                        raise HandoffVerifyError(f"executable or secret-bearing field in handoff JSON: {relative}:{key}")
                    visit(child)
            elif isinstance(item, list):
                for child in item:
                    visit(child)
            elif isinstance(item, str) and REMOTE_COMMAND.search(item):
                raise HandoffVerifyError(f"remote publication payload in handoff JSON: {relative}")

        visit(value)


def _archive_member_names(path: Path) -> list[str]:
    lower = path.name.lower()
    if lower.endswith(".zip"):
        try:
            with zipfile.ZipFile(path) as archive:
                names = archive.namelist()
                for name in names:
                    _safe_archive_name(name, path.name)
                return names
        except zipfile.BadZipFile as error:
            raise HandoffVerifyError(f"invalid handoff zip: {path.name}: {error}") from error
    try:
        with tarfile.open(path, "r:*") as archive:
            names = []
            for member in archive.getmembers():
                _safe_archive_name(member.name, path.name)
                if member.issym() or member.islnk():
                    raise HandoffVerifyError(f"handoff archive contains a link: {path.name}:{member.name}")
                names.append(member.name)
            return names
    except tarfile.TarError as error:
        raise HandoffVerifyError(f"invalid handoff tar: {path.name}: {error}") from error


def _verify_package_semantics(
    root: Path,
    candidate: dict[str, Any],
    *,
    smoke_target: str | None,
) -> dict[str, Any] | None:
    packages = sorted((root / "crate-packages").glob("*.crate"))
    if not packages:
        raise HandoffVerifyError("handoff contains no local crate packages")
    for package in packages:
        names = _archive_member_names(package)
        if not any(name.endswith("/Cargo.toml") for name in names):
            raise HandoffVerifyError(f"crate package has no Cargo.toml: {package.name}")
        if not any(name.endswith("/LICENSE-APACHE") for name in names) or not any(
            name.endswith("/NOTICE") for name in names
        ):
            raise HandoffVerifyError(f"crate package legal metadata is incomplete: {package.name}")
    layout = load_layout()
    manifests = sorted((root / "archives").glob("*.manifest.json"))
    if len(manifests) != len(layout["targets"]):
        raise HandoffVerifyError("handoff archive manifest denominator is incomplete")
    targets: set[str] = set()
    smoke_result: dict[str, Any] | None = None
    for manifest_path in manifests:
        manifest = read_json(manifest_path)
        target = manifest.get("target")
        if target not in layout["targets"] or target in targets:
            raise HandoffVerifyError(f"handoff archive target is invalid or duplicated: {target}")
        targets.add(target)
        archive_relative = _safe_relative(manifest.get("archive"), "handoff archive path")
        archive = root.joinpath(*archive_relative.parts)
        try:
            retained_manifest, retained, results = inspect_archive(
                candidate,
                root,
                archive,
                smoke=target == smoke_target,
            )
        except ArchiveError as error:
            raise HandoffVerifyError(str(error)) from error
        if retained_manifest != manifest_path:
            raise HandoffVerifyError(f"handoff archive manifest selection is ambiguous: {target}")
        if target == smoke_target:
            smoke_result = {
                "archive": archive_relative.as_posix(),
                "archive_id": retained["artifact_id"],
                "manifest": manifest_path.relative_to(root).as_posix(),
                "results": results,
            }
    if targets != set(layout["targets"]):
        raise HandoffVerifyError("handoff archive target denominator changed")
    if smoke_target is not None and smoke_result is None:
        raise HandoffVerifyError(f"handoff has no archive for smoke target: {smoke_target}")
    charts = list((root / "helm").glob("*.tgz"))
    if len(charts) != 1 or not any(name.endswith("/Chart.yaml") for name in _archive_member_names(charts[0])):
        raise HandoffVerifyError("handoff Helm package is missing or invalid")
    for service in ("namesrv", "broker", "controller", "proxy"):
        layout = root / "oci-layout" / service
        if not (layout / "oci-layout").is_file() or not (layout / "index.json").is_file():
            raise HandoffVerifyError(f"handoff OCI layout is incomplete: {service}")
        json.loads((layout / "oci-layout").read_text(encoding="utf-8"))
        json.loads((layout / "index.json").read_text(encoding="utf-8"))
    for required in (root / "legal" / "LICENSE-APACHE", root / "legal" / "NOTICE"):
        if not required.is_file() or required.stat().st_size == 0:
            raise HandoffVerifyError(f"handoff legal file is missing: {required.name}")
    return smoke_result


def verify_handoff(
    handoff_root: Path,
    candidate_manifest: Path,
    candidate_root: Path,
    source_root: Path,
    *,
    mode: str,
    result_id: str,
    platform: str | None = None,
    worker_id: str | None = None,
) -> dict[str, Any]:
    """Verify identity, byte provenance, archive semantics, secrets, and scope."""

    if mode not in {"draft-pre-ready", "final-pre-ready", "ready"}:
        raise HandoffVerifyError("unsupported handoff verification mode")
    if platform is not None:
        expected_result, _target = PLATFORM_TARGETS.get(platform, (None, None))
        if expected_result != result_id or not isinstance(worker_id, str) or not worker_id:
            raise HandoffVerifyError("platform verification result/worker identity is invalid")
    handoff_root = handoff_root.resolve(strict=True)
    candidate_root = candidate_root.resolve(strict=True)
    source_root = source_root.resolve(strict=True)
    candidate = read_json(candidate_manifest.resolve(strict=True))
    validate_candidate(candidate)
    handoff = read_json(handoff_root / "PUBLICATION_HANDOFF.json")
    ensure_no_digest_fields(handoff)
    if _identity(handoff) != _identity(candidate):
        raise HandoffVerifyError("handoff belongs to another candidate run")
    if (
        handoff.get("schema_version") != 1
        or handoff.get("candidate_state") != "ga-candidate-ready"
        or handoff.get("distribution_identity") != "unofficial-community"
        or handoff.get("release_approver") != "mxsm"
        or handoff.get("official_apache_release") is not False
        or handoff.get("remote_publication", {}).get("status") != "not-executed"
        or handoff.get("future_publication", {}).get("executed") is not False
        or handoff.get("future_publication", {}).get("oci_namespace") != "ghcr.io/mxsm/rocketmq-rust"
    ):
        raise HandoffVerifyError("handoff release identity or no-remote boundary is invalid")
    marker = handoff_root / "PUBLICATION_READY.json"
    if mode in {"draft-pre-ready", "final-pre-ready"} and marker.exists():
        raise HandoffVerifyError("pre-ready handoff must not contain PUBLICATION_READY.json")
    if mode == "ready" and not marker.is_file():
        raise HandoffVerifyError("ready handoff has no PUBLICATION_READY.json")
    if mode == "ready":
        marker_value = read_json(marker)
        snapshot = marker_value.get("candidate_snapshot", {})
        portable_handoff = marker_value.get("handoff_identity", {})
        required_results = set(PLATFORM_TARGETS[platform][0] for platform in PLATFORM_TARGETS) | {
            "H02-DRAFT-SEMANTIC",
            "H03-DRAFT-NO-REMOTE",
            "H04-FINAL-SEMANTIC",
            "H05-FINAL-NO-REMOTE",
        }
        if (
            marker_value.get("state") != "publication-ready"
            or _identity(snapshot) != _identity(candidate)
            or snapshot.get("state") != "publication-ready"
            or snapshot.get("generation") != candidate.get("generation")
            or snapshot.get("series_id") != candidate.get("series_id")
            or snapshot.get("series_generation") != candidate.get("series_generation")
            or _identity(portable_handoff) != _identity(handoff)
            or portable_handoff.get("candidate_generation") != handoff.get("candidate_generation")
            or set(marker_value.get("final_result_ids", [])) != required_results
            or marker_value.get("remote_publication", {}).get("status") != "not-executed"
        ):
            raise HandoffVerifyError("publication-ready envelope does not match candidate and handoff state")
        ensure_no_digest_fields(marker_value)
    top_level = {path.name for path in handoff_root.iterdir()}
    if not REQUIRED_TOP_LEVEL.issubset(top_level):
        raise HandoffVerifyError(f"handoff fixed layout is incomplete: {sorted(REQUIRED_TOP_LEVEL - top_level)}")
    if _inventory(handoff_root) != handoff.get("files"):
        raise HandoffVerifyError("handoff closed file inventory changed")
    roots = {"candidate": candidate_root, "repository": source_root}
    destinations: set[str] = set()
    for binding in handoff.get("source_bindings", []):
        if not isinstance(binding, dict) or binding.get("source_domain") not in roots:
            raise HandoffVerifyError("handoff source binding is invalid")
        source_relative = _safe_relative(binding.get("source"), "handoff binding source")
        destination_relative = _safe_relative(binding.get("destination"), "handoff binding destination")
        if destination_relative.as_posix() in destinations:
            raise HandoffVerifyError("handoff source binding destination is duplicated")
        destinations.add(destination_relative.as_posix())
        source = roots[binding["source_domain"]].joinpath(*source_relative.parts)
        destination = handoff_root.joinpath(*destination_relative.parts)
        if not source.is_file() or not destination.is_file() or source.is_symlink() or destination.is_symlink():
            raise HandoffVerifyError(f"handoff source binding is missing or unsafe: {destination_relative}")
        if source.stat().st_size != binding.get("size") or destination.stat().st_size != binding.get("size"):
            raise HandoffVerifyError(f"handoff source binding size differs: {destination_relative}")
        if not _stream_equal(source, destination):
            raise HandoffVerifyError(f"handoff byte content differs from retained source: {destination_relative}")
    for path in sorted(handoff_root.rglob("*")):
        if path.is_file() and path.name != "PUBLICATION_READY.json":
            _scan_file(path, path.relative_to(handoff_root).as_posix())
    smoke_target = PLATFORM_TARGETS[platform][1] if platform is not None else None
    archive_smoke = _verify_package_semantics(
        handoff_root,
        candidate,
        smoke_target=smoke_target,
    )
    evidence = read_json(handoff_root / "evidence" / "EVIDENCE_INDEX.json")
    no_remote = read_json(handoff_root / "evidence" / "NO_REMOTE_PUBLICATION.json")
    if _identity(evidence) != _identity(candidate) or evidence.get("status") != "passed":
        raise HandoffVerifyError("handoff evidence index is invalid")
    if _identity(no_remote) != _identity(candidate) or no_remote.get("remote_publication", {}).get(
        "status"
    ) != "not-executed":
        raise HandoffVerifyError("handoff no-remote evidence is invalid")
    report = {
        "schema_version": 1,
        "candidate_id": candidate["candidate_id"],
        "version": candidate["version"],
        "run_id": candidate["run_id"],
        "attempt": candidate["attempt"],
        "phase": 6,
        "gate_stage": "final-handoff",
        "result_id": result_id,
        "mode": mode,
        "status": "passed",
        "skipped": False,
        "verified_files": len(handoff["files"]) + 1,
        "source_bindings": len(handoff["source_bindings"]),
        "remote_publication": {"status": "not-executed"},
        "secret_scan": {"status": "passed", "findings": []},
    }
    if platform is not None:
        _expected_result, target = PLATFORM_TARGETS[platform]
        if archive_smoke is None:
            raise HandoffVerifyError(f"handoff archive smoke result is missing: {target}")
        report.update(
            {
                "worker_id": worker_id,
                "platform": platform,
                "target": target,
                "archive_id": archive_smoke["archive_id"],
                "archive": archive_smoke["archive"],
                "archive_manifest": archive_smoke["manifest"],
                "archive_smoke_results": archive_smoke["results"],
                "assertions": [
                    {"name": "source-stream-compare", "status": "passed"},
                    {"name": "archive-install-smoke", "status": "passed"},
                    {"name": "secret-scan", "status": "passed"},
                ],
            }
        )
    ensure_no_digest_fields(report)
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--handoff", type=Path, required=True)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--candidate-root", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--draft-pre-ready", action="store_true")
    modes.add_argument("--final-pre-ready", action="store_true")
    modes.add_argument("--ready", action="store_true")
    parser.add_argument("--final-read-only", action="store_true")
    parser.add_argument("--result-id", required=True)
    parser.add_argument("--platform", choices=tuple(PLATFORM_TARGETS))
    parser.add_argument("--worker-id")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    mode = "draft-pre-ready" if args.draft_pre_ready else "final-pre-ready" if args.final_pre_ready else "ready"
    try:
        if args.final_pre_ready != args.final_read_only:
            raise HandoffVerifyError("--final-pre-ready requires --final-read-only and vice versa")
        if args.final_read_only:
            handoff_root = args.handoff.resolve(strict=True)
            try:
                args.output.resolve().relative_to(handoff_root)
            except ValueError:
                pass
            else:
                raise HandoffVerifyError("final-read-only evidence output must be outside the handoff")
        if args.output.exists():
            raise HandoffVerifyError("handoff verification output already exists")
        if args.final_read_only:
            with tempfile.TemporaryDirectory(
                prefix=".handoff-read-only-",
                dir=handoff_root.parent,
            ) as temporary:
                snapshot = Path(temporary)
                inventory = _copy_read_only_snapshot(handoff_root, snapshot)
                report = verify_handoff(
                    handoff_root,
                    args.candidate_manifest,
                    args.candidate_root,
                    args.source_root,
                    mode=mode,
                    result_id=args.result_id,
                    platform=args.platform,
                    worker_id=args.worker_id,
                )
                _verify_read_only_snapshot(handoff_root, snapshot, inventory)
                report["read_only_verified"] = True
        else:
            report = verify_handoff(
                args.handoff,
                args.candidate_manifest,
                args.candidate_root,
                args.source_root,
                mode=mode,
                result_id=args.result_id,
                platform=args.platform,
                worker_id=args.worker_id,
            )
        atomic_write_json(args.output, report)
    except (HandoffVerifyError, OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        print(f"PUBLICATION_HANDOFF_VERIFY_FAILED detail={error}", file=sys.stderr)
        return 1
    print(f"PUBLICATION_HANDOFF_VERIFY_OK result={args.result_id} files={report['verified_files']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
