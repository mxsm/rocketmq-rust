#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import argparse
import json
from pathlib import Path, PurePosixPath
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import zipfile


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "distribution") not in sys.path:
    sys.path.insert(0, str(ROOT / "distribution"))

from release_archive_common import (
    ArchiveError,
    add_unique_record,
    artifact_id,
    candidate_relative,
    compare_inventory,
    draft_partial_path,
    load_candidate,
    load_layout,
    read_policy_json,
    resolve_candidate_path,
    save_draft,
    target_layout,
    write_json,
)
from release_state import read_json, resolve_existing_file


def _safe_name(value: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or "\\" in value:
        raise ArchiveError(f"archive contains an unsafe path: {value}")
    return path


def _extract(archive: Path, output: Path) -> None:
    if archive.suffix == ".zip":
        with zipfile.ZipFile(archive) as value:
            for info in value.infolist():
                _safe_name(info.filename)
            value.extractall(output)
    else:
        with tarfile.open(archive, "r:gz") as value:
            for member in value.getmembers():
                _safe_name(member.name)
                if member.issym() or member.islnk():
                    raise ArchiveError(f"archive contains a link: {member.name}")
            value.extractall(output, filter="data")


def _manifest_for_archive(root: Path, archive: Path) -> tuple[Path, dict]:
    relative = candidate_relative(root, archive, "release archive")
    matches = []
    for path in (root / "archives").glob("*.manifest.json"):
        value = read_json(path)
        if value.get("archive") == relative:
            matches.append((path, value))
    if len(matches) != 1:
        raise ArchiveError(f"expected one manifest for archive, found {len(matches)}")
    return matches[0]


def _validate_manifest(candidate: dict, layout: dict, value: dict) -> None:
    expected_identity = (
        candidate["candidate_id"],
        candidate["version"],
        candidate["run_id"],
        candidate["attempt"],
    )
    actual_identity = (
        value.get("candidate_id"),
        value.get("version"),
        value.get("run_id"),
        value.get("attempt"),
    )
    if actual_identity != expected_identity or value.get("remote_publication") != "not-executed":
        raise ArchiveError("archive manifest does not match the candidate identity")
    target_layout(layout, value.get("target"))
    target = value["target"]
    expected_archive_id = artifact_id(candidate, target, "archive")
    if value.get("artifact_id") != expected_archive_id:
        raise ArchiveError(f"archive artifact identity changed: {target}")
    extension = ".zip" if target_layout(layout, target)["archive_format"] == "zip" else ".tar.gz"
    expected_name = f"rocketmq-rust-{candidate['version']}-{target}{extension}"
    if PurePosixPath(value.get("archive", "")).name != expected_name:
        raise ArchiveError(f"archive filename changed: {target}")
    if len(value.get("binaries", [])) != 6:
        raise ArchiveError("archive manifest does not contain six binary records")
    records = {entry.get("component"): entry for entry in value["binaries"]}
    if len(records) != 6:
        raise ArchiveError("archive binary components are duplicated")
    for binary in layout["binaries"]:
        record = records.get(binary["id"])
        if record is None:
            raise ArchiveError(f"archive manifest has no binary record: {binary['id']}")
        if record.get("requested_features") != binary["requested_features"]:
            raise ArchiveError(f"archive requested features changed: {binary['id']}")
        if record.get("effective_features") != binary["effective_features"]:
            raise ArchiveError(f"archive effective features changed: {binary['id']}")
        required_dependencies = set(binary.get("required_dependencies", []))
        if not required_dependencies.issubset(set(record.get("required_dependencies", []))):
            raise ArchiveError(f"archive dependency closure is incomplete: {binary['id']}")


def verify_archive(candidate_manifest: Path, archive: Path, *, smoke: bool) -> Path | None:
    _manifest, candidate, root = load_candidate(candidate_manifest)
    layout = load_layout()
    archive = resolve_existing_file(archive, "release archive")
    manifest_path, archive_manifest = _manifest_for_archive(root, archive)
    _validate_manifest(candidate, layout, archive_manifest)
    with tempfile.TemporaryDirectory() as temporary:
        extracted = Path(temporary)
        _extract(archive, extracted)
        children = [path for path in extracted.iterdir() if path.is_dir()]
        if len(children) != 1:
            raise ArchiveError("release archive must have exactly one root directory")
        package_root = children[0]
        compare_inventory(package_root, archive_manifest["files"])
        for service in layout["configs"]:
            config = package_root / "conf" / f"{service}.toml"
            with config.open("rb") as handle:
                tomllib.load(handle)
        if not smoke:
            return None
        suffix = target_layout(layout, archive_manifest["target"])["executable_suffix"]
        results = []
        for binary in layout["binaries"]:
            name = binary.get("archive_binary", binary["binary"])
            executable = package_root / "bin" / f"{name}{suffix}"
            completed = subprocess.run(
                [str(executable), "--version", "--verbose"],
                cwd=package_root,
                capture_output=True,
                text=True,
                check=False,
            )
            if completed.returncode != 0:
                raise ArchiveError(
                    f"archive binary version smoke failed: {binary['id']}: {completed.stderr}"
                )
            expected = {
                f"version={candidate['version']}",
                f"artifact_id={artifact_id(candidate, archive_manifest['target'], binary['id'])}",
                f"requested_features={','.join(binary['requested_features'])}",
                f"effective_features={','.join(binary['effective_features'])}",
            }
            missing = sorted(value for value in expected if value not in completed.stdout)
            if missing:
                raise ArchiveError(
                    f"archive binary version metadata mismatch for {binary['id']}: {missing}"
                )
            results.append(
                {"component": binary["id"], "exit_code": 0, "stdout": completed.stdout}
            )
    output = root / "evidence" / archive_manifest["target"] / "HOST_SMOKE.json"
    write_json(
        output,
        {
            "schema_version": 1,
            "candidate_id": candidate["candidate_id"],
            "target": archive_manifest["target"],
            "archive_manifest": candidate_relative(root, manifest_path, "archive manifest"),
            "results": results,
            "status": "passed",
        },
    )
    partial = read_policy_json(
        draft_partial_path(root, archive_manifest["target"]), "candidate partial draft"
    )
    add_unique_record(
        partial,
        "artifacts",
        {
            "id": "host-smoke",
            "kind": "host-smoke",
            "path": candidate_relative(root, output, "host smoke evidence"),
        },
    )
    save_draft(root, archive_manifest["target"], partial)
    return output


def verify_all_manifests(candidate_manifest: Path, manifest_root: Path) -> int:
    _manifest, candidate, _root = load_candidate(candidate_manifest, writable=False)
    layout = load_layout()
    manifests = list(manifest_root.resolve().rglob("*.manifest.json"))
    targets: set[str] = set()
    for path in manifests:
        value = read_json(path)
        _validate_manifest(candidate, layout, value)
        archive = path.parent / PurePosixPath(value["archive"]).name
        if not archive.is_file():
            raise ArchiveError(f"archive referenced by manifest is missing: {archive}")
        target = value["target"]
        if target in targets:
            raise ArchiveError(f"archive manifest target is duplicated: {target}")
        targets.add(target)
    expected = set(layout["targets"])
    if targets != expected:
        raise ArchiveError(
            f"archive manifest target mismatch: missing={sorted(expected - targets)} "
            f"extra={sorted(targets - expected)}"
        )
    return len(manifests)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--archive", type=Path)
    mode.add_argument("--all-manifests", type=Path)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--static-only", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.archive is not None:
            if not args.smoke or args.static_only:
                raise ArchiveError("--archive requires --smoke and forbids --static-only")
            output = verify_archive(args.candidate_manifest, args.archive, smoke=True)
            print(f"RELEASE_ARCHIVE_SMOKE_OK output={output}")
        else:
            if not args.static_only or args.smoke:
                raise ArchiveError("--all-manifests requires --static-only and forbids --smoke")
            count = verify_all_manifests(args.candidate_manifest, args.all_manifests)
            print(f"RELEASE_ARCHIVE_STATIC_OK manifests={count}")
        return 0
    except (ArchiveError, OSError, KeyError, json.JSONDecodeError, tomllib.TOMLDecodeError) as error:
        print(f"RELEASE_ARCHIVE_VERIFY_FAILED detail={error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
