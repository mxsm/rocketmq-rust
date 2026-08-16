#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Create an immutable semantic source snapshot from the registered transfer bundle."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import sys
import tarfile
from typing import Any


DISTRIBUTION = Path(__file__).resolve().parent
if str(DISTRIBUTION) not in sys.path:
    sys.path.insert(0, str(DISTRIBUTION))

from release_state import (
    ReleaseStateError,
    atomic_write_json,
    ensure_no_digest_fields,
    exclusive_lock,
    read_json,
    resolve_existing_file,
    series_lock_path,
    utc_now,
    validate_candidate,
)


class SnapshotError(ReleaseStateError):
    """Raised when a source transfer bundle cannot become a trusted snapshot."""


def _safe_source_path(raw: str) -> PurePosixPath:
    path = PurePosixPath(raw)
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise SnapshotError(f"unsafe source path: {raw}")
    return path


COPY_BUFFER_SIZE = 1024 * 1024


def _read_bundle_manifest(
    bundle: Path, candidate: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, str]]:
    try:
        with tarfile.open(bundle, "r") as archive:
            members = archive.getmembers()
            if any(not member.isfile() for member in members):
                raise SnapshotError("source transfer bundle may contain regular files only")
            by_name = {member.name: member for member in members}
            if len(by_name) != len(members) or "CORE_SOURCE_MANIFEST.json" not in by_name:
                raise SnapshotError("source transfer bundle has duplicate members or no manifest")
            source = archive.extractfile(by_name["CORE_SOURCE_MANIFEST.json"])
            if source is None:
                raise SnapshotError("cannot read source transfer manifest")
            metadata = json.loads(source.read())
            ensure_no_digest_fields(metadata)
            member_names: dict[str, str] = {}
            for raw, member in by_name.items():
                if raw == "CORE_SOURCE_MANIFEST.json":
                    continue
                path = _safe_source_path(raw)
                if path.parts[0] != "source" or len(path.parts) < 2:
                    raise SnapshotError(f"unexpected source transfer member: {raw}")
                relative = PurePosixPath(*path.parts[1:]).as_posix()
                member_names[relative] = member.name
    except (OSError, tarfile.TarError, json.JSONDecodeError, UnicodeDecodeError) as error:
        raise SnapshotError(f"invalid source transfer bundle: {error}") from error
    identity = (metadata.get("version"), metadata.get("run_id"), metadata.get("attempt"))
    expected_identity = (candidate["version"], candidate["run_id"], candidate["attempt"])
    if identity != expected_identity:
        raise SnapshotError(f"source bundle identity mismatch: {identity} != {expected_identity}")
    expected: dict[str, int] = {}
    for item in metadata.get("files", []):
        if not isinstance(item, dict) or item.get("type") != "file":
            raise SnapshotError("source manifest may describe regular files only")
        path = _safe_source_path(item.get("path", "")).as_posix()
        if path in expected or not isinstance(item.get("size"), int) or item["size"] < 0:
            raise SnapshotError(f"invalid source manifest entry: {item!r}")
        expected[path] = item["size"]
    if set(expected) != set(member_names):
        raise SnapshotError(
            f"source member denominator drift: missing={sorted(set(expected) - set(member_names))} "
            f"extra={sorted(set(member_names) - set(expected))}"
        )
    with tarfile.open(bundle, "r") as archive:
        for path, member_name in member_names.items():
            size = archive.getmember(member_name).size
            if size != expected[path]:
                raise SnapshotError(f"source size drift for {path}: {size} != {expected[path]}")
    return metadata, member_names


def _copy_stream(source, output: Path) -> None:
    with output.open("xb") as destination:
        while chunk := source.read(COPY_BUFFER_SIZE):
            destination.write(chunk)


def verify_snapshot_content(
    bundle: Path,
    candidate: dict[str, Any],
    source_root: Path,
    expected: dict[str, int],
) -> None:
    """Compare a snapshot byte-for-byte with its canonical source bundle without digests."""

    metadata, member_names = _read_bundle_manifest(bundle, candidate)
    manifest_expected = {item["path"]: item["size"] for item in metadata["files"]}
    if manifest_expected != expected:
        raise SnapshotError("snapshot manifest differs from the source transfer manifest")
    try:
        with tarfile.open(bundle, "r") as archive:
            for relative, member_name in member_names.items():
                stream = archive.extractfile(member_name)
                if stream is None:
                    raise SnapshotError(f"cannot read source transfer member: {member_name}")
                with (source_root / PurePosixPath(relative)).open("rb") as snapshot:
                    while True:
                        source_chunk = stream.read(COPY_BUFFER_SIZE)
                        snapshot_chunk = snapshot.read(COPY_BUFFER_SIZE)
                        if source_chunk != snapshot_chunk:
                            raise SnapshotError(f"snapshot content differs for {relative}")
                        if not source_chunk:
                            break
    except (OSError, tarfile.TarError) as error:
        raise SnapshotError(f"cannot verify source snapshot content: {error}") from error


def _readonly(path: Path) -> None:
    path.chmod(path.stat().st_mode & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH))


def create_snapshot(candidate_manifest: Path) -> Path:
    candidate_manifest = resolve_existing_file(candidate_manifest, "candidate_manifest")
    candidate = read_json(candidate_manifest)
    validate_candidate(candidate)
    if candidate["sealed"]:
        raise SnapshotError("cannot add a source snapshot to a sealed candidate")
    raw_bundle = candidate.get("build_source_bundle")
    if not isinstance(raw_bundle, str):
        raise SnapshotError("candidate has no registered build source bundle")
    bundle = resolve_existing_file(Path(raw_bundle), "build_source_bundle")
    metadata, member_names = _read_bundle_manifest(bundle, candidate)
    candidate_root = Path(candidate["candidate_root"]).resolve()
    if candidate_manifest.parent.resolve() != candidate_root:
        raise SnapshotError("candidate root and manifest location disagree")
    destination = candidate_root / "source-snapshot"
    staging = candidate_root / ".source-snapshot.staging"
    if destination.exists() or staging.exists():
        raise SnapshotError("source snapshot already exists or has an unresolved staging directory")
    try:
        source_root = staging / "source"
        with tarfile.open(bundle, "r") as archive:
            for relative, member_name in member_names.items():
                output = source_root.joinpath(*PurePosixPath(relative).parts)
                output.parent.mkdir(parents=True, exist_ok=True)
                stream = archive.extractfile(member_name)
                if stream is None:
                    raise SnapshotError(f"cannot read source transfer member: {member_name}")
                _copy_stream(stream, output)
        snapshot = {
            "schema_version": 1,
            "candidate_id": candidate["candidate_id"],
            "version": candidate["version"],
            "run_id": candidate["run_id"],
            "attempt": candidate["attempt"],
            "source_bundle": str(bundle),
            "files": metadata["files"],
            "sealed": True,
            "created_at": utc_now(),
        }
        ensure_no_digest_fields(snapshot)
        snapshot_path = staging / "SOURCE_SNAPSHOT.json"
        atomic_write_json(snapshot_path, snapshot)
        for path in sorted(staging.rglob("*"), reverse=True):
            if path.is_file():
                _readonly(path)
        os.replace(staging, destination)
    except (OSError, ReleaseStateError) as error:
        shutil.rmtree(staging, ignore_errors=True)
        if isinstance(error, SnapshotError):
            raise
        raise SnapshotError(f"cannot create source snapshot: {error}") from error
    snapshot_path = destination / "SOURCE_SNAPSHOT.json"
    series_manifest = resolve_existing_file(Path(candidate["series_manifest"]), "series_manifest")
    with exclusive_lock(series_lock_path(series_manifest)):
        current = read_json(candidate_manifest)
        if current["sealed"] or current.get("source_snapshot") is not None:
            raise SnapshotError("candidate changed while the source snapshot was being created")
        current["source_snapshot"] = str(snapshot_path)
        current["generation"] += 1
        current["updated_at"] = utc_now()
        atomic_write_json(candidate_manifest, current)
    return snapshot_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        output = create_snapshot(args.candidate_manifest)
    except ReleaseStateError as error:
        print(f"CANDIDATE_SOURCE_SNAPSHOT_FAILED detail={error}", file=sys.stderr)
        return 1
    print(f"CANDIDATE_SOURCE_SNAPSHOT_OK manifest={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
