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

"""Transfer an immutable publication-handoff draft between isolated workers."""

from __future__ import annotations

import argparse
import io
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import sys
import tarfile
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "distribution") not in sys.path:
    sys.path.insert(0, str(ROOT / "distribution"))

from release_state import ReleaseStateError, ensure_no_digest_fields, resolve_existing_file


MANIFEST_NAME = "HANDOFF_DRAFT_TRANSFER.json"
PLATFORMS = {"linux", "windows", "macos"}


class HandoffTransferError(ReleaseStateError):
    """Raised when a draft transfer is unsafe, incomplete, or from another run."""


def _safe_relative(value: str, label: str) -> PurePosixPath:
    if not isinstance(value, str) or not value or "\\" in value:
        raise HandoffTransferError(f"{label} is not a safe relative path: {value!r}")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise HandoffTransferError(f"{label} is not a safe relative path: {value!r}")
    return path


def _identity(value: dict[str, Any]) -> tuple[Any, ...]:
    return value.get("candidate_id"), value.get("version"), value.get("run_id"), value.get("attempt")


def _validate_identity(value: dict[str, Any]) -> None:
    candidate_id, version, run_id, attempt = _identity(value)
    if not isinstance(candidate_id, str) or not candidate_id or version != "1.0.0":
        raise HandoffTransferError("handoff draft identity must describe a 1.0.0 final candidate")
    if not isinstance(run_id, str) or not run_id or not isinstance(attempt, int) or attempt < 1:
        raise HandoffTransferError("handoff draft run identity is invalid")


def _inventory(root: Path) -> list[tuple[str, Path, int]]:
    if not root.is_dir():
        raise HandoffTransferError(f"handoff draft directory does not exist: {root}")
    records: list[tuple[str, Path, int]] = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise HandoffTransferError(f"handoff draft contains a symbolic link: {path}")
        if path.is_file():
            relative = path.relative_to(root).as_posix()
            _safe_relative(relative, "handoff draft path")
            if relative == MANIFEST_NAME:
                raise HandoffTransferError(f"handoff draft reserves {MANIFEST_NAME} for transfer metadata")
            records.append((relative, path, path.stat().st_size))
        elif not path.is_dir():
            raise HandoffTransferError(f"handoff draft contains an unsupported file type: {path}")
    if not records:
        raise HandoffTransferError("handoff draft is empty")
    return records


def _tar_info(name: str, size: int) -> tarfile.TarInfo:
    info = tarfile.TarInfo(name)
    info.size = size
    info.mode = 0o644
    info.mtime = 0
    return info


def export_draft(
    draft: Path,
    output: Path,
    candidate: dict[str, Any],
    expected_platforms: list[str],
) -> Path:
    """Export a draft with a closed regular-file inventory and no digests."""

    _validate_identity(candidate)
    platforms = sorted(expected_platforms)
    if set(platforms) != PLATFORMS or len(platforms) != len(PLATFORMS):
        raise HandoffTransferError("handoff draft requires exactly linux, windows, and macos")
    draft = draft.resolve()
    records = _inventory(draft)
    output = output.resolve()
    if output.exists():
        raise HandoffTransferError(f"handoff draft transfer already exists: {output}")
    manifest = {
        "schema_version": 1,
        "candidate_id": candidate["candidate_id"],
        "version": candidate["version"],
        "run_id": candidate["run_id"],
        "attempt": candidate["attempt"],
        "expected_platforms": platforms,
        "files": [{"path": relative, "type": "file", "size": size} for relative, _path, size in records],
    }
    ensure_no_digest_fields(manifest)
    rendered = (json.dumps(manifest, indent=2) + "\n").encode()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.staging")
    if temporary.exists():
        raise HandoffTransferError(f"stale handoff transfer staging file exists: {temporary}")
    try:
        with tarfile.open(temporary, "w") as archive:
            archive.addfile(_tar_info(MANIFEST_NAME, len(rendered)), io.BytesIO(rendered))
            for relative, path, size in records:
                with path.open("rb") as source:
                    archive.addfile(_tar_info(f"payload/{relative}", size), source)
        os.replace(temporary, output)
    except (OSError, tarfile.TarError) as error:
        raise HandoffTransferError(f"cannot export handoff draft: {error}") from error
    finally:
        temporary.unlink(missing_ok=True)
    return output


def _load_archive(bundle: Path) -> tuple[dict[str, Any], list[tarfile.TarInfo]]:
    bundle = resolve_existing_file(bundle, "handoff draft transfer")
    try:
        with tarfile.open(bundle, "r") as archive:
            members = archive.getmembers()
            names: set[str] = set()
            for member in members:
                path = _safe_relative(member.name, "handoff transfer member")
                if member.name in names or member.issym() or member.islnk() or not member.isfile():
                    raise HandoffTransferError(f"unsafe or duplicate handoff transfer member: {path}")
                names.add(member.name)
            try:
                manifest_member = archive.getmember(MANIFEST_NAME)
            except KeyError as error:
                raise HandoffTransferError("handoff transfer manifest is missing") from error
            stream = archive.extractfile(manifest_member)
            if stream is None:
                raise HandoffTransferError("handoff transfer manifest is unreadable")
            manifest = json.loads(stream.read())
    except (OSError, tarfile.TarError, json.JSONDecodeError) as error:
        raise HandoffTransferError(f"cannot read handoff transfer: {error}") from error
    if not isinstance(manifest, dict) or manifest.get("schema_version") != 1:
        raise HandoffTransferError("unsupported handoff transfer manifest")
    ensure_no_digest_fields(manifest)
    _validate_identity(manifest)
    return manifest, members


def read_transfer_manifest(bundle: Path) -> dict[str, Any]:
    """Read and validate transfer metadata without importing its payload."""

    manifest, _members = _load_archive(bundle)
    return manifest


def import_draft(bundle: Path, output: Path, candidate: dict[str, Any]) -> Path:
    """Import a draft atomically after validating identity and the closed inventory."""

    _validate_identity(candidate)
    bundle = resolve_existing_file(bundle, "handoff draft transfer")
    manifest, members = _load_archive(bundle)
    if _identity(manifest) != _identity(candidate):
        raise HandoffTransferError("handoff transfer belongs to another candidate run")
    if set(manifest.get("expected_platforms", [])) != PLATFORMS:
        raise HandoffTransferError("handoff transfer platform denominator is incomplete")
    entries = manifest.get("files")
    if not isinstance(entries, list):
        raise HandoffTransferError("handoff transfer file inventory is invalid")
    expected: dict[str, int] = {}
    for entry in entries:
        if not isinstance(entry, dict) or entry.get("type") != "file":
            raise HandoffTransferError("handoff transfer may contain regular files only")
        relative = _safe_relative(entry.get("path"), "handoff transfer path").as_posix()
        size = entry.get("size")
        if relative in expected or not isinstance(size, int) or size < 0:
            raise HandoffTransferError(f"invalid handoff transfer inventory entry: {relative}")
        expected[relative] = size
    actual = {member.name for member in members}
    closed = {MANIFEST_NAME, *(f"payload/{relative}" for relative in expected)}
    if actual != closed:
        raise HandoffTransferError("handoff transfer members differ from the closed manifest")
    output = output.resolve()
    if output.exists():
        raise HandoffTransferError(f"handoff transfer import already exists: {output}")
    staging = output.with_name(f".{output.name}.staging")
    if staging.exists():
        raise HandoffTransferError(f"stale handoff import staging directory exists: {staging}")
    try:
        staging.mkdir(parents=True)
        with tarfile.open(bundle, "r") as archive:
            for relative, expected_size in expected.items():
                member = archive.getmember(f"payload/{relative}")
                if member.size != expected_size:
                    raise HandoffTransferError(f"handoff transfer size mismatch: {relative}")
                source = archive.extractfile(member)
                if source is None:
                    raise HandoffTransferError(f"handoff transfer member is unreadable: {relative}")
                destination = staging.joinpath(*PurePosixPath(relative).parts)
                destination.parent.mkdir(parents=True, exist_ok=True)
                with destination.open("xb") as target:
                    shutil.copyfileobj(source, target, length=1024 * 1024)
        os.replace(staging, output)
    except (OSError, tarfile.TarError) as error:
        raise HandoffTransferError(f"cannot import handoff transfer: {error}") from error
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return output


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    export = subcommands.add_parser("export")
    export.add_argument("--draft", type=Path, required=True)
    export.add_argument("--candidate-manifest", type=Path, required=True)
    export.add_argument("--output", type=Path, required=True)
    export.add_argument("--expected-platforms", default="linux,windows,macos")
    imported = subcommands.add_parser("import")
    imported.add_argument("--bundle", type=Path, required=True)
    imported.add_argument("--candidate-manifest", type=Path, required=True)
    imported.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        candidate = json.loads(resolve_existing_file(args.candidate_manifest, "candidate manifest").read_text(encoding="utf-8"))
        if args.command == "export":
            result = export_draft(args.draft, args.output, candidate, args.expected_platforms.split(","))
        else:
            result = import_draft(args.bundle, args.output, candidate)
    except (HandoffTransferError, OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        print(f"HANDOFF_DRAFT_TRANSFER_FAILED detail={error}", file=sys.stderr)
        return 1
    print(f"HANDOFF_DRAFT_TRANSFER_OK command={args.command} output={result}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
