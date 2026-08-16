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

"""Create and transfer an explicit generation of an append-only release series."""

from __future__ import annotations

import argparse
import io
import json
from pathlib import Path, PurePosixPath
import re
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
    require_safe_id,
    resolve_existing_file,
    series_lock_path,
    utc_now,
    validate_candidate,
    validate_series,
)


class SeriesError(ReleaseStateError):
    """Raised when release-series state cannot be created or transferred."""


def create_series(root: Path, release_line: str, series_id: str) -> Path:
    if re.fullmatch(r"\d+\.\d+", release_line) is None:
        raise SeriesError("release line must use MAJOR.MINOR")
    require_safe_id(series_id, "series_id")
    manifest = (root / release_line / series_id / "RELEASE_SERIES.json").resolve()
    if manifest.exists():
        raise SeriesError(f"release series already exists: {manifest}")
    value: dict[str, Any] = {
        "schema_version": 1,
        "release_line": release_line,
        "series_id": series_id,
        "generation": 0,
        "next_ordinal": 1,
        "next_rc_suffix": 1,
        "head": None,
        "consecutive_successful_rcs": 0,
        "entries": [],
        "pending_operation": None,
        "created_at": utc_now(),
        "updated_at": utc_now(),
    }
    ensure_no_digest_fields(value)
    atomic_write_json(manifest, value)
    export_control_bundle(manifest, default_control_bundle(manifest, 0))
    return manifest


def default_control_bundle(series_manifest: Path, generation: int) -> Path:
    return series_manifest.resolve().parent / f"RELEASE_SERIES_CONTROL_BUNDLE.g{generation}.tar"


def export_control_bundle(series_manifest: Path, output: Path) -> Path:
    try:
        series_manifest = resolve_existing_file(series_manifest, "series_manifest")
    except ReleaseStateError as error:
        raise SeriesError(str(error)) from error
    with exclusive_lock(series_lock_path(series_manifest)):
        return _export_control_bundle_locked(series_manifest, output)


def _export_control_bundle_locked(series_manifest: Path, output: Path) -> Path:
    value = read_json(series_manifest)
    validate_series(value)
    if value["pending_operation"] is not None:
        raise SeriesError("cannot export a series with a pending operation")
    path_to_ordinal = {
        str(Path(entry["candidate_manifest"]).resolve()): entry["ordinal"] for entry in value["entries"]
    }
    portable_series = json.loads(json.dumps(value))
    candidate_payloads: list[tuple[str, bytes]] = []
    for entry in portable_series["entries"]:
        source_path = Path(entry["candidate_manifest"]).resolve()
        candidate = read_json(resolve_existing_file(source_path, "series candidate manifest"))
        validate_candidate(candidate)
        expected = {
            "ordinal": entry["ordinal"],
            "version": entry["version"],
            "candidate_kind": entry["candidate_kind"],
            "run_id": entry["run_id"],
            "attempt": entry["attempt"],
            "parent_manifest": entry["parent_manifest"],
            "state": entry["state"],
            "outcome": entry["outcome"],
            "sealed": entry["sealed"],
        }
        actual = {key: candidate.get(key) for key in expected}
        if actual != expected:
            raise SeriesError(
                f"candidate manifest and series entry disagree at ordinal {entry['ordinal']}"
            )
        if (
            Path(candidate["series_manifest"]).resolve() != series_manifest
            or candidate["series_id"] != value["series_id"]
            or Path(candidate["candidate_root"]).resolve() != source_path.parent
        ):
            raise SeriesError(
                f"candidate manifest ownership is inconsistent at ordinal {entry['ordinal']}"
            )
        relative = f"candidates/{entry['ordinal']}/CANDIDATE_RUN.json"
        original_parent = candidate.get("parent_manifest")
        parent_ordinal = path_to_ordinal.get(str(Path(original_parent).resolve())) if original_parent else None
        candidate["candidate_root"] = f"candidates/{entry['ordinal']}"
        candidate["series_manifest"] = "RELEASE_SERIES.json"
        candidate["parent_manifest"] = (
            f"candidates/{parent_ordinal}/CANDIDATE_RUN.json" if parent_ordinal is not None else None
        )
        entry["candidate_manifest"] = relative
        entry["parent_manifest"] = candidate["parent_manifest"]
        candidate_payloads.append((relative, (json.dumps(candidate, indent=2) + "\n").encode()))
    if portable_series["head"] is not None:
        head_ordinal = portable_series["head"]["ordinal"]
        portable_series["head"] = next(
            json.loads(json.dumps(entry)) for entry in portable_series["entries"] if entry["ordinal"] == head_ordinal
        )
    ensure_no_digest_fields(portable_series)
    control = {
        "schema_version": 1,
        "release_line": value["release_line"],
        "series_id": value["series_id"],
        "generation": value["generation"],
        "manifest_path": "RELEASE_SERIES.json",
        "candidate_manifests": [name for name, _ in candidate_payloads],
    }
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    with tarfile.open(temporary, "w") as archive:
        payloads = [
            ("CONTROL_BUNDLE.json", (json.dumps(control, indent=2) + "\n").encode()),
            ("RELEASE_SERIES.json", (json.dumps(portable_series, indent=2) + "\n").encode()),
            *candidate_payloads,
        ]
        for name, content in payloads:
            info = tarfile.TarInfo(name)
            info.size = len(content)
            info.mode = 0o444
            archive.addfile(info, io.BytesIO(content))
    temporary.replace(output)
    return output


def _regular_member(archive: tarfile.TarFile, name: str) -> bytes:
    try:
        member = archive.getmember(name)
    except KeyError as error:
        raise SeriesError(f"series control bundle is missing {name}") from error
    path = PurePosixPath(member.name)
    if not member.isfile() or path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise SeriesError(f"unsafe series control member: {member.name}")
    source = archive.extractfile(member)
    if source is None:
        raise SeriesError(f"cannot read series control member: {member.name}")
    return source.read()


def import_control_bundle(bundle: Path, destination: Path, *, expected_generation: int) -> Path:
    try:
        bundle = resolve_existing_file(bundle, "series_control_bundle")
    except ReleaseStateError as error:
        raise SeriesError(str(error)) from error
    try:
        with tarfile.open(bundle, "r") as archive:
            members = archive.getmembers()
            names = {member.name for member in members}
            if len(names) != len(members):
                raise SeriesError("series control bundle contains duplicate members")
            control = json.loads(_regular_member(archive, "CONTROL_BUNDLE.json"))
            if not isinstance(control, dict):
                raise SeriesError("series control metadata must be an object")
            candidate_names = control.get("candidate_manifests")
            if not isinstance(candidate_names, list) or any(not isinstance(name, str) for name in candidate_names):
                raise SeriesError("series control candidate manifest list is invalid")
            expected_names = {
                "CONTROL_BUNDLE.json",
                "RELEASE_SERIES.json",
                *candidate_names,
            }
            if names != expected_names:
                raise SeriesError(f"unexpected series control members: {sorted(names)}")
            series_bytes = _regular_member(archive, "RELEASE_SERIES.json")
            candidate_bytes = {
                name: _regular_member(archive, name) for name in candidate_names
            }
    except (tarfile.TarError, json.JSONDecodeError, UnicodeDecodeError) as error:
        raise SeriesError(f"invalid series control bundle: {error}") from error
    if control.get("generation") != expected_generation:
        raise SeriesError(
            f"series generation mismatch: expected {expected_generation}, found {control.get('generation')}"
        )
    try:
        value = json.loads(series_bytes)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise SeriesError(f"invalid series manifest in control bundle: {error}") from error
    validate_series(value)
    ensure_no_digest_fields(control)
    ensure_no_digest_fields(value)
    if value["generation"] != expected_generation or value["pending_operation"] is not None:
        raise SeriesError("series control bundle is stale or contains a pending operation")
    if (value["release_line"], value["series_id"]) != (control.get("release_line"), control.get("series_id")):
        raise SeriesError("series control metadata does not match its manifest")
    manifest = (destination / value["release_line"] / value["series_id"] / "RELEASE_SERIES.json").resolve()
    if manifest.exists():
        raise SeriesError(f"refusing to overwrite imported series: {manifest}")
    imported_candidates: dict[str, Path] = {}
    for name, content in candidate_bytes.items():
        try:
            candidate = json.loads(content)
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise SeriesError(f"invalid candidate manifest {name}: {error}") from error
        validate_candidate(candidate)
        output = (manifest.parent / PurePosixPath(name)).resolve()
        try:
            output.relative_to(manifest.parent)
        except ValueError as error:
            raise SeriesError(f"candidate control path escapes destination: {name}") from error
        imported_candidates[name] = output
        candidate["candidate_root"] = str(output.parent)
        candidate["series_manifest"] = str(manifest)
        parent = candidate.get("parent_manifest")
        candidate["parent_manifest"] = str((manifest.parent / parent).resolve()) if parent else None
        atomic_write_json(output, candidate)
    for entry in value["entries"]:
        relative = entry["candidate_manifest"]
        if relative not in imported_candidates:
            raise SeriesError(f"series entry has no transferred candidate manifest: {relative}")
        entry["candidate_manifest"] = str(imported_candidates[relative])
        parent = entry.get("parent_manifest")
        entry["parent_manifest"] = str((manifest.parent / parent).resolve()) if parent else None
    if value["head"] is not None:
        head_ordinal = value["head"]["ordinal"]
        value["head"] = next(
            json.loads(json.dumps(entry)) for entry in value["entries"] if entry["ordinal"] == head_ordinal
        )
    atomic_write_json(manifest, value)
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    create = subcommands.add_parser("create")
    create.add_argument("--release-line", required=True)
    create.add_argument("--series-id", required=True)
    create.add_argument("--root", type=Path, required=True)
    export = subcommands.add_parser("export-control")
    export.add_argument("--series-manifest", type=Path, required=True)
    export.add_argument("--output", type=Path, required=True)
    import_command = subcommands.add_parser("import-control")
    import_command.add_argument("--bundle", type=Path, required=True)
    import_command.add_argument("--destination", type=Path, required=True)
    import_command.add_argument("--expected-generation", type=int, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "create":
            manifest = create_series(args.root, args.release_line, args.series_id)
        elif args.command == "export-control":
            manifest = export_control_bundle(args.series_manifest, args.output)
        else:
            manifest = import_control_bundle(
                args.bundle, args.destination, expected_generation=args.expected_generation
            )
    except ReleaseStateError as error:
        print(f"RELEASE_SERIES_FAILED detail={error}", file=sys.stderr)
        return 1
    print(f"RELEASE_SERIES_OK command={args.command} output={manifest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
