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

"""Shared candidate-scoped primitives for local release archives."""

from __future__ import annotations

import json
from pathlib import Path, PurePosixPath
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
for module_root in (ROOT / "distribution", SCRIPTS):
    if str(module_root) not in sys.path:
        sys.path.insert(0, str(module_root))

from release_state import (
    ReleaseStateError,
    atomic_write_json,
    read_json,
    resolve_existing_file,
    resolve_within,
    validate_candidate,
)


LAYOUT_PATH = ROOT / "distribution" / "release-layout.json"


class ArchiveError(ReleaseStateError):
    """Raised when a release archive operation violates the frozen layout."""


def load_candidate(manifest: Path, *, writable: bool = True) -> tuple[Path, dict[str, Any], Path]:
    manifest = resolve_existing_file(manifest, "candidate_manifest")
    candidate = read_json(manifest)
    validate_candidate(candidate)
    if writable and candidate["sealed"]:
        raise ArchiveError("sealed candidates cannot create or change release artifacts")
    root = Path(candidate["candidate_root"]).resolve()
    if manifest.parent.resolve() != root:
        raise ArchiveError("candidate manifest must live at the candidate root")
    return manifest, candidate, root


def load_layout(path: Path = LAYOUT_PATH) -> dict[str, Any]:
    layout = read_json(resolve_existing_file(path, "release_layout"))
    if layout.get("schema_version") != 1:
        raise ArchiveError("unsupported release layout schema")
    binaries = layout.get("binaries")
    targets = layout.get("targets")
    if not isinstance(binaries, list) or len(binaries) != 6 or not isinstance(targets, dict):
        raise ArchiveError("release layout must declare six binaries and target profiles")
    ids = [entry.get("id") for entry in binaries if isinstance(entry, dict)]
    if len(ids) != 6 or len(ids) != len(set(ids)):
        raise ArchiveError("release layout binary identifiers are invalid or duplicated")
    return layout


def target_layout(layout: dict[str, Any], target: str) -> dict[str, Any]:
    value = layout["targets"].get(target)
    if not isinstance(value, dict):
        raise ArchiveError(f"unsupported release target: {target}")
    return value


def candidate_relative(root: Path, path: Path, label: str) -> str:
    resolved = resolve_within(root, path, label)
    return resolved.relative_to(root).as_posix()


def require_relative_path(value: str, label: str) -> PurePosixPath:
    if not isinstance(value, str) or not value:
        raise ArchiveError(f"{label} must be a non-empty candidate-relative path")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or "\\" in value:
        raise ArchiveError(f"{label} is not a safe POSIX relative path: {value}")
    return path


def resolve_candidate_path(root: Path, value: str, label: str) -> Path:
    relative = require_relative_path(value, label)
    return resolve_within(root, root.joinpath(*relative.parts), label)


def file_inventory(root: Path) -> list[dict[str, Any]]:
    root = root.resolve()
    if not root.is_dir():
        raise ArchiveError(f"inventory root is not a directory: {root}")
    records: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ArchiveError(f"release input cannot contain links: {path}")
        relative = path.relative_to(root).as_posix()
        if path.is_dir():
            records.append({"path": relative, "type": "directory", "size": 0})
        elif path.is_file():
            records.append({"path": relative, "type": "file", "size": path.stat().st_size})
        else:
            raise ArchiveError(f"release input has an unsupported file type: {path}")
    return records


def compare_inventory(root: Path, expected: list[dict[str, Any]]) -> None:
    if file_inventory(root) != expected:
        raise ArchiveError(f"release input inventory changed: {root}")


def artifact_id(candidate: dict[str, Any], target: str, component: str) -> str:
    return f"{candidate['candidate_id']}.{target}.{component}"


def draft_partial_path(root: Path, target: str) -> Path:
    return root / "partials" / f"CANDIDATE_PARTIAL.{target}.draft.json"


def sealed_partial_path(root: Path, target: str) -> Path:
    return root / "partials" / f"CANDIDATE_PARTIAL.{target}.json"


def create_partial(candidate: dict[str, Any], target: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "candidate_id": candidate["candidate_id"],
        "version": candidate["version"],
        "run_id": candidate["run_id"],
        "attempt": candidate["attempt"],
        "target": target,
        "worker_id": f"release-{target}",
        "sealed": False,
        "artifacts": [],
        "events": [],
        "execution_contexts": [],
    }


def load_or_create_draft(root: Path, candidate: dict[str, Any], target: str) -> dict[str, Any]:
    path = draft_partial_path(root, target)
    if not path.exists():
        value = create_partial(candidate, target)
        atomic_write_json(path, value)
        return value
    value = read_json(path)
    expected = (
        candidate["candidate_id"],
        candidate["version"],
        candidate["run_id"],
        candidate["attempt"],
        target,
        False,
    )
    actual = (
        value.get("candidate_id"),
        value.get("version"),
        value.get("run_id"),
        value.get("attempt"),
        value.get("target"),
        value.get("sealed"),
    )
    if actual != expected:
        raise ArchiveError("candidate partial does not match the active target run")
    return value


def add_unique_record(partial: dict[str, Any], collection: str, record: dict[str, Any]) -> None:
    values = partial.get(collection)
    identifier = record.get("id")
    if not isinstance(values, list) or not isinstance(identifier, str) or not identifier:
        raise ArchiveError(f"invalid {collection} record")
    if any(item.get("id") == identifier for item in values if isinstance(item, dict)):
        raise ArchiveError(f"duplicate {collection} identifier: {identifier}")
    values.append(record)


def save_draft(root: Path, target: str, partial: dict[str, Any]) -> None:
    if partial.get("sealed") is not False:
        raise ArchiveError("cannot write a sealed candidate partial")
    atomic_write_json(draft_partial_path(root, target), partial)


def read_policy_json(path: Path, label: str) -> dict[str, Any]:
    value = read_json(resolve_existing_file(path, label))
    if value.get("schema_version") != 1:
        raise ArchiveError(f"unsupported {label} schema")
    return value


def write_json(path: Path, value: dict[str, Any]) -> None:
    atomic_write_json(path, value)


def render_json(value: dict[str, Any]) -> str:
    return json.dumps(value, indent=2, ensure_ascii=False) + "\n"
