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

from __future__ import annotations

import argparse
import copy
from fnmatch import fnmatchcase
import json
from pathlib import Path, PurePosixPath
import re
import sys
import tomllib
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "distribution") not in sys.path:
    sys.path.insert(0, str(ROOT / "distribution"))

from create_candidate_source_snapshot import verify_snapshot_content
from release_state import (
    ReleaseStateError,
    atomic_write_json,
    ensure_no_digest_fields,
    read_json,
    resolve_existing_file,
    validate_candidate,
)


COPY_BUFFER_SIZE = 1024 * 1024
MISSING = object()
YAML_KEY = re.compile(r"^(?P<indent>\s*)(?P<key>[A-Za-z0-9_.-]+):(?P<value>.*)$")


class DeltaError(ReleaseStateError):
    pass


def _safe_relative(raw: str) -> PurePosixPath:
    path = PurePosixPath(raw)
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise DeltaError(f"unsafe source path: {raw}")
    return path


def load_policy(path: Path) -> dict[str, Any]:
    policy = read_json(resolve_existing_file(path, "final delta policy"))
    ensure_no_digest_fields(policy)
    if policy.get("schema_version") != 1 or policy.get("final_version") != "1.0.0":
        raise DeltaError("unsupported final delta policy")
    ignored = policy.get("ignored_source_paths")
    if not isinstance(ignored, list) or any(not isinstance(item, str) for item in ignored):
        raise DeltaError("final delta policy ignored_source_paths must be a string list")
    rules = policy.get("rules")
    if not isinstance(rules, list) or not rules:
        raise DeltaError("final delta policy must contain rules")
    names: set[str] = set()
    for rule in rules:
        if not isinstance(rule, dict) or not isinstance(rule.get("name"), str) or rule["name"] in names:
            raise DeltaError("final delta policy rule names must be unique")
        names.add(rule["name"])
        globs = rule.get("globs")
        if not isinstance(globs, list) or not globs or any(not isinstance(item, str) or not item for item in globs):
            raise DeltaError(f"policy rule {rule['name']} must contain globs")
        if rule.get("format") not in {"toml", "cargo-lock", "json", "yaml-scalar"}:
            raise DeltaError(f"policy rule {rule['name']} has an unsupported format")
        if rule["format"] in {"toml", "json", "yaml-scalar"}:
            fields = rule.get("allowed_fields")
            if not isinstance(fields, list) or not fields or any(not isinstance(item, str) for item in fields):
                raise DeltaError(f"policy rule {rule['name']} must contain allowed_fields")
    return policy


def _load_candidate(path: Path) -> tuple[Path, dict[str, Any]]:
    resolved = resolve_existing_file(path, "candidate manifest")
    value = read_json(resolved)
    validate_candidate(value)
    if Path(value["candidate_root"]).resolve() != resolved.parent:
        raise DeltaError("candidate manifest and candidate_root disagree")
    return resolved, value


def _validate_lineage(
    final_path: Path,
    final: dict[str, Any],
    parent_path: Path,
    parent: dict[str, Any],
    policy: dict[str, Any],
) -> None:
    if (
        final["candidate_kind"] != "final"
        or final["version"] != policy["final_version"]
        or final["state"] != "development"
        or final["sealed"]
        or final["outcome"] is not None
    ):
        raise DeltaError("final delta guard requires an unsealed development final candidate")
    try:
        direct_parent = Path(final["parent_manifest"]).resolve()
    except TypeError as error:
        raise DeltaError("final candidate has no direct parent manifest") from error
    if direct_parent != parent_path:
        raise DeltaError("final candidate parent does not match --parent-manifest")
    if (
        parent["candidate_kind"] != "rc"
        or parent["state"] != "rc-candidate-ready"
        or not parent["sealed"]
        or parent["outcome"] != "success"
        or re.fullmatch(r"1\.0\.0-rc\.[1-9][0-9]*", parent["version"]) is None
    ):
        raise DeltaError("parent must be a successful sealed RC candidate")
    if final["series_id"] != parent["series_id"] or final["ordinal"] != parent["ordinal"] + 1:
        raise DeltaError("final candidate is not the direct next entry in the parent release series")
    if final_path == parent_path:
        raise DeltaError("final candidate and parent manifest must differ")


def _load_snapshot(parent: dict[str, Any]) -> tuple[Path, dict[str, int]]:
    raw = parent.get("source_snapshot")
    if not isinstance(raw, str):
        raise DeltaError("successful RC parent has no retained source snapshot")
    manifest = resolve_existing_file(Path(raw), "parent source snapshot")
    snapshot = read_json(manifest)
    ensure_no_digest_fields(snapshot)
    identity = (
        snapshot.get("candidate_id"),
        snapshot.get("version"),
        snapshot.get("run_id"),
        snapshot.get("attempt"),
    )
    expected = (parent["candidate_id"], parent["version"], parent["run_id"], parent["attempt"])
    if identity != expected or snapshot.get("schema_version") != 1 or snapshot.get("sealed") is not True:
        raise DeltaError("parent source snapshot identity or sealed state is invalid")
    records: dict[str, int] = {}
    for item in snapshot.get("files", []):
        if not isinstance(item, dict) or item.get("type") != "file":
            raise DeltaError("parent source snapshot may contain regular files only")
        relative = _safe_relative(item.get("path", "")).as_posix()
        size = item.get("size")
        if relative in records or not isinstance(size, int) or size < 0:
            raise DeltaError(f"invalid parent source snapshot entry: {relative}")
        records[relative] = size
    if not records:
        raise DeltaError("parent source snapshot is empty")
    source_root = manifest.parent / "source"
    if not source_root.is_dir():
        raise DeltaError("parent source snapshot has no source directory")
    snapshot_bundle_path = snapshot.get("source_bundle")
    parent_bundle_path = parent.get("build_source_bundle")
    if not isinstance(snapshot_bundle_path, str) or not isinstance(parent_bundle_path, str):
        raise DeltaError("parent snapshot and candidate must reference their source bundle")
    snapshot_bundle = resolve_existing_file(Path(snapshot_bundle_path), "snapshot source bundle")
    parent_bundle = resolve_existing_file(Path(parent_bundle_path), "parent build source bundle")
    if snapshot_bundle != parent_bundle:
        raise DeltaError("parent snapshot and candidate reference different source bundles")
    verify_snapshot_content(parent_bundle, parent, source_root, records)
    return source_root, records


def _source_files(root: Path, ignored: set[str]) -> dict[str, Path]:
    if not root.is_dir():
        raise DeltaError(f"final source root is not a directory: {root}")
    result: dict[str, Path] = {}
    for path in root.rglob("*"):
        if path.is_symlink():
            raise DeltaError(f"final source contains a symbolic link: {path.relative_to(root)}")
        if not path.is_file():
            continue
        relative = path.relative_to(root).as_posix()
        if relative in ignored:
            continue
        result[relative] = path
    return result


def _stream_equal(left: Path, right: Path) -> bool:
    with left.open("rb") as first, right.open("rb") as second:
        while True:
            first_chunk = first.read(COPY_BUFFER_SIZE)
            second_chunk = second.read(COPY_BUFFER_SIZE)
            if first_chunk != second_chunk:
                return False
            if not first_chunk:
                return True


def _rule_for(path: str, policy: dict[str, Any]) -> dict[str, Any] | None:
    matched = [rule for rule in policy["rules"] if any(fnmatchcase(path, glob) for glob in rule["globs"])]
    if len(matched) > 1:
        raise DeltaError(f"final delta policy is ambiguous for {path}")
    return matched[0] if matched else None


def _diff_values(left: Any, right: Any, path: tuple[str, ...] = ()) -> list[tuple[tuple[str, ...], Any, Any]]:
    if isinstance(left, dict) and isinstance(right, dict):
        result = []
        for key in sorted(set(left) | set(right)):
            result.extend(_diff_values(left.get(key, MISSING), right.get(key, MISSING), path + (str(key),)))
        return result
    if isinstance(left, list) and isinstance(right, list):
        result = []
        for index in range(max(len(left), len(right))):
            old = left[index] if index < len(left) else MISSING
            new = right[index] if index < len(right) else MISSING
            result.extend(_diff_values(old, new, path + (str(index),)))
        return result
    if left != right:
        return [(path, left, right)]
    return []


def _validate_version_transition(old: Any, new: Any, parent_version: str, final_version: str, field: str) -> None:
    if old != parent_version or new != final_version:
        raise DeltaError(
            f"approved field {field} has an invalid version transition: {old!r} -> {new!r}"
        )


def _compare_toml(
    old_path: Path,
    new_path: Path,
    rule: dict[str, Any],
    parent_version: str,
    final_version: str,
) -> list[str]:
    try:
        old = tomllib.loads(old_path.read_text(encoding="utf-8"))
        new = tomllib.loads(new_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise DeltaError(f"cannot parse TOML delta input: {old_path.name}: {error}") from error
    changed: list[str] = []
    for parts, before, after in _diff_values(old, new):
        field = ".".join(parts)
        if not any(fnmatchcase(field, pattern) for pattern in rule["allowed_fields"]):
            raise DeltaError(f"unapproved TOML field changed in {new_path}: {field}")
        _validate_version_transition(before, after, parent_version, final_version, field)
        changed.append(field)
    if not changed:
        raise DeltaError(f"structured delta rule matched a byte-only TOML change: {new_path}")
    return changed


def _compare_cargo_lock(
    old_path: Path,
    new_path: Path,
    parent_version: str,
    final_version: str,
) -> list[str]:
    try:
        old = tomllib.loads(old_path.read_text(encoding="utf-8"))
        new = tomllib.loads(new_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise DeltaError(f"cannot parse Cargo.lock delta: {error}") from error
    old_packages = old.get("package")
    new_packages = new.get("package")
    if not isinstance(old_packages, list) or not isinstance(new_packages, list) or len(old_packages) != len(new_packages):
        raise DeltaError("unapproved Cargo.lock package denominator change")
    masked_old = copy.deepcopy(old)
    masked_new = copy.deepcopy(new)
    changed: list[str] = []
    for index, (before, after) in enumerate(zip(old_packages, new_packages, strict=True)):
        identity_before = (before.get("name"), before.get("source"))
        identity_after = (after.get("name"), after.get("source"))
        if identity_before != identity_after:
            raise DeltaError("unapproved Cargo.lock package identity or order change")
        name, source = identity_before
        if (
            source is None
            and isinstance(name, str)
            and name.startswith("rocketmq")
            and before.get("version") != after.get("version")
        ):
            field = f"package[{name}].version"
            _validate_version_transition(
                before.get("version"), after.get("version"), parent_version, final_version, field
            )
            masked_old["package"][index]["version"] = "<FINAL_VERSION>"
            masked_new["package"][index]["version"] = "<FINAL_VERSION>"
            changed.append(field)
    if masked_old != masked_new:
        raise DeltaError("unapproved Cargo.lock change outside workspace package versions")
    if not changed:
        raise DeltaError(f"Cargo.lock changed without an approved workspace version transition: {new_path}")
    return changed


def _compare_json(
    old_path: Path,
    new_path: Path,
    rule: dict[str, Any],
    parent_version: str,
    final_version: str,
) -> list[str]:
    try:
        old = json.loads(old_path.read_text(encoding="utf-8"))
        new = json.loads(new_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DeltaError(f"cannot parse JSON delta input: {error}") from error
    changed: list[str] = []
    for parts, before, after in _diff_values(old, new):
        field = ".".join(parts)
        if field not in rule["allowed_fields"]:
            raise DeltaError(f"unapproved JSON field changed in {new_path}: {field}")
        _validate_version_transition(before, after, parent_version, final_version, field)
        changed.append(field)
    if not changed:
        raise DeltaError(f"structured delta rule matched a byte-only JSON change: {new_path}")
    return changed


def _yaml_scalars(text: str) -> tuple[list[str], dict[str, tuple[int, str]]]:
    lines = text.splitlines()
    stack: list[tuple[int, str]] = []
    values: dict[str, tuple[int, str]] = {}
    for index, line in enumerate(lines):
        match = YAML_KEY.match(line)
        if match is None:
            continue
        indent = len(match.group("indent"))
        while stack and indent <= stack[-1][0]:
            stack.pop()
        key = match.group("key")
        path = ".".join([item[1] for item in stack] + [key])
        value = match.group("value").strip()
        if value:
            if path in values:
                raise DeltaError(f"duplicate YAML scalar path: {path}")
            values[path] = (index, value)
        else:
            stack.append((indent, key))
    return lines, values


def _unquote_yaml(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


def _compare_yaml(
    old_path: Path,
    new_path: Path,
    rule: dict[str, Any],
    parent_version: str,
    final_version: str,
) -> list[str]:
    try:
        old_lines, old_values = _yaml_scalars(old_path.read_text(encoding="utf-8"))
        new_lines, new_values = _yaml_scalars(new_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError) as error:
        raise DeltaError(f"cannot read YAML delta input: {error}") from error
    if set(old_values) != set(new_values) or len(old_lines) != len(new_lines):
        raise DeltaError("unapproved YAML structure change")
    changed: list[str] = []
    normalized_old = list(old_lines)
    normalized_new = list(new_lines)
    for field in rule["allowed_fields"]:
        if field not in old_values:
            raise DeltaError(f"approved YAML field is missing: {field}")
        old_index, old_value = old_values[field]
        new_index, new_value = new_values[field]
        if old_index != new_index:
            raise DeltaError(f"approved YAML field moved: {field}")
        if old_value != new_value:
            _validate_version_transition(
                _unquote_yaml(old_value), _unquote_yaml(new_value), parent_version, final_version, field
            )
            changed.append(field)
        prefix = old_lines[old_index].split(":", 1)[0]
        normalized_old[old_index] = f"{prefix}: <FINAL_VERSION>"
        normalized_new[new_index] = f"{prefix}: <FINAL_VERSION>"
    if normalized_old != normalized_new:
        raise DeltaError(f"unapproved YAML change outside approved fields: {new_path}")
    if not changed:
        raise DeltaError(f"structured delta rule matched a byte-only YAML change: {new_path}")
    return changed


def _compare_structured(
    old_path: Path,
    new_path: Path,
    rule: dict[str, Any],
    parent_version: str,
    final_version: str,
) -> list[str]:
    kind = rule["format"]
    if kind == "toml":
        return _compare_toml(old_path, new_path, rule, parent_version, final_version)
    if kind == "cargo-lock":
        return _compare_cargo_lock(old_path, new_path, parent_version, final_version)
    if kind == "json":
        return _compare_json(old_path, new_path, rule, parent_version, final_version)
    return _compare_yaml(old_path, new_path, rule, parent_version, final_version)


def compare_candidate(
    candidate_manifest: Path,
    parent_manifest: Path,
    source_root: Path,
    policy_path: Path,
) -> dict[str, Any]:
    policy = load_policy(policy_path)
    final_path, final = _load_candidate(candidate_manifest)
    parent_path, parent = _load_candidate(parent_manifest)
    _validate_lineage(final_path, final, parent_path, parent, policy)
    snapshot_root, snapshot_records = _load_snapshot(parent)
    ignored = {_safe_relative(item).as_posix() for item in policy["ignored_source_paths"]}
    final_files = _source_files(source_root.resolve(), ignored)
    expected_paths = set(snapshot_records)
    actual_paths = set(final_files)
    if expected_paths != actual_paths:
        raise DeltaError(
            f"source denominator drift: missing={sorted(expected_paths - actual_paths)} "
            f"extra={sorted(actual_paths - expected_paths)}"
        )

    byte_equal = 0
    allowed_changes: list[dict[str, Any]] = []
    for relative in sorted(expected_paths):
        old_path = snapshot_root.joinpath(*PurePosixPath(relative).parts)
        new_path = final_files[relative]
        if not old_path.is_file() or old_path.is_symlink():
            raise DeltaError(f"parent source snapshot file is missing or unsafe: {relative}")
        if old_path.stat().st_size != snapshot_records[relative]:
            raise DeltaError(f"parent source snapshot size changed: {relative}")
        if _stream_equal(old_path, new_path):
            byte_equal += 1
            continue
        rule = _rule_for(relative, policy)
        if rule is None:
            raise DeltaError(f"byte content changed outside the final delta policy: {relative}")
        fields = _compare_structured(old_path, new_path, rule, parent["version"], final["version"])
        allowed_changes.append({"path": relative, "rule": rule["name"], "fields": fields})

    report = {
        "schemaVersion": 1,
        "candidateId": final["candidate_id"],
        "version": final["version"],
        "runId": final["run_id"],
        "attempt": final["attempt"],
        "parentCandidateId": parent["candidate_id"],
        "parentVersion": parent["version"],
        "status": "passed",
        "comparedFiles": len(expected_paths),
        "byteEqualFiles": byte_equal,
        "allowedChanges": allowed_changes,
        "remotePublication": "not-executed",
    }
    ensure_no_digest_fields(report)
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate the semantic delta from a sealed RC to the final candidate")
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--parent-manifest", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument(
        "--policy",
        type=Path,
        default=ROOT / "distribution" / "final-candidate-delta-policy.json",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.output.exists():
            raise DeltaError("final candidate delta output already exists")
        report = compare_candidate(
            args.candidate_manifest,
            args.parent_manifest,
            args.source_root,
            args.policy,
        )
        atomic_write_json(args.output.resolve(), report)
    except (ReleaseStateError, OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        print(f"FINAL_CANDIDATE_DELTA_FAILED detail={error}", file=sys.stderr)
        return 1
    print(
        f"FINAL_CANDIDATE_DELTA_OK files={report['comparedFiles']} "
        f"allowed_changes={len(report['allowedChanges'])} output={args.output.resolve()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
