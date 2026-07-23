#!/usr/bin/env python3
#
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

"""Freeze the remaining Rust nightly feature surface until stable closeout."""

from __future__ import annotations

import argparse
import dataclasses
import json
import re
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "scripts" / "stable-surface-policy.json"
FEATURE_ATTRIBUTE_RE = re.compile(r"#!\s*\[\s*feature\s*\((?P<body>.*?)\)\s*\]", re.DOTALL)
FEATURE_START_RE = re.compile(r"#!\s*\[\s*feature\b")
FEATURE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
EXCLUDED_PARTS = frozenset({".git", ".idea", ".vscode", "node_modules", "target"})


class InputError(Exception):
    """An invalid policy or Rust feature declaration."""


@dataclasses.dataclass(frozen=True, order=True)
class NightlyFeature:
    path: str
    feature: str

    def render(self) -> str:
        return f"{self.path}:{self.feature}"


def load_policy(path: Path) -> dict[str, Any]:
    try:
        policy = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise InputError(f"cannot read policy {path}: {error}") from error
    if not isinstance(policy, dict):
        raise InputError("policy must contain a JSON object")
    if policy.get("schema_version") != 1:
        raise InputError("policy schema_version must be 1")
    allowed = policy.get("allowed_features")
    if not isinstance(allowed, list):
        raise InputError("policy allowed_features must be a list")
    return policy


def policy_features(policy: dict[str, Any]) -> set[NightlyFeature]:
    required = {"path", "feature", "owner", "remove_by", "reason"}
    result: set[NightlyFeature] = set()
    for index, raw in enumerate(policy["allowed_features"]):
        if not isinstance(raw, dict):
            raise InputError(f"allowed_features[{index}] must be an object")
        missing = sorted(required - raw.keys())
        if missing:
            raise InputError(f"allowed_features[{index}] missing keys: {', '.join(missing)}")
        path = raw["path"]
        feature = raw["feature"]
        if not isinstance(path, str) or not path or Path(path).is_absolute() or "\\" in path:
            raise InputError(f"allowed_features[{index}].path must be a repository-relative POSIX path")
        if not path.endswith(".rs"):
            raise InputError(f"allowed_features[{index}].path must name a Rust source file")
        if not isinstance(feature, str) or not FEATURE_NAME_RE.fullmatch(feature):
            raise InputError(f"allowed_features[{index}].feature is invalid")
        for field in ("owner", "remove_by", "reason"):
            if not isinstance(raw[field], str) or not raw[field].strip():
                raise InputError(f"allowed_features[{index}].{field} must be non-empty")
        entry = NightlyFeature(path=path, feature=feature)
        if entry in result:
            raise InputError(f"duplicate allowed feature: {entry.render()}")
        result.add(entry)
    return result


def rust_sources(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.rglob("*.rs")
        if not any(part in EXCLUDED_PARTS for part in path.relative_to(root).parts)
    )


def scan_features(root: Path) -> set[NightlyFeature]:
    findings: set[NightlyFeature] = set()
    for path in rust_sources(root):
        try:
            source = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as error:
            raise InputError(f"cannot read Rust source {path}: {error}") from error
        matches = list(FEATURE_ATTRIBUTE_RE.finditer(source))
        if len(matches) != len(FEATURE_START_RE.findall(source)):
            relative = path.relative_to(root).as_posix()
            raise InputError(f"cannot parse every feature attribute in {relative}")
        for match in matches:
            body = match.group("body")
            names = [name.strip() for name in body.split(",")]
            if not names or any(not FEATURE_NAME_RE.fullmatch(name) for name in names):
                relative = path.relative_to(root).as_posix()
                line = source.count("\n", 0, match.start()) + 1
                raise InputError(f"invalid feature declaration at {relative}:{line}")
            for name in names:
                findings.add(NightlyFeature(path=path.relative_to(root).as_posix(), feature=name))
    return findings


def validate(root: Path, policy_path: Path, mode: str) -> str:
    if not root.is_dir():
        raise InputError(f"root does not exist: {root}")
    policy = load_policy(policy_path)
    allowed = policy_features(policy)
    actual = scan_features(root)

    unregistered = sorted(actual - allowed)
    stale = sorted(allowed - actual)
    if unregistered:
        rendered = ", ".join(entry.render() for entry in unregistered)
        raise InputError(f"unregistered nightly features: {rendered}")
    if stale:
        rendered = ", ".join(entry.render() for entry in stale)
        raise InputError(f"stale allowed nightly features: {rendered}")
    if mode == "target" and actual:
        rendered = ", ".join(entry.render() for entry in sorted(actual))
        raise InputError(f"stable target still has nightly features: {rendered}")
    return f"STABLE_SURFACE_GUARD_OK mode={mode} features={len(actual)}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--mode", choices=("baseline", "target"), default="baseline")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        message = validate(args.root.resolve(), args.policy.resolve(), args.mode)
    except InputError as error:
        print(f"STABLE_SURFACE_GUARD_ERROR: {error}", file=sys.stderr)
        return 1
    print(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
