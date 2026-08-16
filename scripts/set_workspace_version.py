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

"""Atomically propagate a release version through the core Cargo topology."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tomllib
from typing import Callable


ROOT = Path(__file__).resolve().parents[1]
VERSION_PATTERN = re.compile(r"^\d+\.\d+\.\d+(?:-dev(?:\.\d+)?|-rc\.[1-9]\d*)?$")
LOCK_PATHS = (
    "Cargo.lock",
    "rocketmq-example/Cargo.lock",
    "fuzz/Cargo.lock",
    "rocketmq-macros/tests/fixtures/renamed-consumer/Cargo.lock",
)


class VersionError(ValueError):
    """Raised when the requested version transaction is invalid."""


@dataclass(frozen=True)
class VersionResult:
    previous_version: str
    version: str
    changed_files: int


def _read_scope(root: Path) -> tuple[list[dict[str, str]], set[str]]:
    path = root / "scripts/core-release-scope.json"
    try:
        scope = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise VersionError(f"cannot read core release scope: {error}") from error
    packages = scope.get("core_packages")
    if not isinstance(packages, list) or not packages:
        raise VersionError("core release scope has no packages")
    names = {item.get("name") for item in packages}
    if None in names or len(names) != len(packages):
        raise VersionError("core release package names must be unique")
    return packages, names


def workspace_inherited_exclusions(root: Path) -> set[str]:
    scope = json.loads((root / "scripts/core-release-scope.json").read_text(encoding="utf-8"))
    inherited: set[str] = set()
    for exclusion in scope.get("workspace_exclusions", []):
        name = exclusion.get("name")
        relative = exclusion.get("path")
        if not isinstance(name, str) or not isinstance(relative, str):
            continue
        path = root / relative / "Cargo.toml"
        if not path.is_file():
            continue
        data = _toml(path, path.read_text(encoding="utf-8"))
        if data.get("package", {}).get("version", {}).get("workspace") is True:
            inherited.add(name)
    return inherited


def _toml(path: Path, text: str) -> dict:
    try:
        return tomllib.loads(text)
    except tomllib.TOMLDecodeError as error:
        raise VersionError(f"invalid TOML in {path}: {error}") from error


def workspace_version(root: Path) -> str:
    path = root / "Cargo.toml"
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
        value = data["workspace"]["package"]["version"]
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError, KeyError, TypeError) as error:
        raise VersionError(f"cannot read workspace package version: {error}") from error
    if not isinstance(value, str):
        raise VersionError("workspace package version must be a string")
    return value


def validate_transition(previous: str, version: str) -> None:
    if not VERSION_PATTERN.fullmatch(version):
        raise VersionError(f"unsupported release version: {version}")
    if previous == version:
        return
    previous_rc = re.fullmatch(r"(\d+\.\d+\.\d+)-rc\.(\d+)", previous)
    next_rc = re.fullmatch(r"(\d+\.\d+\.\d+)-rc\.(\d+)", version)
    if next_rc:
        if previous_rc:
            if next_rc.group(1) != previous_rc.group(1) or int(next_rc.group(2)) != int(previous_rc.group(2)) + 1:
                raise VersionError(f"RC sequence must advance exactly once: {previous} -> {version}")
        elif previous == f"{next_rc.group(1)}-dev":
            if int(next_rc.group(2)) != 1:
                raise VersionError(f"the first RC must follow its development version: {previous} -> {version}")
        elif previous != next_rc.group(1):
            raise VersionError(
                "an RC must follow its own development, RC, or rejected final version: "
                f"{previous} -> {version}"
            )
    elif previous_rc and version != previous_rc.group(1):
        raise VersionError(f"an RC can only advance or promote its own base version: {previous} -> {version}")


def _replace_workspace_version(text: str, previous: str, version: str) -> str:
    pattern = re.compile(
        r"(?ms)(^\[workspace\.package\]\s*$.*?^version\s*=\s*\")" + re.escape(previous) + r"(\")"
    )
    updated, count = pattern.subn(rf"\g<1>{version}\g<2>", text, count=1)
    if count != 1:
        raise VersionError("root Cargo.toml must contain exactly one workspace package version")
    return updated


def _replace_core_dependencies(text: str, core_names: set[str], previous: str, version: str) -> str:
    lines = text.splitlines(keepends=True)
    for index, line in enumerate(lines):
        if "path" not in line or "version" not in line:
            continue
        key_match = re.match(r"\s*([A-Za-z0-9_-]+)\s*=", line)
        package_match = re.search(r"\bpackage\s*=\s*\"([^\"]+)\"", line)
        dependency = package_match.group(1) if package_match else key_match.group(1) if key_match else None
        if dependency not in core_names:
            continue
        lines[index] = re.sub(
            r"(\bversion\s*=\s*\")" + re.escape(previous) + r"(\")",
            rf"\g<1>{version}\g<2>",
            line,
        )
    return "".join(lines)


def _replace_direct_package_version(text: str, package_name: str, previous: str, version: str) -> str:
    data = _toml(Path(f"{package_name}/Cargo.toml"), text)
    package = data.get("package", {})
    if package.get("name") != package_name or package.get("version") != previous:
        return text
    pattern = re.compile(r"(?ms)(^\[package\]\s*$.*?^version\s*=\s*\")" + re.escape(previous) + r"(\")")
    return pattern.sub(rf"\g<1>{version}\g<2>", text, count=1)


def _replace_lock_versions(text: str, core_names: set[str], previous: str, version: str, path: Path) -> str:
    data = _toml(path, text)
    if not isinstance(data.get("package"), list):
        raise VersionError(f"Cargo lock has no package list: {path}")
    blocks = re.split(r"(?=^\[\[package\]\]\s*$)", text, flags=re.MULTILINE)
    updated: list[str] = []
    for block in blocks:
        name_match = re.search(r'^name\s*=\s*"([^"]+)"\s*$', block, re.MULTILINE)
        if name_match and name_match.group(1) in core_names:
            block = re.sub(
                r'(^version\s*=\s*")' + re.escape(previous) + r'("\s*$)',
                rf"\g<1>{version}\g<2>",
                block,
                count=1,
                flags=re.MULTILINE,
            )
        updated.append(block)
    return "".join(updated)


def _release_surface_updates(root: Path, previous: str, version: str) -> dict[Path, bytes]:
    updates: dict[Path, bytes] = {}
    chart = root / "distribution/helm/rocketmq-rust-core/Chart.yaml"
    if chart.is_file():
        original = chart.read_text(encoding="utf-8")
        updated = original
        for field in ("version", "appVersion"):
            updated, count = re.subn(
                rf'(?m)^(\s*{field}\s*:\s*["\']?){re.escape(previous)}(["\']?\s*)$',
                rf"\g<1>{version}\g<2>",
                updated,
                count=1,
            )
            if count != 1:
                raise VersionError(f"core Chart must contain exactly one {field}: {previous}")
        updates[chart] = updated.encode("utf-8")
    values = root / "distribution/helm/rocketmq-rust-core/values.yaml"
    if values.is_file():
        original = values.read_text(encoding="utf-8")
        updated, count = re.subn(
            rf'(?m)^(\s*candidateVersion\s*:\s*["\']?){re.escape(previous)}(["\']?\s*)$',
            rf"\g<1>{version}\g<2>",
            original,
            count=1,
        )
        if count != 1:
            raise VersionError(f"core values must contain candidateVersion: {previous}")
        updates[values] = updated.encode("utf-8")
    policy = root / "docker/core-container-policy.json"
    if policy.is_file():
        try:
            value = json.loads(policy.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise VersionError(f"invalid core container policy: {error}") from error
        if value.get("release_version") != previous:
            raise VersionError(f"core container policy must use {previous}")
        value["release_version"] = version
        updates[policy] = (json.dumps(value, indent=2) + "\n").encode("utf-8")
    return updates


def plan_version_update(root: Path, version: str) -> tuple[str, dict[Path, bytes]]:
    root = root.resolve()
    packages, core_names = _read_scope(root)
    previous = workspace_version(root)
    validate_transition(previous, version)
    manifest_paths = [root / "Cargo.toml"]
    for package in packages:
        manifest_paths.append(root / package["path"] / "Cargo.toml")
    example_manifest = root / "rocketmq-example/Cargo.toml"
    if example_manifest.is_file():
        manifest_paths.append(example_manifest)
    missing = [str(path.relative_to(root)) for path in manifest_paths if not path.is_file()]
    if missing:
        raise VersionError(f"required manifest files are missing: {', '.join(missing)}")

    planned: dict[Path, bytes] = {}
    for path in dict.fromkeys(manifest_paths):
        original = path.read_text(encoding="utf-8")
        updated = _replace_core_dependencies(original, core_names, previous, version)
        if path == root / "Cargo.toml":
            updated = _replace_workspace_version(updated, previous, version)
        else:
            package_name = next((item["name"] for item in packages if root / item["path"] / "Cargo.toml" == path), None)
            if package_name:
                updated = _replace_direct_package_version(updated, package_name, previous, version)
        _toml(path, updated)
        if updated != original:
            planned[path] = updated.encode("utf-8")

    inherited_exclusions = workspace_inherited_exclusions(root)
    for relative in LOCK_PATHS:
        path = root / relative
        if not path.is_file():
            raise VersionError(f"required lockfile is missing: {relative}")
        original = path.read_text(encoding="utf-8")
        lock_names = core_names | inherited_exclusions if relative == "Cargo.lock" else core_names
        updated = _replace_lock_versions(original, lock_names, previous, version, path)
        _toml(path, updated)
        if updated != original:
            planned[path] = updated.encode("utf-8")
    for path, content in _release_surface_updates(root, previous, version).items():
        if path.read_bytes() != content:
            planned[path] = content
    return previous, planned


def _atomic_replace(planned: dict[Path, bytes], validator: Callable[[], None] | None = None) -> None:
    originals = {path: path.read_bytes() for path in planned}
    temporary: dict[Path, Path] = {}
    replaced: list[Path] = []
    try:
        for path, content in planned.items():
            temp = path.with_name(f".{path.name}.version-{os.getpid()}.tmp")
            with temp.open("wb") as output:
                output.write(content)
                output.flush()
                os.fsync(output.fileno())
            temporary[path] = temp
        for path, temp in temporary.items():
            os.replace(temp, path)
            replaced.append(path)
        if validator is not None:
            validator()
    except (OSError, VersionError) as error:
        for path in reversed(replaced):
            path.write_bytes(originals[path])
        raise VersionError(f"version transaction failed and was rolled back: {error}") from error
    finally:
        for temp in temporary.values():
            if temp.exists():
                temp.unlink()


def apply_version(root: Path, version: str, *, validator: Callable[[], None] | None = None) -> VersionResult:
    previous, planned = plan_version_update(root, version)
    _atomic_replace(planned, validator=validator)
    return VersionResult(previous, version, len(planned))


def locked_validator(root: Path) -> Callable[[], None]:
    root = root.resolve()
    commands = (
        ["cargo", "metadata", "--locked", "--format-version", "1", "--no-deps"],
        [
            "cargo",
            "metadata",
            "--locked",
            "--format-version",
            "1",
            "--no-deps",
            "--manifest-path",
            "rocketmq-example/Cargo.toml",
        ],
        [
            "cargo",
            "metadata",
            "--locked",
            "--format-version",
            "1",
            "--no-deps",
            "--manifest-path",
            "fuzz/Cargo.toml",
        ],
        [
            "cargo",
            "check",
            "--locked",
            "--offline",
            "--manifest-path",
            "rocketmq-macros/tests/fixtures/renamed-consumer/Cargo.toml",
        ],
    )

    def validate() -> None:
        for command in commands:
            result = subprocess.run(command, cwd=root, text=True, capture_output=True, check=False)
            if result.returncode != 0:
                detail = (result.stderr or result.stdout).strip().splitlines()[-1:]
                raise VersionError(f"locked validation failed ({' '.join(command)}): {' '.join(detail)}")

    return validate


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--version", required=True)
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args()
    try:
        previous, planned = plan_version_update(args.root, args.version)
        if not args.check_only:
            _atomic_replace(planned, validator=locked_validator(args.root))
    except VersionError as error:
        print(f"SET_WORKSPACE_VERSION_FAILED detail={error}", file=sys.stderr)
        return 1
    print(
        f"SET_WORKSPACE_VERSION_OK previous={previous} version={args.version} "
        f"changed_files={len(planned)} mode={'check' if args.check_only else 'write'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
