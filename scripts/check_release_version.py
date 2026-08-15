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

"""Check semantic release-version alignment without publishing anything."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import re
import sys
import tempfile
import tomllib

import set_workspace_version as setter


ROOT = Path(__file__).resolve().parents[1]
DISTRIBUTION = ROOT / "distribution"
if str(DISTRIBUTION) not in sys.path:
    sys.path.insert(0, str(DISTRIBUTION))

from release_state import ReleaseStateError, validate_candidate


class CandidateVersionError(ValueError):
    """Raised when a real candidate manifest is missing or self-inconsistent."""


def candidate_selector(path: Path) -> tuple[str, Path]:
    try:
        manifest = path.resolve(strict=True)
        value = json.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CandidateVersionError(f"cannot read candidate manifest: {error}") from error
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise CandidateVersionError("candidate manifest must use schema version 1")
    try:
        validate_candidate(value)
    except ReleaseStateError as error:
        raise CandidateVersionError(f"candidate manifest violates its closed schema: {error}") from error
    version = value.get("version")
    if not isinstance(version, str) or setter.VERSION_PATTERN.fullmatch(version) is None:
        raise CandidateVersionError(f"candidate manifest has an unsupported version: {version!r}")
    if value.get("candidate_kind") not in {"rc", "final"} or not value.get("candidate_id"):
        raise CandidateVersionError("candidate manifest has no stable identity or kind")
    candidate_root = Path(value.get("candidate_root", "")).resolve()
    if candidate_root != manifest.parent.resolve():
        raise CandidateVersionError("candidate_root must be the directory containing CANDIDATE_RUN.json")
    return version, candidate_root


@dataclass(frozen=True, order=True)
class VersionFinding:
    code: str
    path: str
    detail: str


def check_version(root: Path, version: str) -> list[VersionFinding]:
    root = root.resolve()
    findings: list[VersionFinding] = []
    try:
        packages, core_names = setter._read_scope(root)
        actual = setter.workspace_version(root)
    except setter.VersionError as error:
        return [VersionFinding("version-input-invalid", "repository", str(error))]
    if actual != version:
        findings.append(VersionFinding("workspace-version-drift", "Cargo.toml", f"expected {version}, found {actual}"))
    manifest_paths = [root / "Cargo.toml", *(root / item["path"] / "Cargo.toml" for item in packages)]
    example = root / "rocketmq-example/Cargo.toml"
    if example.is_file():
        manifest_paths.append(example)
    for path in manifest_paths:
        try:
            text = path.read_text(encoding="utf-8")
            manifest = tomllib.loads(text)
        except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
            findings.append(VersionFinding("manifest-invalid", path.relative_to(root).as_posix(), str(error)))
            continue
        package = manifest.get("package", {})
        if path != root / "Cargo.toml" and isinstance(package.get("version"), str) and package["name"] in core_names:
            if package["version"] != version:
                findings.append(
                    VersionFinding(
                        "manifest-version-drift",
                        path.relative_to(root).as_posix(),
                        f"{package['name']} must use {version}",
                    )
                )
        for line_number, line in enumerate(text.splitlines(), start=1):
            if "path" not in line or "version" not in line:
                continue
            key_match = re.match(r"\s*([A-Za-z0-9_-]+)\s*=", line)
            package_match = re.search(r"\bpackage\s*=\s*\"([^\"]+)\"", line)
            dependency = package_match.group(1) if package_match else key_match.group(1) if key_match else None
            if dependency not in core_names:
                continue
            found = re.search(r"\bversion\s*=\s*\"([^\"]+)\"", line)
            if found is None or found.group(1) != version:
                findings.append(
                    VersionFinding(
                        "manifest-version-drift",
                        f"{path.relative_to(root).as_posix()}:{line_number}",
                        f"{dependency} must use {version}",
                    )
                )
    for relative in setter.LOCK_PATHS:
        path = root / relative
        try:
            data = tomllib.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
            findings.append(VersionFinding("lockfile-invalid", relative, str(error)))
            continue
        lock_names = (
            core_names | setter.workspace_inherited_exclusions(root)
            if relative == "Cargo.lock"
            else core_names
        )
        for package in data.get("package", []):
            if package.get("name") in lock_names and package.get("version") != version:
                findings.append(
                    VersionFinding(
                        "lockfile-version-drift",
                        relative,
                        f"{package.get('name')} must use {version}, found {package.get('version')}",
                    )
                )
    chart = root / "distribution/helm/rocketmq-rust-core/Chart.yaml"
    if chart.is_file():
        text = chart.read_text(encoding="utf-8")
        values = dict(re.findall(r'^\s*(version|appVersion)\s*:\s*["\']?([^"\'\s]+)', text, re.MULTILINE))
        for field in ("version", "appVersion"):
            if values.get(field) != version:
                findings.append(
                    VersionFinding(
                        "chart-version-drift",
                        chart.relative_to(root).as_posix(),
                        f"{field} must use {version}",
                    )
                )
    policy = root / "docker/core-container-policy.json"
    if policy.is_file():
        try:
            release_version = json.loads(policy.read_text(encoding="utf-8")).get("release_version")
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            findings.append(VersionFinding("oci-metadata-invalid", policy.relative_to(root).as_posix(), str(error)))
        else:
            if release_version != version:
                findings.append(
                    VersionFinding(
                        "oci-version-drift",
                        policy.relative_to(root).as_posix(),
                        f"release_version must use {version}",
                    )
                )
    values = root / "distribution/helm/rocketmq-rust-core/values.yaml"
    if values.is_file():
        text = values.read_text(encoding="utf-8")
        match = re.search(r'^\s*candidateVersion\s*:\s*["\']?([^"\'\s]+)', text, re.MULTILINE)
        if match is None or match.group(1) != version:
            findings.append(
                VersionFinding(
                    "chart-values-version-drift",
                    values.relative_to(root).as_posix(),
                    f"candidateVersion must use {version}",
                )
            )
    return sorted(set(findings))


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")


def _fixture(root: Path) -> None:
    scope = {
        "schema_version": 1,
        "core_packages": [{"name": "core", "path": "core", "classification": "registry-publish"}],
        "workspace_exclusions": [],
    }
    _write(root / "scripts/core-release-scope.json", json.dumps(scope))
    _write(
        root / "Cargo.toml",
        '[workspace]\nmembers=["core"]\n[workspace.package]\nversion="1.0.0-dev"\n'
        '[workspace.dependencies]\ncore={version="1.0.0-dev",path="core"}\n',
    )
    _write(root / "core/Cargo.toml", '[package]\nname="core"\nversion.workspace=true\n')
    lock = 'version = 4\n\n[[package]]\nname = "core"\nversion = "1.0.0-dev"\n'
    for relative in setter.LOCK_PATHS:
        _write(root / relative, lock)


def run_fixture_validation(requested_version: str | None = None) -> int:
    with tempfile.TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        _fixture(root)
        if requested_version is None:
            versions = ["1.0.0-rc.1", "1.0.0-rc.2", "1.0.0"]
        else:
            match = re.fullmatch(r"1\.0\.0-rc\.([1-9]\d*)", requested_version)
            if requested_version == "1.0.0":
                versions = ["1.0.0-rc.1", "1.0.0-rc.2", "1.0.0"]
            elif match:
                versions = [f"1.0.0-rc.{suffix}" for suffix in range(1, int(match.group(1)) + 1)]
            else:
                return 1
        for version in versions:
            setter.apply_version(root, version)
            if check_version(root, version):
                return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--version")
    parser.add_argument("--candidate-manifest", type=Path)
    parser.add_argument("--fixture", action="store_true")
    args = parser.parse_args()
    if args.fixture:
        if args.version is None or args.candidate_manifest is not None:
            parser.error("fixture mode requires --version and forbids --candidate-manifest")
        result = run_fixture_validation(args.version)
        print(f"RELEASE_VERSION_FIXTURE_{'OK' if result == 0 else 'FAILED'} version={args.version}")
        return result
    if args.version is not None or args.candidate_manifest is None:
        parser.error("real candidate mode requires --candidate-manifest and forbids bare --version")
    try:
        version, _ = candidate_selector(args.candidate_manifest)
    except CandidateVersionError as error:
        print(f"RELEASE_VERSION_CHECK_FAILED findings=1")
        print(f"RELEASE_VERSION_FINDING code=candidate-selector-invalid path={args.candidate_manifest} detail={error}")
        return 1
    findings = check_version(args.root, version)
    if findings:
        print(f"RELEASE_VERSION_CHECK_FAILED findings={len(findings)}")
        for finding in findings:
            print(f"RELEASE_VERSION_FINDING code={finding.code} path={finding.path} detail={finding.detail}")
        return 1
    print(f"RELEASE_VERSION_CHECK_OK version={version} candidate_manifest={args.candidate_manifest.resolve()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
