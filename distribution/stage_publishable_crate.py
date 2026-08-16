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

"""Stage Cargo workspace packages into a candidate-local registry."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import tarfile
from typing import Any


CRATES_IO_INDEX = "https://github.com/rust-lang/crates.io-index"


class StagingError(ValueError):
    """Raised when a crate cannot be staged without changing its contract."""


def _run(command: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if completed.returncode != 0:
        raise StagingError(
            f"command failed ({completed.returncode}): {' '.join(command)}\n"
            f"{completed.stdout}{completed.stderr}"
        )
    return completed


def _legal_source(workspace_root: Path, policy: dict[str, Any], field: str) -> Path:
    value = policy.get(field)
    if not isinstance(value, str) or not value:
        raise StagingError(f"legal policy is missing {field}")
    source = (workspace_root / value).resolve()
    try:
        source.relative_to(workspace_root.resolve())
    except ValueError as error:
        raise StagingError(f"legal policy {field} escapes the workspace") from error
    if not source.is_file():
        raise StagingError(f"legal source does not exist: {source}")
    return source


def _safe_member(member: tarfile.TarInfo, expected_root: str) -> None:
    path = PurePosixPath(member.name)
    if path.is_absolute() or ".." in path.parts or not path.parts or path.parts[0] != expected_root:
        raise StagingError(f"crate contains an unsafe archive path: {member.name}")
    if member.issym() or member.islnk():
        raise StagingError(f"crate contains a link entry: {member.name}")


def _repack_with_legal(
    source: Path,
    output: Path,
    *,
    package_name: str,
    version: str,
    legal_files: list[tuple[str, Path]],
) -> None:
    expected_root = f"{package_name}-{version}"
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        raise StagingError(f"candidate crate already exists: {output}")
    temporary = output.with_name(output.name + ".tmp")
    try:
        with tarfile.open(source, "r:gz") as archive:
            members = archive.getmembers()
            for member in members:
                _safe_member(member, expected_root)
            names = {member.name for member in members}
            for archive_name, _legal_source_path in legal_files:
                target = f"{expected_root}/{archive_name}"
                if target in names:
                    raise StagingError(f"crate already contains managed legal file {target}")
            with tarfile.open(temporary, "w:gz", format=tarfile.PAX_FORMAT) as packaged:
                for member in members:
                    file_object = archive.extractfile(member) if member.isfile() else None
                    packaged.addfile(member, file_object)
                for archive_name, legal_source in legal_files:
                    content = legal_source.read_bytes()
                    member = tarfile.TarInfo(f"{expected_root}/{archive_name}")
                    member.size = len(content)
                    member.mode = 0o644
                    member.mtime = 0
                    from io import BytesIO

                    packaged.addfile(member, BytesIO(content))
        os.replace(temporary, output)
    finally:
        if temporary.exists():
            temporary.unlink()


def _metadata(workspace_root: Path) -> dict[str, Any]:
    completed = _run(
        [
            "cargo",
            "metadata",
            "--locked",
            "--offline",
            "--format-version",
            "1",
            "--no-deps",
            "--manifest-path",
            str(workspace_root / "Cargo.toml"),
        ],
        cwd=workspace_root,
    )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise StagingError(f"cargo metadata returned invalid JSON: {error}") from error
    if not isinstance(value, dict):
        raise StagingError("cargo metadata did not return an object")
    return value


def _verify_staged_archive(
    output: Path,
    *,
    package_name: str,
    version: str,
    legal_names: tuple[str, str],
) -> None:
    with tarfile.open(output, "r:gz") as archive:
        names = set(archive.getnames())
    expected_root = f"{package_name}-{version}"
    required = {
        f"{expected_root}/Cargo.toml",
        *(f"{expected_root}/{name}" for name in legal_names),
    }
    missing = sorted(required - names)
    if missing:
        raise StagingError(f"staged crate is incomplete: {', '.join(missing)}")


def stage_workspace_crates(
    workspace_root: Path,
    candidate_root: Path,
    *,
    packages: list[dict[str, Any]],
    legal_policy: dict[str, Any],
) -> list[dict[str, Any]]:
    workspace_root = workspace_root.resolve()
    candidate_root = candidate_root.resolve()
    if not packages:
        raise StagingError("no packages were selected for staging")
    license_source = _legal_source(workspace_root, legal_policy, "license_source")
    notice_source = _legal_source(workspace_root, legal_policy, "notice_source")
    license_name = legal_policy.get("license_archive_name")
    notice_name = legal_policy.get("notice_archive_name")
    if not all(isinstance(value, str) and value for value in (license_name, notice_name)):
        raise StagingError("legal policy archive names must not be blank")
    allowed_licenses = legal_policy.get("allowed_licenses", [])
    required_fields = legal_policy.get("required_metadata_fields", [])
    if (
        not isinstance(allowed_licenses, list)
        or not allowed_licenses
        or any(not isinstance(value, str) or not value for value in allowed_licenses)
    ):
        raise StagingError("legal policy allowed_licenses must be a non-empty string list")
    if not isinstance(required_fields, list) or any(
        not isinstance(value, str) or not value for value in required_fields
    ):
        raise StagingError("legal policy required_metadata_fields must be a string list")
    metadata = _metadata(workspace_root)
    metadata_packages = metadata.get("packages")
    workspace_members = metadata.get("workspace_members")
    if not isinstance(metadata_packages, list) or not isinstance(workspace_members, list):
        raise StagingError("cargo metadata has no workspace package inventory")
    by_id = {
        package.get("id"): package
        for package in metadata_packages
        if isinstance(package, dict) and isinstance(package.get("id"), str)
    }
    workspace_packages = {
        package["name"]: package
        for package_id in workspace_members
        if isinstance((package := by_id.get(package_id)), dict)
        and isinstance(package.get("name"), str)
    }
    selected: set[str] = set()
    normalized: list[tuple[str, str, Path]] = []
    for item in packages:
        name = item.get("name")
        version = item.get("version")
        manifest = item.get("manifest")
        if not all(isinstance(value, str) and value for value in (name, version, manifest)):
            raise StagingError("package staging record is incomplete")
        if name in selected:
            raise StagingError(f"package staging record is duplicated: {name}")
        metadata_package = workspace_packages.get(name)
        if not isinstance(metadata_package, dict) or metadata_package.get("version") != version:
            raise StagingError(f"package staging record does not match Cargo metadata: {name}")
        if metadata_package.get("license") not in allowed_licenses:
            raise StagingError(f"package {name} has an unapproved license expression")
        missing_fields = [field for field in required_fields if not metadata_package.get(field)]
        if missing_fields:
            raise StagingError(
                f"package {name} is missing required metadata: {', '.join(missing_fields)}"
            )
        manifest_path = (workspace_root / manifest).resolve()
        try:
            manifest_path.relative_to(workspace_root)
        except ValueError as error:
            raise StagingError(f"manifest for {name} escapes the workspace") from error
        if manifest_path != Path(str(metadata_package.get("manifest_path"))).resolve():
            raise StagingError(f"manifest for {name} does not match Cargo metadata")
        selected.add(name)
        normalized.append((name, version, manifest_path))

    cargo_target = candidate_root / "cargo-package-target"
    excluded = sorted(set(workspace_packages) - selected)
    command = [
        "cargo",
        "package",
        "--workspace",
        "--locked",
        "--offline",
        "--allow-dirty",
        "--no-verify",
        "--manifest-path",
        str(workspace_root / "Cargo.toml"),
        "--target-dir",
        str(cargo_target),
    ]
    for name in excluded:
        command.extend(["--exclude", name])
    _run(command, cwd=workspace_root)
    records: list[dict[str, Any]] = []
    for package_name, version, manifest_path in normalized:
        generated = cargo_target / "package" / f"{package_name}-{version}.crate"
        if not generated.is_file():
            raise StagingError(f"cargo did not generate the expected crate: {generated}")
        output = candidate_root / "crate-packages" / generated.name
        _repack_with_legal(
            generated,
            output,
            package_name=package_name,
            version=version,
            legal_files=[(license_name, license_source), (notice_name, notice_source)],
        )
        _verify_staged_archive(
            output,
            package_name=package_name,
            version=version,
            legal_names=(license_name, notice_name),
        )
        records.append(
            {
                "name": package_name,
                "version": version,
                "manifest": str(manifest_path),
                "crate_path": str(output),
                "command": command,
                "exit_code": 0,
                "legal_files": [license_name, notice_name],
            }
        )
    return records


def stage_crate(
    workspace_root: Path,
    candidate_root: Path,
    *,
    package_name: str,
    version: str,
    manifest_path: Path,
    legal_policy: dict[str, Any],
) -> dict[str, Any]:
    """Stage a package that has no unpublished workspace dependency."""
    manifest = manifest_path.resolve().relative_to(workspace_root.resolve()).as_posix()
    return stage_workspace_crates(
        workspace_root,
        candidate_root,
        packages=[{"name": package_name, "version": version, "manifest": manifest}],
        legal_policy=legal_policy,
    )[0]


def _index_path(index_root: Path, package_name: str) -> Path:
    lower = package_name.lower()
    if len(lower) == 1:
        relative = Path("1") / lower
    elif len(lower) == 2:
        relative = Path("2") / lower
    elif len(lower) == 3:
        relative = Path("3") / lower[0] / lower
    else:
        relative = Path(lower[:2]) / lower[2:4] / lower
    return index_root / relative


def _index_dependencies(package: dict[str, Any], staged_names: set[str]) -> list[dict[str, Any]]:
    dependencies = package.get("dependencies", [])
    if not isinstance(dependencies, list):
        raise StagingError(f"metadata dependencies are invalid for {package.get('name')}")
    rendered: list[dict[str, Any]] = []
    for dependency in dependencies:
        if not isinstance(dependency, dict):
            raise StagingError(f"metadata dependency is invalid for {package.get('name')}")
        actual_name = dependency.get("name")
        if not isinstance(actual_name, str):
            raise StagingError(f"metadata dependency has no name for {package.get('name')}")
        alias = dependency.get("rename")
        rendered.append(
            {
                "name": alias or actual_name,
                "req": dependency.get("req", "*"),
                "features": dependency.get("features", []),
                "optional": bool(dependency.get("optional", False)),
                "default_features": bool(dependency.get("uses_default_features", True)),
                "target": dependency.get("target"),
                "kind": dependency.get("kind") or "normal",
                "registry": (
                    None
                    if actual_name in staged_names
                    else dependency.get("registry") or CRATES_IO_INDEX
                ),
                "package": actual_name if alias else None,
            }
        )
    return rendered


def _split_features(package: dict[str, Any]) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    raw = package.get("features", {})
    if not isinstance(raw, dict):
        raise StagingError(f"metadata features are invalid for {package.get('name')}")
    basic: dict[str, list[str]] = {}
    extended: dict[str, list[str]] = {}
    for name, values in raw.items():
        if not isinstance(name, str) or not isinstance(values, list) or any(
            not isinstance(value, str) for value in values
        ):
            raise StagingError(f"metadata feature is invalid for {package.get('name')}")
        target = (
            extended
            if any(value.startswith("dep:") or "?/" in value for value in values)
            else basic
        )
        target[name] = values
    return basic, extended


def create_local_registry(
    registry_root: Path,
    staged_packages: list[dict[str, Any]],
    metadata: dict[str, Any],
) -> Path:
    registry_root = registry_root.resolve()
    if registry_root.exists():
        raise StagingError(f"local registry already exists: {registry_root}")
    registry_root.mkdir(parents=True)
    index_root = registry_root / "index"
    index_root.mkdir()
    packages = metadata.get("packages")
    if not isinstance(packages, list):
        raise StagingError("cargo metadata has no packages")
    by_name = {
        package.get("name"): package
        for package in packages
        if isinstance(package, dict) and isinstance(package.get("name"), str)
    }
    staged_names = {str(item.get("name")) for item in staged_packages}
    if len(staged_names) != len(staged_packages):
        raise StagingError("staged package names are duplicated")
    for staged in staged_packages:
        name = staged.get("name")
        version = staged.get("version")
        crate_path = Path(str(staged.get("crate_path"))).resolve()
        package = by_name.get(name)
        if (
            not isinstance(name, str)
            or not isinstance(version, str)
            or not isinstance(package, dict)
        ):
            raise StagingError("staged package does not match Cargo metadata")
        if package.get("version") != version or not crate_path.is_file():
            raise StagingError(f"staged package identity is invalid for {name}")
        destination = registry_root / crate_path.name
        if destination.exists():
            raise StagingError(f"local registry crate is duplicated: {destination.name}")
        shutil.copyfile(crate_path, destination)
        basic_features, extended_features = _split_features(package)
        entry: dict[str, Any] = {
            "name": name,
            "vers": version,
            "deps": _index_dependencies(package, staged_names),
            # Cargo requires this field to read any registry index. It is protocol
            # metadata only and is never used as a release or handoff gate.
            "cksum": hashlib.sha256(destination.read_bytes()).hexdigest(),
            "features": basic_features,
            "yanked": False,
            "links": package.get("links"),
        }
        if extended_features:
            entry["features2"] = extended_features
            entry["v"] = 2
        if package.get("rust_version") is not None:
            entry["rust_version"] = package["rust_version"]
        index_path = _index_path(index_root, name)
        index_path.parent.mkdir(parents=True, exist_ok=True)
        index_path.write_text(json.dumps(entry, separators=(",", ":")) + "\n", encoding="utf-8")
    config = {"dl": registry_root.as_uri() + "/{crate}-{version}.crate"}
    (index_root / "config.json").write_text(
        json.dumps(config, indent=2) + "\n", encoding="utf-8", newline="\n"
    )
    _run(["git", "init", "--quiet"], cwd=index_root)
    _run(["git", "config", "user.name", "RocketMQ Rust Release Planner"], cwd=index_root)
    _run(["git", "config", "user.email", "release-planner@localhost"], cwd=index_root)
    _run(["git", "add", "."], cwd=index_root)
    _run(["git", "commit", "--quiet", "-m", "local registry index"], cwd=index_root)
    return index_root


def verify_local_registry(
    registry_root: Path,
    *,
    package_name: str,
    version: str,
    work_root: Path,
) -> dict[str, Any]:
    registry_root = registry_root.resolve()
    index_root = registry_root / "index"
    if not (index_root / "config.json").is_file():
        raise StagingError("local registry index is incomplete")
    work_root = work_root.resolve()
    if work_root.exists():
        raise StagingError(f"local registry consumer root already exists: {work_root}")
    (work_root / ".cargo").mkdir(parents=True)
    (work_root / "src").mkdir()
    (work_root / ".cargo" / "config.toml").write_text(
        f'[registries.local-temp]\nindex = "{index_root.as_uri()}"\n',
        encoding="utf-8",
        newline="\n",
    )
    (work_root / "Cargo.toml").write_text(
        "[package]\n"
        'name = "rocketmq-release-package-check"\n'
        'version = "0.0.0"\n'
        'edition = "2021"\n\n'
        "[workspace]\n\n"
        "[dependencies]\n"
        f'candidate = {{ package = "{package_name}", version = "={version}", '
        'registry = "local-temp", default-features = false }\n',
        encoding="utf-8",
        newline="\n",
    )
    (work_root / "src" / "lib.rs").write_text(
        "pub fn local_registry_resolved() -> bool { true }\n",
        encoding="utf-8",
        newline="\n",
    )
    _run(["cargo", "generate-lockfile"], cwd=work_root)
    command = [
        "cargo",
        "check",
        "--locked",
        "--target-dir",
        str(work_root / "target"),
    ]
    _run(command, cwd=work_root)
    return {"status": "passed", "package": package_name, "version": version, "command": command}
