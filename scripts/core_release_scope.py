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

"""Load and validate the package boundary used by core release tooling."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path
import subprocess
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
SCOPE_PATH = ROOT / "scripts" / "core-release-scope.json"
CLASSIFICATIONS = frozenset(
    {"registry-publish", "internal-only", "binary-only", "non-publish"}
)
FORBIDDEN_CORE_NAMES = frozenset({"rocketmq-dashboard-common", "rocketmq-mcp", "rocketmq-sre"})
FORBIDDEN_CORE_PATH_PARTS = frozenset({"rocketmq-dashboard", "rocketmq-sre", "rocketmq-mcp"})


class ScopeInputError(ValueError):
    """Raised when the scope or Cargo metadata cannot be parsed."""


@dataclass(frozen=True, order=True)
class ScopeFinding:
    scope: str
    code: str
    path: str
    detail: str

    def as_dict(self) -> dict[str, str]:
        return asdict(self)

    def render(self) -> str:
        return (
            f"CORE_SCOPE_FINDING scope={self.scope} code={self.code} "
            f"path={self.path} detail={self.detail}"
        )


def load_scope(path: Path = SCOPE_PATH) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ScopeInputError(f"cannot load {path}: {error}") from error
    if not isinstance(value, dict):
        raise ScopeInputError(f"{path} must contain a JSON object")
    return value


def core_packages(scope: dict[str, Any] | None = None) -> tuple[dict[str, Any], ...]:
    value = (scope or load_scope()).get("core_packages")
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise ScopeInputError("core_packages must be a list of objects")
    return tuple(value)


def excluded_projects(scope: dict[str, Any] | None = None) -> tuple[dict[str, Any], ...]:
    loaded = scope or load_scope()
    values: list[dict[str, Any]] = []
    for field in ("workspace_exclusions", "repository_exclusions"):
        entries = loaded.get(field)
        if not isinstance(entries, list) or any(not isinstance(item, dict) for item in entries):
            raise ScopeInputError(f"{field} must be a list of objects")
        values.extend(entries)
    return tuple(values)


def collect_metadata(root: Path = ROOT) -> dict[str, Any]:
    completed = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1", "--locked"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise ScopeInputError(f"cargo metadata failed: {completed.stderr.strip()}")
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise ScopeInputError(f"cargo metadata returned invalid JSON: {error}") from error
    if not isinstance(value, dict):
        raise ScopeInputError("cargo metadata must return a JSON object")
    return value


def _normalized_relative_path(value: object) -> str | None:
    if not isinstance(value, str) or not value or "\\" in value:
        return None
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        return None
    return path.as_posix().rstrip("/")


def _entries(scope: dict[str, Any], field: str, findings: list[ScopeFinding]) -> list[dict[str, Any]]:
    value = scope.get(field)
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        findings.append(ScopeFinding("core", "scope-section-invalid", "scripts/core-release-scope.json", field))
        return []
    return value


def _validate_paths_and_duplicates(
    entries: Iterable[dict[str, Any]],
    *,
    root: Path,
    finding_scope: str,
    classification_required: bool,
    findings: list[ScopeFinding],
) -> tuple[set[str], set[str]]:
    names: set[str] = set()
    paths: set[str] = set()
    for index, entry in enumerate(entries):
        name = entry.get("name")
        path = _normalized_relative_path(entry.get("path"))
        label = path or f"entry[{index}]"
        if not isinstance(name, str) or not name or path is None:
            findings.append(ScopeFinding(finding_scope, "scope-entry-invalid", label, f"entry={entry!r}"))
            continue
        if name in names or path in paths:
            findings.append(ScopeFinding(finding_scope, "scope-package-duplicate", path, f"package={name}"))
        names.add(name)
        paths.add(path)
        if not (root / path).exists():
            findings.append(ScopeFinding(finding_scope, "scope-path-missing", path, f"package={name}"))
        if classification_required and entry.get("classification") not in CLASSIFICATIONS:
            findings.append(
                ScopeFinding(
                    finding_scope,
                    "scope-classification-invalid",
                    path,
                    f"package={name} classification={entry.get('classification')!r}",
                )
            )
    return names, paths


def _workspace_packages(metadata: dict[str, Any], root: Path, findings: list[ScopeFinding]) -> dict[str, str]:
    packages = metadata.get("packages")
    members = metadata.get("workspace_members")
    if not isinstance(packages, list) or not isinstance(members, list):
        findings.append(ScopeFinding("core", "metadata-invalid", "cargo metadata", "packages/workspace_members"))
        return {}
    by_id = {item.get("id"): item for item in packages if isinstance(item, dict)}
    result: dict[str, str] = {}
    root_resolved = root.resolve()
    for package_id in members:
        package = by_id.get(package_id)
        if not isinstance(package, dict):
            findings.append(ScopeFinding("core", "metadata-member-missing", "cargo metadata", str(package_id)))
            continue
        name = package.get("name")
        manifest = package.get("manifest_path")
        if not isinstance(name, str) or not isinstance(manifest, str):
            findings.append(ScopeFinding("core", "metadata-package-invalid", "cargo metadata", str(package_id)))
            continue
        try:
            relative = Path(manifest).resolve().parent.relative_to(root_resolved).as_posix()
        except ValueError:
            findings.append(ScopeFinding("core", "metadata-path-outside-root", manifest, f"package={name}"))
            continue
        result[name] = relative
    return result


def validate_metadata(
    scope: dict[str, Any],
    metadata: dict[str, Any],
    *,
    root: Path = ROOT,
) -> list[ScopeFinding]:
    findings: list[ScopeFinding] = []
    if scope.get("schema_version") != 1 or scope.get("scope_name") != "core-release":
        findings.append(
            ScopeFinding(
                "core",
                "scope-schema-invalid",
                "scripts/core-release-scope.json",
                "expected schema_version=1 scope_name=core-release",
            )
        )
    declared_classifications = scope.get("allowed_classifications")
    if (
        not isinstance(declared_classifications, list)
        or len(declared_classifications) != len(CLASSIFICATIONS)
        or any(not isinstance(item, str) for item in declared_classifications)
        or set(declared_classifications) != CLASSIFICATIONS
    ):
        findings.append(
            ScopeFinding(
                "core",
                "scope-classifications-invalid",
                "scripts/core-release-scope.json",
                "classification set drifted",
            )
        )

    core = _entries(scope, "core_packages", findings)
    workspace_exclusions = _entries(scope, "workspace_exclusions", findings)
    repository_exclusions = _entries(scope, "repository_exclusions", findings)
    core_names, core_paths = _validate_paths_and_duplicates(
        core,
        root=root,
        finding_scope="core",
        classification_required=True,
        findings=findings,
    )
    excluded_names, excluded_paths = _validate_paths_and_duplicates(
        workspace_exclusions,
        root=root,
        finding_scope="core",
        classification_required=False,
        findings=findings,
    )
    _validate_paths_and_duplicates(
        repository_exclusions,
        root=root,
        finding_scope="repo-global",
        classification_required=False,
        findings=findings,
    )

    if len(core) != 27:
        findings.append(
            ScopeFinding(
                "core",
                "core-package-count",
                "scripts/core-release-scope.json",
                f"expected=27 actual={len(core)}",
            )
        )
    if core_names & excluded_names or core_paths & excluded_paths:
        findings.append(
            ScopeFinding(
                "core",
                "excluded-project-in-core",
                "scripts/core-release-scope.json",
                "workspace exclusion overlaps core",
            )
        )
    for entry in core:
        name = entry.get("name")
        path = _normalized_relative_path(entry.get("path"))
        parts = set(Path(path).parts) if path is not None else set()
        if name in FORBIDDEN_CORE_NAMES or parts & FORBIDDEN_CORE_PATH_PARTS:
            findings.append(
                ScopeFinding(
                    "core",
                    "excluded-project-in-core",
                    path or "core_packages",
                    f"package={name}",
                )
            )

    workspace = _workspace_packages(metadata, root, findings)
    classified = core_names | excluded_names
    for name in sorted(set(workspace) - classified):
        findings.append(ScopeFinding("core", "workspace-package-unclassified", workspace[name], f"package={name}"))
    for name in sorted(classified - set(workspace)):
        findings.append(
            ScopeFinding(
                "core",
                "scope-package-not-in-workspace",
                "scripts/core-release-scope.json",
                f"package={name}",
            )
        )
    scoped_paths = {
        entry["name"]: _normalized_relative_path(entry.get("path"))
        for entry in (*core, *workspace_exclusions)
        if isinstance(entry.get("name"), str)
    }
    for name in sorted(set(workspace) & classified):
        if scoped_paths.get(name) != workspace[name]:
            findings.append(
                ScopeFinding(
                    "core",
                    "workspace-package-path-mismatch",
                    workspace[name],
                    f"package={name} scope_path={scoped_paths.get(name)!r}",
                )
            )

    services = scope.get("core_services")
    expected_services = {"rocketmq-namesrv", "rocketmq-broker", "rocketmq-controller", "rocketmq-proxy"}
    if not isinstance(services, list) or set(services) != expected_services or not set(services) <= core_names:
        findings.append(
            ScopeFinding(
                "core",
                "core-services-invalid",
                "scripts/core-release-scope.json",
                "four core services are required",
            )
        )
    return sorted(set(findings))


def validate_repository(
    *, root: Path = ROOT, scope_path: Path = SCOPE_PATH
) -> tuple[dict[str, Any], list[ScopeFinding]]:
    scope = load_scope(scope_path)
    return scope, validate_metadata(scope, collect_metadata(root), root=root)
