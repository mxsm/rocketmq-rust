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

"""Validate the active architecture release topology without legacy resources."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
import json
import re
import sys
import tomllib
from pathlib import Path
from typing import Any

import core_release_scope


ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = ROOT / "scripts" / "architecture-release-plan.json"
POLICY_PATH = ROOT / "scripts" / "architecture-dependency-policy.json"
BASELINE_PATH = ROOT / "scripts" / "architecture-dependency-baseline.json"
CI_PATH = ROOT / ".github" / "workflows" / "rocketmq-rust-ci.yaml"
VALIDATION_INVENTORY_PATH = ROOT / "scripts" / "architecture-validation-inventory.json"
EDGE_FIELDS = ("caller", "target", "kind", "path", "alias")


@dataclass(frozen=True)
class Finding:
    code: str
    path: str
    detail: str

    def render(self) -> str:
        return f"RELEASE_FINDING code={self.code} path={self.path} detail={self.detail}"


@dataclass(frozen=True)
class ReleaseInventory:
    root_members: tuple[str, ...]
    standalone_projects: tuple[str, ...]
    governance_targets: tuple[str, ...]


def load_json(path: Path, label: str, findings: list[Finding]) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        findings.append(Finding("input-invalid", path.as_posix(), f"{label}: {error}"))
        return None
    if not isinstance(value, dict):
        findings.append(Finding("input-invalid", path.as_posix(), f"{label} must be a JSON object"))
        return None
    return value


def normalized_relative_path(value: object) -> str | None:
    if (
        not isinstance(value, str)
        or not value
        or "\\" in value
        or Path(value).is_absolute()
        or ".." in Path(value).parts
    ):
        return None
    return value


def read_toml(path: Path, label: str, findings: list[Finding]) -> dict[str, Any] | None:
    try:
        value = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        findings.append(Finding("manifest-invalid", path.as_posix(), f"{label}: {error}"))
        return None
    if not isinstance(value, dict):
        findings.append(Finding("manifest-invalid", path.as_posix(), f"{label} must be a TOML table"))
        return None
    return value


def workspace_manifests(root: Path, findings: list[Finding]) -> dict[str, str]:
    root_manifest_path = root / "Cargo.toml"
    root_manifest = read_toml(root_manifest_path, "workspace manifest", findings)
    if root_manifest is None:
        return {}
    members = root_manifest.get("workspace", {}).get("members")
    if not isinstance(members, list) or any(not isinstance(item, str) for item in members):
        findings.append(
            Finding(
                "manifest-section-missing",
                "Cargo.toml",
                "workspace.members must be a list of manifest directories",
            )
        )
        return {}

    packages: dict[str, str] = {}
    for member in members:
        relative = normalized_relative_path(member)
        if relative is None:
            findings.append(Finding("manifest-path-invalid", "Cargo.toml", f"workspace member={member!r}"))
            continue
        manifest_path = root / relative / "Cargo.toml"
        manifest = read_toml(manifest_path, "package manifest", findings)
        if manifest is None:
            continue
        package = manifest.get("package")
        name = package.get("name") if isinstance(package, dict) else None
        if not isinstance(name, str) or not name:
            findings.append(
                Finding(
                    "manifest-section-missing",
                    manifest_path.relative_to(root).as_posix(),
                    "package.name is required",
                )
            )
            continue
        if name in packages:
            findings.append(
                Finding(
                    "package-duplicate",
                    manifest_path.relative_to(root).as_posix(),
                    f"package={name}",
                )
            )
            continue
        packages[name] = f"{relative}/Cargo.toml"
    return packages


def discover_release_inventory(root: Path, findings: list[Finding]) -> ReleaseInventory:
    root_packages = workspace_manifests(root, findings)
    inventory_path = root / VALIDATION_INVENTORY_PATH.relative_to(ROOT)
    inventory = load_json(inventory_path, "architecture validation inventory", findings)
    standalone_paths: list[str] = []
    standalone_packages: set[str] = set()
    if inventory is not None:
        standalone = inventory.get("standalone")
        if not isinstance(standalone, list):
            findings.append(
                Finding(
                    "inventory-section-missing",
                    inventory_path.relative_to(root).as_posix(),
                    "standalone must be a list",
                )
            )
        else:
            for index, project in enumerate(standalone):
                manifest_value = project.get("manifest") if isinstance(project, dict) else None
                relative = normalized_relative_path(manifest_value)
                if relative is None or not relative.endswith("Cargo.toml"):
                    findings.append(
                        Finding(
                            "manifest-path-invalid",
                            inventory_path.relative_to(root).as_posix(),
                            f"standalone[{index}].manifest={manifest_value!r}",
                        )
                    )
                    continue
                standalone_paths.append(relative)
                manifest = read_toml(root / relative, "standalone manifest", findings)
                if manifest is None:
                    continue
                package = manifest.get("package")
                name = package.get("name") if isinstance(package, dict) else None
                if isinstance(name, str) and name:
                    if name in root_packages or name in standalone_packages:
                        findings.append(Finding("package-duplicate", relative, f"package={name}"))
                    standalone_packages.add(name)
                elif not isinstance(manifest.get("workspace"), dict):
                    findings.append(Finding("manifest-section-missing", relative, "package.name or workspace is required"))

    return ReleaseInventory(
        root_members=tuple(sorted(root_packages)),
        standalone_projects=tuple(sorted(standalone_paths)),
        governance_targets=tuple(sorted(set(root_packages) | standalone_packages)),
    )


def edge_identity(edge: dict[str, Any]) -> tuple[str, str, str, str, str] | None:
    if not all(isinstance(edge.get(field), str) and edge[field] for field in EDGE_FIELDS):
        return None
    return tuple(edge[field] for field in EDGE_FIELDS)  # type: ignore[return-value]


def manifest_has_edge(edge: dict[str, Any], root: Path, findings: list[Finding]) -> None:
    identity = edge_identity(edge)
    if identity is None:
        findings.append(Finding("edge-schema-invalid", "release-plan", f"edge={edge!r}"))
        return
    _caller, target, kind, relative_path, alias = identity
    normalized = normalized_relative_path(relative_path)
    if normalized is None or not normalized.endswith("Cargo.toml"):
        findings.append(Finding("manifest-path-invalid", relative_path, "edge path must identify Cargo.toml"))
        return
    manifest = read_toml(root / normalized, "edge manifest", findings)
    if manifest is None:
        return
    section_name = {
        "normal": "dependencies",
        "dev": "dev-dependencies",
        "build": "build-dependencies",
    }.get(kind)
    if section_name is None:
        findings.append(Finding("edge-kind-invalid", normalized, f"kind={kind}"))
        return
    section = manifest.get(section_name)
    if not isinstance(section, dict):
        findings.append(
            Finding("manifest-section-missing", normalized, f"section={section_name}")
        )
        return
    manifest_key = alias if alias in section else target
    specification = section.get(manifest_key)
    if specification is None:
        findings.append(
            Finding(
                "dependency-missing",
                normalized,
                f"section={section_name} alias={alias} target={target}",
            )
        )
        return
    package = specification.get("package") if isinstance(specification, dict) else None
    actual_target = package or manifest_key.replace("_", "-")
    if actual_target != target:
        findings.append(
            Finding(
                "dependency-target-mismatch",
                normalized,
                f"alias={alias} expected={target} actual={actual_target}",
            )
        )


def validate_design_source(plan: dict[str, Any], root: Path, findings: list[Finding]) -> None:
    source = plan.get("design_source")
    if not isinstance(source, str) or "#" not in source:
        findings.append(Finding("design-source-invalid", "release-plan", "path#section is required"))
        return
    relative, fragment = source.split("#", 1)
    normalized = normalized_relative_path(relative)
    if normalized is None or not normalized.startswith("rocketmq-doc/en/") or not fragment:
        findings.append(
            Finding("design-source-invalid", str(source), "source must be a rocketmq-doc/en path#section")
        )
        return
    path = root / normalized
    try:
        content = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        findings.append(Finding("design-source-missing", normalized, str(error)))
        return
    headings = re.findall(r"^#{1,6}\s+(.+?)\s*$", content, re.MULTILINE)
    slugs = {
        re.sub(r"-+", "-", re.sub(r"[^a-z0-9 -]", "", heading.lower()).replace(" ", "-")).strip("-")
        for heading in headings
    }
    if fragment not in slugs:
        findings.append(Finding("design-section-missing", normalized, f"section={fragment}"))


def validate_release_topology(
    plan: dict[str, Any],
    policy: dict[str, Any],
    inventory: ReleaseInventory,
    root: Path,
    findings: list[Finding],
) -> None:
    topology = plan.get("release_topology")
    if not isinstance(topology, dict):
        findings.append(Finding("plan-section-missing", "release-plan", "release_topology"))
        return
    order = topology.get("publish_order")
    if not isinstance(order, list) or any(not isinstance(item, str) for item in order):
        findings.append(Finding("publish-order-invalid", "release-plan", "publish_order must be a string list"))
        return
    if len(order) != len(set(order)):
        findings.append(Finding("publish-order-duplicate", "release-plan", "duplicate package"))

    target_dag = policy.get("target_dag")
    if not isinstance(target_dag, dict):
        findings.append(Finding("policy-section-missing", "dependency-policy", "target_dag"))
        return
    expected = set(target_dag)
    actual = set(order)
    if actual != expected:
        findings.append(
            Finding(
                "publish-package-mismatch",
                "release-plan",
                f"missing={sorted(expected - actual)} extra={sorted(actual - expected)}",
            )
        )
    available = set(inventory.governance_targets)
    root_members = set(inventory.root_members)
    missing = expected - available
    extra_root_members = root_members - expected
    if missing or extra_root_members:
        findings.append(
            Finding(
                "release-inventory-mismatch",
                "scripts/architecture-validation-inventory.json",
                f"missing={sorted(missing)} extra_root_members={sorted(extra_root_members)}",
            )
        )

    dependencies: dict[str, set[str]] = {}
    for caller, values in target_dag.items():
        if not isinstance(values, list) or any(not isinstance(item, str) for item in values):
            findings.append(Finding("target-dag-invalid", "dependency-policy", f"caller={caller}"))
            continue
        dependencies[caller] = set(values)
    target_debt = policy.get("target_debt", {}).get("entries", [])
    if not isinstance(target_debt, list):
        findings.append(Finding("policy-section-missing", "dependency-policy", "target_debt.entries"))
        target_debt = []
    for edge in target_debt:
        if not isinstance(edge, dict):
            findings.append(Finding("edge-schema-invalid", "dependency-policy", f"edge={edge!r}"))
            continue
        identity = edge_identity(edge)
        if identity is None:
            findings.append(Finding("edge-schema-invalid", "dependency-policy", f"edge={edge!r}"))
            continue
        caller, target, _kind, _path, _alias = identity
        remove_by = edge.get("remove_by")
        try:
            expired = not isinstance(remove_by, str) or date.fromisoformat(remove_by) < date.today()
        except ValueError:
            expired = True
        if expired:
            findings.append(
                Finding("target-debt-expired", edge["path"], f"caller={caller} target={target}")
            )
        dependencies.setdefault(caller, set()).add(target)
        manifest_has_edge(edge, root, findings)

    position = {package: index for index, package in enumerate(order)}
    for caller, targets in dependencies.items():
        for target in targets:
            if caller not in position or target not in position:
                continue
            if position[target] >= position[caller]:
                findings.append(
                    Finding(
                        "publish-order-violation",
                        "release-plan",
                        f"dependency={target} caller={caller}",
                    )
                )


def validate_compatibility_windows(
    plan: dict[str, Any],
    baseline: dict[str, Any],
    root: Path,
    findings: list[Finding],
) -> None:
    windows = plan.get("compatibility_windows")
    if not isinstance(windows, dict):
        findings.append(Finding("plan-section-missing", "release-plan", "compatibility_windows"))
        return
    edges = windows.get("preserved_edges")
    if not isinstance(edges, list) or any(not isinstance(edge, dict) for edge in edges):
        findings.append(Finding("edge-schema-invalid", "release-plan", "preserved_edges"))
        return
    baseline_edges = baseline.get("compatibility_manifest_exceptions")
    if not isinstance(baseline_edges, list):
        findings.append(
            Finding("baseline-section-missing", "dependency-baseline", "compatibility_manifest_exceptions")
        )
        return
    expected = {
        (*identity, edge.get("remove_by"))
        for edge in baseline_edges
        if isinstance(edge, dict) and (identity := edge_identity(edge)) is not None
    }
    actual = {
        (*identity, edge.get("remove_by"))
        for edge in edges
        if (identity := edge_identity(edge)) is not None
    }
    if actual != expected or len(actual) != len(edges):
        findings.append(
            Finding(
                "compatibility-window-mismatch",
                "release-plan",
                f"expected={len(expected)} actual={len(actual)}",
            )
        )
    for edge in edges:
        manifest_has_edge(edge, root, findings)


def validate_transition_contract(plan: dict[str, Any], findings: list[Finding]) -> None:
    expected = {
        "source": "scripts/architecture-dependency-policy.json#target_debt",
        "required_mode": "transition",
        "strict_target_required_when_empty": True,
    }
    if plan.get("transition_debt") != expected:
        findings.append(
            Finding("transition-contract-invalid", "release-plan", "transition_debt contract drifted")
        )


def validate_scope_reporting(plan: dict[str, Any], findings: list[Finding]) -> None:
    expected = {
        "core_scope": "scripts/core-release-scope.json",
        "core_command": "python scripts/architecture_release_guard.py --scope core-release",
        "repo_global_command": "python scripts/architecture_release_guard.py --scope repo-global",
        "release_decision_scope": "core-release",
    }
    if plan.get("scope_reporting") != expected:
        findings.append(
            Finding("scope-reporting-invalid", "release-plan", "core/repo-global contract drifted")
        )


def validate_ci(root: Path, findings: list[Finding]) -> None:
    try:
        workflow = (root / CI_PATH.relative_to(ROOT)).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        findings.append(Finding("ci-workflow-missing", CI_PATH.as_posix(), str(error)))
        return
    for command in (
        "python scripts/architecture_dependency_guard.py --mode baseline",
        "python scripts/architecture_dependency_guard.py --mode transition",
        "python scripts/architecture_dependency_guard.py --mode target",
        "python scripts/architecture_release_guard.py",
    ):
        if command not in workflow:
            findings.append(Finding("ci-command-missing", CI_PATH.as_posix(), command))


def validate(
    plan: dict[str, Any],
    policy: dict[str, Any],
    baseline: dict[str, Any],
    *,
    root: Path = ROOT,
    check_ci: bool = True,
) -> list[Finding]:
    findings: list[Finding] = []
    if plan.get("schema_version") != 2 or plan.get("milestone") != "P0":
        findings.append(Finding("plan-schema-invalid", "release-plan", "expected schema_version=2 milestone=P0"))
    validate_design_source(plan, root, findings)
    inventory = discover_release_inventory(root, findings)
    validate_release_topology(plan, policy, inventory, root, findings)
    validate_compatibility_windows(plan, baseline, root, findings)
    validate_transition_contract(plan, findings)
    validate_scope_reporting(plan, findings)
    if check_ci:
        validate_ci(root, findings)
    return sorted(findings, key=Finding.render)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scope", choices=("core-release", "repo-global", "all"), default="all")
    args = parser.parse_args()

    if args.scope in {"core-release", "all"}:
        try:
            scope, scope_findings = core_release_scope.validate_repository()
        except core_release_scope.ScopeInputError as error:
            print(f"ARCHITECTURE_RELEASE_CORE_FAILED findings=1 detail={error}")
            if args.scope == "core-release":
                return 1
            scope_findings = []
            core_failed = True
        else:
            core_only = [item for item in scope_findings if item.scope == "core"]
            core_failed = bool(core_only)
            if core_failed:
                print(f"ARCHITECTURE_RELEASE_CORE_FAILED findings={len(core_only)}")
                for finding in core_only:
                    print(finding.render())
            else:
                print(f"ARCHITECTURE_RELEASE_CORE_OK packages={len(core_release_scope.core_packages(scope))}")
        if args.scope == "core-release":
            return int(core_failed)

    findings: list[Finding] = []
    plan = load_json(PLAN_PATH, "release plan", findings)
    policy = load_json(POLICY_PATH, "dependency policy", findings)
    baseline = load_json(BASELINE_PATH, "dependency baseline", findings)
    if plan is not None and policy is not None and baseline is not None:
        findings.extend(validate(plan, policy, baseline))
    findings = sorted(findings, key=Finding.render)
    if findings:
        print(f"ARCHITECTURE_RELEASE_REPO_GLOBAL_FAILED findings={len(findings)}")
        for finding in findings:
            print(finding.render())
        return 1
    print("ARCHITECTURE_RELEASE_REPO_GLOBAL_OK packages=29 standalone=7 transition_debt=tracked")
    if args.scope == "all":
        if core_failed:
            return 1
        print("ARCHITECTURE_RELEASE_GUARD_OK core=27 repo_global_packages=29 standalone=7")
    return 0


if __name__ == "__main__":
    sys.exit(main())
