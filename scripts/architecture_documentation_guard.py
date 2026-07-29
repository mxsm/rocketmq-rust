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

"""Generate and validate architecture inventory and release-evidence documentation."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import re
import sys
import tomllib
from typing import Any, Iterable


DEFAULT_ROOT = Path(__file__).resolve().parents[1]
POLICY_RELATIVE = Path("scripts/architecture-validation-inventory.json")


@dataclass(frozen=True)
class Finding:
    code: str
    path: str
    detail: str

    def render(self) -> str:
        return f"DOCUMENTATION_FINDING code={self.code} path={self.path} detail={self.detail}"


@dataclass(frozen=True)
class Package:
    name: str
    path: str


@dataclass(frozen=True)
class TokioDeclaration:
    manifest: str
    dependency: str
    features: tuple[str, ...]
    inherited: bool


@dataclass(frozen=True)
class LocalEdge:
    consumer: str
    dependency: str
    target: str


@dataclass(frozen=True)
class Facts:
    formal_toolchain: str
    root_packages: tuple[Package, ...]
    standalone: tuple[dict[str, Any], ...]
    node_projects: tuple[dict[str, Any], ...]
    tokio: tuple[TokioDeclaration, ...]
    local_edges: tuple[LocalEdge, ...]
    evidence_artifacts: tuple[dict[str, str], ...]


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain an object")
    return value


def load_toml(path: Path) -> dict[str, Any]:
    value = tomllib.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a TOML table")
    return value


def normalized(path: Path) -> str:
    return path.as_posix().removeprefix("./")


def dependency_tables(manifest: dict[str, Any]) -> Iterable[dict[str, Any]]:
    for key in ("dependencies", "dev-dependencies", "build-dependencies"):
        table = manifest.get(key, {})
        if isinstance(table, dict):
            yield table
    target = manifest.get("target", {})
    if isinstance(target, dict):
        for config in target.values():
            if not isinstance(config, dict):
                continue
            for key in ("dependencies", "dev-dependencies", "build-dependencies"):
                table = config.get(key, {})
                if isinstance(table, dict):
                    yield table


def package_name(manifest: dict[str, Any], path: Path) -> str:
    package = manifest.get("package", {})
    name = package.get("name") if isinstance(package, dict) else None
    if not isinstance(name, str) or not name:
        raise ValueError(f"{path} has no package.name")
    return name


def collect_root_packages(root: Path, root_manifest: dict[str, Any]) -> tuple[Package, ...]:
    workspace = root_manifest.get("workspace", {})
    members = workspace.get("members", []) if isinstance(workspace, dict) else []
    if not isinstance(members, list) or not members:
        raise ValueError("workspace.members must be a non-empty array")
    packages: list[Package] = []
    for member in members:
        if not isinstance(member, str) or "*" in member:
            raise ValueError("architecture inventory requires explicit workspace member paths")
        manifest_path = root / member / "Cargo.toml"
        manifest = load_toml(manifest_path)
        packages.append(Package(package_name(manifest, manifest_path), normalized(Path(member))))
    return tuple(sorted(packages, key=lambda item: item.name))


def manifest_paths(root: Path, root_manifest: dict[str, Any], policy: dict[str, Any]) -> list[Path]:
    workspace = root_manifest["workspace"]
    paths = [root / "Cargo.toml"]
    paths.extend(root / member / "Cargo.toml" for member in workspace["members"])
    paths.extend(root / entry["manifest"] for entry in policy["standalone"])
    return paths


def dependency_features(value: object) -> tuple[tuple[str, ...], bool]:
    if not isinstance(value, dict):
        return (), False
    features = value.get("features", [])
    normalized_features = tuple(sorted(item for item in features if isinstance(item, str)))
    return normalized_features, value.get("workspace") is True


def collect_tokio(root: Path, manifests: Iterable[Path], root_manifest: dict[str, Any]) -> tuple[TokioDeclaration, ...]:
    workspace_dependencies = root_manifest.get("workspace", {}).get("dependencies", {})
    declarations: list[TokioDeclaration] = []
    for path in manifests:
        manifest = load_toml(path)
        relative = normalized(path.relative_to(root))
        tables = list(dependency_tables(manifest))
        if path == root / "Cargo.toml" and isinstance(workspace_dependencies, dict):
            tables.append(workspace_dependencies)
        seen: set[str] = set()
        for table in tables:
            for dependency in ("tokio", "tokio-util", "tokio-stream"):
                if dependency not in table or dependency in seen:
                    continue
                features, inherited = dependency_features(table[dependency])
                if inherited and isinstance(workspace_dependencies, dict):
                    inherited_features, _ = dependency_features(workspace_dependencies.get(dependency))
                    features = tuple(sorted(set(features) | set(inherited_features)))
                declarations.append(TokioDeclaration(relative, dependency, features, inherited))
                seen.add(dependency)
    return tuple(sorted(declarations, key=lambda item: (item.manifest, item.dependency)))


def collect_local_edges(root: Path, policy: dict[str, Any]) -> tuple[LocalEdge, ...]:
    edges: list[LocalEdge] = []
    for entry in policy["standalone"]:
        manifest_path = root / entry["manifest"]
        manifest = load_toml(manifest_path)
        consumer = package_name(manifest, manifest_path)
        for table in dependency_tables(manifest):
            for dependency, value in table.items():
                if not isinstance(value, dict) or not isinstance(value.get("path"), str):
                    continue
                target_path = (manifest_path.parent / value["path"]).resolve()
                try:
                    relative_target = target_path.relative_to(root.resolve())
                except ValueError:
                    continue
                target_manifest = target_path / "Cargo.toml"
                if not target_manifest.is_file():
                    continue
                target = package_name(load_toml(target_manifest), target_manifest)
                edges.append(LocalEdge(consumer, str(dependency), normalized(relative_target)))
    return tuple(sorted(set(edges), key=lambda item: (item.consumer, item.dependency, item.target)))


def collect_facts(root: Path, policy: dict[str, Any]) -> Facts:
    root_manifest = load_toml(root / "Cargo.toml")
    return Facts(
        formal_toolchain=policy["toolchains"]["formal"],
        root_packages=collect_root_packages(root, root_manifest),
        standalone=tuple(policy["standalone"]),
        node_projects=tuple(policy["node_projects"]),
        tokio=collect_tokio(root, manifest_paths(root, root_manifest, policy), root_manifest),
        local_edges=collect_local_edges(root, policy),
        evidence_artifacts=tuple(policy["evidence_artifacts"]),
    )


def validate_schema(policy: dict[str, Any]) -> list[Finding]:
    findings: list[Finding] = []
    required = {
        "schema_version",
        "toolchains",
        "implementation_baseline",
        "root",
        "standalone",
        "node_projects",
        "tokio_full_allowed",
        "coverage",
        "critical_action_pins",
        "critical_workflows",
        "evidence_artifacts",
        "python_tests",
        "generated_document",
    }
    if set(policy) != required or policy.get("schema_version") != 1:
        findings.append(Finding("policy-schema", normalized(POLICY_RELATIVE), "unexpected top-level schema"))
    standalone = policy.get("standalone", [])
    if not isinstance(standalone, list) or len(standalone) != 5:
        findings.append(Finding("standalone-count", normalized(POLICY_RELATIVE), "exactly five Cargo roots required"))
    return findings


def validate_implementation_baseline(root: Path, policy: dict[str, Any]) -> list[Finding]:
    baseline = policy.get("implementation_baseline")
    expected_fields = {
        "id",
        "generator",
        "output",
        "historical_review_commit",
        "planning_snapshot_commit",
        "historical_difference",
        "commands",
        "required_evidence",
    }
    if not isinstance(baseline, dict) or set(baseline) != expected_fields:
        return [
            Finding(
                "implementation-baseline-schema",
                normalized(POLICY_RELATIVE),
                "unexpected implementation_baseline schema",
            )
        ]
    findings: list[Finding] = []
    if not re.fullmatch(r"architecture-implementation-\d{4}-\d{2}-\d{2}-v\d+", baseline["id"]):
        findings.append(
            Finding("implementation-baseline-id", normalized(POLICY_RELATIVE), "baseline id is not versioned")
        )
    for field in ("historical_review_commit", "planning_snapshot_commit"):
        if not re.fullmatch(r"[0-9a-f]{40}", baseline[field]):
            findings.append(Finding("implementation-baseline-commit", normalized(POLICY_RELATIVE), field))
    for field in ("generator", "output"):
        path = Path(baseline[field])
        if path.is_absolute() or ".." in path.parts:
            findings.append(Finding("implementation-baseline-path", normalized(POLICY_RELATIVE), field))
    if not (root / baseline["generator"]).is_file():
        findings.append(
            Finding("implementation-baseline-generator", baseline["generator"], "generator is missing")
        )
    if not isinstance(baseline["commands"], list) or not baseline["commands"]:
        findings.append(
            Finding("implementation-baseline-commands", normalized(POLICY_RELATIVE), "commands are required")
        )
    evidence = baseline["required_evidence"]
    if (
        not isinstance(evidence, list)
        or not evidence
        or len(evidence) != len(set(evidence))
        or any(Path(path).is_absolute() or ".." in Path(path).parts for path in evidence)
    ):
        findings.append(
            Finding(
                "implementation-baseline-evidence",
                normalized(POLICY_RELATIVE),
                "evidence paths must be unique repository-relative paths",
            )
        )
    return findings


def validate_python_tests(root: Path, policy: dict[str, Any]) -> list[Finding]:
    findings: list[Finding] = []
    config = policy["python_tests"]
    if not isinstance(config, dict) or set(config) != {"expected_count", "ci", "entries"}:
        return [Finding("test-inventory-schema", normalized(POLICY_RELATIVE), "unexpected python_tests schema")]
    entries = config["entries"]
    if not isinstance(entries, list) or config["expected_count"] != 50 or len(entries) != config["expected_count"]:
        findings.append(
            Finding(
                "test-inventory-count",
                normalized(POLICY_RELATIVE),
                f"expected={config['expected_count']} entries={len(entries) if isinstance(entries, list) else 'invalid'}",
            )
        )
        return findings

    expected_fields = {
        "path",
        "owner",
        "tier",
        "trigger_paths",
        "command",
        "estimated_seconds",
        "platform",
        "fixture_policy",
    }
    tiers = {"pr_static", "milestone_contract", "phase_contract", "dynamic_fixture"}
    platforms = {"any", "powershell"}
    fixtures = {"none", "repository-fixtures", "temporary-only"}
    inventory_paths: set[str] = set()
    for index, entry in enumerate(entries):
        label = f"{normalized(POLICY_RELATIVE)}#python_tests.entries[{index}]"
        if not isinstance(entry, dict) or set(entry) != expected_fields:
            findings.append(Finding("test-inventory-schema", label, "unexpected entry fields"))
            continue
        path = entry["path"]
        if (
            not isinstance(path, str)
            or "\\" in path
            or not path.startswith("scripts/tests/test_")
            or not path.endswith(".py")
        ):
            findings.append(Finding("test-inventory-path", label, str(path)))
            continue
        if path in inventory_paths:
            findings.append(Finding("test-inventory-duplicate", path, "duplicate path"))
        inventory_paths.add(path)
        if not (root / path).is_file():
            findings.append(Finding("test-inventory-extra", path, "file is missing"))
        module = path.removesuffix(".py").replace("/", ".")
        expected_command = f"python -m unittest {module} -v"
        if entry["command"] != expected_command:
            findings.append(Finding("test-inventory-command", path, expected_command))
        if not isinstance(entry["owner"], str) or not entry["owner"]:
            findings.append(Finding("test-inventory-owner", path, "owner is required"))
        if entry["tier"] not in tiers:
            findings.append(Finding("test-inventory-tier", path, str(entry["tier"])))
        if entry["platform"] not in platforms:
            findings.append(Finding("test-inventory-platform", path, str(entry["platform"])))
        if entry["fixture_policy"] not in fixtures:
            findings.append(Finding("test-inventory-fixture", path, str(entry["fixture_policy"])))
        if not isinstance(entry["estimated_seconds"], int) or entry["estimated_seconds"] <= 0:
            findings.append(Finding("test-inventory-duration", path, str(entry["estimated_seconds"])))
        if (
            not isinstance(entry["trigger_paths"], list)
            or not entry["trigger_paths"]
            or not all(isinstance(value, str) and value for value in entry["trigger_paths"])
        ):
            findings.append(Finding("test-inventory-trigger", path, "trigger_paths must be non-empty"))

    discovered = {
        normalized(path.relative_to(root))
        for path in (root / "scripts/tests").glob("test_*.py")
    }
    for path in sorted(discovered - inventory_paths):
        findings.append(Finding("test-inventory-missing", path, "test file is not inventoried"))
    for path in sorted(inventory_paths - discovered):
        findings.append(Finding("test-inventory-extra", path, "inventory file is not discovered"))

    ci = config["ci"]
    if not isinstance(ci, dict) or set(ci) != {"guards", "contracts"}:
        findings.append(Finding("test-inventory-ci", normalized(POLICY_RELATIVE), "unexpected ci schema"))
    else:
        workflow_path = policy["root"]["workflow"]
        workflow = (root / workflow_path).read_text(encoding="utf-8")
        for command in ci.values():
            if command not in workflow:
                findings.append(Finding("test-inventory-ci", workflow_path, f"missing {command}"))
        if 'test_*guard.py' in workflow:
            findings.append(Finding("test-inventory-ci", workflow_path, "obsolete guard-only discovery remains"))
    return findings


def validate_toolchains(root: Path, policy: dict[str, Any], facts: Facts) -> list[Finding]:
    findings: list[Finding] = []
    formal = facts.formal_toolchain
    root_manifest = load_toml(root / "Cargo.toml")
    root_version = root_manifest.get("workspace", {}).get("package", {}).get("rust-version")
    toolchain = load_toml(root / "rust-toolchain.toml").get("toolchain", {}).get("channel")
    if root_version != formal or toolchain != formal:
        findings.append(Finding("formal-toolchain", "rust-toolchain.toml", f"expected {formal} everywhere"))
    for entry in facts.standalone:
        manifest = load_toml(root / entry["manifest"])
        version = manifest.get("package", {}).get("rust-version")
        if version != formal:
            findings.append(Finding("standalone-msrv", entry["manifest"], f"expected rust-version {formal}"))
    formal_workflows = {
        policy["root"]["workflow"],
        *(entry["workflow"] for entry in facts.standalone if entry["id"] != "fuzz"),
    }
    for workflow in sorted(formal_workflows):
        path = root / workflow
        if not path.is_file():
            continue
        source = path.read_text(encoding="utf-8")
        if "rust-toolchain@nightly" in source or "toolchain: nightly" in source:
            findings.append(Finding("nightly-formal-gate", workflow, "formal validation must use rust-toolchain.toml"))
    return findings


def validate_routes(root: Path, policy: dict[str, Any], facts: Facts) -> list[Finding]:
    findings: list[Finding] = []
    root_workflow_path = root / policy["root"]["workflow"]
    root_workflow = root_workflow_path.read_text(encoding="utf-8") if root_workflow_path.is_file() else ""
    root_workflow_flat = re.sub(r"\s+", " ", root_workflow)
    for command in policy["root"]["commands"]:
        if command not in root_workflow and command not in root_workflow_flat:
            findings.append(Finding("validation-command-missing", policy["root"]["workflow"], command))
    for entry in facts.standalone:
        for field in ("manifest", "instructions", "workflow"):
            path = root / entry[field]
            if not path.is_file():
                findings.append(Finding("route-missing", entry[field], f"{entry['id']} {field} is missing"))
        workflow_path = root / entry["workflow"]
        if workflow_path.is_file():
            workflow = workflow_path.read_text(encoding="utf-8")
            workflow_flat = re.sub(r"\s+", " ", workflow)
            project = normalized(Path(entry["manifest"]).parent)
            if project not in workflow:
                findings.append(Finding("path-filter-missing", entry["workflow"], f"does not route {project}"))
            for command in entry["commands"]:
                tail = command.replace("cargo +nightly-2026-07-05 ", "cargo ")
                if (
                    command not in workflow
                    and tail not in workflow
                    and command not in workflow_flat
                    and tail not in workflow_flat
                ):
                    findings.append(Finding("validation-command-missing", entry["workflow"], command))
    for entry in facts.node_projects:
        workflow_path = root / entry["workflow"]
        source = workflow_path.read_text(encoding="utf-8") if workflow_path.is_file() else ""
        for command in entry["commands"]:
            if command not in source:
                findings.append(Finding("node-command-missing", entry["workflow"], f"{entry['id']}: {command}"))
    return findings


def validate_tokio(policy: dict[str, Any], facts: Facts) -> list[Finding]:
    allowed = set(policy["tokio_full_allowed"])
    findings: list[Finding] = []
    for declaration in facts.tokio:
        if "full" in declaration.features and declaration.manifest not in allowed:
            findings.append(
                Finding(
                    "tokio-full",
                    declaration.manifest,
                    f"{declaration.dependency} uses full outside an approved application root",
                )
            )
    return findings


def validate_action_pins(root: Path, policy: dict[str, Any]) -> list[Finding]:
    findings: list[Finding] = []
    pins = tuple(policy["critical_action_pins"].values())
    for workflow in policy["critical_workflows"]:
        path = root / workflow
        if not path.is_file():
            findings.append(Finding("critical-workflow-missing", workflow, "workflow is absent"))
            continue
        source = path.read_text(encoding="utf-8")
        for pin in pins:
            if pin not in source:
                findings.append(Finding("critical-action-unpinned", workflow, f"missing {pin}"))
    return findings


def validate_coverage(root: Path, policy: dict[str, Any]) -> list[Finding]:
    coverage = policy["coverage"]
    findings: list[Finding] = []
    config_path = root / coverage["config"]
    config = config_path.read_text(encoding="utf-8") if config_path.is_file() else ""
    for token in (
        f"target: {coverage['root_target']}",
        f"threshold: {coverage['allowed_regression']}",
        f"target: {coverage['patch_target']}",
    ):
        if token not in config:
            findings.append(Finding("coverage-policy", coverage["config"], f"missing {token}"))
    root_workflow = (root / policy["root"]["workflow"]).read_text(encoding="utf-8")
    if "fail_ci_if_error: true" not in root_workflow:
        findings.append(Finding("coverage-upload-optional", policy["root"]["workflow"], "Codecov upload must fail clearly"))
    standalone = (root / coverage["standalone_workflow"]).read_text(encoding="utf-8")
    for entry in policy["standalone"]:
        if entry["id"] == "fuzz":
            continue
        if entry["id"] not in standalone:
            findings.append(Finding("standalone-coverage-missing", coverage["standalone_workflow"], entry["id"]))
    fuzz = (root / coverage["fuzz_workflow"]).read_text(encoding="utf-8")
    if "cargo +nightly-2026-07-05 fuzz run" not in fuzz:
        findings.append(Finding("fuzz-coverage-missing", coverage["fuzz_workflow"], "libFuzzer exercise is absent"))
    return findings


def validate_compatibility(root: Path) -> list[Finding]:
    findings: list[Finding] = []
    producer = (root / "rocketmq-client/src/producer/default_mq_producer.rs").read_text(encoding="utf-8")
    for token in ("borrowed_generations", "compatibility_borrow", "compatibility_config_generation_counts"):
        if token in producer:
            findings.append(Finding("stable-config-history", "rocketmq-client/src/producer/default_mq_producer.rs", token))
    forbidden_paths = (
        "rocketmq-client/src/admin/mq_admin_ext_inner.rs",
        "rocketmq-client/src/admin/mq_admin_ext_async_inner.rs",
        "rocketmq-store/src/compat/message_store_adapter.rs",
    )
    for relative in forbidden_paths:
        if (root / relative).exists():
            findings.append(Finding("retired-file-present", relative, "compatibility file must be deleted"))
    source_roots = (
        root / "rocketmq-client/src",
        root / "rocketmq-store/src",
    )
    for source_root in source_roots:
        for path in source_root.rglob("*.rs"):
            source = path.read_text(encoding="utf-8")
            for token in ("MQAdminExtInner", "MessageStoreInner"):
                if token in source:
                    findings.append(Finding("retired-symbol-present", normalized(path.relative_to(root)), token))
    runtime_root = (root / "rocketmq-runtime/src/lib.rs").read_text(encoding="utf-8")
    task_group = (root / "rocketmq-runtime/src/task_group.rs").read_text(encoding="utf-8")
    if "pub use handle::RuntimeHandle" in runtime_root:
        findings.append(Finding("runtime-handle-public", "rocketmq-runtime/src/lib.rs", "RuntimeHandle re-exported"))
    for token in ("pub fn root(", "spawn_detached"):
        if token in task_group:
            findings.append(Finding("runtime-task-escape", "rocketmq-runtime/src/task_group.rs", token))
    return findings


def validate_document_language(root: Path) -> list[Finding]:
    findings: list[Finding] = []
    paths = [root / "README.md", root / "rocketmq-doc/en", root / "rocketmq-website/docs"]
    patterns = (
        re.compile(r"dashmap.{0,80}lock[- ]free", re.IGNORECASE),
        re.compile(r"lock[- ]free.{0,80}dashmap", re.IGNORECASE),
        re.compile(r"sends? messages? through netty", re.IGNORECASE),
        re.compile(r"connects? to the name server based on the netty client", re.IGNORECASE),
        re.compile(r"automatic failover.{0,40}dledger", re.IGNORECASE),
    )
    for base in paths:
        candidates = [base] if base.is_file() else list(base.rglob("*.md")) + list(base.rglob("*.mdx"))
        for path in candidates:
            source = path.read_text(encoding="utf-8")
            for pattern in patterns:
                if pattern.search(source):
                    findings.append(Finding("stale-architecture-language", normalized(path.relative_to(root)), pattern.pattern))
    return findings


def render_document(policy: dict[str, Any], facts: Facts) -> str:
    lines = [
        "# Architecture validation and release evidence index",
        "",
        "<!-- Generated by scripts/architecture_documentation_guard.py. Do not edit manually. -->",
        "",
        "This index binds architecture validation, compatibility retirement, and production evidence to the",
        "current repository facts. Workflow artifacts are evidence only when their name includes the tested",
        "commit SHA; this document does not claim that a scheduled or release run succeeded.",
            "",
            "## Toolchain and root workspace",
        "",
        f"- Formal Rust toolchain and MSRV: `{facts.formal_toolchain}`.",
        f"- Root workspace packages: {len(facts.root_packages)}.",
        "- Root final gates: `cargo fmt --all -- --check`, strict workspace Clippy, all-feature tests, and",
        "  `cargo doc --workspace --no-deps --all-features`.",
        "",
        "| Package | Workspace path |",
        "|---|---|",
    ]
    lines.extend(f"| `{package.name}` | `{package.path}` |" for package in facts.root_packages)
    baseline = policy["implementation_baseline"]
    lines.extend(
        [
            "",
            "## Current implementation baseline",
            "",
            f"- Baseline ID: `{baseline['id']}`.",
            f"- Generator: `python {baseline['generator']}`.",
            f"- Local artifact: `{baseline['output']}` (generated, not committed).",
            f"- Historical review input: `{baseline['historical_review_commit']}`.",
            f"- Planning snapshot input: `{baseline['planning_snapshot_commit']}`.",
            f"- Distinction: {baseline['historical_difference']}",
            "",
            "The manifest records the current commit, dirty state, normalized Cargo metadata, toolchains,",
            "hardware/filesystem facts, project routes, commands, and evidence checksums. A dirty manifest",
            "is explicitly ineligible as a clean release candidate. Later performance and fault artifacts",
            "must reference this baseline ID or a deliberately versioned successor.",
        ]
    )
    lines.extend(
        [
            "",
            "## Standalone Cargo validation matrix",
            "",
            "| Project | Owner | Toolchain | Manifest | Required commands | Workflow |",
            "|---|---|---|---|---|---|",
        ]
    )
    for entry in facts.standalone:
        commands = "<br>".join(f"`{command}`" for command in entry["commands"])
        lines.append(
            f"| {entry['id']} | {entry['owner']} | `{entry['toolchain']}` | "
            f"`{entry['manifest']}` | {commands} | `{entry['workflow']}` |"
        )
    lines.extend(
        [
            "",
            "## Shared crate to standalone consumer edges",
            "",
            "| Standalone package | Dependency | Root target path |",
            "|---|---|---|",
        ]
    )
    if facts.local_edges:
        lines.extend(
            f"| `{edge.consumer}` | `{edge.dependency}` | `{edge.target}` |" for edge in facts.local_edges
        )
    else:
        lines.append("| _none_ | _none_ | _none_ |")
    lines.extend(
        [
            "",
            "## Tokio feature declarations",
            "",
            "`full` is allowed only at the application roots listed in the validation policy. Workspace",
            "libraries inherit an explicit feature union rather than the `full` meta-feature.",
            "",
            "| Manifest | Dependency | Features | Inherited from workspace |",
            "|---|---|---|---|",
        ]
    )
    for item in facts.tokio:
        features = ", ".join(item.features) if item.features else "_none_"
        lines.append(
            f"| `{item.manifest}` | `{item.dependency}` | `{features}` | "
            f"{'yes' if item.inherited else 'no'} |"
        )
    lines.extend(
        [
            "",
            "## Compatibility retirement",
            "",
            "| Debt | Final state | Evidence |",
            "|---|---|---|",
            "| `StableConfig` borrowed history | Removed | owned `Arc` snapshots; no retained-generation list or unsafe borrow |",
            "| `MessageStoreInner` | Removed | the public `MessageStore` trait is generated in place; no inner trait or legacy adapter |",
            "| `MQAdminExtInner` | Removed | marker, empty alias, modules, and crate-root re-exports deleted |",
            "| Raw runtime/detached spawn | Crate-private or removed | `RuntimeHandle` and root creation are private; detached spawn has no public entry |",
            "",
            "The still-public `MessageStore` and producer facades retain real downstream value and remain narrow",
            "forwarders. Their next deletion review is the next approved major release. `RuntimeContext` remains",
            "document-hidden for tests and migration harnesses; production composition roots use `RuntimeOwner`",
            "and inject `ChildServiceContext`/`TaskGroup` capabilities.",
            "",
            "## Python architecture test inventory",
            "",
            f"- Inventoried test modules: {len(policy['python_tests']['entries'])}.",
            f"- Guard runner: `{policy['python_tests']['ci']['guards']}`.",
            f"- Contract runner: `{policy['python_tests']['ci']['contracts']}`.",
            "",
            "| Tier | Modules |",
            "|---|---:|",
        ]
    )
    for tier in ("pr_static", "milestone_contract", "phase_contract", "dynamic_fixture"):
        count = sum(entry["tier"] == tier for entry in policy["python_tests"]["entries"])
        lines.append(f"| `{tier}` | {count} |")
    lines.extend(
        [
            "",
            "## Evidence workflows and artifact identities",
            "",
            "| Evidence | Workflow | Artifact identity |",
            "|---|---|---|",
        ]
    )
    for artifact in facts.evidence_artifacts:
        lines.append(f"| {artifact['kind']} | `{artifact['workflow']}` | `{artifact['artifact']}` |")
    lines.extend(
        [
            "",
            "Coverage uses a root-workspace auto baseline with a 1% allowed regression and a 50% patch",
            "target. Each standalone application publishes a separate LCOV artifact; the fuzz standalone",
            "reports libFuzzer edge coverage and retains its versioned corpus rather than producing an empty",
            "unit-test LCOV report.",
            "",
            "Critical evidence workflows pin checkout and artifact upload Actions to reviewed commit SHAs.",
            "Benchmark reports must include the runner fingerprint, toolchain, profile, features, commit, samples,",
            "and comparison result required by `scripts/architecture-performance-profiles.json`. Fault and soak",
            "artifacts use the production-readiness and fault-matrix policies; failures retain replay inputs and",
            "diagnostics without committing runtime output.",
            "",
            "## Architecture evidence cross-checks",
            "",
            "- Trait decisions: `scripts/trait-policy-baseline.json` and `rocketmq-doc/en/rust-trait-design-guidelines.md`.",
            "- Dependency and public-facade state: `scripts/architecture-dependency-policy.json` and the strict target guard.",
            "- Manual Pin, production unsafe, panic/unwrap/expect, and historical `mod.rs` state:",
            "  `scripts/rust_hygiene_guard.py` plus its baseline.",
            "- Runtime ownership: `scripts/runtime-task-escape-policy.json` and the enforcing runtime audit.",
            "- Performance thresholds: `scripts/architecture-performance-profiles.json` and the performance guard.",
            "- Distributed evidence: `distribution/kubernetes/fault-matrix-policy.json` and the SLO/fault guards.",
            "",
        ]
    )
    return "\n".join(lines)


def validate(root: Path, policy: dict[str, Any], facts: Facts) -> list[Finding]:
    findings = validate_schema(policy)
    findings.extend(validate_implementation_baseline(root, policy))
    findings.extend(validate_python_tests(root, policy))
    findings.extend(validate_toolchains(root, policy, facts))
    findings.extend(validate_routes(root, policy, facts))
    findings.extend(validate_tokio(policy, facts))
    findings.extend(validate_action_pins(root, policy))
    findings.extend(validate_coverage(root, policy))
    findings.extend(validate_compatibility(root))
    findings.extend(validate_document_language(root))
    document_path = root / policy["generated_document"]
    expected = render_document(policy, facts)
    actual = document_path.read_text(encoding="utf-8") if document_path.is_file() else ""
    if actual != expected:
        findings.append(Finding("generated-document-drift", policy["generated_document"], "run with --write"))
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--write", action="store_true")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT, help=argparse.SUPPRESS)
    args = parser.parse_args()

    root = args.root.resolve()
    try:
        policy = load_json(root / POLICY_RELATIVE)
        facts = collect_facts(root, policy)
        if args.write:
            output = root / policy["generated_document"]
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(render_document(policy, facts), encoding="utf-8")
            print(f"ARCHITECTURE_DOCUMENTATION_WRITTEN path={normalized(output.relative_to(root))}")
            return 0
        findings = validate(root, policy, facts)
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError, tomllib.TOMLDecodeError) as error:
        print(f"DOCUMENTATION_FINDING code=input-invalid path=. detail={error}")
        return 2

    if findings:
        for finding in findings:
            print(finding.render())
        print(f"ARCHITECTURE_DOCUMENTATION_FAILED findings={len(findings)}")
        return 1
    print(
        "ARCHITECTURE_DOCUMENTATION_OK "
        f"root_packages={len(facts.root_packages)} standalone={len(facts.standalone)} "
        f"local_edges={len(facts.local_edges)} tokio_declarations={len(facts.tokio)}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
