#!/usr/bin/env python3
#
# Copyright 2023 The RocketMQ Rust Authors
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

"""Check package layering and dependency cycles without historical snapshots."""

from __future__ import annotations

import argparse
import dataclasses
import json
from pathlib import Path
import subprocess
import sys
import tomllib
from typing import Any, Iterable

import core_release_scope


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "scripts" / "architecture-dependency-policy.json"


class InputError(Exception):
    """An invalid policy, metadata file, or CLI combination."""


@dataclasses.dataclass(frozen=True)
class Edge:
    caller: str
    target: str
    kind: str
    path: str
    alias: str | None = None


@dataclasses.dataclass(frozen=True)
class Finding:
    rule: str
    caller: str
    target: str
    path: str
    kind: str
    detail: str = ""

    def render(self) -> str:
        suffix = f" detail={self.detail}" if self.detail else ""
        return (
            f"VIOLATION rule={self.rule} caller={self.caller} target={self.target} "
            f"path={self.path} kind={self.kind}{suffix}"
        )


def load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise InputError(f"cannot read {label} {path}: {error}") from error
    if not isinstance(value, dict):
        raise InputError(f"{label} must contain a JSON object: {path}")
    return value


def require_keys(value: dict[str, Any], keys: Iterable[str], label: str) -> None:
    missing = sorted(set(keys) - value.keys())
    if missing:
        raise InputError(f"{label} is missing required keys: {', '.join(missing)}")


def validate_policy(policy: dict[str, Any]) -> None:
    require_keys(policy, ("schema_version", "roots", "package_rules", "closure_rules", "facade_rules"), "policy")
    if policy["schema_version"] != 1:
        raise InputError("unsupported dependency policy schema")
    roots = policy["roots"]
    if not isinstance(roots, dict) or not isinstance(roots.get("standalone_manifests"), list):
        raise InputError("standalone_manifests must be a list")
    for path in roots["standalone_manifests"]:
        if not isinstance(path, str) or not path or Path(path).is_absolute() or ".." in Path(path).parts:
            raise InputError("standalone manifests must be repository-relative paths")
    for section in ("package_rules", "closure_rules", "facade_rules"):
        if not isinstance(policy[section], list):
            raise InputError(f"{section} must be a list")
        for rule in policy[section]:
            fields = ("canonical_packages",) if section == "facade_rules" else ("callers", "forbidden_targets")
            if not isinstance(rule, dict):
                raise InputError(f"{section} entries must be objects")
            for field in fields:
                values = rule.get(field)
                if not isinstance(values, list) or not values or any(not isinstance(v, str) or not v for v in values):
                    raise InputError(f"{section}.{field} must contain package names")
            label = "facade" if section == "facade_rules" else "id" if section == "package_rules" else None
            if label is not None and (not isinstance(rule.get(label), str) or not rule[label]):
                raise InputError(f"{section}.{label} must be non-empty")


def read_metadata(metadata_file: Path | None, source_root: Path = ROOT) -> dict[str, Any]:
    if metadata_file is not None:
        return load_json(metadata_file, "metadata")
    completed = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        cwd=source_root,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    if completed.returncode != 0:
        raise InputError(f"cargo metadata failed: {completed.stderr.strip()}")
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise InputError(f"cargo metadata emitted invalid JSON: {error}") from error


def workspace_packages(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    require_keys(metadata, ("packages", "workspace_members"), "metadata")
    members = set(metadata["workspace_members"])
    packages = [item for item in metadata["packages"] if item.get("id") in members]
    if len(packages) != len(members):
        raise InputError("metadata workspace_members do not resolve to unique package entries")
    return packages


def normalize_manifest_path(raw_path: str, source_root: Path) -> str:
    path = Path(raw_path)
    if path.is_absolute():
        try:
            return path.resolve().relative_to(source_root.resolve()).as_posix()
        except ValueError:
            return path.as_posix()
    return Path(raw_path.replace("\\", "/")).as_posix()


def dependency_edges(packages: list[dict[str, Any]], source_root: Path) -> list[Edge]:
    edges: list[Edge] = []
    for package in packages:
        manifest = normalize_manifest_path(str(package.get("manifest_path", "<unknown>")), source_root)
        for dependency in package.get("dependencies", []):
            target = dependency.get("name")
            edges.append(
                Edge(
                    caller=package["name"],
                    target=target,
                    kind=dependency.get("kind") or "normal",
                    path=manifest,
                    alias=(dependency.get("rename") or target).replace("-", "_"),
                )
            )
    return edges


def standalone_dependency_edges(source_root: Path, policy: dict[str, Any]) -> list[Edge]:
    """Read workspace/path dependencies from standalone manifests omitted by root metadata."""
    edges: list[Edge] = []
    for relative_manifest in policy["roots"]["standalone_manifests"]:
        manifest = source_root / relative_manifest
        if not manifest.is_file():
            continue
        try:
            data = tomllib.loads(manifest.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
            raise InputError(f"cannot read standalone manifest {manifest}: {error}") from error
        caller = source_caller(relative_manifest, [])
        sections: list[tuple[str, dict[str, Any]]] = []
        for key, kind in (("dependencies", "normal"), ("build-dependencies", "build"), ("dev-dependencies", "dev")):
            section = data.get(key, {})
            if isinstance(section, dict):
                sections.append((kind, section))
        for target_section in data.get("target", {}).values():
            if not isinstance(target_section, dict):
                continue
            for key, kind in (("dependencies", "normal"), ("build-dependencies", "build"), ("dev-dependencies", "dev")):
                section = target_section.get(key, {})
                if isinstance(section, dict):
                    sections.append((kind, section))
        for kind, section in sections:
            for alias, specification in section.items():
                if not isinstance(specification, dict):
                    continue
                if "path" not in specification and not specification.get("workspace", False):
                    continue
                if specification.get("workspace", False):
                    package_name = resolve_workspace_dependency(manifest, alias, source_root)
                else:
                    package_name = specification.get("package", alias)
                edges.append(
                    Edge(
                        caller,
                        package_name,
                        kind,
                        normalize_manifest_path(relative_manifest, source_root),
                        alias.replace("-", "_"),
                    )
                )
    return edges


def resolve_workspace_dependency(manifest: Path, alias: str, source_root: Path) -> str:
    """Resolve a standalone `{ workspace = true }` alias from an ancestor workspace manifest."""
    current = manifest.parent.resolve()
    root = source_root.resolve()
    while True:
        candidate = current / "Cargo.toml"
        if candidate.is_file() and candidate.resolve() != manifest.resolve():
            try:
                data = tomllib.loads(candidate.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
                raise InputError(f"cannot read workspace manifest {candidate}: {error}") from error
            workspace_dependencies = data.get("workspace", {}).get("dependencies", {})
            if alias in workspace_dependencies:
                inherited = workspace_dependencies[alias]
                if isinstance(inherited, dict):
                    return inherited.get("package", alias)
                return alias
        if current == root:
            break
        if root not in current.parents:
            break
        current = current.parent
    relative = normalize_manifest_path(str(manifest), source_root)
    raise InputError(f"workspace dependency {alias} referenced by {relative} has no ancestor definition")


def source_caller(relative: str, packages: list[dict[str, Any]]) -> str:
    normalized = relative.replace("\\", "/")
    special = {
        "rocketmq-tools/rocketmq-admin/rocketmq-admin-core/": "rocketmq-admin-core",
        "rocketmq-ai/rocketmq-mcp/": "rocketmq-mcp",
        "rocketmq-dashboard/rocketmq-dashboard-gpui/": "rocketmq-dashboard-gpui",
        "rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/": "rocketmq-dashboard-tauri-backend",
        "rocketmq-dashboard/rocketmq-dashboard-web/backend/": "rocketmq-dashboard-web-backend",
        "rocketmq-example/": "rocketmq-example",
    }
    for prefix, name in special.items():
        if normalized.startswith(prefix):
            return name
    first = normalized.split("/", 1)[0]
    names = {item["name"] for item in packages}
    if first in names:
        return first
    if first == "rocketmq-client":
        return "rocketmq-client-rust"
    return first


def package_rule_findings(edges: list[Edge], policy: dict[str, Any]) -> list[Finding]:
    findings: list[Finding] = []
    for rule in policy["package_rules"]:
        callers = set(rule["callers"])
        targets = set(rule["forbidden_targets"])
        for edge in edges:
            if edge.caller in callers and edge.target in targets:
                findings.append(
                    Finding(rule["id"], edge.caller, edge.target, edge.path, edge.kind)
                )
    return findings


def facade_rule_findings(edges: list[Edge], policy: dict[str, Any]) -> list[Finding]:
    findings: list[Finding] = []
    for rule in policy["facade_rules"]:
        facade = rule["facade"]
        canonical_packages = set(rule["canonical_packages"])
        for edge in edges:
            if edge.caller in canonical_packages and edge.target == facade:
                findings.append(
                    Finding(
                        "facade-reverse-dependency",
                        edge.caller,
                        edge.target,
                        edge.path,
                        edge.kind,
                        "canonical package must not depend on its composition facade",
                    )
                )
    return findings


def closure_rule_findings(edges: list[Edge], policy: dict[str, Any]) -> list[Finding]:
    adjacency: dict[str, list[Edge]] = {}
    for edge in edges:
        adjacency.setdefault(edge.caller, []).append(edge)
    findings: list[Finding] = []
    for rule in policy["closure_rules"]:
        for caller in rule["callers"]:
            forbidden = set(rule["forbidden_targets"])
            queue: list[tuple[str, list[Edge]]] = [(caller, [])]
            visited = {caller}
            while queue:
                node, path = queue.pop(0)
                for edge in adjacency.get(node, []):
                    next_path = path + [edge]
                    if edge.target in forbidden:
                        findings.append(
                            Finding(
                                "transitive-forbidden-reachability",
                                caller,
                                edge.target,
                                "->".join([caller] + [item.target for item in next_path]),
                                edge.kind,
                                "graph=normal edge_kinds=" + ",".join(item.kind for item in next_path),
                            )
                        )
                        forbidden.remove(edge.target)
                    if edge.target not in visited:
                        visited.add(edge.target)
                        queue.append((edge.target, next_path))
    return findings


def cycle_findings(edges: list[Edge]) -> list[Finding]:
    adjacency: dict[str, list[Edge]] = {}
    for edge in edges:
        adjacency.setdefault(edge.caller, []).append(edge)
    findings: list[Finding] = []
    reported: set[tuple[tuple[str, str, str, str], ...]] = set()
    nodes = sorted(set(adjacency) | {edge.target for edge in edges})

    # The lexicographically smallest node is the unique start for each simple
    # directed cycle. Edge identity includes kind and manifest path, so parallel
    # normal/build/dev edges and distinct directed paths are never collapsed.
    for start in nodes:
        path_nodes = [start]
        path_edges: list[Edge] = []
        active = {start}

        def visit(node: str) -> None:
            for edge in sorted(
                adjacency.get(node, []),
                key=lambda item: (item.target, item.kind, item.path, item.alias or ""),
            ):
                if edge.target < start:
                    continue
                if edge.target == start:
                    cycle_edges = path_edges + [edge]
                    identity = tuple(
                        (item.caller, item.target, item.kind, item.path)
                        for item in cycle_edges
                    )
                    if identity in reported:
                        continue
                    reported.add(identity)
                    cycle_nodes = path_nodes + [start]
                    findings.append(
                        Finding(
                            "dependency-cycle",
                            edge.caller,
                            edge.target,
                            "->".join(cycle_nodes),
                            edge.kind,
                            "edges=" + ",".join(
                                f"{item.caller}>{item.target}:{item.kind}@{item.path}"
                                for item in cycle_edges
                            ),
                        )
                    )
                elif edge.target not in active:
                    active.add(edge.target)
                    path_nodes.append(edge.target)
                    path_edges.append(edge)
                    visit(edge.target)
                    path_edges.pop()
                    path_nodes.pop()
                    active.remove(edge.target)

        visit(start)
    return findings


def evaluate(
    metadata: dict[str, Any],
    source_root: Path,
    policy: dict[str, Any],
    *,
    scope: str = "all",
) -> list[Finding]:
    packages = workspace_packages(metadata)
    edges = dependency_edges(packages, source_root)
    edges.extend(standalone_dependency_edges(source_root, policy))
    if scope == "core-release":
        scope_document = core_release_scope.load_scope(source_root / "scripts/core-release-scope.json")
        core_names = {entry["name"] for entry in core_release_scope.core_packages(scope_document)}
        edges = [edge for edge in edges if edge.caller in core_names]
    internal_names = {package["name"] for package in packages} | {edge.caller for edge in edges}
    internal = [edge for edge in edges if edge.target in internal_names]
    # Cargo permits dev-dependency cycles; they are not production dependency cycles.
    findings = cycle_findings([edge for edge in internal if edge.kind != "dev"])
    findings.extend(package_rule_findings(edges, policy))
    findings.extend(facade_rule_findings(internal, policy))
    findings.extend(closure_rule_findings([edge for edge in internal if edge.kind == "normal"], policy))
    return sorted(findings, key=lambda finding: finding.render())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope", choices=("core-release", "repo-global", "all"), default="core-release")
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--metadata-file", type=Path)
    parser.add_argument("--source-root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        policy = load_json(args.policy, "policy")
        validate_policy(policy)
        source_root = args.source_root.resolve()
        metadata = read_metadata(args.metadata_file, source_root)
        findings = evaluate(metadata, source_root, policy, scope=args.scope)
        for finding in findings:
            print(finding.render())
        if args.output is not None:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(
                json.dumps({"scope": args.scope, "findings": [dataclasses.asdict(f) for f in findings]}, indent=2) + "\n",
                encoding="utf-8",
            )
        if findings:
            print(f"ARCHITECTURE_DEPENDENCY_GUARD_FAILED findings={len(findings)}")
            return 1
        print(f"ARCHITECTURE_DEPENDENCY_GUARD_OK scope={args.scope}")
        return 0
    except (InputError, OSError, ValueError, KeyError, TypeError) as error:
        print(f"INPUT_ERROR {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
