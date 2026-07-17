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

"""Validate the current and target RocketMQ Rust package dependency boundaries."""

from __future__ import annotations

import argparse
from collections import Counter
import dataclasses
import hashlib
import json
import re
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "scripts" / "architecture-dependency-policy.json"
DEFAULT_BASELINE = ROOT / "scripts" / "architecture-dependency-baseline.json"
CLIENT_USE_RE = re.compile(r"\b(?:use|extern\s+crate)\s+([A-Za-z_][A-Za-z0-9_]*)\b")


class InputError(Exception):
    """An invalid policy, baseline, metadata file, or CLI combination."""


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
    require_keys(
        policy,
        (
            "schema_version",
            "roots",
            "package_counts",
            "planned_packages",
            "package_rules",
            "target_dag",
            "closure_rules",
            "client_policy",
            "facade_rules",
            "compatibility_manifest_policy",
            "milestone_order",
        ),
        "policy",
    )
    if policy["schema_version"] != 1:
        raise InputError(f"unsupported policy schema_version: {policy['schema_version']}")
    counts = policy["package_counts"]
    baseline_count = counts.get("baseline")
    target_count = counts.get("target")
    if not isinstance(baseline_count, int) or not isinstance(target_count, int) or target_count != 32:
        raise InputError("policy package_counts must track an integer milestone baseline and target=32")
    planned_count = target_count - baseline_count
    if planned_count < 0 or len(policy["planned_packages"]) != planned_count:
        raise InputError("policy planned_packages must exactly cover target minus milestone baseline")
    if len(set(policy["planned_packages"])) != planned_count:
        raise InputError("policy planned_packages must contain unique package names")
    if set(policy["planned_packages"]) - set(policy["target_dag"]):
        raise InputError("every planned package must have a target_dag entry")
    if len(policy["target_dag"]) != policy["package_counts"]["target"]:
        raise InputError("target_dag must encode all 32 target workspace packages")

    client_policy = policy["client_policy"]
    require_keys(
        client_policy,
        ("package", "crate_names", "target_manifest_allowlist", "target_source_allowlist"),
        "policy client_policy",
    )
    if set(client_policy) != {
        "package",
        "crate_names",
        "target_manifest_allowlist",
        "target_source_allowlist",
    }:
        raise InputError("policy client_policy contains unsupported keys")
    client = client_policy["package"]
    crate_names = client_policy["crate_names"]
    if not isinstance(client, str) or not client:
        raise InputError("policy client_policy package must be a non-empty string")
    if (
        not isinstance(crate_names, list)
        or not crate_names
        or any(not isinstance(alias, str) or not alias for alias in crate_names)
        or len(crate_names) != len(set(crate_names))
    ):
        raise InputError("policy client_policy crate_names must be unique non-empty strings")

    manifest_identities: set[tuple[str, str, str, str, str]] = set()
    for index, entry in enumerate(client_policy["target_manifest_allowlist"]):
        if not isinstance(entry, dict):
            raise InputError(f"policy client manifest allowlist[{index}] must be an object")
        require_keys(
            entry,
            ("caller", "target", "kind", "path", "alias"),
            f"policy client manifest allowlist[{index}]",
        )
        if set(entry) != {"caller", "target", "kind", "path", "alias"}:
            raise InputError(f"policy client manifest allowlist[{index}] contains unsupported keys")
        path = entry["path"]
        if (
            not isinstance(entry["caller"], str)
            or not entry["caller"]
            or entry["target"] != client
            or entry["kind"] not in {"normal", "dev", "build"}
            or not isinstance(path, str)
            or not path.endswith("Cargo.toml")
            or "\\" in path
            or Path(path).is_absolute()
            or not isinstance(entry["alias"], str)
            or not entry["alias"]
            or entry["alias"] not in crate_names
        ):
            raise InputError(f"policy client manifest allowlist[{index}] is invalid")
        identity = (entry["caller"], entry["target"], entry["kind"], path, entry["alias"])
        if identity in manifest_identities:
            raise InputError(f"duplicate policy client manifest allowlist identity: {identity}")
        manifest_identities.add(identity)

    source_identities: set[tuple[str, str, str]] = set()
    for index, entry in enumerate(client_policy["target_source_allowlist"]):
        if not isinstance(entry, dict):
            raise InputError(f"policy client source allowlist[{index}] must be an object")
        require_keys(
            entry,
            ("caller", "path_prefix", "aliases"),
            f"policy client source allowlist[{index}]",
        )
        if set(entry) != {"caller", "path_prefix", "aliases"}:
            raise InputError(f"policy client source allowlist[{index}] contains unsupported keys")
        prefix = entry["path_prefix"]
        aliases = entry["aliases"]
        if (
            not isinstance(entry["caller"], str)
            or not entry["caller"]
            or not isinstance(prefix, str)
            or not prefix.endswith("/")
            or "\\" in prefix
            or Path(prefix).is_absolute()
            or not isinstance(aliases, list)
            or not aliases
            or any(not isinstance(alias, str) or not alias for alias in aliases)
            or len(aliases) != len(set(aliases))
            or not set(aliases).issubset(crate_names)
        ):
            raise InputError(f"policy client source allowlist[{index}] is invalid")
        for alias in aliases:
            identity = (entry["caller"], prefix, alias)
            if identity in source_identities:
                raise InputError(f"duplicate policy client source allowlist identity: {identity}")
            source_identities.add(identity)


def validate_baseline(baseline: dict[str, Any], policy: dict[str, Any]) -> None:
    require_keys(
        baseline,
        (
            "schema_version",
            "head",
            "cargo_metadata_command",
            "metadata_sha256",
            "rustc_version",
            "cargo_version",
            "generated_output_path",
            "workspace_packages",
            "manifest_exceptions",
            "compatibility_manifest_exceptions",
            "source_exceptions",
        ),
        "baseline",
    )
    if baseline["schema_version"] != 1:
        raise InputError(f"unsupported baseline schema_version: {baseline['schema_version']}")
    if not re.fullmatch(r"[0-9a-f]{64}", baseline["metadata_sha256"]):
        raise InputError("baseline metadata_sha256 must be a lowercase SHA-256 digest")
    for key in ("cargo_metadata_command", "rustc_version", "cargo_version", "generated_output_path"):
        if not isinstance(baseline[key], str) or not baseline[key].strip():
            raise InputError(f"baseline {key} must be a non-empty string")
    for category in ("manifest_exceptions", "compatibility_manifest_exceptions", "source_exceptions"):
        for index, exception in enumerate(baseline[category]):
            if not exception.get("owner") or not exception.get("remove_by"):
                raise InputError(f"baseline {category}[{index}] requires owner and remove_by")
            path = exception.get("path")
            if not isinstance(path, str) or not path or "\\" in path or Path(path).is_absolute():
                raise InputError(f"baseline {category}[{index}] requires a normalized relative path")
    manifest_identities: set[tuple[Any, ...]] = set()
    for index, exception in enumerate(baseline["manifest_exceptions"]):
        if exception.get("rule") is None:
            require_keys(
                exception,
                ("caller", "target", "kind", "path", "alias", "count", "owner", "remove_by"),
                f"baseline manifest_exceptions[{index}]",
            )
            if not isinstance(exception["count"], int) or exception["count"] < 1:
                raise InputError(f"baseline manifest_exceptions[{index}] count must be positive")
            identity = (
                exception["caller"],
                exception["target"],
                exception["kind"],
                exception["path"],
                exception["alias"],
            )
        else:
            require_keys(
                exception,
                ("rule", "caller", "target", "kind", "path", "owner", "remove_by"),
                f"baseline manifest_exceptions[{index}]",
            )
            identity = (
                exception["rule"],
                exception["caller"],
                exception["target"],
                exception["kind"],
                exception["path"],
                exception.get("detail"),
            )
        if identity in manifest_identities:
            raise InputError(f"duplicate baseline manifest exception identity: {identity}")
        manifest_identities.add(identity)
    source_identities: set[tuple[str, str]] = set()
    for index, exception in enumerate(baseline["source_exceptions"]):
        require_keys(
            exception,
            ("path", "alias", "count", "owner", "remove_by"),
            f"baseline source_exceptions[{index}]",
        )
        if exception["path"].endswith("/"):
            raise InputError(f"baseline source_exceptions[{index}] must identify a file, not a directory")
        if not isinstance(exception["count"], int) or exception["count"] < 1:
            raise InputError(f"baseline source_exceptions[{index}] count must be positive")
        identity = (exception["path"], exception["alias"])
        if identity in source_identities:
            raise InputError(f"duplicate baseline source exception identity: {identity}")
        source_identities.add(identity)

    client = policy["client_policy"]["package"]
    temporary_manifest = [
        exception
        for exception in baseline["manifest_exceptions"]
        if exception.get("rule") is None
    ]
    if any(
        exception["caller"] != "rocketmq-proxy"
        or exception["target"] != client
        or exception["kind"] != "normal"
        or exception["path"] != "rocketmq-proxy/Cargo.toml"
        or exception["alias"] != "rocketmq_client_rust"
        or exception["count"] != 1
        or exception["owner"] != "proxy"
        or exception["remove_by"] != "M08"
        for exception in temporary_manifest
    ):
        raise InputError("temporary Client manifest ledger must be the exact Proxy M08 edge")
    source_exception_limits = {
        "rocketmq-proxy/src/cluster.rs": 12,
        "rocketmq-proxy/src/remoting.rs": 1,
    }
    if any(
        exception["path"] not in source_exception_limits
        or exception["count"] > source_exception_limits.get(exception["path"], 0)
        or exception["alias"] != "rocketmq_client_rust"
        or exception["owner"] != "proxy"
        or exception["remove_by"] != "M08"
        for exception in baseline["source_exceptions"]
    ):
        raise InputError("temporary Client source ledger must contain only Proxy M08 source entries")
    compatibility_identities: set[tuple[Any, ...]] = set()
    for index, exception in enumerate(baseline["compatibility_manifest_exceptions"]):
        require_keys(
            exception,
            (
                "rule",
                "caller",
                "target",
                "kind",
                "path",
                "alias",
                "count",
                "owner",
                "reason",
                "remove_by",
                "adr",
            ),
            f"baseline compatibility_manifest_exceptions[{index}]",
        )
        if exception["rule"] != "compatibility-manifest-burn-down":
            raise InputError(f"baseline compatibility_manifest_exceptions[{index}] has an invalid rule")
        if not isinstance(exception["count"], int) or exception["count"] < 0:
            raise InputError(f"baseline compatibility_manifest_exceptions[{index}] count must be non-negative")
        if not exception["reason"] or not exception["adr"]:
            raise InputError(f"baseline compatibility_manifest_exceptions[{index}] requires reason and adr")
        identity = (
            exception["caller"],
            exception["target"],
            exception["kind"],
            exception["path"],
            exception["alias"],
        )
        if identity in compatibility_identities:
            raise InputError(f"duplicate compatibility manifest exception identity: {identity}")
        compatibility_identities.add(identity)


def read_metadata(metadata_file: Path | None) -> dict[str, Any]:
    if metadata_file is not None:
        return load_json(metadata_file, "metadata")
    completed = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        cwd=ROOT,
        capture_output=True,
        text=True,
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


def normalized_metadata_summary(metadata: dict[str, Any], source_root: Path) -> list[dict[str, Any]]:
    """Build a path-normalized, order-stable workspace metadata summary."""
    summary: list[dict[str, Any]] = []
    for package in sorted(workspace_packages(metadata), key=lambda item: item["name"]):
        dependencies = [
            {
                "name": dependency.get("name"),
                "kind": dependency.get("kind") or "normal",
                "rename": dependency.get("rename"),
                "target": dependency.get("target"),
            }
            for dependency in package.get("dependencies", [])
        ]
        summary.append(
            {
                "name": package["name"],
                "version": package.get("version"),
                "manifest_path": normalize_manifest_path(package.get("manifest_path", ""), source_root),
                "dependencies": sorted(
                    dependencies,
                    key=lambda item: (
                        item["name"] or "",
                        item["kind"],
                        item["rename"] or "",
                        str(item["target"] or ""),
                    ),
                ),
            }
        )
    return summary


def normalized_metadata_sha256(metadata: dict[str, Any], source_root: Path) -> str:
    summary = normalized_metadata_summary(metadata, source_root)
    encoded = json.dumps(summary, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def write_and_verify_metadata_evidence(
    metadata: dict[str, Any], source_root: Path, baseline: dict[str, Any]
) -> None:
    output = ROOT / baseline["generated_output_path"]
    summary = normalized_metadata_summary(metadata, source_root)
    try:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        reloaded = json.loads(output.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise InputError(f"cannot generate normalized metadata evidence {output}: {error}") from error
    encoded = json.dumps(reloaded, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    if digest != baseline["metadata_sha256"]:
        raise InputError(
            "generated normalized metadata evidence hash mismatch: "
            f"expected={baseline['metadata_sha256']} actual={digest}"
        )


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


def target_dag_findings(
    edges: list[Edge], policy: dict[str, Any], mode: str
) -> list[Finding]:
    planned = set(policy["planned_packages"])
    findings: list[Finding] = []
    for edge in edges:
        allowed = policy["target_dag"].get(edge.caller)
        if allowed is None or (mode == "baseline" and edge.caller not in planned):
            continue
        if edge.target not in set(allowed):
            findings.append(
                Finding(
                    "target-dag-direct-dependency",
                    edge.caller,
                    edge.target,
                    edge.path,
                    edge.kind,
                )
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
                        f"remove_by={rule['remove_by']}",
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


def manifest_client_findings(
    edges: list[Edge], mode: str, policy: dict[str, Any], baseline: dict[str, Any]
) -> list[Finding]:
    client = policy["client_policy"]["package"]
    findings: list[Finding] = []
    client_edges = [edge for edge in edges if edge.target == client and edge.caller != client]
    actual = Counter(
        (edge.caller, edge.target, edge.kind, edge.path, edge.alias)
        for edge in client_edges
    )
    allowed = Counter(
        (
            entry["caller"],
            entry["target"],
            entry["kind"],
            entry["path"],
            entry["alias"],
        )
        for entry in policy["client_policy"]["target_manifest_allowlist"]
    )
    if mode == "baseline":
        permitted = allowed + Counter(
            (
                item["caller"],
                item["target"],
                item.get("kind", "normal"),
                item["path"],
                item["alias"],
            )
            for item in baseline["manifest_exceptions"]
            if item.get("rule") is None
            for _ in range(item.get("count", 1))
        )
        for identity, count in sorted(actual.items()):
            baseline_count = permitted[identity]
            if count <= baseline_count:
                continue
            caller, target, kind, path, alias = identity
            findings.append(
                Finding(
                    "client-manifest-baseline-growth",
                    caller,
                    target,
                    path,
                    kind,
                    f"alias={alias} count={count} baseline={baseline_count}",
                )
            )
        return findings

    for identity, count in sorted(actual.items()):
        permitted = allowed[identity]
        if count <= permitted:
            continue
        caller, target, kind, path, alias = identity
        findings.append(
            Finding(
                "client-manifest-allowlist",
                caller,
                target,
                path,
                kind,
                f"alias={alias} count={count} allowlist={permitted}",
            )
        )
    return findings


def compatibility_manifest_findings(
    edges: list[Edge], mode: str, policy: dict[str, Any], baseline: dict[str, Any]
) -> list[Finding]:
    if mode != "baseline":
        return []
    ledger = baseline["compatibility_manifest_exceptions"]
    targets = set(policy["compatibility_manifest_policy"]["targets"])
    planned_identities = {(item["caller"], item["target"]) for item in ledger}
    relevant = [
        edge
        for edge in edges
        if edge.caller != edge.target
        and (edge.target in targets or (edge.caller, edge.target) in planned_identities)
    ]
    actual = Counter(
        (edge.caller, edge.target, edge.kind, edge.path, edge.alias)
        for edge in relevant
    )
    expected = Counter(
        (
            item["caller"],
            item["target"],
            item["kind"],
            item["path"],
            item["alias"],
        )
        for item in ledger
        for _ in range(item["count"])
    )
    findings: list[Finding] = []
    for identity, count in sorted(actual.items()):
        permitted = expected[identity]
        if count <= permitted:
            continue
        caller, target, kind, path, alias = identity
        findings.append(
            Finding(
                "compatibility-manifest-baseline-growth",
                caller,
                target,
                path,
                kind,
                f"graph=direct alias={alias} count={count} baseline={permitted}",
            )
        )
    return findings


def apply_baseline_exceptions(findings: list[Finding], baseline: dict[str, Any]) -> list[Finding]:
    """Suppress only findings that exactly match a reviewed, expiring exception."""
    remaining: list[Finding] = []
    for finding in findings:
        matched = False
        for exception in baseline["manifest_exceptions"]:
            rule = exception.get("rule")
            if rule is None:
                continue
            if (
                rule == finding.rule
                and exception.get("caller") == finding.caller
                and exception.get("target") == finding.target
                and exception.get("kind", finding.kind) == finding.kind
                and exception.get("path", finding.path) == finding.path
                and exception.get("detail", finding.detail) == finding.detail
            ):
                matched = True
                break
        if not matched:
            remaining.append(finding)
    return remaining


def source_aliases_by_caller(edges: list[Edge], policy: dict[str, Any]) -> dict[str, set[str]]:
    client = policy["client_policy"]["package"]
    aliases: dict[str, set[str]] = {}
    for edge in edges:
        if edge.target == client and edge.caller != client and edge.alias:
            aliases.setdefault(edge.caller, set()).add(edge.alias)
    return aliases


def source_caller(relative: str, packages: list[dict[str, Any]]) -> str:
    normalized = relative.replace("\\", "/")
    special = {
        "rocketmq-tools/rocketmq-admin/rocketmq-admin-core/": "rocketmq-admin-core",
        "rocketmq-tools/rocketmq-mcp/": "rocketmq-mcp",
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


def source_client_occurrence_allowed(
    relative: str, caller: str, alias: str, policy: dict[str, Any]
) -> bool:
    return any(
        caller == entry["caller"]
        and relative.startswith(entry["path_prefix"])
        and alias in entry["aliases"]
        for entry in policy["client_policy"]["target_source_allowlist"]
    )


def lex_rust_code(text: str, path: Path) -> str:
    """Mask Rust comments and literals while preserving code and line numbers."""
    output = list(text)

    def mask(start: int, end: int) -> None:
        for position in range(start, end):
            if output[position] not in ("\n", "\r"):
                output[position] = " "

    def quoted_end(start_quote: int, quote: str) -> int:
        position = start_quote + 1
        while position < len(text):
            if text[position] == "\\":
                position += 2
                continue
            if text[position] == quote:
                return position + 1
            if quote == "'" and text[position] in "\r\n":
                break
            position += 1
        kind = "string" if quote == '"' else "character"
        raise InputError(f"unterminated Rust {kind} literal in {path}")

    position = 0
    while position < len(text):
        if text.startswith("//", position):
            end = text.find("\n", position + 2)
            end = len(text) if end == -1 else end
            mask(position, end)
            position = end
            continue
        if text.startswith("/*", position):
            start = position
            depth = 1
            position += 2
            while position < len(text) and depth:
                if text.startswith("/*", position):
                    depth += 1
                    position += 2
                elif text.startswith("*/", position):
                    depth -= 1
                    position += 2
                else:
                    position += 1
            if depth:
                raise InputError(f"unterminated Rust block comment in {path}")
            mask(start, position)
            continue

        raw_match = re.match(r"(?:br|r)(#{0,255})\"", text[position:])
        if raw_match is not None:
            start = position
            hashes = raw_match.group(1)
            content_start = position + raw_match.end()
            terminator = '"' + hashes
            end_marker = text.find(terminator, content_start)
            if end_marker == -1:
                raise InputError(f"unterminated Rust raw string literal in {path}")
            position = end_marker + len(terminator)
            mask(start, position)
            continue

        if text.startswith('b"', position):
            start = position
            position = quoted_end(position + 1, '"')
            mask(start, position)
            continue
        if text[position] == '"':
            start = position
            position = quoted_end(position, '"')
            mask(start, position)
            continue
        if text.startswith("b'", position):
            start = position
            position = quoted_end(position + 1, "'")
            mask(start, position)
            continue
        if text[position] == "'":
            lifetime = re.match(r"'[A-Za-z_][A-Za-z0-9_]*", text[position:])
            if lifetime is not None:
                following = position + lifetime.end()
                if following >= len(text) or text[following] != "'":
                    position = following
                    continue
            start = position
            position = quoted_end(position, "'")
            mask(start, position)
            continue
        position += 1
    return "".join(output)


def source_client_findings(
    source_root: Path,
    packages: list[dict[str, Any]],
    edges: list[Edge],
    mode: str,
    policy: dict[str, Any],
    baseline: dict[str, Any],
) -> list[Finding]:
    if not source_root.exists() or not source_root.is_dir():
        raise InputError(f"source root is not a directory: {source_root}")
    aliases_by_caller = source_aliases_by_caller(edges, policy)
    client_package = policy["client_policy"]["package"]
    occurrences: list[tuple[str, str, str, int]] = []
    ignored_parts = {".git", "target", "node_modules"}
    for path in sorted(source_root.rglob("*.rs")):
        if any(part in ignored_parts for part in path.parts):
            continue
        relative = path.relative_to(source_root).as_posix()
        caller = source_caller(relative, packages)
        if caller == client_package:
            continue
        aliases = aliases_by_caller.get(caller, set())
        if not aliases:
            continue
        try:
            source = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as error:
            raise InputError(f"cannot read Rust source {path}: {error}") from error
        if not any(alias in source for alias in aliases):
            continue
        lines = lex_rust_code(source, path).splitlines()
        for line_number, line in enumerate(lines, start=1):
            matched = False
            for alias in sorted(aliases):
                matches = list(re.finditer(rf"\b{re.escape(alias)}\s*::", line))
                for _ in matches:
                    occurrences.append((relative, caller, alias, line_number))
                    matched = True
            if not matched:
                match = CLIENT_USE_RE.search(line)
                if match is not None and match.group(1) in aliases:
                    occurrences.append((relative, caller, match.group(1), line_number))

    findings: list[Finding] = []
    if mode == "baseline":
        temporary_occurrences = [
            occurrence
            for occurrence in occurrences
            if not source_client_occurrence_allowed(
                occurrence[0], occurrence[1], occurrence[2], policy
            )
        ]
        actual = Counter((path, alias) for path, _, alias, _ in temporary_occurrences)
        expected = Counter(
            (item["path"], item["alias"])
            for item in baseline["source_exceptions"]
            for _ in range(item["count"])
        )
        callers = {
            (path, alias): caller
            for path, caller, alias, _ in temporary_occurrences
        }
        for identity, count in sorted(actual.items()):
            permitted = expected[identity]
            if count <= permitted:
                continue
            path, alias = identity
            findings.append(
                Finding(
                    "client-source-baseline-growth",
                    callers[identity],
                    client_package,
                    path,
                    "source",
                    f"alias={alias} count={count} baseline={permitted}",
                )
            )
        return findings

    for relative, caller, alias, line_number in occurrences:
        if source_client_occurrence_allowed(relative, caller, alias, policy):
            continue
        findings.append(
            Finding(
                "client-source-allowlist",
                caller,
                client_package,
                f"{relative}:{line_number}",
                "source",
                f"alias={alias}",
            )
        )
    return findings


def validate_package_state(
    mode: str,
    packages: list[dict[str, Any]],
    policy: dict[str, Any],
    baseline: dict[str, Any],
    allow_missing: bool,
) -> list[str]:
    names = sorted(item["name"] for item in packages)
    messages: list[str] = []
    if mode == "baseline":
        frozen = set(baseline["workspace_packages"])
        planned = set(policy["planned_packages"])
        removed = sorted(frozen - set(names))
        unplanned = sorted(set(names) - frozen - planned)
        if removed:
            raise InputError(f"baseline workspace packages were removed: {', '.join(removed)}")
        if unplanned:
            raise InputError(f"unplanned workspace packages: {', '.join(unplanned)}")
        if len(frozen) != policy["package_counts"]["baseline"]:
            raise InputError(f"frozen baseline must contain {policy['package_counts']['baseline']} packages")
    else:
        missing = sorted(set(policy["planned_packages"]) - set(names))
        if missing and not allow_missing:
            raise InputError(f"missing planned packages: {', '.join(missing)}")
        if missing:
            messages.append(f"TARGET_INCOMPLETE missing_planned_packages={','.join(missing)}")
        elif len(names) != policy["package_counts"]["target"]:
            raise InputError(f"target workspace must contain {policy['package_counts']['target']} packages")
    return messages


def evaluate(
    mode: str,
    metadata: dict[str, Any],
    source_root: Path,
    policy: dict[str, Any],
    baseline: dict[str, Any],
    allow_missing: bool,
) -> tuple[list[Finding], list[str]]:
    packages = workspace_packages(metadata)
    messages = validate_package_state(mode, packages, policy, baseline, allow_missing)
    edges = dependency_edges(packages, source_root)
    edges.extend(standalone_dependency_edges(source_root, policy))
    internal_names = {item["name"] for item in packages}
    internal_edges = [edge for edge in edges if edge.target in internal_names]
    normal_internal_edges = [edge for edge in internal_edges if edge.kind == "normal"]
    findings = cycle_findings(internal_edges)
    findings.extend(package_rule_findings(edges, policy))
    findings.extend(target_dag_findings(internal_edges, policy, mode))
    findings.extend(facade_rule_findings(internal_edges, policy))
    findings.extend(closure_rule_findings(normal_internal_edges, policy))
    if mode == "baseline":
        findings = apply_baseline_exceptions(findings, baseline)
    findings.extend(manifest_client_findings(edges, mode, policy, baseline))
    findings.extend(compatibility_manifest_findings(edges, mode, policy, baseline))
    findings.extend(source_client_findings(source_root, packages, edges, mode, policy, baseline))
    return sorted(findings, key=lambda item: item.render()), messages


def fixture_metadata(packages: list[tuple[str, list[tuple[str, str, str | None]]]]) -> dict[str, Any]:
    values: list[dict[str, Any]] = []
    for name, dependencies in packages:
        values.append(
            {
                "name": name,
                "id": f"fixture:{name}",
                "manifest_path": f"/fixture/{name}/Cargo.toml",
                "dependencies": [
                    {"name": target, "kind": kind, "rename": alias, "target": None}
                    for target, kind, alias in dependencies
                ],
            }
        )
    return {"packages": values, "workspace_members": [item["id"] for item in values]}


def run_fixtures(policy: dict[str, Any], baseline: dict[str, Any]) -> int:
    cases = {
        "cycle": (
            fixture_metadata([("a", [("b", "normal", None)]), ("b", [("a", "dev", None)])]),
            "dependency-cycle",
        ),
        "protocol-transport": (
            fixture_metadata(
                [("rocketmq-protocol", [("rocketmq-transport", "normal", None)]), ("rocketmq-transport", [])]
            ),
            "protocol-no-transport",
        ),
        "store-api-backend": (
            fixture_metadata(
                [("rocketmq-store-api", [("rocketmq-store-local", "build", None)]), ("rocketmq-store-local", [])]
            ),
            "store-api-no-backend",
        ),
        "proxy-local-client": (
            fixture_metadata(
                [
                    ("rocketmq-proxy-local", [("rocketmq-client-rust", "dev", None)]),
                    ("rocketmq-client-rust", []),
                ]
            ),
            "proxy-local-no-client",
        ),
        "foundation-facade": (
            fixture_metadata(
                [("rocketmq-model", [("rocketmq-remoting", "normal", None)]), ("rocketmq-remoting", [])]
            ),
            "foundation-no-facade",
        ),
    }
    failures: list[str] = []

    with tempfile.TemporaryDirectory() as temp_dir:
        source_root = Path(temp_dir)
        clean = fixture_metadata([("rocketmq-model", []), ("rocketmq-protocol", [])])
        findings, _ = evaluate("target", clean, source_root, policy, baseline, True)
        if findings:
            failures.append("clean fixture produced findings")
        for name, (metadata, expected) in cases.items():
            findings, _ = evaluate("target", metadata, source_root, policy, baseline, True)
            if expected not in {finding.rule for finding in findings}:
                failures.append(f"{name} did not produce {expected}")
        alias_metadata = fixture_metadata(
            [
                ("rocketmq-broker", [("rocketmq-client-rust", "normal", "mq_client")]),
                ("rocketmq-client-rust", []),
            ]
        )
        alias_source = source_root / "rocketmq-broker" / "src" / "lib.rs"
        alias_source.parent.mkdir(parents=True)
        alias_source.write_text("use mq_client::producer::DefaultMQProducer;\n", encoding="utf-8")
        alias_findings, _ = evaluate("target", alias_metadata, source_root, policy, baseline, True)
        if "client-source-allowlist" not in {finding.rule for finding in alias_findings}:
            failures.append("client-source-alias did not produce client-source-allowlist")
    if failures:
        for failure in failures:
            print(f"FIXTURE_FAILURE {failure}", file=sys.stderr)
        return 1
    print(f"FIXTURES_OK clean=1 violations={len(cases) + 1}")
    return 0


def write_output(path: Path, mode: str, findings: list[Finding], messages: list[str]) -> None:
    incomplete = any(message.startswith("TARGET_INCOMPLETE") for message in messages)
    payload = {
        "schema_version": 1,
        "mode": mode,
        "status": "incomplete" if incomplete else ("compliant" if not findings else "violation"),
        "messages": messages,
        "findings": [dataclasses.asdict(finding) for finding in findings],
    }
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except OSError as error:
        raise InputError(f"cannot write output {path}: {error}") from error


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("baseline", "target"), default="baseline")
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--metadata-file", type=Path)
    parser.add_argument("--source-root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--fixtures", action="store_true")
    parser.add_argument("--allow-missing-planned-crates", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        policy = load_json(args.policy, "policy")
        baseline = load_json(args.baseline, "baseline")
        validate_policy(policy)
        validate_baseline(baseline, policy)
        if args.fixtures:
            return run_fixtures(policy, baseline)
        metadata = read_metadata(args.metadata_file)
        current_names = {item["name"] for item in workspace_packages(metadata)}
        if (
            args.mode == "baseline"
            and args.metadata_file is None
            and current_names == set(baseline["workspace_packages"])
        ):
            digest = normalized_metadata_sha256(metadata, args.source_root.resolve())
            if digest != baseline["metadata_sha256"]:
                raise InputError(
                    "normalized cargo metadata SHA-256 drift: "
                    f"expected={baseline['metadata_sha256']} actual={digest}"
                )
            write_and_verify_metadata_evidence(metadata, args.source_root.resolve(), baseline)
        findings, messages = evaluate(
            args.mode,
            metadata,
            args.source_root.resolve(),
            policy,
            baseline,
            args.allow_missing_planned_crates,
        )
        for message in messages:
            print(message)
        for finding in findings:
            print(finding.render())
        if args.output is not None:
            write_output(args.output, args.mode, findings, messages)
        if findings:
            print(f"ARCHITECTURE_DEPENDENCY_GUARD_FAILED findings={len(findings)}")
            return 1
        if any(message.startswith("TARGET_INCOMPLETE") for message in messages):
            print("ARCHITECTURE_DEPENDENCY_GUARD_INCOMPLETE")
            return 1
        print(f"ARCHITECTURE_DEPENDENCY_GUARD_OK mode={args.mode}")
        return 0
    except InputError as error:
        print(f"INPUT_ERROR {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
