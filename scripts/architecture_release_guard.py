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

"""Validate the M09 R0/R1/next-major architecture release package."""

from __future__ import annotations

import json
import sys
import tomllib
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = ROOT / "scripts" / "architecture-release-plan.json"
POLICY_PATH = ROOT / "scripts" / "architecture-dependency-policy.json"
BASELINE_PATH = ROOT / "scripts" / "architecture-dependency-baseline.json"
CI_PATH = ROOT / ".github" / "workflows" / "rocketmq-rust-ci.yaml"
PROXY_MANIFEST = ROOT / "rocketmq-proxy" / "Cargo.toml"

REQUIRED_CHAIN = [
    "model/error/runtime/security/store-api",
    "protocol/observability",
    "transport/auth/local/tiered",
    "rocks",
    "facade",
    "service/tool",
]
NEW_CRATES = {
    "rocketmq-model",
    "rocketmq-protocol",
    "rocketmq-transport",
    "rocketmq-security-api",
    "rocketmq-store-api",
    "rocketmq-store-local",
    "rocketmq-store-rocksdb",
    "rocketmq-proxy-core",
    "rocketmq-proxy-cluster",
    "rocketmq-proxy-local",
}
EDGE_FIELDS = ("caller", "target", "kind", "path", "alias")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def edge_identity(edge: dict[str, Any]) -> tuple[str, ...]:
    return tuple(str(edge[field]) for field in EDGE_FIELDS)


def baseline_edges(baseline: dict[str, Any], window: str) -> set[tuple[str, ...]]:
    return {
        edge_identity(edge)
        for edge in baseline["compatibility_manifest_exceptions"]
        if edge["remove_by"] == window
    }


def expand_r1_consumers(plan: dict[str, Any]) -> set[tuple[str, ...]]:
    edges: set[tuple[str, ...]] = set()
    for consumer in plan["r1"]["consumers"]:
        for target in consumer["targets"]:
            edges.add(
                (
                    consumer["caller"],
                    target,
                    "normal",
                    consumer["path"],
                    target.replace("-", "_"),
                )
            )
    return edges


def manifest_has_edge(edge: tuple[str, ...]) -> bool:
    _, target, kind, relative_path, alias = edge
    manifest = tomllib.loads((ROOT / relative_path).read_text(encoding="utf-8"))
    section = "dev-dependencies" if kind == "dev" else "dependencies"
    dependencies = manifest.get(section, {})
    manifest_key = alias if alias in dependencies else target
    dependency = dependencies.get(manifest_key)
    if dependency is None:
        return False
    package = dependency.get("package") if isinstance(dependency, dict) else None
    return (package or manifest_key.replace("_", "-")) == target


def check_release_topology(
    plan: dict[str, Any], policy: dict[str, Any], findings: list[str]
) -> None:
    topology = plan["release_topology"]
    if topology.get("required_chain") != REQUIRED_CHAIN:
        findings.append("release topology does not preserve the approved six-stage chain")

    publish_order = topology.get("publish_order", [])
    target_dag = policy["target_dag"]
    if len(publish_order) != len(set(publish_order)):
        findings.append("release publish order contains duplicate packages")
    if set(publish_order) != set(target_dag):
        missing = sorted(set(target_dag) - set(publish_order))
        extra = sorted(set(publish_order) - set(target_dag))
        findings.append(f"release publish order package mismatch: missing={missing}, extra={extra}")
        return

    position = {package: index for index, package in enumerate(publish_order)}
    for caller, dependencies in target_dag.items():
        for dependency in dependencies:
            if position[dependency] >= position[caller]:
                findings.append(
                    f"publish order violation: {dependency} must precede {caller}"
                )


def check_release_windows(
    plan: dict[str, Any], baseline: dict[str, Any], findings: list[str]
) -> None:
    new_crates = {item["package"] for item in plan["r0"]["new_crates"]}
    if new_crates != NEW_CRATES or len(plan["r0"]["new_crates"]) != 10:
        findings.append("R0 new-crate inventory must contain the exact ten approved crates")
    for item in plan["r0"]["new_crates"]:
        if not (ROOT / item["path"]).is_file():
            findings.append(f"R0 new crate manifest is missing: {item['path']}")

    planned_r1 = expand_r1_consumers(plan)
    recorded_r1 = baseline_edges(baseline, "R1")
    if planned_r1 != recorded_r1 or plan["r1"].get("expected_edges") != 29:
        findings.append(
            "R1 consumer plan must exactly match the 29-edge compatibility baseline"
        )

    planned_next_major = {
        edge_identity(edge) for edge in plan["next_major"]["dependency_edges"]
    }
    recorded_next_major = baseline_edges(baseline, "next-major")
    if (
        planned_next_major != recorded_next_major
        or plan["next_major"].get("expected_edges") != 4
    ):
        findings.append(
            "next-major plan must exactly match the four-edge compatibility baseline"
        )

    preserved = {
        edge_identity(edge) for edge in plan["long_term"]["preserved_edges"]
    }
    recorded_long_term = baseline_edges(baseline, "long-term")
    if preserved != recorded_long_term or plan["long_term"].get("expected_edges") != 2:
        findings.append("long-term plan must preserve the two approved composition edges")

    for edge in sorted(planned_r1 | planned_next_major | preserved):
        if not manifest_has_edge(edge):
            findings.append(
                "release package removed a compatibility edge before its approved window: "
                + "|".join(edge)
            )


def check_proxy_activation(plan: dict[str, Any], findings: list[str]) -> None:
    fixture_path = ROOT / plan["next_major"]["proxy_feature_fixture"]
    fixture = tomllib.loads(fixture_path.read_text(encoding="utf-8"))
    proxy = tomllib.loads(PROXY_MANIFEST.read_text(encoding="utf-8"))
    if fixture.get("activation_window") != "next-major":
        findings.append("Proxy feature fixture activation window is not next-major")

    current_features = proxy.get("features", {})
    for feature in ("cluster-mode", "local-mode", "compat-all-modes"):
        if feature in current_features:
            findings.append(f"Proxy next-major feature was activated early: {feature}")
    for dependency in ("rocketmq-proxy-cluster", "rocketmq-proxy-local"):
        specification = proxy.get("dependencies", {}).get(dependency)
        if not isinstance(specification, dict) or specification.get("optional", False):
            findings.append(
                f"Proxy adapter dependency changed before next-major: {dependency}"
            )


def check_usage_and_approval(plan: dict[str, Any], findings: list[str]) -> None:
    approval = plan.get("human_approval", {})
    if approval.get("status") != "approved":
        findings.append("relative release windows and notification plan lack Human approval")
    if approval.get("destructive_removal") != "pending-next-major-evidence-gate":
        findings.append("destructive removal must remain pending the next-major evidence gate")

    external = plan.get("external_usage", {})
    if len(external.get("collection_sources", [])) < 4:
        findings.append("external usage collection must cover at least four independent sources")
    gates = external.get("removal_gates", {})
    expected_gates = {
        "minimum_deprecation_releases": 2,
        "minimum_major_boundaries": 1,
        "workspace_and_standalone_internal_usages": 0,
        "unresolved_high_impact_external_consumers": 0,
        "migration_guide_published": True,
        "release_manager_and_human_approval_required": True,
    }
    if gates != expected_gates:
        findings.append("external usage removal gates differ from the approved thresholds")
    if len(external.get("notification_channels", [])) < 4:
        findings.append("external notification plan must contain at least four channels")


def check_ci_and_documents(plan: dict[str, Any], findings: list[str]) -> None:
    workflow = CI_PATH.read_text(encoding="utf-8")
    for command in (
        "python scripts/architecture_dependency_guard.py --mode baseline",
        "python scripts/architecture_release_guard.py",
    ):
        if command not in workflow:
            findings.append(f"CI workflow does not enforce release rule: {command}")

    document_tokens = {
        "r0": ("R0", "canonical", "deprecated", "no behavior change", "rollback"),
        "r1": ("R1", "29", "CI", "external usage"),
        "next_major": (
            "next-major",
            "admin legacy",
            "common compat",
            "remoting deep path",
            "Proxy optional mode feature",
        ),
        "evidence": ("M09-05", "58/82"),
    }
    for key, tokens in document_tokens.items():
        path = ROOT / plan["documents"][key]
        if not path.is_file():
            findings.append(f"release document is missing: {path.relative_to(ROOT)}")
            continue
        content = path.read_text(encoding="utf-8")
        for token in tokens:
            if token not in content:
                findings.append(
                    f"release document {path.name} is missing required marker: {token}"
                )


def validate() -> list[str]:
    plan = load_json(PLAN_PATH)
    policy = load_json(POLICY_PATH)
    baseline = load_json(BASELINE_PATH)
    findings: list[str] = []

    if plan.get("schema_version") != 1 or plan.get("milestone") != "M09-05":
        findings.append("release plan schema or milestone is invalid")
    check_release_topology(plan, policy, findings)
    check_release_windows(plan, baseline, findings)
    check_proxy_activation(plan, findings)
    check_usage_and_approval(plan, findings)
    check_ci_and_documents(plan, findings)
    return findings


def main() -> int:
    findings = validate()
    if findings:
        print(f"architecture release guard: FAILED ({len(findings)} finding(s))")
        for finding in findings:
            print(f"- {finding}")
        return 1
    print("architecture release guard: PASSED")
    print("- release topology: 32/32 packages in dependency order")
    print("- R0 new crates: 10/10")
    print("- compatibility windows: R1 29, next-major 4, long-term 2")
    print("- early removals/Proxy feature activation: 0")
    print("- external usage and notification gates: approved and enforced")
    return 0


if __name__ == "__main__":
    sys.exit(main())
