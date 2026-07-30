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

"""Validate risk, deterministic-property, fuzz, and coverage evidence registries."""

from __future__ import annotations

import json
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
RISK_MATRIX = Path("scripts/architecture-risk-test-matrix.json")
DEBT_REGISTRY = Path("scripts/architecture-debt-registry.json")
PROPERTY_REGISTRY = Path("scripts/property-state-suite-registry.json")
FUZZ_REGISTRY = Path("fuzz/corpus-registry.json")
FUZZ_MANIFEST = Path("fuzz/Cargo.toml")
FUZZ_WORKFLOW = Path(".github/workflows/fuzz-ci.yml")
CODECOV_CONFIG = Path("codecov.yml")
INVENTORY = Path("scripts/architecture-validation-inventory.json")


@dataclass(frozen=True)
class Finding:
    code: str
    path: str
    detail: str

    def render(self) -> str:
        return f"{self.code}: {self.path}: {self.detail}"


def load_json(root: Path, relative: Path, findings: list[Finding]) -> dict[str, Any] | None:
    try:
        value = json.loads((root / relative).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        findings.append(Finding("input-invalid", relative.as_posix(), str(error)))
        return None
    if not isinstance(value, dict):
        findings.append(Finding("input-invalid", relative.as_posix(), "top-level value must be an object"))
        return None
    return value


def require_path(root: Path, value: object, label: str, findings: list[Finding]) -> None:
    if not isinstance(value, str) or not value or not (root / value).exists():
        findings.append(Finding("path-missing", label, repr(value)))


def validate_risk_matrix(
    root: Path,
    matrix: dict[str, Any],
    debt: dict[str, Any],
) -> list[Finding]:
    findings: list[Finding] = []
    entries = matrix.get("entries")
    debt_entries = debt.get("entries")
    if matrix.get("schema_version") != 1 or not isinstance(entries, list):
        return [Finding("risk-schema", RISK_MATRIX.as_posix(), "schema_version=1 and entries are required")]
    if not isinstance(debt_entries, list):
        return [Finding("debt-schema", DEBT_REGISTRY.as_posix(), "entries are required")]

    expected = {entry.get("id") for entry in debt_entries if isinstance(entry, dict)}
    actual = [entry.get("debt_id") for entry in entries if isinstance(entry, dict)]
    if len(actual) != len(set(actual)):
        findings.append(Finding("risk-duplicate", RISK_MATRIX.as_posix(), "debt_id values must be unique"))
    if set(actual) != expected:
        findings.append(
            Finding(
                "risk-coverage",
                RISK_MATRIX.as_posix(),
                f"missing={sorted(expected - set(actual))}, extra={sorted(set(actual) - expected)}",
            )
        )

    allowed_risks = {"critical", "high", "normal"}
    advanced_kinds = {"contract", "integration", "property", "fuzz", "loom", "miri", "fault", "perf", "soak"}
    required_test_fields = {
        "kind",
        "sources",
        "trigger_paths",
        "command",
        "duration_seconds",
        "platform",
        "features",
        "artifact",
        "behavior_contract",
    }
    for index, entry in enumerate(entries):
        label = f"{RISK_MATRIX.as_posix()}#entries[{index}]"
        if not isinstance(entry, dict):
            findings.append(Finding("risk-entry", label, "entry must be an object"))
            continue
        if entry.get("risk_class") not in allowed_risks or not entry.get("owner"):
            findings.append(Finding("risk-entry", label, "known risk_class and owner are required"))
        tests = entry.get("tests")
        if not isinstance(tests, list) or not tests:
            findings.append(Finding("risk-test-missing", label, "at least one failing behavior contract is required"))
            continue
        behavior_contracts = 0
        advanced = 0
        for test_index, test in enumerate(tests):
            test_label = f"{label}.tests[{test_index}]"
            if not isinstance(test, dict) or set(test) != required_test_fields:
                findings.append(Finding("risk-test-schema", test_label, "unexpected test fields"))
                continue
            if test["behavior_contract"] is True:
                behavior_contracts += 1
            if test["kind"] in advanced_kinds:
                advanced += 1
            if not isinstance(test["duration_seconds"], int) or test["duration_seconds"] <= 0:
                findings.append(Finding("risk-duration", test_label, "duration_seconds must be positive"))
            if not isinstance(test["trigger_paths"], list) or not test["trigger_paths"]:
                findings.append(Finding("risk-trigger", test_label, "trigger_paths must be non-empty"))
            if "<commit>" not in str(test["artifact"]):
                findings.append(Finding("risk-artifact", test_label, "artifact must be commit-bound"))
            for source in test["sources"] if isinstance(test["sources"], list) else []:
                require_path(root, source, test_label, findings)
        if behavior_contracts == 0:
            findings.append(Finding("risk-behavior", label, "a regression-sensitive behavior contract is required"))
        if entry.get("risk_class") in {"critical", "high"} and advanced == 0:
            findings.append(Finding("risk-depth", label, "critical/high risks cannot rely on line coverage alone"))
    return findings


def validate_property_registry(root: Path, registry: dict[str, Any]) -> list[Finding]:
    findings: list[Finding] = []
    suites = registry.get("suites")
    minimum = registry.get("minimum_categories")
    decision = registry.get("generator_decision")
    if registry.get("schema_version") != 1 or not isinstance(suites, list) or not isinstance(minimum, int):
        return [Finding("property-schema", PROPERTY_REGISTRY.as_posix(), "invalid registry schema")]
    if not isinstance(decision, dict) or set(decision.get("rejected", {})) != {"proptest", "quickcheck"}:
        findings.append(
            Finding("property-decision", PROPERTY_REGISTRY.as_posix(), "generator choice and both alternatives are required")
        )
    categories: set[str] = set()
    ids: list[str] = []
    required_fields = {
        "id",
        "category",
        "owner",
        "source",
        "test",
        "seed",
        "command",
        "expected_tests",
        "pr_cases",
        "nightly_cases",
        "external_network",
        "fixed_ports",
        "arbitrary_sleep",
    }
    for index, suite in enumerate(suites):
        label = f"{PROPERTY_REGISTRY.as_posix()}#suites[{index}]"
        if not isinstance(suite, dict) or set(suite) != required_fields:
            findings.append(Finding("property-suite-schema", label, "unexpected suite fields"))
            continue
        ids.append(suite["id"])
        categories.add(suite["category"])
        require_path(root, suite["source"], label, findings)
        source_path = root / suite["source"]
        if source_path.is_file() and str(suite["test"]) not in source_path.read_text(encoding="utf-8"):
            findings.append(Finding("property-test-missing", label, suite["test"]))
        if not re.fullmatch(r"0x[0-9A-Fa-f]{16}", str(suite["seed"])):
            findings.append(Finding("property-seed", label, "seed must be a replayable 64-bit hexadecimal value"))
        command = suite["command"]
        if (
            not isinstance(command, list)
            or any(not isinstance(argument, str) or not argument for argument in command)
            or command[:2] != ["cargo", "test"]
            or "--exact" not in command
            or "--" not in command
            or not any(str(suite["test"]) in argument for argument in command)
        ):
            findings.append(
                Finding(
                    "property-command",
                    label,
                    "command must run the named Cargo test exactly without a shell",
                )
            )
        if not isinstance(suite["expected_tests"], int) or suite["expected_tests"] <= 0:
            findings.append(Finding("property-test-count", label, "expected_tests must be positive"))
        if not isinstance(suite["pr_cases"], int) or suite["pr_cases"] <= 0:
            findings.append(Finding("property-cases", label, "pr_cases must be positive"))
        if not isinstance(suite["nightly_cases"], int) or suite["nightly_cases"] < suite["pr_cases"]:
            findings.append(Finding("property-cases", label, "nightly_cases must cover every PR case"))
        for isolation in ("external_network", "fixed_ports", "arbitrary_sleep"):
            if suite[isolation] is not False:
                findings.append(Finding("property-isolation", label, f"{isolation} must be false"))
    if len(ids) != len(set(ids)):
        findings.append(Finding("property-duplicate", PROPERTY_REGISTRY.as_posix(), "suite ids must be unique"))
    if len(categories) < minimum or minimum < 5:
        findings.append(
            Finding(
                "property-category-count",
                PROPERTY_REGISTRY.as_posix(),
                f"found {len(categories)}, require at least {max(minimum, 5)}",
            )
        )
    return findings


def validate_fuzz_registry(root: Path, registry: dict[str, Any]) -> list[Finding]:
    findings: list[Finding] = []
    targets = registry.get("targets")
    crash_policy = registry.get("crash_policy")
    if registry.get("schema_version") != 1 or not isinstance(targets, list):
        return [Finding("fuzz-schema", FUZZ_REGISTRY.as_posix(), "invalid registry schema")]
    if not isinstance(crash_policy, dict) or set(crash_policy) != {
        "crash",
        "timeout",
        "out_of_memory",
        "retention_days",
        "artifact",
    }:
        findings.append(Finding("fuzz-crash-policy", FUZZ_REGISTRY.as_posix(), "complete crash classification required"))
    elif len({crash_policy["crash"], crash_policy["timeout"], crash_policy["out_of_memory"]}) != 3:
        findings.append(Finding("fuzz-crash-policy", FUZZ_REGISTRY.as_posix(), "crash/timeout/OOM must be distinct"))

    with (root / FUZZ_MANIFEST).open("rb") as stream:
        manifest = tomllib.load(stream)
    manifest_targets = {entry["name"] for entry in manifest.get("bin", [])}
    registered_targets = {entry.get("name") for entry in targets if isinstance(entry, dict)}
    if manifest_targets != registered_targets:
        findings.append(
            Finding(
                "fuzz-target-coverage",
                FUZZ_REGISTRY.as_posix(),
                f"manifest={sorted(manifest_targets)}, registry={sorted(registered_targets)}",
            )
        )

    required_fields = {
        "name",
        "owner",
        "harness",
        "production_entrypoint",
        "invariants",
        "corpus",
        "dictionary",
        "max_input_bytes",
        "pr_timeout_seconds",
        "nightly_timeout_seconds",
        "weekly_timeout_seconds",
        "retention_days",
    }
    workflow = (root / FUZZ_WORKFLOW).read_text(encoding="utf-8")
    for index, target in enumerate(targets):
        label = f"{FUZZ_REGISTRY.as_posix()}#targets[{index}]"
        if not isinstance(target, dict) or set(target) != required_fields:
            findings.append(Finding("fuzz-target-schema", label, "unexpected target fields"))
            continue
        require_path(root, target["harness"], label, findings)
        require_path(root, target["corpus"], label, findings)
        if not target["owner"] or not target["production_entrypoint"]:
            findings.append(Finding("fuzz-owner", label, "owner and production entrypoint are required"))
        if not isinstance(target["invariants"], list) or not target["invariants"]:
            findings.append(Finding("fuzz-invariant", label, "at least one invariant is required"))
        limits = [
            target["max_input_bytes"],
            target["pr_timeout_seconds"],
            target["nightly_timeout_seconds"],
            target["weekly_timeout_seconds"],
            target["retention_days"],
        ]
        if any(not isinstance(value, int) or value <= 0 for value in limits):
            findings.append(Finding("fuzz-limit", label, "input, timeout, and retention limits must be positive"))
        if not target["pr_timeout_seconds"] < target["nightly_timeout_seconds"] < target["weekly_timeout_seconds"]:
            findings.append(Finding("fuzz-timeout-order", label, "PR < nightly < weekly is required"))
        if re.search(rf"(?m)^\s+- {re.escape(target['name'])}\s*$", workflow) is None:
            findings.append(Finding("fuzz-workflow-target", label, "target is absent from workflow matrix"))
        corpus = root / target["corpus"]
        if corpus.is_dir() and not any(path.is_file() for path in corpus.iterdir()):
            findings.append(Finding("fuzz-corpus-empty", label, "curated corpus needs at least one seed"))
    for required in ("pull_request", "'10'", "'60'", "'900'", "architecture_evidence_governance_guard.py"):
        if required not in workflow:
            findings.append(Finding("fuzz-workflow-policy", FUZZ_WORKFLOW.as_posix(), required))
    return findings


def validate_coverage(root: Path, inventory: dict[str, Any]) -> list[Finding]:
    findings: list[Finding] = []
    patch_target = inventory.get("coverage", {}).get("patch_target")
    codecov = (root / CODECOV_CONFIG).read_text(encoding="utf-8")
    match = re.search(r"(?ms)^\s+patch:\s+default:\s+target:\s+(\d+%)", codecov)
    if patch_target != "70%" or match is None or match.group(1) != patch_target:
        findings.append(
            Finding(
                "coverage-target",
                CODECOV_CONFIG.as_posix(),
                f"approved/current targets must both be 70%, inventory={patch_target}, codecov={match.group(1) if match else None}",
            )
        )
    return findings


def validate(root: Path = ROOT) -> list[Finding]:
    findings: list[Finding] = []
    risk = load_json(root, RISK_MATRIX, findings)
    debt = load_json(root, DEBT_REGISTRY, findings)
    properties = load_json(root, PROPERTY_REGISTRY, findings)
    fuzz = load_json(root, FUZZ_REGISTRY, findings)
    inventory = load_json(root, INVENTORY, findings)
    if risk is not None and debt is not None:
        findings.extend(validate_risk_matrix(root, risk, debt))
    if properties is not None:
        findings.extend(validate_property_registry(root, properties))
    if fuzz is not None:
        findings.extend(validate_fuzz_registry(root, fuzz))
    if inventory is not None:
        findings.extend(validate_coverage(root, inventory))
    return findings


def main() -> int:
    findings = validate()
    if findings:
        for finding in findings:
            print(finding.render())
        return 1
    print("ARCHITECTURE_EVIDENCE_GOVERNANCE_OK risk=14 property_categories=7 fuzz_targets=4 patch=70%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
