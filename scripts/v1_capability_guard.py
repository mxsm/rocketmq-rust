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

"""Validate the RocketMQ Rust 1.0 capability and evidence manifest."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
from pathlib import Path
import sys
from typing import Any

try:
    from scripts.v1_capability_freeze import FREEZE, validate_freeze
except ModuleNotFoundError:
    from v1_capability_freeze import FREEZE, validate_freeze


ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "scripts" / "v1-capability-manifest.json"
SCHEMA_PATH = ROOT / "scripts" / "v1-capability-manifest.schema.json"
REQUIRED_IDS = frozenset(
    {f"F-{number:02d}" for number in range(1, 19)}
    | {f"G-{number:02d}" for number in range(1, 9)}
)
DEFERRED_IDS = frozenset({"G-07", "G-08"})
EXPECTED_EXCLUSIONS = frozenset(
    {"OpenMessaging", "BrokerContainer", "DLedger CommitLog", "Java Controller internal protocols"}
)
IMPLEMENTATION_STATUSES = frozenset(
    {"missing", "placeholder", "partial", "implemented", "unsupported", "not-applicable", "intentionally-unsupported"}
)
EVIDENCE_STATUSES = frozenset({"none", "unit", "component", "interop", "functional-system"})
COMPLETION_STATUSES = frozenset(
    {"blocked", "equivalent", "alternative-equivalent", "not-applicable", "intentionally-unsupported", "deferred-by-scope"}
)
COMPATIBILITY_MODES = frozenset({"wire", "behavior", "rust-native", "not-applicable"})
VARIANCE_CLASSES = frozenset({"java-compatible", "rust-hardening", "rust-enhancement"})
DOMAINS = frozenset({"protocol", "client", "broker", "proxy", "store", "ha", "admin", "release"})
REQUIRED_FIELDS = (
    "capability_id",
    "domain",
    "title",
    "java_baseline",
    "rust_surfaces",
    "profiles",
    "compatibility_mode",
    "implementation_status",
    "evidence_status",
    "test_ids",
    "commands",
    "expected_results",
    "artifacts",
    "ownership",
    "target_phase",
    "target_rc",
    "dependencies",
    "completion_status",
    "variance_class",
)


class CapabilityInputError(ValueError):
    """Raised when a capability input cannot be parsed."""


@dataclass(frozen=True, order=True)
class CapabilityFinding:
    code: str
    path: str
    detail: str

    def as_dict(self) -> dict[str, str]:
        return asdict(self)

    def render(self) -> str:
        return f"V1_CAPABILITY_FINDING code={self.code} path={self.path} detail={self.detail}"


def load_manifest(path: Path = MANIFEST_PATH) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CapabilityInputError(f"cannot load {path}: {error}") from error
    if not isinstance(value, dict):
        raise CapabilityInputError(f"{path} must contain a JSON object")
    return value


def _non_empty_strings(value: object) -> bool:
    return isinstance(value, list) and bool(value) and all(isinstance(item, str) and item for item in value)


def _relative_path(value: object) -> bool:
    if not isinstance(value, str) or not value or "\\" in value or ":" in value:
        return False
    path = Path(value)
    return not path.is_absolute() and ".." not in path.parts


def _finding(findings: list[CapabilityFinding], code: str, path: str, detail: str) -> None:
    findings.append(CapabilityFinding(code, path, detail))


def _validate_record_shape(
    record: dict[str, Any],
    *,
    root: Path,
    findings: list[CapabilityFinding],
) -> None:
    capability_id = record.get("capability_id")
    path = capability_id if isinstance(capability_id, str) else "capabilities"
    for field in REQUIRED_FIELDS:
        if field not in record:
            _finding(findings, "capability-field-missing", path, field)

    if record.get("domain") not in DOMAINS:
        _finding(findings, "domain-invalid", path, repr(record.get("domain")))
    if record.get("compatibility_mode") not in COMPATIBILITY_MODES:
        _finding(findings, "compatibility-mode-invalid", path, repr(record.get("compatibility_mode")))
    if record.get("implementation_status") not in IMPLEMENTATION_STATUSES:
        _finding(findings, "implementation-status-invalid", path, repr(record.get("implementation_status")))
    if record.get("evidence_status") not in EVIDENCE_STATUSES:
        _finding(findings, "evidence-status-invalid", path, repr(record.get("evidence_status")))
    if record.get("completion_status") not in COMPLETION_STATUSES:
        _finding(findings, "completion-status-invalid", path, repr(record.get("completion_status")))
    if record.get("variance_class") not in VARIANCE_CLASSES:
        _finding(findings, "variance-class-invalid", path, repr(record.get("variance_class")))

    for field, code in (
        ("rust_surfaces", "rust-surfaces-missing"),
        ("profiles", "profiles-missing"),
        ("test_ids", "test-ids-missing"),
        ("expected_results", "expected-results-missing"),
    ):
        if not _non_empty_strings(record.get(field)):
            _finding(findings, code, path, field)

    completion = record.get("completion_status")
    commands = record.get("commands")
    if completion != "deferred-by-scope" and not _non_empty_strings(commands):
        _finding(findings, "commands-missing", path, "commands")
    elif completion == "deferred-by-scope" and not isinstance(commands, list):
        _finding(findings, "commands-invalid", path, "deferred commands must be a list")
    if (
        completion != "deferred-by-scope"
        and isinstance(record.get("test_ids"), list)
        and isinstance(commands, list)
        and len(record["test_ids"]) != len(commands)
    ):
        _finding(
            findings,
            "test-command-cardinality-mismatch",
            path,
            f"test_ids={len(record['test_ids'])} commands={len(commands)}",
        )

    surfaces = record.get("rust_surfaces")
    if isinstance(surfaces, list):
        for surface in surfaces:
            if not _relative_path(surface):
                _finding(findings, "rust-surface-path-invalid", path, repr(surface))
            elif not (root / surface).exists():
                _finding(findings, "rust-surface-missing", path, surface)

    baseline = record.get("java_baseline")
    if not isinstance(baseline, dict):
        _finding(findings, "java-baseline-invalid", path, "object required")
    else:
        if baseline.get("version") != "5.5.0" or not _non_empty_strings(baseline.get("symbols")):
            _finding(findings, "java-baseline-invalid", path, "version/symbols")
        if not isinstance(baseline.get("scope"), str) or not baseline["scope"]:
            _finding(findings, "java-baseline-invalid", path, "scope")

    ownership = record.get("ownership")
    roles = ("dri", "reviewer", "release_approver")
    if (
        not isinstance(ownership, dict)
        or any(not isinstance(ownership.get(role), str) or not ownership[role] for role in roles)
        or len({ownership.get(role) for role in roles}) != len(roles)
    ):
        _finding(findings, "ownership-invalid", path, "three distinct roles are required")
    target_phase = record.get("target_phase")
    if not isinstance(target_phase, int) or isinstance(target_phase, bool) or not 0 <= target_phase <= 6:
        _finding(findings, "target-phase-invalid", path, repr(target_phase))
    target_rc = record.get("target_rc")
    if target_rc not in {"1.0.0-rc.1", "deferred"}:
        _finding(findings, "target-rc-invalid", path, repr(target_rc))
    elif completion != "deferred-by-scope" and target_rc == "deferred":
        _finding(findings, "active-target-rc-missing", path, "active capability requires a release candidate")
    elif completion == "deferred-by-scope" and target_rc != "deferred":
        _finding(findings, "deferred-target-rc-invalid", path, repr(target_rc))

    dependencies = record.get("dependencies")
    if not isinstance(dependencies, list) or any(not isinstance(item, str) for item in dependencies):
        _finding(findings, "dependencies-invalid", path, repr(dependencies))
    artifacts = record.get("artifacts")
    if not isinstance(artifacts, list):
        _finding(findings, "artifacts-invalid", path, "list required")
    elif record.get("evidence_status") != "none" and not artifacts:
        _finding(findings, "evidence-artifact-missing", path, "evidence requires an artifact record")
    else:
        for artifact in artifacts:
            if (
                not isinstance(artifact, dict)
                or not _relative_path(artifact.get("path"))
                or not isinstance(artifact.get("run_id"), str)
                or not artifact["run_id"]
            ):
                _finding(findings, "artifact-invalid", path, repr(artifact))


def _validate_status(record: dict[str, Any], findings: list[CapabilityFinding]) -> None:
    capability_id = str(record.get("capability_id", "capabilities"))
    completion = record.get("completion_status")
    implementation = record.get("implementation_status")
    evidence = record.get("evidence_status")
    mode = record.get("compatibility_mode")
    if completion == "intentionally-unsupported":
        _finding(findings, "unsupported-core-capability", capability_id, "core capabilities cannot be exclusions")
    if completion == "deferred-by-scope":
        if capability_id not in DEFERRED_IDS and not capability_id.startswith("D-"):
            _finding(findings, "deferred-capability-not-approved", capability_id, "deferred status is not approved")
        if not isinstance(record.get("deferred_reference"), str) or not record["deferred_reference"]:
            _finding(findings, "deferred-reference-missing", capability_id, "deferred_reference")
        return
    if completion in {"equivalent", "alternative-equivalent"}:
        if implementation != "implemented":
            _finding(findings, "completion-implementation-mismatch", capability_id, str(implementation))
        allowed_evidence = {"component", "interop", "functional-system"}
        if mode == "wire":
            allowed_evidence = {"interop", "functional-system"}
        if mode == "rust-native":
            allowed_evidence = {"functional-system"}
        if evidence not in allowed_evidence:
            _finding(findings, "completion-evidence-mismatch", capability_id, str(evidence))
    if completion == "alternative-equivalent" and mode != "rust-native":
        _finding(findings, "alternative-mode-invalid", capability_id, str(mode))
    if completion == "not-applicable" and implementation != "not-applicable":
        _finding(findings, "not-applicable-mismatch", capability_id, str(implementation))


def validate_manifest(
    manifest: dict[str, Any],
    *,
    root: Path = ROOT,
    phase: int = 0,
) -> list[CapabilityFinding]:
    findings: list[CapabilityFinding] = []
    if manifest.get("schema_version") != 1 or manifest.get("release_line") != "1.0":
        _finding(findings, "manifest-schema-invalid", "manifest", "schema_version=1 release_line=1.0 required")
    if manifest.get("java_baseline_version") != "5.5.0" or not SCHEMA_PATH.is_file():
        _finding(findings, "manifest-schema-invalid", "manifest", "Java baseline/schema missing")

    capabilities = manifest.get("capabilities")
    if not isinstance(capabilities, list) or any(not isinstance(item, dict) for item in capabilities):
        _finding(findings, "capabilities-invalid", "manifest", "capabilities must be an object list")
        return findings

    ids = [item.get("capability_id") for item in capabilities]
    string_ids = {item for item in ids if isinstance(item, str)}
    if len(ids) != len(string_ids):
        _finding(findings, "capability-id-duplicate", "manifest", "capability IDs must be unique strings")
    for capability_id in sorted(REQUIRED_IDS - string_ids):
        _finding(findings, "required-capability-missing", capability_id, "required by the 1.0 denominator")
    for capability_id in sorted(string_ids - REQUIRED_IDS):
        _finding(findings, "capability-id-unapproved", capability_id, "not in the frozen denominator")

    all_test_ids: list[str] = []
    for record in capabilities:
        _validate_record_shape(record, root=root, findings=findings)
        _validate_status(record, findings)
        capability_id = str(record.get("capability_id", "capabilities"))
        title = str(record.get("title", "")).lower().replace(" ", "")
        if any(term in title for term in ("openmessaging", "brokercontainer", "dledgercommitlog")):
            _finding(findings, "excluded-capability-in-denominator", capability_id, str(record.get("title")))
        test_ids = record.get("test_ids")
        if isinstance(test_ids, list):
            all_test_ids.extend(item for item in test_ids if isinstance(item, str))
        dependencies = record.get("dependencies")
        if isinstance(dependencies, list):
            for dependency in dependencies:
                if dependency not in string_ids or dependency == capability_id:
                    _finding(findings, "dependency-invalid", capability_id, str(dependency))
        baseline = record.get("java_baseline")
        baseline_scope = baseline.get("scope") if isinstance(baseline, dict) else None
        if capability_id == "F-09" and (
            record.get("compatibility_mode") == "wire" or baseline_scope == "controller-internal"
        ):
            _finding(findings, "controller-java-internal-wire", capability_id, "pure Rust Controller boundary")
        if phase >= 6 and capability_id not in DEFERRED_IDS and record.get("completion_status") not in {
            "equivalent",
            "alternative-equivalent",
            "not-applicable",
        }:
            _finding(findings, "release-capability-blocked", capability_id, str(record.get("completion_status")))

    if len(all_test_ids) != len(set(all_test_ids)):
        _finding(findings, "test-id-duplicate", "manifest", "test IDs must be globally unique")

    exclusions = manifest.get("exclusions")
    if not isinstance(exclusions, list) or any(not isinstance(item, dict) for item in exclusions):
        _finding(findings, "exclusions-invalid", "manifest", "exclusions must be an object list")
    else:
        titles = {item.get("title") for item in exclusions}
        if titles != EXPECTED_EXCLUSIONS or len(exclusions) != len(EXPECTED_EXCLUSIONS):
            _finding(findings, "exclusions-invalid", "manifest", "approved exclusion set drifted")
        for item in exclusions:
            if item.get("completion_status") != "intentionally-unsupported" or not item.get("reason"):
                _finding(findings, "exclusion-status-invalid", str(item.get("exclusion_id")), repr(item))
    if phase >= 6:
        try:
            freeze = load_manifest(FREEZE)
            freeze_findings = validate_freeze(manifest, freeze, root=root)
        except (CapabilityInputError, ValueError) as error:
            freeze_findings = [str(error)]
        for detail in freeze_findings:
            _finding(findings, "phase6-freeze-invalid", "freeze", detail)
    return sorted(set(findings))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", type=int, choices=range(0, 7), default=0)
    args = parser.parse_args()
    try:
        manifest = load_manifest()
    except CapabilityInputError as error:
        print(f"V1_CAPABILITY_GUARD_INPUT_FAILED detail={error}")
        return 1
    findings = validate_manifest(manifest, phase=args.phase)
    if findings:
        print(f"V1_CAPABILITY_GUARD_FAILED phase={args.phase} findings={len(findings)}")
        for finding in findings:
            print(finding.render())
        return 1
    deferred = sum(item.get("completion_status") == "deferred-by-scope" for item in manifest["capabilities"])
    print(
        f"V1_CAPABILITY_GUARD_OK phase={args.phase} capabilities={len(manifest['capabilities'])} "
        f"active={len(manifest['capabilities']) - deferred} deferred={deferred} exclusions={len(manifest['exclusions'])}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
