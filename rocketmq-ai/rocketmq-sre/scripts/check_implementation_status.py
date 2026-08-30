#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Validate the evidence-backed AI SRE implementation status baseline."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path, PurePosixPath
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_MANIFEST = (
    REPOSITORY_ROOT
    / "rocketmq-ai"
    / "rocketmq-sre"
    / "config"
    / "implementation"
    / "implementation-status.v1.json"
)
EXPECTED_SCHEMA = "rocketmq-sre.implementation-status.v1"
EXPECTED_STATUSES = ("completed", "partial", "deferred", "not_started")
EXPECTED_MATURITY = (
    "implemented",
    "contract_tested",
    "live_smoke_passed",
    "production_certified",
)
EXPECTED_EVIDENCE_KINDS = {"configuration", "source", "test", "smoke"}
REQUIRED_QUALIFICATION_EVIDENCE_KINDS = {"configuration", "test", "smoke"}
BASELINE_ASSERTIONS = {
    "total": "requirement_count",
    "required": "required_count",
    "optional": "optional_count",
    "query": "query_count",
    "not_production_verified": "unsupported_count",
    "required_query": "required_query_count",
    "required_not_production_verified": "required_unsupported_count",
    "optional_query": "optional_query_count",
    "optional_not_production_verified": "optional_unsupported_count",
}
LOCAL_PATH = re.compile(r"(?:^[A-Za-z]:[\\/]|^/(?:home|Users|tmp)/)")
SENSITIVE_VALUE = re.compile(
    r"(?:-----BEGIN [A-Z ]*PRIVATE KEY-----|\bBearer\s+[A-Za-z0-9._~-]+|\bsk-[A-Za-z0-9_-]{12,})",
    re.IGNORECASE,
)
QUALIFICATION_ID = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")


def load_manifest(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as source:
        value = json.load(source)
    if not isinstance(value, dict):
        raise ValueError("manifest root must be an object")
    return value


def _all_strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        strings: list[str] = []
        for key, child in value.items():
            strings.extend(_all_strings(key))
            strings.extend(_all_strings(child))
        return strings
    if isinstance(value, list):
        strings = []
        for child in value:
            strings.extend(_all_strings(child))
        return strings
    return []


def _validate_evidence_path(path_value: Any, repository_root: Path) -> list[str]:
    if not isinstance(path_value, str) or not path_value:
        return ["evidence path must be a non-empty string"]
    path = PurePosixPath(path_value)
    findings: list[str] = []
    if path.is_absolute() or ".." in path.parts or LOCAL_PATH.search(path_value):
        findings.append(f"evidence path must be repository-relative: {path_value}")
        return findings
    if "docs" in path.parts:
        findings.append(f"documentation is not accepted as implementation evidence: {path_value}")
    if not (repository_root / Path(*path.parts)).exists():
        findings.append(f"evidence path does not exist: {path_value}")
    return findings


def _source_assertions(path: Path) -> dict[str, int]:
    text = path.read_text(encoding="utf-8")
    return {
        name: int(value)
        for name, value in re.findall(r"assert_eq!\((\w+),\s*(\d+)\);", text)
    }


def validate_manifest(manifest: dict[str, Any], repository_root: Path) -> list[str]:
    findings: list[str] = []
    if manifest.get("schema_version") != EXPECTED_SCHEMA:
        findings.append(f"schema_version must be {EXPECTED_SCHEMA}")
    if manifest.get("status_definitions") != list(EXPECTED_STATUSES):
        findings.append("status_definitions must use the canonical ordered vocabulary")
    if manifest.get("maturity_definitions") != list(EXPECTED_MATURITY):
        findings.append("maturity_definitions must use the canonical ordered vocabulary")

    for value in _all_strings(manifest):
        if SENSITIVE_VALUE.search(value):
            findings.append("manifest contains a credential-like value")
        if LOCAL_PATH.search(value):
            findings.append("manifest contains a machine-local filesystem path")

    baseline = manifest.get("evidence_requirement_baseline")
    if not isinstance(baseline, dict):
        findings.append("evidence_requirement_baseline must be an object")
    else:
        numeric_values = {key: baseline.get(key) for key in BASELINE_ASSERTIONS}
        if not all(isinstance(value, int) and value >= 0 for value in numeric_values.values()):
            findings.append("all Evidence baseline counts must be non-negative integers")
        else:
            if baseline["total"] != baseline["required"] + baseline["optional"]:
                findings.append("Evidence total must equal required plus optional")
            if baseline["total"] != baseline["query"] + baseline["not_production_verified"]:
                findings.append("Evidence total must equal query plus not_production_verified")
            if baseline["required"] != (
                baseline["required_query"] + baseline["required_not_production_verified"]
            ):
                findings.append("required Evidence counts are inconsistent")
            if baseline["optional"] != (
                baseline["optional_query"] + baseline["optional_not_production_verified"]
            ):
                findings.append("optional Evidence counts are inconsistent")

        source = baseline.get("source")
        source_findings = _validate_evidence_path(source, repository_root)
        findings.extend(source_findings)
        if not source_findings:
            assertions = _source_assertions(repository_root / Path(*PurePosixPath(source).parts))
            for field, assertion in BASELINE_ASSERTIONS.items():
                if assertions.get(assertion) != baseline.get(field):
                    findings.append(
                        f"Evidence baseline {field} does not match {assertion} in {source}"
                    )

    areas = manifest.get("capability_areas")
    if not isinstance(areas, list) or not areas:
        findings.append("capability_areas must be a non-empty array")
        return findings

    seen_ids: set[str] = set()
    for index, area in enumerate(areas):
        location = f"capability_areas[{index}]"
        if not isinstance(area, dict):
            findings.append(f"{location} must be an object")
            continue
        area_id = area.get("id")
        if not isinstance(area_id, str) or not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", area_id):
            findings.append(f"{location}.id must be a stable kebab-case identifier")
        elif area_id in seen_ids:
            findings.append(f"duplicate capability id: {area_id}")
        else:
            seen_ids.add(area_id)

        status = area.get("status")
        if status not in EXPECTED_STATUSES:
            findings.append(f"{location}.status is not recognized")

        maturity = area.get("maturity")
        if not isinstance(maturity, dict) or tuple(maturity) != EXPECTED_MATURITY:
            findings.append(f"{location}.maturity must contain the ordered canonical levels")
        elif not all(isinstance(maturity[level], bool) for level in EXPECTED_MATURITY):
            findings.append(f"{location}.maturity values must be booleans")
        else:
            reached_false = False
            for level in EXPECTED_MATURITY:
                reached_false = reached_false or not maturity[level]
                if reached_false and maturity[level]:
                    findings.append(f"{location}.maturity cannot skip a lower level")
                    break

        evidence = area.get("evidence")
        if not isinstance(evidence, list):
            findings.append(f"{location}.evidence must be an array")
            evidence = []
        qualification_evidence: dict[str, set[str]] = {}
        has_smoke_evidence = False
        for evidence_index, item in enumerate(evidence):
            evidence_location = f"{location}.evidence[{evidence_index}]"
            if not isinstance(item, dict):
                findings.append(f"{evidence_location} must be an object")
                continue
            kind = item.get("kind")
            if kind not in EXPECTED_EVIDENCE_KINDS:
                findings.append(f"{evidence_location}.kind is not recognized")
            elif kind == "smoke":
                has_smoke_evidence = True
            findings.extend(_validate_evidence_path(item.get("path"), repository_root))

            qualification = item.get("qualification")
            if qualification is None:
                continue
            if not isinstance(qualification, str) or not QUALIFICATION_ID.fullmatch(qualification):
                findings.append(f"{evidence_location}.qualification must be a stable kebab-case identifier")
            elif kind in EXPECTED_EVIDENCE_KINDS:
                qualification_evidence.setdefault(qualification, set()).add(kind)

        for qualification, kinds in sorted(qualification_evidence.items()):
            missing = REQUIRED_QUALIFICATION_EVIDENCE_KINDS - kinds
            if missing:
                findings.append(
                    f"{location} qualification {qualification} is missing evidence kinds: "
                    f"{', '.join(sorted(missing))}"
                )

        if (
            isinstance(maturity, dict)
            and maturity.get("live_smoke_passed") is True
            and not has_smoke_evidence
        ):
            findings.append(f"{location} claims live_smoke_passed without smoke evidence")

        if status in {"completed", "partial"} and not evidence:
            findings.append(f"{location} requires implementation evidence")
        if status == "partial" and not area.get("limitations"):
            findings.append(f"{location} requires explicit limitations")
        if status in {"deferred", "not_started"} and not area.get("reason"):
            findings.append(f"{location} requires an explicit reason")

    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", nargs="?", type=Path, default=DEFAULT_MANIFEST)
    args = parser.parse_args()

    try:
        manifest = load_manifest(args.manifest)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"IMPLEMENTATION_STATUS_FAILED unable_to_load={error}")
        return 1

    findings = validate_manifest(manifest, REPOSITORY_ROOT)
    if findings:
        for finding in findings:
            print(f"IMPLEMENTATION_STATUS_FINDING {finding}")
        print(f"IMPLEMENTATION_STATUS_FAILED findings={len(findings)}")
        return 1

    print(
        "IMPLEMENTATION_STATUS_OK "
        f"capability_areas={len(manifest['capability_areas'])} "
        f"evidence_requirements={manifest['evidence_requirement_baseline']['total']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
