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

"""Verify the isolated core image workflow without invoking remote publication."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[1]
EXPECTED_SERVICES = {"rocketmq-namesrv", "rocketmq-broker", "rocketmq-controller", "rocketmq-proxy"}
EXCLUDED_TOKENS = {
    "rocketmq-mcp",
    "rocketmq-sre",
    "rocketmq-dashboard",
    "openmessaging",
    "brokercontainer",
    "broker-container",
    "dledger",
}
VERSION = re.compile(r"^1\.0\.0(?:-rc\.[1-9]\d*)?$")


@dataclass(frozen=True, order=True)
class PublicationFinding:
    code: str
    path: str
    detail: str


def _mapping_block(text: str, key: str, indent: int) -> list[str]:
    prefix = " " * indent
    lines = text.splitlines()
    start = next(
        (index for index, line in enumerate(lines) if line == f"{prefix}{key}:"),
        None,
    )
    if start is None:
        return []
    block: list[str] = []
    for line in lines[start + 1 :]:
        stripped = line.lstrip(" ")
        if stripped and not stripped.startswith("#") and len(line) - len(stripped) <= indent:
            break
        block.append(line)
    return block


def _mapping_keys(block: list[str], indent: int) -> set[str]:
    pattern = re.compile(rf"^\s{{{indent}}}([A-Za-z0-9_-]+):")
    return {match.group(1) for line in block if (match := pattern.match(line))}


def verify(workflow: Path, policy: Path) -> list[PublicationFinding]:
    findings: list[PublicationFinding] = []
    try:
        workflow_text = workflow.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        return [PublicationFinding("workflow-input", str(workflow), str(error))]
    try:
        policy_value = json.loads(policy.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        return [PublicationFinding("policy-input", str(policy), str(error))]
    trigger_block = _mapping_block(workflow_text, "on", 0)
    trigger_keys = _mapping_keys(trigger_block, 2)
    if trigger_keys - {"workflow_dispatch"}:
        findings.append(PublicationFinding("automatic-trigger", str(workflow), "only workflow_dispatch is allowed"))
    if "workflow_dispatch" not in trigger_keys:
        findings.append(PublicationFinding("manual-trigger", str(workflow), "workflow_dispatch is required"))
    if re.search(r"(?ms)publish:.*?default:\s*false", workflow_text) is None:
        findings.append(PublicationFinding("publish-default", str(workflow), "publish input must default to false"))
    if re.search(r"(?ms)^permissions:\s*\n\s+contents:\s*read", workflow_text) is None:
        findings.append(
            PublicationFinding(
                "default-permissions", str(workflow), "top-level contents: read is required"
            )
        )
    publish_block = _mapping_block(workflow_text, "publish-candidate", 2)
    condition_line = next(
        (line.split(":", 1)[1].split("#", 1)[0].strip() for line in publish_block if re.match(r"^\s{4}if:", line)),
        "",
    )
    condition_tokens = ("github.event_name == 'workflow_dispatch'", "inputs.publish == true")
    if not all(token in condition_line for token in condition_tokens):
        findings.append(
            PublicationFinding(
                "publish-condition", str(workflow), "remote job lacks the manual publish condition"
            )
        )
    if not any(line.strip() == "environment: core-release-publication" for line in publish_block):
        findings.append(
            PublicationFinding(
                "protected-environment", str(workflow), "remote job lacks protected environment"
            )
        )
    if 'check_release_version.py --version "${{ inputs.version }}" --fixture' not in workflow_text:
        findings.append(
            PublicationFinding(
                "version-validation", str(workflow), "workflow input is not semantically validated"
            )
        )
    dry_run = workflow_text.split("publish-candidate:", 1)[0]
    if "secrets." in dry_run or re.search(r"(?m)^\s+packages:\s*write", dry_run):
        findings.append(
            PublicationFinding(
                "dry-run-secret-route",
                str(workflow),
                "dry-run path reads a secret or write permission",
            )
        )
    services = policy_value.get("services")
    if not isinstance(services, list) or set(services) != EXPECTED_SERVICES:
        findings.append(PublicationFinding("service-scope", str(policy), f"expected {sorted(EXPECTED_SERVICES)}"))
    missing_workflow_services = sorted(service for service in EXPECTED_SERVICES if service not in workflow_text)
    if missing_workflow_services:
        findings.append(
            PublicationFinding(
                "workflow-service-scope",
                str(workflow),
                f"missing {missing_workflow_services}",
            )
        )
    version = policy_value.get("release_version")
    if not isinstance(version, str) or VERSION.fullmatch(version) is None:
        findings.append(PublicationFinding("release-version", str(policy), repr(version)))
    combined = workflow_text.lower() + "\n" + json.dumps(policy_value).lower()
    leaked = sorted(token for token in EXCLUDED_TOKENS if token in combined)
    if leaked:
        findings.append(PublicationFinding("excluded-service", "core-release", ",".join(leaked)))
    return sorted(set(findings))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workflow", type=Path, default=ROOT / ".github/workflows/core-service-image-publish.yml")
    parser.add_argument("--policy", type=Path, default=ROOT / "docker/core-container-policy.json")
    args = parser.parse_args(argv)
    findings = verify(args.workflow, args.policy)
    if findings:
        for finding in findings:
            print(f"CORE_IMAGE_PUBLICATION_FINDING code={finding.code} path={finding.path} detail={finding.detail}")
        return 1
    print("CORE_IMAGE_PUBLICATION_OK services=4 remote_publication=not-executed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
