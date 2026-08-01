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

"""Validate the lightweight pre-GA architecture candidate record."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


REQUIRED_FIELDS = {"schema_version", "commit", "environment", "checks", "known_failures"}
CHECK_FIELDS = {"name", "command", "status"}
COMMIT = re.compile(r"^[0-9a-f]{7,40}$")


def validate(value: Any) -> list[str]:
    if not isinstance(value, dict):
        return ["candidate record must be a JSON object"]

    findings: list[str] = []
    missing = REQUIRED_FIELDS - value.keys()
    unexpected = value.keys() - REQUIRED_FIELDS
    for field in sorted(missing):
        findings.append(f"missing required field: {field}")
    for field in sorted(unexpected):
        findings.append(f"unexpected field: {field}")
    if missing:
        return findings

    if value["schema_version"] != 1:
        findings.append("schema_version must be 1")
    commit = value["commit"]
    if not isinstance(commit, str) or COMMIT.fullmatch(commit) is None:
        findings.append("commit must be a 7-to-40 character lowercase Git object id")

    environment = value["environment"]
    if not isinstance(environment, dict) or set(environment) != {"os", "rustc"}:
        findings.append("environment must contain exactly os and rustc")
    elif any(not isinstance(environment[field], str) or not environment[field].strip() for field in ("os", "rustc")):
        findings.append("environment os and rustc must be non-empty strings")

    checks = value["checks"]
    if not isinstance(checks, list) or not checks:
        findings.append("checks must be a non-empty list")
    else:
        names: set[str] = set()
        for index, check in enumerate(checks):
            if not isinstance(check, dict) or set(check) != CHECK_FIELDS:
                findings.append(f"checks[{index}] must contain exactly name, command, and status")
                continue
            if any(not isinstance(check[field], str) or not check[field].strip() for field in CHECK_FIELDS):
                findings.append(f"checks[{index}] fields must be non-empty strings")
                continue
            if check["name"] in names:
                findings.append(f"checks[{index}] duplicates check name {check['name']}")
            names.add(check["name"])
            if check["status"] != "passed":
                findings.append(f"checks[{index}] must pass")
        if "pr_static" not in names:
            findings.append("checks must include pr_static")

    known_failures = value["known_failures"]
    if not isinstance(known_failures, list):
        findings.append("known_failures must be a list")
    elif known_failures:
        findings.append("known_failures must be empty for an accepted candidate")
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--record", type=Path, required=True)
    args = parser.parse_args()
    try:
        value = json.loads(args.record.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        print(f"ARCHITECTURE_CANDIDATE_FAILED input={error}", file=sys.stderr)
        return 2

    findings = validate(value)
    if findings:
        for finding in findings:
            print(f"ARCHITECTURE_CANDIDATE_FINDING {finding}", file=sys.stderr)
        print(f"ARCHITECTURE_CANDIDATE_FAILED findings={len(findings)}", file=sys.stderr)
        return 1
    print(
        f"ARCHITECTURE_CANDIDATE_OK commit={value['commit']} "
        f"checks={len(value['checks'])} known_failures=0"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
