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

"""Run every registered deterministic property suite and reject zero-test passes."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "scripts" / "property-state-suite-registry.json"
RESULT = re.compile(
    r"test result: ok\. (?P<passed>\d+) passed; 0 failed; "
    r"(?P<ignored>\d+) ignored; 0 measured; (?P<filtered>\d+) filtered out"
)


def successful_test_count(output: str) -> int:
    """Return the total number of tests reported by successful Cargo targets."""
    return sum(int(match.group("passed")) for match in RESULT.finditer(output))


def load_suites(path: Path) -> list[dict[str, Any]]:
    """Load registered suites from the canonical JSON registry."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    suites = payload.get("suites")
    if payload.get("schema_version") != 1 or not isinstance(suites, list):
        raise ValueError("property suite registry has an invalid schema")
    return suites


def execute_suite(root: Path, suite: dict[str, Any]) -> None:
    """Execute one suite without a shell and enforce its expected test count."""
    suite_id = str(suite.get("id", "<unknown>"))
    command = suite.get("command")
    expected = suite.get("expected_tests")
    if (
        not isinstance(command, list)
        or not command
        or any(not isinstance(argument, str) or not argument for argument in command)
        or not isinstance(expected, int)
        or expected <= 0
    ):
        raise ValueError(f"{suite_id}: command and expected_tests must be explicit")

    print(f"PROPERTY_SUITE_START id={suite_id} command={json.dumps(command)}", flush=True)
    result = subprocess.run(
        command,
        cwd=root,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    print(result.stdout, end="" if result.stdout.endswith("\n") else "\n")
    if result.returncode != 0:
        raise RuntimeError(f"{suite_id}: command exited with {result.returncode}")

    passed = successful_test_count(result.stdout)
    if passed != expected:
        raise RuntimeError(
            f"{suite_id}: expected {expected} executed test(s), Cargo reported {passed}"
        )
    print(f"PROPERTY_SUITE_PASS id={suite_id} tests={passed}", flush=True)


def main() -> int:
    """Run the selected property suites."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--registry", type=Path, default=REGISTRY)
    parser.add_argument("--suite", action="append", default=[])
    args = parser.parse_args()

    try:
        suites = load_suites(args.registry.resolve())
        selected = set(args.suite)
        if selected:
            known = {str(suite.get("id")) for suite in suites}
            unknown = selected - known
            if unknown:
                raise ValueError(f"unknown property suite(s): {sorted(unknown)}")
            suites = [suite for suite in suites if suite.get("id") in selected]
        for suite in suites:
            execute_suite(args.root.resolve(), suite)
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as error:
        print(f"PROPERTY_SUITE_FAILED {error}", file=sys.stderr)
        return 1

    print(f"PROPERTY_SUITES_OK suites={len(suites)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
