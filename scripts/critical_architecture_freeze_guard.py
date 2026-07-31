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

"""Freeze critical lifecycle and remote-header panic debt at its current ceiling."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import re
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = ROOT / "scripts/critical-architecture-freeze.json"
EXCLUDED_PARTS = {"tests", "benches", "target"}
CFG_TEST_MODULE = re.compile(
    r"(?m)^\s*#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]\s*\r?\n\s*mod\s+tests\b"
)


@dataclass(frozen=True)
class Finding:
    """A rule whose live production count exceeds its configured ceiling."""

    rule: str
    path: str
    count: int
    maximum: int

    def render(self) -> str:
        return (
            f"CRITICAL_ARCHITECTURE_FREEZE_GROWTH rule={self.rule} "
            f"path={self.path} count={self.count} maximum={self.maximum}"
        )


def evaluate(root: Path, policy: dict[str, Any]) -> list[Finding]:
    """Evaluate production Rust sources against a parsed policy."""
    rules = policy.get("rules")
    if policy.get("schema_version") != 1 or not isinstance(rules, dict) or not rules:
        raise ValueError("policy requires schema_version=1 and a non-empty rules object")

    findings: list[Finding] = []
    for rule, settings in rules.items():
        if not isinstance(rule, str) or not isinstance(settings, dict):
            raise ValueError("every rule must be a named object")
        maximum = settings.get("maximum")
        matchers = settings.get("matchers")
        if not isinstance(maximum, int) or maximum < 0:
            raise ValueError(f"{rule}: maximum must be a non-negative integer")
        if not isinstance(matchers, list) or not matchers:
            raise ValueError(f"{rule}: matchers must be a non-empty list")

        total = 0
        matched_paths: set[str] = set()
        for matcher in matchers:
            if not isinstance(matcher, dict) or set(matcher) != {"path", "pattern"}:
                raise ValueError(f"{rule}: every matcher requires only path and pattern")
            path = _resolve_policy_path(root, matcher["path"], rule)
            pattern = re.compile(str(matcher["pattern"]), re.MULTILINE | re.DOTALL)
            for source in _source_files(path):
                text = _production_source(source)
                count = len(tuple(pattern.finditer(text)))
                if count:
                    total += count
                    matched_paths.add(source.relative_to(root).as_posix())

        if total > maximum:
            findings.append(
                Finding(
                    rule=rule,
                    path=",".join(sorted(matched_paths)) or "<no-matching-path>",
                    count=total,
                    maximum=maximum,
                )
            )
    return findings


def counts(root: Path, policy: dict[str, Any]) -> dict[str, tuple[int, int]]:
    """Return live and maximum counts for successful reporting."""
    result: dict[str, tuple[int, int]] = {}
    for rule, settings in policy["rules"].items():
        maximum = settings["maximum"]
        total = 0
        for matcher in settings["matchers"]:
            path = _resolve_policy_path(root, matcher["path"], rule)
            pattern = re.compile(str(matcher["pattern"]), re.MULTILINE | re.DOTALL)
            total += sum(
                len(tuple(pattern.finditer(_production_source(source)))) for source in _source_files(path)
            )
        result[rule] = (total, maximum)
    return result


def _resolve_policy_path(root: Path, value: object, rule: str) -> Path:
    if not isinstance(value, str) or not value or Path(value).is_absolute():
        raise ValueError(f"{rule}: matcher path must be repository-relative")
    resolved_root = root.resolve()
    resolved = (root / value).resolve()
    if resolved != resolved_root and resolved_root not in resolved.parents:
        raise ValueError(f"{rule}: matcher path escapes the repository root")
    return resolved


def _source_files(path: Path) -> tuple[Path, ...]:
    if path.is_file():
        candidates = (path,) if path.suffix == ".rs" else ()
    elif path.is_dir():
        candidates = tuple(path.rglob("*.rs"))
    else:
        candidates = ()
    return tuple(
        candidate
        for candidate in candidates
        if not EXCLUDED_PARTS.intersection(candidate.parts)
    )


def _production_source(path: Path) -> str:
    text = path.read_text(encoding="utf-8")
    test_boundary = CFG_TEST_MODULE.search(text)
    return text[: test_boundary.start()] if test_boundary else text


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--policy", type=Path, default=POLICY_PATH)
    args = parser.parse_args()

    try:
        policy = json.loads(args.policy.read_text(encoding="utf-8"))
        findings = evaluate(args.root, policy)
    except (OSError, json.JSONDecodeError, ValueError, re.error) as error:
        print(f"CRITICAL_ARCHITECTURE_FREEZE_INVALID {error}", file=sys.stderr)
        return 2

    if findings:
        for finding in findings:
            print(finding.render(), file=sys.stderr)
        return 1

    summary = " ".join(
        f"{rule}={live}/{maximum}" for rule, (live, maximum) in counts(args.root, policy).items()
    )
    print(f"CRITICAL_ARCHITECTURE_FREEZE_OK {summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
