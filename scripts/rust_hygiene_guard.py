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

"""Enforce SAFETY comments and non-growth Rust hygiene inventories."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import NamedTuple

sys.path.insert(0, str(Path(__file__).resolve().parent))
import environment_write_guard as rust_source  # noqa: E402


ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / "scripts" / "rust-hygiene-baseline.json"

UNSAFE_REGION = re.compile(r"\bunsafe\s*(?:impl\b|\{)")
MANUAL_PIN = re.compile(r"\b(?:get_unchecked_mut|map_unchecked(?:_mut)?|Pin\s*::\s*new_unchecked)\b")
PANIC_SURFACE = re.compile(r"(?:\.\s*(unwrap|expect)\s*\(|\b(panic|unreachable)\s*!\s*\()")
FUNCTION = re.compile(
    r"\b(?:async\s+)?(?:unsafe\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)",
    re.MULTILINE,
)


class SafetyFinding(NamedTuple):
    path: str
    line: int


def is_test_only(offset: int, ranges: list[tuple[int, int]]) -> bool:
    return any(start <= offset < end for start, end in ranges)


def preceding_safety_comment(source: str, offset: int) -> bool:
    current_line_start = source.rfind("\n", 0, offset) + 1
    lines = source[:current_line_start].splitlines()
    for line in reversed(lines[-10:]):
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("//"):
            if stripped.startswith("// SAFETY:"):
                return True
            continue
        if stripped.startswith("#["):
            continue
        return False
    return False


def enclosing_function(masked: str, offset: int) -> str:
    matches = list(FUNCTION.finditer(masked, 0, offset))
    return matches[-1].group(1) if matches else "<module>"


def normalized_line(masked: str, offset: int) -> str:
    start = masked.rfind("\n", 0, offset) + 1
    end = masked.find("\n", offset)
    if end == -1:
        end = len(masked)
    return re.sub(r"\s+", "", masked[start:end])


def debt_entry(relative: str, kind: str, masked: str, source: str, offset: int) -> dict[str, object]:
    line = source.count("\n", 0, offset) + 1
    item = enclosing_function(masked, offset)
    snippet = normalized_line(masked, offset)
    fingerprint = hashlib.sha256(f"{relative}\0{kind}\0{item}\0{snippet}".encode()).hexdigest()[:20]
    return {
        "identity": f"{relative}:{kind}:{item}:{fingerprint}",
        "path": relative,
        "kind": kind,
        "item": item,
        "line": line,
        "fingerprint": fingerprint,
        "classification": "reviewed legacy occurrence; additions are forbidden",
        "owner": "architecture maintainers",
    }


def scan_source(source: str, relative: str) -> tuple[list[SafetyFinding], list[dict[str, object]]]:
    masked = rust_source.mask_comments_and_literals(source)
    test_ranges = rust_source.test_module_ranges(masked)
    safety_findings: list[SafetyFinding] = []
    debt: list[dict[str, object]] = []

    for match in UNSAFE_REGION.finditer(masked):
        if is_test_only(match.start(), test_ranges):
            continue
        if not preceding_safety_comment(source, match.start()):
            safety_findings.append(
                SafetyFinding(relative, source.count("\n", 0, match.start()) + 1)
            )

    for kind, pattern in (("manual_pin", MANUAL_PIN), ("panic_surface", PANIC_SURFACE)):
        duplicates: Counter[str] = Counter()
        for match in pattern.finditer(masked):
            if is_test_only(match.start(), test_ranges):
                continue
            entry = debt_entry(relative, kind, masked, source, match.start())
            base_identity = str(entry["identity"])
            ordinal = duplicates[base_identity]
            duplicates[base_identity] += 1
            entry["identity"] = f"{base_identity}:{ordinal}"
            debt.append(entry)

    return safety_findings, debt


def scan_tree(root: Path) -> tuple[list[SafetyFinding], list[dict[str, object]]]:
    safety_findings: list[SafetyFinding] = []
    debt: list[dict[str, object]] = []
    for path in rust_source.production_sources(root):
        relative = path.relative_to(root).as_posix()
        file_safety, file_debt = scan_source(path.read_text(encoding="utf-8"), relative)
        safety_findings.extend(file_safety)
        debt.extend(file_debt)

    for path in sorted(root.rglob("mod.rs")):
        relative_path = path.relative_to(root)
        if "target" in relative_path.parts or "src" not in relative_path.parts:
            continue
        relative = relative_path.as_posix()
        fingerprint = hashlib.sha256(relative.encode()).hexdigest()[:20]
        debt.append(
            {
                "identity": f"{relative}:legacy_mod_rs:{fingerprint}:0",
                "path": relative,
                "kind": "legacy_mod_rs",
                "item": "<module>",
                "line": 1,
                "fingerprint": fingerprint,
                "classification": "legacy module layout; additions are forbidden",
                "owner": "owning crate maintainers",
            }
        )

    return safety_findings, sorted(debt, key=lambda entry: str(entry["identity"]))


def write_baseline(path: Path, debt: list[dict[str, object]]) -> None:
    payload = {
        "schema_version": 1,
        "policy": {
            "unsafe": "Every production unsafe block or impl requires an adjacent // SAFETY: comment.",
            "debt": "Existing panic surfaces, manual Pin projection, and mod.rs files may be deleted but not added.",
        },
        "entries": debt,
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def load_baseline(path: Path) -> set[str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != 1 or not isinstance(payload.get("entries"), list):
        raise ValueError("rust hygiene baseline has an unsupported schema")
    identities = [entry.get("identity") for entry in payload["entries"]]
    if any(not isinstance(identity, str) or not identity for identity in identities):
        raise ValueError("rust hygiene baseline contains an invalid identity")
    if len(identities) != len(set(identities)):
        raise ValueError("rust hygiene baseline contains duplicate identities")
    return set(identities)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--baseline", type=Path, default=BASELINE)
    parser.add_argument("--write-baseline", action="store_true")
    args = parser.parse_args()

    root = args.root.resolve()
    safety_findings, debt = scan_tree(root)
    if safety_findings:
        for finding in safety_findings:
            print(
                f"{finding.path}:{finding.line}: unsafe region requires an adjacent // SAFETY: comment",
                file=sys.stderr,
            )
        print(f"RUST_HYGIENE_GUARD_FAILED unsafe_findings={len(safety_findings)}", file=sys.stderr)
        return 1

    if args.write_baseline:
        write_baseline(args.baseline, debt)
        print(f"RUST_HYGIENE_BASELINE_WRITTEN entries={len(debt)} path={args.baseline}")
        return 0

    try:
        baseline = load_baseline(args.baseline)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"RUST_HYGIENE_GUARD_FAILED baseline={error}", file=sys.stderr)
        return 1

    current = {str(entry["identity"]): entry for entry in debt}
    additions = [current[identity] for identity in sorted(current.keys() - baseline)]
    if additions:
        for entry in additions:
            print(
                f"{entry['path']}:{entry['line']}: new {entry['kind']} occurrence "
                f"in {entry['item']}",
                file=sys.stderr,
            )
        print(f"RUST_HYGIENE_GUARD_FAILED new_debt={len(additions)}", file=sys.stderr)
        return 1

    counts = Counter(str(entry["kind"]) for entry in debt)
    print(
        "RUST_HYGIENE_GUARD_OK "
        f"manual_pin={counts['manual_pin']} "
        f"panic_surface={counts['panic_surface']} "
        f"legacy_mod_rs={counts['legacy_mod_rs']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
