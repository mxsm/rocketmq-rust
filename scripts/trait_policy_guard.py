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

"""Inventory production async-trait strategies and empty public markers."""

from __future__ import annotations

import argparse
from collections import Counter
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from environment_write_guard import mask_comments_and_literals, test_module_ranges


ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / "scripts" / "trait-policy-baseline.json"
ASYNC_ATTR = re.compile(r"#\s*\[\s*(?:(?:async_trait|tonic)\s*::\s*)?async_trait(?:\s*\([^]]*\))?\s*\]")
TRAIT_VARIANT_ATTR = re.compile(r"#\s*\[\s*trait_variant\s*::\s*make\s*\([^]]*\)\s*\]")
TRAIT_START = re.compile(
    r"\b(?:(?:pub(?:\s*\([^)]*\))?\s+)?(?:unsafe\s+)?)trait\s+([A-Za-z_][A-Za-z0-9_]*)[^{;]*\{"
)
ASYNC_FN = re.compile(r"\basync\s+fn\s+([A-Za-z_][A-Za-z0-9_]*)")
EMPTY_PUBLIC_TRAIT = re.compile(
    r"\bpub(?:\s*\([^)]*\))?\s+trait\s+([A-Za-z_][A-Za-z0-9_]*)[^;{]*\{\s*\}",
    re.DOTALL,
)


def owner_for(path: str) -> str:
    if path.startswith("rocketmq-tools/rocketmq-admin/"):
        return "admin"
    if path.startswith("rocketmq-tools/rocketmq-mcp/"):
        return "mcp"
    if path.startswith("rocketmq-dashboard/"):
        return "dashboard"
    return path.split("/", 1)[0].removeprefix("rocketmq-")


def line_number(source: str, offset: int) -> int:
    return source.count("\n", 0, offset) + 1


def in_ranges(offset: int, ranges: list[tuple[int, int]]) -> bool:
    return any(start <= offset < end for start, end in ranges)


def matching_brace(source: str, opening: int) -> int | None:
    depth = 0
    for offset in range(opening, len(source)):
        if source[offset] == "{":
            depth += 1
        elif source[offset] == "}":
            depth -= 1
            if depth == 0:
                return offset + 1
    return None


def following_item(masked: str, offset: int) -> str:
    match = re.search(
        r"\b(trait|impl)\s+(?:<[^>{}]*>\s*)?([A-Za-z_][A-Za-z0-9_]*)",
        masked[offset:],
    )
    if match is None:
        return "unknown"
    return f"{match.group(1)} {match.group(2)}"


def inventory_source(relative: str, source: str) -> list[dict[str, Any]]:
    masked = mask_comments_and_literals(source)
    tests = test_module_ranges(masked)
    owner = owner_for(relative)
    entries: list[dict[str, Any]] = []

    for kind, pattern, decision in (
        ("async_trait", ASYNC_ATTR, "migrate-on-touch"),
        ("trait_variant", TRAIT_VARIANT_ATTR, "retain-send-contract-review-on-touch"),
    ):
        for match in pattern.finditer(masked):
            if in_ranges(match.start(), tests):
                continue
            entries.append(
                {
                    "kind": kind,
                    "path": relative,
                    "line": line_number(source, match.start()),
                    "item": following_item(masked, match.end()),
                    "owner": owner,
                    "decision": decision,
                }
            )

    for trait in TRAIT_START.finditer(masked):
        if in_ranges(trait.start(), tests):
            continue
        end = matching_brace(masked, trait.end() - 1)
        if end is None:
            continue
        for method in ASYNC_FN.finditer(masked, trait.end(), end):
            entries.append(
                {
                    "kind": "native_async",
                    "path": relative,
                    "line": line_number(source, method.start()),
                    "item": f"trait {trait.group(1)}::{method.group(1)}",
                    "owner": owner,
                    "decision": "preferred-static-dispatch",
                }
            )

    for marker in EMPTY_PUBLIC_TRAIT.finditer(masked):
        if in_ranges(marker.start(), tests):
            continue
        name = marker.group(1)
        entries.append(
            {
                "kind": "empty_marker",
                "path": relative,
                "line": line_number(source, marker.start()),
                "item": f"trait {name}",
                "owner": owner,
                "decision": (
                    "remove-in-P2.4"
                    if name == "MQAdminExtInner"
                    else "retain-reviewed-marker"
                ),
            }
        )
    return entries


def production_sources(root: Path = ROOT) -> list[Path]:
    values: list[Path] = []
    for directory, names, files in os.walk(root):
        names[:] = [name for name in names if name not in {".git", "target", "node_modules"}]
        directory_path = Path(directory)
        relative_directory = directory_path.relative_to(root)
        if "src" not in relative_directory.parts:
            if "src" not in names:
                continue
        for name in files:
            if not name.endswith(".rs"):
                continue
            path = directory_path / name
            relative = path.relative_to(root)
            if "src" not in relative.parts:
                continue
            values.append(path)
    return sorted(values)


def current_inventory(root: Path = ROOT) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for path in production_sources(root):
        relative = path.relative_to(root).as_posix()
        entries.extend(inventory_source(relative, path.read_text(encoding="utf-8")))
    return sorted(
        entries,
        key=lambda entry: (
            entry["kind"],
            entry["path"],
            entry["line"],
            entry["item"],
        ),
    )


def identity(entry: dict[str, Any]) -> tuple[object, ...]:
    # Line numbers are report metadata, not debt identity: moving an unchanged
    # trait into a narrower module boundary must not manufacture new debt.
    return tuple(entry[key] for key in ("kind", "path", "item", "owner", "decision"))


def compare_entries(
    baseline: list[dict[str, Any]],
    current: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    baseline_counts = Counter(identity(entry) for entry in baseline)
    remaining = baseline_counts.copy()
    additions: list[dict[str, Any]] = []
    for entry in current:
        entry_identity = identity(entry)
        if remaining[entry_identity] > 0:
            remaining[entry_identity] -= 1
        else:
            additions.append(entry)
    current_counts = Counter(identity(entry) for entry in current)
    removed = sum((baseline_counts - current_counts).values())
    return additions, removed


def load_baseline() -> dict[str, Any]:
    try:
        value = json.loads(BASELINE.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read trait policy baseline: {error}") from error
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != 1
        or not isinstance(value.get("entries"), list)
    ):
        raise ValueError("trait policy baseline must use schema_version=1 and an entries list")
    return value


def write_baseline(entries: list[dict[str, Any]]) -> None:
    payload = {
        "schema_version": 1,
        "source": "production Rust files under src/",
        "policy": "rocketmq-doc/en/rust-trait-design-guidelines.md",
        "entries": entries,
    }
    BASELINE.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write-baseline", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        current = current_inventory()
        if args.write_baseline:
            write_baseline(current)
            print(f"TRAIT_POLICY_BASELINE_WRITTEN entries={len(current)}")
            return 0
        baseline = load_baseline()["entries"]
        additions, removed = compare_entries(baseline, current)
        for entry in additions:
            print(
                "TRAIT_POLICY_FINDING "
                f"kind={entry['kind']} path={entry['path']}:{entry['line']} "
                f"item={entry['item']} owner={entry['owner']}"
            )
        if additions:
            print(f"TRAIT_POLICY_GUARD_FAILED additions={len(additions)}")
            return 1
        print(
            f"TRAIT_POLICY_GUARD_OK current={len(current)} "
            f"baseline={len(baseline)} removed={removed}"
        )
        return 0
    except (OSError, UnicodeDecodeError, ValueError) as error:
        print(f"TRAIT_POLICY_INPUT_ERROR {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
