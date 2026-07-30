#!/usr/bin/env python3
#
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

"""Govern long-argument and broad Rust lint exceptions as non-growth debt."""

from __future__ import annotations

import argparse
from collections import Counter
import json
import re
import sys
from pathlib import Path
from typing import Any

from environment_write_guard import mask_comments_and_literals, production_sources


ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "scripts" / "rust-lint-debt-registry.json"
TARGET_LINTS = {
    "clippy::too_many_arguments",
    "clippy::result_large_err",
    "dead_code",
    "unused_variables",
}
ALLOW = re.compile(r"(?P<prefix>#!|#)\s*\[\s*allow\s*\((?P<body>.*?)\)\s*\]", re.DOTALL)
FOLLOWING_ITEM = re.compile(
    r"\b(?:pub(?:\s*\([^)]*\))?\s+)?(?:async\s+)?(?:unsafe\s+)?"
    r"(fn|struct|enum|trait|impl|mod|type|const|static)\s+([A-Za-z_][A-Za-z0-9_]*)"
)
REASON = re.compile(r"\breason\s*=\s*\"(?P<value>(?:\\.|[^\"\\])*)\"")
THRESHOLD = re.compile(r"^\s*too-many-arguments-threshold\s*=\s*(\d+)\s*$", re.MULTILINE)
REQUIRED_FIELDS = {
    "identity",
    "path",
    "lints",
    "scope",
    "item",
    "owner",
    "reason",
    "removal_issue",
}


def owner_for(relative: str) -> str:
    if relative.startswith("rocketmq-tools/rocketmq-admin/"):
        return "admin"
    if relative.startswith("rocketmq-tools/rocketmq-mcp/"):
        return "mcp"
    if relative.startswith("rocketmq-dashboard/"):
        return "dashboard"
    return relative.split("/", 1)[0]


def normalized_lints(body: str) -> tuple[str, ...]:
    before_reason = body.split("reason", 1)[0]
    return tuple(
        sorted(
            lint
            for raw in before_reason.split(",")
            if (lint := re.sub(r"\s+", "", raw)) in TARGET_LINTS
        )
    )


def following_item(masked: str, offset: int) -> tuple[str, str]:
    match = FOLLOWING_ITEM.search(masked, offset)
    if match is None:
        return "item", "<unknown>"
    return ("module" if match.group(1) == "mod" else "item", f"{match.group(1)} {match.group(2)}")


def inventory_source(relative: str, source: str) -> list[dict[str, Any]]:
    masked = mask_comments_and_literals(source)
    entries: list[dict[str, Any]] = []
    duplicates: Counter[str] = Counter()
    for match in ALLOW.finditer(masked):
        raw = source[match.start():match.end()]
        lints = normalized_lints(raw[raw.find("(") + 1:raw.rfind(")")])
        if not lints:
            continue
        if match.group("prefix") == "#!":
            scope, item = "crate", "<crate>"
        else:
            scope, item = following_item(masked, match.end())
        base = f"{relative}:{scope}:{item}:{','.join(lints)}"
        ordinal = duplicates[base]
        duplicates[base] += 1
        inline_reason = REASON.search(raw)
        entries.append(
            {
                "identity": f"{base}:{ordinal}",
                "path": relative,
                "lints": list(lints),
                "scope": scope,
                "item": item,
                "owner": owner_for(relative),
                "reason": (
                    inline_reason.group("value")
                    if inline_reason
                    else "reviewed legacy exception; additions require a narrower API or explicit registry review"
                ),
                "removal_issue": "ARC-ALLOW-001",
            }
        )
    return entries


def current_inventory(root: Path = ROOT) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for path in production_sources(root):
        relative = path.relative_to(root).as_posix()
        entries.extend(inventory_source(relative, path.read_text(encoding="utf-8")))
    return sorted(entries, key=lambda entry: entry["identity"])


def render_registry(entries: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "too_many_arguments": {
            "current_threshold": 12,
            "next_approved_threshold": 8,
            "next_condition": "all signatures newly exposed after this baseline use request/config structs",
        },
        "maximum_entries": len(entries),
        "entries": entries,
    }


def validate_registry(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {
        "schema_version",
        "too_many_arguments",
        "maximum_entries",
        "entries",
    }:
        raise ValueError("unexpected registry schema")
    if value["schema_version"] != 1:
        raise ValueError("unsupported registry schema")
    threshold = value["too_many_arguments"]
    if threshold != {
        "current_threshold": 12,
        "next_approved_threshold": 8,
        "next_condition": "all signatures newly exposed after this baseline use request/config structs",
    }:
        raise ValueError("threshold contract must record 20 -> 12 and the approved 8 target")
    entries = value["entries"]
    if not isinstance(value["maximum_entries"], int) or not isinstance(entries, list):
        raise ValueError("invalid registry counts")
    identities: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != REQUIRED_FIELDS:
            raise ValueError("invalid lint debt entry")
        if entry["scope"] not in {"crate", "module", "item"}:
            raise ValueError("invalid lint debt scope")
        if (
            not isinstance(entry["lints"], list)
            or not entry["lints"]
            or not set(entry["lints"]).issubset(TARGET_LINTS)
        ):
            raise ValueError("invalid lint list")
        for field in REQUIRED_FIELDS - {"lints"}:
            if not isinstance(entry[field], str) or not entry[field]:
                raise ValueError("lint debt metadata must be non-empty")
        if entry["identity"] in identities:
            raise ValueError("duplicate lint debt identity")
        identities.add(entry["identity"])
    return value


def compare(registry: dict[str, Any], current: list[dict[str, Any]], clippy_config: str) -> list[str]:
    findings: list[str] = []
    match = THRESHOLD.search(clippy_config)
    if match is None or int(match.group(1)) != 12:
        findings.append("too-many-arguments-threshold must remain 12")
    approved = {entry["identity"] for entry in registry["entries"]}
    actual = {entry["identity"] for entry in current}
    for entry in current:
        if entry["identity"] not in approved:
            findings.append(
                f"unregistered {entry['scope']}-scope allow {entry['identity']}; "
                "new crate/module-wide allows and unowned item exceptions are forbidden"
            )
    for identity in sorted(approved - actual):
        findings.append(f"stale lint debt identity {identity}")
    if len(current) > registry["maximum_entries"]:
        findings.append(
            f"lint debt grew: current={len(current)} maximum={registry['maximum_entries']}"
        )
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write-registry", action="store_true")
    parser.add_argument("--root", type=Path, default=ROOT, help=argparse.SUPPRESS)
    args = parser.parse_args()
    root = args.root.resolve()
    try:
        current = current_inventory(root)
        if args.write_registry:
            REGISTRY.write_text(json.dumps(render_registry(current), indent=2, sort_keys=True) + "\n", encoding="utf-8")
            print(f"RUST_LINT_DEBT_WRITTEN entries={len(current)}")
            return 0
        registry = validate_registry(json.loads(REGISTRY.read_text(encoding="utf-8")))
        findings = compare(registry, current, (root / ".clippy.toml").read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        print(f"RUST_LINT_DEBT_FAILED input={error}", file=sys.stderr)
        return 2
    if findings:
        for finding in findings:
            print(f"RUST_LINT_DEBT_FINDING {finding}", file=sys.stderr)
        print(f"RUST_LINT_DEBT_FAILED findings={len(findings)}", file=sys.stderr)
        return 1
    scopes = Counter(entry["scope"] for entry in current)
    print(
        f"RUST_LINT_DEBT_OK entries={len(current)} "
        f"crate={scopes['crate']} module={scopes['module']} item={scopes['item']} threshold=12 next=8"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
