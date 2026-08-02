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

"""Require deliberate classification for core runtime, transport, and data-plane exports."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

from environment_write_guard import mask_comments_and_literals


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "scripts" / "public-api-intent.json"
CRATES = {
    "rocketmq-client-rust": (
        "client",
        (
            "rocketmq-client/src/lib.rs",
            "rocketmq-client/src/public_api.rs",
            "rocketmq-client/src/prelude.rs",
        ),
    ),
    "rocketmq-transport": (
        "transport",
        (
            "rocketmq-transport/src/lib.rs",
            "rocketmq-transport/src/public_api.rs",
            "rocketmq-transport/src/prelude.rs",
        ),
    ),
    "rocketmq-runtime": (
        "runtime",
        (
            "rocketmq-runtime/src/lib.rs",
            "rocketmq-runtime/src/public_api.rs",
            "rocketmq-runtime/src/prelude.rs",
        ),
    ),
    "rocketmq-store": (
        "store",
        (
            "rocketmq-store/src/lib.rs",
            "rocketmq-store/src/public_api.rs",
        ),
    ),
    "rocketmq-store-local": (
        "store-local",
        ("rocketmq-store-local/src/lib.rs",),
    ),
    "rocketmq-store-rocksdb": (
        "store-rocksdb",
        ("rocketmq-store-rocksdb/src/lib.rs",),
    ),
    "rocketmq-model": (
        "model",
        ("rocketmq-model/src/lib.rs",),
    ),
    "rocketmq-protocol": (
        "protocol",
        ("rocketmq-protocol/src/lib.rs",),
    ),
}
PUBLIC_DECLARATION = re.compile(
    r"\bpub\s+(?P<kind>mod|use|type|struct|enum|trait|fn|const|static)\s+"
)
CATEGORIES = {"stable", "experimental", "compat"}
REQUIRED_ENTRY_FIELDS = {
    "identity",
    "path",
    "declaration",
    "category",
    "owner",
    "rationale",
    "removal_condition",
}


def brace_depths(masked: str) -> list[int]:
    depths: list[int] = [0] * (len(masked) + 1)
    depth = 0
    for index, character in enumerate(masked):
        depths[index] = depth
        if character == "{":
            depth += 1
        elif character == "}":
            depth = max(0, depth - 1)
    depths[len(masked)] = depth
    return depths


def declaration_end(masked: str, start: int, kind: str) -> int:
    semicolon = masked.find(";", start)
    opening = masked.find("{", start)
    if kind == "mod" and opening != -1 and (semicolon == -1 or opening < semicolon):
        return opening
    if semicolon == -1:
        return len(masked)
    return semicolon


def inventory_source(relative: str, source: str, owner: str) -> list[dict[str, str]]:
    masked = mask_comments_and_literals(source)
    depths = brace_depths(masked)
    entries: list[dict[str, str]] = []
    for match in PUBLIC_DECLARATION.finditer(masked):
        if depths[match.start()] != 0:
            continue
        kind = match.group("kind")
        end = declaration_end(masked, match.start(), kind)
        declaration = re.sub(r"\s+", " ", source[match.start():end].strip())
        identity = f"{relative}:{declaration}"
        prefix = source[max(0, match.start() - 160):match.start()]
        if "compat" in declaration.lower() or "legacy" in declaration.lower() or "RocketMQRuntime" in declaration:
            category = "compat"
            rationale = "temporary source-level adapter; no new behavior may be added"
            removal_condition = "remove at the 2.0.0 source-compatibility boundary"
        elif relative.endswith("/prelude.rs"):
            category = "stable"
            rationale = "minimal common-use import set"
            removal_condition = "retain while the stable entry point remains supported"
        elif kind == "mod" or "#[doc(hidden)]" in prefix:
            category = "experimental"
            rationale = "explicitly exposed module or diagnostic surface under non-growth review"
            removal_condition = "promote with an API decision or narrow/remove before 2.0.0"
        else:
            category = "stable"
            rationale = "deliberate root entry point"
            removal_condition = "retain while the stable entry point remains supported"
        entries.append(
            {
                "identity": identity,
                "path": relative,
                "declaration": declaration,
                "category": category,
                "owner": owner,
                "rationale": rationale,
                "removal_condition": removal_condition,
            }
        )
    return entries


def current_inventory(root: Path = ROOT) -> dict[str, list[dict[str, str]]]:
    result: dict[str, list[dict[str, str]]] = {}
    for crate, (owner, paths) in CRATES.items():
        entries: list[dict[str, str]] = []
        for relative in paths:
            entries.extend(inventory_source(relative, (root / relative).read_text(encoding="utf-8"), owner))
        result[crate] = sorted(entries, key=lambda entry: entry["identity"])
    return result


def render_manifest(
    inventory: dict[str, list[dict[str, str]]],
    previous: dict[str, Any] | None = None,
) -> dict[str, Any]:
    previous_entries: dict[str, dict[str, str]] = {}
    if isinstance(previous, dict):
        crates = previous.get("crates")
        if isinstance(crates, dict):
            for spec in crates.values():
                if not isinstance(spec, dict) or not isinstance(spec.get("entries"), list):
                    continue
                for entry in spec["entries"]:
                    if isinstance(entry, dict) and isinstance(entry.get("identity"), str):
                        previous_entries[entry["identity"]] = entry

    preserved_inventory: dict[str, list[dict[str, str]]] = {}
    for crate, entries in inventory.items():
        preserved_entries: list[dict[str, str]] = []
        for entry in entries:
            preserved = dict(entry)
            previous_entry = previous_entries.get(entry["identity"])
            if previous_entry is not None:
                for field in ("category", "owner", "rationale", "removal_condition"):
                    value = previous_entry.get(field)
                    if isinstance(value, str) and value:
                        preserved[field] = value
            preserved_entries.append(preserved)
        preserved_inventory[crate] = preserved_entries

    return {
        "schema_version": 1,
        "policy": "Every deliberate export is classified; additions and count growth fail closed.",
        "crates": {
            crate: {
                "maximum_exports": len(entries),
                "entries": entries,
            }
            for crate, entries in sorted(preserved_inventory.items())
        },
    }


def validate_manifest(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {"schema_version", "policy", "crates"}:
        raise ValueError("unexpected manifest schema")
    if value["schema_version"] != 1 or not isinstance(value["policy"], str) or not value["policy"]:
        raise ValueError("invalid manifest metadata")
    crates = value["crates"]
    if not isinstance(crates, dict) or set(crates) != set(CRATES):
        raise ValueError("manifest must cover every configured public API boundary")
    for crate, spec in crates.items():
        if not isinstance(spec, dict) or set(spec) != {"maximum_exports", "entries"}:
            raise ValueError(f"{crate} has an invalid manifest entry")
        entries = spec["entries"]
        if not isinstance(spec["maximum_exports"], int) or spec["maximum_exports"] < 0 or not isinstance(entries, list):
            raise ValueError(f"{crate} has invalid export limits")
        identities: set[str] = set()
        for entry in entries:
            if not isinstance(entry, dict) or set(entry) != REQUIRED_ENTRY_FIELDS:
                raise ValueError(f"{crate} contains invalid export metadata")
            if entry["category"] not in CATEGORIES:
                raise ValueError(f"{crate} contains an invalid category")
            if any(not isinstance(entry[field], str) or not entry[field] for field in REQUIRED_ENTRY_FIELDS):
                raise ValueError(f"{crate} contains empty export metadata")
            if entry["identity"] in identities:
                raise ValueError(f"{crate} contains duplicate export identities")
            identities.add(entry["identity"])
    return value


def compare(manifest: dict[str, Any], inventory: dict[str, list[dict[str, str]]]) -> list[str]:
    findings: list[str] = []
    for crate, entries in inventory.items():
        expected = manifest["crates"][crate]
        approved_entries = {entry["identity"]: entry for entry in expected["entries"]}
        current_entries = {entry["identity"]: entry for entry in entries}
        approved = set(approved_entries)
        current = set(current_entries)
        for identity in sorted(current - approved):
            findings.append(f"{crate}: unclassified export: {identity}")
        for identity in sorted(approved - current):
            findings.append(f"{crate}: stale export declaration: {identity}")
        for identity in sorted(approved & current):
            for field in ("path", "declaration", "owner"):
                if approved_entries[identity][field] != current_entries[identity][field]:
                    findings.append(f"{crate}: {field} metadata drift: {identity}")
        if len(entries) > expected["maximum_exports"]:
            findings.append(
                f"{crate}: export count grew: current={len(entries)} maximum={expected['maximum_exports']}"
            )
    return findings


def summary(manifest: dict[str, Any]) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    for crate, spec in manifest["crates"].items():
        counts = {category: 0 for category in sorted(CATEGORIES)}
        for entry in spec["entries"]:
            counts[entry["category"]] += 1
        counts["total"] = len(spec["entries"])
        result[crate] = counts
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write-manifest", action="store_true")
    parser.add_argument("--root", type=Path, default=ROOT, help=argparse.SUPPRESS)
    args = parser.parse_args()
    root = args.root.resolve()
    try:
        inventory = current_inventory(root)
        if args.write_manifest:
            previous = None
            if MANIFEST.exists():
                previous = json.loads(MANIFEST.read_text(encoding="utf-8"))
            value = validate_manifest(render_manifest(inventory, previous))
            generated_findings = compare(value, inventory)
            if generated_findings:
                raise ValueError("generated manifest is inconsistent: " + "; ".join(generated_findings))
            MANIFEST.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            print(
                "PUBLIC_API_INTENT_WRITTEN "
                + " ".join(f"{crate}={len(entries)}" for crate, entries in inventory.items())
            )
            return 0
        manifest = validate_manifest(json.loads(MANIFEST.read_text(encoding="utf-8")))
        findings = compare(manifest, inventory)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        print(f"PUBLIC_API_INTENT_FAILED input={error}", file=sys.stderr)
        return 2
    if findings:
        for finding in findings:
            print(f"PUBLIC_API_INTENT_FINDING {finding}", file=sys.stderr)
        print(f"PUBLIC_API_INTENT_FAILED findings={len(findings)}", file=sys.stderr)
        return 1
    counts = summary(manifest)
    print(
        "PUBLIC_API_INTENT_OK "
        + " ".join(f"{crate}={values['total']}" for crate, values in sorted(counts.items()))
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
