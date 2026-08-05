#!/usr/bin/env python3
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

"""Fail-closed comparison of pinned Java schema and Rust header source inventory."""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path


FIELD_PATTERN = re.compile(
    r"(?P<attrs>(?:\s*#\[[\s\S]*?\]\s*)*)(?:pub\s+)?(?P<name>\w+)\s*:\s*(?P<type>[\s\S]+)$"
)
RENAME_PATTERN = re.compile(r'\brename\s*=\s*"([^"]+)"')
ALIAS_PATTERN = re.compile(r'\balias\s*=\s*"([^"]+)"')
DEFAULT_PATTERN = re.compile(r'\bdefault\s*=\s*"([^"]+)"')


@dataclass(frozen=True)
class RustField:
    key: str
    rust_type: str
    wire_type: str
    presence: str
    aliases: tuple[str, ...]
    default_path: str | None
    declared_in: str
    inheritance_depth: int


def load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def snake_to_camel(value: str) -> str:
    parts = value.split("_")
    return parts[0] + "".join(part[:1].upper() + part[1:] for part in parts[1:])


def struct_body(source: str, name: str) -> str:
    match = re.search(rf"\bpub\s+struct\s+{re.escape(name)}\b[^{{]*{{", source)
    if match is None:
        raise ValueError(f"unable to find public struct {name}")
    start = source.find("{", match.start()) + 1
    depth = 1
    for index in range(start, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[start:index]
    raise ValueError(f"unterminated struct {name}")


def split_fields(body: str) -> list[str]:
    fields: list[str] = []
    start = 0
    angle = bracket = paren = brace = 0
    in_string = False
    escaped = False
    for index, char in enumerate(body):
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "<":
            angle += 1
        elif char == ">":
            angle = max(0, angle - 1)
        elif char == "[":
            bracket += 1
        elif char == "]":
            bracket = max(0, bracket - 1)
        elif char == "(":
            paren += 1
        elif char == ")":
            paren = max(0, paren - 1)
        elif char == "{":
            brace += 1
        elif char == "}":
            brace = max(0, brace - 1)
        elif char == "," and angle == bracket == paren == brace == 0:
            fields.append(body[start:index])
            start = index + 1
    if body[start:].strip():
        fields.append(body[start:])
    return fields


def strip_line_comments(value: str) -> str:
    return "\n".join(line.split("//", 1)[0] for line in value.splitlines()).strip()


def unwrap_option(rust_type: str) -> tuple[str, bool]:
    compact = re.sub(r"\s+", "", rust_type)
    match = re.fullmatch(r"Option<(.+)>", compact)
    return (match.group(1), True) if match else (compact, False)


def simple_type(rust_type: str) -> str:
    return rust_type.rsplit("::", 1)[-1]


def normalize_type(rust_type: str) -> str:
    base, _ = unwrap_option(rust_type)
    name = simple_type(base)
    if name in {"CheetahString", "String", "str"}:
        return "string"
    if name in {"i8", "i16", "i32", "u8", "u16", "u32"}:
        return "i32"
    if name in {"i64", "u64", "isize", "usize"}:
        return "i64"
    if name == "bool":
        return "bool"
    if name == "BoundaryType":
        return "boundary-type"
    return f"struct:{name}"


def normalize_java_type(java_type: str) -> str:
    if java_type == "java.lang.String":
        return "string"
    if java_type in {"int", "java.lang.Integer", "short", "java.lang.Short", "byte", "java.lang.Byte"}:
        return "i32"
    if java_type in {"long", "java.lang.Long"}:
        return "i64"
    if java_type in {"boolean", "java.lang.Boolean"}:
        return "bool"
    if java_type == "org.apache.rocketmq.common.BoundaryType":
        return "boundary-type"
    return f"java:{java_type}"


def parse_direct_fields(entry: dict[str, object], repo_root: Path) -> list[tuple[RustField, bool, str | None]]:
    source_path = repo_root / str(entry["rustSource"])
    source = source_path.read_text(encoding="utf-8")
    body = strip_line_comments(struct_body(source, str(entry["rustType"])))
    parsed: list[tuple[RustField, bool, str | None]] = []
    for raw in split_fields(body):
        segment = strip_line_comments(raw)
        match = FIELD_PATTERN.fullmatch(segment)
        if match is None:
            if segment.strip():
                raise ValueError(f"unparsed field in {source_path}: {segment!r}")
            continue
        attrs = match.group("attrs")
        if re.search(r"#\[serde\([^]]*\bskip\b", attrs):
            continue
        name = match.group("name")
        rust_type = match.group("type").strip()
        base_type, optional = unwrap_option(rust_type)
        key_match = RENAME_PATTERN.search(attrs)
        key = key_match.group(1) if key_match else snake_to_camel(name)
        aliases = tuple(ALIAS_PATTERN.findall(attrs))
        default_match = DEFAULT_PATTERN.search(attrs)
        required = "#[required]" in attrs
        flatten = bool(re.search(r"#\[serde\([^]]*\bflatten\b", attrs))
        wire_type = normalize_type(rust_type)
        if (
            entry["rustType"] == "SendMessageRequestHeaderV2"
            and name in {"a", "b", "c", "d", "e", "f", "g", "h"}
        ):
            required = True
        presence = "required" if required else "optional"
        if not optional and not required and wire_type in {"i32", "i64", "bool"}:
            presence = "primitive"
        parsed.append(
            (
                RustField(
                    key=key,
                    rust_type=rust_type,
                    wire_type=wire_type,
                    presence=presence,
                    aliases=aliases,
                    default_path=default_match.group(1) if default_match else None,
                    declared_in=str(entry["rustTypeId"]),
                    inheritance_depth=0,
                ),
                flatten,
                simple_type(base_type) if flatten else None,
            )
        )
    return parsed


def build_inventory(mapping: dict[str, object], repo_root: Path) -> dict[str, list[RustField]]:
    entries = {str(entry["rustType"]): entry for entry in mapping["entries"]}
    cache: dict[str, list[RustField]] = {}
    active: set[str] = set()

    def expand(name: str) -> list[RustField]:
        if name in cache:
            return cache[name]
        if name in active:
            raise ValueError(f"flatten cycle involving {name}")
        entry = entries.get(name)
        if entry is None:
            raise ValueError(f"flattened header type is not mapped: {name}")
        active.add(name)
        result: list[RustField] = []
        seen: set[str] = set()
        for field, flatten, nested_name in parse_direct_fields(entry, repo_root):
            nested = expand(nested_name) if flatten and nested_name else [field]
            for child in nested:
                expanded = RustField(
                    key=child.key,
                    rust_type=child.rust_type,
                    wire_type=child.wire_type,
                    presence=child.presence,
                    aliases=child.aliases,
                    default_path=child.default_path,
                    declared_in=child.declared_in,
                    inheritance_depth=child.inheritance_depth + (1 if flatten else 0),
                )
                if expanded.key in seen:
                    raise ValueError(f"duplicate canonical key {expanded.key} in {entry['rustTypeId']}")
                seen.add(expanded.key)
                result.append(expanded)
        active.remove(name)
        cache[name] = result
        return result

    for name in entries:
        expand(name)
    return {str(entry["rustTypeId"]): cache[str(entry["rustType"])] for entry in mapping["entries"]}


def validate_review_metadata(overrides: dict[str, object]) -> list[str]:
    errors: list[str] = []
    for group in ("defaults", "nameMappings", "aliasConflictPolicies", "requiredDrift"):
        for index, entry in enumerate(overrides.get(group, [])):
            for field in ("owner", "reason", "referenceSource"):
                if not entry.get(field):
                    errors.append(f"schema-overrides.{group}[{index}] lacks {field}")
    return errors


def compare(
    mapping: dict[str, object],
    java_schema: dict[str, object],
    inventory: dict[str, list[RustField]],
    overrides: dict[str, object],
    extensions: dict[str, object],
) -> list[str]:
    errors = validate_review_metadata(overrides)
    extension_fields = {
        str(entry["rustTypeId"]): set(entry["fields"])
        for entry in extensions.get("extensions", [])
    }
    required_decisions = {
        (str(entry["rustType"]), str(entry["field"])): entry
        for entry in overrides.get("requiredDrift", [])
    }
    mapping_by_id = {str(entry["rustTypeId"]): entry for entry in mapping["entries"]}
    java_by_id = {str(header["rustTypeId"]): header for header in java_schema["headers"]}

    mapped_ids = {type_id for type_id, entry in mapping_by_id.items() if entry["status"] == "mapped"}
    if mapped_ids != set(java_by_id):
        errors.append(
            f"mapped/schema type IDs differ: missing={sorted(mapped_ids - set(java_by_id))}, "
            f"extra={sorted(set(java_by_id) - mapped_ids)}"
        )

    for type_id in sorted(mapped_ids & set(java_by_id)):
        entry = mapping_by_id[type_id]
        rust_fields = {field.key: field for field in inventory[type_id]}
        java_fields = {str(field["key"]): field for field in java_by_id[type_id]["fields"]}
        allowed_extra = extension_fields.get(type_id, set())
        missing = set(java_fields) - set(rust_fields)
        extra = set(rust_fields) - set(java_fields) - allowed_extra
        if missing:
            errors.append(f"{type_id}: missing Rust canonical fields {sorted(missing)}")
        if extra:
            errors.append(f"{type_id}: unreviewed Rust extension fields {sorted(extra)}")
        for key in sorted(set(rust_fields) & set(java_fields)):
            rust_field = rust_fields[key]
            java_field = java_fields[key]
            java_wire_type = normalize_java_type(str(java_field["javaType"]))
            if rust_field.wire_type != java_wire_type:
                errors.append(
                    f"{type_id}.{key}: type {rust_field.wire_type} != Java {java_wire_type}"
                )
            java_presence = str(java_field["presence"])
            if rust_field.presence != java_presence:
                decision = required_decisions.get((str(entry["rustType"]), key))
                if decision is None or decision.get("javaPresence") != java_presence:
                    errors.append(
                        f"{type_id}.{key}: presence {rust_field.presence} != Java {java_presence} without review"
                    )

    return errors


def main() -> int:
    script = Path(__file__).resolve()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=script.parents[2])
    parser.add_argument(
        "--mapping",
        type=Path,
        default=script.parent / "header-class-map.json",
    )
    parser.add_argument(
        "--java-schema",
        type=Path,
        default=script.parents[2]
        / "rocketmq-protocol"
        / "tests"
        / "fixtures"
        / "request_header_codec"
        / "java-schema.json",
    )
    parser.add_argument("--overrides", type=Path, default=script.parent / "schema-overrides.json")
    parser.add_argument("--extensions", type=Path, default=script.parent / "extension-allowlist.json")
    parser.add_argument("--inventory-output", type=Path)
    args = parser.parse_args()

    try:
        mapping = load_json(args.mapping)
        java_schema = load_json(args.java_schema)
        overrides = load_json(args.overrides)
        extensions = load_json(args.extensions)
        inventory = build_inventory(mapping, args.repo_root.resolve())
        if args.inventory_output:
            document = {
                "schemaVersion": 1,
                "headers": [
                    {
                        "rustTypeId": type_id,
                        "fields": [field.__dict__ for field in fields],
                    }
                    for type_id, fields in sorted(inventory.items())
                ],
            }
            args.inventory_output.parent.mkdir(parents=True, exist_ok=True)
            args.inventory_output.write_text(
                json.dumps(document, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
                newline="\n",
            )
        errors = compare(mapping, java_schema, inventory, overrides, extensions)
        if errors:
            for error in errors:
                print(f"ERROR: {error}", file=sys.stderr)
            print(f"schema comparison failed with {len(errors)} unreviewed differences", file=sys.stderr)
            return 1
        print(
            f"schema comparison passed: {java_schema['mappedHeaderCount']} Java mappings, "
            "0 unreviewed differences"
        )
        return 0
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
