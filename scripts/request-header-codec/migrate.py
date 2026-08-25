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

"""Inventory, plan, and guard RequestHeaderCodec migrations without rewriting Rust source."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any


DERIVE_STRUCT_PATTERN = re.compile(
    r"#\[derive\((?P<derive>[\s\S]*?)\)\]"
    r"(?P<attrs>(?:\s*#\[[\s\S]*?\])*)\s*pub\s+struct\s+(?P<name>\w+)",
)
FIELD_PATTERN = re.compile(
    r"(?P<attrs>(?:\s*#\[[\s\S]*?\]\s*)*)(?:pub\s+)?(?P<name>\w+)\s*:\s*(?P<type>[\s\S]+)$"
)
FAST_IMPL_PATTERN = re.compile(r"\bimpl(?:\s*<[^>]+>)?\s+FastCodesHeader\s+for\s+(?P<name>\w+)")
TYPE_ID_PATTERN = re.compile(r'\btype_id\s*=\s*"([^"]+)"')
JAVA_CLASS_PATTERN = re.compile(r'\bjava_class\s*=\s*"([^"]+)"')

MANUAL_BASELINE_TYPES = {
    "rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2"
}
KNOWN_POST_BASELINE_V2_EXCEPTIONS = {
    "rocketmq_protocol::protocol::header::check_transaction_state_response_header::CheckTransactionStateResponseHeader": {
        "owner": "rocketmq-rust protocol maintainers",
        "reason": "The response header was added after the 150-derive historical snapshot and predates the no-new-V2 guard",
        "reviewBy": "2027-08-01",
    }
}
HOT_REQUEST_CODES = {
    "CONSUMER_SEND_MSG_BACK",
    "GET_CONSUMER_STATUS_FROM_CLIENT",
    "PULL_MESSAGE",
    "QUERY_CONSUME_QUEUE",
    "SEARCH_OFFSET_BY_TIMESTAMP",
    "SEND_BATCH_MESSAGE",
    "SEND_MESSAGE",
    "SEND_MESSAGE_V2",
}


@dataclass(frozen=True)
class FieldShape:
    name: str
    rust_type: str
    flatten: bool
    legacy_required: bool


@dataclass(frozen=True)
class HeaderInventory:
    type_id: str
    name: str
    source: str
    codec: str
    fast: bool
    declared_type_id: str | None
    declared_java_class: str | None
    fields: tuple[FieldShape, ...]
    semantic_risk: bool


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def serialize(document: dict[str, Any]) -> str:
    return json.dumps(document, ensure_ascii=False, indent=2) + "\n"


def strip_line_comments(value: str) -> str:
    return "\n".join(line.split("//", 1)[0] for line in value.splitlines()).strip()


def struct_body(source: str, name: str) -> str:
    match = re.search(rf"\bpub\s+struct\s+{re.escape(name)}\b[^{{]*{{", source)
    if match is None:
        raise ValueError(f"unable to find public struct {name}")
    start = source.find("{", match.start()) + 1
    depth = 1
    in_string = False
    escaped = False
    for index in range(start, len(source)):
        char = source[index]
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
        elif char == "{":
            depth += 1
        elif char == "}":
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


def parse_fields(body: str, source_path: Path, name: str) -> tuple[FieldShape, ...]:
    fields: list[FieldShape] = []
    for raw in split_fields(strip_line_comments(body)):
        segment = strip_line_comments(raw)
        match = FIELD_PATTERN.fullmatch(segment)
        if match is None:
            if segment:
                raise ValueError(f"unparsed field in {source_path}::{name}: {segment!r}")
            continue
        attrs = match.group("attrs")
        if re.search(r"#\[serde\([^]]*\bskip\b", attrs):
            continue
        fields.append(
            FieldShape(
                name=match.group("name"),
                rust_type=re.sub(r"\s+", "", match.group("type")),
                flatten=bool(re.search(r"#\[serde\([^]]*\bflatten\b", attrs)),
                legacy_required=bool(re.search(r"#\[\s*required\s*\]", attrs)),
            )
        )
    return tuple(fields)


def source_type_id(source_root: Path, source: Path, name: str) -> str:
    relative = source.relative_to(source_root).with_suffix("")
    return f"rocketmq_protocol::{'::'.join(relative.parts)}::{name}"


def scan_headers(repo_root: Path) -> tuple[dict[str, HeaderInventory], list[str], set[str]]:
    source_root = repo_root / "rocketmq-protocol" / "src"
    headers: dict[str, HeaderInventory] = {}
    legacy_derives: list[str] = []
    fast_impls: set[str] = set()
    fast_impl_names: list[tuple[Path, str]] = []

    for source_path in sorted(source_root.rglob("*.rs")):
        source = source_path.read_text(encoding="utf-8")
        relative = (Path("rocketmq-protocol") / "src" / source_path.relative_to(source_root)).as_posix()
        for match in DERIVE_STRUCT_PATTERN.finditer(source):
            derives = {item.strip().rsplit("::", 1)[-1] for item in match.group("derive").split(",")}
            codecs = derives & {"RequestHeaderCodec", "RequestHeaderCodecV2", "RequestHeaderCodecV3"}
            if not codecs:
                continue
            name = match.group("name")
            type_id = source_type_id(source_root, source_path, name)
            if "RequestHeaderCodec" in codecs:
                legacy_derives.append(type_id)
                continue
            if len(codecs) != 1:
                raise ValueError(f"{type_id} declares multiple request-header codecs: {sorted(codecs)}")
            codec = "v3" if "RequestHeaderCodecV3" in codecs else "v2"
            attrs = match.group("attrs")
            header_attrs = "\n".join(re.findall(r"#\[header\(([\s\S]*?)\)\]", attrs))
            body = struct_body(source, name)
            inventory = HeaderInventory(
                type_id=type_id,
                name=name,
                source=relative,
                codec=codec,
                fast=codec == "v3" and bool(re.search(r"(?:^|,)\s*fast\s*(?:,|$)", header_attrs)),
                declared_type_id=(TYPE_ID_PATTERN.search(header_attrs).group(1) if TYPE_ID_PATTERN.search(header_attrs) else None),
                declared_java_class=(
                    JAVA_CLASS_PATTERN.search(header_attrs).group(1) if JAVA_CLASS_PATTERN.search(header_attrs) else None
                ),
                fields=parse_fields(body, source_path, name),
                semantic_risk=bool(
                    re.search(
                        r"#\[\s*required\s*\]|#\[\s*header\([^]]*\b(?:required|default|default_with|range|alias)\b|\b[ui](?:32|64)\b",
                        body,
                    )
                ),
            )
            if type_id in headers:
                raise ValueError(f"duplicate request-header type ID {type_id}")
            headers[type_id] = inventory
        fast_impl_names.extend((source_path, match.group("name")) for match in FAST_IMPL_PATTERN.finditer(source))

    by_name: dict[str, list[HeaderInventory]] = {}
    for header in headers.values():
        by_name.setdefault(header.name, []).append(header)
    for source_path, name in fast_impl_names:
        candidates = [header for header in by_name.get(name, []) if repo_root / header.source == source_path]
        if len(candidates) != 1:
            raise ValueError(f"unable to resolve FastCodesHeader implementation {source_path}::{name}")
        fast_impls.add(candidates[0].type_id)
    return headers, legacy_derives, fast_impls


def field_base_type(rust_type: str) -> str:
    value = rust_type
    while True:
        match = re.fullmatch(r"(?:Option|Box)<(.+)>", value)
        if match is None:
            return value
        value = match.group(1)


def resolve_flatten_target(
    header: HeaderInventory,
    field: FieldShape,
    headers: dict[str, HeaderInventory],
    repo_root: Path,
) -> str:
    base = field_base_type(field.rust_type)
    simple = base.rsplit("::", 1)[-1]
    candidates = [candidate for candidate in headers.values() if candidate.name == simple]
    if len(candidates) == 1:
        return candidates[0].type_id
    local = [candidate for candidate in candidates if candidate.source == header.source]
    if len(local) == 1:
        return local[0].type_id

    source = (repo_root / header.source).read_text(encoding="utf-8")
    imported: dict[str, HeaderInventory] = {}
    for use_path in re.findall(rf"\buse\s+([^;{{}}]*\b{re.escape(simple)})\s*;", source):
        normalized = use_path.strip().replace("crate::", "rocketmq_protocol::", 1)
        imported.update((candidate.type_id, candidate) for candidate in candidates if candidate.type_id == normalized)
    if len(imported) == 1:
        return next(iter(imported))
    raise ValueError(f"unable to resolve flattened type {base} in {header.type_id}")


def flatten_graph(
    headers: dict[str, HeaderInventory], repo_root: Path
) -> tuple[dict[str, list[str]], dict[str, int]]:
    graph: dict[str, list[str]] = {}
    for type_id, header in headers.items():
        graph[type_id] = [
            resolve_flatten_target(header, field, headers, repo_root) for field in header.fields if field.flatten
        ]

    depths: dict[str, int] = {}
    active: set[str] = set()

    def depth(type_id: str) -> int:
        if type_id in depths:
            return depths[type_id]
        if type_id in active:
            raise ValueError(f"flatten cycle involving {type_id}")
        active.add(type_id)
        value = max((1 + depth(nested) for nested in graph[type_id]), default=0)
        active.remove(type_id)
        depths[type_id] = value
        return value

    for type_id in graph:
        depth(type_id)
    return graph, depths


def historical_v2_type_ids(repo_root: Path, mapping: dict[str, Any]) -> list[str]:
    commit = str(mapping["rustHistoricalCommit"])
    historical: list[str] = []
    for entry in mapping["entries"]:
        result = subprocess.run(
            ["git", "-C", str(repo_root), "show", f"{commit}:{entry['rustSource']}"],
            check=False,
            capture_output=True,
        )
        if result.returncode != 0:
            continue
        source = result.stdout.decode("utf-8")
        matches = [match for match in DERIVE_STRUCT_PATTERN.finditer(source) if match.group("name") == entry["rustType"]]
        if len(matches) == 1 and "RequestHeaderCodecV2" in matches[0].group("derive"):
            historical.append(str(entry["rustTypeId"]))
    expected = int(mapping["historicalDeriveCount"])
    if len(historical) != expected:
        raise ValueError(f"expected {expected} historical V2 derives, found {len(historical)}")
    return sorted(historical)


def entry_wave(header: HeaderInventory, mapping_entry: dict[str, Any], flatten_depth: int) -> str:
    if header.codec == "v3":
        return "A"
    if mapping_entry["status"] == "rust-only-extension":
        return "E"
    if flatten_depth:
        return "B"
    if header.semantic_risk:
        return "C"
    return "D"


def entry_risk(header: HeaderInventory, mapping_entry: dict[str, Any], flatten_depth: int) -> str:
    if header.fast or HOT_REQUEST_CODES.intersection(mapping_entry["requestCodes"]):
        return "tier1"
    if flatten_depth or header.semantic_risk:
        return "tier2"
    return "tier3"


def initial_baseline(
    repo_root: Path,
    mapping: dict[str, Any],
    headers: dict[str, HeaderInventory],
    fast_impls: set[str],
) -> dict[str, Any]:
    historical = historical_v2_type_ids(repo_root, mapping)
    legacy_required = sorted(
        f"{header.type_id}::{field.name}"
        for header in headers.values()
        for field in header.fields
        if field.legacy_required
    )
    return {
        "v2TypeIds": historical,
        "legacyV2Exceptions": KNOWN_POST_BASELINE_V2_EXCEPTIONS,
        "acceptedV3TypeIds": sorted(type_id for type_id, header in headers.items() if header.codec == "v3"),
        "legacyRequiredFields": legacy_required,
        "legacyFastImpls": sorted(fast_impls),
    }


def build_manifest(
    repo_root: Path,
    mapping: dict[str, Any],
    headers: dict[str, HeaderInventory],
    fast_impls: set[str],
    existing: dict[str, Any] | None,
) -> dict[str, Any]:
    graph, depths = flatten_graph(headers, repo_root)
    baseline = existing["baseline"] if existing else initial_baseline(repo_root, mapping, headers, fast_impls)
    accepted_v3 = sorted(
        set(baseline["acceptedV3TypeIds"])
        | {type_id for type_id, header in headers.items() if header.codec == "v3"}
    )
    baseline = {**baseline, "acceptedV3TypeIds": accepted_v3}
    baseline_v2 = set(baseline["v2TypeIds"])
    exceptions = baseline["legacyV2Exceptions"]

    entries: list[dict[str, Any]] = []
    for mapping_entry in mapping["entries"]:
        type_id = str(mapping_entry["rustTypeId"])
        header = headers[type_id]
        extension = None
        if mapping_entry["status"] == "rust-only-extension":
            extension = {
                "classification": "rust-extension-safe",
                "owner": "rocketmq-rust protocol maintainers",
                "reason": mapping_entry["reason"],
                "reviewBy": "2027-08-01",
            }
        if type_id in baseline_v2:
            baseline_codec = "v2"
        elif type_id in MANUAL_BASELINE_TYPES:
            baseline_codec = "manual"
        else:
            baseline_codec = "absent"
        entries.append(
            {
                "rustTypeId": type_id,
                "rustType": mapping_entry["rustType"],
                "rustSource": mapping_entry["rustSource"],
                "javaClass": mapping_entry["javaClass"],
                "requestCodes": mapping_entry["requestCodes"],
                "baselineCodec": baseline_codec,
                "v2AtBaseline": type_id in baseline_v2,
                "legacyV2Exception": exceptions.get(type_id),
                "currentCodec": header.codec,
                "flattenDepth": depths[type_id],
                "flattenTypeIds": graph[type_id],
                "fastCodec": header.fast,
                "risk": entry_risk(header, mapping_entry, depths[type_id]),
                "wave": entry_wave(header, mapping_entry, depths[type_id]),
                "owner": "remoting",
                "status": "migrated" if header.codec == "v3" else "pending",
                "extensionDecision": extension,
            }
        )

    v3_count = sum(entry["currentCodec"] == "v3" for entry in entries)
    return {
        "schemaVersion": 1,
        "contractVersion": "request-header-codec-v3-migration-v1",
        "rustHistoricalCommit": mapping["rustHistoricalCommit"],
        "javaCommit": mapping["javaCommit"],
        "generatedBy": "scripts/request-header-codec/migrate.py inventory",
        "summary": {
            "entryCount": len(entries),
            "mappedCount": mapping["mappedCount"],
            "rustOnlyExtensionCount": mapping["rustOnlyExtensionCount"],
            "v2Count": len(entries) - v3_count,
            "v3Count": v3_count,
            "pendingCount": len(entries) - v3_count,
            "migratedCount": v3_count,
        },
        "baseline": baseline,
        "entries": entries,
    }


def digest(path: Path) -> str:
    payload = path.read_bytes().replace(b"\r\n", b"\n")
    return hashlib.sha256(payload).hexdigest()


def validate_review_date(value: str, label: str, errors: list[str]) -> None:
    try:
        review_by = date.fromisoformat(value)
    except ValueError:
        errors.append(f"{label} has invalid review date {value!r}")
        return
    if review_by < date.today():
        errors.append(f"{label} review expired on {value}")


def validate_contract_assets(repo_root: Path, mapping: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    script_root = repo_root / "scripts" / "request-header-codec"
    fixture_root = repo_root / "rocketmq-protocol" / "tests" / "fixtures" / "request_header_codec"
    java_schema = load_json(fixture_root / "java-schema.json")
    fixture_manifest = load_json(fixture_root / "manifest.json")
    extensions = load_json(script_root / "extension-allowlist.json")
    aliases = load_json(script_root / "legacy-alias-window.json")
    overrides = load_json(script_root / "schema-overrides.json")

    if mapping["javaCommit"] != java_schema["javaCommit"] or mapping["javaCommit"] != fixture_manifest["javaCommit"]:
        errors.append("mapping, Java schema, and fixture manifest pin different Java commits")
    mapped_ids = {entry["rustTypeId"] for entry in mapping["entries"] if entry["status"] == "mapped"}
    schema_ids = {header["rustTypeId"] for header in java_schema["headers"]}
    if mapped_ids != schema_ids:
        errors.append("header mapping and pinned Java schema contain different stable type IDs")
    rust_only = {entry["rustTypeId"] for entry in mapping["entries"] if entry["status"] == "rust-only-extension"}
    if rust_only != set(extensions["rustOnlyTypes"]):
        errors.append("header mapping and extension allowlist contain different Rust-only type IDs")

    for extension in extensions["extensions"]:
        validate_review_date(extension["expiry"], f"extension {extension['rustTypeId']}", errors)

    for section in ("schema", "goldenIndex"):
        record = fixture_manifest[section]
        path = fixture_root / record["file"]
        if not path.is_file() or digest(path) != record["sha256"]:
            errors.append(f"fixture manifest checksum mismatch for {record['file']}")
    for record in fixture_manifest["goldenFiles"] + fixture_manifest["legacyEmptyHeaders"]:
        path = fixture_root / record["file"]
        if not path.is_file() or digest(path) != record["sha256"]:
            errors.append(f"fixture manifest checksum mismatch for {record['file']}")

    type_ids_by_name: dict[str, list[str]] = {}
    for entry in mapping["entries"]:
        type_ids_by_name.setdefault(entry["rustType"], []).append(entry["rustTypeId"])
    expected_aliases: set[tuple[str, str, str]] = set()
    for override in overrides["aliasConflictPolicies"]:
        candidates = type_ids_by_name.get(override["rustType"], [])
        if len(candidates) != 1:
            errors.append(f"alias override uses ambiguous Rust type {override['rustType']}")
            continue
        expected_aliases.update((candidates[0], override["canonical"], alias) for alias in override["aliases"])
    actual_aliases = {
        (entry["rustTypeId"], entry["canonical"], entry["alias"]) for entry in aliases["aliases"]
    }
    if expected_aliases != actual_aliases:
        errors.append("legacy alias window and reviewed alias policies differ")
    for entry in aliases["aliases"]:
        validate_review_date(entry["reviewBy"], f"legacy alias {entry['rustTypeId']}.{entry['alias']}", errors)
    return errors


def validate_inventory(
    mapping: dict[str, Any],
    manifest: dict[str, Any],
    headers: dict[str, HeaderInventory],
    legacy_derives: list[str],
    fast_impls: set[str],
) -> list[str]:
    errors: list[str] = []
    mapping_by_id = {entry["rustTypeId"]: entry for entry in mapping["entries"]}
    if len(mapping_by_id) != len(mapping["entries"]):
        errors.append("header-class-map.json contains duplicate stable type IDs")
    if set(mapping_by_id) != set(headers):
        errors.append(
            "source/mapping type IDs differ: "
            f"unregistered={sorted(set(headers) - set(mapping_by_id))}, "
            f"missing={sorted(set(mapping_by_id) - set(headers))}"
        )
    for type_id in sorted(set(mapping_by_id) & set(headers)):
        entry = mapping_by_id[type_id]
        header = headers[type_id]
        if entry["rustSource"] != header.source or entry["rustType"] != header.name:
            errors.append(f"{type_id} source/name drifted from the reviewed mapping")
        if header.codec == "v3":
            if header.declared_type_id != type_id:
                errors.append(f"{type_id} must declare its full stable V3 type_id")
            if entry["javaClass"] is not None and header.declared_java_class != entry["javaClass"]:
                errors.append(f"{type_id} V3 java_class drifted from the reviewed mapping")

    if legacy_derives:
        errors.append(f"legacy RequestHeaderCodec derives are forbidden: {sorted(legacy_derives)}")
    baseline = manifest["baseline"]
    allowed_v2 = set(baseline["v2TypeIds"]) | set(baseline["legacyV2Exceptions"])
    current_v2 = {type_id for type_id, header in headers.items() if header.codec == "v2"}
    if current_v2 - allowed_v2:
        errors.append(f"new V2 derives are forbidden: {sorted(current_v2 - allowed_v2)}")
    current_v3 = {type_id for type_id, header in headers.items() if header.codec == "v3"}
    accepted_v3 = set(baseline["acceptedV3TypeIds"])
    if accepted_v3 - current_v3:
        errors.append(f"accepted V3 headers cannot return to V2: {sorted(accepted_v3 - current_v3)}")

    legacy_required = {
        f"{header.type_id}::{field.name}"
        for header in headers.values()
        for field in header.fields
        if field.legacy_required
    }
    frozen_required = set(baseline["legacyRequiredFields"])
    if legacy_required - frozen_required:
        errors.append(f"new legacy #[required] fields are forbidden: {sorted(legacy_required - frozen_required)}")
    frozen_fast = set(baseline["legacyFastImpls"])
    if fast_impls - frozen_fast:
        errors.append(f"new standalone FastCodesHeader implementations are forbidden: {sorted(fast_impls - frozen_fast)}")
    return errors


def check(repo_root: Path, manifest_path: Path) -> int:
    try:
        mapping = load_json(repo_root / "scripts" / "request-header-codec" / "header-class-map.json")
        manifest = load_json(manifest_path)
        headers, legacy_derives, fast_impls = scan_headers(repo_root)
        errors = validate_inventory(mapping, manifest, headers, legacy_derives, fast_impls)
        errors.extend(validate_contract_assets(repo_root, mapping))
        expected = build_manifest(repo_root, mapping, headers, fast_impls, manifest)
        if serialize(expected) != manifest_path.read_text(encoding="utf-8"):
            errors.append("migration.json is stale; regenerate it with migrate.py inventory --output")
        if errors:
            for error in errors:
                print(f"ERROR: {error}", file=sys.stderr)
            print(f"request-header migration guard failed with {len(errors)} error(s)", file=sys.stderr)
            return 1
        summary = manifest["summary"]
        print(
            "request-header migration guard passed: "
            f"{summary['entryCount']} registered, {summary['v3Count']} V3, "
            f"{summary['v2Count']} V2, {summary['pendingCount']} pending, "
            f"{len(legacy_derives)} production legacy derive uses"
        )
        return 0
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


def inventory(repo_root: Path, manifest_path: Path, output: Path | None) -> int:
    try:
        mapping = load_json(repo_root / "scripts" / "request-header-codec" / "header-class-map.json")
        existing = load_json(manifest_path) if manifest_path.is_file() else None
        headers, legacy_derives, fast_impls = scan_headers(repo_root)
        if legacy_derives:
            raise ValueError(f"legacy RequestHeaderCodec derives are forbidden: {legacy_derives}")
        if set(headers) != {entry["rustTypeId"] for entry in mapping["entries"]}:
            raise ValueError("source inventory differs from header-class-map.json")
        document = build_manifest(repo_root, mapping, headers, fast_impls, existing)
        rendered = serialize(document)
        if output:
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(rendered, encoding="utf-8", newline="\n")
            print(f"wrote {output} with {document['summary']['entryCount']} entries")
        else:
            print(rendered, end="")
        return 0
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


def plan(repo_root: Path, manifest_path: Path) -> int:
    try:
        manifest = load_json(manifest_path)
        pending = [entry for entry in manifest["entries"] if entry["status"] == "pending"]
        print(
            f"RequestHeaderCodec migration plan: {len(pending)} pending, "
            f"{manifest['summary']['migratedCount']} migrated"
        )
        for wave in ("B", "C", "D", "E"):
            entries = [entry for entry in pending if entry["wave"] == wave]
            print(f"\nWave {wave}: {len(entries)}")
            for entry in entries:
                print(
                    f"- [{entry['risk']}] {entry['rustTypeId']} "
                    f"(flattenDepth={entry['flattenDepth']}, fast={str(entry['fastCodec']).lower()})"
                )
        return 0
    except (OSError, KeyError, json.JSONDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


def parse_args() -> argparse.Namespace:
    script = Path(__file__).resolve()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("inventory", "plan", "check"))
    parser.add_argument("--repo-root", type=Path, default=script.parents[2])
    parser.add_argument("--manifest", type=Path, default=script.parent / "migration.json")
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    manifest_path = args.manifest.resolve()
    if args.mode == "inventory":
        return inventory(repo_root, manifest_path, args.output.resolve() if args.output else None)
    if args.output:
        print("error: --output is only valid with inventory", file=sys.stderr)
        return 2
    if args.mode == "plan":
        return plan(repo_root, manifest_path)
    return check(repo_root, manifest_path)


if __name__ == "__main__":
    raise SystemExit(main())
