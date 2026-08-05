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

"""Generate the reviewed Rust-to-Java request-header class mapping."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


RUST_HISTORICAL_COMMIT = "0c4722568a74987f7be51df12ec87dbfdc05fbba"
JAVA_COMMIT = "2daf0e2ca91a1592d18235d43e5d709d1c35d15f"
HISTORICAL_DERIVE_COUNT = 150
EXPECTED_DERIVE_COUNT = 151

DERIVE_PATTERN = re.compile(
    r"#\[derive\((?P<derive>[^\]]*RequestHeaderCodecV2[^\]]*)\)\]"
    r"(?P<attrs>(?:\s*#\[[^\]]+\])*)\s*pub struct\s+(?P<name>\w+)",
    re.DOTALL,
)
PACKAGE_PATTERN = re.compile(r"\bpackage\s+([\w.]+)\s*;")
ACTION_PATTERN = re.compile(
    r"@RocketMQAction\s*\([^)]*?\bvalue\s*=\s*RequestCode\.([A-Z0-9_]+)",
    re.DOTALL,
)


JAVA_NAME_OVERRIDES = {
    "CleanBrokerDataRequestHeader": "CleanControllerBrokerDataRequestHeader",
    "ExchangeHaInfoResponseHeader": "ExchangeHAInfoResponseHeader",
    "ExportRocksdbConfigToJsonRequestHeader": "ExportRocksDBConfigToJsonRequestHeader",
    "GetTopicStatsRequestHeader": "GetTopicStatsInfoRequestHeader",
    "ListAclRequestHeader": "ListAclsRequestHeader",
}

RUST_ALIASES = {
    "CleanBrokerDataRequestHeader": [
        "rocketmq_protocol::protocol::header::controller::clean_broker_data_request_header::CleanControllerBrokerDataRequestHeader"
    ]
}

RUST_ONLY_EXTENSIONS = {
    "GetNamesrvConfigRequestHeader": {
        "requestCodes": ["GET_NAMESRV_CONFIG"],
        "reason": "Rust probe-only extension for the otherwise empty Java request header",
    },
    "MaintenanceRequestHeader": {
        "requestCodes": [
            "MAINTENANCE_CREATE_CONTROLLER_SNAPSHOT",
            "MAINTENANCE_CREATE_STORE_CHECKPOINT",
            "MAINTENANCE_VERIFY_CHECKPOINT",
            "MAINTENANCE_RESTORE_VERIFY",
        ],
        "reason": "Rust maintenance authorization and fencing extension",
    },
    "UpdateBrokerConfigRequestHeader": {
        "requestCodes": ["UPDATE_BROKER_CONFIG_CAS"],
        "reason": "Rust compare-and-set broker configuration extension",
    },
    "UpdateBrokerConfigResponseHeader": {
        "requestCodes": ["UPDATE_BROKER_CONFIG_CAS"],
        "reason": "Rust compare-and-set broker configuration response extension",
    },
    "UpdateGlobalWhiteAddrsConfigRequestHeader": {
        "requestCodes": ["UPDATE_GLOBAL_WHITE_ADDRS_CONFIG"],
        "reason": "Rust typed header for a Java request that uses dynamic fields and body data",
    },
    "UpdateSubscriptionGroupConfigCasRequestHeader": {
        "requestCodes": ["UPDATE_SUBSCRIPTION_GROUP_CONFIG_CAS"],
        "reason": "Rust compare-and-set subscription group extension",
    },
    "UpdateSubscriptionGroupConfigCasResponseHeader": {
        "requestCodes": ["UPDATE_SUBSCRIPTION_GROUP_CONFIG_CAS"],
        "reason": "Rust compare-and-set subscription group response extension",
    },
    "UpdateTopicConfigCasRequestHeader": {
        "requestCodes": ["UPDATE_TOPIC_CONFIG_CAS"],
        "reason": "Rust compare-and-set topic configuration extension",
    },
    "UpdateTopicConfigCasResponseHeader": {
        "requestCodes": ["UPDATE_TOPIC_CONFIG_CAS"],
        "reason": "Rust compare-and-set topic configuration response extension",
    },
}


@dataclass(frozen=True)
class RustHeader:
    name: str
    source: str
    type_id: str


MANUAL_HEADERS = [
    RustHeader(
        name="SendMessageRequestHeaderV2",
        source="rocketmq-protocol/src/protocol/header/message_operation_header/send_message_request_header_v2.rs",
        type_id="rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2",
    )
]


@dataclass(frozen=True)
class JavaHeader:
    fqcn: str
    source: str
    text: str


def git_output(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return result.stdout.strip()


def verify_java_checkout(java_repo: Path, allow_dirty: bool) -> None:
    actual = git_output(java_repo, "rev-parse", "HEAD")
    if actual != JAVA_COMMIT:
        raise ValueError(f"Java HEAD must be {JAVA_COMMIT}, found {actual}")
    dirty = git_output(java_repo, "status", "--short")
    if dirty and not allow_dirty:
        raise ValueError("Java worktree must be clean; use --allow-dirty for diagnostics only")


def inventory_rust_headers(repo_root: Path) -> list[RustHeader]:
    source_root = repo_root / "rocketmq-protocol" / "src"
    headers: list[RustHeader] = []
    for source in sorted(source_root.rglob("*.rs")):
        text = source.read_text(encoding="utf-8")
        relative = source.relative_to(source_root).with_suffix("")
        module = "::".join(relative.parts)
        for match in DERIVE_PATTERN.finditer(text):
            name = match.group("name")
            headers.append(
                RustHeader(
                    name=name,
                    source=(Path("rocketmq-protocol") / "src" / relative.with_suffix(".rs")).as_posix(),
                    type_id=f"rocketmq_protocol::{module}::{name}",
                )
            )
    if len(headers) != EXPECTED_DERIVE_COUNT:
        raise ValueError(
            f"expected {EXPECTED_DERIVE_COUNT} RequestHeaderCodecV2 derives, found {len(headers)}"
        )
    headers.extend(MANUAL_HEADERS)
    type_ids = [header.type_id for header in headers]
    if len(type_ids) != len(set(type_ids)):
        raise ValueError("duplicate stable Rust type ID detected")
    return headers


def inventory_java_headers(java_repo: Path) -> dict[str, JavaHeader]:
    source_root = java_repo / "remoting" / "src" / "main" / "java"
    headers: dict[str, JavaHeader] = {}
    for source in sorted(source_root.rglob("*.java")):
        text = source.read_text(encoding="utf-8")
        package_match = PACKAGE_PATTERN.search(text)
        if package_match is None:
            continue
        name = source.stem
        if name in headers:
            raise ValueError(f"duplicate Java header class name: {name}")
        headers[name] = JavaHeader(
            fqcn=f"{package_match.group(1)}.{name}",
            source=source.relative_to(java_repo).as_posix(),
            text=text,
        )
    return headers


def infer_direction(name: str) -> str:
    if name.endswith("ResponseHeader"):
        return "response"
    if name.endswith("RequestHeader"):
        return "request"
    return "shared"


def generate(repo_root: Path, java_repo: Path, allow_dirty: bool) -> dict[str, object]:
    verify_java_checkout(java_repo, allow_dirty)
    rust_headers = inventory_rust_headers(repo_root)
    java_headers = inventory_java_headers(java_repo)
    entries: list[dict[str, object]] = []
    unresolved: list[str] = []

    for rust in rust_headers:
        java_name = JAVA_NAME_OVERRIDES.get(rust.name, rust.name)
        java = java_headers.get(java_name)
        extension = RUST_ONLY_EXTENSIONS.get(rust.name)
        if java is None and extension is None:
            unresolved.append(rust.name)
            continue

        if java is None:
            entries.append(
                {
                    "rustTypeId": rust.type_id,
                    "rustType": rust.name,
                    "rustAliases": RUST_ALIASES.get(rust.name, []),
                    "rustSource": rust.source,
                    "javaClass": None,
                    "javaSource": None,
                    "direction": infer_direction(rust.name),
                    "requestCodes": extension["requestCodes"],
                    "exactName": False,
                    "javaFast": False,
                    "status": "rust-only-extension",
                    "reason": extension["reason"],
                }
            )
            continue

        request_codes = sorted(set(ACTION_PATTERN.findall(java.text)))
        entries.append(
            {
                "rustTypeId": rust.type_id,
                "rustType": rust.name,
                "rustAliases": RUST_ALIASES.get(rust.name, []),
                "rustSource": rust.source,
                "javaClass": java.fqcn,
                "javaSource": java.source,
                "direction": infer_direction(rust.name),
                "requestCodes": request_codes,
                "exactName": rust.name == java_name,
                "javaFast": bool(re.search(r"\bimplements\s+FastCodesHeader\b", java.text)),
                "status": "mapped",
                "reason": "explicit-name mapping" if rust.name != java_name else "exact-name mapping",
            }
        )

    if unresolved:
        raise ValueError(f"unmapped Rust headers: {', '.join(sorted(unresolved))}")

    entries.sort(key=lambda entry: str(entry["rustTypeId"]))
    mapped = sum(entry["status"] == "mapped" for entry in entries)
    extensions = sum(entry["status"] == "rust-only-extension" for entry in entries)
    return {
        "schemaVersion": 1,
        "rustHistoricalCommit": RUST_HISTORICAL_COMMIT,
        "javaCommit": JAVA_COMMIT,
        "generatedBy": "scripts/request-header-codec/generate_header_class_map.py",
        "historicalDeriveCount": HISTORICAL_DERIVE_COUNT,
        "currentDeriveCount": EXPECTED_DERIVE_COUNT,
        "manualHeaderCount": len(MANUAL_HEADERS),
        "entryCount": len(entries),
        "mappedCount": mapped,
        "rustOnlyExtensionCount": extensions,
        "entries": entries,
    }


def serialize(document: dict[str, object]) -> str:
    return json.dumps(document, ensure_ascii=False, indent=2, sort_keys=False) + "\n"


def parse_args() -> argparse.Namespace:
    script = Path(__file__).resolve()
    default_root = script.parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=default_root)
    parser.add_argument("--java-repo", type=Path, required=True)
    parser.add_argument(
        "--output",
        type=Path,
        default=script.parent / "header-class-map.json",
    )
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--allow-dirty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        document = generate(args.repo_root.resolve(), args.java_repo.resolve(), args.allow_dirty)
        rendered = serialize(document)
        if args.check:
            if not args.output.is_file():
                print(f"missing generated mapping: {args.output}", file=sys.stderr)
                return 1
            if args.output.read_text(encoding="utf-8") != rendered:
                print(f"generated mapping is stale: {args.output}", file=sys.stderr)
                return 1
            print(
                f"header mapping is current: {document['entryCount']} entries "
                f"({document['mappedCount']} Java mappings, "
                f"{document['rustOnlyExtensionCount']} Rust extensions)"
            )
            return 0
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8", newline="\n")
        print(f"wrote {args.output} with {document['entryCount']} entries")
        return 0
    except (OSError, subprocess.CalledProcessError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
