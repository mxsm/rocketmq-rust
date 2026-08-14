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

"""Generate the semantic Java 5.5 compatibility inventory used by release gates."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "scripts" / "fixtures" / "java-5.5-core-inventory.json"
REQUEST_CODE = "remoting/src/main/java/org/apache/rocketmq/remoting/protocol/RequestCode.java"
RESPONSE_CODES = (
    "remoting/src/main/java/org/apache/rocketmq/remoting/protocol/RemotingSysResponseCode.java",
    "remoting/src/main/java/org/apache/rocketmq/remoting/protocol/ResponseCode.java",
)
HEADER_ROOT = "remoting/src/main/java/org/apache/rocketmq/remoting/protocol/header"
BODY_ROOT = "remoting/src/main/java/org/apache/rocketmq/remoting/protocol/body"
PROXY_SERVER = "proxy/src/main/java/org/apache/rocketmq/proxy/remoting/RemotingProtocolServer.java"
ADMIN_STARTUP = "tools/src/main/java/org/apache/rocketmq/tools/command/MQAdminStartup.java"
CONTROLLER_PAYLOADS = (
    "controller/src/main/java/org/apache/rocketmq/controller/impl/task/BrokerCloseChannelRequest.java",
    "controller/src/main/java/org/apache/rocketmq/controller/impl/task/CheckNotActiveBrokerRequest.java",
    "controller/src/main/java/org/apache/rocketmq/controller/impl/task/GetBrokerLiveInfoRequest.java",
    "controller/src/main/java/org/apache/rocketmq/controller/impl/task/GetSyncStateDataRequest.java",
    "controller/src/main/java/org/apache/rocketmq/controller/impl/task/RaftBrokerHeartBeatEventRequest.java",
)
CONFIG_SOURCES = (
    "common/src/main/java/org/apache/rocketmq/common/BrokerConfig.java",
    "common/src/main/java/org/apache/rocketmq/common/ControllerConfig.java",
    "common/src/main/java/org/apache/rocketmq/common/namesrv/NamesrvConfig.java",
    "store/src/main/java/org/apache/rocketmq/store/config/MessageStoreConfig.java",
    "tieredstore/src/main/java/org/apache/rocketmq/tieredstore/MessageStoreConfig.java",
    "proxy/src/main/java/org/apache/rocketmq/proxy/config/ProxyConfig.java",
    "remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyServerConfig.java",
    "remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyClientConfig.java",
    "client/src/main/java/org/apache/rocketmq/client/ClientConfig.java",
)
REQUIRED_REQUEST_CODES = frozenset(
    {
        "DELETE_TOPIC_IN_BROKER_LIST",
        "DELETE_SUBSCRIPTION_GROUP_LIST",
        "UPDATE_AND_CREATE_SUBSCRIPTIONGROUP",
    }
)
REQUIRED_PROXY_GAPS = frozenset(
    {
        "CONSUMER_SEND_MSG_BACK",
        "END_TRANSACTION",
        "RECALL_MESSAGE",
        "POP_MESSAGE",
        "ACK_MESSAGE",
        "CHANGE_MESSAGE_INVISIBLETIME",
        "GET_CONSUMER_CONNECTION_LIST",
    }
)
CONTAINER_COMMANDS = frozenset({"AddBrokerSubCommand", "RemoveBrokerSubCommand"})
CONSTANT_PATTERN = re.compile(r"public\s+static\s+final\s+(int|short)\s+([A-Z0-9_]+)\s*=\s*(-?\d+)\s*;")
TYPE_PATTERN = re.compile(r"\b(?:class|interface|enum|record)\s+([A-Za-z_$][A-Za-z0-9_$]*)")
FIELD_PATTERN = re.compile(
    r"^\s*(?:private|protected|public)\s+(?!static\b)(?:final\s+)?[A-Za-z0-9_$<>, ?.\[\]]+\s+"
    r"([A-Za-z_$][A-Za-z0-9_$]*)\s*(?:=|;)",
    re.MULTILINE,
)


class InventoryError(ValueError):
    """Raised when the Java source tree cannot produce a valid inventory."""


def _source(java_root: Path, relative: str) -> str:
    path = java_root / relative
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        raise InventoryError(f"cannot read required Java source {relative}: {error}") from error


def _symbol(source: str, relative: str) -> str:
    match = TYPE_PATTERN.search(source)
    if match is None:
        raise InventoryError(f"cannot identify Java type in {relative}")
    return match.group(1)


def _fields(source: str) -> list[str]:
    return sorted(set(FIELD_PATTERN.findall(source)))


def _constants(java_root: Path, sources: Iterable[str], purpose: str) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for relative in sources:
        for primitive, symbol, value in CONSTANT_PATTERN.findall(_source(java_root, relative)):
            numeric = int(value)
            classification = "raw"
            if purpose == "request" and 1014 <= numeric <= 1018:
                classification = "controller-internal-not-applicable"
            elif purpose == "request" and symbol in REQUIRED_REQUEST_CODES:
                classification = "active"
            entries.append(
                {
                    "symbol": symbol,
                    "value": numeric,
                    "primitive": primitive,
                    "purpose": f"Java {purpose} code",
                    "source": relative,
                    "classification": classification,
                    "required_active": purpose == "request" and symbol in REQUIRED_REQUEST_CODES,
                }
            )
    return sorted(entries, key=lambda item: (item["value"], item["symbol"]))


def _types(java_root: Path, relative_root: str, purpose: str) -> list[dict[str, Any]]:
    directory = java_root / relative_root
    if not directory.is_dir():
        raise InventoryError(f"required Java source directory is missing: {relative_root}")
    entries: list[dict[str, Any]] = []
    for path in sorted(directory.rglob("*.java")):
        relative = path.relative_to(java_root).as_posix()
        source = _source(java_root, relative)
        entries.append(
            {
                "symbol": _symbol(source, relative),
                "purpose": purpose,
                "source": relative,
                "fields": _fields(source),
                "classification": "raw",
            }
        )
    return entries


def _controller_payloads(java_root: Path) -> list[dict[str, Any]]:
    entries = []
    for relative in CONTROLLER_PAYLOADS:
        source = _source(java_root, relative)
        entries.append(
            {
                "symbol": _symbol(source, relative),
                "purpose": "Java JRaft Controller internal payload",
                "source": relative,
                "fields": _fields(source),
                "classification": "controller-internal-not-applicable",
            }
        )
    return sorted(entries, key=lambda item: item["symbol"])


def _proxy_routes(java_root: Path) -> list[dict[str, Any]]:
    source = _source(java_root, PROXY_SERVER)
    pattern = re.compile(
        r"registerProcessor\(RequestCode\.([A-Z0-9_]+),\s*([A-Za-z0-9_.]+),\s*(?:this\.)?([A-Za-z0-9_]+)\)"
    )
    entries = [
        {
            "request_code": code,
            "handler": handler,
            "executor": executor,
            "purpose": "Java Proxy remoting route",
            "source": PROXY_SERVER,
            "classification": "active",
            "required_gap": code in REQUIRED_PROXY_GAPS,
        }
        for code, handler, executor in pattern.findall(source)
    ]
    return sorted(entries, key=lambda item: item["request_code"])


def _admin_operations(java_root: Path) -> list[dict[str, Any]]:
    source = _source(java_root, ADMIN_STARTUP)
    symbols = re.findall(r"initCommand\(new\s+([A-Za-z0-9_]+)\(\)\);", source)
    command_root = java_root / "tools/src/main/java/org/apache/rocketmq/tools/command"
    by_name = {path.name: path for path in command_root.rglob("*.java")}
    entries = []
    for symbol in symbols:
        path = by_name.get(f"{symbol}.java")
        if path is None:
            raise InventoryError(f"cannot locate Admin command source for {symbol}")
        relative = path.relative_to(java_root).as_posix()
        command_source = _source(java_root, relative)
        command_match = re.search(
            r"String\s+commandName\s*\(\s*\)\s*\{.*?return\s+\"([^\"]+)\"\s*;",
            command_source,
            re.DOTALL,
        )
        entries.append(
            {
                "symbol": symbol,
                "command": command_match.group(1) if command_match else None,
                "purpose": "Java mqadmin operation",
                "source": relative,
                "classification": "excluded-broker-container" if symbol in CONTAINER_COMMANDS else "active",
            }
        )
    return entries


def _config_keys(java_root: Path) -> list[dict[str, Any]]:
    entries = []
    for relative in CONFIG_SOURCES:
        source = _source(java_root, relative)
        owner = _symbol(source, relative)
        for key in _fields(source):
            entries.append(
                {
                    "symbol": f"{owner}.{key}",
                    "key": key,
                    "purpose": "Java configuration property",
                    "source": relative,
                    "classification": "active",
                }
            )
    return sorted(entries, key=lambda item: (item["source"], item["key"]))


def generate_inventory(java_root: Path) -> dict[str, Any]:
    java_root = java_root.resolve()
    inventory = {
        "schema_version": 1,
        "java_version": "5.5.0",
        "inventory_semantics": {
            "raw": "A symbol exists in the Java 5.5 source inventory; this does not claim behavioral coverage.",
            "active": "A user-visible Java route, operation, field, or explicitly required protocol item.",
            "excluded": "An approved product-scope exclusion retained for raw recognition only.",
        },
        "request_codes": _constants(java_root, (REQUEST_CODE,), "request"),
        "response_codes": _constants(java_root, RESPONSE_CODES, "response"),
        "headers": _types(java_root, HEADER_ROOT, "Java remoting header"),
        "bodies": _types(java_root, BODY_ROOT, "Java remoting body"),
        "controller_internal_payloads": _controller_payloads(java_root),
        "proxy_routes": _proxy_routes(java_root),
        "admin_operations": _admin_operations(java_root),
        "config_keys": _config_keys(java_root),
    }
    findings = validate_inventory(inventory)
    if findings:
        raise InventoryError("; ".join(findings))
    return inventory


def validate_inventory(inventory: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    expected_counts = {
        "request_codes": 171,
        "response_codes": 68,
        "headers": 145,
        "bodies": 64,
        "controller_internal_payloads": 5,
        "proxy_routes": 24,
        "admin_operations": 96,
    }
    if inventory.get("schema_version") != 1 or inventory.get("java_version") != "5.5.0":
        findings.append("schema/version mismatch")
    for section, expected in expected_counts.items():
        entries = inventory.get(section)
        if not isinstance(entries, list) or len(entries) != expected:
            findings.append(f"{section} count must be {expected}")
    requests = {item.get("symbol"): item for item in inventory.get("request_codes", [])}
    for symbol in REQUIRED_REQUEST_CODES:
        if not requests.get(symbol, {}).get("required_active"):
            findings.append(f"required request code missing: {symbol}")
    internal_values = {
        item.get("value")
        for item in inventory.get("request_codes", [])
        if item.get("classification") == "controller-internal-not-applicable"
    }
    if internal_values != set(range(1014, 1019)):
        findings.append("Controller-internal request-code set drifted")
    route_codes = {item.get("request_code") for item in inventory.get("proxy_routes", [])}
    if not REQUIRED_PROXY_GAPS.issubset(route_codes):
        findings.append("required Proxy route set is incomplete")
    admin_exclusions = {
        item.get("symbol")
        for item in inventory.get("admin_operations", [])
        if item.get("classification") != "active"
    }
    if admin_exclusions != CONTAINER_COMMANDS:
        findings.append("Admin exclusion set drifted")
    query_headers = [item for item in inventory.get("headers", []) if item.get("symbol") == "QueryMessageRequestHeader"]
    if len(query_headers) != 1 or not {"indexType", "lastKey"}.issubset(query_headers[0].get("fields", [])):
        findings.append("QUERY_MESSAGE indexType/lastKey fields are missing")
    if not isinstance(inventory.get("config_keys"), list) or not inventory["config_keys"]:
        findings.append("configuration key inventory is empty")
    return findings


def _render(inventory: dict[str, Any]) -> str:
    return json.dumps(inventory, indent=2, ensure_ascii=False) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--java-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--check", action="store_true")
    action.add_argument("--write", action="store_true")
    args = parser.parse_args()
    try:
        inventory = generate_inventory(args.java_root)
    except InventoryError as error:
        print(f"JAVA_55_INVENTORY_INPUT_FAILED detail={error}", file=sys.stderr)
        return 1
    rendered = _render(inventory)
    if args.check:
        try:
            existing = args.output.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as error:
            print(f"JAVA_55_INVENTORY_CHECK_FAILED detail={error}", file=sys.stderr)
            return 1
        if existing != rendered:
            print("JAVA_55_INVENTORY_CHECK_FAILED detail=generated inventory differs from fixture", file=sys.stderr)
            return 1
    elif args.write:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("w", encoding="utf-8", newline="\n") as output:
            output.write(rendered)
    else:
        print(rendered, end="")
    print(
        "JAVA_55_INVENTORY_OK "
        f"requests={len(inventory['request_codes'])} responses={len(inventory['response_codes'])} "
        f"headers={len(inventory['headers'])} bodies={len(inventory['bodies'])} "
        f"proxy_routes={len(inventory['proxy_routes'])} admin_operations={len(inventory['admin_operations'])}",
        file=sys.stderr if not (args.check or args.write) else sys.stdout,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
