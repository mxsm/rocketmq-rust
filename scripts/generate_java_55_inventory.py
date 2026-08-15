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
from pathlib import PurePosixPath
import re
import sys
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "scripts" / "fixtures" / "java-5.5-core-inventory.json"
DEFAULT_ADMIN_MATRIX_OUTPUT = ROOT / "scripts" / "admin-operation-matrix.json"
DEFAULT_ADMIN_MATRIX_DOC_OUTPUT = ROOT / "rocketmq-doc" / "en" / "admin" / "java-55-operation-map.md"
ADMIN_CLI_ROOT = "rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/src/commands"
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
ADMIN_PLACEHOLDER_COMMANDS = frozenset(
    {
        "deleteSubGroup",
        "deleteTopic",
        "queryMsgByKey",
        "queryMsgByUniqueKey",
        "updateTopic",
    }
)
ADMIN_MODULE_ALIASES = {"metadata": "export"}
ADMIN_CLI_DOMAIN_ALIASES = {"metadata": "export", "namesrv": "nameserver"}
ADMIN_HANDLER_OWNERS = {
    "auth": ("broker",),
    "broker": ("broker",),
    "cluster": ("nameserver", "broker"),
    "connection": ("broker",),
    "consumer": ("broker",),
    "controller": ("controller",),
    "export": ("nameserver", "broker", "local-filesystem"),
    "ha": ("broker", "controller"),
    "lite": ("nameserver", "broker"),
    "message": ("broker",),
    "namesrv": ("nameserver",),
    "offset": ("broker",),
    "producer": ("nameserver", "broker"),
    "queue": ("broker",),
    "stats": ("nameserver", "broker"),
    "topic": ("nameserver", "broker"),
}
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


def _rust_source(rust_root: Path, relative: str) -> str:
    path = rust_root / relative
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        raise InventoryError(f"cannot read required Rust source {relative}: {error}") from error


def _admin_cli_index(rust_root: Path) -> tuple[dict[tuple[str, str], str], dict[str, str]]:
    command_root = rust_root / ADMIN_CLI_ROOT
    if not command_root.is_dir():
        raise InventoryError(f"required Rust Admin CLI source directory is missing: {ADMIN_CLI_ROOT}")

    command_types: dict[tuple[str, str], str] = {}
    command_pattern = re.compile(
        r'#\[command\((?:(?!#\[command).)*?name\s*=\s*"([^"]+)"'
        r'(?:(?!#\[command).)*?\)\]\s*[A-Za-z0-9_]+\(([A-Za-z0-9_]+)\)',
        re.DOTALL,
    )
    for module_path in sorted(command_root.glob("*.rs")):
        relative = module_path.relative_to(rust_root).as_posix()
        for command, command_type in command_pattern.findall(_rust_source(rust_root, relative)):
            key = (module_path.stem, command)
            if key in command_types:
                raise InventoryError(f"duplicate Rust Admin CLI command mapping: {key}")
            command_types[key] = command_type

    struct_sources: dict[str, str] = {}
    for path in sorted(command_root.rglob("*.rs")):
        relative = path.relative_to(rust_root).as_posix()
        for command_type in re.findall(r"pub\s+struct\s+([A-Za-z0-9_]+)", _rust_source(rust_root, relative)):
            previous = struct_sources.setdefault(command_type, relative)
            if previous != relative:
                raise InventoryError(f"duplicate Rust Admin CLI command type: {command_type}")
    return command_types, struct_sources


def _admin_side_effect(command: str) -> dict[str, str]:
    if command.startswith(("export", "rocksDBConfigToJson")):
        return {
            "class": "local-artifact-write",
            "description": "Reads administrative state and writes only the explicitly selected local export artifact.",
        }
    if command.startswith(("send", "checkMsgSendRT", "clusterRT")):
        return {
            "class": "message-io",
            "description": "Produces test or user-selected messages while preserving the command-specific remote result.",
        }
    if command.startswith(
        (
            "add",
            "clean",
            "clone",
            "create",
            "delete",
            "elect",
            "remove",
            "remapping",
            "reset",
            "set",
            "skip",
            "start",
            "switch",
            "trigger",
            "update",
            "wipe",
        )
    ):
        return {
            "class": "remote-state-mutation",
            "description": "Mutates only the command-selected RocketMQ resource and reports partial remote failures.",
        }
    return {
        "class": "read-only-query",
        "description": "Reads administrative state without intentionally mutating remote RocketMQ resources.",
    }


def _typed_names(source: str, suffix: str, excluded: frozenset[str]) -> list[str]:
    return sorted(
        {
            value
            for value in re.findall(rf"\b([A-Z][A-Za-z0-9_]*{suffix})\b", source)
            if value not in excluded
        }
    )


def generate_admin_operation_matrix(
    inventory: dict[str, Any],
    *,
    java_root: Path,
    rust_root: Path = ROOT,
) -> dict[str, Any]:
    command_types, struct_sources = _admin_cli_index(rust_root)
    operations: list[dict[str, Any]] = []

    for java_operation in inventory["admin_operations"]:
        symbol = java_operation["symbol"]
        command = java_operation["command"]
        java_source = java_operation["source"]
        java_command_source = _source(java_root, java_source)
        java_request_codes = sorted(set(re.findall(r"RequestCode\.([A-Z0-9_]+)", java_command_source)))
        source_domain = PurePosixPath(java_source).parent.name
        module = ADMIN_MODULE_ALIASES.get(source_domain, source_domain)
        cli_domain = ADMIN_CLI_DOMAIN_ALIASES.get(source_domain, source_domain)
        excluded = symbol in CONTAINER_COMMANDS

        base: dict[str, Any] = {
            "operation_id": f"mqadmin.{cli_domain}.{command}",
            "java_symbol": symbol,
            "java_command": command,
            "java_method": f"{symbol}.execute(CommandLine, RPCHook)",
            "java_request_codes": java_request_codes,
            "java_request_code_resolution": "direct-command-source"
            if java_request_codes
            else "delegated-to-java-admin-api",
            "java_source": java_source,
            "classification": "excluded" if excluded else "active",
            "exclusion_reason": "BrokerContainer" if excluded else None,
        }
        if excluded:
            base.update(
                {
                    "rust_admin_core_methods": [],
                    "cli_command_id": None,
                    "tui_command_id": None,
                    "status": "excluded",
                    "status_reason": "BrokerContainer is outside the RocketMQ-rust 1.0 core release scope.",
                }
            )
            operations.append(base)
            continue

        command_type = command_types.get((module, command))
        if command_type is None:
            raise InventoryError(f"Rust Admin CLI command is missing for Java operation {symbol}/{command}")
        rust_cli_source = struct_sources.get(command_type)
        if rust_cli_source is None:
            raise InventoryError(f"Rust Admin CLI type source is missing: {command_type}")
        rust_source = _rust_source(rust_root, rust_cli_source)
        core_methods = sorted(
            set(re.findall(r"([A-Za-z][A-Za-z0-9_]*Service::[A-Za-z0-9_]+)", rust_source))
        )
        if not core_methods:
            raise InventoryError(f"Rust Admin Core method is missing for {cli_domain}.{command}")

        request_types = _typed_names(rust_source, "Request", frozenset()) or [f"{command_type} arguments"]
        response_types = _typed_names(
            rust_source,
            "Result",
            frozenset({"Result", "RocketMQResult"}),
        ) or ["RocketMQResult<()> status"]
        placeholder = command in ADMIN_PLACEHOLDER_COMMANDS
        base.update(
            {
                "rust_admin_core_methods": core_methods,
                "cli_command_id": f"{cli_domain}.{command}",
                "tui_command_id": None,
                "rust_cli_source": rust_cli_source,
                "handler_owners": list(ADMIN_HANDLER_OWNERS[module]),
                "authorization": {
                    "context": "AdminCredentials",
                    "enforcement": "Remoting ACL or target-service authorization",
                    "permission": "command-specific resource permission",
                },
                "typed_request": request_types,
                "typed_response": response_types,
                "expected_side_effects": _admin_side_effect(command),
                "error_mapping": {
                    "success": "CLI exit 0",
                    "runtime_or_authorization_error": "typed RocketMQ error mapped to non-zero CLI exit",
                    "usage_or_unknown_command": "Clap usage exit 2",
                },
                "test_id": f"G05-{symbol}",
                "test_command": "cargo test -p rocketmq-admin-cli --test java_parity_inventory",
                "status": "placeholder" if placeholder else "alternative-equivalent",
                "status_reason": "Known high-risk Admin behavior requires the dedicated closure task."
                if placeholder
                else "Typed Rust Admin Core and CLI route provide the Java user capability with Rust-native structure.",
            }
        )
        operations.append(base)

    status_counts: dict[str, int] = {}
    for operation in operations:
        status = operation["status"]
        status_counts[status] = status_counts.get(status, 0) + 1
    return {
        "schema_version": 1,
        "java_version": "5.5.0",
        "scope": "core-release",
        "counts": {"raw": 96, "excluded": 2, "active": 94},
        "status_counts": dict(sorted(status_counts.items())),
        "status_semantics": {
            "alternative-equivalent": "The Rust-native API and CLI provide the same user capability without Java implementation-shape parity.",
            "placeholder": "The command is reachable but a documented user-visible behavior remains incomplete.",
            "missing": "No usable Rust operation currently provides the Java user capability.",
            "excluded": "The raw Java command is retained only as an approved product-scope exclusion.",
        },
        "operations": operations,
    }


def _markdown_cell(value: object) -> str:
    if value is None:
        return "—"
    if isinstance(value, list):
        rendered = "<br>".join(f"`{item}`" for item in value)
    else:
        rendered = str(value)
    return rendered.replace("|", "\\|").replace("\n", " ")


def render_admin_operation_markdown(matrix: dict[str, Any]) -> str:
    counts = matrix["counts"]
    status_counts = matrix["status_counts"]
    lines = [
        "# Java 5.5 Admin operation map",
        "",
        "> Generated by `scripts/generate_java_55_inventory.py`; edit the generator or source mappings, not this file.",
        "",
        "This map records the raw Java `mqadmin` denominator and the RocketMQ-rust 1.0 core surface. "
        "It is an inventory, not a claim that placeholder operations are release-ready.",
        "",
        f"- Raw operations: **{counts['raw']}**",
        f"- BrokerContainer exclusions: **{counts['excluded']}**",
        f"- Core active operations: **{counts['active']}**",
        f"- Known placeholders: **{status_counts.get('placeholder', 0)}**",
        "",
        "## Status model",
        "",
        "- `alternative-equivalent`: a typed Rust Admin Core method and CLI route provide the same user capability.",
        "- `placeholder`: the route exists, but a documented user-visible behavior remains incomplete.",
        "- `missing`: no usable Rust operation currently provides the Java capability.",
        "- `excluded`: the raw Java command is retained only as an approved scope exclusion.",
        "",
        "The default guard validates inventory structure and mappings. "
        "`python scripts/admin_operation_guard.py --require-complete` is the release-completion gate and rejects "
        "both `placeholder` and `missing`.",
        "",
        "## Operation matrix",
        "",
        "| Java command | Java request code(s) | Rust CLI | Rust Admin Core method(s) | Handler owner(s) | Status |",
        "|---|---|---|---|---|---|",
    ]
    for operation in matrix["operations"]:
        lines.append(
            "| "
            + " | ".join(
                (
                    f"`{operation['java_symbol']}` / `{operation['java_command']}`",
                    _markdown_cell(operation["java_request_codes"])
                    if operation["java_request_codes"]
                    else "delegated to Java Admin API",
                    _markdown_cell(operation.get("cli_command_id")),
                    _markdown_cell(operation.get("rust_admin_core_methods", [])),
                    _markdown_cell(operation.get("handler_owners", [])),
                    f"`{operation['status']}`",
                )
            )
            + " |"
        )

    lines.extend(["", "## Known incomplete operations", ""])
    placeholders = [operation for operation in matrix["operations"] if operation["status"] == "placeholder"]
    for operation in placeholders:
        lines.append(
            f"- `{operation['cli_command_id']}` — {operation['status_reason']} "
            f"Tracked by the Admin high-risk behavior closure work."
        )

    lines.extend(["", "## Approved exclusions", ""])
    for operation in matrix["operations"]:
        if operation["classification"] == "excluded":
            lines.append(
                f"- `{operation['java_symbol']}` / `{operation['java_command']}` — "
                "BrokerContainer is not part of the RocketMQ-rust 1.0 core release."
            )
    lines.extend(
        [
            "",
            "OpenMessaging does not occur in the Java 5.5 `MQAdminStartup` raw inventory, so it contributes zero "
            "operations and does not reduce the active denominator.",
            "",
        ]
    )
    return "\n".join(lines)


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
    parser.add_argument(
        "--admin-matrix-output",
        type=Path,
        help="Also generate or check the Java 5.5 to Rust Admin operation matrix.",
    )
    parser.add_argument(
        "--admin-matrix-doc-output",
        type=Path,
        help="Also generate or check the Markdown view of the Admin operation matrix.",
    )
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--check", action="store_true")
    action.add_argument("--write", action="store_true")
    args = parser.parse_args()
    if args.admin_matrix_doc_output is not None and args.admin_matrix_output is None:
        parser.error("--admin-matrix-doc-output requires --admin-matrix-output")
    try:
        inventory = generate_inventory(args.java_root)
        admin_matrix = (
            generate_admin_operation_matrix(inventory, java_root=args.java_root.resolve())
            if args.admin_matrix_output is not None
            else None
        )
    except InventoryError as error:
        print(f"JAVA_55_INVENTORY_INPUT_FAILED detail={error}", file=sys.stderr)
        return 1
    rendered = _render(inventory)
    rendered_admin_matrix = _render(admin_matrix) if admin_matrix is not None else None
    rendered_admin_doc = render_admin_operation_markdown(admin_matrix) if admin_matrix is not None else None
    if args.check:
        try:
            existing = args.output.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as error:
            print(f"JAVA_55_INVENTORY_CHECK_FAILED detail={error}", file=sys.stderr)
            return 1
        if existing != rendered:
            print("JAVA_55_INVENTORY_CHECK_FAILED detail=generated inventory differs from fixture", file=sys.stderr)
            return 1
        if args.admin_matrix_output is not None and rendered_admin_matrix is not None:
            try:
                existing_admin_matrix = args.admin_matrix_output.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError) as error:
                print(f"JAVA_55_INVENTORY_CHECK_FAILED detail={error}", file=sys.stderr)
                return 1
            if existing_admin_matrix != rendered_admin_matrix:
                print("JAVA_55_INVENTORY_CHECK_FAILED detail=generated Admin matrix differs from fixture", file=sys.stderr)
                return 1
        if args.admin_matrix_doc_output is not None and rendered_admin_doc is not None:
            try:
                existing_admin_doc = args.admin_matrix_doc_output.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError) as error:
                print(f"JAVA_55_INVENTORY_CHECK_FAILED detail={error}", file=sys.stderr)
                return 1
            if existing_admin_doc != rendered_admin_doc:
                print("JAVA_55_INVENTORY_CHECK_FAILED detail=generated Admin operation map differs", file=sys.stderr)
                return 1
    elif args.write:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("w", encoding="utf-8", newline="\n") as output:
            output.write(rendered)
        if args.admin_matrix_output is not None and rendered_admin_matrix is not None:
            args.admin_matrix_output.parent.mkdir(parents=True, exist_ok=True)
            with args.admin_matrix_output.open("w", encoding="utf-8", newline="\n") as output:
                output.write(rendered_admin_matrix)
        if args.admin_matrix_doc_output is not None and rendered_admin_doc is not None:
            args.admin_matrix_doc_output.parent.mkdir(parents=True, exist_ok=True)
            with args.admin_matrix_doc_output.open("w", encoding="utf-8", newline="\n") as output:
                output.write(rendered_admin_doc)
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
