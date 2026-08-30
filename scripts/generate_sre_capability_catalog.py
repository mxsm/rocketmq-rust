#!/usr/bin/env python3
"""Generate the Phase 00 SRE capability catalog from the Admin TUI source."""

from __future__ import annotations

import argparse
import ast
import dataclasses
import hashlib
import json
import pathlib
import re
import sys


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
CATALOG_SOURCE = REPO_ROOT / "rocketmq-tools/rocketmq-admin/rocketmq-admin-tui/src/commands/catalog.rs"
DEFAULT_OUTPUT = REPO_ROOT / "rocketmq-ai/rocketmq-sre/config/capabilities/rocketmq-capability-catalog.v1.yaml"


@dataclasses.dataclass(frozen=True)
class Command:
    identifier: str
    domain: str
    title: str
    description: str
    legacy_risk: str


@dataclasses.dataclass(frozen=True)
class ComponentSurface:
    component: str
    source_path: str
    source_symbol: str
    exposure: str
    backlog: str


COMPONENT_SURFACES = (
    ComponentSurface(
        "NameServer",
        "rocketmq-namesrv/src/bootstrap.rs",
        "NameServerBootstrap",
        "metrics_observable_local",
        "add an authenticated bounded route diagnostics resource",
    ),
    ComponentSurface(
        "Broker",
        "rocketmq-broker/src/broker_bootstrap.rs",
        "BrokerBootstrap",
        "metrics_observable_local",
        "add an authenticated bounded broker lifecycle and health resource",
    ),
    ComponentSurface(
        "Controller",
        "rocketmq-controller/src/controller/open_raft_controller.rs",
        "OpenRaftController",
        "metrics_observable_local",
        "add an authenticated bounded quorum diagnostics resource",
    ),
    ComponentSurface(
        "Proxy",
        "rocketmq-proxy/src/observability.rs",
        "ProxyMetricsSnapshot",
        "in_process_only",
        "add a sanitized read-only evidence adapter and production query verification",
    ),
    ComponentSurface(
        "Client",
        "rocketmq-client/src/runtime.rs",
        "ClientRuntime",
        "implemented_local",
        "add a sanitized client diagnostics adapter without message bodies or credentials",
    ),
    ComponentSurface(
        "Store",
        "rocketmq-store/src/capability.rs",
        "StoreHealthSnapshot",
        "in_process_only",
        "expose bounded health recovery and background rebuild evidence",
    ),
    ComponentSurface(
        "RocksDB",
        "rocketmq-store/src/message_store/rocksdb_message_store.rs",
        "RocksDBMessageStore",
        "metrics_observable_local",
        "correlate database amplification and cache metrics with Store health evidence",
    ),
    ComponentSurface(
        "TieredStore",
        "rocketmq-tieredstore/src/store.rs",
        "TieredStore",
        "metrics_observable_local",
        "add bounded provider and dispatcher failure evidence",
    ),
    ComponentSurface(
        "Auth",
        "rocketmq-auth/src/runtime.rs",
        "AuthRuntime",
        "in_process_only",
        "expose only aggregate sanitized authorization outcomes",
    ),
    ComponentSurface(
        "Runtime",
        "rocketmq-runtime/src/diagnostics.rs",
        "RuntimeDiagnosticsViewV1",
        "mcp_system_resource_only",
        "add authenticated component-local diagnostics adapters",
    ),
    ComponentSurface(
        "Observability",
        "rocketmq-observability/src/status.rs",
        "ObservabilityStatusViewV1",
        "mcp_system_resource_only",
        "production-verify exporter state for every server component",
    ),
    ComponentSurface(
        "MCP",
        "rocketmq-ai/rocketmq-mcp/src/resources/capability.rs",
        "CapabilityManifest",
        "queryable",
        "production-verify audit and exporter recovery alerts",
    ),
    ComponentSurface(
        "Dashboard",
        "rocketmq-dashboard/rocketmq-dashboard-common/src/dashboard.rs",
        "DashboardBrokerOverviewRequest",
        "separate_operator_ui",
        "retain deep links only; do not share sessions or mutation APIs with AI SRE",
    ),
    ComponentSurface(
        "Kubernetes",
        "distribution/kubernetes/base/manifest.yaml",
        "kind: Deployment",
        "deployment_assets",
        "add production Helm packaging after Kind parity is complete",
    ),
)

EXPECTED_COMPONENT_SURFACES = {
    "NameServer",
    "Broker",
    "Controller",
    "Proxy",
    "Client",
    "Store",
    "RocksDB",
    "TieredStore",
    "Auth",
    "Runtime",
    "Observability",
    "MCP",
    "Dashboard",
    "Kubernetes",
}


def _balanced_call(text: str, open_index: int) -> str:
    depth = 0
    in_string = False
    escaped = False
    for index in range(open_index, len(text)):
        char = text[index]
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
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return text[open_index + 1 : index]
    raise ValueError(f"unterminated spec call at byte {open_index}")


def _top_level_arguments(call: str) -> list[str]:
    arguments: list[str] = []
    start = 0
    depths = {"(": 0, "[": 0, "{": 0}
    closing = {")": "(", "]": "[", "}": "{"}
    in_string = False
    escaped = False
    for index, char in enumerate(call):
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
        elif char in depths:
            depths[char] += 1
        elif char in closing:
            depths[closing[char]] -= 1
        elif char == "," and all(depth == 0 for depth in depths.values()):
            arguments.append(call[start:index].strip())
            start = index + 1
    arguments.append(call[start:].strip())
    return arguments


def _rust_string(value: str) -> str:
    parsed = ast.literal_eval(value)
    if not isinstance(parsed, str):
        raise ValueError(f"expected Rust string literal, got {value}")
    return parsed


def parse_commands(text: str) -> list[Command]:
    commands: list[Command] = []
    for match in re.finditer(r"\bspec\(", text):
        if text[max(0, match.start() - 3) : match.start()] == "fn ":
            continue
        open_index = match.end() - 1
        arguments = _top_level_arguments(_balanced_call(text, open_index))
        if len(arguments) < 5:
            continue
        category = re.fullmatch(r"CommandCategory::([A-Za-z0-9_]+)", arguments[1])
        risk = re.fullmatch(r"RiskLevel::([A-Za-z0-9_]+)", arguments[4])
        if category is None or risk is None:
            continue
        commands.append(
            Command(
                identifier=_rust_string(arguments[0]),
                domain=category.group(1),
                title=_rust_string(arguments[2]),
                description=_rust_string(arguments[3]),
                legacy_risk=risk.group(1).lower(),
            )
        )
    return commands


def sre_class(command: Command) -> str:
    if command.legacy_risk == "safe":
        plan_tokens = ("allocate", "preview", "export")
        return "Plan" if any(token in command.identifier for token in plan_tokens) else "Read"
    if command.legacy_risk == "dangerous":
        return "R3"
    r2_tokens = (
        "offset",
        "broker",
        "controller",
        "container",
        "ha.",
        "role",
        "restart",
        "shutdown",
        "resume",
        "suspend",
    )
    return "R2" if any(token in command.identifier for token in r2_tokens) else "R1"


def _yaml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def render(commands: list[Command], revision: str) -> str:
    domains = sorted({command.domain for command in commands})
    lines = [
        "# Generated by scripts/generate_sre_capability_catalog.py. Do not edit by hand.",
        'schema_version: "rocketmq-sre.capability-catalog.v1"',
        "source:",
        '  path: "rocketmq-tools/rocketmq-admin/rocketmq-admin-tui/src/commands/catalog.rs"',
        f"  revision: {_yaml_string(revision)}",
        f"  domains: {len(domains)}",
        f"  actions: {len(commands)}",
        f"  component_surfaces: {len(COMPONENT_SURFACES)}",
        "component_source_surfaces:",
    ]
    for surface in COMPONENT_SURFACES:
        lines.extend(
            [
                f"  - component: {_yaml_string(surface.component)}",
                f"    source_path: {_yaml_string(surface.source_path)}",
                f"    source_symbol: {_yaml_string(surface.source_symbol)}",
                f"    exposure: {_yaml_string(surface.exposure)}",
                f"    backlog: {_yaml_string(surface.backlog)}",
            ]
        )
    lines.append(
        "capabilities:",
    )
    for command in commands:
        lines.extend(
            [
                f"  - id: {_yaml_string(command.identifier)}",
                f"    domain: {_yaml_string(command.domain)}",
                f"    title: {_yaml_string(command.title)}",
                f"    description: {_yaml_string(command.description)}",
                f"    legacy_risk: {_yaml_string(command.legacy_risk)}",
                f"    sre_class: {_yaml_string(sre_class(command))}",
                '    exposure: "admin_rpc"',
                '    source_path: "rocketmq-tools/rocketmq-admin/rocketmq-admin-tui/src/commands/catalog.rs"',
                '    source_symbol: "command_catalog"',
                '    supported_versions: [">=1.0.0"]',
                "    maturity:",
                "      implemented_local: true",
                f"      queryable: {str(command.legacy_risk == 'safe').lower()}",
                "      observable: false",
                "      diagnosable: false",
                f"      plannable: {str(command.legacy_risk != 'dangerous').lower()}",
                "      executable: false",
                "    execution_supported: false",
            ]
        )
    return "\n".join(lines) + "\n"


def source_revision() -> str:
    """Return a commit-independent revision for the exact catalog source."""
    return f"sha256:{hashlib.sha256(CATALOG_SOURCE.read_bytes()).hexdigest()}"


def validate_component_surfaces() -> list[str]:
    errors: list[str] = []
    components = [surface.component for surface in COMPONENT_SURFACES]
    if len(set(components)) != len(components):
        errors.append("component source-surface names are not unique")
    actual = set(components)
    if actual != EXPECTED_COMPONENT_SURFACES:
        errors.append(
            "component source surfaces differ from the required set: "
            f"missing={sorted(EXPECTED_COMPONENT_SURFACES - actual)}, "
            f"unknown={sorted(actual - EXPECTED_COMPONENT_SURFACES)}"
        )
    for surface in COMPONENT_SURFACES:
        source = REPO_ROOT / surface.source_path
        if not source.is_file():
            errors.append(f"{surface.component} source path does not exist: {surface.source_path}")
            continue
        if surface.source_symbol not in source.read_text(encoding="utf-8"):
            errors.append(
                f"{surface.component} source symbol `{surface.source_symbol}` "
                f"was not found in {surface.source_path}"
            )
        if not surface.exposure or not surface.backlog:
            errors.append(f"{surface.component} must record current exposure and backlog")
        if surface.component != "MCP" and surface.exposure in {
            "queryable",
            "metrics_queryable",
        }:
            errors.append(
                f"{surface.component} cannot claim remote queryability in Phase 00"
            )
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=pathlib.Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    commands = parse_commands(CATALOG_SOURCE.read_text(encoding="utf-8"))
    domains = {command.domain for command in commands}
    if len(commands) != 102 or len(domains) != 18:
        print(
            f"expected 102 commands across 18 domains, found {len(commands)} across {len(domains)}",
            file=sys.stderr,
        )
        return 1
    if len({command.identifier for command in commands}) != len(commands):
        print("capability IDs are not unique", file=sys.stderr)
        return 1
    surface_errors = validate_component_surfaces()
    if surface_errors:
        for error in surface_errors:
            print(error, file=sys.stderr)
        return 1

    rendered = render(commands, source_revision())
    output = args.output.resolve()
    if args.check:
        if not output.exists() or output.read_text(encoding="utf-8") != rendered:
            print(f"{output} is stale; regenerate it", file=sys.stderr)
            return 1
        return 0

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered, encoding="utf-8", newline="\n")
    print(f"wrote {len(commands)} capabilities across {len(domains)} domains to {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
