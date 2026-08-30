#!/usr/bin/env python3
"""Fail closed when RocketMQ read adapters bypass the connector ReadGateway."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONNECTOR = ROOT / "crates" / "rocketmq-sre-connector"
CONTROL_PLANE = ROOT / "crates" / "rocketmq-sre-control-plane"


def source_text(root: Path) -> str:
    return "\n".join(path.read_text(encoding="utf-8") for path in sorted((root / "src").rglob("*.rs")))


def connector_adapter_bypasses() -> list[str]:
    allowed = {
        Path("read_gateway/admin.rs"),
        Path("read_gateway/mcp.rs"),
        Path("sources.rs"),
        Path("sources/admin_query.rs"),
        Path("sources/mcp.rs"),
    }
    bypasses: list[str] = []
    source_root = CONNECTOR / "src"
    forbidden = (
        "McpSource",
        "AdminQuerySource",
        ".query_producer_connections(",
        ".query_consumer_connections(",
    )
    for path in sorted(source_root.rglob("*.rs")):
        relative = path.relative_to(source_root)
        if relative in allowed:
            continue
        text = path.read_text(encoding="utf-8")
        for marker in forbidden:
            if marker in text:
                bypasses.append(f"connector-adapter-bypass:{relative.as_posix()}:{marker}")
    return bypasses


def findings() -> list[str]:
    result: list[str] = []
    control_manifest = (CONTROL_PLANE / "Cargo.toml").read_text(encoding="utf-8")
    for forbidden in ("rocketmq-admin-core", "rocketmq-mcp", "rocketmq-sre-connector"):
        if forbidden in control_manifest:
            result.append(f"control-plane-forbidden-dependency:{forbidden}")

    connector_manifest = (CONNECTOR / "Cargo.toml").read_text(encoding="utf-8")
    if 'features = ["read-client-adapter"]' not in connector_manifest:
        result.append("connector-admin-read-feature-missing")
    for forbidden in ("mutation-client-adapter", "dangerous-tools", "admin-full"):
        if forbidden in connector_manifest:
            result.append(f"connector-mutation-feature:{forbidden}")

    sources = (CONNECTOR / "src" / "sources.rs").read_text(encoding="utf-8")
    required = (
        "read_gateway: ConnectorReadGateway",
        "ReadContext",
        "read_gateway.mcp_query",
        "read_gateway.admin_query",
    )
    for marker in required:
        if marker not in sources:
            result.append(f"source-manager-gateway-contract-missing:{marker}")
    for forbidden in ("mcp: McpSource", "admin: AdminQuerySource", "self.mcp.query", "self.admin.query"):
        if forbidden in sources:
            result.append(f"source-manager-direct-adapter:{forbidden}")

    connector_sources = source_text(CONNECTOR)
    if "mod read_gateway;" not in connector_sources:
        result.append("read-gateway-module-missing")
    result.extend(connector_adapter_bypasses())
    return result


def main() -> int:
    violations = findings()
    if violations:
        for violation in violations:
            print(f"READ_GATEWAY_BOUNDARY_FINDING {violation}")
        print(f"READ_GATEWAY_BOUNDARY_FAILED findings={len(violations)}")
        return 1
    print("READ_GATEWAY_BOUNDARY_OK direct_adapter_calls=0 mutation_features=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
