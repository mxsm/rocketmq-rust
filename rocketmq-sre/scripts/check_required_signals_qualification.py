#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Validate the disposable Required Signals live-qualification contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SRE_ROOT = REPOSITORY_ROOT / "rocketmq-sre"
DEFAULT_MANIFEST = SRE_ROOT / "config" / "qualification" / "required-signals.v1.json"
EXPECTED_SCHEMA = "rocketmq-sre.required-signals-qualification.v1"
EXPECTED_COMPONENTS = (
    ("broker", "broker", "broker.availability", "rocketmq_broker_up_ratio"),
    ("nameserver", "name_server", "nameserver.active_brokers", "rocketmq_namesrv_active_brokers"),
    (
        "controller",
        "controller",
        "controller.quorum_health",
        "rocketmq_controller_quorum_health_ratio",
    ),
    ("proxy", "proxy", "proxy.availability", "rocketmq_proxy_up_ratio"),
    ("mcp", "mcp", "mcp.operation_requests", "rocketmq_mcp_requests_total"),
)
MANIFEST_FILES = {
    "broker": "broker.yaml",
    "nameserver": "nameserver.yaml",
    "controller": "controller.yaml",
    "proxy": "proxy.yaml",
    "mcp": "mcp.yaml",
}


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as source:
        value = json.load(source)
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def validate_manifest(manifest: dict[str, Any], sre_root: Path = SRE_ROOT) -> list[str]:
    findings: list[str] = []
    expected_header = {
        "schema_version": EXPECTED_SCHEMA,
        "environment": "disposable_compose_otlp_backends",
        "operating_mode": "read_only",
        "production_certified": False,
    }
    for field, expected in expected_header.items():
        if manifest.get(field) != expected:
            findings.append(f"{field} must remain {expected!r}")
    if manifest.get("limits") != {
        "query_window_minutes": 10,
        "retry_seconds": 120,
        "maximum_components": 5,
    }:
        findings.append("qualification limits drifted")
    if manifest.get("safety") != {
        "caller_promql": False,
        "target_mutations": 0,
        "executor_calls": 0,
        "execution_agent_calls": 0,
        "message_bodies_recorded": False,
        "credentials_recorded": False,
    }:
        findings.append("read-only safety boundary drifted")

    components = manifest.get("components")
    if not isinstance(components, list):
        findings.append("components must be an array")
        return findings
    actual = []
    for component in components:
        if not isinstance(component, dict):
            findings.append("each component must be an object")
            continue
        actual.append(
            (
                component.get("query_component"),
                component.get("evidence_component"),
                component.get("representative_requirement_id"),
                component.get("canonical_metric"),
            )
        )
        canonical = component.get("canonical_metric")
        if isinstance(canonical, str) and component.get("collector_metric") != f"rocketmq_{canonical}":
            findings.append(f"{component.get('query_component')} Collector alias is inconsistent")
    if tuple(actual) != EXPECTED_COMPONENTS:
        findings.append("component qualification matrix drifted")

    for query_component, _, requirement_id, canonical_metric in EXPECTED_COMPONENTS:
        path = sre_root / "config" / "observability" / "required-signals" / MANIFEST_FILES[query_component]
        text = path.read_text(encoding="utf-8")
        if f"requirement_id: {requirement_id}" not in text:
            findings.append(f"{query_component} representative requirement is absent from its signal manifest")
        if f"query_resource: metrics/{canonical_metric}" not in text:
            findings.append(f"{query_component} canonical metric route drifted")

    source = (sre_root / "crates" / "rocketmq-sre-connector" / "src" / "sources" / "prometheus.rs").read_text(
        encoding="utf-8"
    )
    for marker in (
        "ROCKETMQ_SERVICE_NAMESPACE_PREFIX",
        "fn metric_expression",
        'format!("{canonical} or {ROCKETMQ_SERVICE_NAMESPACE_PREFIX}{metric}{{{selector}}}")',
    ):
        if marker not in source:
            findings.append(f"Prometheus alias resolver is missing {marker}")

    composite = (
        sre_root
        / "crates"
        / "rocketmq-sre-connector"
        / "src"
        / "sources"
        / "required_signals.rs"
    ).read_text(encoding="utf-8")
    for marker in ("fn per_signal_row_budget", "signal_max_rows"):
        if marker not in composite:
            findings.append(f"Required Signals aggregate is missing {marker}")

    runtime = (
        sre_root
        / "crates"
        / "rocketmq-sre-connector"
        / "src"
        / "sources"
        / "runtime_diagnostics.rs"
    ).read_text(encoding="utf-8")
    if "serde_json::from_value(raw).map_err(|_| schema_mismatch(\"runtime\"))" not in runtime:
        findings.append("runtime diagnostics no longer consumes gateway-validated System Resource data")

    repository_root = sre_root.parent
    mcp_app = (repository_root / "rocketmq-tools" / "rocketmq-mcp" / "src" / "app.rs").read_text(
        encoding="utf-8"
    )
    mcp_server = (
        repository_root / "rocketmq-tools" / "rocketmq-mcp" / "src" / "protocol" / "server.rs"
    ).read_text(encoding="utf-8")
    for marker, source_name, source_text in (
        ("McpMetricsRecorder::from_handle", "MCP application", mcp_app),
        ("with_metrics(self.app.metrics().clone())", "MCP protocol server", mcp_server),
    ):
        if marker not in source_text:
            findings.append(f"{source_name} is missing instance-owned request metrics")

    smoke = (sre_root / "scripts" / "phase00-smoke.ps1").read_text(encoding="utf-8")
    for marker in (
        "config/qualification/required-signals.v1.json",
        "Assert-RequiredSignalsQualification",
        "representative_requirement_id",
        "/v1/conversations",
        "/v1/evidence/",
        "metrics/range/",
    ):
        if marker not in smoke:
            findings.append(f"live smoke is missing {marker}")
    for forbidden in (
        "127.0.0.1:8091/internal/v1/evidence/query",
        "127.0.0.1:8091/internal/v1/capabilities",
    ):
        if forbidden in smoke:
            findings.append("live smoke bypasses the authenticated reverse Connector channel")
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    arguments = parser.parse_args()
    findings = validate_manifest(load_json(arguments.manifest))
    if findings:
        for finding in findings:
            print(f"ERROR: {finding}")
        return 1
    print("Required Signals qualification contract is valid for five read-only component routes.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
