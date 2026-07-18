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

"""Regenerate the checked-in M11 telemetry semantic registry."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import telemetry_semantic_guard as guard


ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = ROOT / "scripts" / "telemetry-semantic-registry.json"
FIXTURE_PATH = ROOT / "scripts" / "telemetry-semantic-guard-violations.json"

HIGH_CARDINALITY_ATTRIBUTES = {
    "messaging.message.id",
    "messaging.rocketmq.message.keys",
}
PUBLIC_ATTRIBUTES = {
    "success",
    "result",
    "state",
    "signal_type",
}
SPAN_ATTRIBUTES = {
    "BROKER_RECEIVE_SEND": [
        "messaging.system",
        "messaging.operation.name",
        "messaging.destination.name",
        "server.address",
        "messaging.destination.partition.id",
    ],
    "STORE_APPEND": [
        "messaging.system",
        "messaging.operation.name",
        "messaging.destination.name",
        "messaging.message.body.size",
    ],
    "PRODUCER_SEND": [
        "messaging.system",
        "messaging.operation.name",
        "messaging.destination.name",
        "messaging.message.id",
        "messaging.message.body.size",
        "messaging.rocketmq.message.keys",
    ],
    "CONSUMER_PROCESS": [
        "messaging.system",
        "messaging.operation.name",
        "messaging.destination.name",
        "messaging.consumer.group.name",
        "server.address",
        "messaging.destination.partition.id",
        "rocketmq.consume.mode",
        "rocketmq.batch.message_count",
        "messaging.message.id",
        "messaging.message.body.size",
        "messaging.rocketmq.message.keys",
    ],
}
EVENT_CONTRACTS = {
    "AUTH_DECISION": ("auth-mcp", "security", ["operation", "decision", "result", "reason"]),
    "AUTH_RELOAD": ("auth-mcp", "security", ["result", "reason"]),
    "MCP_ACTION": ("auth-mcp", "mcp", ["operation", "result", "reason"]),
    "TASK_LIFECYCLE": ("task", "runtime", ["task_type", "state", "result", "reason"]),
    "RECOVERY_STATE": ("recovery-cache", "storage", ["component", "state", "result", "reason"]),
    "EXPORTER_DROP": ("exporter", "observability", ["reason", "signal_type", "dropped_count"]),
    "EXPORTER_SHUTDOWN": (
        "exporter",
        "observability",
        ["result", "reason", "queued_items", "queued_bytes", "dropped_count"],
    ),
}


def metric_family(metric_id: str) -> str:
    if "metrics_label_dropped" in metric_id:
        return "exporter"
    if any(token in metric_id for token in ("watermark", "lag", "behind", "inflight", "ready", "queueing")):
        return "watermark-lag"
    if any(token in metric_id for token in ("connection", "bytes", "throughput", "size", "disk_usage")):
        return "connection-bytes"
    if any(token in metric_id for token in ("rocksdb", "tiered", "storage", "store_")):
        return "recovery-cache"
    return "request"


def attribute_contract(attribute_id: str) -> dict[str, Any]:
    high = attribute_id in HIGH_CARDINALITY_ATTRIBUTES
    privacy = "sensitive" if high else "public" if attribute_id in PUBLIC_ATTRIBUTES else "operational"
    return {
        "id": attribute_id,
        "cardinality": "high" if high else "bounded",
        "privacy": privacy,
        "max_distinct": 1_000 if high else 10_000,
        "max_value_bytes": 512 if high else 256,
        "default_enabled": not high,
        "redaction": "hash" if high else "none",
    }


def deprecation() -> dict[str, Any]:
    return {
        "status": "active",
        "since": "1.0.0",
        "replacement": None,
        "remove_after": None,
    }


def signal_privacy(attributes: list[str]) -> str:
    return "sensitive" if HIGH_CARDINALITY_ATTRIBUTES.intersection(attributes) else "operational"


def build_registry() -> dict[str, Any]:
    inventory = guard.build_source_inventory()
    extra_attributes = {
        attribute
        for attributes in SPAN_ATTRIBUTES.values()
        for attribute in attributes
    }
    extra_attributes.update(
        attribute
        for _, _, attributes in EVENT_CONTRACTS.values()
        for attribute in attributes
    )
    attribute_ids = set(inventory["labels"].values()) | extra_attributes
    signals: list[dict[str, Any]] = []

    for metric in inventory["metrics"]:
        owner = re.sub(r"(?<!^)(?=[A-Z])", "-", metric["source"]).lower()
        signals.append(
            {
                "id": metric["id"],
                "source_symbol": metric["source_symbol"],
                "signal_type": "metric",
                "kind": metric["kind"],
                "unit": metric["unit"],
                "owner": owner,
                "catalog": metric["catalog"],
                "family": metric_family(metric["id"]),
                "stability": "stable",
                "attributes": metric["attributes"],
                "cardinality_budget": 10_000 if metric["attributes"] else 1,
                "privacy": signal_privacy(metric["attributes"]),
                "sampling": {"strategy": "aggregate"},
                "deprecation": deprecation(),
            }
        )

    span_owners = {
        "BROKER_RECEIVE_SEND": "broker",
        "STORE_APPEND": "store",
        "PRODUCER_SEND": "client",
        "CONSUMER_PROCESS": "client",
    }
    for symbol, span_id in inventory["spans"].items():
        attributes = SPAN_ATTRIBUTES[symbol]
        signals.append(
            {
                "id": span_id,
                "source_symbol": symbol,
                "signal_type": "span",
                "owner": span_owners[symbol],
                "family": "recovery-cache" if symbol == "STORE_APPEND" else "request",
                "stability": "stable",
                "attributes": attributes,
                "cardinality_budget": 1_000,
                "privacy": signal_privacy(attributes),
                "sampling": {
                    "strategy": "ratio",
                    "default_ratio": 0.01,
                    "max_events_per_operation": 0,
                },
                "deprecation": deprecation(),
            }
        )

    for symbol, event_id in inventory["events"].items():
        family, owner, attributes = EVENT_CONTRACTS[symbol]
        signals.append(
            {
                "id": event_id,
                "source_symbol": symbol,
                "signal_type": "log",
                "owner": owner,
                "family": family,
                "stability": "stable",
                "attributes": attributes,
                "cardinality_budget": 1_000,
                "privacy": signal_privacy(attributes),
                "sampling": {
                    "strategy": "rate_limit",
                    "max_per_second": 100,
                    "max_events_per_operation": 1,
                },
                "deprecation": deprecation(),
            }
        )

    outage = {
        field: inventory["outage"][constant]
        for constant, field in guard.EXPECTED_OUTAGE_CONSTANTS.items()
    }
    outage.update(
        {
            "enqueue": "try_enqueue",
            "overflow": "drop_newest",
            "shutdown_deadline": "absolute",
            "data_plane_blocking": False,
            "drop_signal": "rocketmq.exporter.drop",
            "shutdown_signal": "rocketmq.exporter.shutdown",
            "measurements": [
                "accepted",
                "drained",
                "dropped_by_reason",
                "queued_items",
                "queued_bytes",
            ],
        }
    )
    return {
        "schema_version": 1,
        "registry_version": "1.0.0",
        "milestone": "M11-01",
        "description": "Canonical telemetry semantics and collector-outage behavior for RocketMQ Rust.",
        "required_families": sorted(guard.REQUIRED_FAMILIES),
        "attributes": [attribute_contract(item) for item in sorted(attribute_ids)],
        "signals": signals,
        "outage_policy": outage,
    }


def build_violation_fixtures(registry: dict[str, Any]) -> dict[str, Any]:
    high_index = next(
        index
        for index, attribute in enumerate(registry["attributes"])
        if attribute["cardinality"] == "high"
    )
    deprecated_index = next(
        index for index, signal in enumerate(registry["signals"]) if signal["signal_type"] == "log"
    )
    return {
        "schema_version": 1,
        "registry_version": registry["registry_version"],
        "fixtures": [
            {
                "id": "unknown-signal",
                "operation": "set",
                "path": ["signals", 0, "id"],
                "value": "rocketmq_unknown_metric",
                "expected": "unknown registry signal",
            },
            {
                "id": "undeclared-attribute",
                "operation": "set",
                "path": ["signals", 0, "attributes"],
                "value": ["undeclared.attribute"],
                "expected": "undeclared attributes",
            },
            {
                "id": "high-cardinality-default",
                "operation": "set",
                "path": ["attributes", high_index, "default_enabled"],
                "value": True,
                "expected": "high-cardinality attribute must be disabled by default",
            },
            {
                "id": "unbounded-event-rate",
                "operation": "delete",
                "path": ["signals", deprecated_index, "sampling", "max_per_second"],
                "expected": "rate-limited event requires max_per_second",
            },
            {
                "id": "invalid-deprecation-window",
                "operation": "set",
                "path": ["signals", deprecated_index, "deprecation"],
                "value": {
                    "status": "deprecated",
                    "since": "1.0.0",
                    "replacement": "rocketmq.auth.replacement",
                    "remove_after": "1.1.0",
                },
                "expected": "deprecation window must span at least two minor versions",
            },
            {
                "id": "blocking-outage-enqueue",
                "operation": "set",
                "path": ["outage_policy", "data_plane_blocking"],
                "value": True,
                "expected": "outage_policy.data_plane_blocking must remain False",
            },
            {
                "id": "weakened-byte-bound",
                "operation": "set",
                "path": ["outage_policy", "max_queue_bytes"],
                "value": 0,
                "expected": "outage_policy.max_queue_bytes must match Rust constant",
            },
        ],
    }


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def main() -> int:
    registry = build_registry()
    findings = guard.validate_registry(registry)
    if findings:
        raise SystemExit("refusing to write invalid registry:\n- " + "\n- ".join(findings))
    write_json(REGISTRY_PATH, registry)
    write_json(FIXTURE_PATH, build_violation_fixtures(registry))
    print(
        f"generated {REGISTRY_PATH.relative_to(ROOT)} with "
        f"{len(registry['signals'])} signals and {len(registry['attributes'])} attributes"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
