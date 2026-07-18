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

"""Enforce the versioned RocketMQ telemetry semantic registry."""

from __future__ import annotations

import argparse
import json
import re
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGISTRY = ROOT / "scripts" / "telemetry-semantic-registry.json"
SEMANTIC_SOURCE = ROOT / "rocketmq-observability" / "src" / "semantic.rs"
CATALOG_SOURCE = ROOT / "rocketmq-observability" / "src" / "metrics" / "catalog.rs"
SPAN_SOURCE = ROOT / "rocketmq-observability" / "src" / "trace" / "span_names.rs"
OUTAGE_SOURCE = ROOT / "rocketmq-observability" / "src" / "exporter" / "outage.rs"

REQUIRED_FAMILIES = {
    "request",
    "watermark-lag",
    "connection-bytes",
    "task",
    "recovery-cache",
    "auth-mcp",
    "exporter",
}
REQUIRED_SIGNAL_FIELDS = {
    "id",
    "source_symbol",
    "signal_type",
    "owner",
    "family",
    "stability",
    "attributes",
    "cardinality_budget",
    "privacy",
    "sampling",
    "deprecation",
}
EXPECTED_OUTAGE_CONSTANTS = {
    "DEFAULT_MAX_QUEUE_ITEMS": "max_queue_items",
    "DEFAULT_MAX_QUEUE_BYTES": "max_queue_bytes",
    "DEFAULT_MAX_RECORD_BYTES": "max_record_bytes",
    "DEFAULT_MAX_EXPORT_BATCH_ITEMS": "max_export_batch_items",
    "DEFAULT_SCHEDULED_DELAY_MILLIS": "scheduled_delay_millis",
    "DEFAULT_EXPORT_TIMEOUT_MILLIS": "export_timeout_millis",
    "DEFAULT_SHUTDOWN_TIMEOUT_MILLIS": "shutdown_timeout_millis",
}
FORBIDDEN_ATTRIBUTE_RE = re.compile(
    r"(?:password|passwd|credential|private[._-]?key|access[._-]?key|secret|token|message[._-]?body(?![._-]?size))",
    re.IGNORECASE,
)
SEMVER_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")


class SourceInventoryError(ValueError):
    """Raised when the Rust telemetry inventory cannot be parsed exactly."""


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read JSON {path}: {error}") from error
    if not isinstance(value, dict):
        raise ValueError(f"JSON root must be an object: {path}")
    return value


def module_body(source: str, module: str) -> str:
    match = re.search(rf"pub\s+mod\s+{re.escape(module)}\s*\{{", source)
    if match is None:
        raise SourceInventoryError(f"missing module: {module}")
    depth = 1
    cursor = match.end()
    while cursor < len(source) and depth:
        if source[cursor] == "{":
            depth += 1
        elif source[cursor] == "}":
            depth -= 1
        cursor += 1
    if depth:
        raise SourceInventoryError(f"unterminated module: {module}")
    return source[match.end() : cursor - 1]


def parse_string_constants(source: str) -> dict[str, str]:
    return dict(re.findall(r'pub\s+const\s+(\w+)\s*:\s*&str\s*=\s*"([^"]+)"\s*;', source))


def catalog_array_body(source: str, name: str) -> str:
    match = re.search(rf"pub\s+const\s+{re.escape(name)}\s*:.*?=\s*&\[(.*?)\n\];", source, re.DOTALL)
    if match is None:
        raise SourceInventoryError(f"missing catalog array: {name}")
    return match.group(1)


def parse_label_sets(source: str, labels: dict[str, str]) -> dict[str, list[str]]:
    label_sets: dict[str, list[str]] = {}
    pattern = re.compile(r"const\s+(\w+)\s*:\s*&\[&str\]\s*=\s*&\[(.*?)\];", re.DOTALL)
    for name, body in pattern.findall(source):
        symbols = re.findall(r"labels::(\w+)", body)
        try:
            label_sets[name] = [labels[symbol] for symbol in symbols]
        except KeyError as error:
            raise SourceInventoryError(f"unknown label symbol in {name}: {error.args[0]}") from error
    return label_sets


def parse_catalog(
    source: str,
    metrics: dict[str, str],
    labels: dict[str, str],
) -> list[dict[str, Any]]:
    label_sets = parse_label_sets(source, labels)
    descriptor_re = re.compile(
        r"MetricDescriptor\s*\{\s*"
        r"name:\s*metrics::(\w+),\s*"
        r"kind:\s*MetricKind::(\w+),\s*"
        r'unit:\s*"([^"]+)",\s*'
        r"labels:\s*(.*?),\s*"
        r"source:\s*MetricSource::(\w+),\s*\}",
        re.DOTALL,
    )
    descriptors: list[dict[str, Any]] = []
    for catalog_name in ("JAVA_METRICS", "RUST_METRICS"):
        body = catalog_array_body(source, catalog_name)
        matches = descriptor_re.findall(body)
        if not matches:
            raise SourceInventoryError(f"no descriptors parsed from {catalog_name}")
        for symbol, kind, unit, label_expression, owner in matches:
            if symbol not in metrics:
                raise SourceInventoryError(f"catalog references unknown metric symbol: {symbol}")
            expression = label_expression.strip()
            if expression.startswith("&["):
                label_symbols = re.findall(r"labels::(\w+)", expression)
                try:
                    attributes = [labels[item] for item in label_symbols]
                except KeyError as error:
                    raise SourceInventoryError(
                        f"catalog metric {symbol} references unknown label: {error.args[0]}"
                    ) from error
            else:
                if expression not in label_sets:
                    raise SourceInventoryError(f"catalog metric {symbol} uses unknown label set: {expression}")
                attributes = label_sets[expression]
            descriptors.append(
                {
                    "id": metrics[symbol],
                    "source_symbol": symbol,
                    "kind": re.sub(r"(?<!^)(?=[A-Z])", "_", kind).lower(),
                    "unit": unit,
                    "attributes": attributes,
                    "source": owner,
                    "catalog": "java" if catalog_name == "JAVA_METRICS" else "rust",
                }
            )
    return descriptors


def parse_integer_expression(expression: str) -> int:
    normalized = expression.replace("_", "").strip()
    if not re.fullmatch(r"[0-9*\s]+", normalized):
        raise SourceInventoryError(f"unsupported integer expression: {expression}")
    result = 1
    for factor in normalized.split("*"):
        result *= int(factor.strip())
    return result


def parse_outage_constants(source: str) -> dict[str, int]:
    constants: dict[str, int] = {}
    for constant in EXPECTED_OUTAGE_CONSTANTS:
        match = re.search(
            rf"pub\s+const\s+{re.escape(constant)}\s*:\s*(?:usize|u64)\s*=\s*([^;]+);",
            source,
        )
        if match is None:
            raise SourceInventoryError(f"missing outage constant: {constant}")
        constants[constant] = parse_integer_expression(match.group(1))
    return constants


def build_source_inventory() -> dict[str, Any]:
    semantic_source = SEMANTIC_SOURCE.read_text(encoding="utf-8")
    metrics = parse_string_constants(module_body(semantic_source, "metrics"))
    labels = parse_string_constants(module_body(semantic_source, "labels"))
    events = parse_string_constants(module_body(semantic_source, "events"))
    catalog = parse_catalog(CATALOG_SOURCE.read_text(encoding="utf-8"), metrics, labels)
    spans = parse_string_constants(SPAN_SOURCE.read_text(encoding="utf-8"))
    outage = parse_outage_constants(OUTAGE_SOURCE.read_text(encoding="utf-8"))

    catalog_symbols = {item["source_symbol"] for item in catalog}
    if catalog_symbols != set(metrics):
        missing = sorted(set(metrics) - catalog_symbols)
        unknown = sorted(catalog_symbols - set(metrics))
        raise SourceInventoryError(f"metric/catalog drift: missing={missing} unknown={unknown}")
    if len(catalog) != len({item["id"] for item in catalog}):
        raise SourceInventoryError("duplicate metric identifiers in combined catalog")
    return {
        "metrics": catalog,
        "spans": spans,
        "events": events,
        "labels": labels,
        "outage": outage,
    }


def version_tuple(value: Any) -> tuple[int, int, int] | None:
    if not isinstance(value, str):
        return None
    match = SEMVER_RE.fullmatch(value)
    if match is None:
        return None
    return tuple(int(part) for part in match.groups())


def validate_deprecation(signal: dict[str, Any], findings: list[str]) -> None:
    signal_id = signal.get("id", "<unknown>")
    deprecation = signal.get("deprecation")
    if not isinstance(deprecation, dict):
        findings.append(f"{signal_id}: deprecation must be an object")
        return
    status = deprecation.get("status")
    since = version_tuple(deprecation.get("since"))
    if status not in {"active", "deprecated"} or since is None:
        findings.append(f"{signal_id}: invalid deprecation status or since version")
        return
    replacement = deprecation.get("replacement")
    remove_after = version_tuple(deprecation.get("remove_after"))
    if status == "active":
        if replacement is not None or deprecation.get("remove_after") is not None:
            findings.append(f"{signal_id}: active signal cannot declare replacement or removal")
        return
    if not isinstance(replacement, str) or not replacement or remove_after is None:
        findings.append(f"{signal_id}: deprecated signal requires replacement and remove_after")
        return
    since_minor = since[0] * 10_000 + since[1]
    removal_minor = remove_after[0] * 10_000 + remove_after[1]
    if removal_minor - since_minor < 2:
        findings.append(f"{signal_id}: deprecation window must span at least two minor versions")


def validate_attributes(registry: dict[str, Any], findings: list[str]) -> dict[str, dict[str, Any]]:
    raw_attributes = registry.get("attributes")
    if not isinstance(raw_attributes, list):
        findings.append("attributes must be a list")
        return {}
    attributes: dict[str, dict[str, Any]] = {}
    for attribute in raw_attributes:
        if not isinstance(attribute, dict) or not isinstance(attribute.get("id"), str):
            findings.append("attribute entries must be objects with string ids")
            continue
        attribute_id = attribute["id"]
        if attribute_id in attributes:
            findings.append(f"duplicate attribute: {attribute_id}")
            continue
        attributes[attribute_id] = attribute
        if FORBIDDEN_ATTRIBUTE_RE.search(attribute_id):
            findings.append(f"forbidden sensitive attribute identifier: {attribute_id}")
        cardinality = attribute.get("cardinality")
        privacy = attribute.get("privacy")
        if cardinality not in {"low", "bounded", "high"}:
            findings.append(f"{attribute_id}: invalid cardinality")
        if privacy not in {"public", "operational", "sensitive"}:
            findings.append(f"{attribute_id}: invalid privacy")
        if not isinstance(attribute.get("max_distinct"), int) or attribute.get("max_distinct", 0) <= 0:
            findings.append(f"{attribute_id}: max_distinct must be positive")
        if not isinstance(attribute.get("max_value_bytes"), int) or attribute.get("max_value_bytes", 0) <= 0:
            findings.append(f"{attribute_id}: max_value_bytes must be positive")
        if cardinality == "high" and attribute.get("default_enabled") is not False:
            findings.append(f"{attribute_id}: high-cardinality attribute must be disabled by default")
        if privacy == "sensitive" and attribute.get("redaction") not in {"hash", "drop"}:
            findings.append(f"{attribute_id}: sensitive attribute requires hash or drop redaction")
    return attributes


def validate_signal_common(
    signal: Any,
    attributes: dict[str, dict[str, Any]],
    findings: list[str],
) -> None:
    if not isinstance(signal, dict):
        findings.append("signal entry must be an object")
        return
    signal_id = signal.get("id", "<unknown>")
    missing_fields = sorted(REQUIRED_SIGNAL_FIELDS - set(signal))
    if missing_fields:
        findings.append(f"{signal_id}: missing signal fields {missing_fields}")
    if signal.get("signal_type") not in {"metric", "span", "log"}:
        findings.append(f"{signal_id}: invalid signal_type")
    if signal.get("stability") not in {"stable", "experimental"}:
        findings.append(f"{signal_id}: invalid stability")
    if signal.get("privacy") not in {"public", "operational", "sensitive"}:
        findings.append(f"{signal_id}: invalid privacy")
    budget = signal.get("cardinality_budget")
    if not isinstance(budget, int) or isinstance(budget, bool) or budget <= 0:
        findings.append(f"{signal_id}: cardinality_budget must be a positive integer")
    signal_attributes = signal.get("attributes")
    if not isinstance(signal_attributes, list) or len(signal_attributes) != len(set(signal_attributes)):
        findings.append(f"{signal_id}: attributes must be a unique list")
    else:
        unknown = sorted(set(signal_attributes) - set(attributes))
        if unknown:
            findings.append(f"{signal_id}: undeclared attributes {unknown}")
        high = [item for item in signal_attributes if attributes.get(item, {}).get("cardinality") == "high"]
        if high and signal.get("privacy") != "sensitive":
            findings.append(f"{signal_id}: high-cardinality attributes require sensitive privacy")
    sampling = signal.get("sampling")
    if not isinstance(sampling, dict) or sampling.get("strategy") not in {
        "aggregate",
        "ratio",
        "rate_limit",
    }:
        findings.append(f"{signal_id}: invalid sampling policy")
    elif signal.get("signal_type") in {"span", "log"}:
        limit = sampling.get("max_events_per_operation")
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 0:
            findings.append(f"{signal_id}: span/log sampling requires a finite per-operation limit")
        if sampling.get("strategy") == "ratio":
            ratio = sampling.get("default_ratio")
            if not isinstance(ratio, (int, float)) or isinstance(ratio, bool) or not 0 <= ratio <= 1:
                findings.append(f"{signal_id}: ratio sampling must remain between zero and one")
        if sampling.get("strategy") == "rate_limit":
            rate = sampling.get("max_per_second")
            if not isinstance(rate, int) or isinstance(rate, bool) or rate <= 0:
                findings.append(f"{signal_id}: rate-limited event requires max_per_second")
    validate_deprecation(signal, findings)


def validate_source_sync(
    signals: list[dict[str, Any]],
    inventory: dict[str, Any],
    findings: list[str],
) -> None:
    registry_by_key = {
        (signal.get("signal_type"), signal.get("id")): signal
        for signal in signals
        if isinstance(signal, dict)
    }
    expected_keys = {
        *(('metric', item["id"]) for item in inventory["metrics"]),
        *(("span", value) for value in inventory["spans"].values()),
        *(("log", value) for value in inventory["events"].values()),
    }
    actual_keys = set(registry_by_key)
    for signal_type, signal_id in sorted(expected_keys - actual_keys):
        findings.append(f"missing registry signal: {signal_type}:{signal_id}")
    for signal_type, signal_id in sorted(actual_keys - expected_keys):
        findings.append(f"unknown registry signal: {signal_type}:{signal_id}")

    for metric in inventory["metrics"]:
        signal = registry_by_key.get(("metric", metric["id"]))
        if signal is None:
            continue
        for field in ("source_symbol", "kind", "unit", "attributes"):
            if signal.get(field) != metric[field]:
                findings.append(
                    f"{metric['id']}: registry {field}={signal.get(field)!r} "
                    f"does not match source {metric[field]!r}"
                )
        expected_owner = re.sub(r"(?<!^)(?=[A-Z])", "-", metric["source"]).lower()
        if signal.get("owner") != expected_owner:
            findings.append(f"{metric['id']}: owner does not match catalog source {expected_owner}")
        if signal.get("catalog") != metric["catalog"]:
            findings.append(f"{metric['id']}: catalog provenance does not match source")

    for signal_type, source_key, source_values in (
        ("span", "spans", inventory["spans"]),
        ("log", "events", inventory["events"]),
    ):
        for symbol, signal_id in source_values.items():
            signal = registry_by_key.get((signal_type, signal_id))
            if signal is not None and signal.get("source_symbol") != symbol:
                findings.append(f"{signal_id}: source_symbol does not match {source_key} source")


def validate_outage_policy(
    registry: dict[str, Any],
    inventory: dict[str, Any],
    signal_ids: set[str],
    findings: list[str],
) -> None:
    policy = registry.get("outage_policy")
    if not isinstance(policy, dict):
        findings.append("outage_policy must be an object")
        return
    for constant, field in EXPECTED_OUTAGE_CONSTANTS.items():
        expected = inventory["outage"][constant]
        if policy.get(field) != expected:
            findings.append(f"outage_policy.{field} must match Rust constant {constant}={expected}")
    required_values = {
        "enqueue": "try_enqueue",
        "overflow": "drop_newest",
        "shutdown_deadline": "absolute",
        "data_plane_blocking": False,
        "drop_signal": "rocketmq.exporter.drop",
        "shutdown_signal": "rocketmq.exporter.shutdown",
    }
    for field, expected in required_values.items():
        if policy.get(field) != expected:
            findings.append(f"outage_policy.{field} must remain {expected!r}")
    for signal_field in ("drop_signal", "shutdown_signal"):
        if policy.get(signal_field) not in signal_ids:
            findings.append(f"outage_policy.{signal_field} references an unknown signal")
    measurements = policy.get("measurements")
    required_measurements = {
        "accepted",
        "drained",
        "dropped_by_reason",
        "queued_items",
        "queued_bytes",
    }
    if not isinstance(measurements, list) or not required_measurements.issubset(measurements):
        findings.append("outage_policy.measurements does not expose every bounded-queue outcome")


def validate_registry(registry: dict[str, Any], inventory: dict[str, Any] | None = None) -> list[str]:
    findings: list[str] = []
    if registry.get("schema_version") != 1 or registry.get("milestone") != "M11-01":
        findings.append("registry schema_version or milestone is invalid")
    if version_tuple(registry.get("registry_version")) is None:
        findings.append("registry_version must be semantic versioning")
    attributes = validate_attributes(registry, findings)
    signals = registry.get("signals")
    if not isinstance(signals, list):
        findings.append("signals must be a list")
        return findings
    keys: list[tuple[Any, Any]] = []
    for signal in signals:
        validate_signal_common(signal, attributes, findings)
        if isinstance(signal, dict):
            keys.append((signal.get("signal_type"), signal.get("id")))
    if len(keys) != len(set(keys)):
        findings.append("duplicate signal type/id pairs in registry")
    families = {signal.get("family") for signal in signals if isinstance(signal, dict)}
    missing_families = sorted(REQUIRED_FAMILIES - families)
    if missing_families:
        findings.append(f"registry is missing required families: {missing_families}")

    source_inventory = inventory or build_source_inventory()
    validate_source_sync(signals, source_inventory, findings)
    signal_ids = {signal.get("id") for signal in signals if isinstance(signal, dict)}
    validate_outage_policy(registry, source_inventory, signal_ids, findings)
    return findings


def apply_fixture(document: dict[str, Any], fixture: dict[str, Any]) -> dict[str, Any]:
    """Apply a deterministic violation fixture used by guard regression tests."""

    result = deepcopy(document)
    path = fixture.get("path")
    if not isinstance(path, list) or not path:
        raise ValueError("fixture path must be a non-empty list")
    parent: Any = result
    for component in path[:-1]:
        parent = parent[component]
    operation = fixture.get("operation")
    terminal = path[-1]
    if operation == "set":
        parent[terminal] = deepcopy(fixture.get("value"))
    elif operation == "delete":
        del parent[terminal]
    elif operation == "append":
        parent[terminal].append(deepcopy(fixture.get("value")))
    else:
        raise ValueError(f"unsupported fixture operation: {operation}")
    return result


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        registry = load_json(args.registry)
        inventory = build_source_inventory()
        findings = validate_registry(registry, inventory)
    except (OSError, ValueError) as error:
        print(f"telemetry semantic guard: FAIL\n- {error}")
        return 1
    if findings:
        print("telemetry semantic guard: FAIL")
        for finding in findings:
            print(f"- {finding}")
        return 1
    signal_counts = {
        signal_type: sum(1 for signal in registry["signals"] if signal["signal_type"] == signal_type)
        for signal_type in ("metric", "span", "log")
    }
    print(
        "telemetry semantic guard: PASS "
        f"version={registry['registry_version']} metrics={signal_counts['metric']} "
        f"spans={signal_counts['span']} logs={signal_counts['log']} "
        f"attributes={len(registry['attributes'])}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
