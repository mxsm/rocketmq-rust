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

"""Generate the static Rust metric catalog from its checked-in TOML schema."""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DIR = ROOT / "rocketmq-observability" / "src" / "metrics" / "catalog" / "schema"
GENERATED_PATH = ROOT / "rocketmq-observability" / "src" / "metrics" / "catalog" / "generated.rs"
SEMANTIC_PATH = ROOT / "rocketmq-observability" / "src" / "semantic.rs"
SCHEMA_VERSION = 1
PARTITIONS = (("java", 0, 94), ("rust", 94, 122))
KIND_NAMES = {"Counter", "Gauge", "Histogram", "ObservableGauge", "UpDownCounter"}
SOURCE_NAMES = {
    "Broker",
    "Client",
    "NameServer",
    "Pop",
    "Remoting",
    "Store",
    "Timer",
    "RocksDb",
    "TieredStore",
    "Proxy",
    "Controller",
    "Observability",
    "Mcp",
    "Runtime",
}
ROOT_KEYS = {"schema_version", "metric"}
METRIC_KEYS = {"index", "symbol", "kind", "unit", "labels", "source", "description"}
REQUIRED_METRIC_KEYS = METRIC_KEYS - {"description"}
RUST_IDENTIFIER_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")
RUST_STRING_LITERAL_RE = r'"(?:[^"\\\r\n]|\\.)*"'


class SchemaError(ValueError):
    """Raised when a metric catalog schema cannot produce a stable catalog."""


def module_body(source: str, module: str) -> str:
    match = re.search(rf"pub\s+mod\s+{re.escape(module)}\s*\{{", source)
    if match is None:
        raise SchemaError(f"semantic source is missing module {module!r}")
    depth = 1
    cursor = match.end()
    while cursor < len(source) and depth:
        if source[cursor] == "{":
            depth += 1
        elif source[cursor] == "}":
            depth -= 1
        cursor += 1
    if depth:
        raise SchemaError(f"semantic module {module!r} is unterminated")
    return source[match.end() : cursor - 1]


def decode_rust_string_literal(literal: str) -> str:
    if len(literal) < 2 or literal[0] != '"' or literal[-1] != '"':
        raise SchemaError(f"invalid Rust string literal: {literal!r}")

    decoded: list[str] = []
    cursor = 1
    end = len(literal) - 1
    simple_escapes = {"0": "\0", "t": "\t", "n": "\n", "r": "\r", "\"": '"', "'": "'", "\\": "\\"}
    while cursor < end:
        character = literal[cursor]
        if character != "\\":
            decoded.append(character)
            cursor += 1
            continue
        cursor += 1
        if cursor >= end:
            raise SchemaError(f"unterminated Rust string escape: {literal!r}")
        escape = literal[cursor]
        if escape in simple_escapes:
            decoded.append(simple_escapes[escape])
            cursor += 1
            continue
        if escape == "x":
            digits = literal[cursor + 1 : cursor + 3]
            if len(digits) != 2 or not re.fullmatch(r"[0-9A-Fa-f]{2}", digits):
                raise SchemaError(f"invalid Rust byte escape: {literal!r}")
            decoded.append(chr(int(digits, 16)))
            cursor += 3
            continue
        if escape == "u" and cursor + 1 < end and literal[cursor + 1] == "{":
            closing = literal.find("}", cursor + 2, end)
            digits = literal[cursor + 2 : closing] if closing != -1 else ""
            if (
                closing == -1
                or not re.fullmatch(r"[0-9A-Fa-f_]{1,6}", digits)
                or digits.startswith("_")
                or digits.endswith("_")
            ):
                raise SchemaError(f"invalid Rust Unicode escape: {literal!r}")
            codepoint = int(digits.replace("_", ""), 16)
            if codepoint > 0x10FFFF or 0xD800 <= codepoint <= 0xDFFF:
                raise SchemaError(f"invalid Rust Unicode scalar: {literal!r}")
            decoded.append(chr(codepoint))
            cursor = closing + 1
            continue
        raise SchemaError(f"unsupported Rust string escape: {literal!r}")
    return "".join(decoded)


def parse_semantic_string_constants(source: str, module: str) -> dict[str, str]:
    constants: dict[str, str] = {}
    body = module_body(source, module)
    declared_symbols = set(re.findall(r"pub\s+const\s+(\w+)\s*:\s*&str", body))
    pattern = re.compile(
        rf"pub\s+const\s+(\w+)\s*:\s*&str\s*=\s*({RUST_STRING_LITERAL_RE})\s*;",
        re.DOTALL,
    )
    for symbol, literal in pattern.findall(body):
        if symbol in constants:
            raise SchemaError(f"semantic module {module!r} contains duplicate symbol {symbol!r}")
        constants[symbol] = decode_rust_string_literal(literal)
    unsupported = declared_symbols - set(constants)
    if unsupported:
        raise SchemaError(
            f"semantic module {module!r} contains unsupported string declarations: {sorted(unsupported)}"
        )
    return constants


def semantic_symbols() -> tuple[dict[str, str], set[str]]:
    source = SEMANTIC_PATH.read_text(encoding="utf-8")
    metric_symbols = parse_semantic_string_constants(source, "metrics")
    label_symbols = set(parse_semantic_string_constants(source, "labels"))
    return metric_symbols, label_symbols


def parse_schema(path: Path) -> dict[str, Any]:
    try:
        value = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise SchemaError(f"cannot read TOML schema {path}: {error}") from error
    if not isinstance(value, dict):
        raise SchemaError(f"schema root must be a table: {path}")
    return value


def validate_partition(
    document: dict[str, Any],
    *,
    path: Path,
    partition: str,
    first_index: int,
    expected_count: int,
    metric_symbols: dict[str, str],
    label_symbols: set[str],
) -> list[dict[str, Any]]:
    unknown_root_keys = set(document) - ROOT_KEYS
    if unknown_root_keys:
        raise SchemaError(f"{path}: unknown root keys: {sorted(unknown_root_keys)}")
    schema_version = document.get("schema_version")
    if (
        not isinstance(schema_version, int)
        or isinstance(schema_version, bool)
        or schema_version != SCHEMA_VERSION
    ):
        raise SchemaError(
            f"{path}: schema_version must be the integer {SCHEMA_VERSION}, got {schema_version!r}"
        )
    metrics = document.get("metric")
    if not isinstance(metrics, list):
        raise SchemaError(f"{path}: metric must be an array of tables")
    if len(metrics) != expected_count:
        raise SchemaError(
            f"{path}: {partition} partition must contain {expected_count} metrics, got {len(metrics)}"
        )

    validated: list[dict[str, Any]] = []
    for position, raw_metric in enumerate(metrics):
        location = f"{path}: metric[{position}]"
        if not isinstance(raw_metric, dict):
            raise SchemaError(f"{location} must be a table")
        unknown_metric_keys = set(raw_metric) - METRIC_KEYS
        if unknown_metric_keys:
            raise SchemaError(f"{location}: unknown keys: {sorted(unknown_metric_keys)}")
        missing_metric_keys = REQUIRED_METRIC_KEYS - set(raw_metric)
        if missing_metric_keys:
            raise SchemaError(f"{location}: missing required keys: {sorted(missing_metric_keys)}")

        index = raw_metric["index"]
        symbol = raw_metric["symbol"]
        kind = raw_metric["kind"]
        unit = raw_metric["unit"]
        labels = raw_metric["labels"]
        source = raw_metric["source"]
        if not isinstance(index, int) or isinstance(index, bool):
            raise SchemaError(f"{location}: index must be an integer")
        if index != first_index + position:
            raise SchemaError(
                f"{location}: index must be contiguous; expected {first_index + position}, got {index}"
            )
        if not isinstance(symbol, str) or not RUST_IDENTIFIER_RE.fullmatch(symbol):
            raise SchemaError(f"{location}: symbol must be an uppercase Rust identifier")
        if symbol not in metric_symbols:
            raise SchemaError(f"{location}: unknown semantic metric symbol {symbol!r}")
        if not isinstance(kind, str) or kind not in KIND_NAMES:
            raise SchemaError(f"{location}: unknown metric kind {kind!r}")
        if not isinstance(unit, str):
            raise SchemaError(f"{location}: unit must be a string")
        if any(ord(character) < 0x20 for character in unit):
            raise SchemaError(f"{location}: unit must not contain C0 control characters")
        if not isinstance(labels, list) or not all(isinstance(label, str) for label in labels):
            raise SchemaError(f"{location}: labels must be an array of strings")
        if len(labels) != len(set(labels)):
            raise SchemaError(f"{location}: labels must not contain duplicates")
        for label in labels:
            if not RUST_IDENTIFIER_RE.fullmatch(label):
                raise SchemaError(f"{location}: label {label!r} must be an uppercase Rust identifier")
            if label not in label_symbols:
                raise SchemaError(f"{location}: unknown semantic label symbol {label!r}")
        if not isinstance(source, str) or source not in SOURCE_NAMES:
            raise SchemaError(f"{location}: unknown metric source {source!r}")
        description = raw_metric.get("description")
        if description is not None and (not isinstance(description, str) or not description):
            raise SchemaError(f"{location}: description must be a non-empty string when present")
        validated.append(
            {
                "partition": partition,
                "index": index,
                "symbol": symbol,
                "name": metric_symbols[symbol],
                "kind": kind,
                "unit": unit,
                "labels": tuple(labels),
                "source": source,
            }
        )
    return validated


def validate_global_catalog(descriptors: list[dict[str, Any]]) -> None:
    indexes = [descriptor["index"] for descriptor in descriptors]
    if indexes != list(range(sum(count for _, _, count in PARTITIONS))):
        raise SchemaError("catalog indexes must be globally unique and contiguous from zero")
    symbols = [descriptor["symbol"] for descriptor in descriptors]
    if len(symbols) != len(set(symbols)):
        raise SchemaError("catalog contains duplicate semantic metric symbols")
    names = [descriptor["name"] for descriptor in descriptors]
    if len(names) != len(set(names)):
        raise SchemaError("catalog contains duplicate resolved metric names")


def load_catalog() -> list[dict[str, Any]]:
    metric_symbols, label_symbols = semantic_symbols()
    descriptors: list[dict[str, Any]] = []
    for partition, first_index, expected_count in PARTITIONS:
        path = SCHEMA_DIR / partition / "catalog.toml"
        descriptors.extend(
            validate_partition(
                parse_schema(path),
                path=path,
                partition=partition,
                first_index=first_index,
                expected_count=expected_count,
                metric_symbols=metric_symbols,
                label_symbols=label_symbols,
            )
        )
    validate_global_catalog(descriptors)
    return descriptors


def rust_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def render_catalog(descriptors: list[dict[str, Any]]) -> str:
    label_sets: dict[tuple[str, ...], str] = {}
    for descriptor in descriptors:
        labels = descriptor["labels"]
        label_sets.setdefault(labels, f"METRIC_LABELS_{len(label_sets)}")

    lines = [
        "// Copyright 2026 The RocketMQ Rust Authors",
        "//",
        "// Licensed under the Apache License, Version 2.0 (the \"License\");",
        "// you may not use this file except in compliance with the License.",
        "// You may obtain a copy of the License at",
        "//",
        "//     http://www.apache.org/licenses/LICENSE-2.0",
        "//",
        "// Unless required by applicable law or agreed to in writing, software",
        "// distributed under the License is distributed on an \"AS IS\" BASIS,",
        "// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.",
        "// See the License for the specific language governing permissions and",
        "// limitations under the License.",
        "",
        "// @generated by scripts/generate_metric_catalog.py; do not edit manually.",
        "",
    ]
    for labels, name in label_sets.items():
        if labels:
            lines.append(f"const {name}: &[&str] = &[")
            lines.extend(f"    labels::{label}," for label in labels)
            lines.append("];\n")
        else:
            lines.append(f"const {name}: &[&str] = &[];\n")

    for partition, _, _ in PARTITIONS:
        constant_name = f"{partition.upper()}_METRICS"
        lines.append(f"pub const {constant_name}: &[MetricDescriptor] = &[")
        for descriptor in (item for item in descriptors if item["partition"] == partition):
            lines.extend(
                [
                    "    MetricDescriptor {",
                    f"        name: metrics::{descriptor['symbol']},",
                    f"        kind: MetricKind::{descriptor['kind']},",
                    f'        unit: "{rust_string(descriptor["unit"])}",',
                    f"        labels: {label_sets[descriptor['labels']]},",
                    f"        source: MetricSource::{descriptor['source']},",
                    "    },",
                ]
            )
        lines.extend(["];", ""])
    return "\n".join(lines)


def run(*, check: bool) -> int:
    try:
        generated = render_catalog(load_catalog())
    except SchemaError as error:
        print(f"metric catalog schema error: {error}", file=sys.stderr)
        return 1
    if check:
        try:
            current = GENERATED_PATH.read_text(encoding="utf-8")
        except OSError as error:
            print(f"metric catalog generated output is missing: {error}", file=sys.stderr)
            return 1
        if current != generated:
            print(
                "metric catalog generated output drift: run python scripts/generate_metric_catalog.py",
                file=sys.stderr,
            )
            return 1
        print("metric catalog generated output is current")
        return 0
    GENERATED_PATH.parent.mkdir(parents=True, exist_ok=True)
    GENERATED_PATH.write_text(generated, encoding="utf-8", newline="\n")
    print(f"generated {GENERATED_PATH.relative_to(ROOT)}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail when generated Rust is stale")
    return run(check=parser.parse_args().check)


if __name__ == "__main__":
    raise SystemExit(main())
