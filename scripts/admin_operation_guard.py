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

"""Validate the Java 5.5 to RocketMQ-rust core Admin operation matrix."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
from pathlib import Path
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
MATRIX_PATH = ROOT / "scripts" / "admin-operation-matrix.json"
JAVA_INVENTORY_PATH = ROOT / "scripts" / "fixtures" / "java-5.5-core-inventory.json"
EXPECTED_EXCLUSIONS = frozenset({"AddBrokerSubCommand", "RemoveBrokerSubCommand"})
ACTIVE_STATUSES = frozenset({"equivalent", "alternative-equivalent", "missing", "placeholder"})
COMPLETE_STATUSES = frozenset({"equivalent", "alternative-equivalent"})
REQUIRED_ACTIVE_FIELDS = (
    "operation_id",
    "java_symbol",
    "java_command",
    "java_method",
    "java_source",
    "cli_command_id",
    "rust_cli_source",
    "rust_admin_core_methods",
    "handler_owners",
    "authorization",
    "typed_request",
    "typed_response",
    "expected_side_effects",
    "error_mapping",
    "test_id",
    "test_command",
    "status",
    "status_reason",
)


class GuardInputError(ValueError):
    """Raised when a guard input cannot be loaded."""


@dataclass(frozen=True, order=True)
class GuardFinding:
    code: str
    path: str
    detail: str

    def as_dict(self) -> dict[str, str]:
        return asdict(self)

    def render(self) -> str:
        return f"ADMIN_OPERATION_FINDING code={self.code} path={self.path} detail={self.detail}"


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise GuardInputError(f"cannot load {path}: {error}") from error
    if not isinstance(value, dict):
        raise GuardInputError(f"{path} must contain a JSON object")
    return value


def _finding(findings: list[GuardFinding], code: str, path: str, detail: object) -> None:
    findings.append(GuardFinding(code, path, str(detail)))


def _non_empty(value: object) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, list):
        return bool(value) and all(_non_empty(item) for item in value)
    if isinstance(value, dict):
        return bool(value) and all(_non_empty(item) for item in value.values())
    return value is not None


def _relative_file(value: object, root: Path) -> bool:
    if not isinstance(value, str) or not value or "\\" in value or ":" in value:
        return False
    path = Path(value)
    return not path.is_absolute() and ".." not in path.parts and (root / path).is_file()


def validate_matrix(
    matrix: dict[str, Any],
    java_inventory: dict[str, Any],
    *,
    root: Path = ROOT,
    require_complete: bool = False,
) -> list[GuardFinding]:
    findings: list[GuardFinding] = []
    if matrix.get("schema_version") != 1 or matrix.get("java_version") != "5.5.0":
        _finding(findings, "schema-invalid", "matrix", "schema_version=1 and java_version=5.5.0 are required")
    if matrix.get("scope") != "core-release":
        _finding(findings, "scope-invalid", "matrix.scope", repr(matrix.get("scope")))
    if matrix.get("counts") != {"raw": 96, "excluded": 2, "active": 94}:
        _finding(findings, "counts-invalid", "matrix.counts", repr(matrix.get("counts")))

    java_operations = java_inventory.get("admin_operations")
    operations = matrix.get("operations")
    if not isinstance(java_operations, list) or len(java_operations) != 96:
        _finding(findings, "java-denominator-invalid", "java.admin_operations", repr(java_operations))
        java_operations = []
    if not isinstance(operations, list) or any(not isinstance(item, dict) for item in operations):
        _finding(findings, "operations-invalid", "matrix.operations", "96 object entries are required")
        return sorted(set(findings))
    if len(operations) != 96:
        _finding(findings, "raw-count-invalid", "matrix.operations", len(operations))

    java_by_symbol = {item.get("symbol"): item for item in java_operations if isinstance(item, dict)}
    operation_ids: set[str] = set()
    java_symbols: set[str] = set()
    cli_ids: set[str] = set()
    test_ids: set[str] = set()
    exclusions: set[str] = set()
    active_count = 0

    for index, operation in enumerate(operations):
        path = f"operations[{index}]"
        operation_id = operation.get("operation_id")
        java_symbol = operation.get("java_symbol")
        classification = operation.get("classification")
        status = operation.get("status")

        for value, seen, code in (
            (operation_id, operation_ids, "operation-id-duplicate"),
            (java_symbol, java_symbols, "java-symbol-duplicate"),
        ):
            if not isinstance(value, str) or not value:
                _finding(findings, code.replace("duplicate", "missing"), path, repr(value))
            elif value in seen:
                _finding(findings, code, path, value)
            else:
                seen.add(value)

        java_operation = java_by_symbol.get(java_symbol)
        if java_operation is None:
            _finding(findings, "java-operation-missing", path, repr(java_symbol))
        else:
            for matrix_field, java_field in (("java_command", "command"), ("java_source", "source")):
                if operation.get(matrix_field) != java_operation.get(java_field):
                    _finding(
                        findings,
                        "java-operation-drift",
                        f"{path}.{matrix_field}",
                        f"matrix={operation.get(matrix_field)!r} java={java_operation.get(java_field)!r}",
                    )

        if classification == "excluded":
            exclusions.add(str(java_symbol))
            if status != "excluded" or operation.get("exclusion_reason") != "BrokerContainer":
                _finding(findings, "exclusion-invalid", path, f"status={status!r}")
            if operation.get("cli_command_id") is not None or operation.get("rust_admin_core_methods") != []:
                _finding(findings, "excluded-surface-exposed", path, repr(operation.get("cli_command_id")))
            continue

        if classification != "active":
            _finding(findings, "classification-invalid", path, repr(classification))
            continue
        active_count += 1
        test_id = operation.get("test_id")
        if not isinstance(test_id, str) or not test_id:
            _finding(findings, "test-id-missing", path, repr(test_id))
        elif test_id in test_ids:
            _finding(findings, "test-id-duplicate", path, test_id)
        else:
            test_ids.add(test_id)
        if status not in ACTIVE_STATUSES:
            _finding(findings, "status-invalid", f"{path}.status", repr(status))
        elif require_complete and status not in COMPLETE_STATUSES:
            _finding(findings, "active-operation-incomplete", f"{path}.status", f"{java_symbol}={status}")

        for field in REQUIRED_ACTIVE_FIELDS:
            if not _non_empty(operation.get(field)):
                _finding(findings, "active-field-missing", f"{path}.{field}", repr(operation.get(field)))

        cli_id = operation.get("cli_command_id")
        if isinstance(cli_id, str) and cli_id:
            if cli_id in cli_ids:
                _finding(findings, "cli-command-duplicate", f"{path}.cli_command_id", cli_id)
            cli_ids.add(cli_id)
        if not _relative_file(operation.get("rust_cli_source"), root):
            _finding(findings, "rust-cli-source-invalid", f"{path}.rust_cli_source", operation.get("rust_cli_source"))

    if active_count != 94:
        _finding(findings, "active-count-invalid", "matrix.operations", active_count)
    if exclusions != EXPECTED_EXCLUSIONS:
        _finding(findings, "exclusion-set-invalid", "matrix.operations", repr(sorted(exclusions)))
    if java_symbols != set(java_by_symbol):
        missing = sorted(set(java_by_symbol) - java_symbols)
        extra = sorted(java_symbols - set(java_by_symbol))
        _finding(findings, "java-denominator-drift", "matrix.operations", f"missing={missing} extra={extra}")
    return sorted(set(findings))


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", type=Path, default=MATRIX_PATH)
    parser.add_argument("--java-inventory", type=Path, default=JAVA_INVENTORY_PATH)
    parser.add_argument(
        "--require-complete",
        action="store_true",
        help="Reject active operations still classified as missing or placeholder.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        matrix = load_json(args.matrix)
        java_inventory = load_json(args.java_inventory)
    except GuardInputError as error:
        print(f"ADMIN_OPERATION_GUARD_INPUT_ERROR {error}", file=sys.stderr)
        return 2
    findings = validate_matrix(matrix, java_inventory, require_complete=args.require_complete)
    if findings:
        for finding in findings:
            print(finding.render())
        print(f"ADMIN_OPERATION_GUARD_FAILED findings={len(findings)}", file=sys.stderr)
        return 1
    status_counts: dict[str, int] = {}
    for operation in matrix["operations"]:
        status = operation["status"]
        status_counts[status] = status_counts.get(status, 0) + 1
    rendered_status = ",".join(f"{key}={status_counts[key]}" for key in sorted(status_counts))
    print(f"ADMIN_OPERATION_GUARD_OK raw=96 excluded=2 active=94 statuses={rendered_status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
