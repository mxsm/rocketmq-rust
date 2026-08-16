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

"""Generate deterministic Java 5.5 Admin behavior golden fixtures."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MATRIX = ROOT / "scripts" / "admin-operation-matrix.json"
DEFAULT_OUTPUT = ROOT / "scripts" / "fixtures" / "admin-java-55" / "operation-goldens.json"

ERROR_EXIT_CODES = {
    "invalid-input": 64,
    "not-found": 66,
    "partial-failure": 70,
    "timeout": 75,
    "permission": 77,
}
ERROR_CODES = {
    "invalid-input": "ILLEGAL_ARGUMENT",
    "not-found": "QUERY_NOT_FOUND",
    "partial-failure": "BROKER_OPERATION_FAILED",
    "timeout": "TIMEOUT",
    "permission": "BROKER_PERMISSION_DENIED",
}
ERROR_CYCLES = {
    "read-only-query": ("not-found", "timeout", "permission", "invalid-input"),
    "remote-state-mutation": ("permission", "partial-failure", "invalid-input", "timeout", "not-found"),
    "local-artifact-write": ("invalid-input", "permission", "timeout", "partial-failure"),
    "message-io": ("timeout", "partial-failure", "not-found", "permission", "invalid-input"),
}
SUCCESS_STATES = {
    "read-only-query": ("baseline", "baseline", True),
    "remote-state-mutation": ("baseline", "desired", True),
    "local-artifact-write": ("absent", "present", True),
    "message-io": ("pending", "processed", False),
}


def read_contract(operation: dict[str, Any]) -> dict[str, str] | None:
    if operation.get("expected_side_effects", {}).get("class") != "read-only-query":
        return None
    return {
        "ordering": "stable",
        "pagination": "per-broker-last-key"
        if operation.get("cli_command_id") == "message.queryMsgByKey"
        else "not-applicable",
        "empty_result": "typed-empty",
        "partial_target_failure": "preserve-successes-and-warnings-when-multi-target",
    }


class GoldenInputError(ValueError):
    """Raised when the Admin matrix cannot produce a complete fixture."""


def load_matrix(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise GoldenInputError(f"cannot load {path}: {error}") from error
    if not isinstance(value, dict):
        raise GoldenInputError(f"{path} must contain a JSON object")
    return value


def generate_goldens(matrix: dict[str, Any]) -> dict[str, Any]:
    if matrix.get("java_version") != "5.5.0" or matrix.get("scope") != "core-release":
        raise GoldenInputError("Admin matrix must describe the Java 5.5 core-release scope")
    active = [operation for operation in matrix.get("operations", []) if operation.get("classification") == "active"]
    if len(active) != 94:
        raise GoldenInputError(f"Admin matrix must contain 94 active operations, found {len(active)}")

    error_indexes = {effect: 0 for effect in ERROR_CYCLES}
    operations = []
    for operation in active:
        effect = operation.get("expected_side_effects", {}).get("class")
        if effect not in ERROR_CYCLES:
            raise GoldenInputError(f"unsupported side-effect class for {operation.get('operation_id')}: {effect!r}")
        error_cycle = ERROR_CYCLES[effect]
        error_kind = error_cycle[error_indexes[effect] % len(error_cycle)]
        error_indexes[effect] += 1
        state_before, state_after, idempotent = SUCCESS_STATES[effect]
        test_id = operation["test_id"]
        partial_failure = error_kind == "partial-failure"
        scenarios = [
            {
                "scenario_id": f"{test_id}-success",
                "case": "success",
                "outcome": "success",
                "error_kind": None,
                "expected_error_code": None,
                "expected_exit_code": 0,
                "state_before": state_before,
                "state_after": state_after,
                "idempotent": idempotent,
                "partial_failure": False,
                "retry_boundary": "none",
                "result_shape": "ordered-nonempty" if effect == "read-only-query" else "not-applicable",
            },
            {
                "scenario_id": f"{test_id}-{error_kind}",
                "case": "error",
                "outcome": "error",
                "error_kind": error_kind,
                "expected_error_code": ERROR_CODES[error_kind],
                "expected_exit_code": ERROR_EXIT_CODES[error_kind],
                "state_before": state_before,
                "state_after": "partially-applied" if partial_failure else state_before,
                "idempotent": False,
                "partial_failure": partial_failure,
                "retry_boundary": "bounded" if error_kind == "timeout" else "none",
                "result_shape": "not-applicable",
            },
        ]
        if effect == "read-only-query":
            scenarios.extend(
                [
                    {
                        "scenario_id": f"{test_id}-empty",
                        "case": "empty",
                        "outcome": "success",
                        "error_kind": None,
                        "expected_error_code": None,
                        "expected_exit_code": 0,
                        "state_before": state_before,
                        "state_after": state_before,
                        "idempotent": True,
                        "partial_failure": False,
                        "retry_boundary": "none",
                        "result_shape": "empty",
                    },
                    {
                        "scenario_id": f"{test_id}-partial-failure",
                        "case": "partial-failure",
                        "outcome": "error",
                        "error_kind": "partial-failure",
                        "expected_error_code": ERROR_CODES["partial-failure"],
                        "expected_exit_code": ERROR_EXIT_CODES["partial-failure"],
                        "state_before": state_before,
                        "state_after": state_before,
                        "idempotent": False,
                        "partial_failure": True,
                        "retry_boundary": "none",
                        "result_shape": "ordered-partial",
                    },
                ]
            )
        operations.append(
            {
                "operation_id": operation["operation_id"],
                "test_id": test_id,
                "cli_command_id": operation["cli_command_id"],
                "java_request_codes": operation["java_request_codes"],
                "typed_request": operation["typed_request"],
                "typed_response": operation["typed_response"],
                "authorization": operation["authorization"],
                "side_effect_class": effect,
                "read_contract": read_contract(operation),
                "scenarios": scenarios,
            }
        )

    return {
        "schema_version": 1,
        "java_version": "5.5.0",
        "scope": "core-release",
        "counts": {"operations": len(operations), "scenarios": sum(len(item["scenarios"]) for item in operations)},
        "operations": operations,
    }


def render(value: dict[str, Any]) -> str:
    return json.dumps(value, indent=2, ensure_ascii=False) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", type=Path, default=DEFAULT_MATRIX)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--check", action="store_true")
    action.add_argument("--write", action="store_true")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    try:
        rendered = render(generate_goldens(load_matrix(args.matrix)))
    except GoldenInputError as error:
        print(f"ADMIN_GOLDEN_GENERATION_FAILED detail={error}", file=sys.stderr)
        return 1

    if args.check:
        try:
            existing = args.output.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as error:
            print(f"ADMIN_GOLDEN_CHECK_FAILED detail={error}", file=sys.stderr)
            return 1
        if existing != rendered:
            print("ADMIN_GOLDEN_CHECK_FAILED detail=generated fixture differs from committed fixture", file=sys.stderr)
            return 1
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("w", encoding="utf-8", newline="\n") as output:
            output.write(rendered)
    print("ADMIN_GOLDEN_OK operations=94 scenarios=278")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
