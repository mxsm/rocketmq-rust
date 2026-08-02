#!/usr/bin/env python3
#
# Copyright 2023 The RocketMQ Rust Authors
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

"""Enforce SAFETY comments and non-growth Rust hygiene inventories."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import NamedTuple

sys.path.insert(0, str(Path(__file__).resolve().parent))
import environment_write_guard as rust_source  # noqa: E402


ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / "scripts" / "rust-hygiene-baseline.json"

UNSAFE_REGION = re.compile(r"\bunsafe\s*(?:impl\b|\{)")
MANUAL_PIN = re.compile(r"\b(?:get_unchecked_mut|map_unchecked(?:_mut)?|Pin\s*::\s*new_unchecked)\b")
PANIC_SURFACE = re.compile(r"(?:\.\s*(unwrap|expect)\s*\(|\b(panic|unreachable)\s*!\s*\()")
PUBLIC_SAFE_FUNCTION = re.compile(
    r"\bpub\s+(?:(?:async|const|extern)\b\s*)*fn\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)",
    re.MULTILINE,
)
PUBLIC_TRAIT = re.compile(
    r"\bpub\s+(?:unsafe\s+)?trait\s+[A-Za-z_][A-Za-z0-9_]*",
    re.MULTILINE,
)
TRAIT_FUNCTION = re.compile(r"\bfn\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)", re.MULTILINE)
RAW_POINTER = re.compile(r"\*(?:const|mut)\b")
FUNCTION = re.compile(
    r"\b(?:async\s+)?(?:unsafe\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)",
    re.MULTILINE,
)
CFG_ATTRIBUTE = re.compile(r"#\s*\[\s*cfg\s*\((?P<body>.*?)\)\s*\]", re.DOTALL)
DEBT_FIELDS = {
    "identity",
    "path",
    "kind",
    "item",
    "line",
    "fingerprint",
    "classification",
    "owner",
    "reachability",
    "justification",
    "expiry",
}

REVIEWED_PANIC_INVARIANTS: dict[tuple[str, str], tuple[str, str]] = {
    (
        "rocketmq-broker/src/processor/admin_broker_processor/broker_config_request_handler.rs",
        "prepare_runtime_info",
    ): (
        "lifecycle_invariant",
        "broker initialization installs the message store before runtime diagnostics are served",
    ),
    ("rocketmq-namesrv/src/bootstrap.rs", "component_task_group"): (
        "lifecycle_invariant",
        "the injected NameServer service context always owns a component task group",
    ),
    ("rocketmq-runtime/src/resource_budget/budget.rs", "try_rebind"): (
        "resource_budget_invariant",
        "a successful budget rebind retains the validated root reservation",
    ),
    ("rocketmq-sre/crates/rocketmq-sre-control-plane/src/openapi.rs", "<module>"): (
        "checked_artifact_invariant",
        "the checked-in OpenAPI document is validated by repository tests before release",
    ),
    ("rocketmq-store-api/src/ha_contract.rs", "try_new"): (
        "validated_constructor_invariant",
        "ReplicaCount validates that the value is at least two before constructing NonZeroUsize",
    ),
    ("rocketmq-store/src/runtime.rs", "new"): (
        "resource_budget_invariant",
        "Store runtime child budgets are derived from a validated parent budget configuration",
    ),
    (
        "rocketmq-tools/rocketmq-admin/rocketmq-admin-core/src/client_adapter/mutation.rs",
        "inner",
    ): (
        "documented_state_invariant",
        "MutationAdminGuard documents that access after the guard is consumed is a programmer error",
    ),
    (
        "rocketmq-tools/rocketmq-admin/rocketmq-admin-core/src/client_adapter/mutation.rs",
        "inner_mut",
    ): (
        "documented_state_invariant",
        "MutationAdminGuard documents that access after the guard is consumed is a programmer error",
    ),
    ("rocketmq-tools/rocketmq-mcp/src/guard/sanitizer.rs", "<module>"): (
        "static_definition_invariant",
        "literal sanitizer regular expressions are compiled once and covered by sanitizer tests",
    ),
    ("rocketmq-transport/src/clients/rocketmq_tokio_client.rs", "new_with_cl_and_telemetry"): (
        "resource_budget_invariant",
        "the transport request budget is clamped before constructing its non-zero limit",
    ),
}


class SafetyFinding(NamedTuple):
    path: str
    line: int
    reason: str


def is_test_only(offset: int, ranges: list[tuple[int, int]]) -> bool:
    return any(start <= offset < end for start, end in ranges)


def is_test_source_path(relative: str) -> bool:
    path = Path(relative)
    stem = path.stem.lower()
    return (
        "tests" in {part.lower() for part in path.parts}
        or stem == "tests"
        or stem.startswith("test_")
        or stem.endswith("_test")
        or stem.endswith("_tests")
    )


def split_cfg_arguments(arguments: str) -> list[str]:
    result: list[str] = []
    depth = 0
    start = 0
    for index, character in enumerate(arguments):
        if character == "(":
            depth += 1
        elif character == ")":
            depth = max(0, depth - 1)
        elif character == "," and depth == 0:
            result.append(arguments[start:index].strip())
            start = index + 1
    result.append(arguments[start:].strip())
    return [argument for argument in result if argument]


def cfg_requires_test(expression: str) -> bool:
    expression = expression.strip()
    if expression == "test" or re.fullmatch(r'feature\s*=\s*"test-support"', expression):
        return True
    match = re.fullmatch(r"(?P<operator>all|any)\s*\((?P<arguments>.*)\)", expression, re.DOTALL)
    if match is None:
        return False
    requirements = [cfg_requires_test(argument) for argument in split_cfg_arguments(match.group("arguments"))]
    if not requirements:
        return False
    if match.group("operator") == "all":
        return any(requirements)
    return all(requirements)


def cfg_test_item_ranges(masked: str, source: str | None = None) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    for match in CFG_ATTRIBUTE.finditer(masked):
        body = (
            source[match.start("body"):match.end("body")]
            if source is not None
            else match.group("body")
        )
        if not cfg_requires_test(body):
            continue
        cursor = match.end()
        while True:
            whitespace = re.match(r"\s*", masked[cursor:])
            cursor += len(whitespace.group(0)) if whitespace else 0
            if not masked.startswith("#[", cursor):
                break
            attribute_end = masked.find("]", cursor + 2)
            if attribute_end == -1:
                break
            cursor = attribute_end + 1

        opening = masked.find("{", cursor)
        semicolon = masked.find(";", cursor)
        if semicolon != -1 and (opening == -1 or semicolon < opening):
            ranges.append((match.start(), semicolon + 1))
            continue
        if opening == -1:
            continue
        depth = 0
        for index in range(opening, len(masked)):
            if masked[index] == "{":
                depth += 1
            elif masked[index] == "}":
                depth -= 1
                if depth == 0:
                    ranges.append((match.start(), index + 1))
                    break
    return ranges


def brace_depths(masked: str) -> list[int]:
    depths = [0] * (len(masked) + 1)
    depth = 0
    for index, character in enumerate(masked):
        depths[index] = depth
        if character == "{":
            depth += 1
        elif character == "}":
            depth = max(0, depth - 1)
    depths[len(masked)] = depth
    return depths


def matching_delimiter(masked: str, opening: int, opener: str, closer: str) -> int | None:
    depth = 0
    for index in range(opening, len(masked)):
        character = masked[index]
        if character == opener:
            depth += 1
        elif character == closer:
            depth -= 1
            if depth == 0:
                return index
    return None


def matching_generic_delimiter(masked: str, opening: int) -> int | None:
    angle_depth = 0
    nested_depth = 0
    for index in range(opening, len(masked)):
        character = masked[index]
        if character in "([{":
            nested_depth += 1
        elif character in ")]}":
            nested_depth = max(0, nested_depth - 1)
        elif nested_depth == 0 and character == "<":
            angle_depth += 1
        elif nested_depth == 0 and character == ">" and masked[index - 1] != "-":
            angle_depth -= 1
            if angle_depth == 0:
                return index
    return None


def function_parameters(masked: str, name_end: int) -> str | None:
    cursor = name_end
    while cursor < len(masked) and masked[cursor].isspace():
        cursor += 1
    if cursor < len(masked) and masked[cursor] == "<":
        closing = matching_generic_delimiter(masked, cursor)
        if closing is None:
            return None
        cursor = closing + 1
        while cursor < len(masked) and masked[cursor].isspace():
            cursor += 1
    if cursor >= len(masked) or masked[cursor] != "(":
        return None
    closing = matching_delimiter(masked, cursor, "(", ")")
    if closing is None:
        return None
    return masked[cursor + 1:closing]


def public_trait_body_ranges(masked: str) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    for match in PUBLIC_TRAIT.finditer(masked):
        opening = masked.find("{", match.end())
        if opening == -1:
            continue
        closing = matching_delimiter(masked, opening, "{", "}")
        if closing is not None:
            ranges.append((opening, closing))
    return ranges


def trait_function_is_unsafe(masked: str, trait_opening: int, function_start: int) -> bool:
    boundary = max(
        trait_opening,
        masked.rfind(";", trait_opening + 1, function_start),
        masked.rfind("{", trait_opening + 1, function_start),
        masked.rfind("}", trait_opening + 1, function_start),
    )
    return re.search(r"\bunsafe\b", masked[boundary + 1:function_start]) is not None


def public_safe_functions(masked: str) -> list[tuple[int, str, str]]:
    functions: list[tuple[int, str, str]] = []
    for match in PUBLIC_SAFE_FUNCTION.finditer(masked):
        parameters = function_parameters(masked, match.end())
        if parameters is not None:
            functions.append((match.start(), match.group("name"), parameters))

    depths = brace_depths(masked)
    for opening, closing in public_trait_body_ranges(masked):
        item_depth = depths[opening] + 1
        for match in TRAIT_FUNCTION.finditer(masked, opening + 1, closing):
            if depths[match.start()] != item_depth or trait_function_is_unsafe(masked, opening, match.start()):
                continue
            parameters = function_parameters(masked, match.end())
            if parameters is not None:
                functions.append((match.start(), match.group("name"), parameters))
    return functions


def preceding_safety_comment(source: str, offset: int) -> bool:
    current_line_start = source.rfind("\n", 0, offset) + 1
    lines = source[:current_line_start].splitlines()
    for line in reversed(lines[-10:]):
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("//"):
            if stripped.startswith("// SAFETY:"):
                return True
            continue
        if stripped.startswith("#["):
            continue
        return False
    return False


def enclosing_function(masked: str, offset: int) -> str:
    matches = list(FUNCTION.finditer(masked, 0, offset))
    return matches[-1].group(1) if matches else "<module>"


def normalized_line(masked: str, offset: int) -> str:
    start = masked.rfind("\n", 0, offset) + 1
    end = masked.find("\n", offset)
    if end == -1:
        end = len(masked)
    return re.sub(r"\s+", "", masked[start:end])


def debt_entry(relative: str, kind: str, masked: str, source: str, offset: int) -> dict[str, object]:
    line = source.count("\n", 0, offset) + 1
    item = enclosing_function(masked, offset)
    snippet = normalized_line(masked, offset)
    fingerprint = hashlib.sha256(f"{relative}\0{kind}\0{item}\0{snippet}".encode()).hexdigest()[:20]
    classification = {
        "panic_surface": "internal_invariant",
        "manual_pin": "unsafe_invariant",
        "legacy_mod_rs": "legacy_layout",
    }.get(kind, "reviewed_legacy")
    justification = "reviewed legacy identity; additions are forbidden and deletion is monotonic"
    if kind == "panic_surface" and (relative, item) in REVIEWED_PANIC_INVARIANTS:
        classification, justification = REVIEWED_PANIC_INVARIANTS[(relative, item)]
    return {
        "identity": f"{relative}:{kind}:{item}:{fingerprint}",
        "path": relative,
        "kind": kind,
        "item": item,
        "line": line,
        "fingerprint": fingerprint,
        "classification": classification,
        "owner": relative.split("/", 1)[0],
        "reachability": "production-internal",
        "justification": justification,
        "expiry": "2.0.0",
    }


def scan_source(source: str, relative: str) -> tuple[list[SafetyFinding], list[dict[str, object]]]:
    if is_test_source_path(relative):
        return [], []
    masked = rust_source.mask_comments_and_literals(source)
    test_ranges = rust_source.test_module_ranges(masked) + cfg_test_item_ranges(masked, source)
    safety_findings: list[SafetyFinding] = []
    debt: list[dict[str, object]] = []

    for offset, name, parameters in public_safe_functions(masked):
        if is_test_only(offset, test_ranges):
            continue
        if RAW_POINTER.search(parameters):
            safety_findings.append(
                SafetyFinding(
                    relative,
                    source.count("\n", 0, offset) + 1,
                    f"safe public function {name} accepts a raw pointer",
                )
            )

    for match in UNSAFE_REGION.finditer(masked):
        if is_test_only(match.start(), test_ranges):
            continue
        if not preceding_safety_comment(source, match.start()):
            safety_findings.append(
                SafetyFinding(
                    relative,
                    source.count("\n", 0, match.start()) + 1,
                    "unsafe region requires an adjacent // SAFETY: comment",
                )
            )

    for kind, pattern in (("manual_pin", MANUAL_PIN), ("panic_surface", PANIC_SURFACE)):
        duplicates: Counter[str] = Counter()
        for match in pattern.finditer(masked):
            if is_test_only(match.start(), test_ranges):
                continue
            entry = debt_entry(relative, kind, masked, source, match.start())
            base_identity = str(entry["identity"])
            ordinal = duplicates[base_identity]
            duplicates[base_identity] += 1
            entry["identity"] = f"{base_identity}:{ordinal}"
            debt.append(entry)

    return safety_findings, debt


def scan_tree(root: Path) -> tuple[list[SafetyFinding], list[dict[str, object]]]:
    safety_findings: list[SafetyFinding] = []
    debt: list[dict[str, object]] = []
    for path in rust_source.production_sources(root):
        relative = path.relative_to(root).as_posix()
        file_safety, file_debt = scan_source(path.read_text(encoding="utf-8"), relative)
        safety_findings.extend(file_safety)
        debt.extend(file_debt)

    for path in sorted(root.rglob("mod.rs")):
        relative_path = path.relative_to(root)
        if "target" in relative_path.parts or "src" not in relative_path.parts:
            continue
        relative = relative_path.as_posix()
        fingerprint = hashlib.sha256(relative.encode()).hexdigest()[:20]
        debt.append(
            {
                "identity": f"{relative}:legacy_mod_rs:{fingerprint}:0",
                "path": relative,
                "kind": "legacy_mod_rs",
                "item": "<module>",
                "line": 1,
                "fingerprint": fingerprint,
                "classification": "legacy module layout; additions are forbidden",
                "owner": "owning crate maintainers",
                "reachability": "production-layout",
                "justification": "legacy module identity retained only until its owning module is touched",
                "expiry": "2.0.0",
            }
        )

    return safety_findings, sorted(debt, key=lambda entry: str(entry["identity"]))


def write_baseline(path: Path, debt: list[dict[str, object]]) -> None:
    payload = {
        "schema_version": 2,
        "policy": {
            "unsafe": "Every production unsafe block or impl requires an adjacent // SAFETY: comment.",
            "safe_raw_pointer_api": "Public safe functions must not accept raw pointers.",
            "debt": "Existing panic surfaces, manual Pin projection, and mod.rs files may be deleted but not added.",
        },
        "entries": debt,
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def load_baseline(path: Path) -> set[str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != 2 or not isinstance(payload.get("entries"), list):
        raise ValueError("rust hygiene baseline has an unsupported schema")
    for entry in payload["entries"]:
        if not isinstance(entry, dict) or set(entry) != DEBT_FIELDS:
            raise ValueError("rust hygiene baseline contains an invalid debt entry")
        if any(not isinstance(entry[field], str) or not entry[field] for field in DEBT_FIELDS - {"line"}):
            raise ValueError("rust hygiene baseline contains empty debt metadata")
        if not isinstance(entry["line"], int) or entry["line"] < 1:
            raise ValueError("rust hygiene baseline contains an invalid line")
    identities = [entry.get("identity") for entry in payload["entries"]]
    if any(not isinstance(identity, str) or not identity for identity in identities):
        raise ValueError("rust hygiene baseline contains an invalid identity")
    if len(identities) != len(set(identities)):
        raise ValueError("rust hygiene baseline contains duplicate identities")
    return set(identities)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--baseline", type=Path, default=BASELINE)
    parser.add_argument("--write-baseline", action="store_true")
    args = parser.parse_args()

    root = args.root.resolve()
    safety_findings, debt = scan_tree(root)
    if safety_findings:
        for finding in safety_findings:
            print(f"{finding.path}:{finding.line}: {finding.reason}", file=sys.stderr)
        print(f"RUST_HYGIENE_GUARD_FAILED hygiene_findings={len(safety_findings)}", file=sys.stderr)
        return 1

    if args.write_baseline:
        write_baseline(args.baseline, debt)
        print(f"RUST_HYGIENE_BASELINE_WRITTEN entries={len(debt)} path={args.baseline}")
        return 0

    try:
        baseline = load_baseline(args.baseline)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"RUST_HYGIENE_GUARD_FAILED baseline={error}", file=sys.stderr)
        return 1

    current = {str(entry["identity"]): entry for entry in debt}
    additions = [current[identity] for identity in sorted(current.keys() - baseline)]
    if additions:
        for entry in additions:
            print(
                f"{entry['path']}:{entry['line']}: new {entry['kind']} occurrence "
                f"in {entry['item']}",
                file=sys.stderr,
            )
        print(f"RUST_HYGIENE_GUARD_FAILED new_debt={len(additions)}", file=sys.stderr)
        return 1

    counts = Counter(str(entry["kind"]) for entry in debt)
    reviewed_invariants = sum(
        1
        for entry in debt
        if (str(entry["path"]), str(entry["item"])) in REVIEWED_PANIC_INVARIANTS
    )
    excluded_test_sources = sum(
        1
        for path in rust_source.production_sources(root)
        if is_test_source_path(path.relative_to(root).as_posix())
    )
    print(
        "RUST_HYGIENE_GUARD_OK "
        f"manual_pin={counts['manual_pin']} "
        f"panic_surface={counts['panic_surface']} "
        f"legacy_mod_rs={counts['legacy_mod_rs']} "
        f"reviewed_invariants={reviewed_invariants} "
        f"excluded_test_sources={excluded_test_sources}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
