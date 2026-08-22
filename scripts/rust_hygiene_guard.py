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
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import NamedTuple

sys.path.insert(0, str(Path(__file__).resolve().parent))
import environment_write_guard as rust_source  # noqa: E402
import core_release_scope  # noqa: E402
import rust_production_sources  # noqa: E402


ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / "scripts" / "rust-hygiene-baseline.json"

UNSAFE_REGION = re.compile(r"\bunsafe\s*(?:impl\b|\{)")
UNSAFE_FORM = re.compile(r"\bunsafe\s+(?P<form>fn|trait|impl|extern)\b|\bunsafe\s*(?P<block>\{)")
MANUAL_PIN = re.compile(r"\b(?:get_unchecked_mut|map_unchecked(?:_mut)?|Pin\s*::\s*new_unchecked)\b")
PANIC_SURFACE = re.compile(r"(?:\.\s*(unwrap|expect)\s*\(|\b(panic|unreachable)\s*!\s*\()")
USE_STATEMENT = re.compile(
    r"\b(?:pub(?:\s*\([^)]*\))?\s+)?use\s+(?P<body>[^;]+);",
    re.MULTILINE,
)
LEGACY_RUNTIME = re.compile(r"\bRocketMQRuntime\b")
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
    "classification",
    "owner",
    "reachability",
    "justification",
    "expiry",
}
UNSAFE_DEBT_FIELDS = {"ordinal"}
UNSAFE_DEBT_KINDS = {"unsafe_block", "unsafe_fn", "unsafe_impl", "unsafe_trait", "unsafe_extern"}
PROTOCOL_PREFIX = "rocketmq-protocol/"
BASELINE_POLICY = {
    "unsafe": "Every production unsafe block or impl requires an adjacent // SAFETY: comment; protocol unsafe identities may not grow.",
    "safe_raw_pointer_api": "Public safe functions must not accept raw pointers.",
    "debt": "Existing panic surfaces, manual Pin projection, protocol unsafe regions, and mod.rs files may be deleted but not added.",
    "legacy_runtime": "Non-canonical production RocketMQRuntime use is forbidden.",
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
        "rocketmq-sre/crates/rocketmq-sre-connector/src/sources/projection.rs",
        "apply",
    ): (
        "validated_dispatch_invariant",
        "diagnostic projections are routed before the canonical-only projection match",
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


def use_leaves(tree: str, prefix: tuple[str, ...] = ()) -> list[tuple[tuple[str, ...], str]]:
    """Flatten the small use-tree subset needed for panic macro aliases."""

    tree = re.sub(r"\br#", "", tree.strip()).removeprefix("::")
    opening = tree.find("{")
    if opening >= 0 and tree.endswith("}"):
        base = tuple(part.strip() for part in tree[:opening].rstrip(":").split("::") if part.strip())
        return [
            leaf
            for branch in tree[opening + 1 : -1].split(",")
            for leaf in use_leaves(branch, prefix + base)
        ]
    parts = re.split(r"\s+as\s+", tree, maxsplit=1)
    path = prefix + tuple(part.strip() for part in parts[0].split("::") if part.strip())
    local = parts[1].strip() if len(parts) == 2 else (path[-1] if path else "")
    return [(path, local)] if path and local not in {"", "_", "*"} else []


def panic_surface_offsets(masked: str) -> list[int]:
    """Return direct panic surfaces plus practical std/core alias declarations and calls."""

    offsets = {match.start() for match in PANIC_SURFACE.finditer(masked)}
    bindings = [
        (path, local, statement.start())
        for statement in USE_STATEMENT.finditer(masked)
        for path, local in use_leaves(statement.group("body"))
    ]
    aliases: set[str] = set()
    changed = True
    while changed:
        changed = False
        for path, local, offset in bindings:
            source = path[-1]
            direct = source in {"panic", "unreachable"} and (
                len(path) == 1 or path[0] in {"std", "core"}
            )
            chained = source in aliases and (len(path) == 1 or path[0] in {"self", "super", "crate"})
            if (direct or chained) and local not in aliases:
                aliases.add(local)
                offsets.add(offset)
                changed = True
    for alias in aliases - {"panic", "unreachable"}:
        offsets.update(
            match.start()
            for match in re.finditer(rf"(?<![\w])(?:r#)?{re.escape(alias)}\s*!\s*[({{\[]", masked)
        )
    return sorted(offsets)


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


def legacy_runtime_offsets(masked: str, relative: str) -> list[int]:
    """Reject runtime references outside the legacy definition and root re-export."""

    allowed: list[tuple[int, int]] = []
    if relative == "rocketmq-runtime/src/legacy.rs":
        for declaration in re.finditer(r"\b(?:pub\s+enum|impl)\s+RocketMQRuntime\b", masked):
            opening = masked.find("{", declaration.end())
            closing = matching_delimiter(masked, opening, "{", "}") if opening >= 0 else None
            if closing is not None:
                allowed.append((declaration.start(), closing + 1))
    elif relative == "rocketmq-runtime/src/lib.rs":
        reexport = re.search(r"\bpub\s+use\s+legacy\s*::\s*RocketMQRuntime\s*;", masked)
        if reexport is not None:
            allowed.append(reexport.span())
    return [
        match.start()
        for match in LEGACY_RUNTIME.finditer(masked)
        if not any(start <= match.start() < end for start, end in allowed)
    ]


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


def unsafe_occurrences(masked: str) -> list[tuple[int, str, str, int]]:
    """Classify production unsafe forms with stable owner-local ordinals."""

    found: list[tuple[int, str, str, int]] = []
    ordinals: Counter[tuple[str, str]] = Counter()
    for match in UNSAFE_FORM.finditer(masked):
        form = match.group("form") or "block"
        kind = f"unsafe_{form}"
        owner = enclosing_function(masked, match.start())
        if form in {"fn", "trait"}:
            name = re.match(r"\s+([A-Za-z_][A-Za-z0-9_]*)", masked[match.end() :])
            owner = name.group(1) if name is not None else "<unknown>"
        elif form == "impl":
            opening = masked.find("{", match.end())
            header = masked[match.end() : opening if opening >= 0 else match.end()]
            owner = re.sub(r"\s+", "", header) or "<unknown>"
        elif form == "extern":
            owner = "<module>"
        key = (kind, owner)
        ordinal = ordinals[key]
        ordinals[key] += 1
        found.append((match.start(), kind, owner, ordinal))
    return found


def unsafe_debt_entry(
    relative: str, source: str, offset: int, kind: str, owner: str, ordinal: int
) -> dict[str, object]:
    return {
        "identity": f"{relative}:{kind}:{owner}:{ordinal}",
        "path": relative,
        "kind": kind,
        "item": owner,
        "line": source.count("\n", 0, offset) + 1,
        "classification": "unsafe_invariant",
        "owner": owner,
        "ordinal": ordinal,
        "reachability": "production-internal",
        "justification": "reviewed protocol unsafe identity; additions are forbidden and deletion is monotonic",
        "expiry": "2.0.0",
    }


def debt_entry(relative: str, kind: str, masked: str, source: str, offset: int) -> dict[str, object]:
    line = source.count("\n", 0, offset) + 1
    item = enclosing_function(masked, offset)
    classification = {
        "panic_surface": "internal_invariant",
        "manual_pin": "unsafe_invariant",
        "legacy_mod_rs": "legacy_layout",
    }.get(kind, "reviewed_legacy")
    justification = "reviewed legacy identity; additions are forbidden and deletion is monotonic"
    if kind == "panic_surface" and (relative, item) in REVIEWED_PANIC_INVARIANTS:
        classification, justification = REVIEWED_PANIC_INVARIANTS[(relative, item)]
    return {
        "identity": f"{relative}:{kind}:{item}",
        "path": relative,
        "kind": kind,
        "item": item,
        "line": line,
        "classification": classification,
        "owner": relative.split("/", 1)[0],
        "reachability": "production-internal",
        "justification": justification,
        "expiry": "2.0.0",
    }


def scan_source(
    source: str, relative: str, *, production_reachable: bool = False
) -> tuple[list[SafetyFinding], list[dict[str, object]]]:
    if not production_reachable and is_test_source_path(relative):
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

    for offset, kind, owner, ordinal in unsafe_occurrences(masked):
        if is_test_only(offset, test_ranges):
            continue
        if relative.startswith(PROTOCOL_PREFIX):
            debt.append(unsafe_debt_entry(relative, source, offset, kind, owner, ordinal))

    for offset in legacy_runtime_offsets(masked, relative):
        if not is_test_only(offset, test_ranges):
            safety_findings.append(
                SafetyFinding(
                    relative,
                    source.count("\n", 0, offset) + 1,
                    "non-canonical production RocketMQRuntime use is forbidden",
                )
            )

    for kind, offsets in (
        ("manual_pin", (match.start() for match in MANUAL_PIN.finditer(masked))),
        ("panic_surface", iter(panic_surface_offsets(masked))),
    ):
        duplicates: Counter[str] = Counter()
        for offset in offsets:
            if is_test_only(offset, test_ranges):
                continue
            entry = debt_entry(relative, kind, masked, source, offset)
            base_identity = str(entry["identity"])
            ordinal = duplicates[base_identity]
            duplicates[base_identity] += 1
            entry["identity"] = f"{base_identity}:{ordinal}"
            debt.append(entry)

    return safety_findings, debt


def scan_tree(
    root: Path,
    *,
    scope: str = "all",
) -> tuple[list[SafetyFinding], list[dict[str, object]]]:
    safety_findings: list[SafetyFinding] = []
    debt: list[dict[str, object]] = []
    scope_document = (
        core_release_scope.load_scope(root / "scripts/core-release-scope.json")
        if scope != "all"
        else None
    )
    root_filter = (
        (lambda relative: core_release_scope.path_in_scope(relative, scope, scope_document))
        if scope_document is not None
        else None
    )
    sources, discovery_findings = rust_production_sources.production_sources(
        root, cfg_requires_test, root_filter
    )
    safety_findings.extend(
        SafetyFinding(finding.path, finding.line, finding.reason)
        for finding in discovery_findings
    )
    for path in sources:
        relative = path.relative_to(root).as_posix()
        if scope_document is not None and not core_release_scope.path_in_scope(
            relative, scope, scope_document
        ):
            continue
        file_safety, file_debt = scan_source(
            path.read_text(encoding="utf-8"), relative, production_reachable=True
        )
        safety_findings.extend(file_safety)
        debt.extend(file_debt)

    for path in (path for path in sources if path.name == "mod.rs"):
        relative_path = path.relative_to(root)
        relative = relative_path.as_posix()
        if scope_document is not None and not core_release_scope.path_in_scope(
            relative, scope, scope_document
        ):
            continue
        debt.append(
            {
                "identity": f"{relative}:legacy_mod_rs:<module>:0",
                "path": relative,
                "kind": "legacy_mod_rs",
                "item": "<module>",
                "line": 1,
                "classification": "legacy module layout; additions are forbidden",
                "owner": "owning crate maintainers",
                "reachability": "production-layout",
                "justification": "legacy module identity retained only until its owning module is touched",
                "expiry": "2.0.0",
            }
        )

    return safety_findings, sorted(debt, key=lambda entry: str(entry["identity"]))


def load_baseline(path: Path, *, scope: str = "all", root: Path = ROOT) -> set[str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if (
        payload.get("schema_version") != 3
        or payload.get("policy") != BASELINE_POLICY
        or not isinstance(payload.get("entries"), list)
    ):
        raise ValueError("rust hygiene baseline has an unsupported schema")
    for entry in payload["entries"]:
        unsafe = isinstance(entry, dict) and entry.get("kind") in UNSAFE_DEBT_KINDS
        expected = DEBT_FIELDS | (UNSAFE_DEBT_FIELDS if unsafe else set())
        if not isinstance(entry, dict) or set(entry) != expected:
            raise ValueError("rust hygiene baseline contains an invalid debt entry")
        if any(not isinstance(entry[field], str) or not entry[field] for field in DEBT_FIELDS - {"line"}):
            raise ValueError("rust hygiene baseline contains empty debt metadata")
        if not isinstance(entry["line"], int) or entry["line"] < 1:
            raise ValueError("rust hygiene baseline contains an invalid line")
        if unsafe and (not isinstance(entry["ordinal"], int) or entry["ordinal"] < 0):
            raise ValueError("rust hygiene baseline contains an invalid unsafe ordinal")
    scope_document = core_release_scope.load_scope(root / "scripts/core-release-scope.json")
    identities = [
        entry.get("identity")
        for entry in payload["entries"]
        if core_release_scope.path_in_scope(entry["path"], scope, scope_document)
    ]
    if any(not isinstance(identity, str) or not identity for identity in identities):
        raise ValueError("rust hygiene baseline contains an invalid identity")
    if len(identities) != len(set(identities)):
        raise ValueError("rust hygiene baseline contains duplicate identities")
    return set(identities)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--baseline", type=Path, default=BASELINE)
    parser.add_argument("--write-baseline", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument(
        "--scope",
        choices=("core-release", "repo-global", "all"),
        default="all",
    )
    parser.add_argument("--identity", choices=("structural",), default="structural")
    args = parser.parse_args()

    root = args.root.resolve()
    if args.write_baseline:
        print(
            "RUST_HYGIENE_GUARD_FAILED whole-baseline rewrites are disabled; review identities individually",
            file=sys.stderr,
        )
        return 2
    safety_findings, debt = scan_tree(root, scope=args.scope)
    if safety_findings:
        for finding in safety_findings:
            print(f"{finding.path}:{finding.line}: {finding.reason}", file=sys.stderr)
        print(f"RUST_HYGIENE_GUARD_FAILED hygiene_findings={len(safety_findings)}", file=sys.stderr)
        return 1

    try:
        baseline = load_baseline(args.baseline, scope=args.scope, root=root)
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
    protocol_unsafe = sum(counts[kind] for kind in UNSAFE_DEBT_KINDS)
    print(
        "RUST_HYGIENE_GUARD_OK "
        f"manual_pin={counts['manual_pin']} "
        f"protocol_unsafe={protocol_unsafe} "
        f"panic_surface={counts['panic_surface']} "
        f"legacy_mod_rs={counts['legacy_mod_rs']} "
        f"reviewed_invariants={reviewed_invariants} excluded_test_sources=0"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
