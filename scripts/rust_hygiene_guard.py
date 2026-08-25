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
import subprocess
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
TYPED_FILTER_DEBT_FIELDS = {"ordinal"}
UNSAFE_DEBT_KINDS = {"unsafe_block", "unsafe_fn", "unsafe_impl", "unsafe_trait", "unsafe_extern"}
PROTOCOL_PREFIX = "rocketmq-protocol/"
FILTER_LEGACY_COMPATIBILITY_OWNERS = {
    "rocketmq-filter/src/filter.rs",
    "rocketmq-filter/src/filter/filter_spi.rs",
    "rocketmq-filter/src/filter/filter_sql_filter.rs",
    "rocketmq-filter/src/filter/sql_runtime/compile_error.rs",
}
FILTER_DEPRECATION_NOTE = "use Filter::try_compile and FilterCompileError"
TYPED_FILTER_DEBT_KINDS = {"legacy_filter_compile", "local_filter_error"}
TYPED_FILTER_BASELINE_METADATA = {
    "classification": "legacy_filter_compatibility",
    "owner": "rocketmq-filter maintainers",
    "reachability": "production-compatibility-owner",
    "justification": "compiler-resolved legacy Filter compatibility use retained only in a canonical owner",
    "expiry": "2.0.0",
}
FILTER_COMPILE_DEFINITION_PATHS = frozenset(
    {
        "filter::filter_spi::Filter::compile",
        "rocketmq_filter::filter::Filter::compile",
    }
)
FILTER_ERROR_DEFINITION_PATHS = frozenset(
    {
        "filter::filter_spi::FilterError",
        "filter::filter_spi::FilterError::new",
        "filter::filter_spi::FilterError::message",
        "rocketmq_filter::filter::FilterError",
        "rocketmq_filter::filter::FilterError::new",
        "rocketmq_filter::filter::FilterError::message",
    }
)
DEPRECATED_DIAGNOSTIC = re.compile(
    r"^use of deprecated (?P<form>[^`]+) `(?P<definition>[^`]+)`: (?P<note>.+)$"
)
FILTER_ERROR_DEPRECATION_ANCHOR = re.compile(
    rf'^\#\[deprecated\(since = "1\.0\.0", note = "{re.escape(FILTER_DEPRECATION_NOTE)}"\)\]\r?\n'
    r"^\#\[derive\(Debug, Clone\)\]\r?\n"
    r"^pub struct FilterError\b",
    re.MULTILINE,
)
FILTER_COMPILE_DEPRECATION_ANCHOR = re.compile(
    rf'^    \#\[deprecated\(since = "1\.0\.0", note = "{re.escape(FILTER_DEPRECATION_NOTE)}"\)\]\r?\n'
    r"^    \#\[allow\(\r?\n"
    r"(?:(?!^    \)\]).*\r?\n)*?"
    r"^    \)\]\r?\n"
    r"^    fn compile\(&self,",
    re.MULTILINE,
)
BASELINE_POLICY = {
    "unsafe": "Every production unsafe block or impl requires an adjacent // SAFETY: comment; protocol unsafe identities may not grow.",
    "safe_raw_pointer_api": "Public safe functions must not accept raw pointers.",
    "debt": "Existing panic surfaces, manual Pin projection, protocol unsafe regions, and mod.rs files may be deleted but not added.",
    "legacy_runtime": "Non-canonical production RocketMQRuntime use is forbidden.",
    "typed_filter": "Deprecated Filter::compile and local FilterError diagnostics are compiler-resolved; only exact baseline-recorded occurrences in canonical compatibility owners are retained.",
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


class TypedFilterGuardError(RuntimeError):
    """A compiler-resolved typed-filter check could not be verified."""


class TypedFilterDiagnostic(NamedTuple):
    kind: str
    relative: str
    offset: int
    line: int


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


def cfg_requires_builtin_test(expression: str) -> bool:
    """Return whether a cfg expression is impossible unless Rust's test cfg is set.

    This deliberately does not treat feature flags as test-only. A feature such
    as ``test-support`` can be enabled in a production build, while ``test`` is
    supplied only by Rust's built-in test compilation mode.
    """

    expression = expression.strip()
    if expression == "test":
        return True
    match = re.fullmatch(r"(?P<operator>all|any)\s*\((?P<arguments>.*)\)", expression, re.DOTALL)
    if match is None:
        return False
    requirements = [
        cfg_requires_builtin_test(argument) for argument in split_cfg_arguments(match.group("arguments"))
    ]
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


def cfg_builtin_test_item_ranges(masked: str, source: str) -> list[tuple[int, int]]:
    """Find items that require the built-in ``cfg(test)`` condition.

    The source scanner's broader test-support convention is intentionally not
    used by the compiler-diagnostic guard: feature flags remain production
    reachable for this migration rule.
    """

    ranges: list[tuple[int, int]] = []
    for match in CFG_ATTRIBUTE.finditer(masked):
        body = source[match.start("body"):match.end("body")]
        if not cfg_requires_builtin_test(body):
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


def filter_deprecation_kind(message: object) -> str | None:
    """Classify only frozen legacy filter definitions from a rustc diagnostic."""

    if not isinstance(message, str):
        raise TypedFilterGuardError("deprecated diagnostic has no message text")
    match = DEPRECATED_DIAGNOSTIC.fullmatch(message)
    if match is None:
        return None
    definition = match.group("definition")
    form = match.group("form")
    if definition not in FILTER_COMPILE_DEFINITION_PATHS | FILTER_ERROR_DEFINITION_PATHS:
        return None
    if match.group("note") != FILTER_DEPRECATION_NOTE:
        raise TypedFilterGuardError(
            f"frozen filter deprecation note drifted for {definition!r}: {match.group('note')!r}"
        )
    if definition in FILTER_COMPILE_DEFINITION_PATHS and form == "method":
        return "legacy_filter_compile"
    if definition in FILTER_ERROR_DEFINITION_PATHS:
        return "local_filter_error"
    raise TypedFilterGuardError(f"unknown frozen filter deprecated definition: {definition!r}")


def validate_filter_deprecation_anchors(root: Path) -> None:
    """Fail closed if the two frozen definitions stop advertising the migration note."""

    source_path = root / "rocketmq-filter/src/filter/filter_spi.rs"
    try:
        source = source_path.read_text(encoding="utf-8")
    except OSError as error:
        raise TypedFilterGuardError(f"cannot verify Filter deprecation anchors: {error}") from error
    masked = rust_source.mask_comments_and_literals(source)
    error_anchor = FILTER_ERROR_DEPRECATION_ANCHOR.search(source)
    compile_anchor = FILTER_COMPILE_DEPRECATION_ANCHOR.search(source)
    trait = re.search(r"^pub trait Filter\b", masked, re.MULTILINE)
    trait_opening = masked.find("{", trait.end()) if trait is not None else -1
    trait_closing = (
        matching_delimiter(masked, trait_opening, "{", "}") if trait_opening >= 0 else None
    )
    compile_code = (
        masked[compile_anchor.start():compile_anchor.end()] if compile_anchor is not None else ""
    )
    error_code = masked[error_anchor.start():error_anchor.end()] if error_anchor is not None else ""
    if (
        error_anchor is None
        or compile_anchor is None
        or trait is None
        or trait_closing is None
        or not trait.start() <= compile_anchor.start() < trait_closing
        or not re.search(r"^    #\[deprecated\(", compile_code, re.MULTILINE)
        or not re.search(r"^    #\[allow\(", compile_code, re.MULTILINE)
        or not re.search(r"^    fn compile\(&self,", compile_code, re.MULTILINE)
        or not re.search(r"^#\[deprecated\(", error_code, re.MULTILINE)
        or not re.search(r"^#\[derive\(Debug, Clone\)", error_code, re.MULTILINE)
        or not re.search(r"^pub struct FilterError\b", error_code, re.MULTILINE)
    ):
        raise TypedFilterGuardError("frozen Filter and FilterError deprecation anchors are missing or drifted")


def cargo_metadata(root: Path) -> dict[str, object]:
    """Load the resolved workspace graph required for the reverse dependency closure."""

    command = ["cargo", "metadata", "--locked", "--all-features", "--format-version", "1"]
    try:
        completed = subprocess.run(
            command,
            cwd=root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            check=False,
        )
    except OSError as error:
        raise TypedFilterGuardError(f"cargo metadata could not start: {error}") from error
    if completed.returncode != 0:
        raise TypedFilterGuardError(
            f"cargo metadata failed with exit {completed.returncode}: {completed.stderr.strip()}"
        )
    try:
        metadata = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise TypedFilterGuardError(f"cargo metadata emitted invalid JSON: {error}") from error
    if not isinstance(metadata, dict):
        raise TypedFilterGuardError("cargo metadata did not return an object")
    return metadata


def filter_reverse_dependency_packages(metadata: dict[str, object]) -> list[str]:
    """Return the workspace package names in rocketmq-filter's resolved reverse closure."""

    packages = metadata.get("packages")
    members = metadata.get("workspace_members")
    resolve = metadata.get("resolve")
    if not isinstance(packages, list) or not isinstance(members, list) or not isinstance(resolve, dict):
        raise TypedFilterGuardError("cargo metadata is missing packages, workspace members, or resolve graph")
    by_id = {
        package.get("id"): package
        for package in packages
        if isinstance(package, dict) and isinstance(package.get("id"), str)
    }
    workspace_members = {member for member in members if isinstance(member, str)}
    filter_ids = [
        package_id
        for package_id in workspace_members
        if isinstance(by_id.get(package_id), dict) and by_id[package_id].get("name") == "rocketmq-filter"
    ]
    if len(filter_ids) != 1:
        raise TypedFilterGuardError("cargo metadata must resolve exactly one workspace rocketmq-filter package")
    nodes = resolve.get("nodes")
    if not isinstance(nodes, list):
        raise TypedFilterGuardError("cargo metadata resolve graph has no nodes")
    reverse: dict[str, set[str]] = {package_id: set() for package_id in workspace_members}
    for node in nodes:
        if not isinstance(node, dict) or not isinstance(node.get("id"), str) or not isinstance(node.get("deps"), list):
            raise TypedFilterGuardError("cargo metadata resolve node is malformed")
        node_id = node["id"]
        if node_id not in workspace_members:
            continue
        for dependency in node["deps"]:
            if not isinstance(dependency, dict) or not isinstance(dependency.get("pkg"), str):
                raise TypedFilterGuardError("cargo metadata dependency edge is malformed")
            dependency_id = dependency["pkg"]
            if dependency_id in reverse:
                reverse[dependency_id].add(node_id)
    closure = {filter_ids[0]}
    pending = [filter_ids[0]]
    while pending:
        package_id = pending.pop()
        for dependent in reverse.get(package_id, set()):
            if dependent not in closure:
                closure.add(dependent)
                pending.append(dependent)
    names: list[str] = []
    for package_id in sorted(closure):
        package = by_id.get(package_id)
        name = package.get("name") if isinstance(package, dict) else None
        if not isinstance(name, str) or not name:
            raise TypedFilterGuardError(f"workspace package {package_id!r} has no name")
        names.append(name)
    if len(names) != len(set(names)):
        raise TypedFilterGuardError("rocketmq-filter reverse closure has ambiguous package names")
    return sorted(names)


def read_utf8_source(path: Path) -> str:
    """Read compiler-addressed source without translating CRLF byte positions."""

    with path.open(encoding="utf-8", newline="") as source:
        return source.read()


def normalise_primary_span(record: dict[str, object], root: Path) -> tuple[str, str, int, int]:
    """Resolve a unique compiler primary span to a UTF-8 source offset under the repository root."""

    message = record.get("message")
    if not isinstance(message, dict) or not isinstance(message.get("spans"), list):
        raise TypedFilterGuardError("typed filter diagnostic has no spans")
    primary = [span for span in message["spans"] if isinstance(span, dict) and span.get("is_primary")]
    if len(primary) != 1:
        raise TypedFilterGuardError("typed filter diagnostic has no unique primary span")
    span = primary[0]
    file_name = span.get("file_name")
    byte_start = span.get("byte_start")
    line_start = span.get("line_start")
    if not isinstance(file_name, str) or not isinstance(byte_start, int) or not isinstance(line_start, int):
        raise TypedFilterGuardError("typed filter primary span is malformed")
    path = Path(file_name)
    path = path if path.is_absolute() else root / path
    try:
        relative = path.resolve().relative_to(root.resolve()).as_posix()
    except (OSError, ValueError) as error:
        raise TypedFilterGuardError(f"typed filter primary span is outside the repository: {file_name!r}") from error
    try:
        source = read_utf8_source(path)
    except OSError as error:
        raise TypedFilterGuardError(f"cannot read typed filter primary source {relative}: {error}") from error
    encoded = source.encode("utf-8")
    if byte_start < 0 or byte_start > len(encoded):
        raise TypedFilterGuardError(f"typed filter primary span has invalid byte offset in {relative}")
    try:
        offset = len(encoded[:byte_start].decode("utf-8"))
    except UnicodeDecodeError as error:
        raise TypedFilterGuardError(f"typed filter primary span is not on a UTF-8 boundary in {relative}") from error
    computed_line = source.count("\n", 0, offset) + 1
    if line_start != computed_line:
        raise TypedFilterGuardError(f"typed filter primary span has inconsistent line information in {relative}")
    return relative, source, offset, computed_line


def typed_filter_diagnostic(record: object, root: Path) -> TypedFilterDiagnostic | None:
    """Classify one cargo compiler-message record, rejecting incomplete frozen diagnostics."""

    if not isinstance(record, dict) or record.get("reason") != "compiler-message":
        return None
    message = record.get("message")
    if not isinstance(message, dict):
        raise TypedFilterGuardError("cargo compiler-message is malformed")
    code = message.get("code")
    code_value = code.get("code") if isinstance(code, dict) else None
    if code_value != "deprecated":
        return None
    kind = filter_deprecation_kind(message.get("message"))
    if kind is None:
        return None
    target = record.get("target")
    target_kind = target.get("kind") if isinstance(target, dict) else None
    if not isinstance(target_kind, list) or not target_kind or not all(isinstance(value, str) for value in target_kind):
        raise TypedFilterGuardError("typed filter diagnostic has no target kind")
    relative, source, offset, line = normalise_primary_span(record, root)
    if "test" in target_kind:
        return None
    masked = rust_source.mask_comments_and_literals(source)
    test_ranges = rust_source.test_module_ranges(masked) + cfg_builtin_test_item_ranges(masked, source)
    if is_test_only(offset, test_ranges):
        return None
    return TypedFilterDiagnostic(kind, relative, offset, line)


def enclosing_typed_filter_item(masked: str, offset: int) -> str:
    """Name the containing function or implementation header for a typed-filter occurrence."""

    functions: list[tuple[int, int, str]] = []
    for match in FUNCTION.finditer(masked):
        opening = masked.find("{", match.end())
        if opening == -1:
            continue
        closing = matching_delimiter(masked, opening, "{", "}")
        if closing is not None and match.start() <= offset <= closing:
            functions.append((match.start(), closing, match.group(1)))
    if functions:
        return max(functions, key=lambda candidate: candidate[0])[2]

    implementations: list[tuple[int, int, str]] = []
    for match in re.finditer(r"\bimpl\b", masked):
        opening = masked.find("{", match.end())
        if opening == -1:
            continue
        closing = matching_delimiter(masked, opening, "{", "}")
        if closing is not None and match.start() <= offset < opening:
            header = re.sub(r"\s+", " ", masked[match.start():opening]).strip()
            implementations.append((match.start(), closing, header))
    if implementations:
        return max(implementations, key=lambda candidate: candidate[0])[2]
    return "<module>"


def typed_filter_debt_entries(diagnostics: list[TypedFilterDiagnostic], root: Path) -> list[dict[str, object]]:
    """Turn unique production diagnostics into stable, owner-local baseline identities."""

    occurrences: dict[tuple[str, str, str], list[TypedFilterDiagnostic]] = {}
    for diagnostic in diagnostics:
        source = read_utf8_source(root / diagnostic.relative)
        masked = rust_source.mask_comments_and_literals(source)
        item = enclosing_typed_filter_item(masked, diagnostic.offset)
        occurrences.setdefault((diagnostic.relative, diagnostic.kind, item), []).append(diagnostic)
    entries: list[dict[str, object]] = []
    for (relative, kind, item), group in sorted(occurrences.items()):
        unique = {(diagnostic.offset, diagnostic.line): diagnostic for diagnostic in group}
        for ordinal, diagnostic in enumerate(unique[key] for key in sorted(unique)):
            entries.append(
                {
                    "identity": f"{relative}:{kind}:{item}:{ordinal}",
                    "path": relative,
                    "kind": kind,
                    "item": item,
                    "line": diagnostic.line,
                    **TYPED_FILTER_BASELINE_METADATA,
                    "ordinal": ordinal,
                }
            )
    return entries


def run_typed_filter_clippy(root: Path, packages: list[str], *, all_features: bool) -> list[TypedFilterDiagnostic]:
    """Parse cargo's JSON diagnostics for one resolved feature pass."""

    if not packages:
        raise TypedFilterGuardError("cargo metadata produced an empty typed filter package closure")
    command = ["cargo", "clippy", "--locked"]
    for package in packages:
        command.extend(["-p", package])
    command.append("--all-targets")
    if all_features:
        command.append("--all-features")
    command.extend(["--no-deps", "--message-format=json", "--", "--force-warn", "deprecated"])
    try:
        completed = subprocess.run(
            command,
            cwd=root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            check=False,
        )
    except OSError as error:
        raise TypedFilterGuardError(f"cargo clippy could not start: {error}") from error
    if completed.returncode != 0:
        raise TypedFilterGuardError(
            f"cargo clippy failed with exit {completed.returncode}: {completed.stderr.strip()}"
        )
    diagnostics: list[TypedFilterDiagnostic] = []
    for raw_line in completed.stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as error:
            raise TypedFilterGuardError(f"cargo clippy emitted malformed JSON: {error}") from error
        classified = typed_filter_diagnostic(record, root)
        if classified is not None:
            diagnostics.append(classified)
    return diagnostics


def scan_typed_filter_deprecations(root: Path) -> list[dict[str, object]]:
    """Resolve legacy Filter use through rustc instead of source-text heuristics."""

    validate_filter_deprecation_anchors(root)
    packages = filter_reverse_dependency_packages(cargo_metadata(root))
    diagnostics = run_typed_filter_clippy(root, packages, all_features=False)
    diagnostics.extend(run_typed_filter_clippy(root, packages, all_features=True))
    return typed_filter_debt_entries(diagnostics, root)


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
        payload.get("schema_version") != 4
        or payload.get("policy") != BASELINE_POLICY
        or not isinstance(payload.get("entries"), list)
    ):
        raise ValueError("rust hygiene baseline has an unsupported schema")
    for entry in payload["entries"]:
        unsafe = isinstance(entry, dict) and entry.get("kind") in UNSAFE_DEBT_KINDS
        typed_filter = isinstance(entry, dict) and entry.get("kind") in TYPED_FILTER_DEBT_KINDS
        expected = DEBT_FIELDS | (
            UNSAFE_DEBT_FIELDS if unsafe else TYPED_FILTER_DEBT_FIELDS if typed_filter else set()
        )
        if not isinstance(entry, dict) or set(entry) != expected:
            raise ValueError("rust hygiene baseline contains an invalid debt entry")
        if any(not isinstance(entry[field], str) or not entry[field] for field in DEBT_FIELDS - {"line"}):
            raise ValueError("rust hygiene baseline contains empty debt metadata")
        if not isinstance(entry["line"], int) or entry["line"] < 1:
            raise ValueError("rust hygiene baseline contains an invalid line")
        typed_identity_kind = next(
            (
                kind
                for kind in TYPED_FILTER_DEBT_KINDS
                if f":{kind}:" in entry["identity"]
            ),
            None,
        )
        if typed_identity_kind is not None and entry["kind"] != typed_identity_kind:
            raise ValueError("rust hygiene baseline contains a disguised typed filter kind")
        identity_prefix = f"{entry['path']}:{entry['kind']}:{entry['item']}:"
        if not entry["identity"].startswith(identity_prefix):
            raise ValueError("rust hygiene baseline contains a structurally inconsistent identity")
        identity_ordinal = entry["identity"][len(identity_prefix):]
        if not identity_ordinal.isascii() or not identity_ordinal.isdecimal():
            raise ValueError("rust hygiene baseline contains an invalid identity ordinal")
        if (unsafe or typed_filter) and (
            not isinstance(entry["ordinal"], int) or entry["ordinal"] < 0
        ):
            raise ValueError("rust hygiene baseline contains an invalid unsafe ordinal")
        if (unsafe or typed_filter) and int(identity_ordinal) != entry["ordinal"]:
            raise ValueError("rust hygiene baseline contains an identity ordinal mismatch")
        if typed_filter and entry["path"] not in FILTER_LEGACY_COMPATIBILITY_OWNERS:
            raise ValueError("rust hygiene baseline contains a non-canonical typed filter compatibility owner")
        if typed_filter:
            expected_identity = f"{entry['path']}:{entry['kind']}:{entry['item']}:{entry['ordinal']}"
            if entry["identity"] != expected_identity:
                raise ValueError("rust hygiene baseline contains a forged typed filter identity")
            if any(entry[field] != value for field, value in TYPED_FILTER_BASELINE_METADATA.items()):
                raise ValueError("rust hygiene baseline contains invalid typed filter metadata")
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
    try:
        debt.extend(scan_typed_filter_deprecations(root))
    except TypedFilterGuardError as error:
        print(f"RUST_HYGIENE_GUARD_FAILED typed_filter={error}", file=sys.stderr)
        return 1
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
        f"typed_filter={sum(counts[kind] for kind in TYPED_FILTER_DEBT_KINDS)} "
        f"reviewed_invariants={reviewed_invariants} excluded_test_sources=0"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
