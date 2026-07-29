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

"""Freeze the legacy MessageStore facade and govern Broker migration.

The scanner masks comments and literals before parsing the trait body. This is
deliberately narrower than a Rust compiler, but it derives every governed
method from balanced source tokens instead of trusting a handwritten count.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASELINE = Path("scripts/message-store-capability-baseline.json")
TRAIT_PATH = Path("rocketmq-store/src/base/message_store.rs")
BROKER_ROOT = Path("rocketmq-broker/src")


@dataclass(frozen=True)
class TraitMethod:
    name: str
    line: int
    is_async: bool
    returns_result: bool


def _mask_rust(source: str) -> str:
    """Replace comments and literals with spaces while preserving newlines."""

    result = list(source)
    index = 0
    length = len(source)
    while index < length:
        if source.startswith("//", index):
            end = source.find("\n", index)
            end = length if end < 0 else end
            for position in range(index, end):
                result[position] = " "
            index = end
            continue
        if source.startswith("/*", index):
            depth = 1
            position = index + 2
            while position < length and depth:
                if source.startswith("/*", position):
                    depth += 1
                    position += 2
                elif source.startswith("*/", position):
                    depth -= 1
                    position += 2
                else:
                    position += 1
            for masked in range(index, min(position, length)):
                if source[masked] != "\n":
                    result[masked] = " "
            index = position
            continue

        raw = re.match(r'r(#{0,255})"', source[index:])
        if raw:
            hashes = raw.group(1)
            terminator = '"' + hashes
            end = source.find(terminator, index + len(raw.group(0)))
            end = length if end < 0 else end + len(terminator)
            for position in range(index, end):
                if source[position] != "\n":
                    result[position] = " "
            index = end
            continue

        if source[index] == '"':
            position = index + 1
            while position < length:
                if source[position] == "\\":
                    position += 2
                    continue
                position += 1
                if source[position - 1] == '"':
                    break
            for masked in range(index, min(position, length)):
                if source[masked] != "\n":
                    result[masked] = " "
            index = position
            continue

        if source[index] == "'" and index + 2 < length:
            position = index + 1
            if source[position] == "\\":
                position += 2
            else:
                position += 1
            if position < length and source[position] == "'":
                position += 1
                for masked in range(index, position):
                    result[masked] = " "
                index = position
                continue
        index += 1
    return "".join(result)


def _balanced_body(masked: str, declaration: re.Pattern[str]) -> tuple[int, int]:
    match = declaration.search(masked)
    if not match:
        raise ValueError("MessageStore trait declaration was not found")
    opening = masked.find("{", match.end())
    if opening < 0:
        raise ValueError("MessageStore trait has no body")
    depth = 0
    for index in range(opening, len(masked)):
        token = masked[index]
        if token == "{":
            depth += 1
        elif token == "}":
            depth -= 1
            if depth == 0:
                return opening + 1, index
    raise ValueError("MessageStore trait body is unbalanced")


def scan_trait(root: Path) -> list[TraitMethod]:
    source = (root / TRAIT_PATH).read_text(encoding="utf-8")
    masked = _mask_rust(source)
    start, end = _balanced_body(masked, re.compile(r"\bpub\s+trait\s+MessageStore\b"))
    body = masked[start:end]
    declaration = re.compile(r"(?m)^\s*(?P<async>async\s+)?fn\s+(?P<name>[A-Za-z_]\w*)\s*(?:<[^;{]*?>)?\s*\(")
    matches = list(declaration.finditer(body))
    methods: list[TraitMethod] = []
    for position, match in enumerate(matches):
        signature_end = matches[position + 1].start() if position + 1 < len(matches) else len(body)
        signature = body[match.start():signature_end].split(";", maxsplit=1)[0].split("{", maxsplit=1)[0]
        methods.append(
            TraitMethod(
                name=match.group("name"),
                line=source.count("\n", 0, start + match.start()) + 1,
                is_async=match.group("async") is not None,
                returns_result="Result<" in signature,
            )
        )
    duplicates = sorted(name for name, count in Counter(method.name for method in methods).items() if count > 1)
    if duplicates:
        raise ValueError(f"duplicate MessageStore methods: {', '.join(duplicates)}")
    return methods


def _production_source(path: Path) -> str:
    source = path.read_text(encoding="utf-8")
    return source.split("#[cfg(test)]", maxsplit=1)[0]


def discover_broker_dependencies(root: Path) -> tuple[dict[str, int], set[str]]:
    dependencies: dict[str, int] = {}
    used_methods: set[str] = set()
    method_names = {method.name for method in scan_trait(root)}
    call_pattern = re.compile(r"\.\s*([A-Za-z_]\w*)\s*\(")
    for path in sorted((root / BROKER_ROOT).rglob("*.rs")):
        production = _production_source(path)
        masked = _mask_rust(production)
        count = len(re.findall(r"\bMessageStore\b", masked))
        if count:
            dependencies[path.relative_to(root).as_posix()] = count
        used_methods.update(match.group(1) for match in call_pattern.finditer(masked) if match.group(1) in method_names)
    return dependencies, used_methods


def discover_callers(root: Path, methods: Iterable[TraitMethod]) -> dict[str, dict[str, int]]:
    names = {method.name for method in methods}
    callers: dict[str, dict[str, int]] = defaultdict(dict)
    call_pattern = re.compile(r"\.\s*([A-Za-z_]\w*)\s*\(")
    for crate in sorted(root.glob("rocketmq-*")):
        source_root = crate / "src"
        if not source_root.is_dir():
            continue
        for path in sorted(source_root.rglob("*.rs")):
            production = _production_source(path)
            if "MessageStore" not in production:
                continue
            masked = _mask_rust(production)
            if not re.search(r"\bMessageStore\b", masked):
                continue
            counts = Counter(match.group(1) for match in call_pattern.finditer(masked) if match.group(1) in names)
            relative = path.relative_to(root).as_posix()
            for name, count in counts.items():
                callers[name][relative] = count
    return callers


def method_group(name: str) -> tuple[str, str, str]:
    if name in {"load", "start", "init", "shutdown", "shutdown_gracefully", "destroy", "is_shutdown"}:
        return "lifecycle", "StoreLifecycle", "control"
    if name.startswith(("put_", "async_put", "assign_", "increase_", "append_", "on_commit")):
        return "append", "MessageAppender", "hot"
    if name.startswith(("get_message", "look_message", "select_one", "query_message", "get_data", "check_message")):
        return "read", "MessageReader", "hot"
    if "offset" in name or "consume_queue" in name or name.startswith(("find_", "estimate_", "recover_topic")):
        return "offset/index", "OffsetIndex", "hot"
    if "checkpoint" in name or "flush" in name or "commit" in name:
        return "checkpoint", "ReleaseCheckpointStore", "control"
    if any(token in name for token in ("ha_", "master", "slave", "replica", "broker_role")):
        return "replication/HA", "ReplicationControl", "control"
    if any(token in name for token in ("health", "busy", "writeable", "running_flags", "transient_store")):
        return "health", "StoreHealth", "control"
    if any(token in name for token in ("config", "hook", "dispatcher", "delete", "clean_", "runtime_info")):
        return "admin/config", "StoreAdministration", "control"
    return "internal", "BackendInternal", "internal"


def backend_support(group: str) -> str:
    if group == "lifecycle":
        return "Local, RocksDB, Tiered"
    if group in {"append", "read", "offset/index", "checkpoint", "health"}:
        return "Local and RocksDB; Tiered only through its claimed narrow surface"
    if group == "replication/HA":
        return "Local/RocksDB composition; not a Tiered capability"
    return "Backend-specific facade; no cross-backend claim"


def render_report(root: Path, baseline: dict, methods: list[TraitMethod]) -> str:
    dependencies, used_methods = discover_broker_dependencies(root)
    callers = discover_callers(root, methods)
    lines = [
        "# MessageStore Capability Migration Board",
        "",
        "This file is generated by `python scripts/message_store_capability_guard.py --write-report`.",
        "The legacy facade is frozen: reductions are allowed, additions fail the guard.",
        "",
        "## Current burn-down",
        "",
        f"- Derived facade methods: {len(methods)} (non-growth ceiling: {baseline['max_trait_methods']}).",
        f"- Methods called by Broker production source: {len(used_methods)} "
        f"(release threshold: {baseline['max_broker_used_methods']}).",
        f"- Broker production files naming the wide facade: {len(dependencies)} "
        f"(non-growth ceiling: {baseline['max_broker_direct_paths']}).",
        f"- Wide-facade identifier occurrences in those files: {sum(dependencies.values())} "
        f"(non-growth ceiling: {baseline['max_broker_direct_occurrences']}).",
        "",
        "## Method and caller matrix",
        "",
        "| Method | Group | Target capability | Heat | Error | Lane | Backend support | Production callers |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for method in methods:
        group, capability, heat = method_group(method.name)
        method_callers = callers.get(method.name, {})
        caller_text = ", ".join(
            f"`{path}` ({count})" for path, count in sorted(method_callers.items())
        ) or "None detected"
        lines.append(
            f"| `{method.name}` | {group} | `{capability}` | {heat} | "
            f"{'typed `Result`' if method.returns_result else 'value/status'} | "
            f"{'native async' if method.is_async else 'caller thread'} | "
            f"{backend_support(group)} | {caller_text} |"
        )

    lines.extend(
        [
            "",
            "## Broker wide-facade allowlist",
            "",
            "| Path | Occurrences | Owner | Reason | Removal condition |",
            "|---|---:|---|---|---|",
        ]
    )
    entries = {entry["path"]: entry for entry in baseline["broker_allowlist"]}
    for path, count in sorted(dependencies.items()):
        entry = entries[path]
        lines.append(
            f"| `{path}` | {count} | {entry['owner']} | {entry['reason']} | "
            f"{entry['removal_condition']} |"
        )

    lines.extend(
        [
            "",
            "## Migration order",
            "",
            "1. Finish transaction, schedule, timer, and pop ports.",
            "2. Finish failover, replication, and pre-online ports.",
            "3. Finish admin and control-plane ports.",
            "4. Retain the concrete store only at composition and lifecycle ownership roots.",
            "",
            "The current release gate is at most 80 facade methods used by Broker production code. "
            "This report does not claim that all compatibility paths are removed; it makes every "
            "remaining path owned and mechanically non-growing.",
            "",
        ]
    )
    return "\n".join(lines)


def build_baseline(root: Path, report_path: str) -> dict:
    methods = scan_trait(root)
    dependencies, used_methods = discover_broker_dependencies(root)
    return {
        "schema_version": 1,
        "trait_path": TRAIT_PATH.as_posix(),
        "report_path": report_path,
        "max_trait_methods": len(methods),
        "allowed_methods": [method.name for method in methods],
        "max_broker_used_methods": 80,
        "max_broker_direct_paths": len(dependencies),
        "max_broker_direct_occurrences": sum(dependencies.values()),
        "broker_allowlist": [
            {
                "path": path,
                "owner": "broker-store-migration",
                "reason": "Existing Broker compatibility consumer pending its narrow capability port.",
                "removal_condition": "Remove when this module receives only its use-case capability or becomes a composition-only owner.",
            }
            for path in dependencies
        ],
    }


def validate(root: Path, baseline: dict) -> list[str]:
    findings: list[str] = []
    expected_keys = {
        "schema_version",
        "trait_path",
        "report_path",
        "max_trait_methods",
        "allowed_methods",
        "max_broker_used_methods",
        "max_broker_direct_paths",
        "max_broker_direct_occurrences",
        "broker_allowlist",
    }
    if set(baseline) != expected_keys or baseline.get("schema_version") != 1:
        return ["baseline schema is invalid"]
    if baseline["trait_path"] != TRAIT_PATH.as_posix():
        findings.append("baseline trait_path does not identify the canonical MessageStore")

    try:
        methods = scan_trait(root)
    except (OSError, ValueError) as error:
        return [str(error)]
    names = [method.name for method in methods]
    allowed = baseline["allowed_methods"]
    unexpected = sorted(set(names) - set(allowed))
    if unexpected:
        findings.append(f"new MessageStore methods are forbidden: {', '.join(unexpected)}")
    if len(methods) > baseline["max_trait_methods"]:
        findings.append(
            f"MessageStore grew to {len(methods)} methods; ceiling is {baseline['max_trait_methods']}"
        )

    dependencies, used_methods = discover_broker_dependencies(root)
    entries = baseline["broker_allowlist"]
    malformed = [
        str(index)
        for index, entry in enumerate(entries)
        if set(entry) != {"path", "owner", "reason", "removal_condition"}
        or not all(isinstance(entry[field], str) and entry[field].strip() for field in entry)
    ]
    if malformed:
        findings.append(f"Broker allowlist entries lack owner/reason/removal condition: {', '.join(malformed)}")
    allowlist_paths = [entry.get("path", "") for entry in entries]
    if len(allowlist_paths) != len(set(allowlist_paths)):
        findings.append("Broker allowlist contains duplicate paths")
    unexpected_paths = sorted(set(dependencies) - set(allowlist_paths))
    stale_paths = sorted(set(allowlist_paths) - set(dependencies))
    if unexpected_paths:
        findings.append(f"new Broker MessageStore dependencies are forbidden: {', '.join(unexpected_paths)}")
    if stale_paths:
        findings.append(f"remove stale Broker MessageStore allowlist entries: {', '.join(stale_paths)}")
    if len(dependencies) > baseline["max_broker_direct_paths"]:
        findings.append(
            f"Broker direct dependency files grew to {len(dependencies)}; "
            f"ceiling is {baseline['max_broker_direct_paths']}"
        )
    occurrences = sum(dependencies.values())
    if occurrences > baseline["max_broker_direct_occurrences"]:
        findings.append(
            f"Broker MessageStore occurrences grew to {occurrences}; "
            f"ceiling is {baseline['max_broker_direct_occurrences']}"
        )
    if len(used_methods) > baseline["max_broker_used_methods"]:
        findings.append(
            f"Broker uses {len(used_methods)} facade methods; "
            f"release threshold is {baseline['max_broker_used_methods']}"
        )

    if not findings:
        report = render_report(root, baseline, methods)
        report_path = root / baseline["report_path"]
        if not report_path.is_file() or report_path.read_text(encoding="utf-8") != report:
            findings.append(
                f"generated migration board is stale: run "
                f"`python scripts/message_store_capability_guard.py --write-report`"
            )
    return findings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--write-baseline", action="store_true")
    parser.add_argument("--write-report", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = args.root.resolve()
    baseline_path = args.baseline if args.baseline.is_absolute() else root / args.baseline
    if args.write_baseline:
        report_path = "rocketmq-doc/en/message-store-capability-migration.md"
        baseline = build_baseline(root, report_path)
        baseline_path.parent.mkdir(parents=True, exist_ok=True)
        baseline_path.write_text(json.dumps(baseline, indent=2) + "\n", encoding="utf-8")
    else:
        try:
            baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            print(f"message-store-capability-guard: {error}", file=sys.stderr)
            return 1

    if args.write_report or args.write_baseline:
        methods = scan_trait(root)
        report = render_report(root, baseline, methods)
        report_path = root / baseline["report_path"]
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(report, encoding="utf-8")

    findings = validate(root, baseline)
    if findings:
        for finding in findings:
            print(f"message-store-capability-guard: {finding}", file=sys.stderr)
        return 1
    methods = scan_trait(root)
    dependencies, used_methods = discover_broker_dependencies(root)
    print(
        "message-store-capability-guard: "
        f"methods={len(methods)} broker_used_methods={len(used_methods)} "
        f"direct_paths={len(dependencies)} occurrences={sum(dependencies.values())}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
