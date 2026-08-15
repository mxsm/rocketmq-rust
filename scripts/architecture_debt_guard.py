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

"""Validate the single architecture debt registry and its generated view."""

from __future__ import annotations

import argparse
import ast
from dataclasses import dataclass
import json
from pathlib import Path
import sys
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import core_release_scope


DEFAULT_ROOT = Path(__file__).resolve().parents[1]
REGISTRY_RELATIVE = Path("scripts/architecture-debt-registry.json")
REQUIRED_CLASSES = {"compatibility", "panic", "trait", "allow", "runtime_adapter", "facade"}
REQUIRED_ENTRY_FIELDS = {
    "id",
    "class",
    "owner",
    "status",
    "reason",
    "adr",
    "removal_condition",
    "target_release",
    "evidence",
    "scope_count",
}


@dataclass(frozen=True)
class Finding:
    code: str
    path: str
    detail: str

    def render(self) -> str:
        return f"DEBT_FINDING code={self.code} path={self.path} detail={self.detail}"


def load_registry(root: Path) -> dict[str, Any]:
    value = json.loads((root / REGISTRY_RELATIVE).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("architecture debt registry must contain an object")
    return value


def normalized_path(value: object) -> str | None:
    if not isinstance(value, str) or not value or "\\" in value:
        return None
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        return None
    return path.as_posix()


def validate_registry(root: Path, registry: dict[str, Any]) -> list[Finding]:
    findings: list[Finding] = []
    if set(registry) != {"schema_version", "release_boundary", "generated_document", "entries"}:
        findings.append(Finding("schema", REGISTRY_RELATIVE.as_posix(), "unexpected top-level fields"))
        return findings
    if registry["schema_version"] != 1 or registry["release_boundary"] != "2.0.0":
        findings.append(Finding("schema", REGISTRY_RELATIVE.as_posix(), "expected schema 1 and release 2.0.0"))

    document = normalized_path(registry["generated_document"])
    if document is None:
        findings.append(Finding("generated-document", REGISTRY_RELATIVE.as_posix(), "invalid generated document path"))

    entries = registry["entries"]
    if not isinstance(entries, list):
        findings.append(Finding("entries", REGISTRY_RELATIVE.as_posix(), "entries must be an array"))
        return findings

    identities: set[str] = set()
    active_classes: set[str] = set()
    for index, entry in enumerate(entries):
        label = f"{REGISTRY_RELATIVE.as_posix()}#entries[{index}]"
        if not isinstance(entry, dict):
            findings.append(Finding("entry-schema", label, "entry must be an object"))
            continue
        missing = sorted(REQUIRED_ENTRY_FIELDS - entry.keys())
        if missing:
            findings.append(Finding("entry-schema", label, f"missing {','.join(missing)}"))
            continue

        identity = entry["id"]
        if not isinstance(identity, str) or not identity:
            findings.append(Finding("entry-id", label, "id must be non-empty"))
        elif identity in identities:
            findings.append(Finding("entry-id", label, f"duplicate {identity}"))
        else:
            identities.add(identity)

        debt_class = entry["class"]
        status = entry["status"]
        if debt_class not in REQUIRED_CLASSES:
            findings.append(Finding("entry-class", label, f"unsupported {debt_class}"))
        if status not in {"active", "resolved"}:
            findings.append(Finding("entry-status", label, f"unsupported {status}"))
        if status == "active":
            active_classes.add(debt_class)

        for field in ("owner", "reason", "removal_condition"):
            if not isinstance(entry[field], str) or not entry[field].strip():
                findings.append(Finding("entry-field", label, f"{field} must be non-empty"))
        if entry["target_release"] != registry["release_boundary"]:
            findings.append(Finding("release-boundary", label, str(entry["target_release"])))
        if not isinstance(entry["scope_count"], int) or entry["scope_count"] < 0:
            findings.append(Finding("scope-count", label, "scope_count must be a non-negative integer"))

        adr = normalized_path(entry["adr"])
        if adr is None or not (root / adr).is_file():
            findings.append(Finding("adr-missing", label, str(entry["adr"])))
        elif "Status: Accepted" not in (root / adr).read_text(encoding="utf-8"):
            findings.append(Finding("adr-unaccepted", adr, identity))

        evidence = entry["evidence"]
        if not isinstance(evidence, list) or not evidence:
            findings.append(Finding("evidence", label, "at least one evidence path is required"))
        else:
            for value in evidence:
                path = normalized_path(value)
                if path is None or not (root / path).is_file():
                    findings.append(Finding("evidence-missing", label, str(value)))

        source_checks = entry.get("source_checks", [])
        if not isinstance(source_checks, list):
            findings.append(Finding("source-check", label, "source_checks must be an array"))
            continue
        for check_index, check in enumerate(source_checks):
            check_label = f"{label}.source_checks[{check_index}]"
            if not isinstance(check, dict) or set(check) != {"pattern", "max_count", "paths"}:
                findings.append(Finding("source-check", check_label, "unexpected schema"))
                continue
            if not isinstance(check["pattern"], str) or not check["pattern"]:
                findings.append(Finding("source-check", check_label, "pattern must be non-empty"))
                continue
            if not isinstance(check["max_count"], int) or check["max_count"] < 0:
                findings.append(Finding("source-check", check_label, "max_count must be non-negative"))
                continue
            count = 0
            for value in check["paths"]:
                path = normalized_path(value)
                if path is None or not (root / path).is_file():
                    findings.append(Finding("source-check-path", check_label, str(value)))
                    continue
                count += (root / path).read_text(encoding="utf-8").count(check["pattern"])
            if count > check["max_count"]:
                findings.append(
                    Finding(
                        "resolved-debt-regressed" if status == "resolved" else "active-debt-growth",
                        label,
                        f"pattern={check['pattern']} count={count} max={check['max_count']}",
                    )
                )

    missing_classes = sorted(REQUIRED_CLASSES - active_classes)
    if missing_classes:
        findings.append(
            Finding("class-coverage", REGISTRY_RELATIVE.as_posix(), f"missing active {','.join(missing_classes)}")
        )
    return findings


def error_allowlist_count(source: str, *, scope: str) -> int:
    tree = ast.parse(source)
    allowlist_names = {
        "INTERNAL_ERROR_ALLOWLIST",
        "ANYHOW_RESULT_ALLOWLIST",
        "PROCESSOR_GENERIC_RESPONSE_ALLOWLIST",
        "SOURCE_STRINGIFICATION_ALLOWLIST",
    }
    count = 0
    for node in tree.body:
        name = None
        value = None
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id
            value = node.value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            name = node.target.id
            value = node.value
        if name not in allowlist_names or value is None:
            continue
        entries = ast.literal_eval(value)
        count += sum(core_release_scope.path_in_scope(path, scope) for path in entries)
    return count


def validate_specialist_ledgers(
    root: Path,
    registry: dict[str, Any],
    *,
    scope: str = "core-release",
) -> list[Finding]:
    findings: list[Finding] = []
    entries = {entry["id"]: entry for entry in registry["entries"]}

    dependency = json.loads(
        (root / "scripts/architecture-dependency-baseline.json").read_text(encoding="utf-8")
    )
    compatibility = dependency["compatibility_manifest_exceptions"]
    registered_compatibility = {
        entry["id"] for entry in registry["entries"] if entry["class"] == "compatibility" and entry["status"] == "active"
    }
    baseline_compatibility = {entry.get("debt_id") for entry in compatibility}
    if baseline_compatibility != registered_compatibility:
        findings.append(
            Finding(
                "compatibility-ledger-drift",
                "scripts/architecture-dependency-baseline.json",
                f"baseline={sorted(str(value) for value in baseline_compatibility)} "
                f"registry={sorted(registered_compatibility)}",
            )
        )
    for entry in compatibility:
        debt = entries.get(entry.get("debt_id"))
        if debt is None:
            continue
        if (
            entry["owner"] != debt["owner"]
            or entry["remove_by"] != debt["target_release"]
            or entry["adr"] != debt["adr"]
        ):
            findings.append(
                Finding("compatibility-ledger-drift", "scripts/architecture-dependency-baseline.json", debt["id"])
            )

    hygiene = json.loads((root / "scripts/rust-hygiene-baseline.json").read_text(encoding="utf-8"))
    panic_count = sum(entry["kind"] == "panic_surface" for entry in hygiene["entries"])
    if entries["ARC-PANIC-001"]["scope_count"] != panic_count:
        findings.append(Finding("scope-drift", "scripts/rust-hygiene-baseline.json", f"panic={panic_count}"))

    traits = json.loads((root / "scripts/trait-policy-baseline.json").read_text(encoding="utf-8"))
    if entries["ARC-TRAIT-001"]["scope_count"] != len(traits["entries"]):
        findings.append(Finding("scope-drift", "scripts/trait-policy-baseline.json", f"traits={len(traits['entries'])}"))

    runtime = json.loads((root / "scripts/runtime-audit-baseline.json").read_text(encoding="utf-8-sig"))
    runtime_count = sum(
        len(runtime["categories"][category]["fingerprints"])
        for category in ("current-runtime-adapter-sites", "task-group-root-sites")
    )
    if entries["ARC-RUNTIME-001"]["scope_count"] != runtime_count:
        findings.append(Finding("scope-drift", "scripts/runtime-audit-baseline.json", f"runtime={runtime_count}"))

    error_source = (root / "scripts/error_architecture_guard.py").read_text(encoding="utf-8")
    allow_count = error_allowlist_count(error_source, scope=scope)
    lint_registry = json.loads(
        (root / "scripts/rust-lint-debt-registry.json").read_text(encoding="utf-8")
    )
    allow_count += len(lint_registry["entries"])
    if entries["ARC-ALLOW-001"]["scope_count"] != allow_count:
        findings.append(
            Finding(
                "scope-drift",
                "scripts/error_architecture_guard.py,scripts/rust-lint-debt-registry.json",
                f"allow={allow_count}",
            )
        )

    policy = json.loads((root / "scripts/architecture-dependency-policy.json").read_text(encoding="utf-8"))
    facade_count = sum(
        entry["class"] == "facade" and entry["status"] == "active" for entry in registry["entries"]
    )
    if facade_count != len(policy["facade_rules"]):
        findings.append(
            Finding(
                "facade-ledger-drift",
                "scripts/architecture-dependency-policy.json",
                f"policy={len(policy['facade_rules'])} registry={facade_count}",
            )
        )
    return findings


def render_document(registry: dict[str, Any]) -> str:
    lines = [
        "# Architecture debt register",
        "",
        "<!-- Generated by scripts/architecture_debt_guard.py. Do not edit manually. -->",
        "",
        f"Release boundary: `{registry['release_boundary']}`.",
        "",
        "| ID | Class | Owner | Status | Scope | Removal condition | Evidence |",
        "|---|---|---|---|---:|---|---|",
    ]
    for entry in sorted(registry["entries"], key=lambda item: item["id"]):
        evidence = "<br>".join(f"`{path}`" for path in entry["evidence"])
        lines.append(
            f"| `{entry['id']}` | `{entry['class']}` | `{entry['owner']}` | "
            f"`{entry['status']}` | {entry['scope_count']} | {entry['removal_condition']} | {evidence} |"
        )
    lines.extend(
        [
            "",
            "Removed internal crates, facade re-exports, old module paths, and historical migration",
            "evidence are not compatibility surfaces. Protocol, wire, persisted-layout, and implemented",
            "behavior contracts remain fail-closed.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--write", action="store_true")
    parser.add_argument(
        "--scope",
        choices=("core-release", "repo-global", "all"),
        default="all",
    )
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT, help=argparse.SUPPRESS)
    args = parser.parse_args()
    root = args.root.resolve()
    try:
        registry = load_registry(root)
        findings = validate_registry(root, registry)
        findings.extend(validate_specialist_ledgers(root, registry, scope=args.scope))
        document = root / registry["generated_document"]
        rendered = render_document(registry)
        if args.write:
            if findings:
                for finding in findings:
                    print(finding.render())
                return 1
            document.parent.mkdir(parents=True, exist_ok=True)
            document.write_text(rendered, encoding="utf-8", newline="\n")
            print(f"ARCHITECTURE_DEBT_WRITTEN path={document.relative_to(root).as_posix()}")
            return 0
        actual = document.read_text(encoding="utf-8") if document.is_file() else ""
        if actual != rendered:
            findings.append(Finding("generated-document-drift", registry["generated_document"], "run with --write"))
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as error:
        print(f"DEBT_FINDING code=input-invalid path=. detail={error}")
        return 2

    if findings:
        for finding in findings:
            print(finding.render())
        print(f"ARCHITECTURE_DEBT_FAILED findings={len(findings)}")
        return 1
    active = sum(entry["status"] == "active" for entry in registry["entries"])
    print(
        f"ARCHITECTURE_DEBT_OK scope={args.scope} "
        f"entries={len(registry['entries'])} active={active}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
