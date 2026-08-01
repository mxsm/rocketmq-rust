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

"""Rank production Rust hotspots and enforce maintainability non-growth."""

from __future__ import annotations

import argparse
import json
import math
import re
import subprocess
import sys
from collections import defaultdict
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import environment_write_guard as rust_source  # noqa: E402


ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / "scripts" / "module-maintainability-baseline.json"
REPORT = ROOT / "rocketmq-doc" / "en" / "module-maintainability-board.md"
DECISIONS = ROOT / "rocketmq-doc" / "en" / "hotspot-module-decisions.md"
REVIEW_LINES = 500
HARD_LINES = 800
RANKED_HOTSPOTS = 20
FORBIDDEN_FRAGMENT_NAMES = {"impl_2.rs", "misc.rs", "utils2.rs"}

PUBLIC_ITEM = re.compile(
    r"(?m)^\s*pub\s+(?:(?:async|unsafe)\s+)?(?:fn|struct|enum|trait|type|const|static|mod)\b"
)
REEXPORT = re.compile(r"(?m)^\s*pub\s+use\b")
LOCK_SITE = re.compile(r"\b(?:Mutex|RwLock|Atomic[A-Z][A-Za-z0-9_]*)\b")
STATE_OWNER = re.compile(
    r"(?m)^\s*(?:pub(?:\([^)]*\))?\s+)?struct\s+[A-Za-z0-9_]*(?:State|Runtime|Manager|Service|Context)\b"
)
TEST_FUNCTION = re.compile(r"(?m)^\s*#\[(?:tokio::)?test[^\]]*\]\s*(?:\r?\n\s*#\[[^\]]+\]\s*)*")
USE_TARGET = re.compile(r"(?m)^\s*use\s+(?:crate::([A-Za-z0-9_]+)|(rocketmq_[A-Za-z0-9_]+))")
DECISION_HEADING = re.compile(r"(?m)^### `([^`\r\n]+)`\s*$")
DECISION_FIELD = re.compile(r"(?m)^- (Decision|Owner|State owner|Evidence|Revisit when):\s*(.+?)\s*$")


@dataclass(frozen=True)
class History:
    commits: int = 0
    contributors: int = 0
    defects: int = 0


@dataclass(frozen=True)
class FileMetrics:
    path: str
    crate: str
    production_lines: int
    public_items: int
    reexports: int
    lock_sites: int
    state_owners: int
    fan_out: int
    test_functions: int
    churn_commits: int
    contributors: int
    defect_commits: int
    score: float


@dataclass(frozen=True)
class Finding:
    code: str
    path: str
    detail: str


def normalized(path: Path) -> str:
    return path.as_posix()


def is_test_only_file(path: Path) -> bool:
    name = path.name
    return (
        name == "tests.rs"
        or name.startswith("test_")
        or name.endswith("_test.rs")
        or name.endswith("_tests.rs")
        or any(part in {"test", "tests", "benches", "examples", "fuzz"} for part in path.parts)
    )


def production_projection(source: str) -> str:
    masked = rust_source.mask_comments_and_literals(source)
    ranges = rust_source.test_module_ranges(masked)
    if not ranges:
        return source
    projected = list(source)
    for start, end in ranges:
        for index in range(start, end):
            if projected[index] not in "\r\n":
                projected[index] = " "
    return "".join(projected)


def production_code_lines(source: str) -> int:
    """Count non-comment production lines after removing test-only modules."""
    projected = production_projection(source)
    masked = rust_source.mask_comments_and_literals(projected)
    return sum(bool(line.strip()) for line in masked.splitlines())


def crate_name(relative: Path) -> str:
    parts = relative.parts
    if parts[0] == "rocketmq-dashboard" and len(parts) > 1:
        return parts[1]
    if parts[0] == "rocketmq-tools" and len(parts) > 2 and parts[1] == "rocketmq-admin":
        return parts[2]
    if parts[0] == "rocketmq-tools" and len(parts) > 1:
        return parts[1]
    return parts[0]


def git_history(root: Path) -> dict[str, History]:
    command = [
        "git",
        "log",
        "--since=12 months ago",
        "--format=@@%H%x09%ae%x09%s",
        "--name-only",
        "--no-renames",
        "--",
        "*.rs",
    ]
    try:
        result = subprocess.run(
            command,
            cwd=root,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return {}

    commits: dict[str, set[str]] = defaultdict(set)
    contributors: dict[str, set[str]] = defaultdict(set)
    defects: dict[str, set[str]] = defaultdict(set)
    commit = ""
    author = ""
    is_defect = False
    for raw in result.stdout.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("@@"):
            fields = line[2:].split("\t", 2)
            commit = fields[0]
            author = fields[1] if len(fields) > 1 else "unknown"
            subject = fields[2] if len(fields) > 2 else ""
            is_defect = bool(re.search(r"\b(?:fix|bug|revert|rollback|panic|deadlock|race)\b", subject, re.I))
            continue
        if not commit or not line.endswith(".rs"):
            continue
        path = line.replace("\\", "/")
        commits[path].add(commit)
        contributors[path].add(author)
        if is_defect:
            defects[path].add(commit)
    return {
        path: History(len(values), len(contributors[path]), len(defects[path]))
        for path, values in commits.items()
    }


def score_metrics(
    lines: int,
    public_items: int,
    lock_sites: int,
    state_owners: int,
    fan_out: int,
    tests: int,
    history: History,
) -> float:
    score = (
        lines / 40
        + history.commits * 2.5
        + history.contributors * 4
        + history.defects * 5
        + public_items * 1.5
        + lock_sites * 1.25
        + state_owners * 5
        + fan_out * 2
        + math.sqrt(tests) * 2
    )
    return round(score, 3)


def scan_file(root: Path, path: Path, history: History) -> FileMetrics:
    relative_path = path.relative_to(root)
    relative = normalized(relative_path)
    source = path.read_text(encoding="utf-8")
    production = production_projection(source)
    masked = rust_source.mask_comments_and_literals(production)
    # Documentation and comments are required quality controls, not
    # module-complexity growth.
    production_lines = production_code_lines(source)
    public_items = len(PUBLIC_ITEM.findall(masked))
    reexports = len(REEXPORT.findall(masked))
    lock_sites = len(LOCK_SITE.findall(masked))
    state_owners = len(STATE_OWNER.findall(masked))
    fan_out = len({left or right for left, right in USE_TARGET.findall(masked)})
    tests = len(TEST_FUNCTION.findall(source))
    return FileMetrics(
        path=relative,
        crate=crate_name(relative_path),
        production_lines=production_lines,
        public_items=public_items,
        reexports=reexports,
        lock_sites=lock_sites,
        state_owners=state_owners,
        fan_out=fan_out,
        test_functions=tests,
        churn_commits=history.commits,
        contributors=history.contributors,
        defect_commits=history.defects,
        score=score_metrics(
            production_lines,
            public_items,
            lock_sites,
            state_owners,
            fan_out,
            tests,
            history,
        ),
    )


def scan_tree(root: Path) -> list[FileMetrics]:
    history = git_history(root)
    metrics = []
    for path in rust_source.production_sources(root):
        relative = path.relative_to(root)
        if is_test_only_file(relative):
            continue
        key = normalized(relative)
        metrics.append(scan_file(root, path, history.get(key, History())))
    return sorted(metrics, key=lambda item: (-item.score, -item.production_lines, item.path))


def validate_decision_ledger(hotspots: list[FileMetrics], document: str) -> list[Finding]:
    """Require an explicit, reviewable decision for every ranked hotspot."""
    headings = list(DECISION_HEADING.finditer(document))
    sections: dict[str, list[dict[str, str]]] = defaultdict(list)
    for index, heading in enumerate(headings):
        end = headings[index + 1].start() if index + 1 < len(headings) else len(document)
        fields = {key.lower(): value.strip() for key, value in DECISION_FIELD.findall(document[heading.end() : end])}
        sections[heading.group(1)].append(fields)

    findings = []
    required = {"decision", "owner", "state owner", "evidence", "revisit when"}
    for hotspot in hotspots:
        entries = sections.get(hotspot.path, [])
        if not entries:
            findings.append(Finding("missing-hotspot-decision", hotspot.path, "ranked hotspot lacks an ADR decision"))
            continue
        if len(entries) != 1:
            findings.append(
                Finding("duplicate-hotspot-decision", hotspot.path, f"expected one ADR section, found {len(entries)}")
            )
            continue
        fields = entries[0]
        decision = fields.get("decision", "").rstrip(".").strip("`")
        incomplete = required - fields.keys()
        if decision not in {"decomposed", "retained"}:
            incomplete.add("decision=decomposed|retained")
        if any(len(fields.get(field, "").strip()) < 12 for field in required - {"decision"}):
            incomplete.add("substantive field content")
        if incomplete:
            findings.append(
                Finding(
                    "incomplete-hotspot-decision",
                    hotspot.path,
                    f"missing or invalid: {', '.join(sorted(incomplete))}",
                )
            )
    return findings


def inferred_use_cases(path: str) -> list[str]:
    stem = Path(path).stem.replace("_", " ")
    if "consumer" in path:
        return ["lifecycle", "assignment and routing", "message flow", "offset and shutdown"]
    if "commit_log" in path:
        return ["append", "read and segment lifecycle", "flush and replication", "recovery"]
    if "bootstrap" in path:
        return ["validation", "composition and listener ownership", "readiness and shutdown"]
    if "grpc" in path or "proxy" in path:
        return ["request adaptation", "data-path dispatch", "stream lifecycle", "status mapping"]
    if "admin" in path:
        return ["request validation", "admin dispatch", "response projection", "error mapping"]
    if "config" in path:
        return ["typed configuration", "validation", "serialization", "compatibility projection"]
    return [stem, "lifecycle", "request execution", "result projection"]


def governance(metrics: FileMetrics) -> dict[str, Any]:
    owner = f"{metrics.crate} maintainers"
    state_owner = (
        f"`{Path(metrics.path).stem}` remains the single composition owner; "
        "child use-case modules may receive narrow references but must not expose lock guards."
    )
    return {
        "path": metrics.path,
        "owner": owner,
        "use_cases": inferred_use_cases(metrics.path),
        "state_owner": state_owner,
        "test_strategy": (
            f"Run focused `{metrics.crate}` behavior tests, strict Clippy, "
            "stable-surface checks, and affected standalone consumers."
        ),
        "removal_condition": (
            "Remove from the ranked board after production LOC is at most 800, "
            "public surface does not grow, and independent child fixtures cover each extracted use case."
        ),
        "metrics": asdict(metrics),
    }


def baseline_payload(metrics: list[FileMetrics]) -> dict[str, Any]:
    files = {
        item.path: {
            "production_lines": item.production_lines,
            "public_items": item.public_items,
            "reexports": item.reexports,
        }
        for item in sorted(metrics, key=lambda item: item.path)
    }
    crate_totals: dict[str, dict[str, int]] = defaultdict(lambda: {"public_items": 0, "reexports": 0})
    for item in metrics:
        crate_totals[item.crate]["public_items"] += item.public_items
        crate_totals[item.crate]["reexports"] += item.reexports
    return {
        "schema_version": 2,
        "thresholds": {
            "review_lines": REVIEW_LINES,
            "hard_lines": HARD_LINES,
            "ranked_hotspots": RANKED_HOTSPOTS,
        },
        "policy": {
            "line_growth": "Existing hotspots may shrink but not grow; new production modules must stay at or below 800 lines.",
            "public_surface": "Per-file and per-crate public item and re-export counts may shrink but not grow without review.",
            "ownership": "State owners, lock sites, and dependency fan-out may shrink but not grow without review.",
            "ranking": "Ranking combines production LOC, history, contributors, defects, public surface, state/lock ownership, fan-out, and test cost.",
        },
        "files": files,
        "crate_public_surface": dict(sorted(crate_totals.items())),
        "hotspots": [governance(item) for item in metrics[:RANKED_HOTSPOTS]],
    }


def validate_schema(payload: dict[str, Any]) -> list[Finding]:
    findings = []
    if payload.get("schema_version") != 2:
        return [Finding("baseline-schema", "scripts/module-maintainability-baseline.json", "expected schema 2")]
    thresholds = payload.get("thresholds", {})
    if thresholds != {
        "review_lines": REVIEW_LINES,
        "hard_lines": HARD_LINES,
        "ranked_hotspots": RANKED_HOTSPOTS,
    }:
        findings.append(Finding("baseline-thresholds", str(BASELINE), f"unexpected thresholds={thresholds}"))
    hotspots = payload.get("hotspots")
    required = {"path", "owner", "use_cases", "state_owner", "test_strategy", "removal_condition", "metrics"}
    if not isinstance(hotspots, list) or len(hotspots) != RANKED_HOTSPOTS:
        findings.append(Finding("hotspot-count", str(BASELINE), f"expected {RANKED_HOTSPOTS} governed hotspots"))
        return findings
    for entry in hotspots:
        path = str(entry.get("path", "<invalid>"))
        if set(entry) != required:
            findings.append(Finding("hotspot-schema", path, "unexpected governance fields"))
            continue
        values = [entry["owner"], entry["state_owner"], entry["test_strategy"], entry["removal_condition"]]
        if any(not isinstance(value, str) or len(value.strip()) < 12 for value in values):
            findings.append(Finding("hotspot-governance", path, "owner and contracts must be explicit"))
        if not isinstance(entry["use_cases"], list) or len(entry["use_cases"]) < 2:
            findings.append(Finding("hotspot-use-cases", path, "at least two use-case boundaries are required"))
        metrics = entry.get("metrics")
        required_metrics = {"lock_sites", "state_owners", "fan_out"}
        if not isinstance(metrics, dict) or not required_metrics.issubset(metrics):
            findings.append(Finding("hotspot-metrics-schema", path, "ownership and fan-out metrics are required"))
    return findings


def compare(metrics: list[FileMetrics], baseline: dict[str, Any]) -> list[Finding]:
    findings = validate_schema(baseline)
    files = baseline.get("files", {})
    governed_metrics = {
        entry.get("path"): entry.get("metrics", {})
        for entry in baseline.get("hotspots", [])
        if isinstance(entry, dict)
    }
    governed = set(governed_metrics)
    for item in metrics:
        if Path(item.path).name in FORBIDDEN_FRAGMENT_NAMES:
            findings.append(
                Finding(
                    "mechanical-module-fragment",
                    item.path,
                    "use a domain use-case name instead of a numbered or miscellaneous fragment",
                )
            )
        previous = files.get(item.path)
        if previous is None:
            if item.production_lines > HARD_LINES:
                findings.append(Finding("new-oversized-module", item.path, f"lines={item.production_lines} max={HARD_LINES}"))
            if item.public_items or item.reexports:
                findings.append(
                    Finding(
                        "new-public-surface",
                        item.path,
                        f"public_items={item.public_items} reexports={item.reexports}",
                    )
                )
            continue
        previous_lines = int(previous["production_lines"])
        if previous_lines > HARD_LINES and item.production_lines > previous_lines:
            findings.append(
                Finding("hotspot-growth", item.path, f"baseline={previous_lines} current={item.production_lines}")
            )
        if previous_lines <= HARD_LINES and item.production_lines > HARD_LINES:
            findings.append(
                Finding("module-crossed-hard-limit", item.path, f"baseline={previous_lines} current={item.production_lines}")
            )
        for field in ("public_items", "reexports"):
            if int(getattr(item, field)) > int(previous[field]):
                findings.append(
                    Finding(
                        "public-surface-growth",
                        item.path,
                        f"{field} baseline={previous[field]} current={getattr(item, field)}",
                    )
                )
        for field, code in (
            ("lock_sites", "lock-site-growth"),
            ("state_owners", "state-owner-growth"),
            ("fan_out", "fan-out-growth"),
        ):
            governed_previous = governed_metrics.get(item.path)
            baseline_value = governed_previous.get(field) if governed_previous is not None else None
            if baseline_value is not None and int(getattr(item, field)) > int(baseline_value):
                findings.append(
                    Finding(
                        code,
                        item.path,
                        f"{field} baseline={baseline_value} current={getattr(item, field)}",
                    )
                )
    for item in metrics[:RANKED_HOTSPOTS]:
        if item.path not in governed:
            findings.append(Finding("ungoverned-ranked-hotspot", item.path, "ranked file lacks ownership contract"))

    current_crates: dict[str, dict[str, int]] = defaultdict(lambda: {"public_items": 0, "reexports": 0})
    for item in metrics:
        current_crates[item.crate]["public_items"] += item.public_items
        current_crates[item.crate]["reexports"] += item.reexports
    for crate, current in current_crates.items():
        previous = baseline.get("crate_public_surface", {}).get(crate)
        if previous is None:
            if current["public_items"] or current["reexports"]:
                findings.append(Finding("new-public-crate", crate, f"surface={current}"))
            continue
        for field in ("public_items", "reexports"):
            if current[field] > int(previous[field]):
                findings.append(
                    Finding(
                        "crate-public-surface-growth",
                        crate,
                        f"{field} baseline={previous[field]} current={current[field]}",
                    )
                )
    return findings


def render_report(payload: dict[str, Any]) -> str:
    lines = [
        "# Module maintainability board",
        "",
        "<!-- Generated by scripts/module_maintainability_guard.py. Do not edit manually. -->",
        "",
        "Ranking is deliberately multi-factor: production LOC, twelve-month churn and contributors,",
        "defect/revert history, public surface, lock and state ownership, dependency fan-out, and test cost.",
        "A high rank is a review priority, not evidence that line count alone is a defect.",
        "",
        "| Rank | File | Score | Production LOC | Churn | Authors | Defects | Public | Locks | State owners | Fan-out |",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for rank, entry in enumerate(payload["hotspots"], start=1):
        metric = entry["metrics"]
        lines.append(
            f"| {rank} | `{entry['path']}` | {metric['score']:.3f} | {metric['production_lines']} | "
            f"{metric['churn_commits']} | {metric['contributors']} | {metric['defect_commits']} | "
            f"{metric['public_items'] + metric['reexports']} | {metric['lock_sites']} | "
            f"{metric['state_owners']} | {metric['fan_out']} |"
        )
    lines.extend(["", "## Ownership and extraction contracts", ""])
    for entry in payload["hotspots"]:
        use_cases = ", ".join(f"`{value}`" for value in entry["use_cases"])
        lines.extend(
            [
                f"### `{entry['path']}`",
                "",
                f"- Owner: {entry['owner']}.",
                f"- Use-case boundaries: {use_cases}.",
                f"- State ownership: {entry['state_owner']}",
                f"- Tests: {entry['test_strategy']}",
                f"- Exit: {entry['removal_condition']}",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--baseline", type=Path, default=BASELINE)
    parser.add_argument("--report", type=Path, default=REPORT)
    parser.add_argument("--decisions", type=Path, default=DECISIONS)
    parser.add_argument("--write-baseline", action="store_true")
    parser.add_argument("--write-report", action="store_true")
    args = parser.parse_args()

    root = args.root.resolve()
    metrics = scan_tree(root)
    try:
        decision_document = args.decisions.read_text(encoding="utf-8")
    except OSError:
        decision_document = ""
    decision_findings = validate_decision_ledger(metrics[:RANKED_HOTSPOTS], decision_document)
    if args.write_baseline:
        if decision_findings:
            for finding in decision_findings:
                print(f"MODULE_FINDING code={finding.code} path={finding.path} detail={finding.detail}", file=sys.stderr)
            print(f"MODULE_MAINTAINABILITY_FAILED findings={len(decision_findings)}", file=sys.stderr)
            return 1
        payload = baseline_payload(metrics)
        write_json(args.baseline, payload)
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(render_report(payload), encoding="utf-8")
        print(
            f"MODULE_MAINTAINABILITY_BASELINE_WRITTEN files={len(metrics)} "
            f"hotspots={RANKED_HOTSPOTS}"
        )
        return 0

    try:
        baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        print(f"MODULE_MAINTAINABILITY_FAILED baseline={error}", file=sys.stderr)
        return 1
    findings = compare(metrics, baseline)
    findings.extend(decision_findings)
    expected_report = render_report(baseline)
    if args.write_report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(expected_report, encoding="utf-8")
    elif not args.report.is_file() or args.report.read_text(encoding="utf-8") != expected_report:
        findings.append(Finding("stale-report", normalized(args.report), "run with --write-report"))

    if findings:
        for finding in findings:
            print(f"MODULE_FINDING code={finding.code} path={finding.path} detail={finding.detail}", file=sys.stderr)
        print(f"MODULE_MAINTAINABILITY_FAILED findings={len(findings)}", file=sys.stderr)
        return 1
    oversized = sum(item.production_lines > HARD_LINES for item in metrics)
    print(
        f"MODULE_MAINTAINABILITY_OK files={len(metrics)} oversized={oversized} "
        f"ranked={RANKED_HOTSPOTS}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
