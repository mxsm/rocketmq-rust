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

"""Reconcile the cargo test and cargo nextest rounds recorded by the CI nextest pilot.

Every measured round is compared. A round diverges when the runners select different tests,
ignore different tests, run different doctests, or fail different tests. Failures are compared
by identity (test binary and test name, or doctest crate and doctest name), not only by count.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
import json
from pathlib import Path
import re
import xml.etree.ElementTree as ET


RUNNING = re.compile(r"^\s+Running .*\((?P<path>[^()]+)\)\s*$")
DOC_TESTS = re.compile(r"^\s+Doc-tests (?P<crate>\S+)\s*$")
RESULT = re.compile(r"^test result: \w+\. (?P<passed>\d+) passed; (?P<failed>\d+) failed; (?P<ignored>\d+) ignored;")
NEXTEST_SUMMARY = re.compile(r"^\s+Summary \[")
DETAIL_LIMIT = 10


@dataclass
class TargetResult:
    """The result line of one cargo test target, and the tests it reports as failed."""

    passed: int = 0
    failed: int = 0
    ignored: int = 0
    failures: tuple[str, ...] = ()
    completed: bool = False


@dataclass
class CargoRun:
    unit: dict[str, TargetResult] = field(default_factory=dict)
    doc: dict[str, TargetResult] = field(default_factory=dict)


@dataclass
class NextestRun:
    run: int
    skipped: int | None
    failures: set[tuple[str, str]]


def binary_ids(list_json: str) -> dict[str, str]:
    """Map test binary file names to nextest binary IDs from `cargo nextest list --message-format json`."""
    suites = json.loads(list_json)["rust-suites"]
    return {Path(suite["binary-path"]).name: binary_id for binary_id, suite in suites.items()}


def _failed_names(lines: list[str]) -> tuple[str, ...]:
    """Return the names listed in the `failures:` block that ends `lines`, if there is one."""
    index = len(lines) - 1
    while index >= 0 and not lines[index].strip():
        index -= 1
    names: list[str] = []
    while index >= 0 and lines[index].startswith("    "):
        names.append(lines[index].strip())
        index -= 1
    if index >= 0 and lines[index].strip() == "failures:":
        return tuple(reversed(names))
    return ()


def parse_cargo_log(text: str, ids: dict[str, str]) -> CargoRun:
    """Parse `cargo test` output into per-target results.

    Only the last `test result:` line of a target counts. Tests that re-run their own binary in a
    child process print the child's result first, followed by the target's own result.
    """
    run = CargoRun()
    current: TargetResult | None = None
    lines: list[str] = []
    for line in text.splitlines():
        if match := RUNNING.match(line):
            name = Path(match["path"]).name
            current = run.unit.setdefault(ids.get(name, name), TargetResult())
            lines = []
        elif match := DOC_TESTS.match(line):
            current = run.doc.setdefault(match["crate"], TargetResult())
            lines = []
        elif current is not None and (match := RESULT.match(line)):
            current.passed, current.failed, current.ignored = (
                int(match["passed"]), int(match["failed"]), int(match["ignored"]))
            current.failures = _failed_names(lines)
            current.completed = True
            lines = []
        elif current is not None:
            lines.append(line)
    return run


def parse_nextest(junit_xml: str, log: str) -> NextestRun:
    """Read executed tests and failures from nextest JUnit output and skipped tests from its summary."""
    run, failures = 0, set()
    for case in ET.fromstring(junit_xml).iter("testcase"):
        run += 1
        if any(child.tag in {"failure", "error"} for child in case):
            failures.add((case.get("classname", ""), case.get("name", "")))
    summaries = [line for line in log.splitlines() if NEXTEST_SUMMARY.match(line)]
    skipped = None
    if summaries:
        match = re.search(r"(\d+) skipped", summaries[-1])
        skipped = int(match[1]) if match else 0
    return NextestRun(run=run, skipped=skipped, failures=failures)


def _difference(label: str, cargo: set, nextest: set) -> list[str]:
    def render(items: set) -> str:
        shown = ", ".join(f"`{' '.join(item)}`" for item in sorted(items)[:DETAIL_LIMIT])
        return shown + (f" and {len(items) - DETAIL_LIMIT} more" if len(items) > DETAIL_LIMIT else "")

    problems = []
    if only_cargo := cargo - nextest:
        problems.append(f"{label} only under cargo test: {render(only_cargo)}")
    if only_nextest := nextest - cargo:
        problems.append(f"{label} only under the nextest run: {render(only_nextest)}")
    return problems


def reconcile_round(cargo: CargoRun, nextest: NextestRun, doc: CargoRun) -> tuple[dict[str, str], list[str]]:
    """Compare one round. Returns table cells and the divergences found."""
    problems = [f"cargo test target `{name}` did not report a test result"
                for name, result in cargo.unit.items() if not result.completed]
    problems += [f"doctests for `{name}` did not report a test result"
                 for runs in (cargo, doc) for name, result in runs.doc.items() if not result.completed]

    cargo_run = sum(result.passed + result.failed for result in cargo.unit.values())
    cargo_ignored = sum(result.ignored for result in cargo.unit.values())
    cargo_failures = {(target, name) for target, result in cargo.unit.items() for name in result.failures}
    if cargo_run != nextest.run:
        problems.append(f"tests run differ: cargo test {cargo_run}, nextest {nextest.run}")
    if nextest.skipped is None:
        problems.append("nextest summary line is missing")
    elif cargo_ignored != nextest.skipped:
        problems.append(f"ignored tests differ: cargo test {cargo_ignored}, nextest skipped {nextest.skipped}")
    if len(cargo_failures) != len(nextest.failures):
        problems.append(f"failed tests differ in count: cargo test {len(cargo_failures)}, "
                        f"nextest {len(nextest.failures)}")
    problems += _difference("failed", cargo_failures, nextest.failures)

    def doc_totals(run: CargoRun) -> tuple[int, int, set[tuple[str, str]]]:
        executed = sum(result.passed + result.failed for result in run.doc.values())
        ignored = sum(result.ignored for result in run.doc.values())
        failed = {(crate, name) for crate, result in run.doc.items() for name in result.failures}
        return executed, ignored, failed

    cargo_doc_run, cargo_doc_ignored, cargo_doc_failures = doc_totals(cargo)
    doc_run, doc_ignored, doc_failures = doc_totals(doc)
    if (cargo_doc_run, cargo_doc_ignored) != (doc_run, doc_ignored):
        problems.append(f"doctests differ: cargo test {cargo_doc_run} run / {cargo_doc_ignored} ignored, "
                        f"cargo test --doc {doc_run} run / {doc_ignored} ignored")
    problems += _difference("failed doctests", cargo_doc_failures, doc_failures)

    cells = {
        "tests": f"{cargo_run} / {nextest.run}",
        "ignored": f"{cargo_ignored} / {'?' if nextest.skipped is None else nextest.skipped}",
        "failed": f"{len(cargo_failures)} / {len(nextest.failures)}",
        "doctests": f"{cargo_doc_run} / {doc_run}",
        "failed_doctests": f"{len(cargo_doc_failures)} / {len(doc_failures)}",
    }
    return cells, problems


def compare(logs: Path, runs: int) -> tuple[str, bool]:
    """Reconcile every round under `logs`. Returns a Markdown report and whether all rounds agree."""
    list_path = logs / "nextest-list.json"
    ids = binary_ids(list_path.read_text(encoding="utf-8")) if list_path.exists() else {}
    rows = ["| Round | Tests run | Ignored / skipped | Failed | Doctests run | Failed doctests | Result |",
            "| --- | ---: | ---: | ---: | ---: | ---: | --- |"]
    details: list[str] = []
    for round_number in range(1, runs + 1):
        paths = {
            "cargo test log": logs / f"cargo-test-{round_number}.log",
            "nextest log": logs / f"nextest-{round_number}.log",
            "nextest JUnit report": logs / f"junit-{round_number}.xml",
            "cargo test --doc log": logs / f"doc-{round_number}.log",
        }
        if missing := [name for name, path in paths.items() if not path.exists()]:
            rows.append(f"| {round_number} | | | | | | diverged |")
            details.append(f"- Round {round_number}: missing {', '.join(missing)}")
            continue
        text = {name: path.read_text(encoding="utf-8", errors="replace") for name, path in paths.items()}
        cells, problems = reconcile_round(
            parse_cargo_log(text["cargo test log"], ids),
            parse_nextest(text["nextest JUnit report"], text["nextest log"]),
            parse_cargo_log(text["cargo test --doc log"], ids),
        )
        rows.append(f"| {round_number} | {cells['tests']} | {cells['ignored']} | {cells['failed']} | "
                    f"{cells['doctests']} | {cells['failed_doctests']} | {'diverged' if problems else 'match'} |")
        details += [f"- Round {round_number}: {problem}" for problem in problems]

    report = ["Reconciliation per round (`cargo test` / nextest; doctests: inside `cargo test` / "
              "separate `cargo test --doc`):", "", *rows]
    if details:
        report += ["", "Divergences:", "", *details]
    return "\n".join(report) + "\n", not details


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--logs", type=Path, required=True, help="Directory with the pilot's per-round logs")
    parser.add_argument("--runs", type=int, required=True, help="Number of measured rounds")
    args = parser.parse_args()
    report, agreed = compare(args.logs, args.runs)
    print(report, end="")
    return 0 if agreed else 1


if __name__ == "__main__":
    raise SystemExit(main())
