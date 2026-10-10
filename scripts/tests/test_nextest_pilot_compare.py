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

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from xml.sax.saxutils import escape

from scripts import nextest_pilot_compare


BINARIES = {
    "demo-crate": "target/debug/deps/demo_crate-0123456789abcdef",
    "demo-crate::integration": "target/debug/deps/integration-fedcba9876543210",
}
TESTS = {
    "demo-crate": ["unit::existing_failure", "unit::passes"],
    "demo-crate::integration": ["regression_on_nextest", "stable"],
}
IGNORED = {"demo-crate": 1, "demo-crate::integration": 0}
DOCTESTS = ["src/lib.rs - api (line 3)", "src/lib.rs - api::Item (line 9)"]


def cargo_target(header: str, tests: list[str], failed: set[str], ignored: int) -> list[str]:
    lines = [header, "", f"running {len(tests) + ignored} tests"]
    lines += [f"test {name} ... {'FAILED' if name in failed else 'ok'}" for name in tests]
    lines += ["test ignored_case ... ignored"] * ignored
    status = "FAILED" if failed else "ok"
    if failed:
        lines += ["", "failures:", ""]
        for name in sorted(failed):
            lines += [f"---- {name} stdout ----", f"thread '{name}' panicked at src/lib.rs:1:1:", ""]
        lines += ["", "failures:", *[f"    {name}" for name in sorted(failed)]]
    lines += ["", f"test result: {status}. {len(tests) - len(failed)} passed; {len(failed)} failed; "
                  f"{ignored} ignored; 0 measured; 0 filtered out; finished in 0.01s", ""]
    return lines


def doc_section(failed: set[str]) -> list[str]:
    return cargo_target("   Doc-tests demo_crate", DOCTESTS, failed, 0)


def cargo_log(failed: set[tuple[str, str]], doc_failed: set[str] = frozenset()) -> str:
    lines: list[str] = []
    for binary_id, path in BINARIES.items():
        source = "unittests src/lib.rs" if "::" not in binary_id else "tests/integration.rs"
        names = {name for target, name in failed if target == binary_id}
        lines += cargo_target(f"     Running {source} ({path})", TESTS[binary_id], names, IGNORED[binary_id])
    return "\n".join(lines + doc_section(set(doc_failed)))


def junit(failed: set[tuple[str, str]], skip: tuple[str, str] | None = None) -> str:
    cases = []
    for binary_id, names in TESTS.items():
        for name in names:
            if (binary_id, name) == skip:
                continue
            failure = '<failure message="panicked" type="test failure"/>' if (binary_id, name) in failed else ""
            cases.append(f'<testcase name="{escape(name)}" classname="{binary_id}">{failure}</testcase>')
    return f'<testsuites name="nextest-run"><testsuite name="demo">{"".join(cases)}</testsuite></testsuites>'


def nextest_log(failed: set[tuple[str, str]], run: int = 4, skipped: int = 1) -> str:
    return (f"    Starting {run} tests across 2 binaries ({skipped} test skipped)\n"
            f"     Summary [   1.000s] {run} tests run: {run - len(failed)} passed, "
            f"{len(failed)} failed, {skipped} skipped\n")


class NextestPilotCompareTests(unittest.TestCase):
    existing = ("demo-crate", "unit::existing_failure")
    regression = ("demo-crate::integration", "regression_on_nextest")

    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory(prefix="nextest-pilot-")
        self.addCleanup(directory.cleanup)
        self.logs = Path(directory.name)
        suites = {binary_id: {"binary-path": f"/work/{path}"} for binary_id, path in BINARIES.items()}
        (self.logs / "nextest-list.json").write_text(json.dumps({"rust-suites": suites}), encoding="utf-8")

    def write_round(self, number: int, *, cargo_failed=frozenset(), nextest_failed=frozenset(),
                    cargo_doc_failed=frozenset(), doc_failed=frozenset(), skip=None) -> None:
        run = 4 - (1 if skip else 0)
        (self.logs / f"cargo-test-{number}.log").write_text(cargo_log(set(cargo_failed), cargo_doc_failed))
        (self.logs / f"nextest-{number}.log").write_text(nextest_log(set(nextest_failed), run=run))
        (self.logs / f"junit-{number}.xml").write_text(junit(set(nextest_failed), skip))
        (self.logs / f"doc-{number}.log").write_text("\n".join(doc_section(set(doc_failed))))

    def compare(self, runs: int = 3) -> tuple[str, bool]:
        return nextest_pilot_compare.compare(self.logs, runs)

    def test_rounds_with_the_same_shared_failure_agree(self) -> None:
        for number in (1, 2, 3):
            self.write_round(number, cargo_failed={self.existing}, nextest_failed={self.existing})
        report, agreed = self.compare()
        self.assertTrue(agreed, report)
        self.assertEqual(3, report.count("| match |"))

    def test_early_nextest_only_failure_fails_even_when_later_rounds_match(self) -> None:
        self.write_round(1, nextest_failed={self.regression})
        self.write_round(2)
        self.write_round(3)
        report, agreed = self.compare()
        self.assertFalse(agreed)
        self.assertIn("Round 1: failed only under the nextest run: "
                      "`demo-crate::integration regression_on_nextest`", report)
        self.assertNotIn("Round 2:", report)

    def test_equal_failure_counts_with_different_failed_tests_diverge(self) -> None:
        for number in (1, 2, 3):
            self.write_round(number, cargo_failed={self.existing}, nextest_failed={self.regression})
        report, agreed = self.compare()
        self.assertFalse(agreed)
        self.assertNotIn("differ in count", report)
        self.assertIn("Round 3: failed only under cargo test: `demo-crate unit::existing_failure`", report)
        self.assertIn("Round 3: failed only under the nextest run: "
                      "`demo-crate::integration regression_on_nextest`", report)

    def test_doctest_failures_are_compared_by_identity(self) -> None:
        self.write_round(1, cargo_doc_failed={DOCTESTS[0]}, doc_failed={DOCTESTS[1]})
        report, agreed = self.compare(runs=1)
        self.assertFalse(agreed)
        self.assertIn(f"failed doctests only under cargo test: `demo_crate {DOCTESTS[0]}`", report)
        self.assertIn(f"failed doctests only under the nextest run: `demo_crate {DOCTESTS[1]}`", report)

    def test_different_test_selection_diverges(self) -> None:
        self.write_round(1, skip=("demo-crate::integration", "stable"))
        report, agreed = self.compare(runs=1)
        self.assertFalse(agreed)
        self.assertIn("Round 1: tests run differ: cargo test 4, nextest 3", report)

    def test_missing_round_output_diverges(self) -> None:
        self.write_round(1)
        report, agreed = self.compare(runs=2)
        self.assertFalse(agreed)
        self.assertIn("Round 2: missing cargo test log", report)

    def test_target_without_a_result_line_diverges(self) -> None:
        self.write_round(1)
        log = self.logs / "cargo-test-1.log"
        log.write_text(log.read_text().split("     Running tests/integration.rs")[0]
                       + f"     Running tests/integration.rs ({BINARIES['demo-crate::integration']})\n"
                       + "error: test failed, to rerun pass `--test integration`\n"
                       + "\n".join(doc_section(set())))
        report, agreed = self.compare(runs=1)
        self.assertFalse(agreed)
        self.assertIn("cargo test target `demo-crate::integration` did not report a test result", report)

    def test_child_process_results_inside_a_target_are_not_counted_twice(self) -> None:
        child = ["running 1 test", "test probe ... ok", "",
                 "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 0.00s", ""]
        parsed = nextest_pilot_compare.parse_cargo_log(
            "\n".join([f"     Running tests/integration.rs ({BINARIES['demo-crate::integration']})", *child,
                       "test result: ok. 2 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; "
                       "finished in 0.01s"]),
            {Path(path).name: binary_id for binary_id, path in BINARIES.items()},
        )
        result = parsed.unit["demo-crate::integration"]
        self.assertEqual((2, 0, 1), (result.passed, result.failed, result.ignored))

    def test_command_line_exit_code_reports_divergence(self) -> None:
        self.write_round(1, nextest_failed={self.regression})
        result = subprocess.run(
            [sys.executable, "-m", "scripts.nextest_pilot_compare", "--logs", str(self.logs), "--runs", "1"],
            cwd=Path(nextest_pilot_compare.__file__).resolve().parents[1], capture_output=True, text=True,
        )
        self.assertEqual(1, result.returncode, result.stderr)
        self.assertIn("| 1 | 4 / 4 | 1 / 1 | 0 / 1 |", result.stdout)


if __name__ == "__main__":
    unittest.main()
