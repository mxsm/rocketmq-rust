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

import importlib.util
import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MATRIX_PATH = ROOT / "scripts/v1-functional-test-matrix.json"


def load_runner():
    path = ROOT / "scripts/core_release_checks.py"
    spec = importlib.util.spec_from_file_location("core_release_checks", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class CoreReleaseCheckRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.assertTrue(MATRIX_PATH.is_file(), "functional route matrix is missing")
        self.matrix = json.loads(MATRIX_PATH.read_text(encoding="utf-8"))

    def test_active_capabilities_and_runner_routes_are_bidirectionally_closed(self) -> None:
        manifest = json.loads((ROOT / "scripts/v1-capability-manifest.json").read_text(encoding="utf-8"))
        expected = {
            (item["capability_id"], test_id, command, item["target_phase"])
            for item in manifest["capabilities"]
            if item["completion_status"] != "deferred-by-scope"
            for test_id, command in zip(item["test_ids"], item["commands"], strict=True)
        }
        actual = {
            (item["capability_id"], item["test_id"], " ".join(item["argv"]), item["phase"])
            for item in self.matrix["capability_routes"]
        }

        self.assertEqual(expected, actual)
        self.assertEqual(len(actual), len(self.matrix["capability_routes"]))

    def test_core_routes_do_not_include_excluded_projects_or_content_identity_gates(self) -> None:
        serialized = json.dumps(self.matrix).lower()
        for excluded in ("rocketmq-mcp", "rocketmq-sre", "rocketmq-dashboard"):
            self.assertNotIn(excluded, serialized)
        for forbidden in ("sha256", "fingerprint", "architecture_dependency_guard", "architecture_release_guard"):
            self.assertNotIn(forbidden, serialized)

    def test_route_order_and_phase_five_static_contract_are_active(self) -> None:
        routes = self.matrix["fixed_routes"]
        order = [route["category"] for route in routes if route["phase"] == 0 and route["scope"] == "core"]
        self.assertEqual(
            ["scope", "capability", "inventory", "format", "clippy", "focused-test", "guard", "version", "routing", "diff"],
            order,
        )
        pending = [route for route in routes if route["status"] == "pending-phase5"]
        self.assertEqual([], pending)
        release_static = next(route for route in routes if route["id"] == "RELEASE-STATIC")
        self.assertEqual((5, "active", "core"), (release_static["phase"], release_static["status"], release_static["scope"]))

    def test_package_commands_are_generated_from_the_27_package_allowlist(self) -> None:
        runner = load_runner()
        scope = runner.load_scope(ROOT)
        format_argv = runner.expand_generated_command("core-format", scope)
        clippy_argv = runner.expand_generated_command("core-clippy", scope)

        self.assertEqual(27, format_argv.count("-p"))
        self.assertEqual(27, clippy_argv.count("-p"))
        self.assertNotIn("--workspace", format_argv)
        self.assertNotIn("--workspace", clippy_argv)

    def test_test_summary_rejects_zero_executed_all_ignored_and_failures(self) -> None:
        runner = load_runner()
        self.assertFalse(runner.test_summary("0 tests, 0 benchmarks", "test result: ok. 0 passed; 0 failed; 0 ignored").valid)
        self.assertFalse(runner.test_summary("one: test\n", "test result: ok. 0 passed; 0 failed; 1 ignored").valid)
        self.assertFalse(runner.test_summary("one: test\n", "test result: FAILED. 0 passed; 1 failed; 0 ignored").valid)
        summary = runner.test_summary("one: test\ntwo: test\n", "test result: ok. 2 passed; 0 failed; 0 ignored")
        self.assertTrue(summary.valid)
        self.assertEqual((2, 2, 2, 0, 0), (summary.matched, summary.executed, summary.passed, summary.failed, summary.ignored))

    def test_shell_entry_points_share_the_python_runner(self) -> None:
        powershell = (ROOT / "scripts/run-core-release-checks.ps1").read_text(encoding="utf-8")
        bash = (ROOT / "scripts/run-core-release-checks.sh").read_text(encoding="utf-8")

        self.assertIn("core_release_checks.py", powershell)
        self.assertIn("core_release_checks.py", bash)
        self.assertIn("--phase", powershell)
        self.assertIn("--phase", bash)
        self.assertIn("candidate_run.py validate", powershell)
        self.assertIn("candidate_run.py validate", bash)
        self.assertIn("release_evidence_guard.py", powershell)
        self.assertIn("release_evidence_guard.py", bash)

    def test_ci_has_required_core_job_and_legacy_identity_steps_are_report_only(self) -> None:
        workflow = (ROOT / ".github/workflows/rocketmq-rust-ci.yaml").read_text(encoding="utf-8")
        self.assertIn("core-release-short-checks:", workflow)
        self.assertIn("run: python scripts/core_release_static_guard.py", workflow)
        for name in ("Check dependency baseline", "Check dependency transition", "Check architecture release package"):
            block = workflow.split(f"- name: {name}", 1)[1].split("- name:", 1)[0]
            self.assertIn("continue-on-error: true", block)
        strict_target = workflow.split("- name: Run strict target and retain the debt report", 1)[1].split(
            "- name:", 1
        )[0]
        self.assertIn("continue-on-error: true", strict_target)


if __name__ == "__main__":
    unittest.main()
