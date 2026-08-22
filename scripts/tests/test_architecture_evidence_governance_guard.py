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

import copy
import json
import subprocess
import sys
import unittest

from scripts import architecture_evidence_governance_guard as guard
from scripts import run_property_state_suites as property_runner


class ArchitectureEvidenceGovernanceGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.risk = json.loads((guard.ROOT / guard.RISK_MATRIX).read_text(encoding="utf-8"))
        self.debt = json.loads((guard.ROOT / guard.DEBT_REGISTRY).read_text(encoding="utf-8"))
        self.properties = json.loads((guard.ROOT / guard.PROPERTY_REGISTRY).read_text(encoding="utf-8"))
        self.fuzz = json.loads((guard.ROOT / guard.FUZZ_REGISTRY).read_text(encoding="utf-8"))

    def test_live_governance_package_passes(self) -> None:
        result = subprocess.run(
            [sys.executable, str(guard.ROOT / "scripts/architecture_evidence_governance_guard.py")],
            cwd=guard.ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("ARCHITECTURE_EVIDENCE_GOVERNANCE_OK", result.stdout)
        self.assertIn(f"risk={len(self.risk['entries'])}", result.stdout)

    def test_live_risk_matrix_tracks_every_registered_debt(self) -> None:
        self.assertEqual(len(self.debt["entries"]), len(self.risk["entries"]))
        self.assertEqual(15, len(self.risk["entries"]))

    def test_missing_debt_mapping_is_rejected(self) -> None:
        invalid = copy.deepcopy(self.risk)
        invalid["entries"].pop()
        findings = guard.validate_risk_matrix(guard.ROOT, invalid, self.debt)
        self.assertIn("risk-coverage", {finding.code for finding in findings})

    def test_line_coverage_only_is_rejected_for_high_risk(self) -> None:
        invalid = copy.deepcopy(self.risk)
        entry = invalid["entries"][0]
        entry["risk_class"] = "critical"
        entry["tests"][0]["kind"] = "line-coverage"
        findings = guard.validate_risk_matrix(guard.ROOT, invalid, self.debt)
        self.assertIn("risk-depth", {finding.code for finding in findings})

    def test_property_suite_requires_a_replayable_seed(self) -> None:
        invalid = copy.deepcopy(self.properties)
        invalid["suites"][0]["seed"] = "random"
        findings = guard.validate_property_registry(guard.ROOT, invalid)
        self.assertIn("property-seed", {finding.code for finding in findings})

    def test_property_suite_requires_an_exact_non_shell_command(self) -> None:
        invalid = copy.deepcopy(self.properties)
        invalid["suites"][0]["command"].remove("--exact")
        findings = guard.validate_property_registry(guard.ROOT, invalid)
        self.assertIn("property-command", {finding.code for finding in findings})

    def test_property_runner_distinguishes_one_test_from_zero_test_passes(self) -> None:
        zero = "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 31 filtered out"
        one = "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 30 filtered out"

        self.assertEqual(0, property_runner.successful_test_count(zero))
        self.assertEqual(1, property_runner.successful_test_count(one))

    def test_fuzz_timeout_is_not_classified_as_a_crash(self) -> None:
        invalid = copy.deepcopy(self.fuzz)
        invalid["crash_policy"]["timeout"] = invalid["crash_policy"]["crash"]
        findings = guard.validate_fuzz_registry(guard.ROOT, invalid)
        self.assertIn("fuzz-crash-policy", {finding.code for finding in findings})


if __name__ == "__main__":
    unittest.main()
