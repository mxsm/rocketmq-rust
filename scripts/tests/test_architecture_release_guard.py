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
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))

import architecture_release_guard as guard  # noqa: E402


class ArchitectureReleaseGuardTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.plan = json.loads(guard.PLAN_PATH.read_text(encoding="utf-8"))
        cls.policy = json.loads(guard.POLICY_PATH.read_text(encoding="utf-8"))
        cls.baseline = json.loads(guard.BASELINE_PATH.read_text(encoding="utf-8"))

    def test_live_release_package_passes(self) -> None:
        result = subprocess.run(
            [sys.executable, str(SCRIPTS / "architecture_release_guard.py")],
            cwd=ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("R1 29, next-major 4, long-term 2", result.stdout)
        self.assertIn("12/12 absent, 3/3 canonical replacements", result.stdout)
        self.assertIn(
            "Phase 1-3/R01-R18,R20,R22-R25; "
            "R19/R21 and Phase 4 R26-R31 excluded",
            result.stdout,
        )
        self.assertIn("R25 closeout: four-party approved", result.stdout)
        self.assertIn("current remaining: none; current scope complete", result.stdout)

    def test_objective_scope_excludes_performance_platform_and_phase4_follow_up(
        self,
    ) -> None:
        findings: list[str] = []
        guard.check_objective_scope(self.plan, findings)
        self.assertEqual([], findings)
        self.assertEqual(
            guard.REQUIRED_OBJECTIVE_SCOPE,
            self.plan["objective_scope"],
        )
        self.assertEqual(
            guard.REQUIRED_CURRENT_SCOPE_CLOSEOUT,
            self.plan["current_scope_closeout"],
        )

        invalid = copy.deepcopy(self.plan)
        invalid["objective_scope"]["architecture_refactor_execution_items"].insert(
            18,
            "R19",
        )
        findings = []
        guard.check_objective_scope(invalid, findings)
        self.assertTrue(
            any("objective scope" in finding for finding in findings),
            findings,
        )

        checklist_path = (
            "docs/plans/architecture-refactor-migration/CHECKLIST.md"
        )
        checklist = (ROOT / checklist_path).read_text(encoding="utf-8")
        findings = []
        guard.check_objective_scope(
            self.plan,
            findings,
            source_overrides={
                checklist_path: checklist
                + "\n- [ ] R19: incorrectly reactivated\n"
            },
        )
        self.assertTrue(
            any(
                "excluded follow-up items must not be active refactor checkboxes"
                in finding
                for finding in findings
            ),
            findings,
        )

        invalid = copy.deepcopy(self.plan)
        invalid["current_scope_closeout"]["signatures"]["REV"] = "pending"
        findings = []
        guard.check_objective_scope(invalid, findings)
        self.assertIn(
            "R25 closeout must bind all four approvals to the frozen current-scope candidate",
            findings,
        )

    def test_publish_order_covers_target_dag_and_respects_dependencies(self) -> None:
        findings: list[str] = []
        guard.check_release_topology(self.plan, self.policy, findings)
        self.assertEqual([], findings)

        invalid = copy.deepcopy(self.plan)
        order = invalid["release_topology"]["publish_order"]
        order.remove("rocketmq-protocol")
        order.append("rocketmq-protocol")
        findings = []
        guard.check_release_topology(invalid, self.policy, findings)
        self.assertTrue(
            any("publish order violation" in finding for finding in findings),
            findings,
        )

    def test_release_windows_exactly_match_compatibility_ledger(self) -> None:
        self.assertEqual(29, len(guard.expand_r1_consumers(self.plan)))
        self.assertEqual(29, len(guard.baseline_edges(self.baseline, "R1")))
        self.assertEqual(4, len(guard.baseline_edges(self.baseline, "next-major")))
        self.assertEqual(2, len(guard.baseline_edges(self.baseline, "long-term")))

        invalid = copy.deepcopy(self.plan)
        invalid["r1"]["consumers"][0]["targets"].pop()
        findings: list[str] = []
        guard.check_release_windows(invalid, self.baseline, findings)
        self.assertIn(
            "R1 consumer plan must exactly match the 29-edge compatibility baseline",
            findings,
        )

    def test_ten_new_crates_and_release_documents_are_complete(self) -> None:
        self.assertEqual(
            guard.NEW_CRATES,
            {item["package"] for item in self.plan["r0"]["new_crates"]},
        )
        findings: list[str] = []
        guard.check_ci_and_documents(self.plan, findings)
        self.assertEqual([], findings)

    def test_proxy_next_major_features_are_not_activated_early(self) -> None:
        findings: list[str] = []
        guard.check_proxy_activation(self.plan, findings)
        self.assertEqual([], findings)

    def test_human_approval_explicitly_approves_early_removal(self) -> None:
        findings: list[str] = []
        guard.check_usage_and_approval(self.plan, findings)
        self.assertEqual([], findings)
        self.assertEqual(
            "approved-early-human-override",
            self.plan["human_approval"]["destructive_removal"],
        )

    def test_arc_mut_removal_inventory_and_replacements_are_complete(self) -> None:
        findings: list[str] = []
        guard.check_arc_mut_deprecations(self.plan, findings)
        self.assertEqual([], findings)
        self.assertEqual(
            guard.REQUIRED_ARC_MUT_DEPRECATIONS,
            {
                surface["id"]: (
                    surface["path"],
                    surface["declaration"],
                    surface["note"],
                )
                for surface in self.plan["arc_mut_deprecation"]["surfaces"]
            },
        )
        self.assertEqual(
            guard.REQUIRED_ARC_MUT_REPLACEMENTS,
            {
                replacement["id"]: (
                    replacement["path"],
                    replacement["declaration"],
                )
                for replacement in self.plan["arc_mut_deprecation"]["canonical_replacements"]
            },
        )

    def test_arc_mut_removal_contract_fails_closed(self) -> None:
        invalid = copy.deepcopy(self.plan)
        invalid["arc_mut_deprecation"]["surfaces"].pop()
        findings: list[str] = []
        guard.check_arc_mut_deprecations(invalid, findings)
        self.assertIn(
            "ArcMut removal inventory differs from the approved 12-surface contract",
            findings,
        )

        surface_id = "arc-mut-type"
        relative_path, declaration, _note = guard.REQUIRED_ARC_MUT_DEPRECATIONS[surface_id]
        findings = []
        guard.check_arc_mut_deprecations(
            self.plan,
            findings,
            source_overrides={relative_path: f"{declaration} {{}}\n"},
        )
        self.assertIn(
            f"ArcMut removed surface returned: {surface_id}",
            findings,
        )

        replacement_id = "timer-config-constructor"
        replacement_path, replacement_declaration = guard.REQUIRED_ARC_MUT_REPLACEMENTS[replacement_id]
        replacement_source = (ROOT / replacement_path).read_text(encoding="utf-8")
        replacement_index = replacement_source.index(replacement_declaration)
        deprecated_replacement = (
            replacement_source[:replacement_index]
            + '#[deprecated(since = "1.0.0", note = "invalid canonical replacement")]\n    '
            + replacement_source[replacement_index:]
        )
        findings = []
        guard.check_arc_mut_deprecations(
            self.plan,
            findings,
            source_overrides={replacement_path: deprecated_replacement},
        )
        self.assertIn(
            f"ArcMut canonical replacement is deprecated: {replacement_id}",
            findings,
        )


if __name__ == "__main__":
    unittest.main()
