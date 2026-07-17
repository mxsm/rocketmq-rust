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

    def test_human_approval_does_not_approve_early_removal(self) -> None:
        findings: list[str] = []
        guard.check_usage_and_approval(self.plan, findings)
        self.assertEqual([], findings)
        self.assertEqual(
            "pending-next-major-evidence-gate",
            self.plan["human_approval"]["destructive_removal"],
        )


if __name__ == "__main__":
    unittest.main()
