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

import hashlib
import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "scripts" / "run-architecture-release-rollback.ps1"
ROLLBACK_POLICY = ROOT / "distribution" / "kubernetes" / "rollback-policy.json"
MAINTENANCE_POLICY = ROOT / "distribution" / "config" / "maintenance-policy.json"
APPLY_ORDER = ["controller", "namesrv", "broker", "proxy", "mcp"]


class ReleaseRollbackTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.powershell = shutil.which("pwsh") or shutil.which("powershell")
        if cls.powershell is None:
            raise unittest.SkipTest("PowerShell is required for rollback runner tests")
        cls.policy = json.loads(ROLLBACK_POLICY.read_text(encoding="utf-8"))
        cls.maintenance_policy = json.loads(MAINTENANCE_POLICY.read_text(encoding="utf-8"))
        cls.runner_source = RUNNER.read_text(encoding="utf-8")

    def test_policy_pins_fail_closed_maintenance_authorization(self) -> None:
        reference = self.policy["maintenance_policy"]
        self.assertEqual(1, reference["version"])
        self.assertEqual(hashlib.sha256(MAINTENANCE_POLICY.read_bytes()).hexdigest(), reference["sha256"])
        self.assertTrue(self.maintenance_policy["require_authentication"])
        self.assertTrue(self.maintenance_policy["require_authorization"])
        self.assertTrue(self.maintenance_policy["require_fencing_token"])
        self.assertEqual(1, self.maintenance_policy["resource_budget"]["max_concurrent_operations"])

        grants = {
            grant["role"]: set(grant["capabilities"])
            for grant in self.maintenance_policy["role_grants"]
        }
        self.assertEqual({"release_checkpoint"}, grants["release_operator"])
        self.assertNotIn("release_checkpoint", grants.get("administrator", set()))

    def test_state_machine_exercises_failure_resume_and_reverse_compensation(self) -> None:
        result = self.run_validate()
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("RELEASE_ROLLBACK_VALIDATION_OK", result.stdout)
        self.assertIn("failure_cases=5", result.stdout)
        self.assertIn("resume_prefixes=6", result.stdout)
        self.assertIn("compensation_prefixes=21", result.stdout)
        self.assertEqual(APPLY_ORDER, self.policy["apply_order"])
        self.assertEqual(list(reversed(APPLY_ORDER)), self.policy["compensation_order"])

    def test_runner_uses_lease_fencing_and_resource_version_cas(self) -> None:
        for contract in (
            "coordination.k8s.io/v1",
            "metadata.resourceVersion",
            "Acquire-RollbackLease",
            "Renew-RollbackLease",
            "Save-JournalRecord",
            "Get-PendingApplyStages",
            "Get-PendingCompensationStages",
            "persistent_volume_uids",
            "acknowledged_messages_preserved",
            "consumer_offsets_preserved",
        ):
            self.assertIn(contract, self.runner_source)

        lowered = self.runner_source.lower()
        self.assertNotIn("helm uninstall", lowered)
        self.assertNotIn('"delete",\n            "persistentvolumeclaim"', lowered)
        self.assertTrue(all(self.policy["preservation"].values()))

    def test_validate_only_rejects_policy_order_drift(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rocketmq-rollback-policy-") as temporary:
            policy_path = Path(temporary) / "rollback-policy.json"
            tampered = json.loads(json.dumps(self.policy))
            tampered["compensation_order"] = APPLY_ORDER
            policy_path.write_text(json.dumps(tampered, indent=2) + "\n", encoding="utf-8")

            result = self.run_validate("-PolicyPath", str(policy_path))
            self.assertNotEqual(0, result.returncode)
            self.assertIn("compensation_order must exactly reverse apply_order", result.stderr)

    def test_validate_only_rejects_partial_artifact_binding(self) -> None:
        result = self.run_validate("-BaselineStatePath", "baseline.json")
        self.assertNotEqual(0, result.returncode)
        self.assertIn(
            "BaselineStatePath, CandidateStatePath, and CheckpointSetPath must be supplied together",
            result.stderr,
        )

    def run_validate(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                self.powershell,
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(RUNNER),
                "-ValidateOnly",
                *arguments,
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )


if __name__ == "__main__":
    unittest.main()
