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
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
BASELINE_PATH = SCRIPTS / "v1-upgrade-baseline.json"


class V1UpgradeBaselineTests(unittest.TestCase):
    def load_baseline(self) -> dict[str, object]:
        self.assertTrue(BASELINE_PATH.is_file(), "upgrade baseline is missing")
        return json.loads(BASELINE_PATH.read_text(encoding="utf-8"))

    def validate_fixture(self, baseline: dict[str, object]) -> list[dict[str, str]]:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "baseline.json"
            path.write_text(json.dumps(baseline), encoding="utf-8")
            code = """
import json
from pathlib import Path
import sys
import v1_upgrade_baseline_guard as guard

baseline = guard.load_baseline(Path(sys.argv[1]))
findings = guard.validate_baseline(baseline, root=Path(sys.argv[2]))
print(json.dumps([finding.as_dict() for finding in findings]))
"""
            completed = subprocess.run(
                [sys.executable, "-c", code, str(path), str(ROOT)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
                env={**os.environ, "PYTHONPATH": str(SCRIPTS)},
            )
        self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
        return json.loads(completed.stdout)

    def test_live_v090_baseline_passes(self) -> None:
        completed = subprocess.run(
            [sys.executable, str(SCRIPTS / "v1_upgrade_baseline_guard.py"), "--baseline", str(BASELINE_PATH)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
        self.assertIn("V1_UPGRADE_BASELINE_OK writer=0.9.0 fixtures=2 transitions=3", completed.stdout)

    def test_baseline_has_real_store_and_broker_semantics(self) -> None:
        baseline = self.load_baseline()
        fixtures = {fixture["fixture_id"]: fixture for fixture in baseline["fixtures"]}

        self.assertEqual({"localfile-store", "broker-metadata"}, set(fixtures))
        store = fixtures["localfile-store"]["expected_records"]
        self.assertEqual("V1UpgradeTopic", store["topic"])
        self.assertEqual(["v0.9.0-message-0", "v0.9.0-message-1"], store["message_bodies"])
        self.assertEqual(2, store["queue_max_offset"])
        broker = fixtures["broker-metadata"]["expected_records"]
        self.assertEqual("V1UpgradeGroup", broker["group"])
        self.assertEqual(1, broker["consumer_offset"])
        self.assertEqual("inactive-default", broker["timer_state"])
        self.assertEqual("legacy-absent", broker["pop_profile_state"])

    def test_content_digest_fields_are_forbidden(self) -> None:
        text = BASELINE_PATH.read_text(encoding="utf-8").lower()
        for forbidden in ('"sha', '"hash', '"digest', 'checksum'):
            self.assertNotIn(forbidden, text)

    def test_wrong_writer_version_is_rejected(self) -> None:
        baseline = self.load_baseline()
        baseline["upgrade_from"]["version"] = "1.0.0"
        findings = self.validate_fixture(baseline)
        self.assertIn("writer-version-invalid", {item["code"] for item in findings})

    def test_missing_fixture_file_is_rejected(self) -> None:
        baseline = self.load_baseline()
        baseline["fixtures"][0]["files"].append("rocketmq-store/tests/fixtures/upgrade/v0.9.0/missing")
        findings = self.validate_fixture(baseline)
        self.assertIn("fixture-file-missing", {item["code"] for item in findings})

    def test_missing_expected_records_is_rejected(self) -> None:
        baseline = self.load_baseline()
        baseline["fixtures"][0]["expected_records"] = {}
        findings = self.validate_fixture(baseline)
        self.assertIn("expected-records-missing", {item["code"] for item in findings})

    def test_unsafe_downgrade_contract_is_rejected(self) -> None:
        baseline = self.load_baseline()
        baseline["format_transitions"][0]["downgrade"] = "old-reader-opens-new-format"
        findings = self.validate_fixture(baseline)
        self.assertIn("downgrade-contract-unsafe", {item["code"] for item in findings})

    def test_change_control_requires_both_reviewers_and_reason(self) -> None:
        baseline = self.load_baseline()
        baseline["change_control"] = {"store_reviewer": "", "broker_reviewer": "", "reason": ""}
        findings = self.validate_fixture(baseline)
        self.assertIn("change-control-invalid", {item["code"] for item in findings})

    def test_baseline_never_runs_the_current_writer(self) -> None:
        baseline = self.load_baseline()
        commands = " ".join(baseline["generation"]["commands"])
        self.assertIn("v0.9.0", commands)
        self.assertNotIn("cargo run --release", commands)
        self.assertEqual("read-only-test-input", baseline["generation"]["regeneration_policy"])


if __name__ == "__main__":
    unittest.main()
