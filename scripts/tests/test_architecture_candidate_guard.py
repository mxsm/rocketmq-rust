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
FIXTURE = SCRIPTS / "tests" / "fixtures" / "architecture-candidate" / "pass.json"
sys.path.insert(0, str(SCRIPTS))

import architecture_candidate_guard as guard  # noqa: E402


class ArchitectureCandidateGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.record = json.loads(FIXTURE.read_text(encoding="utf-8"))

    def test_lightweight_candidate_record_passes(self) -> None:
        self.assertEqual([], guard.validate(self.record))

    def test_required_candidate_fields_fail_closed(self) -> None:
        for field in ("commit", "environment", "checks", "known_failures"):
            with self.subTest(field=field):
                invalid = copy.deepcopy(self.record)
                del invalid[field]
                self.assertTrue(guard.validate(invalid))

    def test_heavy_production_evidence_fields_are_rejected(self) -> None:
        for field in ("policy_sha256", "artifact_hash", "image_digest", "signature", "promotion"):
            with self.subTest(field=field):
                invalid = copy.deepcopy(self.record)
                invalid[field] = "not-part-of-pr-static"
                findings = guard.validate(invalid)
                self.assertTrue(any("unexpected field" in finding for finding in findings))

    def test_failed_checks_and_known_failures_reject_the_candidate(self) -> None:
        invalid = copy.deepcopy(self.record)
        invalid["checks"][0]["status"] = "failed"
        invalid["known_failures"] = ["pr_static failed"]

        findings = guard.validate(invalid)

        self.assertTrue(any("must pass" in finding for finding in findings))
        self.assertTrue(any("must be empty" in finding for finding in findings))

    def test_cli_accepts_the_committed_fixture(self) -> None:
        result = subprocess.run(
            [sys.executable, str(SCRIPTS / "architecture_candidate_guard.py"), "--record", str(FIXTURE)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("ARCHITECTURE_CANDIDATE_OK", result.stdout)


if __name__ == "__main__":
    unittest.main()
