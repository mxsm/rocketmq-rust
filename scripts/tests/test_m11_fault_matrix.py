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

"""Positive-fixture and deliberate-violation tests for M11-11 evidence."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
GUARD = REPO_ROOT / "scripts" / "fault_matrix_guard.py"
FIXTURE = Path("scripts/tests/fixtures/m11-fault-matrix/pass")


class FaultMatrixGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="m11-fault-matrix-")
        self.root = Path(self.temporary.name)
        shutil.copytree(REPO_ROOT / "distribution", self.root / "distribution")
        (self.root / "docker").mkdir(parents=True)
        shutil.copy2(REPO_ROOT / "docker" / "Dockerfile.base", self.root / "docker" / "Dockerfile.base")
        (self.root / "scripts" / "tests" / "fixtures" / "m11-fault-matrix").mkdir(parents=True)
        shutil.copy2(
            REPO_ROOT / "scripts" / "kind-architecture-refactor-e2e.ps1",
            self.root / "scripts" / "kind-architecture-refactor-e2e.ps1",
        )
        shutil.copy2(
            REPO_ROOT / "scripts" / "tests" / "test_m11_fault_matrix.py",
            self.root / "scripts" / "tests" / "test_m11_fault_matrix.py",
        )
        shutil.copytree(
            REPO_ROOT / FIXTURE,
            self.root / FIXTURE,
        )
        (self.root / ".github" / "workflows").mkdir(parents=True)
        shutil.copy2(
            REPO_ROOT / ".github" / "workflows" / "kubernetes-fault-matrix.yml",
            self.root / ".github" / "workflows" / "kubernetes-fault-matrix.yml",
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @property
    def run_path(self) -> Path:
        return self.root / FIXTURE / "run.json"

    def run_guard(self, *arguments: str, expect_success: bool) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            [sys.executable, str(GUARD), "--root", str(self.root), *arguments],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        if expect_success:
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("guard passed", result.stdout)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertIn("ERROR:", result.stderr)
        return result

    def mutate_run(self, mutation) -> None:  # type: ignore[no-untyped-def]
        run = json.loads(self.run_path.read_text(encoding="utf-8"))
        mutation(run)
        self.run_path.write_text(json.dumps(run, indent=2) + "\n", encoding="utf-8")

    def test_policy_and_positive_fixture_pass(self) -> None:
        self.run_guard("--policy-only", expect_success=True)
        self.run_guard("--evidence", str(FIXTURE), "--allow-fixture", expect_success=True)

    def test_policy_hash_ignores_line_endings(self) -> None:
        policy = self.root / "distribution" / "kubernetes" / "fault-matrix-policy.json"
        policy.write_bytes(policy.read_bytes().replace(b"\n", b"\r\n"))
        self.run_guard("--evidence", str(FIXTURE), "--allow-fixture", expect_success=True)

    def test_fixture_requires_explicit_opt_in(self) -> None:
        result = self.run_guard("--evidence", str(FIXTURE), expect_success=False)
        self.assertIn("--allow-fixture", result.stderr)

    def test_non_dynamic_production_evidence_is_rejected(self) -> None:
        self.mutate_run(lambda run: run.update(fixture=False))
        result = self.run_guard("--evidence", str(FIXTURE), expect_success=False)
        self.assertIn("dynamic execution", result.stderr)

    def test_failed_global_assertion_is_rejected(self) -> None:
        self.mutate_run(lambda run: run["global_assertions"].update(rollback_verified=False))
        result = self.run_guard("--evidence", str(FIXTURE), "--allow-fixture", expect_success=False)
        self.assertIn("rollback_verified", result.stderr)

    def test_missing_scenario_is_rejected(self) -> None:
        self.mutate_run(lambda run: run["scenarios"].pop())
        result = self.run_guard("--evidence", str(FIXTURE), "--allow-fixture", expect_success=False)
        self.assertIn("exact ordered seven scenarios", result.stderr)

    def test_scenario_assertion_key_drift_is_rejected(self) -> None:
        self.mutate_run(lambda run: run["scenarios"][0]["assertions"].pop("queue_offset_preserved"))
        result = self.run_guard("--evidence", str(FIXTURE), "--allow-fixture", expect_success=False)
        self.assertIn("rolling_upgrade: assertion keys drifted", result.stderr)

    def test_placeholder_image_digest_is_rejected(self) -> None:
        self.mutate_run(
            lambda run: run["candidate_images"].update(
                broker="example.invalid/broker@sha256:" + "0" * 64
            )
        )
        result = self.run_guard("--evidence", str(FIXTURE), "--allow-fixture", expect_success=False)
        self.assertIn("placeholder digest", result.stderr)

    def test_unresolved_fault_is_rejected(self) -> None:
        self.mutate_run(lambda run: run.update(unresolved_faults=["worker-remains-cordoned"]))
        result = self.run_guard("--evidence", str(FIXTURE), "--allow-fixture", expect_success=False)
        self.assertIn("unresolved_faults", result.stderr)

    def test_artifact_hash_mismatch_is_rejected(self) -> None:
        artifact = self.root / FIXTURE / "artifacts" / "rolling_upgrade.txt"
        artifact.write_text("tampered\n", encoding="utf-8")
        result = self.run_guard("--evidence", str(FIXTURE), "--allow-fixture", expect_success=False)
        self.assertIn("artifact hash mismatch", result.stderr)

    def test_runner_that_drops_durability_marker_is_rejected(self) -> None:
        runner = self.root / "scripts" / "kind-architecture-refactor-e2e.ps1"
        source = runner.read_text(encoding="utf-8")
        self.assertIn("commitlog_offset_preserved", source)
        runner.write_text(source.replace("commitlog_offset_preserved", "offset_not_checked"), encoding="utf-8")
        result = self.run_guard("--policy-only", expect_success=False)
        self.assertIn("commitlog_offset_preserved", result.stderr)


if __name__ == "__main__":
    unittest.main()
