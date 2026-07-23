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

"""Positive-fixture and deliberate-violation tests for R24 evidence."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
GUARD = REPO_ROOT / "scripts" / "architecture_slo_guard.py"
FIXTURE = Path("scripts/tests/fixtures/m11-slo/pass")


class ArchitectureSloGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="m11-slo-")
        self.root = Path(self.temporary.name)
        paths = (
            "distribution/config/architecture-production-readiness-policy.json",
            "distribution/config/grafana-architecture-production-readiness.json",
            "distribution/config/prometheus-architecture-production-readiness-alerts.yaml",
            "distribution/kubernetes/fault-matrix-policy.json",
            "rocketmq-doc/en/architecture-production-readiness-runbook.md",
            "scripts/telemetry-semantic-registry.json",
            "scripts/run-architecture-slo-evidence.ps1",
            "scripts/architecture_slo_guard.py",
            "scripts/tests/test_architecture_slo_guard.py",
            ".github/workflows/architecture-slo-evidence.yml",
        )
        for relative in paths:
            source = REPO_ROOT / relative
            target = self.root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
        shutil.copytree(REPO_ROOT / FIXTURE, self.root / FIXTURE)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @property
    def run_path(self) -> Path:
        return self.root / FIXTURE / "run.json"

    def run_guard(
        self, *arguments: str, expect_success: bool
    ) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            [sys.executable, str(GUARD), "--root", str(self.root), *arguments],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
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
        self.run_path.write_text(
            json.dumps(run, indent=2) + "\n", encoding="utf-8"
        )

    def test_policy_and_positive_fixture_pass(self) -> None:
        self.run_guard("--policy-only", expect_success=True)
        self.run_guard(
            "--evidence", str(FIXTURE), "--allow-fixture", expect_success=True
        )

    def test_fixture_requires_explicit_opt_in(self) -> None:
        result = self.run_guard(
            "--evidence", str(FIXTURE), expect_success=False
        )
        self.assertIn("--allow-fixture", result.stderr)

    def test_non_dynamic_production_evidence_is_rejected(self) -> None:
        self.mutate_run(lambda run: run.update(fixture=False))
        result = self.run_guard(
            "--evidence", str(FIXTURE), expect_success=False
        )
        self.assertIn("dynamic execution", result.stderr)

    def test_short_soak_is_rejected(self) -> None:
        self.mutate_run(
            lambda run: run.update(finished_at="2026-01-01T05:59:59Z")
        )
        result = self.run_guard(
            "--evidence", str(FIXTURE), "--allow-fixture", expect_success=False
        )
        self.assertIn("soak duration", result.stderr)

    def test_failed_objective_is_rejected(self) -> None:
        self.mutate_run(
            lambda run: run["objectives"][1].update(observed=1001)
        )
        result = self.run_guard(
            "--evidence", str(FIXTURE), "--allow-fixture", expect_success=False
        )
        self.assertIn("send_message_p99_millis", result.stderr)

    def test_fault_candidate_mismatch_is_rejected(self) -> None:
        fault = self.root / FIXTURE / "artifacts" / "fault-run.json"
        value = json.loads(fault.read_text(encoding="utf-8"))
        value["candidate_images"]["broker"] = (
            "registry.example.invalid/rocketmq/broker@sha256:"
            "89abcdef0123456789abcdef0123456789abcdef0123456789abcdef01234567"
        )
        fault.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
        result = self.run_guard(
            "--evidence", str(FIXTURE), "--allow-fixture", expect_success=False
        )
        self.assertIn("candidate image maps differ", result.stderr)

    def test_artifact_hash_mismatch_is_rejected(self) -> None:
        artifact = self.root / FIXTURE / "artifacts" / "objectives.txt"
        artifact.write_text("tampered\n", encoding="utf-8")
        result = self.run_guard(
            "--evidence", str(FIXTURE), "--allow-fixture", expect_success=False
        )
        self.assertIn("artifact hash mismatch", result.stderr)

    def test_unregistered_dashboard_metric_is_rejected(self) -> None:
        dashboard = (
            self.root
            / "distribution/config/grafana-architecture-production-readiness.json"
        )
        source = dashboard.read_text(encoding="utf-8")
        dashboard.write_text(
            source.replace(
                "rocketmq_consumer_lag_messages",
                "rocketmq_unregistered_release_metric",
                1,
            ),
            encoding="utf-8",
        )
        result = self.run_guard("--policy-only", expect_success=False)
        self.assertIn("unregistered metric", result.stderr)

    def test_release_artifact_hash_mismatch_is_rejected(self) -> None:
        self.mutate_run(
            lambda run: run["release_artifacts"].update(
                {
                    "rocketmq-doc/en/architecture-production-readiness-runbook.md": "f"
                    * 64
                }
            )
        )
        result = self.run_guard(
            "--evidence", str(FIXTURE), "--allow-fixture", expect_success=False
        )
        self.assertIn("release artifact hash mismatch", result.stderr)


if __name__ == "__main__":
    unittest.main()
