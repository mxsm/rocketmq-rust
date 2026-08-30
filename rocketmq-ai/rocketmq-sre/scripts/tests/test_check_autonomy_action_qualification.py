#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for the bounded-autonomy action qualification validator."""

from __future__ import annotations

import copy
import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "check_autonomy_action_qualification.py"
SPEC = importlib.util.spec_from_file_location("check_autonomy_action_qualification", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def valid_report() -> dict:
    actions = []
    for action_id in sorted(MODULE.EXPECTED_ACTIONS):
        actions.append(
            {
                "id": action_id,
                "outcomes": {outcome: "passed" for outcome in MODULE.EXPECTED_OUTCOMES},
                "lifecycle": {
                    "initial_mode": "disabled",
                    "final_mode": "supervised",
                    "shadow_samples": 20,
                    "supervised_successes": 5,
                    "observation_window_days": 7,
                    "shadow_cohorts": 1,
                    "supervised_cohorts": 1,
                    "same_family_critic_denied": True,
                    "autonomous_transition_executed": False,
                    "expected_deny_paused": False,
                    "execution_failure_paused": True,
                    "critic_transport": "offline_scripted",
                    "primary_model_family": "qualification-primary",
                    "critic_model_family": "qualification-critic",
                },
                "live": {"state": "succeeded", "target_mutations": 1},
                "recovery": {"verified_success": "succeeded"},
            }
        )
    return {
        "schema_version": MODULE.REPORT_SCHEMA,
        "revision": "a" * 40,
        "source_clean": True,
        "environment": "disposable_kind",
        "started_at": "2026-08-05T00:00:00Z",
        "finished_at": "2026-08-05T00:20:00Z",
        "status": "passed",
        "live_mode_ceiling": "supervised",
        "unattended_autonomous_execution": False,
        "model_provider_network_calls": 0,
        "secrets_recorded": False,
        "message_bodies_recorded": False,
        "actions": actions,
        "cleanup": {
            "status": "passed",
            "disposable_kind_destroyed": True,
            "owned_runtime_artifacts_removed": True,
            "qualification_fragments_removed": True,
            "target_state_restored": True,
        },
    }


class AutonomyActionQualificationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = MODULE.load_json(MODULE.DEFAULT_MANIFEST)

    def test_committed_manifest_is_valid(self) -> None:
        self.assertEqual(MODULE.validate_manifest(self.manifest), [])

    def test_rejects_action_drift_and_unsafe_live_autonomy(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["actions"].pop()
        manifest["live_mode_ceiling"] = "autonomous"
        manifest["unattended_autonomous_execution"] = True

        findings = MODULE.validate_manifest(manifest)

        self.assertTrue(any("action set drifted" in finding for finding in findings))
        self.assertIn("live_mode_ceiling must remain 'supervised'", findings)
        self.assertIn("unattended_autonomous_execution must remain False", findings)

    def test_rejects_model_calls_and_same_family_critic(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["model_provider_network_calls"] = True
        manifest["offline_critic_fixture"]["critic_model_family"] = "qualification-primary"

        findings = MODULE.validate_manifest(manifest)

        self.assertIn("model_provider_network_calls must remain False", findings)
        self.assertIn("offline Critic fixture must use heterogeneous model families", findings)

    def test_valid_report_passes(self) -> None:
        self.assertEqual(MODULE.validate_report(valid_report()), [])

    def test_rejects_autonomous_transition_and_incomplete_counts(self) -> None:
        report = valid_report()
        lifecycle = report["actions"][0]["lifecycle"]
        lifecycle["final_mode"] = "autonomous"
        lifecycle["autonomous_transition_executed"] = True
        lifecycle["shadow_samples"] = 19

        findings = MODULE.validate_report(report)

        self.assertTrue(any("Disabled-to-Supervised" in finding for finding in findings))
        self.assertTrue(any("must not execute an Autonomous transition" in finding for finding in findings))
        self.assertTrue(any("sample counts drifted" in finding for finding in findings))

    def test_rejects_model_calls_sensitive_values_and_incomplete_cleanup(self) -> None:
        report = valid_report()
        report["model_provider_network_calls"] = 1
        report["unsafe"] = "Bearer qualification-token"
        report["cleanup"]["disposable_kind_destroyed"] = False

        findings = MODULE.validate_report(report)

        self.assertIn("report model_provider_network_calls must remain 0", findings)
        self.assertIn("report contains a credential-like value", findings)
        self.assertIn("cleanup proof is incomplete", findings)

    def test_rejects_non_passing_outcome_and_missing_action(self) -> None:
        report = valid_report()
        report["actions"][0]["outcomes"]["kill_switch_denied"] = "failed"
        report["actions"].pop()

        findings = MODULE.validate_report(report)

        self.assertTrue(any("non-passing" in finding for finding in findings))
        self.assertIn("report must contain each autonomy action exactly once", findings)


if __name__ == "__main__":
    unittest.main()
