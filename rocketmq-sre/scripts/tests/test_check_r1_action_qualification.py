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
"""Tests for the R1 action qualification validator."""

from __future__ import annotations

import copy
import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "check_r1_action_qualification.py"
SPEC = importlib.util.spec_from_file_location("check_r1_action_qualification", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def valid_report() -> dict:
    actions = []
    for index, action_id in enumerate(sorted(MODULE.EXPECTED_ACTIONS), start=1):
        automatic = action_id in {
            "observability.logger_level_ttl.v1",
            "proxy.scale_out_one.v1",
        }
        actions.append(
            {
                "id": action_id,
                "outcomes": {outcome: "passed" for outcome in MODULE.EXPECTED_OUTCOMES},
                "live": {
                    "state": "succeeded",
                    "execution_id": f"0000000{index}-0000-4000-8000-00000000000{index}",
                    "correlation_id": f"1000000{index}-0000-4000-8000-00000000000{index}",
                    "approval_events": 1,
                    "intent_records": 1,
                    "result_records": 1,
                    "confirmed_agent_effects": 1,
                    "successful_verifications": 1,
                    "verification_evidence_records": 3,
                    "target_mutations": 1,
                },
                "recovery": {
                    "recovery_mode": "automatic_compensation" if automatic else "manual_takeover_safe_stop",
                    "verified_success": "succeeded",
                    "verification_failure": "rolled_back" if automatic else "escalated",
                    "rollback_failure": "escalated" if automatic else "not_applicable_manual_takeover",
                    "compensation_intents": 2,
                    "active_quarantines": 1,
                },
            }
        )
    return {
        "schema_version": MODULE.REPORT_SCHEMA,
        "revision": "a" * 40,
        "source_clean": True,
        "environment": "disposable_kind",
        "started_at": "2026-08-05T00:00:00Z",
        "finished_at": "2026-08-05T00:10:00Z",
        "status": "passed",
        "model_provider_network_calls": 0,
        "secrets_recorded": False,
        "message_bodies_recorded": False,
        "actions": actions,
        "cleanup": {
            "status": "passed",
            "proxy_replicas_restored": True,
            "logger_ttl_restored": True,
            "proxy_ready": True,
            "collector_ready": True,
            "owned_resources_removed": True,
        },
    }


class R1ActionQualificationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = MODULE.load_json(MODULE.DEFAULT_MANIFEST)

    def test_committed_manifest_is_valid(self) -> None:
        self.assertEqual(MODULE.validate_manifest(self.manifest), [])

    def test_rejects_model_network_calls_and_missing_action(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["model_provider_network_calls"] = True
        manifest["actions"].pop()

        findings = MODULE.validate_manifest(manifest)

        self.assertIn("model-provider network calls must remain disabled", findings)
        self.assertTrue(any("R1 action set drifted" in finding for finding in findings))

    def test_valid_report_passes(self) -> None:
        self.assertEqual(MODULE.validate_report(valid_report()), [])

    def test_rejects_partial_or_unrestored_report(self) -> None:
        report = valid_report()
        action = report["actions"][0]
        action["outcomes"]["verification_failure"] = "failed"
        action["live"]["target_mutations"] = 2
        report["cleanup"]["proxy_replicas_restored"] = False

        findings = MODULE.validate_report(report)

        self.assertTrue(any("non-passing" in finding for finding in findings))
        self.assertTrue(any("target_mutations" in finding for finding in findings))
        self.assertIn("cleanup proof is incomplete", findings)

    def test_rejects_ai_calls_and_sensitive_report_values(self) -> None:
        report = valid_report()
        report["model_provider_network_calls"] = 1
        report["unsafe"] = "Bearer qualification-token"

        findings = MODULE.validate_report(report)

        self.assertIn("report must prove zero model-provider network calls", findings)
        self.assertIn("report contains a credential-like value", findings)


if __name__ == "__main__":
    unittest.main()
