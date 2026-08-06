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
"""Tests for independent operator handoff qualification."""

from __future__ import annotations

import copy
import importlib.util
import unittest
from datetime import datetime, timezone
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "check_independent_operator_handoff_qualification.py"
SPEC = importlib.util.spec_from_file_location("check_independent_operator_handoff_qualification", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class IndependentOperatorHandoffQualificationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = MODULE.load_json(MODULE.DEFAULT_MANIFEST)
        cls.now = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)

    def valid_report(self) -> dict:
        incoming = "operator://target/incoming"
        return {
            "schema_version": "rocketmq-sre.independent-operator-handoff-report.v1",
            "handoff_ref": "handoff://target/primary",
            "environment_ref": "environment://target/primary",
            "source_revision": "a" * 40,
            "checklist_digest": MODULE.file_digest(MODULE.DEFAULT_CHECKLIST),
            "observed_at": "2026-08-07T11:55:00Z",
            "valid_until": "2026-09-06T11:55:00Z",
            "handoff_qualified": True,
            "production_certified": False,
            "grants_execution_authority": False,
            "unattended_autonomous_execution": False,
            "attestation": {
                "incoming_operator_ref": incoming,
                "outgoing_operator_ref": "operator://target/outgoing",
                "approval_ref": "approval://target/handoff-primary",
                "incoming_was_not_contributor": True,
                "incoming_confirmed_limitations": True,
                "outgoing_disclosed_limitations": True,
                "signed_at": "2026-08-07T11:55:00Z",
            },
            "exercise_results": [
                {
                    "exercise_id": exercise["id"],
                    "environment_scope": exercise["environment_scope"],
                    "status": "passed",
                    "performed_by": incoming,
                    "source_revision": "a" * 40,
                    "started_at": "2026-08-07T11:45:00Z",
                    "completed_at": "2026-08-07T11:50:00Z",
                    "evidence_digest": f"sha256:{index:064x}",
                    "independently_executed": True,
                    "unresolved_deviations": 0,
                }
                for index, exercise in enumerate(self.manifest["required_exercises"], start=1)
            ],
            "summary": {
                "expected_exercises": 8,
                "passed_exercises": 8,
                "independent_operator_signoff": True,
                "production_target_mutations": 0,
                "qualification_environment_only": True,
                "cleanup_completed": True,
                "model_provider_calls": 0,
                "credentials_recorded": False,
                "message_bodies_recorded": False,
                "raw_logs_recorded": False,
            },
        }

    def validate(self, report: dict, path: str = r"F:\rocketmq-sre-evidence\handoff\report.json") -> list[str]:
        return MODULE.validate_report(report, self.manifest, report_path=path, now=self.now)

    def test_committed_contract_is_valid_and_bound_to_checklist(self) -> None:
        self.assertEqual(MODULE.validate_manifest(self.manifest), [])
        self.assertEqual(len(self.manifest["required_exercises"]), 8)
        self.assertRegex(MODULE.file_digest(MODULE.DEFAULT_CHECKLIST), r"^sha256:[0-9a-f]{64}$")

    def test_accepts_complete_independent_handoff_report(self) -> None:
        self.assertEqual(self.validate(self.valid_report()), [])

    def test_rejects_missing_duplicate_and_unexpected_exercises(self) -> None:
        report = self.valid_report()
        report["exercise_results"].pop()
        report["exercise_results"].append(copy.deepcopy(report["exercise_results"][0]))
        report["exercise_results"][1]["exercise_id"] = "unexpected"

        findings = self.validate(report)

        self.assertTrue(any("missing required exercise" in finding for finding in findings))
        self.assertTrue(any("duplicate exercise result" in finding for finding in findings))
        self.assertTrue(any("unexpected exercise result" in finding for finding in findings))

    def test_rejects_non_independent_or_wrong_operator_execution(self) -> None:
        report = self.valid_report()
        report["exercise_results"][0]["performed_by"] = "operator://target/outgoing"
        report["exercise_results"][1]["independently_executed"] = False

        findings = self.validate(report)

        self.assertTrue(any("performed_by" in finding for finding in findings))
        self.assertTrue(any("independently_executed" in finding for finding in findings))

    def test_rejects_expired_overlong_or_invalid_exercise_times(self) -> None:
        report = self.valid_report()
        report["observed_at"] = "2026-08-01T00:00:00Z"
        report["valid_until"] = "2027-08-01T00:00:00Z"
        report["exercise_results"][0]["completed_at"] = "2026-08-07T13:00:00Z"

        findings = self.validate(report)

        self.assertTrue(any("validity window" in finding for finding in findings))
        self.assertTrue(any("expired or not currently valid" in finding for finding in findings))
        self.assertTrue(any("exercise time window" in finding for finding in findings))

    def test_rejects_same_operator_or_incomplete_attestation(self) -> None:
        report = self.valid_report()
        report["attestation"]["outgoing_operator_ref"] = report["attestation"]["incoming_operator_ref"]
        report["attestation"]["incoming_was_not_contributor"] = False
        report["attestation"]["approval_ref"] = "pending"

        findings = self.validate(report)

        self.assertTrue(any("must be distinct" in finding for finding in findings))
        self.assertTrue(any("incoming_was_not_contributor" in finding for finding in findings))
        self.assertTrue(any("approval_ref" in finding for finding in findings))

    def test_rejects_sensitive_content_and_personal_identifiers(self) -> None:
        report = self.valid_report()
        report["attestation"]["incoming_operator_ref"] = "operator@example.com"
        report["attestation"]["outgoing_operator_ref"] = "Bearer local-secret"

        findings = self.validate(report)

        self.assertTrue(any("personal identifier" in finding for finding in findings))
        self.assertTrue(any("sensitive material" in finding for finding in findings))

    def test_rejects_raw_endpoint_machine_path_and_extra_payload(self) -> None:
        report = self.valid_report()
        report["raw_output"] = r"D:\handoff\raw.log"
        report["exercise_results"][0]["endpoint"] = "https://internal.example"

        findings = self.validate(report)

        self.assertTrue(any("bounded report schema" in finding for finding in findings))
        self.assertTrue(any("raw endpoint" in finding for finding in findings))
        self.assertTrue(any("machine-local path" in finding for finding in findings))

    def test_rejects_report_outside_machine_local_roots(self) -> None:
        report = self.valid_report()

        outside = self.validate(report, r"C:\Users\operator\handoff.json")
        escaped = self.validate(report, r"F:\rocketmq-sre-evidence\..\handoff.json")

        self.assertTrue(any("allowed machine-local root" in finding for finding in outside))
        self.assertTrue(any("allowed machine-local root" in finding for finding in escaped))

    def test_rejects_checklist_or_revision_drift(self) -> None:
        report = self.valid_report()
        report["checklist_digest"] = "sha256:" + "0" * 64
        report["source_revision"] = "main"
        report["exercise_results"][0]["source_revision"] = "b" * 40

        findings = self.validate(report)

        self.assertTrue(any("checklist_digest" in finding for finding in findings))
        self.assertTrue(any("source_revision" in finding for finding in findings))

    def test_rejects_product_certification_authority_or_unsafe_summary(self) -> None:
        report = self.valid_report()
        report["production_certified"] = True
        report["grants_execution_authority"] = True
        report["unattended_autonomous_execution"] = True
        report["summary"]["production_target_mutations"] = 1
        report["summary"]["cleanup_completed"] = False

        findings = self.validate(report)

        self.assertTrue(any("production_certified" in finding for finding in findings))
        self.assertTrue(any("grants_execution_authority" in finding for finding in findings))
        self.assertTrue(any("unattended_autonomous_execution" in finding for finding in findings))
        self.assertTrue(any("handoff safety summary" in finding for finding in findings))

    def test_rejects_wrong_scope_failure_or_unresolved_deviation(self) -> None:
        report = self.valid_report()
        result = report["exercise_results"][0]
        result["environment_scope"] = "production_mutation"
        result["status"] = "partial"
        result["unresolved_deviations"] = 1

        findings = self.validate(report)

        self.assertTrue(any("environment_scope" in finding for finding in findings))
        self.assertTrue(any("must pass" in finding for finding in findings))
        self.assertTrue(any("unresolved_deviations" in finding for finding in findings))


if __name__ == "__main__":
    unittest.main()
