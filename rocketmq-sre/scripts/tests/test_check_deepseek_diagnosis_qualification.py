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
"""Tests for the DeepSeek AI SRE diagnosis qualification validator."""

from __future__ import annotations

import copy
import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "check_deepseek_diagnosis_qualification.py"
SPEC = importlib.util.spec_from_file_location("check_deepseek_diagnosis_qualification", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def valid_report() -> dict:
    return {
        "schema_version": MODULE.REPORT_SCHEMA,
        "candidate_commit": "a" * 40,
        "source_clean": True,
        "environment": MODULE.ENVIRONMENT,
        "provider": "deepseek",
        "protocol": "responses_api",
        "model": "deepseek-v4-flash",
        "started_at": "2026-08-06T00:00:00Z",
        "finished_at": "2026-08-06T00:01:00Z",
        "status": "passed",
        "diagnosis": {
            "mode": "model_assisted",
            "authorized_evidence_citations": True,
            "cited_evidence_count": 1,
            "input_tokens_present": True,
            "output_tokens_present": True,
            "schema_repairs": 0,
            "model_network_calls": 4,
            "invocation_persisted": True,
            "stream_sessions": 2,
            "completed_semantic_streams": 1,
            "stream_event_count": 4,
            "stream_terminal_verified": True,
            "stream_cancellation_verified": True,
            "read_only_tool_selections": 1,
            "tool_execution_calls": 0,
            "mutation_calls": 0,
            "execution_eligible": False,
        },
        "safety": {
            "production_certified": False,
            "unattended_autonomous_execution": False,
            "secrets_recorded": False,
            "prompts_recorded": False,
            "responses_recorded": False,
            "message_bodies_recorded": False,
        },
        "cleanup": {
            "postgres_container_removed": True,
            "database_url_cleared": True,
            "api_key_environment_cleared": True,
        },
    }


class DeepSeekDiagnosisQualificationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = MODULE.load_json(MODULE.DEFAULT_MANIFEST)

    def test_committed_manifest_is_valid(self) -> None:
        self.assertEqual(MODULE.validate_manifest(self.manifest), [])

    def test_valid_report_passes(self) -> None:
        self.assertEqual(MODULE.validate_report(valid_report()), [])

    def test_rejects_unbounded_or_non_model_assisted_run(self) -> None:
        report = valid_report()
        report["diagnosis"]["mode"] = "rules_only"
        report["diagnosis"]["model_network_calls"] = 6
        report["diagnosis"]["schema_repairs"] = 2

        findings = MODULE.validate_report(report)

        self.assertIn("diagnosis.mode must be 'model_assisted'", findings)
        self.assertIn("diagnosis.model_network_calls must be between four and five", findings)
        self.assertIn("diagnosis.schema_repairs must be between zero and one", findings)

    def test_rejects_mutation_or_unauthorized_citation(self) -> None:
        report = valid_report()
        report["diagnosis"]["authorized_evidence_citations"] = False
        report["diagnosis"]["mutation_calls"] = 1

        findings = MODULE.validate_report(report)

        self.assertIn("diagnosis.authorized_evidence_citations must be True", findings)
        self.assertIn("diagnosis.mutation_calls must be 0", findings)

    def test_rejects_sensitive_payload_and_production_claim(self) -> None:
        report = valid_report()
        report["response_body"] = "Bearer qualification-token"
        report["safety"]["production_certified"] = True

        findings = MODULE.validate_report(report)

        self.assertIn("report contains a forbidden sensitive payload field", findings)
        self.assertIn("report contains a credential-like value", findings)
        self.assertIn("safety proof drifted from the supervised read-only boundary", findings)

    def test_manifest_rejects_weakened_read_only_assertion(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["required_assertions"]["tool_execution_calls"] = 1

        self.assertIn("required DeepSeek diagnosis assertions drifted", MODULE.validate_manifest(manifest))


if __name__ == "__main__":
    unittest.main()
