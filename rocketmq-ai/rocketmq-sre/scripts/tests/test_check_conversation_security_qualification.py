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
"""Tests for the Conversation security qualification validator."""

from __future__ import annotations

import copy
import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "check_conversation_security_qualification.py"
SPEC = importlib.util.spec_from_file_location("check_conversation_security_qualification", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def valid_report() -> dict:
    return {
        "schema_version": MODULE.REPORT_SCHEMA,
        "status": "passed",
        "candidate_commit": "a" * 40,
        "source_clean": True,
        "started_at": "2026-08-06T00:00:00Z",
        "finished_at": "2026-08-06T00:01:00Z",
        "scenario_matrix": {
            "schema_version": MODULE.MANIFEST_SCHEMA,
            "scenario_count": 8,
            "passed_count": 8,
            "fixed_read_only_query_count": 4,
            "unsupported_count": 3,
            "rejected_count": 1,
            "scope_preserved": True,
            "tool_allowlist_preserved": True,
        },
        "citation_coverage": {
            "high_confidence_threshold_percent": 80,
            "high_confidence_conclusions": 1,
            "cited_high_confidence_conclusions": 1,
            "coverage_percent": 100.0,
        },
        "desktop_ui": {
            "browser": "chromium",
            "viewport_width": 1600,
            "viewport_height": 1000,
            "provisional_observed": True,
            "preview_reset_observed": True,
            "unsafe_preview_persisted": False,
            "safe_terminal_persisted": True,
            "authorized_citation_visible": True,
            "execution_eligible": False,
        },
        "safety": {
            "effective_access": "read_only",
            "mutation_calls": 0,
            "executor_calls": 0,
            "execution_agent_calls": 0,
            "secrets_recorded": False,
            "prompts_recorded": False,
            "responses_recorded": False,
            "message_bodies_recorded": False,
        },
    }


class ConversationSecurityQualificationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = MODULE.load_json(MODULE.DEFAULT_MANIFEST)

    def test_committed_manifest_is_valid(self) -> None:
        self.assertEqual(MODULE.validate_manifest(self.manifest), [])

    def test_valid_report_passes(self) -> None:
        self.assertEqual(MODULE.validate_report(valid_report()), [])

    def test_rejects_incomplete_scenario_matrix(self) -> None:
        report = valid_report()
        report["scenario_matrix"]["passed_count"] = 7

        self.assertIn(
            "scenario_matrix.passed_count must be 8",
            MODULE.validate_report(report),
        )

    def test_rejects_incomplete_citation_coverage(self) -> None:
        report = valid_report()
        report["citation_coverage"]["cited_high_confidence_conclusions"] = 0
        report["citation_coverage"]["coverage_percent"] = 0.0

        findings = MODULE.validate_report(report)

        self.assertIn(
            "citation_coverage.cited_high_confidence_conclusions must equal high_confidence_conclusions",
            findings,
        )
        self.assertIn("citation_coverage.coverage_percent must be 100.0", findings)

    def test_rejects_persisted_preview_or_execution_authority(self) -> None:
        report = valid_report()
        report["desktop_ui"]["unsafe_preview_persisted"] = True
        report["desktop_ui"]["execution_eligible"] = True

        findings = MODULE.validate_report(report)

        self.assertIn("desktop_ui.unsafe_preview_persisted must be False", findings)
        self.assertIn("desktop_ui.execution_eligible must be False", findings)

    def test_rejects_mutation_or_sensitive_payload(self) -> None:
        report = valid_report()
        report["safety"]["mutation_calls"] = 1
        report["model_prompt"] = "Bearer qualification-token"

        findings = MODULE.validate_report(report)

        self.assertIn("safety.mutation_calls must be 0", findings)
        self.assertIn("report contains a forbidden sensitive payload field", findings)
        self.assertIn("report contains a credential-like value", findings)

    def test_manifest_rejects_a_weakened_boundary(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["required_assertions"]["mutation_calls"] = 1

        self.assertIn(
            "required Conversation security assertions drifted",
            MODULE.validate_manifest(manifest),
        )


if __name__ == "__main__":
    unittest.main()
