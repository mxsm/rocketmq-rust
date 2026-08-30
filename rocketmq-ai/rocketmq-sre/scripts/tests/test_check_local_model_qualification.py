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
"""Tests for the credential-free local-model qualification validator."""

from __future__ import annotations

import copy
import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "check_local_model_qualification.py"
SPEC = importlib.util.spec_from_file_location("check_local_model_qualification", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def valid_report() -> dict:
    return {
        "schema_version": MODULE.REPORT_SCHEMA,
        "candidate_commit": "a" * 40,
        "source_clean": True,
        "environment": MODULE.ENVIRONMENT,
        "operating_mode": "supervised_read_only",
        "started_at": "2026-08-06T00:00:00Z",
        "finished_at": "2026-08-06T00:01:00Z",
        "status": "passed",
        "runtime": {
            "provider": "ollama",
            "protocol": "openai_compatible_chat_completions",
            "image": MODULE.IMAGE,
            "image_id": "sha256:" + "1" * 64,
            "model": MODULE.MODEL,
            "model_digest": "sha256:" + "2" * 64,
            "model_size_bytes": 398_000_000,
            "endpoint_scope": "loopback_only",
            "model_calls": 1,
            "response_non_empty": True,
            "response_bytes": 2,
            "tool_calls": 0,
            "credential_present": False,
            "input_tokens": 12,
            "output_tokens": 1,
            "artifact_download_network": True,
        },
        "safety": {
            "production_certified": False,
            "unattended_autonomous_execution": False,
            "external_model_provider_calls": 0,
            "target_mutations": 0,
            "executor_calls": 0,
            "execution_agent_calls": 0,
            "secrets_recorded": False,
            "prompts_recorded": False,
            "responses_recorded": False,
            "message_bodies_recorded": False,
        },
        "cleanup": {
            "container_removed": True,
            "volume_removed": True,
            "endpoint_environment_cleared": True,
            "model_environment_cleared": True,
            "image_preexisting_before": False,
            "image_state_restored": True,
        },
    }


class LocalModelQualificationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = MODULE.load_json(MODULE.DEFAULT_MANIFEST)

    def test_committed_manifest_is_valid(self) -> None:
        self.assertEqual(MODULE.validate_manifest(self.manifest), [])

    def test_valid_report_passes(self) -> None:
        self.assertEqual(MODULE.validate_report(valid_report()), [])

    def test_rejects_external_or_credentialed_runtime(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["runtime"]["endpoint_scope"] = "public"
        manifest["runtime"]["credential_required"] = True

        findings = MODULE.validate_manifest(manifest)

        self.assertIn("runtime contract drifted from pinned credential-free loopback Ollama", findings)

    def test_rejects_extra_model_calls_or_oversized_output(self) -> None:
        report = valid_report()
        report["runtime"]["model_calls"] = 2
        report["runtime"]["response_bytes"] = 4_097

        findings = MODULE.validate_report(report)

        self.assertIn("runtime.model_calls must be 1", findings)
        self.assertIn("runtime.response_bytes must be between one and 4096", findings)

    def test_rejects_mutation_production_or_external_provider_claims(self) -> None:
        report = valid_report()
        report["safety"]["production_certified"] = True
        report["safety"]["target_mutations"] = 1
        report["safety"]["external_model_provider_calls"] = 1

        self.assertIn(
            "safety proof drifted from the supervised local-only boundary",
            MODULE.validate_report(report),
        )

    def test_rejects_payload_machine_path_or_secret(self) -> None:
        report = valid_report()
        report["endpoint_url"] = "http://127.0.0.1:11434/v1"
        report["response_body"] = "Bearer qualification-token"

        findings = MODULE.validate_report(report)

        self.assertIn("report contains a forbidden payload or machine-local path field", findings)
        self.assertIn("report contains credential-like material", findings)

    def test_rejects_incomplete_cleanup(self) -> None:
        report = valid_report()
        report["cleanup"]["volume_removed"] = False

        self.assertIn("cleanup proof is incomplete", MODULE.validate_report(report))


if __name__ == "__main__":
    unittest.main()
