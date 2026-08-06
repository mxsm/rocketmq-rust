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
"""Tests for the live Conversation qualification validator."""

from __future__ import annotations

import copy
import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "check_live_conversation_qualification.py"
SPEC = importlib.util.spec_from_file_location("check_live_conversation_qualification", SCRIPT)
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
        "environment": MODULE.ENVIRONMENT,
        "model": {
            "provider": MODULE.PROVIDER,
            "online_provider_calls": 0,
            "production_certified": False,
        },
        "stream": {
            "schema_version": MODULE.STREAM_SCHEMA,
            "session_count": 2,
            "accepted_count": 2,
            "terminal_count": 2,
            "provisional_delta_count": 2,
            "event_count": 10,
            "max_frame_bytes": 4096,
            "max_response_bytes": 1024 * 1024,
            "sequence_verified": True,
            "terminal_unique": True,
            "disconnect_cancellation_contract_tested": True,
        },
        "consumer_lag": {
            "source": "rocketmq-mcp",
            "resource": "consumer-lag/group/topic",
            "total_lag": 10,
            "citation_authorized": True,
            "persisted": True,
            "diagnostic_pack": "consumer-lag.v2",
        },
        "broker_runtime": {
            "source": "rocketmq-mcp",
            "resource": "broker-runtime/rocketmq-dev-broker",
            "broker_up": True,
            "broker_rows": 1,
            "active_broker_rows": 1,
            "citation_authorized": True,
            "persisted": True,
            "diagnostic_pack": "broker-health.v1",
        },
        "safety": {
            "mutation_calls": 0,
            "executor_calls": 0,
            "execution_agent_calls": 0,
            "effective_access": "read_only",
            "secrets_recorded": False,
            "prompts_recorded": False,
            "responses_recorded": False,
            "message_bodies_recorded": False,
        },
    }


class LiveConversationQualificationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = MODULE.load_json(MODULE.DEFAULT_MANIFEST)

    def test_committed_manifest_is_valid(self) -> None:
        self.assertEqual(MODULE.validate_manifest(self.manifest), [])

    def test_valid_report_passes(self) -> None:
        self.assertEqual(MODULE.validate_report(valid_report()), [])

    def test_rejects_rules_only_or_unpersisted_evidence(self) -> None:
        report = valid_report()
        report["consumer_lag"]["persisted"] = False
        report["consumer_lag"]["citation_authorized"] = False

        findings = MODULE.validate_report(report)

        self.assertIn("consumer_lag.persisted must be True", findings)
        self.assertIn("consumer_lag.citation_authorized must be True", findings)

    def test_rejects_unbounded_or_invalid_stream(self) -> None:
        report = valid_report()
        report["stream"]["terminal_count"] = 3
        report["stream"]["max_frame_bytes"] = 256 * 1024 + 1
        report["stream"]["provisional_delta_count"] = 0

        findings = MODULE.validate_report(report)

        self.assertIn("stream.terminal_count must be 2", findings)
        self.assertIn("stream.max_frame_bytes must be between one and 262144", findings)
        self.assertIn("stream.provisional_delta_count must be at least two", findings)

    def test_rejects_mutation_or_inactive_broker(self) -> None:
        report = valid_report()
        report["safety"]["mutation_calls"] = 1
        report["broker_runtime"]["broker_up"] = False

        findings = MODULE.validate_report(report)

        self.assertIn("safety.mutation_calls must be 0", findings)
        self.assertIn("broker_runtime.broker_up must be True", findings)

    def test_rejects_sensitive_payload(self) -> None:
        report = valid_report()
        report["response_body"] = "Bearer qualification-token"

        findings = MODULE.validate_report(report)

        self.assertIn("report contains a forbidden sensitive payload field", findings)
        self.assertIn("report contains a credential-like value", findings)

    def test_manifest_rejects_weakened_read_only_assertion(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["required_assertions"]["executor_calls"] = 1

        self.assertIn("required live Conversation assertions drifted", MODULE.validate_manifest(manifest))


if __name__ == "__main__":
    unittest.main()
