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
"""Tests for the asymmetric Executor partition qualification validator."""

from __future__ import annotations

import copy
import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "check_asymmetric_executor_partition_qualification.py"
SPEC = importlib.util.spec_from_file_location("check_asymmetric_executor_partition_qualification", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def valid_report() -> dict:
    return {
        "schema_version": MODULE.REPORT_SCHEMA,
        "candidate_commit": "a" * 40,
        "source_clean": True,
        "environment": MODULE.ENVIRONMENT,
        "started_at": "2026-08-06T00:00:00Z",
        "finished_at": "2026-08-06T00:01:00Z",
        "status": "passed",
        "connectivity": {
            "old_executor_authority_reachable_after_partition": False,
            "old_executor_agent_reachable_after_partition": True,
            "agent_authority_reachable_during_takeover": True,
        },
        "fencing": {
            "old_epoch": 1,
            "active_epoch": 2,
            "stale_dispatch_rejected": True,
            "stale_effect_rows": 0,
            "stale_target_writes": 0,
            "fresh_target_writes": 1,
            "fence_rejections": 1,
        },
        "safety": {
            "model_provider_network_calls": 0,
            "production_certified": False,
            "unattended_autonomous_execution": False,
            "secrets_recorded": False,
            "message_bodies_recorded": False,
        },
        "cleanup": {
            "postgres_container_removed": True,
            "database_url_cleared": True,
        },
    }


class AsymmetricExecutorPartitionQualificationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = MODULE.load_json(MODULE.DEFAULT_MANIFEST)

    def test_committed_manifest_is_valid(self) -> None:
        self.assertEqual(MODULE.validate_manifest(self.manifest), [])

    def test_valid_report_passes(self) -> None:
        self.assertEqual(MODULE.validate_report(valid_report()), [])

    def test_rejects_stale_and_duplicate_writes(self) -> None:
        report = valid_report()
        report["fencing"]["stale_target_writes"] = 1
        report["fencing"]["fresh_target_writes"] = 2

        findings = MODULE.validate_report(report)

        self.assertIn("fencing.stale_target_writes must be 0", findings)
        self.assertIn("fencing.fresh_target_writes must be 1", findings)

    def test_rejects_symmetric_connectivity_or_weak_epoch_proof(self) -> None:
        report = valid_report()
        report["connectivity"]["old_executor_agent_reachable_after_partition"] = False
        report["fencing"]["active_epoch"] = 1
        report["fencing"]["fence_rejections"] = 0

        findings = MODULE.validate_report(report)

        self.assertIn("asymmetric connectivity proof is incomplete", findings)
        self.assertIn("active_epoch must be an integer greater than old_epoch", findings)
        self.assertIn("fencing.fence_rejections must be at least one", findings)

    def test_rejects_production_claim_and_sensitive_value(self) -> None:
        report = valid_report()
        report["safety"]["production_certified"] = True
        report["unsafe"] = "Bearer qualification-token"

        findings = MODULE.validate_report(report)

        self.assertIn("safety proof drifted from the rules-only non-production boundary", findings)
        self.assertIn("report contains a credential-like value", findings)

    def test_manifest_rejects_weakened_assertion(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["required_assertions"]["stale_effect_rows"] = 1

        self.assertIn(
            "required asymmetric-partition assertions drifted",
            MODULE.validate_manifest(manifest),
        )


if __name__ == "__main__":
    unittest.main()
