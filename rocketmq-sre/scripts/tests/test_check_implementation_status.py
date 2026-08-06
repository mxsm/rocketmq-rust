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
"""Tests for the AI SRE implementation status validator."""

from __future__ import annotations

import copy
import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "check_implementation_status.py"
SPEC = importlib.util.spec_from_file_location("check_implementation_status", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ImplementationStatusTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = MODULE.load_manifest(MODULE.DEFAULT_MANIFEST)

    def test_checked_in_manifest_is_valid(self) -> None:
        self.assertEqual(
            MODULE.validate_manifest(self.manifest, MODULE.REPOSITORY_ROOT),
            [],
        )

    def test_rejects_completed_area_without_evidence(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["capability_areas"][0]["evidence"] = []

        findings = MODULE.validate_manifest(manifest, MODULE.REPOSITORY_ROOT)

        self.assertIn("capability_areas[0] requires implementation evidence", findings)

    def test_rejects_skipped_maturity_level(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        maturity = manifest["capability_areas"][0]["maturity"]
        maturity["contract_tested"] = False
        maturity["live_smoke_passed"] = True

        findings = MODULE.validate_manifest(manifest, MODULE.REPOSITORY_ROOT)

        self.assertIn("capability_areas[0].maturity cannot skip a lower level", findings)

    def test_rejects_credential_like_value(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["capability_areas"][0]["limitations"] = ["Bearer unsafe-example-token"]

        findings = MODULE.validate_manifest(manifest, MODULE.REPOSITORY_ROOT)

        self.assertIn("manifest contains a credential-like value", findings)

    def test_rejects_local_or_documentation_evidence(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["capability_areas"][0]["evidence"] = [
            {"kind": "test", "path": "C:/workspace/result.txt"},
            {"kind": "test", "path": "rocketmq-sre/docs/compatibility.md"},
        ]

        findings = MODULE.validate_manifest(manifest, MODULE.REPOSITORY_ROOT)

        self.assertTrue(any("repository-relative" in finding for finding in findings))
        self.assertTrue(any("not accepted" in finding for finding in findings))

    def test_rejects_evidence_baseline_drift(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["evidence_requirement_baseline"]["query"] = 17
        manifest["evidence_requirement_baseline"]["not_production_verified"] = 52

        findings = MODULE.validate_manifest(manifest, MODULE.REPOSITORY_ROOT)

        self.assertTrue(any("query does not match query_count" in finding for finding in findings))

    def test_rejects_incomplete_qualification_evidence(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["capability_areas"][0]["evidence"].append(
            {
                "kind": "configuration",
                "path": "rocketmq-sre/config/qualification/provider-failover.v1.json",
                "qualification": "provider-failover",
            }
        )

        findings = MODULE.validate_manifest(manifest, MODULE.REPOSITORY_ROOT)

        self.assertIn(
            "capability_areas[0] qualification provider-failover is missing evidence kinds: smoke, test",
            findings,
        )

    def test_rejects_live_smoke_claim_without_smoke_evidence(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        diagnostic_packs = manifest["capability_areas"][2]
        diagnostic_packs["evidence"] = [
            evidence for evidence in diagnostic_packs["evidence"] if evidence["kind"] != "smoke"
        ]

        findings = MODULE.validate_manifest(manifest, MODULE.REPOSITORY_ROOT)

        self.assertIn(
            "capability_areas[2] claims live_smoke_passed without smoke evidence",
            findings,
        )

    def test_rejects_invalid_qualification_identifier(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["capability_areas"][0]["evidence"][0]["qualification"] = "Phase 6"

        findings = MODULE.validate_manifest(manifest, MODULE.REPOSITORY_ROOT)

        self.assertIn(
            "capability_areas[0].evidence[0].qualification must be a stable kebab-case identifier",
            findings,
        )


if __name__ == "__main__":
    unittest.main()
