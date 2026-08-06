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
"""Tests for target-environment Evidence source qualification."""

from __future__ import annotations

import copy
import importlib.util
import unittest
from datetime import datetime, timezone
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "check_target_evidence_source_qualification.py"
SPEC = importlib.util.spec_from_file_location("check_target_evidence_source_qualification", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class TargetEvidenceSourceQualificationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = MODULE.load_json(MODULE.DEFAULT_MANIFEST)
        cls.pack_manifest = MODULE.load_json(MODULE.DEFAULT_PACK_MANIFEST)
        cls.now = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)

    def valid_report(self) -> dict:
        routes = MODULE.required_routes(self.pack_manifest)
        sources = sorted({route[2] for route in routes})
        return {
            "schema_version": "rocketmq-sre.target-evidence-source-qualification-report.v1",
            "environment_ref": "environment://production/primary",
            "source_revision": "a" * 40,
            "catalog_digest": MODULE.canonical_digest(self.pack_manifest),
            "observed_at": "2026-08-07T11:55:00Z",
            "valid_until": "2026-09-06T11:55:00Z",
            "evidence_sources_qualified": True,
            "production_certified": False,
            "grants_execution_authority": False,
            "unattended_autonomous_execution": False,
            "source_bindings": [
                {
                    "source": source,
                    "integration_ref": f"integration://production/{source}",
                    "identity_ref": f"identity://production/{source}",
                    "owner_ref": f"owner://production/{source}",
                    "production_backend": True,
                    "tls_verified": True,
                    "workload_identity_verified": True,
                    "tenant_scope_verified": True,
                    "cluster_scope_verified": True,
                }
                for source in sources
            ],
            "route_results": [
                {
                    "pack_id": pack_id,
                    "evidence_key": evidence_key,
                    "source": source,
                    "resource_prefix": resource_prefix,
                    "status": "passed",
                    "sample_count": 3,
                    "observed_at": "2026-08-07T11:54:00Z",
                    "evidence_digest": f"sha256:{index:064x}",
                    "query_executed": True,
                    "canonical_schema_valid": True,
                    "tenant_scope_enforced": True,
                    "cluster_scope_enforced": True,
                    "freshness_enforced": True,
                    "row_bound_enforced": True,
                    "byte_bound_enforced": True,
                    "redaction_verified": True,
                    "missing_semantics_verified": True,
                }
                for index, (pack_id, evidence_key, source, resource_prefix) in enumerate(routes, start=1)
            ],
            "summary": {
                "expected_routes": 32,
                "passed_routes": 32,
                "source_types": 6,
                "target_mutations": 0,
                "executor_calls": 0,
                "execution_agent_calls": 0,
                "model_provider_calls": 0,
                "credentials_recorded": False,
                "message_bodies_recorded": False,
                "evidence_payloads_recorded": False,
            },
        }

    def validate(self, report: dict, path: str = r"D:\rocketmq-sre-evidence\target\report.json") -> list[str]:
        return MODULE.validate_report(
            report,
            self.manifest,
            self.pack_manifest,
            report_path=path,
            now=self.now,
        )

    def test_committed_contract_tracks_all_required_routes(self) -> None:
        self.assertEqual(MODULE.validate_manifest(self.manifest, self.pack_manifest), [])
        routes = MODULE.required_routes(self.pack_manifest)
        self.assertEqual(len(routes), 32)
        self.assertEqual(
            {route[2] for route in routes},
            {"admin-query", "kubernetes", "prometheus", "rocketmq-mcp", "runtime", "topology"},
        )

    def test_accepts_complete_machine_local_report(self) -> None:
        self.assertEqual(self.validate(self.valid_report()), [])

    def test_rejects_missing_duplicate_and_unexpected_routes(self) -> None:
        report = self.valid_report()
        report["route_results"].pop()
        report["route_results"].append(copy.deepcopy(report["route_results"][0]))
        report["route_results"][1]["pack_id"] = "unexpected.v1"

        findings = self.validate(report)

        self.assertTrue(any("missing required route" in finding for finding in findings))
        self.assertTrue(any("duplicate route result" in finding for finding in findings))
        self.assertTrue(any("unexpected route result" in finding for finding in findings))

    def test_rejects_failed_or_incomplete_route_proof(self) -> None:
        report = self.valid_report()
        route = report["route_results"][0]
        route["status"] = "missing"
        route["sample_count"] = 2
        route["redaction_verified"] = False

        findings = self.validate(report)

        self.assertTrue(any("must pass" in finding for finding in findings))
        self.assertTrue(any("sample_count" in finding for finding in findings))
        self.assertTrue(any("redaction_verified" in finding for finding in findings))

    def test_rejects_stale_expired_or_overlong_attestation(self) -> None:
        report = self.valid_report()
        report["observed_at"] = "2026-08-01T00:00:00Z"
        report["valid_until"] = "2027-08-01T00:00:00Z"
        report["route_results"][0]["observed_at"] = "2026-08-01T00:00:00Z"

        findings = self.validate(report)

        self.assertTrue(any("validity window" in finding for finding in findings))
        self.assertTrue(any("expired or not currently valid" in finding for finding in findings))
        self.assertTrue(any("freshness window" in finding for finding in findings))

    def test_rejects_missing_or_unverified_source_binding(self) -> None:
        report = self.valid_report()
        report["source_bindings"].pop()
        report["source_bindings"][0]["tls_verified"] = False

        findings = self.validate(report)

        self.assertTrue(any("source binding set" in finding for finding in findings))
        self.assertTrue(any("tls_verified" in finding for finding in findings))

    def test_rejects_sensitive_content_and_personal_identifiers(self) -> None:
        report = self.valid_report()
        report["source_bindings"][0]["owner_ref"] = "operator@example.com"
        report["source_bindings"][1]["identity_ref"] = "Bearer secret-value"

        findings = self.validate(report)

        self.assertTrue(any("personal identifier" in finding for finding in findings))
        self.assertTrue(any("sensitive material" in finding for finding in findings))

    def test_rejects_nonopaque_refs_and_invalid_revision_or_digest(self) -> None:
        report = self.valid_report()
        report["environment_ref"] = "production-primary"
        report["source_revision"] = "main"
        report["catalog_digest"] = "sha256:" + "0" * 64
        report["source_bindings"][0]["integration_ref"] = "https://internal.example"
        report["route_results"][0]["evidence_digest"] = "invalid"

        findings = self.validate(report)

        self.assertTrue(any("environment_ref" in finding for finding in findings))
        self.assertTrue(any("source_revision" in finding for finding in findings))
        self.assertTrue(any("catalog_digest" in finding for finding in findings))
        self.assertTrue(any("integration_ref" in finding for finding in findings))
        self.assertTrue(any("evidence_digest" in finding for finding in findings))

    def test_rejects_report_outside_machine_local_evidence_roots(self) -> None:
        report = self.valid_report()

        findings = self.validate(report, r"C:\Users\operator\report.json")
        escaped = self.validate(report, r"D:\rocketmq-sre-evidence\..\outside\report.json")

        self.assertTrue(any("allowed machine-local root" in finding for finding in findings))
        self.assertTrue(any("allowed machine-local root" in finding for finding in escaped))

    def test_rejects_authority_or_product_certification_claims(self) -> None:
        report = self.valid_report()
        report["production_certified"] = True
        report["grants_execution_authority"] = True
        report["unattended_autonomous_execution"] = True
        report["summary"]["target_mutations"] = 1

        findings = self.validate(report)

        self.assertTrue(any("production_certified" in finding for finding in findings))
        self.assertTrue(any("grants_execution_authority" in finding for finding in findings))
        self.assertTrue(any("unattended_autonomous_execution" in finding for finding in findings))
        self.assertTrue(any("read-only summary" in finding for finding in findings))

    def test_rejects_summary_drift(self) -> None:
        report = self.valid_report()
        report["summary"]["passed_routes"] = 31
        report["summary"]["source_types"] = 5

        self.assertTrue(any("summary" in finding for finding in self.validate(report)))


if __name__ == "__main__":
    unittest.main()
