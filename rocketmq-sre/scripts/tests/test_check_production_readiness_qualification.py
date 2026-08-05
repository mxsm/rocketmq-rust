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

from __future__ import annotations

import copy
import importlib.util
import json
import sys
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "check_production_readiness_qualification.py"
SPEC = importlib.util.spec_from_file_location("check_production_readiness_qualification", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class ProductionReadinessQualificationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.manifest = MODULE.load_json(MODULE.DEFAULT_MANIFEST)
        self.report = {
            "schema_version": MODULE.REPORT_SCHEMA,
            "status": "passed",
            "environment": "disposable_kind",
            "revision": "a" * 40,
            "source_clean": True,
            "started_at": "2026-08-05T00:00:00Z",
            "finished_at": "2026-08-05T06:01:00Z",
            "production_certified": False,
            "model_provider_network_calls": 0,
            "unattended_autonomous_execution": False,
            "live_mode_ceiling": "supervised",
            "secrets_recorded": False,
            "message_bodies_recorded": False,
            "sources": [
                {
                    "id": source,
                    "status": "passed",
                    "schema_version": f"fixture.{source}.v1",
                    "sha256": f"sha256:{index:064x}",
                    "revision": "a" * 40,
                }
                for index, source in enumerate(sorted(MODULE.EXPECTED_SOURCES), 1)
            ],
            "soak": {
                "planned_duration_seconds": 21600,
                "observed_duration_seconds": 21600.1,
                "samples_observed": 360,
                "sampled_availability_ratio": 1.0,
                "full_duration_qualification": True,
                "final_all_ready": True,
                "data_plane_independent": True,
                "unresolved_faults": [],
                "faults": [
                    {
                        "id": fault,
                        "recovered": True,
                        "data_plane_remained_ready": None if fault == "broker_pod_replacement" else True,
                        "data_plane_probe_phase": (
                            "after_recovery" if fault == "broker_pod_replacement" else "during_outage"
                        ),
                        "data_plane_recovery_verified": True,
                        "recovery_seconds": 2.0,
                        "bounded_data_plane_probe": {
                            "sent_messages": 10,
                            "received_messages": 10,
                            "acknowledged_messages": 10,
                            "message_bodies_recorded": False,
                        },
                    }
                    for fault in sorted(MODULE.EXPECTED_FAULTS)
                ],
                "resource_summary": {"samples": 360, "cpu_percent_max": 20.0, "memory_bytes_max": 1_000_000},
            },
            "scale": {
                "logical_clusters": 100,
                "topic_assets": 10000,
                "consumer_group_assets": 10000,
                "page_limit": 500,
                "page_samples": 40,
                "oversized_page_rejected": True,
                "quota_backpressure_verified": True,
                "cleanup_verified": True,
            },
            "measurements": {
                "evidence_query": {"samples": 100, "p95_millis": 2.0, "unit": "milliseconds"},
                "policy_evaluation": {"samples": 10000, "p99_millis": 0.2, "unit": "milliseconds"},
                "execution_precheck": {"samples": 1000, "p95_millis": 0.3, "unit": "milliseconds"},
            },
            "operational_measurements": {
                "samples": 1000,
                "error_count": 0,
                "error_rate": 0.0,
                "execution_queue_depth_samples": 1000,
                "execution_queue_depth_max": 0,
            },
            "handoff": {
                "checklist_validated": True,
                "command_paths_validated": True,
                "independent_operator_signoff": False,
                "required_for_production": True,
            },
            "cleanup": {
                "status": "passed",
                "disposable_kind_destroyed": True,
                "owned_containers_removed": True,
                "owned_artifacts_removed": True,
            },
        }

    def test_repository_manifest_is_valid(self) -> None:
        self.assertEqual(MODULE.validate_manifest(self.manifest), [])

    def test_complete_report_is_valid(self) -> None:
        self.assertEqual(MODULE.validate_report(self.report, self.manifest), [])

    def test_missing_fault_and_latency_fail_closed(self) -> None:
        report = copy.deepcopy(self.report)
        report["soak"]["faults"].pop()
        report["scale"]["page_samples"] = 39
        report["measurements"]["policy_evaluation"]["p99_millis"] = 100
        findings = MODULE.validate_report(report, self.manifest)
        self.assertTrue(any("fault set drifted" in finding for finding in findings))
        self.assertTrue(any("pagination minimum" in finding for finding in findings))
        self.assertTrue(any("exceeds 50 ms" in finding for finding in findings))

    def test_fault_probe_timing_cannot_overstate_data_plane_continuity(self) -> None:
        report = copy.deepcopy(self.report)
        broker = next(
            fault for fault in report["soak"]["faults"] if fault["id"] == "broker_pod_replacement"
        )
        broker["data_plane_probe_phase"] = "during_outage"
        broker["data_plane_remained_ready"] = True
        component = next(
            fault for fault in report["soak"]["faults"] if fault["id"] == "collector_outage"
        )
        component["data_plane_probe_phase"] = "after_recovery"
        component["data_plane_remained_ready"] = None
        findings = MODULE.validate_report(report, self.manifest)
        self.assertTrue(any("post-recovery probing" in finding for finding in findings))
        self.assertTrue(any("did not preserve" in finding for finding in findings))

    def test_live_provider_or_fabricated_signoff_fails_closed(self) -> None:
        report = copy.deepcopy(self.report)
        report["model_provider_network_calls"] = 1
        report["handoff"]["independent_operator_signoff"] = True
        findings = MODULE.validate_report(report, self.manifest)
        self.assertTrue(any("model_provider_network_calls" in finding for finding in findings))
        self.assertTrue(any("must not fabricate" in finding for finding in findings))

    def test_stale_source_and_operational_drift_fail_closed(self) -> None:
        report = copy.deepcopy(self.report)
        report["sources"][0]["revision"] = "b" * 40
        report["operational_measurements"]["error_rate"] = 0.02
        report["operational_measurements"]["execution_queue_depth_max"] = 257
        findings = MODULE.validate_report(report, self.manifest)
        self.assertTrue(any("revision must match" in finding for finding in findings))
        self.assertTrue(any("error_rate exceeds" in finding for finding in findings))
        self.assertTrue(any("queue_depth_max exceeds" in finding for finding in findings))

    def test_sensitive_material_is_rejected(self) -> None:
        report = copy.deepcopy(self.report)
        report["note"] = "Bearer qualification-token"
        findings = MODULE.validate_report(report, self.manifest)
        self.assertIn("report contains credential-like material", findings)

    def test_fixture_stays_json_serializable(self) -> None:
        self.assertIsInstance(json.loads(json.dumps(self.report)), dict)


if __name__ == "__main__":
    unittest.main()
