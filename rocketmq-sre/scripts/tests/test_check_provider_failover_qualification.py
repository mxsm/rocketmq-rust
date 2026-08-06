# Copyright 2026 The RocketMQ Rust Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "check_provider_failover_qualification.py"
SPEC = importlib.util.spec_from_file_location("provider_failover_checker", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKER)


def valid_manifest() -> dict:
    return {
        "schema_version": "rocketmq-sre.provider-failover-qualification.v1",
        "environment": "docker_postgresql_local_primary_deepseek_secondary",
        "operating_mode": "supervised_read_only",
        "production_certified": False,
        "unattended_autonomous_execution": False,
        "providers": {
            "primary": {
                "kind": "loopback_fault_fixture",
                "network": "loopback_only",
                "credential": "ephemeral_process_fixture",
            },
            "secondary": {
                "provider": "deepseek",
                "protocol": "responses_api",
                "model": "deepseek-v4-flash",
                "network": "real_https",
            },
            "descriptors_only": ["zhipu-glm", "kimi-moonshot"],
        },
        "required_scenarios": {
            "transient_primary_to_live_secondary": {
                "primary_error": "service_unavailable",
                "secondary_result": "model_assisted",
                "authorized_evidence_citations": True,
                "minimum_secondary_attempts": 1,
                "maximum_secondary_attempts": 2,
                "maximum_schema_repairs": 1,
                "maximum_diagnosis_attempts": 1,
            },
            "policy_denial_stops_fallback": {"secondary_calls": 0, "result": "rules_only"},
            "unsupported_capability_stops_fallback": {"secondary_calls": 0, "result": "rules_only"},
            "invalid_schema_stops_fallback": {"secondary_calls": 0, "result": "rules_only"},
            "invalid_citation_stops_fallback": {"secondary_calls": 0, "result": "rules_only"},
            "all_unavailable_rules_only": {"result": "rules_only", "execution_eligible": False},
        },
        "repository_evidence": {
            "live_test_path": "rocketmq-sre/crates/rocketmq-sre-control-plane/src/models/service/live_provider_failover.rs",
            "live_test": "transient_primary_falls_back_to_live_deepseek_and_failures_remain_rules_only",
            "runner": "rocketmq-sre/scripts/provider-failover-qualification.ps1",
            "checker": "rocketmq-sre/scripts/check_provider_failover_qualification.py",
        },
        "live_report": {
            "schema_version": "rocketmq-sre.provider-failover-qualification-report.v1",
            "machine_local_only": True,
            "allowed_roots": [r"D:\rocketmq-sre-evidence", r"F:\rocketmq-sre-evidence"],
            "secrets_recorded": False,
            "prompts_recorded": False,
            "responses_recorded": False,
            "message_bodies_recorded": False,
        },
    }


def valid_report() -> dict:
    return {
        "schema_version": "rocketmq-sre.provider-failover-qualification-report.v1",
        "status": "passed",
        "candidate_commit": "a" * 40,
        "source_clean": True,
        "environment": "docker_postgresql_local_primary_deepseek_secondary",
        "started_at": "2026-08-06T00:00:00Z",
        "finished_at": "2026-08-06T00:01:00Z",
        "scenarios": {
            "transient_primary_to_live_secondary": {
                "result": "model_assisted",
                "primary_attempts": 1,
                "primary_error": "service_unavailable",
                "secondary_attempts": 1,
                "diagnosis_attempts": 1,
                "rules_only_attempts": 0,
                "secondary_failed_attempts": 0,
                "schema_repairs": 0,
                "actual_provider": "deepseek",
                "actual_model": "deepseek-v4-flash",
                "fallback_chain": ["qualification-primary-transient"],
                "authorized_evidence_citations": True,
                "cited_evidence_count": 1,
                "invocation_persisted": True,
            },
            "policy_denial_stops_fallback": {
                "result": "rules_only",
                "primary_attempts": 1,
                "primary_error": "policy_denied",
                "secondary_attempts": 0,
            },
            "unsupported_capability_stops_fallback": {
                "result": "rules_only",
                "primary_attempts": 1,
                "primary_error": "capability_unsupported",
                "secondary_attempts": 0,
            },
            "invalid_schema_stops_fallback": {
                "result": "rules_only",
                "primary_attempts": 2,
                "primary_error": "schema_validation_failed",
                "secondary_attempts": 0,
                "schema_repairs": 1,
            },
            "invalid_citation_stops_fallback": {
                "result": "rules_only",
                "primary_attempts": 2,
                "primary_error": "schema_validation_failed",
                "secondary_attempts": 0,
                "schema_repairs": 1,
            },
            "all_unavailable_rules_only": {
                "result": "rules_only",
                "primary_attempts": 1,
                "primary_error": "service_unavailable",
                "secondary_attempts": 0,
                "execution_eligible": False,
            },
        },
        "provider_certification": {
            "deepseek": "live_smoke_passed",
            "zhipu_glm": "descriptor_only",
            "kimi_moonshot": "descriptor_only",
        },
        "safety": {
            "effective_access": "read_only",
            "production_certified": False,
            "unattended_autonomous_execution": False,
            "mutation_calls": 0,
            "executor_calls": 0,
            "execution_agent_calls": 0,
            "secrets_recorded": False,
            "prompts_recorded": False,
            "responses_recorded": False,
            "message_bodies_recorded": False,
        },
        "cleanup": {
            "postgres_container_removed": True,
            "database_url_cleared": True,
            "api_key_environment_cleared": True,
            "loopback_credential_environment_cleared": True,
        },
    }


class ProviderFailoverQualificationTests(unittest.TestCase):
    def test_valid_contract_and_report_pass(self) -> None:
        self.assertEqual(CHECKER.validate_manifest(valid_manifest()), [])
        self.assertEqual(CHECKER.validate_report(valid_report()), [])

    def test_live_secondary_identity_is_exact(self) -> None:
        report = valid_report()
        report["scenarios"]["transient_primary_to_live_secondary"]["actual_provider"] = "openai"
        self.assertTrue(CHECKER.validate_report(report))

    def test_live_secondary_repair_is_bounded_and_accounted(self) -> None:
        report = valid_report()
        live = report["scenarios"]["transient_primary_to_live_secondary"]
        live["secondary_attempts"] = 2
        live["secondary_failed_attempts"] = 1
        live["schema_repairs"] = 1
        self.assertEqual(CHECKER.validate_report(report), [])
        live["secondary_attempts"] = 5
        self.assertTrue(CHECKER.validate_report(report))

    def test_live_retry_cannot_replace_same_diagnosis_fallback(self) -> None:
        report = valid_report()
        live = report["scenarios"]["transient_primary_to_live_secondary"]
        live["primary_attempts"] = 2
        live["diagnosis_attempts"] = 2
        live["rules_only_attempts"] = 1
        self.assertTrue(CHECKER.validate_report(report))

    def test_non_fallback_safe_error_cannot_call_secondary(self) -> None:
        report = valid_report()
        report["scenarios"]["policy_denial_stops_fallback"]["secondary_attempts"] = 1
        self.assertTrue(CHECKER.validate_report(report))

    def test_unsupported_capability_cannot_call_secondary(self) -> None:
        report = valid_report()
        report["scenarios"]["unsupported_capability_stops_fallback"]["secondary_attempts"] = 1
        self.assertTrue(CHECKER.validate_report(report))

    def test_unauthorized_citation_cannot_call_secondary(self) -> None:
        report = valid_report()
        report["scenarios"]["invalid_citation_stops_fallback"]["secondary_attempts"] = 1
        self.assertTrue(CHECKER.validate_report(report))

    def test_rules_only_never_becomes_executable(self) -> None:
        report = valid_report()
        report["scenarios"]["all_unavailable_rules_only"]["execution_eligible"] = True
        self.assertTrue(CHECKER.validate_report(report))

    def test_uncertified_provider_cannot_be_overstated(self) -> None:
        report = valid_report()
        report["provider_certification"]["kimi_moonshot"] = "live_smoke_passed"
        self.assertTrue(CHECKER.validate_report(report))

    def test_sensitive_or_local_material_is_rejected(self) -> None:
        report = valid_report()
        report["credential"] = "sk-abcdefghijklmnopqrstuv"
        report["debug_path"] = r"C:\Users\operator\secret.txt"
        findings = CHECKER.validate_report(report)
        self.assertTrue(any("sensitive" in finding or "credential" in finding for finding in findings))
        self.assertTrue(any("absolute local path" in finding for finding in findings))

    def test_incomplete_cleanup_is_rejected(self) -> None:
        report = valid_report()
        report["cleanup"]["api_key_environment_cleared"] = False
        self.assertTrue(CHECKER.validate_report(report))


if __name__ == "__main__":
    unittest.main()
