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

import argparse
import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import message_path_qualification as qualification  # noqa: E402


class FakeExecutor:
    def __init__(self, *, fail_first: bool = False) -> None:
        self.calls: list[list[str]] = []
        self.fail_first = fail_first

    def __call__(self, command: list[str], cwd: Path, timeout_seconds: int) -> qualification.CommandResult:
        del cwd, timeout_seconds
        self.calls.append(command)
        if self.fail_first and len(self.calls) == 1:
            return qualification.CommandResult(2, "", "injected benchmark failure", 1)
        output = Path(command[command.index("--output-json") + 1])
        output.parent.mkdir(parents=True, exist_ok=True)
        scenario = command[command.index("--scenario") + 1]
        count = int(command[command.index("--message-count") + 1])
        size = int(command[command.index("--message-size") + 1])
        batch_size = int(command[command.index("--batch-size") + 1])
        run_id = command[command.index("--run-id") + 1]
        namesrv = command[command.index("--namesrv") + 1]
        topic = command[command.index("--topic") + 1]
        measurement = {
            "schema_version": 1,
            "artifact_kind": "rocketmq_message_path_measurement",
            "run_id": run_id,
            "scenario": scenario,
            "operation": "consume" if scenario == "lite-pull" else "send",
            "target": {"namesrv_addr": namesrv, "topic": topic},
            "workload": {
                "message_count": count,
                "message_size_bytes": size,
                "batch_size": batch_size,
            },
            "result": {
                "duration_us": 1_000_000,
                "success_count": count,
                "send_failed_count": 0,
                "response_failed_count": 0,
                "throughput_messages_per_second": float(count),
                "payload_mib_per_second": max(0.001, count * size / 1024 / 1024),
                "latency_us": {
                    "samples": count,
                    "average": 900.0,
                    "p50": 800,
                    "p95": 950,
                    "p99": 1_000,
                    "p999": 1_100,
                    "max": 1_200,
                },
            },
        }
        output.write_text(json.dumps(measurement), encoding="utf-8")
        return qualification.CommandResult(0, "benchmark passed", "", 10)


class MessagePathQualificationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.policy = qualification.load_json(qualification.DEFAULT_POLICY)

    def args(self, output_dir: Path, **overrides: object) -> argparse.Namespace:
        values: dict[str, object] = {
            "mode": "smoke",
            "namesrv": "127.0.0.1:19876",
            "confirm_target": "127.0.0.1:19876",
            "topic": "QualificationTopic",
            "durability_contract": "async-flush-single-replica",
            "run_id": "qualification-test",
            "repetitions": 1,
            "output_dir": output_dir,
            "command_timeout_seconds": 30,
            "subject_role": None,
            "subject_commit": None,
            "artifact_manifest": None,
            "deployment_digest": None,
            "target_id": None,
            "cluster_uid": None,
            "effective_config": None,
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    def test_committed_policy_is_valid_and_release_is_evidence_gated(self) -> None:
        self.assertEqual([], qualification.validate_policy(self.policy))
        release = self.policy["modes"]["release"]
        self.assertGreaterEqual(release["minimum_repetitions"], 5)
        self.assertEqual(21_600, release["minimum_soak_seconds"])
        self.assertEqual(qualification.EXTERNAL_EVIDENCE, set(release["required_external_evidence"]))

    def test_plan_is_pure_and_uses_exact_argument_vectors(self) -> None:
        plan = qualification.build_plan(
            self.policy,
            "smoke",
            "127.0.0.1:19876",
            "QualificationTopic",
            "planned-run",
            1,
        )

        self.assertEqual("rocketmq_message_path_qualification_plan", plan["artifact_kind"])
        self.assertEqual(4, len(plan["commands"]))
        self.assertTrue(all(command[0] == "cargo" and "--output-json" in command for command in plan["commands"]))
        self.assertTrue(
            all(
                command[command.index("--example") + 1] == "client-production-benchmark"
                for command in plan["commands"]
            )
        )

    def test_target_confirmation_mismatch_rejects_before_execution(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            executor = FakeExecutor()
            args = self.args(Path(temporary), confirm_target="127.0.0.1:9876")

            with self.assertRaisesRegex(qualification.QualificationError, "exactly match"):
                qualification.run_qualification(self.policy, args, executor)

            self.assertEqual([], executor.calls)

    def test_successful_smoke_writes_hash_bound_report(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            executor = FakeExecutor()
            args = self.args(Path(temporary))
            with mock.patch.object(qualification, "git_snapshot", return_value=("a" * 40, True)), mock.patch.object(
                qualification,
                "environment_record",
                return_value={"hardware_id": "sha256:test"},
            ):
                report, report_path = qualification.run_qualification(self.policy, args, executor)

            self.assertEqual("pass", report["status"])
            self.assertTrue(report["measurement_qualified"])
            self.assertEqual("rocketmq_message_path_measurement_set", report["artifact_kind"])
            self.assertEqual(4, len(report["workloads"]))
            self.assertEqual(4, len(executor.calls))
            self.assertTrue(report_path.is_file())
            self.assertTrue(all(item["sha256"] for item in report["artifacts"]))

    def test_benchmark_failure_is_recorded_and_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            executor = FakeExecutor(fail_first=True)
            args = self.args(Path(temporary), run_id="qualification-failure")
            with mock.patch.object(qualification, "git_snapshot", return_value=("a" * 40, False)), mock.patch.object(
                qualification,
                "environment_record",
                return_value={"hardware_id": "sha256:test"},
            ):
                report, _ = qualification.run_qualification(self.policy, args, executor)

            self.assertEqual("fail", report["status"])
            self.assertFalse(report["measurement_qualified"])
            self.assertIn("exit code 2", report["failures"][0])

    def test_release_measurement_requires_immutable_identity_before_execution(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            executor = FakeExecutor()
            args = self.args(
                Path(temporary),
                mode="release",
                run_id="qualification-release",
                repetitions=5,
                durability_contract="sync-flush-required-replica-acks",
            )
            with mock.patch.object(qualification, "git_snapshot", return_value=("b" * 40, False)), mock.patch.object(
                qualification,
                "environment_record",
                return_value={"hardware_id": "sha256:test"},
            ):
                report, _ = qualification.run_qualification(self.policy, args, executor)

            self.assertEqual("fail", report["status"])
            self.assertFalse(report["measurement_qualified"])
            self.assertEqual(0, len(executor.calls))
            self.assertTrue(any("artifact-manifest" in finding for finding in report["failures"]))
            self.assertTrue(any("deployment-digest" in finding for finding in report["failures"]))
            self.assertTrue(any("target-id" in finding for finding in report["failures"]))

    def test_comparison_requires_matching_contract_and_thresholds(self) -> None:
        policy_hash = qualification.canonical_sha256(self.policy)
        baseline = {
            "schema_version": 2,
            "artifact_kind": "rocketmq_message_path_measurement_set",
            "status": "pass",
            "mode": "release",
            "measurement_qualified": True,
            "policy_sha256": policy_hash,
            "business_contract": "java-equivalent-message-semantics",
            "durability_contract": "async-flush-single-replica",
            "subject": {
                "role": "baseline",
                "commit": "a" * 40,
                "artifact_manifest_sha256": "sha256:" + "1" * 64,
                "deployment_digest": "sha256:" + "2" * 64,
            },
            "environment": {"hardware_id": "sha256:test"},
            "target": {
                "target_id": "target-a",
                "cluster_uid": "cluster-a",
                "effective_config_sha256": "sha256:" + "3" * 64,
            },
            "repetitions_per_workload": 5,
            "workloads": [
                {
                    "id": "sync-128b",
                    "parameters": {"scenario": "sync"},
                    "aggregate": {
                        "throughput_messages_per_second_median": 1000.0,
                        "p99_latency_us_median": 1000.0,
                    },
                }
            ],
        }
        candidate = copy.deepcopy(baseline)
        candidate["subject"]["role"] = "candidate"
        candidate["subject"]["commit"] = "b" * 40
        candidate["subject"]["deployment_digest"] = "sha256:" + "4" * 64
        candidate["workloads"][0]["aggregate"]["throughput_messages_per_second_median"] = 950.0
        candidate["workloads"][0]["aggregate"]["p99_latency_us_median"] = 1100.0

        passed = qualification.compare_reports(self.policy, baseline, candidate)
        self.assertEqual("pass", passed["status"])

        candidate["workloads"][0]["aggregate"]["throughput_messages_per_second_median"] = 800.0
        failed = qualification.compare_reports(self.policy, baseline, candidate)
        self.assertEqual("fail", failed["status"])
        self.assertTrue(any("throughput regression" in finding for finding in failed["failures"]))

        candidate = copy.deepcopy(baseline)
        candidate["subject"]["role"] = "candidate"
        candidate["subject"]["commit"] = "b" * 40
        candidate["subject"]["deployment_digest"] = "sha256:" + "4" * 64
        candidate["durability_contract"] = "sync-flush-required-replica-acks"
        mismatched = qualification.compare_reports(self.policy, baseline, candidate)
        self.assertEqual("fail", mismatched["status"])
        self.assertTrue(any("durability contracts differ" in finding for finding in mismatched["failures"]))

    def test_comparison_rejects_unbound_status_only_document(self) -> None:
        result = qualification.compare_reports(self.policy, {"status": "pass"}, {"status": "pass"})

        self.assertEqual("fail", result["status"])
        self.assertFalse(result["release_comparison_qualified"])
        self.assertTrue(any("contract is invalid" in finding for finding in result["failures"]))

    def test_final_qualification_rejects_candidate_and_soak_binding_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            candidate = {
                "schema_version": 2,
                "artifact_kind": "rocketmq_message_path_measurement_set",
                "status": "pass",
                "measurement_qualified": True,
                "mode": "release",
                "policy_sha256": qualification.canonical_sha256(self.policy),
                "business_contract": "java-equivalent-message-semantics",
                "durability_contract": "strict",
                "subject": {
                    "role": "candidate",
                    "commit": "b" * 40,
                    "artifact_manifest_sha256": "sha256:" + "1" * 64,
                    "deployment_digest": "sha256:" + "2" * 64,
                },
                "environment": {"hardware_id": "sha256:" + "3" * 64},
                "target": {
                    "target_id": "target-a",
                    "cluster_uid": "cluster-a",
                    "effective_config_sha256": "sha256:" + "4" * 64,
                },
                "repetitions_per_workload": 5,
                "workloads": [],
            }
            candidate_path = root / "candidate.json"
            candidate_path.write_text(json.dumps(candidate), encoding="utf-8")
            candidate_hash = "sha256:" + qualification.sha256_file(candidate_path)
            comparison = {
                "schema_version": 2,
                "artifact_kind": "rocketmq_message_path_comparison",
                "status": "pass",
                "release_comparison_qualified": True,
                "durability_contract": "strict",
                "candidate": {
                    "report_sha256": candidate_hash,
                    "commit": "b" * 40,
                    "deployment_digest": "sha256:" + "2" * 64,
                    "target_id": "target-a",
                    "cluster_uid": "cluster-a",
                    "effective_config_sha256": "sha256:" + "4" * 64,
                },
            }
            fault = {
                "candidate_commit": "b" * 40,
                "dynamic_execution": True,
                "fixture": False,
                "release_identity": {
                    "deployment_digest": "sha256:" + "2" * 64,
                    "target_id": "target-a",
                    "cluster_uid": "cluster-a",
                    "effective_config_sha256": "sha256:" + "4" * 64,
                    "durability_contract": "strict",
                },
            }
            rpo = {
                "schema_version": 1,
                "artifact_kind": "controller_failover_qualification_evidence",
                "status": "pass",
                "strict_qualification_passed": True,
                "candidate_commit": "b" * 40,
                "deployment_digest": "sha256:" + "2" * 64,
                "target_id": "target-a",
                "cluster_uid": "cluster-a",
                "effective_config_sha256": "sha256:" + "4" * 64,
                "durability_contract": "strict",
                "ledger_sha256": "sha256:" + "6" * 64,
                "repetitions": 5,
                "put_ok_messages": {
                    "put_ok_count": 10000,
                    "recovered_once_count": 10000,
                    "missing_count": 0,
                    "duplicate_count": 0,
                    "unexpected_count": 0,
                    "payload_mismatch_count": 0,
                    "offset_mismatch_count": 0,
                    "rpo_zero": True,
                    "exact_recovery": True,
                },
                "confirm_offset": {"valid": True, "observations": 10, "violation_count": 0},
            }
            raw_samples = root / "raw-samples.ndjson"
            raw_samples.write_text('{"sample":1}\n', encoding="utf-8")
            raw_digest = qualification.sha256_file(raw_samples)
            soak = {
                "schema_version": 1,
                "artifact_kind": "rocketmq_message_path_soak_report",
                "profile": "full",
                "status": "pass",
                "monotonic_growth_detected": False,
                "duration_seconds": 21600,
                "release_identity": {
                    "commit": "b" * 40,
                    "deployment_digest": "sha256:" + "2" * 64,
                    "target_id": "wrong-target",
                    "cluster_uid": "cluster-a",
                    "effective_config_sha256": "sha256:" + "4" * 64,
                    "durability_contract": "strict",
                },
                "sampling": {"coverage_percent": 99.9, "max_gap_seconds": 60},
                "workload": {
                    "attempted": 10000,
                    "put_ok": 10000,
                    "consumed": 10000,
                    "send_failures": 0,
                    "consume_failures": 0,
                    "missing": 0,
                    "duplicates": 0,
                    "corrupt": 0,
                },
                "pods": [{"name": "broker-0", "uid": "pod-uid", "restarts": 0, "oom_killed": False}],
                "series": [{"status": "pass", "raw_artifact_sha256": "sha256:" + raw_digest}],
                "artifacts": [{"path": raw_samples.name, "sha256": raw_digest}],
            }
            paths = {}
            for name, value in (("comparison", comparison), ("fault", fault), ("rpo", rpo), ("soak", soak)):
                path = root / f"{name}.json"
                path.write_text(json.dumps(value), encoding="utf-8")
                paths[name] = path
            args = argparse.Namespace(
                candidate_commit="b" * 40,
                candidate_measurement=candidate_path,
                performance_comparison=paths["comparison"],
                fault_evidence=paths["fault"],
                rpo_evidence=paths["rpo"],
                soak_report=paths["soak"],
            )

            findings, _, _ = qualification.validate_final_evidence(self.policy, args)

            self.assertIn("soak release identity target_id differs", findings)


if __name__ == "__main__":
    unittest.main()
