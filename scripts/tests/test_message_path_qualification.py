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
            "performance_comparison": None,
            "fault_evidence": None,
            "soak_report": None,
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
            self.assertFalse(report["release_qualified"])
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
            self.assertFalse(report["release_qualified"])
            self.assertIn("exit code 2", report["failures"][0])

    def test_release_cannot_run_without_external_evidence(self) -> None:
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
            self.assertFalse(report["release_qualified"])
            self.assertEqual(0, len(executor.calls))
            self.assertEqual(3, len(report["failures"]))

    def test_comparison_requires_matching_contract_and_thresholds(self) -> None:
        policy_hash = qualification.canonical_sha256(self.policy)
        baseline = {
            "schema_version": 1,
            "artifact_kind": "rocketmq_message_path_qualification_report",
            "status": "pass",
            "mode": "smoke",
            "release_qualified": False,
            "policy_sha256": policy_hash,
            "business_contract": "java-equivalent-message-semantics",
            "durability_contract": "async-flush-single-replica",
            "environment": {"hardware_id": "sha256:test"},
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
        candidate["workloads"][0]["aggregate"]["throughput_messages_per_second_median"] = 950.0
        candidate["workloads"][0]["aggregate"]["p99_latency_us_median"] = 1100.0

        passed = qualification.compare_reports(self.policy, baseline, candidate)
        self.assertEqual("pass", passed["status"])

        candidate["workloads"][0]["aggregate"]["throughput_messages_per_second_median"] = 800.0
        failed = qualification.compare_reports(self.policy, baseline, candidate)
        self.assertEqual("fail", failed["status"])
        self.assertTrue(any("throughput regression" in finding for finding in failed["failures"]))

        candidate = copy.deepcopy(baseline)
        candidate["durability_contract"] = "sync-flush-required-replica-acks"
        mismatched = qualification.compare_reports(self.policy, baseline, candidate)
        self.assertEqual("fail", mismatched["status"])
        self.assertTrue(any("durability contracts differ" in finding for finding in mismatched["failures"]))


if __name__ == "__main__":
    unittest.main()
