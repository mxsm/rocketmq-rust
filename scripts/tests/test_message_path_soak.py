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

import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import message_path_soak as soak  # noqa: E402


class MessagePathSoakTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = soak.read_json(ROOT / "scripts" / "message-path-soak-policy.json")
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        self.identity = self.directory / "release-identity.json"
        self.identity.write_text(
            json.dumps(
                {
                    "commit": "a" * 40,
                    "deployment_digest": "sha256:" + "b" * 64,
                    "target_id": "kind-local",
                    "cluster_uid": "cluster-uid",
                    "effective_config_sha256": "sha256:" + "c" * 64,
                    "durability_contract": "sync-flush-required-replica-acks",
                }
            ),
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def write_samples(
        self,
        *,
        rss_growth: bool = False,
        queue_overflow: bool = False,
        restart: bool = False,
        omit_metric: str | None = None,
        skip_timestamps: set[int] | None = None,
    ) -> Path:
        path = self.directory / "raw-samples.ndjson"
        records: list[dict[str, object]] = []
        skip_timestamps = skip_timestamps or set()
        for step in range(16):
            if step in skip_timestamps:
                continue
            timestamp = 1_700_000_000 + step * 5
            records.append(
                {
                    "timestamp": timestamp,
                    "pod": {
                        "name": "broker-0",
                        "uid": "pod-uid",
                        "restarts": 1 if restart and step == 15 else 0,
                        "oom_killed": False,
                    },
                }
            )
            rss = 128 * 1024 * 1024 + (step * 48 * 1024 * 1024 if rss_growth else step * 1024)
            queue = 120 if queue_overflow and step == 7 else (0 if step > 12 else 10)
            metrics = {
                "process_rss_bytes": ("pod/broker-0", rss),
                "process_memory_limit_bytes": ("pod/broker-0", 1024 * 1024 * 1024),
                "process_tasks": ("pod/broker-0", 12),
                "process_threads": ("pod/broker-0", 8),
                "process_open_fds": ("pod/broker-0", 32),
                "rocketmq_runtime_tasks": ("component=broker", 12),
                "rocketmq_resource_queue_items": ("budget=commands", queue),
                "rocketmq_resource_queue_capacity_items": ("budget=commands", 100),
                "rocketmq_resource_queue_bytes": ("budget=commands", queue * 1024),
                "rocketmq_resource_queue_capacity_bytes": ("budget=commands", 100 * 1024),
                "rocketmq_resource_cache_usage_bytes": ("budget=rocksdb", 128 * 1024 * 1024),
                "rocketmq_resource_cache_budget_bytes": ("budget=rocksdb", 512 * 1024 * 1024),
                "rocketmq_storage_flush_behind_bytes": ("component=store", 0),
                "rocketmq_storage_dispatch_behind_bytes": ("component=store", 0),
                "rocketmq_store_ha_replication_lag_bytes": ("component=store", 0),
                "rocketmq_receipt_renewal_due_lag_micros": ("component=proxy", 10_000),
            }
            for metric, (scope, value) in metrics.items():
                if metric == omit_metric:
                    continue
                records.append({"timestamp": timestamp, "metric": metric, "scope": scope, "value": value})
        path.write_text("\n".join(json.dumps(record) for record in records) + "\n", encoding="utf-8")
        return path

    def analyze(self, samples: Path) -> dict[str, object]:
        output = self.directory / "soak-report.json"
        return soak.analyze(self.policy, "smoke", samples, self.identity, output, None)

    def test_policy_preserves_six_hour_release_contract(self) -> None:
        self.assertEqual([], soak.validate_policy(self.policy))
        full = self.policy["profiles"]["full"]
        self.assertEqual(1800, full["warmup_seconds"])
        self.assertEqual(21600, full["observation_seconds"])
        self.assertEqual(900, full["cooldown_seconds"])

    def test_stable_series_pass_and_bind_raw_artifact(self) -> None:
        report = self.analyze(self.write_samples())
        self.assertEqual("pass", report["status"])
        self.assertFalse(report["monotonic_growth_detected"])
        self.assertEqual([], soak.validate_report(self.policy, self.directory / "soak-report.json"))

    def test_rss_monotonic_growth_fails(self) -> None:
        report = self.analyze(self.write_samples(rss_growth=True))
        self.assertEqual("fail", report["status"])
        self.assertTrue(report["monotonic_growth_detected"])

    def test_queue_capacity_violation_fails(self) -> None:
        report = self.analyze(self.write_samples(queue_overflow=True))
        self.assertEqual("fail", report["status"])
        self.assertTrue(any("hard capacity" in finding for finding in report["failures"]))

    def test_missing_required_metric_fails_closed(self) -> None:
        report = self.analyze(self.write_samples(omit_metric="rocketmq_runtime_tasks"))
        self.assertEqual("fail", report["status"])
        self.assertTrue(any("required metric rocketmq_runtime_tasks" in finding for finding in report["failures"]))

    def test_restart_fails(self) -> None:
        report = self.analyze(self.write_samples(restart=True))
        self.assertEqual("fail", report["status"])
        self.assertTrue(any("restarted" in finding for finding in report["failures"]))

    def test_sampling_gap_fails(self) -> None:
        report = self.analyze(self.write_samples(skip_timestamps={4, 5, 6}))
        self.assertEqual("fail", report["status"])
        self.assertTrue(any("maximum gap" in finding for finding in report["failures"]))

    def test_artifact_tamper_is_rejected(self) -> None:
        samples = self.write_samples()
        self.analyze(samples)
        samples.write_text(samples.read_text(encoding="utf-8") + "{}\n", encoding="utf-8")
        findings = soak.validate_report(self.policy, self.directory / "soak-report.json")
        self.assertTrue(any("tampered" in finding for finding in findings))

    def test_invalid_full_duration_policy_is_rejected(self) -> None:
        invalid = copy.deepcopy(self.policy)
        invalid["profiles"]["full"]["observation_seconds"] = 3600
        self.assertTrue(any("6h observation" in finding for finding in soak.validate_policy(invalid)))


if __name__ == "__main__":
    unittest.main()
