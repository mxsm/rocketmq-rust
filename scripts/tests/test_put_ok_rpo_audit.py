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

import argparse
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import put_ok_rpo_audit as audit  # noqa: E402


class PutOkRpoAuditTest(unittest.TestCase):
    def write_ndjson(self, path: Path, records: list[dict]) -> None:
        path.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")

    def fixture(self, root: Path, count: int = 3) -> argparse.Namespace:
        ledger = []
        for sequence in range(count):
            ledger.append({
                "sequence": sequence,
                "audit_id": f"audit-{sequence}",
                "unique_key": f"key-{sequence}",
                "broker_message_id": f"msg-{sequence}",
                "offset_message_id": f"offset-{sequence}",
                "broker_name": "rocketmq-broker",
                "queue_id": 0,
                "queue_offset": sequence,
                "commit_log_offset": sequence * 100,
                "store_size": 100,
                "end_offset": (sequence + 1) * 100,
                "payload_sha256": "sha256:" + f"{sequence + 1:x}" * 64,
                "put_ok_at_utc": "2026-08-12T00:00:00Z",
            })
        ledger_path = root / "ledger.ndjson"
        observed_path = root / "observed.ndjson"
        confirm_path = root / "confirm.ndjson"
        timelines_path = root / "timelines.json"
        self.write_ndjson(ledger_path, ledger)
        self.write_ndjson(observed_path, ledger)
        self.write_ndjson(confirm_path, [{"authority_epoch": 1, "confirm_offset": 100, "legal_in_sync_ack_offset": 100}])
        timelines_path.write_text(json.dumps([{
            "single_writable_master": True,
            "milestones": [
                {"milestone": name, "elapsed_millis": index * 10}
                for index, name in enumerate(audit.MILESTONES)
            ],
        }]), encoding="utf-8")
        return argparse.Namespace(
            ledger=ledger_path,
            observations=[observed_path],
            timelines=timelines_path,
            confirm_offsets=confirm_path,
            output=root / "report.json",
            run_id="audit-run",
            candidate_commit="a" * 40,
            deployment_digest="sha256:" + "b" * 64,
            target_id="kind-a",
            cluster_uid="cluster-a",
            effective_config_sha256="sha256:" + "c" * 64,
            durability_contract=audit.STRICT_CONTRACT,
            minimum_messages=count,
            repetitions=1,
            max_rto_millis=1_000,
        )

    def test_exact_recovery_passes_with_bound_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            report = audit.qualify(self.fixture(Path(temporary)))
        self.assertEqual("pass", report["status"])
        self.assertTrue(report["strict_qualification_passed"])
        self.assertEqual(3, report["put_ok_messages"]["recovered_once_count"])

    def test_missing_duplicate_and_payload_drift_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            args = self.fixture(Path(temporary))
            records = audit.read_ndjson(args.observations[0])
            records[0]["payload_sha256"] = "sha256:" + "f" * 64
            self.write_ndjson(args.observations[0], [records[0], records[0], records[1]])
            report = audit.qualify(args)
        self.assertEqual("fail", report["status"])
        self.assertEqual(1, report["put_ok_messages"]["missing_count"])
        self.assertEqual(1, report["put_ok_messages"]["duplicate_count"])
        self.assertGreater(report["put_ok_messages"]["payload_mismatch_count"], 0)

    def test_bad_identity_timeline_and_confirm_boundary_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            args = self.fixture(Path(temporary))
            args.candidate_commit = "not-a-commit"
            timelines = json.loads(args.timelines.read_text(encoding="utf-8"))
            timelines[0]["milestones"].reverse()
            args.timelines.write_text(json.dumps(timelines), encoding="utf-8")
            self.write_ndjson(args.confirm_offsets, [{"authority_epoch": 1, "confirm_offset": 101, "legal_in_sync_ack_offset": 100}])
            report = audit.qualify(args)
        self.assertFalse(report["strict_qualification_passed"])
        self.assertTrue(any("candidate_commit" in reason for reason in report["rejection_reasons"]))
        self.assertTrue(any("T0-T5" in reason for reason in report["rejection_reasons"]))
        self.assertFalse(report["confirm_offset"]["valid"])


if __name__ == "__main__":
    unittest.main()
