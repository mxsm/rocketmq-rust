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
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import message_path_release as release  # noqa: E402


RUNNER = ROOT / "scripts" / "run-message-path-release.ps1"


class MessagePathReleaseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.powershell = shutil.which("pwsh") or shutil.which("powershell")

    def write_json(self, path: Path, value: object) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
        return path

    def rollback_inputs(self, root: Path) -> argparse.Namespace:
        commit = "b" * 40
        candidate = {
            "status": "pass",
            "measurement_qualified": True,
            "durability_contract": "sync-flush-required-replica-acks",
            "subject": {
                "role": "candidate",
                "commit": commit,
                "deployment_digest": "sha256:" + "2" * 64,
            },
            "target": {
                "target_id": "kind-local-qualified",
                "cluster_uid": "cluster-uid",
                "effective_config_sha256": "sha256:" + "3" * 64,
            },
        }
        baseline = {
            "release_id": "baseline-release",
            "source_commit": "a" * 40,
            "storage_generation": 7,
            "identity": {"commit": "a" * 40, "config_digest": "sha256:" + "3" * 64},
        }
        candidate_state = {
            "release_id": "candidate-release",
            "source_commit": commit,
            "storage_generation": 7,
            "identity": {"commit": commit, "config_digest": "sha256:" + "3" * 64},
        }

        def checkpoint(release_id: str, checkpoint_id: str, fence: int) -> dict[str, object]:
            return {
                "checkpointSetId": checkpoint_id,
                "releaseId": release_id,
                "generation": 7,
                "fencingToken": fence,
                "stores": [{"artifact": {"checkpointId": checkpoint_id + "-store"}}],
            }

        rollback_checkpoint = checkpoint("candidate-release", "rollback-checkpoint", 11)
        forward_checkpoint = checkpoint("baseline-release", "forward-checkpoint", 12)

        def proof(checkpoint_value: dict[str, object], target: str) -> dict[str, object]:
            checkpoint_id = str(checkpoint_value["checkpointSetId"])
            return {
                "schema_version": 1,
                "checkpoint_set_id": checkpoint_id,
                "target_release_id": target,
                "generation": 7,
                "fencing_token": checkpoint_value["fencingToken"],
                "verified_at": "2026-08-12T10:00:00Z",
                "acknowledged_messages_preserved": True,
                "consumer_offsets_preserved": True,
                "wal_retained": True,
                "persistent_volumes_reused": True,
                "store_checkpoint_ids": [checkpoint_id + "-store"],
            }

        paths = {
            "candidate_measurement": self.write_json(root / "candidate.json", candidate),
            "baseline_state": self.write_json(root / "baseline-state.json", baseline),
            "candidate_state": self.write_json(root / "candidate-state.json", candidate_state),
            "rollback_checkpoint": self.write_json(root / "rollback-checkpoint.json", rollback_checkpoint),
            "forward_checkpoint": self.write_json(root / "forward-checkpoint.json", forward_checkpoint),
            "rollback_proof": self.write_json(
                root / "rollback-proof.json", proof(rollback_checkpoint, "baseline-release")
            ),
            "forward_proof": self.write_json(
                root / "forward-proof.json", proof(forward_checkpoint, "candidate-release")
            ),
        }
        rollback_log = root / "rollback-source.log"
        rollback_log.write_text(
            "RELEASE_ROLLBACK_OK operation_id=one direction=Rollback target_release_id=baseline-release\n",
            encoding="utf-8",
        )
        forward_log = root / "forward-source.log"
        forward_log.write_text(
            "RELEASE_ROLLBACK_OK operation_id=two direction=Forward target_release_id=candidate-release\n",
            encoding="utf-8",
        )
        return argparse.Namespace(
            **paths,
            rollback_log=rollback_log,
            forward_log=forward_log,
            output=root / "rollback" / "rollback-evidence.json",
        )

    def test_committed_schema_is_closed(self) -> None:
        schema = release.read_json(release.DEFAULT_SCHEMA)
        release.validate_schema_contract(schema)

    def test_release_runner_validates_all_owned_contracts(self) -> None:
        if self.powershell is None:
            self.skipTest("PowerShell is required for release runner tests")
        result = subprocess.run(
            [
                self.powershell,
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(RUNNER),
                "-Mode",
                "Validate",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("MESSAGE_PATH_RELEASE_VALIDATION_OK", result.stdout)
        self.assertIn("RELEASE_ROLLBACK_VALIDATION_OK", result.stdout)

    def test_release_runner_requires_bidirectional_safe_rehearsal(self) -> None:
        source = RUNNER.read_text(encoding="utf-8")
        for contract in (
            'Invoke-RollbackTransition $powerShell "Rollback"',
            'Invoke-RollbackTransition $powerShell "Forward"',
            '"--rollback-evidence", $rollbackEvidence',
            '$ReleaseScript, "package"',
            '$ReleaseScript, "verify"',
        ):
            self.assertIn(contract, source)
        lowered = source.lower()
        self.assertNotIn("helm uninstall", lowered)
        self.assertNotIn("delete persistentvolumeclaim", lowered)

    def test_build_rollback_evidence_binds_both_transitions(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rocketmq-release-") as temporary:
            args = self.rollback_inputs(Path(temporary))
            evidence = release.build_rollback_evidence(args)

            self.assertTrue(evidence["rehearsal_qualified"])
            self.assertEqual(["rollback", "forward"], [step["direction"] for step in evidence["steps"]])
            self.assertTrue(all(evidence["assertions"].values()))
            self.assertEqual("sha256:" + release.sha256_file(args.candidate_measurement), evidence["candidate_measurement_sha256"])
            self.assertEqual(8, len(evidence["artifacts"]))
            self.assertTrue(args.output.is_file())

    def test_build_rollback_evidence_rejects_preservation_drift(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rocketmq-release-") as temporary:
            args = self.rollback_inputs(Path(temporary))
            proof = release.read_json(args.rollback_proof)
            proof["acknowledged_messages_preserved"] = False
            self.write_json(args.rollback_proof, proof)

            with self.assertRaisesRegex(release.ReleaseError, "acknowledged_messages_preserved"):
                release.build_rollback_evidence(args)

    def bundle_source(self, root: Path, commit: str = "b" * 40) -> Path:
        for name in release.REQUIRED_RELEASE_DIRECTORIES:
            (root / name).mkdir(parents=True, exist_ok=True)
        (root / "environment" / "toolchain-lock.json").write_text("{}\n", encoding="utf-8")
        for name in ("ab", "fault", "rpo", "soak"):
            (root / name / "raw.json").write_text("{}\n", encoding="utf-8")
        qualification = {
            "artifact_kind": "rocketmq_message_path_qualification_report",
            "status": "pass",
            "release_qualified": True,
            "candidate_commit": commit,
            "subject": {"commit": commit},
            "target": {"target_id": "kind-local-qualified"},
            "durability_contract": "sync-flush-required-replica-acks",
        }
        rollback = {
            "artifact_kind": "rocketmq_message_path_rollback_evidence",
            "status": "pass",
            "rehearsal_qualified": True,
            "candidate_commit": commit,
        }
        self.write_json(root / "qualification" / "qualification-report.json", qualification)
        self.write_json(root / "qualification" / "rollback" / "rollback-evidence.json", rollback)
        return root

    def package_args(self, source: Path, archive: Path) -> argparse.Namespace:
        return argparse.Namespace(
            source_root=source,
            archive_output=archive,
            qualification_report=Path("qualification/qualification-report.json"),
            rollback_evidence=Path("qualification/rollback/rollback-evidence.json"),
            minisign_secret_key=None,
            read_only=False,
        )

    def verify_args(self, bundle: Path) -> argparse.Namespace:
        return argparse.Namespace(
            bundle=bundle,
            qualification_report=Path("qualification/qualification-report.json"),
            rollback_evidence=Path("qualification/rollback/rollback-evidence.json"),
            minisign_public_key=None,
        )

    def test_package_and_verify_bind_every_file(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rocketmq-release-") as temporary:
            root = Path(temporary)
            source = self.bundle_source(root / "source")
            archive = root / "archive"
            inventory = release.package_bundle(self.package_args(source, archive))

            self.assertGreaterEqual(inventory["artifact_count"], 7)
            self.assertEqual(inventory, release.verify_bundle(self.verify_args(archive)))

            (archive / "fault" / "raw.json").write_text('{"tampered":true}\n', encoding="utf-8")
            with self.assertRaisesRegex(release.ReleaseError, "hash or size differs"):
                release.verify_bundle(self.verify_args(archive))

    def test_package_rejects_secret_like_paths_and_existing_output(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rocketmq-release-") as temporary:
            root = Path(temporary)
            source = self.bundle_source(root / "source")
            (source / "environment" / "runtime-secret.yaml").write_text("forbidden\n", encoding="utf-8")
            with self.assertRaisesRegex(release.ReleaseError, "secret-like"):
                release.package_bundle(self.package_args(source, root / "archive"))

            source = self.bundle_source(root / "source-plural")
            (source / "environment" / "credentials.json").write_text("forbidden\n", encoding="utf-8")
            with self.assertRaisesRegex(release.ReleaseError, "secret-like"):
                release.package_bundle(self.package_args(source, root / "archive-plural"))

            source = self.bundle_source(root / "source-clean")
            archive = root / "existing"
            archive.mkdir()
            with self.assertRaisesRegex(release.ReleaseError, "already exists"):
                release.package_bundle(self.package_args(source, archive))

    def test_rollback_candidate_binding_is_not_inferred(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rocketmq-release-") as temporary:
            args = self.rollback_inputs(Path(temporary))
            state = release.read_json(args.candidate_state)
            state = copy.deepcopy(state)
            state["source_commit"] = "c" * 40
            self.write_json(args.candidate_state, state)
            with self.assertRaisesRegex(release.ReleaseError, "commit differs"):
                release.build_rollback_evidence(args)


if __name__ == "__main__":
    unittest.main()
