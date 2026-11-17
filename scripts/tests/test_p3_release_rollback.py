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
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "scripts" / "run-architecture-release-rollback.ps1"
ROLLBACK_POLICY = ROOT / "distribution" / "kubernetes" / "rollback-policy.json"
MAINTENANCE_POLICY = ROOT / "distribution" / "config" / "maintenance-policy.json"
APPLY_ORDER = ["controller", "namesrv", "broker", "proxy", "mcp"]


class ReleaseRollbackTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.powershell = shutil.which("pwsh") or shutil.which("powershell")
        if cls.powershell is None:
            raise unittest.SkipTest("PowerShell is required for rollback runner tests")
        code_page = subprocess.run(
            [cls.powershell, "-NoProfile", "-Command", "[Console]::OutputEncoding.CodePage"],
            capture_output=True,
            encoding="ascii",
            check=True,
        ).stdout.strip()
        cls.powershell_encoding = f"cp{code_page}"
        cls.policy = json.loads(ROLLBACK_POLICY.read_text(encoding="utf-8"))
        cls.maintenance_policy = json.loads(MAINTENANCE_POLICY.read_text(encoding="utf-8"))
        cls.runner_source = RUNNER.read_text(encoding="utf-8")

    def test_policy_pins_fail_closed_maintenance_authorization(self) -> None:
        reference = self.policy["maintenance_policy"]
        self.assertEqual(1, reference["version"])
        self.assertEqual(hashlib.sha256(MAINTENANCE_POLICY.read_bytes()).hexdigest(), reference["sha256"])
        self.assertTrue(self.maintenance_policy["require_authentication"])
        self.assertTrue(self.maintenance_policy["require_authorization"])
        self.assertTrue(self.maintenance_policy["require_fencing_token"])
        self.assertEqual(1, self.maintenance_policy["resource_budget"]["max_concurrent_operations"])

        grants = {
            grant["role"]: set(grant["capabilities"])
            for grant in self.maintenance_policy["role_grants"]
        }
        self.assertEqual({"release_checkpoint"}, grants["release_operator"])
        self.assertNotIn("release_checkpoint", grants.get("administrator", set()))

    def test_state_machine_exercises_failure_resume_and_reverse_compensation(self) -> None:
        result = self.run_validate()
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("RELEASE_ROLLBACK_VALIDATION_OK", result.stdout)
        self.assertIn("failure_cases=5", result.stdout)
        self.assertIn("resume_prefixes=6", result.stdout)
        self.assertIn("compensation_prefixes=21", result.stdout)
        self.assertEqual(APPLY_ORDER, self.policy["apply_order"])
        self.assertEqual(list(reversed(APPLY_ORDER)), self.policy["compensation_order"])

    def test_runner_uses_lease_fencing_and_resource_version_cas(self) -> None:
        for contract in (
            "coordination.k8s.io/v1",
            "metadata.resourceVersion",
            "Acquire-RollbackLease",
            "Renew-RollbackLease",
            "Save-JournalRecord",
            "Get-PendingApplyStages",
            "Get-PendingCompensationStages",
            "persistent_volume_uids",
            "acknowledged_messages_preserved",
            "consumer_offsets_preserved",
        ):
            self.assertIn(contract, self.runner_source)

        lowered = self.runner_source.lower()
        self.assertNotIn("helm uninstall", lowered)
        self.assertNotIn('"delete",\n            "persistentvolumeclaim"', lowered)
        self.assertTrue(all(self.policy["preservation"].values()))

    def test_validate_only_rejects_policy_order_drift(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rocketmq-rollback-policy-") as temporary:
            policy_path = Path(temporary) / "rollback-policy.json"
            tampered = json.loads(json.dumps(self.policy))
            tampered["compensation_order"] = APPLY_ORDER
            policy_path.write_text(json.dumps(tampered, indent=2) + "\n", encoding="utf-8")

            result = self.run_validate("-PolicyPath", str(policy_path))
            self.assertNotEqual(0, result.returncode)
            self.assertIn("compensation_order must exactly reverse apply_order", result.stderr)

    def test_validate_only_rejects_partial_artifact_binding(self) -> None:
        result = self.run_validate("-BaselineStatePath", "baseline.json")
        self.assertNotEqual(0, result.returncode)
        self.assertIn(
            "BaselineStatePath, CandidateStatePath, and CheckpointSetPath",
            result.stderr,
        )
        self.assertIn("supplied together", result.stderr)

    def test_complete_v1_checkpoint_set_preserves_backend_key(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rocketmq-rollback-checkpoint-") as temporary:
            root = Path(temporary)
            baseline, candidate, checkpoint_path = self.write_complete_fixture(root)
            arguments = (
                "-Direction", "Rollback",
                "-BaselineStatePath", str(baseline),
                "-CandidateStatePath", str(candidate),
                "-CheckpointSetPath", str(checkpoint_path),
            )
            result = self.run_validate(*arguments, root=root)
            self.assertEqual(0, result.returncode, result.stderr)
            self.assertIn("RELEASE_ROLLBACK_VALIDATION_OK mode=complete", result.stdout)

            checkpoint_set = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            for index, store in enumerate(checkpoint_set["stores"]):
                for retain_backend in (False, True):
                    with self.subTest(backend=store["backend"], retain_backend=retain_backend):
                        tampered = copy.deepcopy(checkpoint_set)
                        changed = tampered["stores"][index]
                        changed["storeKind"] = changed["backend"]
                        if not retain_backend:
                            del changed["backend"]
                        checkpoint_path.write_text(json.dumps(tampered), encoding="utf-8")
                        result = self.run_validate(*arguments, root=root)
                        self.assertNotEqual(0, result.returncode)
                        self.assertIn(
                            "checkpoint set.store properties must be exactly",
                            result.stderr,
                        )

    def write_complete_fixture(self, root: Path) -> tuple[Path, Path, Path]:
        profile_path = Path("distribution/config/production-feature-profile.json")
        profile = json.loads((ROOT / profile_path).read_text(encoding="utf-8"))
        for relative in [
            "scripts/run-architecture-release-rollback.ps1",
            "scripts/set-architecture-release-state.ps1",
            *profile["config_bundle_files"],
        ]:
            destination = root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / relative, destination)

        def write_json(relative: str, value: dict) -> Path:
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
            return path

        def sha256(path: Path) -> str:
            return hashlib.sha256(path.read_bytes()).hexdigest()

        config_files = [
            {"path": relative, "sha256": sha256(root / relative)}
            for relative in profile["config_bundle_files"]
        ]
        manifest = "".join(f"{entry['sha256']}  {entry['path']}\n" for entry in config_files)
        config_digest = "sha256:" + hashlib.sha256(manifest.encode()).hexdigest()
        source_commit = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=True,
        ).stdout.strip()
        generated_at = datetime.now(UTC).isoformat()
        build = {
            "source_commit": source_commit,
            "rust_toolchain": "1.95.0",
            "cargo_lock_sha256": sha256(ROOT / "Cargo.lock"),
            "feature_profile_sha256": sha256(root / profile_path),
            "config_digest": config_digest,
        }
        states = []
        for release_id in ("baseline", "candidate"):
            images = {}
            evidence = []
            for index, (service, config) in enumerate(profile["services"].items(), start=1):
                reference = f"rocketmq-rust/{service}:{source_commit[:7]}"
                binary_hash = str(index) * 64
                sbom_path = write_json(f"{release_id}/{service}.cdx.json", {
                    "bomFormat": "CycloneDX",
                    "specVersion": "1.6",
                    "version": 1,
                    "metadata": {
                        "timestamp": generated_at,
                        "component": {
                            "type": "application", "name": config["package"], "version": "test",
                            "hashes": [{"alg": "SHA-256", "content": binary_hash}],
                        },
                        "properties": [
                            {"name": "rocketmq:source-commit", "value": source_commit},
                            {"name": "rocketmq:image-reference", "value": reference},
                            {"name": "rocketmq:config-digest", "value": config_digest},
                        ],
                    },
                    "components": [{"type": "library", "name": "fixture", "version": "1"}],
                })
                image = {
                    "service": service,
                    "reference": reference,
                    "image_id": "sha256:" + binary_hash,
                    "image_config_digest": "sha256:" + binary_hash,
                    "binary_sha256": binary_hash,
                    "resolved_features": config["resolved_features"],
                }
                sbom = {"path": sbom_path.relative_to(root).as_posix(), "sha256": sha256(sbom_path)}
                evidence.append({**image, "sbom_path": sbom["path"], "sbom_sha256": sbom["sha256"]})
                images[service] = {**image, **build, "sbom": {**sbom, "format": "cyclonedx-1.6-json"}}
            provenance_path = write_json(f"{release_id}/provenance.json", {
                "schema_version": 1,
                "release_id": release_id,
                "generated_at": generated_at,
                **build,
                "local_only": True,
                "remote_push_performed": False,
                "images": evidence,
            })
            for image in images.values():
                image["provenance"] = {
                    "path": provenance_path.relative_to(root).as_posix(),
                    "sha256": sha256(provenance_path),
                    "format": "rocketmq-local-provenance-v1",
                }
            states.append(write_json(f"{release_id}/release-state.json", {
                "schema_version": 1,
                "release_id": release_id,
                "created_at": generated_at,
                "source_commit": source_commit,
                "images": images,
                "config_bundle": {
                    "digest": config_digest,
                    "files": config_files,
                    "helm_values": "distribution/helm/rocketmq-rust/values-production-controller-ha.yaml",
                    "config_map_template": "distribution/helm/rocketmq-rust/templates/configmaps.yaml",
                    "schema": "distribution/config/release-state.schema.json",
                },
                "secret_references": [{
                    "name": "rocketmq-runtime-secrets", "namespace": "rocketmq",
                    "provider": "kubernetes", "version": "local-reference-1",
                    "mount_path": "/var/run/secrets/rocketmq",
                }],
                "identity": {
                    "commit": source_commit, "nonce": release_id, "config_digest": config_digest,
                    "secret_version": "local-reference-1", "storage_generation": 1,
                },
                "storage_generation": 1,
                "cluster_import": {"kind": "none", "name": "rocketmq"},
            }))

        def artifact(checkpoint_id: str) -> dict:
            return {
                "schemaVersion": 1, "checkpointId": checkpoint_id, "checkpointSetId": "set-1",
                "generation": 1, "barrierId": "barrier-1", "createdAtUnixMillis": 123456,
                "lengthBytes": 4096, "sha256": "a" * 64,
                "uri": f"file:///checkpoints/{checkpoint_id}",
            }

        checkpoint_path = write_json("checkpoint-set.json", {
            "schemaVersion": 1, "checkpointSetId": "set-1", "releaseId": "candidate",
            "generation": 1, "barrierId": "barrier-1",
            "policyVersion": self.maintenance_policy["policy_version"],
            "fencingToken": 1, "createdAtUnixMillis": 123456,
            "controller": {
                "artifact": artifact("controller"), "snapshotId": "snapshot-1",
                "lastAppliedIndex": 1, "lastAppliedTerm": 1, "voterIds": [1],
            },
            "stores": [{
                "artifact": artifact(f"broker-{index}"), "memberId": f"broker-{index}",
                "backend": backend,
                "offsets": {"appendedOffset": 100, "durableOffset": 90, "consumeQueueOffset": 80, "indexOffset": 70},
                "storageIdentity": {"volumeId": f"volume-{index}", "walGeneration": 1},
                "walRetained": True, "persistentVolumeRetained": True,
            } for index, backend in enumerate(("local", "rocks_db"))],
        })
        return states[0], states[1], checkpoint_path

    def run_validate(self, *arguments: str, root: Path = ROOT) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                self.powershell,
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(root / "scripts" / RUNNER.name),
                "-ValidateOnly",
                *arguments,
            ],
            cwd=root,
            env={**os.environ, "NO_COLOR": "1"},
            capture_output=True,
            encoding=self.powershell_encoding,
            check=False,
        )


if __name__ == "__main__":
    unittest.main()
