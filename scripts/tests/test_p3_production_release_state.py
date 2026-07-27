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
import shutil
import subprocess
import tomllib
import unittest
import uuid
from datetime import UTC, datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
PROFILE_PATH = ROOT / "distribution" / "config" / "production-feature-profile.json"
STATE_SCHEMA_PATH = ROOT / "distribution" / "config" / "release-state.schema.json"
PROVENANCE_SCHEMA_PATH = ROOT / "distribution" / "config" / "image-provenance.schema.json"
SBOM_SCHEMA_PATH = ROOT / "distribution" / "config" / "image-sbom.schema.json"
TRANSITION_POLICY_PATH = ROOT / "distribution" / "kubernetes" / "release-state-transition-policy.json"
SERVICES = ("broker", "namesrv", "controller", "proxy", "mcp")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def feature_closure(features: dict[str, list[str]], roots: list[str]) -> list[str]:
    resolved: set[str] = set()
    pending = list(roots)
    while pending:
        feature = pending.pop()
        if feature in resolved:
            continue
        if feature not in features:
            raise AssertionError(f"missing feature {feature}")
        resolved.add(feature)
        for member in features[feature]:
            candidate = member.split("/", 1)[0]
            if candidate in features:
                pending.append(candidate)
    return sorted(resolved)


class ProductionReleaseStateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.profile = json.loads(PROFILE_PATH.read_text(encoding="utf-8"))
        cls.state_schema = json.loads(STATE_SCHEMA_PATH.read_text(encoding="utf-8"))
        cls.provenance_schema = json.loads(PROVENANCE_SCHEMA_PATH.read_text(encoding="utf-8"))
        cls.sbom_schema = json.loads(SBOM_SCHEMA_PATH.read_text(encoding="utf-8"))
        cls.transition_policy = json.loads(TRANSITION_POLICY_PATH.read_text(encoding="utf-8"))
        cls.builder = (ROOT / "scripts" / "build-production-images.ps1").read_text(encoding="utf-8")
        cls.reconciler = (ROOT / "scripts" / "set-architecture-release-state.ps1").read_text(encoding="utf-8")
        cls.dockerfile = (ROOT / "docker" / "Dockerfile.base").read_text(encoding="utf-8")

    def test_production_features_are_explicit_and_resolve_exactly(self) -> None:
        self.assertEqual("production", self.profile["profile"])
        self.assertTrue(self.profile["build_mode"]["locked"])
        self.assertTrue(self.profile["build_mode"]["release"])
        self.assertFalse(self.profile["build_mode"]["default_features"])
        self.assertTrue(self.profile["build_mode"]["local_images_only"])
        self.assertFalse(self.profile["build_mode"]["remote_push_enabled"])
        self.assertEqual(set(SERVICES), set(self.profile["services"]))

        for service_name, service in self.profile["services"].items():
            with self.subTest(service=service_name):
                manifest = tomllib.loads((ROOT / service["manifest"]).read_text(encoding="utf-8"))
                features = manifest["features"]
                self.assertEqual(["production"], service["features"])
                self.assertEqual(
                    service["resolved_features"],
                    feature_closure(features, service["features"]),
                )
                command = (
                    f"cargo build --locked --release --package {service['package']} "
                    f"--no-default-features --features production --bin {service['binary']}"
                )
                self.assertIn(command, self.dockerfile)

    def test_builder_is_local_only_and_binds_verifiable_metadata(self) -> None:
        self.assertIn("docker buildx build", self.builder)
        self.assertIn("--load", self.builder)
        self.assertNotIn("--push", self.builder)
        self.assertNotIn("[System.IO.Path]::GetRelativePath", self.builder)
        self.assertIn("production images must be built from a clean checkout", self.builder)
        self.assertIn('imageReference = "rocketmq-rust/$serviceName`:$shortCommit"', self.builder)
        self.assertIn("Invoke-Captured -Executable docker -Arguments @(", self.builder)
        self.assertIn(".MakeRelativeUri(", self.builder)
        for field in (
            "source_commit",
            "rust_toolchain",
            "cargo_lock_sha256",
            "feature_profile_sha256",
            "resolved_features",
            "binary_sha256",
            "image_id",
            "image_config_digest",
            "config_digest",
            "sbom",
            "provenance",
        ):
            self.assertIn(field, self.builder)
        for label in (
            "io.rocketmq.build.rust-toolchain",
            "io.rocketmq.build.cargo-lock-sha256",
            "io.rocketmq.build.production-feature-profile-sha256",
            "io.rocketmq.release.config-digest",
        ):
            self.assertIn(label, self.dockerfile)

    def test_schemas_define_complete_release_and_local_provenance(self) -> None:
        self.assertEqual("https://json-schema.org/draft/2020-12/schema", self.state_schema["$schema"])
        self.assertFalse(self.state_schema["additionalProperties"])
        self.assertEqual(
            {
                "schema_version",
                "release_id",
                "created_at",
                "source_commit",
                "images",
                "config_bundle",
                "secret_references",
                "identity",
                "storage_generation",
                "cluster_import",
            },
            set(self.state_schema["required"]),
        )
        self.assertEqual(set(SERVICES), set(self.state_schema["properties"]["images"]["required"]))
        identity = self.state_schema["properties"]["identity"]
        self.assertEqual(
            {"commit", "nonce", "config_digest", "secret_version", "storage_generation"},
            set(identity["required"]),
        )
        secret_references = self.state_schema["properties"]["secret_references"]
        self.assertEqual(1, secret_references["minItems"])
        self.assertEqual(1, secret_references["maxItems"])
        self.assertEqual(True, self.provenance_schema["properties"]["local_only"]["const"])
        self.assertEqual(False, self.provenance_schema["properties"]["remote_push_performed"]["const"])
        self.assertEqual("CycloneDX", self.sbom_schema["properties"]["bomFormat"]["const"])
        self.assertEqual("1.6", self.sbom_schema["properties"]["specVersion"]["const"])

    def test_transition_policy_is_ordered_and_compensates_in_reverse(self) -> None:
        apply_order = [entry["step"] for entry in self.transition_policy["apply_order"]]
        self.assertEqual(list(reversed(apply_order)), self.transition_policy["compensation_order"])
        self.assertTrue(self.transition_policy["local_images_only"])
        self.assertFalse(self.transition_policy["remote_push_enabled"])
        self.assertTrue(self.transition_policy["forbid_image_only_transition"])
        self.assertIn("kind load docker-image", self.reconciler)
        self.assertIn("k3d image import", self.reconciler)
        self.assertIn("helm rollback", self.reconciler)
        self.assertIn("previous-active-release-state.json", self.reconciler)
        self.assertIn('"before"', self.reconciler)
        self.assertIn('"after"', self.reconciler)

    def test_helm_release_identity_binds_config_secret_and_storage(self) -> None:
        values = (ROOT / "distribution" / "helm" / "rocketmq-rust" / "values.yaml").read_text(encoding="utf-8")
        values_schema = (
            ROOT / "distribution" / "helm" / "rocketmq-rust" / "values.schema.json"
        ).read_text(encoding="utf-8")
        helpers = (
            ROOT / "distribution" / "helm" / "rocketmq-rust" / "templates" / "_helpers.tpl"
        ).read_text(encoding="utf-8")
        for field in ("configDigest", "secretVersion", "storageGeneration"):
            self.assertIn(field, values)
            self.assertIn(field, values_schema)
            self.assertIn(field, helpers)
        for annotation in (
            "rocketmq.apache.org/release-config-digest",
            "rocketmq.apache.org/release-secret-version",
            "rocketmq.apache.org/storage-generation",
        ):
            self.assertIn(annotation, helpers)
            self.assertEqual(
                5,
                (ROOT / "distribution" / "kubernetes" / "base" / "manifest.yaml")
                .read_text(encoding="utf-8")
                .count(annotation),
            )
        self.assertEqual(5, (ROOT / "distribution" / "helm" / "rocketmq-rust" / "templates" / "workloads.yaml")
                         .read_text(encoding="utf-8")
                         .count('include "rocketmq.releaseAnnotations"'))

    def test_validate_only_accepts_complete_state_and_rejects_secret_material(self) -> None:
        powershell = shutil.which("pwsh") or shutil.which("powershell")
        self.assertIsNotNone(powershell, "PowerShell is required to validate the release-state reconciler")
        fixture_root = ROOT / ".rocketmq" / f"release-state-test-{uuid.uuid4().hex}"
        fixture_root.mkdir(parents=True)
        try:
            state_path = self.write_fixture(fixture_root)
            result = self.run_validate(powershell, state_path)
            self.assertEqual(0, result.returncode, result.stderr)
            self.assertIn("RELEASE_STATE_VALIDATION_OK mode=schema", result.stdout)

            state = json.loads(state_path.read_text(encoding="utf-8"))
            state["secret_references"][0]["secret_value"] = "must-not-appear"
            state_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
            result = self.run_validate(powershell, state_path)
            self.assertNotEqual(0, result.returncode)
            self.assertIn("forbidden Secret field", result.stderr)
        finally:
            shutil.rmtree(fixture_root)

    def write_fixture(self, fixture_root: Path) -> Path:
        source_commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        config_files = []
        for relative in self.profile["config_bundle_files"]:
            config_files.append({"path": relative, "sha256": sha256_file(ROOT / relative)})
        manifest = "".join(f"{entry['sha256']}  {entry['path']}\n" for entry in config_files)
        config_digest = "sha256:" + sha256_bytes(manifest.encode())
        cargo_lock_sha256 = sha256_file(ROOT / "Cargo.lock")
        feature_profile_sha256 = sha256_file(PROFILE_PATH)
        generated_at = datetime.now(UTC).isoformat()

        evidence = []
        sboms: dict[str, tuple[str, str]] = {}
        for index, service_name in enumerate(SERVICES, start=1):
            sbom = {
                "bomFormat": "CycloneDX",
                "specVersion": "1.6",
                "version": 1,
                "metadata": {
                    "timestamp": generated_at,
                    "component": {
                        "type": "application",
                        "name": self.profile["services"][service_name]["package"],
                        "version": "test",
                        "hashes": [{"alg": "SHA-256", "content": f"{index}" * 64}],
                    },
                    "properties": [
                        {"name": "rocketmq:source-commit", "value": source_commit},
                        {"name": "rocketmq:image-reference", "value": f"rocketmq-rust/{service_name}:abcdef1"},
                        {"name": "rocketmq:config-digest", "value": config_digest},
                    ],
                },
                "components": [{"type": "library", "name": "fixture", "version": "1"}],
            }
            sbom_path = fixture_root / f"{service_name}.cdx.json"
            sbom_path.write_text(json.dumps(sbom, indent=2) + "\n", encoding="utf-8")
            relative_sbom = sbom_path.relative_to(ROOT).as_posix()
            sboms[service_name] = (relative_sbom, sha256_file(sbom_path))
            evidence.append(
                {
                    "service": service_name,
                    "reference": f"rocketmq-rust/{service_name}:abcdef1",
                    "image_id": "sha256:" + f"{index}" * 64,
                    "image_config_digest": "sha256:" + f"{index}" * 64,
                    "binary_sha256": f"{index}" * 64,
                    "resolved_features": self.profile["services"][service_name]["resolved_features"],
                    "sbom_path": relative_sbom,
                    "sbom_sha256": sha256_file(sbom_path),
                }
            )

        provenance = {
            "schema_version": 1,
            "release_id": "abcdef1-local-abcdef1",
            "generated_at": generated_at,
            "source_commit": source_commit,
            "rust_toolchain": "1.95.0",
            "cargo_lock_sha256": cargo_lock_sha256,
            "feature_profile_sha256": feature_profile_sha256,
            "config_digest": config_digest,
            "local_only": True,
            "remote_push_performed": False,
            "images": evidence,
        }
        provenance_path = fixture_root / "provenance.json"
        provenance_path.write_text(json.dumps(provenance, indent=2) + "\n", encoding="utf-8")
        relative_provenance = provenance_path.relative_to(ROOT).as_posix()
        provenance_sha256 = sha256_file(provenance_path)

        images = {}
        for index, service_name in enumerate(SERVICES, start=1):
            images[service_name] = {
                "service": service_name,
                "reference": f"rocketmq-rust/{service_name}:abcdef1",
                "image_id": "sha256:" + f"{index}" * 64,
                "image_config_digest": "sha256:" + f"{index}" * 64,
                "binary_sha256": f"{index}" * 64,
                "source_commit": source_commit,
                "rust_toolchain": "1.95.0",
                "cargo_lock_sha256": cargo_lock_sha256,
                "feature_profile_sha256": feature_profile_sha256,
                "resolved_features": self.profile["services"][service_name]["resolved_features"],
                "config_digest": config_digest,
                "sbom": {
                    "path": sboms[service_name][0],
                    "sha256": sboms[service_name][1],
                    "format": "cyclonedx-1.6-json",
                },
                "provenance": {
                    "path": relative_provenance,
                    "sha256": provenance_sha256,
                    "format": "rocketmq-local-provenance-v1",
                },
            }
        state = {
            "schema_version": 1,
            "release_id": "abcdef1-local-abcdef1",
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
            "secret_references": [
                {
                    "name": "rocketmq-runtime-secrets",
                    "namespace": "rocketmq",
                    "provider": "kubernetes",
                    "version": "local-reference-1",
                    "mount_path": "/var/run/secrets/rocketmq",
                }
            ],
            "identity": {
                "commit": source_commit,
                "nonce": "local-abcdef1",
                "config_digest": config_digest,
                "secret_version": "local-reference-1",
                "storage_generation": 1,
            },
            "storage_generation": 1,
            "cluster_import": {"kind": "none", "name": "rocketmq"},
        }
        state_path = fixture_root / "release-state.json"
        state_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
        return state_path

    @staticmethod
    def run_validate(powershell: str, state_path: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                powershell,
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(ROOT / "scripts" / "set-architecture-release-state.ps1"),
                "-StatePath",
                str(state_path),
                "-ValidateOnly",
                "-SchemaOnly",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )


if __name__ == "__main__":
    unittest.main()
