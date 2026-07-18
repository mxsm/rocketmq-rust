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

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
GUARD = REPO_ROOT / "scripts" / "kubernetes_assets_guard.py"


class KubernetesAssetsGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="m11-kubernetes-assets-")
        self.root = Path(self.temporary.name)
        shutil.copytree(REPO_ROOT / "distribution", self.root / "distribution")
        (self.root / "docker").mkdir()
        shutil.copy2(REPO_ROOT / "docker" / "container-policy.json", self.root / "docker" / "container-policy.json")
        (self.root / "scripts").mkdir()
        shutil.copy2(
            REPO_ROOT / "scripts" / "kubernetes-assets-contract.ps1",
            self.root / "scripts" / "kubernetes-assets-contract.ps1",
        )
        (self.root / ".github" / "workflows").mkdir(parents=True)
        shutil.copy2(
            REPO_ROOT / ".github" / "workflows" / "kubernetes-assets-ci.yml",
            self.root / ".github" / "workflows" / "kubernetes-assets-ci.yml",
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def run_guard(self, expect_success: bool) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            [sys.executable, str(GUARD), "--root", str(self.root)],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        if expect_success:
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("KUBERNETES_ASSETS_GUARD_OK", result.stdout)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertIn("KUBERNETES_ASSETS_GUARD_ERROR", result.stderr)
        return result

    def mutate_text(self, relative: str, old: str, new: str, count: int = 1) -> None:
        path = self.root / relative
        source = path.read_text(encoding="utf-8")
        self.assertIn(old, source)
        path.write_text(source.replace(old, new, count), encoding="utf-8")

    def test_repository_contract_passes(self) -> None:
        self.run_guard(expect_success=True)

    def test_controller_quorum_drift_is_rejected(self) -> None:
        path = self.root / "distribution/kubernetes/deployment-policy.json"
        policy = json.loads(path.read_text(encoding="utf-8"))
        policy["services"]["controller"]["replicas"] = 2
        path.write_text(json.dumps(policy, indent=2), encoding="utf-8")
        result = self.run_guard(expect_success=False)
        self.assertIn("Controller", result.stderr)

    def test_inline_secret_object_is_rejected(self) -> None:
        path = self.root / "distribution/kubernetes/base/manifest.yaml"
        with path.open("a", encoding="utf-8") as stream:
            stream.write("\n---\napiVersion: v1\nkind: Secret\nmetadata:\n  name: inline\nstringData:\n  token: unsafe\n")
        result = self.run_guard(expect_success=False)
        self.assertIn("kind: Secret", result.stderr)

    def test_mutable_image_tag_is_rejected(self) -> None:
        path = self.root / "distribution/kubernetes/base/manifest.yaml"
        with path.open("a", encoding="utf-8") as stream:
            stream.write("\n# forbidden regression\n# image: ghcr.io/mxsm/rocketmq-rust/broker:latest\n")
        result = self.run_guard(expect_success=False)
        self.assertIn(":latest", result.stderr)

    def test_readiness_probe_before_m11_10_is_rejected(self) -> None:
        self.mutate_text(
            "distribution/helm/rocketmq-rust/templates/workloads.yaml",
            "          resources:\n",
            "          readinessProbe: {tcpSocket: {port: 10911}}\n          resources:\n",
        )
        result = self.run_guard(expect_success=False)
        self.assertIn("M11-10", result.stderr)

    def test_capability_drop_weakening_is_rejected(self) -> None:
        self.mutate_text(
            "distribution/kubernetes/base/manifest.yaml",
            "            - ALL",
            "            - NET_ADMIN",
        )
        result = self.run_guard(expect_success=False)
        self.assertIn("missing security/topology contract - ALL", result.stderr)

    def test_secure_overlay_placeholder_digest_is_rejected(self) -> None:
        self.mutate_text(
            "distribution/kubernetes/overlays/secure/kustomization.yaml",
            "sha256:" + "1" * 64,
            "sha256:" + "0" * 64,
        )
        result = self.run_guard(expect_success=False)
        self.assertIn("broker", result.stderr)

    def test_proxy_pvc_regression_is_rejected(self) -> None:
        path = self.root / "distribution/kubernetes/deployment-policy.json"
        policy = json.loads(path.read_text(encoding="utf-8"))
        policy["services"]["proxy"]["persistence"] = {
            "required": True,
            "size": "10Gi",
            "storage_class_required": True,
            "retention": "Retain",
        }
        path.write_text(json.dumps(policy, indent=2), encoding="utf-8")
        result = self.run_guard(expect_success=False)
        self.assertIn("stateless", result.stderr)


if __name__ == "__main__":
    unittest.main()
