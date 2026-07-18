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


class ServiceLifecycleGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="m11-service-lifecycle-")
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

    def run_guard(self) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            [sys.executable, str(GUARD), "--root", str(self.root)],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("KUBERNETES_ASSETS_GUARD_ERROR", result.stderr)
        return result

    def mutate_text(self, relative: str, old: str, new: str, count: int = 1) -> None:
        path = self.root / relative
        source = path.read_text(encoding="utf-8")
        self.assertIn(old, source)
        path.write_text(source.replace(old, new, count), encoding="utf-8")

    def mutate_policy(self, key: str, value: object) -> None:
        path = self.root / "distribution/kubernetes/deployment-policy.json"
        policy = json.loads(path.read_text(encoding="utf-8"))
        policy["lifecycle"][key] = value
        path.write_text(json.dumps(policy, indent=2), encoding="utf-8")

    def test_grace_period_that_does_not_cover_shutdown_is_rejected(self) -> None:
        self.mutate_policy("termination_grace_period_seconds", 45)
        result = self.run_guard()
        self.assertIn("lifecycle policy drifted", result.stderr)

    def test_duplicate_shutdown_deadline_extension_is_rejected(self) -> None:
        self.mutate_policy("duplicate_shutdown_extends_deadline", True)
        result = self.run_guard()
        self.assertIn("lifecycle policy drifted", result.stderr)

    def test_tcp_probe_is_rejected(self) -> None:
        self.mutate_text(
            "distribution/helm/rocketmq-rust/templates/workloads.yaml",
            "          resources:\n",
            "          startupProbe: {tcpSocket: {port: 8088}}\n          resources:\n",
        )
        result = self.run_guard()
        self.assertIn("tcpSocket:", result.stderr)

    def test_wrong_readiness_path_is_rejected(self) -> None:
        self.mutate_text(
            "distribution/kubernetes/base/manifest.yaml",
            "path: /readyz",
            "path: /healthz",
        )
        result = self.run_guard()
        self.assertIn("readiness probe", result.stderr)


if __name__ == "__main__":
    unittest.main()
