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
import importlib.util
import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def load_guard():
    path = ROOT / "scripts" / "container_image_guard.py"
    spec = importlib.util.spec_from_file_location("container_image_guard", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load container_image_guard.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


GUARD = load_guard()


class ContainerFoundationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.policy = GUARD.load_policy()
        cls.dockerfile = (ROOT / cls.policy["foundation_dockerfile"]).read_text(encoding="utf-8")
        cls.workflow = (ROOT / cls.policy["workflow"]["path"]).read_text(encoding="utf-8")
        cls.supply_script = (ROOT / "scripts" / "container-supply-chain.ps1").read_text(encoding="utf-8")
        cls.dockerfiles = GUARD.repository_dockerfiles()

    def audit(
        self,
        *,
        policy=None,
        dockerfile=None,
        workflow=None,
        supply_script=None,
        dockerfiles=None,
    ):
        return GUARD.audit_foundation(
            policy or self.policy,
            dockerfile if dockerfile is not None else self.dockerfile,
            workflow if workflow is not None else self.workflow,
            supply_script if supply_script is not None else self.supply_script,
            dockerfiles if dockerfiles is not None else self.dockerfiles,
        )

    def test_repository_foundation_contract_passes(self) -> None:
        self.assertEqual([], self.audit())
        result = subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "container_image_guard.py")],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("CONTAINER_IMAGE_GUARD_OK", result.stdout)

    def test_mutable_base_or_root_identity_is_rejected(self) -> None:
        mutable = self.dockerfile.replace(
            self.policy["base_images"]["runtime"]["reference"],
            "docker.io/library/debian:latest",
        )
        findings = self.audit(dockerfile=mutable)
        self.assertTrue(any("ARG RUNTIME_IMAGE=" in finding for finding in findings))
        self.assertTrue(any("mutable latest" in finding for finding in findings))

        root = self.dockerfile.replace("USER 10001:10001", "USER 0:0")
        self.assertTrue(any("USER 10001:10001" in finding for finding in self.audit(dockerfile=root)))

    def test_unpinned_action_or_weakened_scanner_is_rejected(self) -> None:
        checkout_sha = self.policy["workflow"]["actions"]["actions/checkout"]
        workflow = self.workflow.replace(
            f"actions/checkout@{checkout_sha}",
            "actions/checkout@v7",
        )
        self.assertTrue(any("actions/checkout" in finding for finding in self.audit(workflow=workflow)))

        weakened = self.supply_script.replace("--severity", "--ignore-unfixed --severity", 1)
        self.assertTrue(any("must not ignore unfixed" in finding for finding in self.audit(supply_script=weakened)))

    def test_unregistered_dockerfile_or_extended_exception_is_rejected(self) -> None:
        dockerfiles = set(self.dockerfiles)
        dockerfiles.add("docker/Dockerfile.unreviewed")
        self.assertTrue(any("unregistered Dockerfile" in finding for finding in self.audit(dockerfiles=dockerfiles)))

        policy = copy.deepcopy(self.policy)
        policy["compatibility_exceptions"][0]["expires_at"] = "M11-12"
        self.assertTrue(any("must expire at M11-08" in finding for finding in self.audit(policy=policy)))

    def test_missing_read_only_or_signature_verification_is_rejected(self) -> None:
        no_read_only = self.supply_script.replace("--read-only", "--read-write", 1)
        self.assertTrue(any("--read-only" in finding for finding in self.audit(supply_script=no_read_only)))

        no_verify = self.supply_script.replace("verify-blob", "verify-disabled", 1)
        self.assertTrue(any("verify-blob" in finding for finding in self.audit(supply_script=no_verify)))

    def test_weakened_policy_or_package_snapshot_is_rejected(self) -> None:
        policy = copy.deepcopy(self.policy)
        policy["supply_chain"]["critical_vulnerability_policy"]["maximum_findings"] = 1
        self.assertTrue(any("vulnerability policy" in finding for finding in self.audit(policy=policy)))

        policy = copy.deepcopy(self.policy)
        policy["supply_chain"]["signature"]["published_digest_pattern"] = ".*"
        self.assertTrue(any("GHCR service digest" in finding for finding in self.audit(policy=policy)))

        missing_package = self.dockerfile.replace("    libssl3", "    removed-libssl3", 1)
        self.assertTrue(any("libssl3" in finding for finding in self.audit(dockerfile=missing_package)))


if __name__ == "__main__":
    unittest.main()
