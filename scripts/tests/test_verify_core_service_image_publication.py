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

import tempfile
import unittest
from pathlib import Path

from scripts.tests.release_test_support import ROOT, load_module


class CoreServiceImagePublicationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.verifier = load_module(
            "verify_core_service_image_publication", "scripts/verify_core_service_image_publication.py"
        )

    def test_repository_core_workflow_is_manual_build_only_by_default(self) -> None:
        findings = self.verifier.verify(
            ROOT / ".github/workflows/core-service-image-publish.yml",
            ROOT / "docker/core-container-policy.json",
        )
        self.assertEqual([], findings)

    def test_automatic_trigger_or_excluded_service_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workflow = root / "workflow.yml"
            workflow.write_text(
                (ROOT / ".github/workflows/core-service-image-publish.yml")
                .read_text(encoding="utf-8")
                .replace("workflow_dispatch:", "push:\n  workflow_dispatch:"),
                encoding="utf-8",
            )
            policy = root / "policy.json"
            policy.write_text(
                (ROOT / "docker/core-container-policy.json")
                .read_text(encoding="utf-8")
                .replace('"rocketmq-proxy"', '"rocketmq-proxy", "rocketmq-mcp"'),
                encoding="utf-8",
            )
            codes = {finding.code for finding in self.verifier.verify(workflow, policy)}
            self.assertIn("automatic-trigger", codes)
            self.assertIn("service-scope", codes)

    def test_publish_default_and_dry_run_secret_route_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workflow = root / "workflow.yml"
            workflow.write_text(
                (ROOT / ".github/workflows/core-service-image-publish.yml")
                .read_text(encoding="utf-8")
                .replace("default: false", "default: true")
                .replace(
                    'run: echo "Local candidate only; remote push is not executed"',
                    "run: echo ${{ secrets.CARGO_REGISTRY_TOKEN }}",
                ),
                encoding="utf-8",
            )
            codes = {
                finding.code
                for finding in self.verifier.verify(workflow, ROOT / "docker/core-container-policy.json")
            }
            self.assertIn("publish-default", codes)
            self.assertIn("dry-run-secret-route", codes)

    def test_unlisted_automatic_trigger_and_misplaced_condition_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            workflow = root / "workflow.yml"
            workflow.write_text(
                (ROOT / ".github/workflows/core-service-image-publish.yml")
                .read_text(encoding="utf-8")
                .replace("workflow_dispatch:", "pull_request_target:\n  workflow_dispatch:")
                .replace(
                    "if: github.event_name == 'workflow_dispatch' && inputs.publish == true",
                    "if: false # github.event_name == 'workflow_dispatch' && inputs.publish == true",
                ),
                encoding="utf-8",
            )

            codes = {
                finding.code
                for finding in self.verifier.verify(
                    workflow, ROOT / "docker/core-container-policy.json"
                )
            }
            self.assertIn("automatic-trigger", codes)
            self.assertIn("publish-condition", codes)


if __name__ == "__main__":
    unittest.main()
