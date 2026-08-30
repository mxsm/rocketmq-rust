#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Regression tests for the reproducible Phase 00 Compose workflow."""

from __future__ import annotations

import json
import re
import unittest
from pathlib import Path


SRE_ROOT = Path(__file__).resolve().parents[2]


class Phase00ComposeContractTest(unittest.TestCase):
    def test_rocketmq_images_receive_the_current_source_revision(self) -> None:
        compose = (SRE_ROOT / "deploy" / "dev" / "compose.yaml").read_text(encoding="utf-8")
        dev_script = (SRE_ROOT / "scripts" / "dev.ps1").read_text(encoding="utf-8")

        self.assertIn("SOURCE_REVISION: ${SOURCE_REVISION:-0000000000000000000000000000000000000000}", compose)
        self.assertEqual(compose.count("args: *rocketmq-build-args"), 5)
        self.assertIn("git -C $repositoryRoot rev-parse HEAD", dev_script)
        self.assertIn("Remove-Item Env:SOURCE_REVISION", dev_script)

    def test_control_plane_uses_a_valid_development_grant_key(self) -> None:
        compose = (SRE_ROOT / "deploy" / "dev" / "compose.yaml").read_text(encoding="utf-8")
        match = re.search(r"ROCKETMQ_SRE_GRANT_SIGNING_KEY:\s*([^\s]+)", compose)

        self.assertIsNotNone(match)
        self.assertGreaterEqual(len(match.group(1)), 32)

    def test_ui_runtime_and_toolchain_peer_versions_are_aligned(self) -> None:
        package = json.loads((SRE_ROOT / "ui" / "package.json").read_text(encoding="utf-8"))
        lock = json.loads((SRE_ROOT / "ui" / "package-lock.json").read_text(encoding="utf-8"))

        self.assertEqual(package["dependencies"]["react"], "^18.3.1")
        self.assertEqual(package["dependencies"]["react-dom"], "^18.3.1")
        self.assertEqual(package["devDependencies"]["typescript"], "^5.9.3")
        self.assertEqual(lock["packages"]["node_modules/react"]["version"], "18.3.1")
        self.assertEqual(lock["packages"]["node_modules/react-dom"]["version"], "18.3.1")

    def test_smoke_uses_the_reverse_connector_conversation_path(self) -> None:
        smoke = (SRE_ROOT / "scripts" / "phase00-smoke.ps1").read_text(encoding="utf-8")

        self.assertIn("/v1/conversations", smoke)
        self.assertIn("/v1/evidence/", smoke)
        self.assertIn("'restart', 'sre-control-plane-mtls'", smoke)
        self.assertIn("rocketmq_mcp_requests_total", smoke)
        self.assertIn("'ROCKETMQ_SRE_PROBE_MAX_MESSAGES=1'", smoke)
        self.assertIn("function Drain-ProbeLag([int]$MaxBatches = 12)", smoke)
        self.assertIn("function Wait-ConnectorNotReady([int]$Seconds = 45)", smoke)
        self.assertIn("-Headers (Get-PublicApiHeaders)", smoke)
        self.assertNotIn("rocketmq_rocketmq_mcp_requests_total", smoke)
        self.assertNotIn("127.0.0.1:8091/internal/v1/evidence/query", smoke)
        self.assertNotIn("127.0.0.1:8091/internal/v1/capabilities", smoke)

    def test_smoke_covers_identity_fail_closed_boundaries(self) -> None:
        smoke = (SRE_ROOT / "scripts" / "phase00-smoke.ps1").read_text(encoding="utf-8")
        issuer = (
            SRE_ROOT
            / "crates"
            / "rocketmq-sre-eval"
            / "src"
            / "bin"
            / "phase00_dev_issuer.rs"
        ).read_text(encoding="utf-8")

        self.assertIn('/admin/fixture-token', issuer)
        for profile in ("wrong_audience", "missing_read_scope", "different_cluster"):
            self.assertIn(profile, smoke)
        self.assertIn("error=\"invalid_token\"", smoke)
        self.assertIn("error=\"insufficient_scope\"", smoke)
        self.assertIn("cluster_not_allowed", smoke)
        self.assertIn("leaked an access token", smoke)


if __name__ == "__main__":
    unittest.main()
