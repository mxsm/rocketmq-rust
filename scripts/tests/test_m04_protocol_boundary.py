# Copyright 2023 The RocketMQ Rust Authors
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
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class ProtocolBoundaryTests(unittest.TestCase):
    def test_workspace_exposes_runtime_neutral_protocol_crate(self) -> None:
        result = subprocess.run(
            ["cargo", "metadata", "--format-version", "1", "--no-deps"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        packages = {package["name"]: package for package in json.loads(result.stdout)["packages"]}
        self.assertIn("rocketmq-protocol", packages)

        policy = json.loads((ROOT / "scripts" / "architecture-dependency-policy.json").read_text(encoding="utf-8"))
        self.assertEqual(policy["package_counts"]["target"], len(packages))
        self.assertIn("rocketmq-protocol", policy["planned_packages"])

        protocol = packages["rocketmq-protocol"]
        self.assertEqual([], protocol["features"]["default"])
        self.assertEqual(["dep:simd-json"], protocol["features"]["simd"])

        normal_dependencies = {
            dependency["name"]
            for dependency in protocol["dependencies"]
            if dependency["kind"] is None
        }
        forbidden = {
            "rocketmq-common",
            "rocketmq-remoting",
            "rocketmq-runtime",
            "rocketmq-rust",
            "tokio",
            "tokio-rustls",
        }
        self.assertTrue(forbidden.isdisjoint(normal_dependencies))


if __name__ == "__main__":
    unittest.main()
