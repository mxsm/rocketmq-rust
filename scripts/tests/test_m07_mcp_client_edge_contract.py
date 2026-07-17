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
import tomllib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MCP = ROOT / "rocketmq-tools" / "rocketmq-mcp"
FORBIDDEN_DIRECT_PACKAGES = {
    "rocketmq-auth",
    "rocketmq-client-rust",
    "rocketmq-common",
    "rocketmq-error",
    "rocketmq-remoting",
}
FORBIDDEN_SOURCE_CRATES = {
    "rocketmq_client",
    "rocketmq_client_rust",
    "rocketmq_common",
    "rocketmq_remoting",
}


def load_toml(path: Path) -> dict:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def mcp_sources() -> str:
    paths = sorted((MCP / "src").rglob("*.rs")) + sorted((MCP / "tests").rglob("*.rs"))
    return "\n".join(path.read_text(encoding="utf-8") for path in paths)


class McpClientEdgeContractTest(unittest.TestCase):
    def test_manifest_routes_client_access_only_through_admin_core(self) -> None:
        manifest = load_toml(MCP / "Cargo.toml")
        dependencies = set(manifest["dependencies"])

        self.assertIn("rocketmq-admin-core", dependencies)
        self.assertEqual(set(), dependencies & FORBIDDEN_DIRECT_PACKAGES)

    def test_lockfile_has_no_redundant_mcp_direct_edges(self) -> None:
        lockfile = load_toml(ROOT / "Cargo.lock")
        package = next(item for item in lockfile["package"] if item["name"] == "rocketmq-mcp")
        dependencies = {
            dependency.split(" ", 1)[0]
            for dependency in package["dependencies"]
        }

        self.assertIn("rocketmq-admin-core", dependencies)
        self.assertEqual(set(), dependencies & FORBIDDEN_DIRECT_PACKAGES)

    def test_source_uses_admin_facade_without_client_or_facade_bypass(self) -> None:
        source = mcp_sources()
        admin_session = (MCP / "src" / "adapter" / "admin_session.rs").read_text(encoding="utf-8")

        self.assertIn("rocketmq_admin_core::", admin_session)
        for crate_name in FORBIDDEN_SOURCE_CRATES:
            self.assertNotIn(f"{crate_name}::", source)

    def test_architecture_baseline_cannot_restore_the_mcp_client_edge(self) -> None:
        policy = json.loads(
            (ROOT / "scripts" / "architecture-dependency-policy.json").read_text(encoding="utf-8")
        )
        baseline = json.loads(
            (ROOT / "scripts" / "architecture-dependency-baseline.json").read_text(encoding="utf-8")
        )
        client_policy = policy["client_policy"]

        self.assertNotIn(
            "rocketmq-mcp",
            {entry["caller"] for entry in client_policy["target_manifest_allowlist"]},
        )
        self.assertNotIn(
            "rocketmq-mcp",
            {entry["caller"] for entry in client_policy["target_source_allowlist"]},
        )
        self.assertNotIn("rocketmq-client-rust", policy["target_dag"]["rocketmq-mcp"])
        self.assertFalse(
            any(
                item.get("caller") == "rocketmq-mcp"
                and item.get("target") in FORBIDDEN_DIRECT_PACKAGES
                for item in baseline["manifest_exceptions"]
                + baseline["compatibility_manifest_exceptions"]
            )
        )


if __name__ == "__main__":
    unittest.main()
