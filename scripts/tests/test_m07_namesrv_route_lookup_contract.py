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
NAMESRV = ROOT / "rocketmq-namesrv"
ROUTE_LOOKUP = (
    NAMESRV
    / "src"
    / "processor"
    / "cluster_test_request_processor"
    / "route_lookup.rs"
)
FORBIDDEN_CLIENT_CRATES = {"rocketmq_client", "rocketmq_client_rust"}


def load_toml(path: Path) -> dict:
    return tomllib.loads(path.read_text(encoding="utf-8"))


class NamesrvRouteLookupContractTest(unittest.TestCase):
    def test_manifest_uses_protocol_and_transport_without_client(self) -> None:
        dependencies = set(load_toml(NAMESRV / "Cargo.toml")["dependencies"])

        self.assertIn("rocketmq-protocol", dependencies)
        self.assertIn("rocketmq-transport", dependencies)
        self.assertNotIn("rocketmq-client", dependencies)
        self.assertNotIn("rocketmq-client-rust", dependencies)

        lockfile = load_toml(ROOT / "Cargo.lock")
        package = next(item for item in lockfile["package"] if item["name"] == "rocketmq-namesrv")
        locked_dependencies = {item.split(" ", 1)[0] for item in package["dependencies"]}
        self.assertIn("rocketmq-protocol", locked_dependencies)
        self.assertIn("rocketmq-transport", locked_dependencies)
        self.assertNotIn("rocketmq-client-rust", locked_dependencies)

    def test_namesrv_source_has_no_client_boundary_bypass(self) -> None:
        source = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted((NAMESRV / "src").rglob("*.rs"))
        )
        for crate_name in FORBIDDEN_CLIENT_CRATES:
            self.assertNotIn(f"{crate_name}::", source)

        route_lookup = ROUTE_LOOKUP.read_text(encoding="utf-8")
        production = route_lookup.split("#[cfg(test)]", 1)[0]
        self.assertIn("rocketmq_protocol::", production)
        self.assertIn("rocketmq_transport::", production)
        for forbidden in (
            "rocketmq_remoting::",
            "DefaultMQAdminExt",
            "MQAdminExt",
            "MQClientManager",
            "ArcMut",
        ):
            self.assertNotIn(forbidden, production)

    def test_lookup_reuses_one_absolute_request_deadline(self) -> None:
        source = ROUTE_LOOKUP.read_text(encoding="utf-8").split("#[cfg(test)]", 1)[0]

        self.assertEqual(1, source.count("ShutdownDeadline::after(self.request_timeout)"))
        self.assertIn(".invoke(endpoint, route_request(topic), deadline)", source)
        self.assertIn("self.resolver.resolve(deadline)", source)
        self.assertNotIn("tokio::time::timeout(", source)

    def test_lookup_is_owned_by_service_context_and_shutdown_is_awaited(self) -> None:
        route_lookup = ROUTE_LOOKUP.read_text(encoding="utf-8").split("#[cfg(test)]", 1)[0]
        bootstrap = (NAMESRV / "src" / "bootstrap.rs").read_text(encoding="utf-8")

        self.assertIn("service_context.task_group().clone()", route_lookup)
        self.assertIn("service_context.child(\"transport\")", route_lookup)
        self.assertIn(".shutdown_until(ShutdownDeadline::after(ROUTE_LOOKUP_SHUTDOWN_TIMEOUT))", route_lookup)
        self.assertNotIn("RuntimeOwner", route_lookup)
        self.assertNotIn("tokio::spawn", route_lookup)
        self.assertIn('context.child("namesrv.cluster-test-route-lookup")', bootstrap)
        self.assertIn("cluster_test_route_lookup.shutdown().await", bootstrap)

    def test_architecture_baseline_cannot_restore_namesrv_client_edge(self) -> None:
        policy = json.loads(
            (ROOT / "scripts" / "architecture-dependency-policy.json").read_text(encoding="utf-8")
        )
        baseline = json.loads(
            (ROOT / "scripts" / "architecture-dependency-baseline.json").read_text(encoding="utf-8")
        )

        self.assertNotIn(
            "rocketmq-namesrv",
            {entry["caller"] for entry in policy["client_policy"]["target_manifest_allowlist"]},
        )
        self.assertIn("rocketmq-protocol", policy["target_dag"]["rocketmq-namesrv"])
        self.assertIn("rocketmq-transport", policy["target_dag"]["rocketmq-namesrv"])
        self.assertNotIn("rocketmq-client-rust", policy["target_dag"]["rocketmq-namesrv"])
        self.assertFalse(
            any(
                item.get("caller") == "rocketmq-namesrv"
                and item.get("target") == "rocketmq-client-rust"
                for item in baseline["manifest_exceptions"]
                + baseline["compatibility_manifest_exceptions"]
            )
        )
        self.assertFalse(
            any(
                item.get("owner") == "namesrv"
                and item.get("alias") in FORBIDDEN_CLIENT_CRATES
                for item in baseline["source_exceptions"]
            )
        )


if __name__ == "__main__":
    unittest.main()
