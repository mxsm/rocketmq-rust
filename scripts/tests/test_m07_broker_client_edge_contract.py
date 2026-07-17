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
BROKER = ROOT / "rocketmq-broker"
FORBIDDEN_CLIENT_CRATES = {"rocketmq_client", "rocketmq_client_rust"}


def load_toml(path: Path) -> dict:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


class BrokerClientEdgeContractTest(unittest.TestCase):
    def test_manifest_and_lockfile_use_owner_crates_without_client(self) -> None:
        dependencies = set(load_toml(BROKER / "Cargo.toml")["dependencies"])

        self.assertIn("rocketmq-model", dependencies)
        self.assertIn("rocketmq-store-api", dependencies)
        self.assertNotIn("rocketmq-client", dependencies)
        self.assertNotIn("rocketmq-client-rust", dependencies)

        lockfile = load_toml(ROOT / "Cargo.lock")
        package = next(item for item in lockfile["package"] if item["name"] == "rocketmq-broker")
        locked_dependencies = {item.split(" ", 1)[0] for item in package["dependencies"]}
        self.assertIn("rocketmq-model", locked_dependencies)
        self.assertIn("rocketmq-store-api", locked_dependencies)
        self.assertNotIn("rocketmq-client-rust", locked_dependencies)

    def test_broker_source_has_no_client_boundary_bypass(self) -> None:
        source = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted((BROKER / "src").rglob("*.rs"))
        )
        for crate_name in FORBIDDEN_CLIENT_CRATES:
            self.assertNotIn(f"{crate_name}::", source)
        self.assertNotIn("rocketmq_client_api::", source)

    def test_remote_results_are_model_owned_and_transport_state_is_broker_local(self) -> None:
        outer_api = read(BROKER / "src" / "out_api" / "broker_outer_api.rs")
        pull = read(BROKER / "src" / "out_api" / "pull.rs")
        result = read(BROKER / "src" / "out_api" / "result.rs")
        send = read(BROKER / "src" / "out_api" / "send.rs")

        self.assertIn("Option<PullOutcome<MessageExt>>", outer_api)
        self.assertIn("rocketmq_model::result::PullOutcome", pull)
        self.assertIn("rocketmq_model::result::SendResult", send)
        self.assertIn("struct BrokerPullResponse", result)
        for source in (outer_api, pull, result, send):
            self.assertNotIn("PullResultExt", source)
            self.assertNotIn("rocketmq_client", source)

    def test_local_transaction_and_pop_reads_use_store_api_outcome(self) -> None:
        pop = read(
            BROKER
            / "src"
            / "processor"
            / "processor_service"
            / "pop_revive_service.rs"
        )
        transaction_bridge = read(
            BROKER
            / "src"
            / "transaction"
            / "queue"
            / "transactional_message_bridge.rs"
        )
        adapter = read(BROKER / "src" / "store_read.rs")

        for source in (pop, transaction_bridge):
            self.assertIn("rocketmq_store_api::ReadOutcome", source)
            self.assertNotIn("GetMessageResult", source)
            self.assertNotIn("GetMessageStatus", source)
            self.assertNotIn("PullResult", source)
        self.assertIn("get_result_from_legacy", adapter)
        self.assertIn("ReadOutcome<MessageExt>", adapter)

    def test_route_and_assignment_are_projected_by_their_real_owners(self) -> None:
        route = read(BROKER / "src" / "topic" / "route.rs")
        manager = read(
            BROKER
            / "src"
            / "topic"
            / "manager"
            / "topic_route_info_manager.rs"
        )
        assignment = read(BROKER / "src" / "processor" / "query_assignment_processor.rs")

        self.assertIn("struct BrokerPublishRoute", route)
        self.assertIn("BrokerPublishRoute::from_topic_route_data", manager)
        self.assertIn("rocketmq_remoting::rpc::client_metadata::ClientMetadata", route)
        self.assertNotIn("TopicPublishInfo", manager)
        self.assertIn("rocketmq_model::allocation::AllocateMessageQueueStrategy", assignment)
        self.assertNotIn("rocketmq_client", assignment)

    def test_architecture_baseline_cannot_restore_broker_client_edge(self) -> None:
        policy = json.loads(
            read(ROOT / "scripts" / "architecture-dependency-policy.json")
        )
        baseline = json.loads(
            read(ROOT / "scripts" / "architecture-dependency-baseline.json")
        )

        self.assertNotIn(
            "rocketmq-broker",
            {entry["caller"] for entry in policy["client_policy"]["target_manifest_allowlist"]},
        )
        self.assertNotIn("rocketmq-client-rust", policy["target_dag"]["rocketmq-broker"])
        self.assertFalse(
            any(
                item.get("caller") == "rocketmq-broker"
                and item.get("target") == "rocketmq-client-rust"
                for item in baseline["manifest_exceptions"]
                + baseline["compatibility_manifest_exceptions"]
            )
        )
        self.assertFalse(
            any(
                item.get("owner") == "broker"
                and item.get("alias") in FORBIDDEN_CLIENT_CRATES
                for item in baseline["source_exceptions"]
            )
        )


if __name__ == "__main__":
    unittest.main()
