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

import importlib.util
import json
import sys
import unittest
from dataclasses import replace
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "migrate.py"
SPEC = importlib.util.spec_from_file_location("request_header_migrate", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
migrate = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = migrate
SPEC.loader.exec_module(migrate)

COMPARE_SCRIPT = SCRIPT.parent / "compare_header_schema.py"
COMPARE_SPEC = importlib.util.spec_from_file_location("request_header_compare", COMPARE_SCRIPT)
assert COMPARE_SPEC is not None and COMPARE_SPEC.loader is not None
compare = importlib.util.module_from_spec(COMPARE_SPEC)
sys.modules[COMPARE_SPEC.name] = compare
COMPARE_SPEC.loader.exec_module(compare)


class MigrationGuardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.repo_root = SCRIPT.parents[2]
        cls.mapping = migrate.load_json(SCRIPT.parent / "header-class-map.json")
        cls.manifest = migrate.load_json(SCRIPT.parent / "migration.json")
        cls.headers, cls.legacy_derives, cls.fast_impls = migrate.scan_headers(cls.repo_root)

    def test_checked_in_inventory_is_complete_and_deterministic(self) -> None:
        expected = migrate.build_manifest(
            self.repo_root,
            self.mapping,
            self.headers,
            self.fast_impls,
            self.manifest,
        )
        self.assertEqual(migrate.serialize(expected), migrate.serialize(self.manifest))
        self.assertEqual(len(self.headers), 152)
        self.assertEqual(sum(header.codec == "v3" for header in self.headers.values()), 20)

    def test_duplicate_simple_names_keep_distinct_stable_paths(self) -> None:
        graph, depths = migrate.flatten_graph(self.headers, self.repo_root)
        rpc_topic = "rocketmq_protocol::rpc::topic_request_header::TopicRequestHeader"
        namesrv_topic = (
            "rocketmq_protocol::protocol::header::namesrv::topic_operation_header::TopicRequestHeader"
        )
        self.assertIn(rpc_topic, graph)
        self.assertIn(namesrv_topic, graph)
        self.assertEqual(graph[rpc_topic], ["rocketmq_protocol::rpc::rpc_request_header::RpcRequestHeader"])
        self.assertEqual(graph[namesrv_topic], ["rocketmq_protocol::rpc::rpc_request_header::RpcRequestHeader"])
        self.assertEqual(depths[rpc_topic], 1)
        self.assertEqual(depths[namesrv_topic], 1)

        schema_inventory = compare.build_inventory(self.mapping, self.repo_root)
        broker = (
            "rocketmq_protocol::protocol::header::broker::broker_heartbeat_request_header::"
            "BrokerHeartbeatRequestHeader"
        )
        namesrv_broker = (
            "rocketmq_protocol::protocol::header::namesrv::broker_request::BrokerHeartbeatRequestHeader"
        )
        self.assertTrue(all(field.declared_in == broker for field in schema_inventory[broker]))
        self.assertTrue(all(field.declared_in == namesrv_broker for field in schema_inventory[namesrv_broker]))

    def test_new_v2_and_v3_regression_are_rejected(self) -> None:
        type_id = "rocketmq_protocol::rpc::rpc_request_header::RpcRequestHeader"
        regressed = dict(self.headers)
        regressed[type_id] = replace(regressed[type_id], codec="v2")
        errors = migrate.validate_inventory(
            self.mapping,
            self.manifest,
            regressed,
            self.legacy_derives,
            self.fast_impls,
        )
        self.assertTrue(any("cannot return to V2" in error for error in errors))

        synthetic_id = "rocketmq_protocol::protocol::header::synthetic::SyntheticHeader"
        synthetic = replace(
            next(iter(self.headers.values())),
            type_id=synthetic_id,
            name="SyntheticHeader",
            source="rocketmq-protocol/src/protocol/header/synthetic.rs",
            codec="v2",
        )
        with_new_v2 = {**self.headers, synthetic_id: synthetic}
        mapping = json.loads(json.dumps(self.mapping))
        mapping["entries"].append(
            {
                "rustTypeId": synthetic_id,
                "rustType": "SyntheticHeader",
                "rustSource": synthetic.source,
                "javaClass": None,
            }
        )
        errors = migrate.validate_inventory(
            mapping,
            self.manifest,
            with_new_v2,
            self.legacy_derives,
            self.fast_impls,
        )
        self.assertTrue(any("new V2 derives are forbidden" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
