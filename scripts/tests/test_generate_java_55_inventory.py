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
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "scripts" / "fixtures" / "java-5.5-core-inventory.json"


def load_generator():
    path = ROOT / "scripts" / "generate_java_55_inventory.py"
    spec = importlib.util.spec_from_file_location("generate_java_55_inventory", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class Java55InventoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.assertTrue(FIXTURE.is_file(), "Java 5.5 inventory fixture is missing")
        self.inventory = json.loads(FIXTURE.read_text(encoding="utf-8"))

    def test_inventory_has_expected_raw_denominators(self) -> None:
        self.assertEqual("5.5.0", self.inventory["java_version"])
        self.assertEqual(171, len(self.inventory["request_codes"]))
        self.assertEqual(68, len(self.inventory["response_codes"]))
        self.assertEqual(145, len(self.inventory["headers"]))
        self.assertEqual(64, len(self.inventory["bodies"]))
        self.assertEqual(24, len(self.inventory["proxy_routes"]))
        self.assertEqual(96, len(self.inventory["admin_operations"]))

    def test_controller_internal_codes_and_payloads_are_not_active(self) -> None:
        internal = {
            item["value"]: item for item in self.inventory["request_codes"] if 1014 <= item["value"] <= 1018
        }
        self.assertEqual(set(range(1014, 1019)), set(internal))
        self.assertTrue(all(item["classification"] == "controller-internal-not-applicable" for item in internal.values()))
        self.assertEqual(5, len(self.inventory["controller_internal_payloads"]))
        self.assertTrue(
            all(item["classification"] == "controller-internal-not-applicable" for item in self.inventory["controller_internal_payloads"])
        )

    def test_required_active_protocol_and_proxy_items_are_explicit(self) -> None:
        request_codes = {item["symbol"]: item for item in self.inventory["request_codes"]}
        for symbol in (
            "DELETE_TOPIC_IN_BROKER_LIST",
            "DELETE_SUBSCRIPTION_GROUP_LIST",
            "UPDATE_AND_CREATE_SUBSCRIPTIONGROUP",
        ):
            self.assertTrue(request_codes[symbol]["required_active"])

        query_header = next(item for item in self.inventory["headers"] if item["symbol"] == "QueryMessageRequestHeader")
        self.assertTrue({"indexType", "lastKey"}.issubset(query_header["fields"]))

        required_gaps = {item["request_code"] for item in self.inventory["proxy_routes"] if item["required_gap"]}
        self.assertEqual(
            {
                "CONSUMER_SEND_MSG_BACK",
                "END_TRANSACTION",
                "RECALL_MESSAGE",
                "POP_MESSAGE",
                "ACK_MESSAGE",
                "CHANGE_MESSAGE_INVISIBLETIME",
                "GET_CONSUMER_CONNECTION_LIST",
            },
            required_gaps,
        )
        self.assertTrue(all(item["classification"] == "active" for item in self.inventory["proxy_routes"]))

    def test_broker_container_admin_operations_are_raw_but_excluded(self) -> None:
        excluded = [item for item in self.inventory["admin_operations"] if item["classification"] != "active"]
        self.assertEqual(
            {"AddBrokerSubCommand", "RemoveBrokerSubCommand"},
            {item["symbol"] for item in excluded},
        )
        self.assertTrue(all(item["classification"] == "excluded-broker-container" for item in excluded))
        self.assertEqual(94, sum(item["classification"] == "active" for item in self.inventory["admin_operations"]))

    def test_fixture_has_only_portable_semantic_source_data(self) -> None:
        forbidden = {"sha", "hash", "digest", "commit", "revision"}

        def walk(value: object) -> None:
            if isinstance(value, dict):
                self.assertTrue(forbidden.isdisjoint(key.lower() for key in value))
                for child in value.values():
                    walk(child)
            elif isinstance(value, list):
                for child in value:
                    walk(child)
            elif isinstance(value, str) and "/src/" in value:
                self.assertNotIn("\\", value)
                self.assertFalse(Path(value).is_absolute())

        walk(self.inventory)

    def test_generator_validation_accepts_the_checked_fixture(self) -> None:
        generator = load_generator()
        self.assertEqual([], generator.validate_inventory(self.inventory))


if __name__ == "__main__":
    unittest.main()
