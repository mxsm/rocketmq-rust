# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

import unittest

from scripts.tests.architecture_contract_helpers import load_json
from scripts.tests.architecture_contract_helpers import load_toml


class StoreFacadeTieredContractTests(unittest.TestCase):
    def test_backend_features_select_explicit_packages(self) -> None:
        features = load_toml("rocketmq-store/Cargo.toml")["features"]

        self.assertEqual(["dep:rocketmq-store-rocksdb"], features["rocksdb_store"])
        self.assertEqual(["dep:rocketmq-tieredstore"], features["tieredstore"])
        self.assertIn("local_file_store", features["default"])

    def test_remaining_store_facade_edges_are_governed_not_source_frozen(self) -> None:
        registry = load_json("scripts/architecture-debt-registry.json")
        entries = {
            entry["id"]: entry
            for entry in registry["entries"]
            if entry["class"] in {"compatibility", "facade"} and entry["status"] == "active"
        }

        self.assertIn("ARC-COMP-001", entries)
        self.assertIn("ARC-COMP-002", entries)
        self.assertIn("ARC-FACADE-001", entries)
        self.assertEqual({"2.0.0"}, {entry["target_release"] for entry in entries.values()})


if __name__ == "__main__":
    unittest.main()
