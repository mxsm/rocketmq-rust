# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

import unittest

from scripts.tests.architecture_contract_helpers import ROOT
from scripts.tests.architecture_contract_helpers import normal_dependencies
from scripts.tests.architecture_contract_helpers import source


class M06HALocalContractTests(unittest.TestCase):
    def test_local_package_owns_ha_algorithms_without_store_reverse_edge(self) -> None:
        dependencies = normal_dependencies("rocketmq-store-local/Cargo.toml")

        self.assertTrue((ROOT / "rocketmq-store-local/src/ha/replication.rs").is_file())
        self.assertTrue({"rocketmq-store", "rocketmq-broker"}.isdisjoint(dependencies))

    def test_store_composition_calls_the_local_ha_boundary(self) -> None:
        group_transfer = source("rocketmq-store/src/ha/group_transfer_service.rs")

        self.assertIn("rocketmq_store_local::ha::replication", group_transfer)
        self.assertTrue((ROOT / "rocketmq-store/tests/ha_semantics_tests.rs").is_file())
        self.assertTrue((ROOT / "rocketmq-store-local/tests/ha_transfer_boundary.rs").is_file())


if __name__ == "__main__":
    unittest.main()
