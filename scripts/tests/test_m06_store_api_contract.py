# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

import unittest

from scripts.tests.architecture_contract_helpers import ROOT
from scripts.tests.architecture_contract_helpers import normal_dependencies
from scripts.tests.architecture_contract_helpers import source


class StoreApiContractTests(unittest.TestCase):
    def test_capability_surface_is_explicit(self) -> None:
        exports = source("rocketmq-store-api/src/lib.rs")
        for capability in (
            "AdminStore",
            "MessageAppender",
            "MessageReader",
            "OffsetIndex",
            "ReplicationControl",
            "StoreHealth",
            "StoreLifecycle",
        ):
            self.assertIn(f"pub use capability::{capability}", exports)

    def test_store_api_remains_runtime_and_backend_neutral(self) -> None:
        dependencies = normal_dependencies("rocketmq-store-api/Cargo.toml")

        self.assertTrue(
            {
                "rocketmq-runtime",
                "tokio",
                "rocketmq-store",
                "rocketmq-store-local",
                "rocketmq-store-rocksdb",
                "rocketmq-tieredstore",
                "rocketmq-broker",
            }.isdisjoint(dependencies)
        )

    def test_read_and_lease_results_are_owned_by_store_api(self) -> None:
        api = source("rocketmq-store-api/src/lib.rs")

        self.assertIn("pub struct LeasedBytes", api)
        self.assertIn("pub struct SelectResult", api)
        self.assertIn("pub struct ReadOutcome", api)
        self.assertTrue((ROOT / "rocketmq-store/tests/public_api_contract.rs").is_file())


if __name__ == "__main__":
    unittest.main()
