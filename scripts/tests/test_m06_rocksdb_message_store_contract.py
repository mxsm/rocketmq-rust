# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

import unittest

from scripts.tests.architecture_contract_helpers import ROOT
from scripts.tests.architecture_contract_helpers import load_toml
from scripts.tests.architecture_contract_helpers import normal_dependencies


class RocksDbMessageStoreContractTests(unittest.TestCase):
    def test_rocksdb_is_an_explicit_optional_backend(self) -> None:
        manifest = load_toml("rocketmq-store/Cargo.toml")

        self.assertEqual(["dep:rocketmq-store-rocksdb"], manifest["features"]["rocksdb_store"])
        self.assertIn("rocketmq-store-rocksdb", normal_dependencies("rocketmq-store/Cargo.toml"))
        self.assertIn("local_file_store", manifest["features"]["default"])

    def test_rocksdb_semantics_and_capabilities_have_executable_contracts(self) -> None:
        for relative in (
            "rocketmq-store/tests/capability_conformance_tests.rs",
            "rocketmq-store/tests/rocksdb_foundation_tests.rs",
            "rocketmq-store/tests/rocksdb_store_semantics_tests.rs",
        ):
            self.assertTrue((ROOT / relative).is_file(), relative)


if __name__ == "__main__":
    unittest.main()
