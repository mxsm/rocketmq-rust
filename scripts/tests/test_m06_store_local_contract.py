# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

import unittest

from scripts.tests.architecture_contract_helpers import ROOT
from scripts.tests.architecture_contract_helpers import normal_dependencies


class StoreLocalContractTests(unittest.TestCase):
    def test_local_store_has_one_canonical_module_tree(self) -> None:
        for relative in (
            "rocketmq-store-local/src/commit_log.rs",
            "rocketmq-store-local/src/consume_queue/mod.rs",
            "rocketmq-store-local/src/index/mod.rs",
            "rocketmq-store-local/src/mapped_file.rs",
            "rocketmq-store-local/src/ha.rs",
            "rocketmq-store-local/src/timer.rs",
            "rocketmq-store-local/src/pop.rs",
        ):
            self.assertTrue((ROOT / relative).is_file(), relative)

    def test_local_store_does_not_depend_on_aggregate_store_or_broker(self) -> None:
        dependencies = normal_dependencies("rocketmq-store-local/Cargo.toml")

        self.assertIn("rocketmq-store-api", dependencies)
        self.assertTrue({"rocketmq-store", "rocketmq-broker"}.isdisjoint(dependencies))

    def test_persisted_layout_and_lease_semantics_have_real_tests(self) -> None:
        for relative in (
            "rocketmq-store-local/tests/storage_layout_golden.rs",
            "rocketmq-store-local/tests/consume_queue_record.rs",
            "rocketmq-store-local/tests/index_codec.rs",
            "rocketmq-store-local/src/mapped_file/write_lease_scenarios.rs",
            "rocketmq-store-local/tests/mapped_write_lease_loom.rs",
            "rocketmq-store-local/src/mapped_file/write_lease_miri_tests.rs",
            "rocketmq-store-local/tests/commit_log_recovery_orchestration.rs",
        ):
            self.assertTrue((ROOT / relative).is_file(), relative)

    def test_storage_contracts_do_not_restore_deleted_source_facades(self) -> None:
        for relative in (
            "rocketmq-store/tests/m06_store_local_compatibility.rs",
            "rocketmq-store/tests/m06_store_local_record_compatibility.rs",
            "rocketmq-store/tests/m06_store_local_commitlog_compatibility.rs",
        ):
            self.assertFalse((ROOT / relative).exists(), relative)


if __name__ == "__main__":
    unittest.main()
