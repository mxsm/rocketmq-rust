# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

import unittest

from scripts.tests.architecture_contract_helpers import ROOT
from scripts.tests.architecture_contract_helpers import normal_dependencies
from scripts.tests.architecture_contract_helpers import run_dependency_guard
from scripts.tests.architecture_contract_helpers import source


class StorageCloseoutContractTests(unittest.TestCase):
    def test_dependency_target_keeps_backend_direction_closed(self) -> None:
        result = run_dependency_guard("target")

        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("TARGET_COMPATIBILITY_LEDGER active_edges=2 entries=2", result.stdout)

    def test_store_api_is_used_by_real_broker_paths(self) -> None:
        broker_dependencies = normal_dependencies("rocketmq-broker/Cargo.toml")
        send_processor = source("rocketmq-broker/src/processor/send_message_processor.rs")
        query_processor = source("rocketmq-broker/src/processor/query_message_processor.rs")

        self.assertIn("rocketmq-store-api", broker_dependencies)
        self.assertIn("rocketmq_store_api::MessageAppender", send_processor)
        self.assertIn("rocketmq_store_api::StoreError", query_processor)

    def test_capability_conformance_is_not_a_source_compatibility_facade(self) -> None:
        self.assertTrue((ROOT / "rocketmq-store/tests/capability_conformance_tests.rs").is_file())
        for name in (
            "m06_store_local_compatibility.rs",
            "m06_store_local_record_compatibility.rs",
            "m06_store_local_commitlog_compatibility.rs",
        ):
            self.assertFalse((ROOT / "rocketmq-store/tests" / name).exists())


if __name__ == "__main__":
    unittest.main()
