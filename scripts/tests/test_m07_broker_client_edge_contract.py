# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

import unittest

from scripts.tests.architecture_contract_helpers import normal_dependencies
from scripts.tests.architecture_contract_helpers import source


class BrokerClientEdgeContractTest(unittest.TestCase):
    def test_broker_does_not_depend_on_the_client_package(self) -> None:
        dependencies = normal_dependencies("rocketmq-broker/Cargo.toml")

        self.assertNotIn("rocketmq-client-rust", dependencies)
        self.assertIn("rocketmq-store-api", dependencies)
        self.assertIn("rocketmq-transport", dependencies)

    def test_broker_read_and_append_paths_use_current_capabilities(self) -> None:
        transaction = source("rocketmq-broker/src/transaction/queue/transactional_message_bridge.rs")
        send = source("rocketmq-broker/src/processor/send_message_processor.rs")

        self.assertIn("rocketmq_store_api::ReadOutcome", transaction)
        self.assertIn("rocketmq_store_api::MessageAppender", send)
        self.assertIn("rocketmq_store_api::StoreHealth", send)


if __name__ == "__main__":
    unittest.main()
