# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

import unittest

from scripts.tests.architecture_contract_helpers import ROOT
from scripts.tests.architecture_contract_helpers import normal_dependencies
from scripts.tests.architecture_contract_helpers import source


class SchemaOwnershipTests(unittest.TestCase):
    def test_removed_schema_owner_packages_stay_deleted(self) -> None:
        for package in ("rocketmq-common", "rocketmq-remoting", "rocketmq"):
            self.assertFalse((ROOT / package).exists(), package)

    def test_protocol_owns_request_codes_commands_and_message_codec(self) -> None:
        self.assertIn("pub enum RequestCode", source("rocketmq-protocol/src/code/request_code.rs"))
        self.assertIn("pub struct RemotingCommand", source("rocketmq-protocol/src/protocol/remoting_command.rs"))
        self.assertIn("MESSAGE_MAGIC_CODE", source("rocketmq-protocol/src/protocol/body/message_codec.rs"))

    def test_protocol_schema_direction_has_no_high_level_owner(self) -> None:
        dependencies = normal_dependencies("rocketmq-protocol/Cargo.toml")
        self.assertTrue(
            {
                "rocketmq-client-rust",
                "rocketmq-broker",
                "rocketmq-namesrv",
                "rocketmq-controller",
                "rocketmq-store",
                "rocketmq-proxy",
            }.isdisjoint(dependencies)
        )


if __name__ == "__main__":
    unittest.main()
