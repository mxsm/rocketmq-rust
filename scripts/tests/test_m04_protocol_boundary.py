# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

import unittest

from scripts.tests.architecture_contract_helpers import ROOT
from scripts.tests.architecture_contract_helpers import load_json
from scripts.tests.architecture_contract_helpers import normal_dependencies
from scripts.tests.architecture_contract_helpers import workspace_packages


class ProtocolBoundaryTests(unittest.TestCase):
    def test_workspace_exposes_the_current_protocol_crate(self) -> None:
        packages = workspace_packages()
        policy = load_json("scripts/architecture-dependency-policy.json")

        self.assertIn("rocketmq-protocol", packages)
        self.assertEqual(set(packages), set(policy["target_dag"]))
        self.assertNotIn("rocketmq-protocol", policy["planned_packages"])

    def test_protocol_is_runtime_and_transport_neutral(self) -> None:
        dependencies = normal_dependencies("rocketmq-protocol/Cargo.toml")

        self.assertTrue({"rocketmq-model", "rocketmq-error", "rocketmq-macros"} <= dependencies)
        self.assertTrue(
            {
                "rocketmq-transport",
                "rocketmq-runtime",
                "rocketmq-common",
                "rocketmq-remoting",
                "tokio",
                "tonic",
            }.isdisjoint(dependencies)
        )

    def test_protocol_wire_goldens_are_checked_in(self) -> None:
        for relative in (
            "rocketmq-protocol/tests/message_codec_compatibility.rs",
            "rocketmq-protocol/tests/remoting_wire_golden.rs",
            "rocketmq-protocol/tests/fixtures/remoting_command_rocketmq_v1.hex",
        ):
            self.assertTrue((ROOT / relative).is_file(), relative)


if __name__ == "__main__":
    unittest.main()
