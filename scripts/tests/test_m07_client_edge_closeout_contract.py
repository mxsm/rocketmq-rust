# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

import unittest

from scripts.tests.architecture_contract_helpers import load_json
from scripts.tests.architecture_contract_helpers import run_dependency_guard


class ClientEdgeCloseoutContractTest(unittest.TestCase):
    def test_client_manifest_allowlist_is_exactly_current_consumers(self) -> None:
        policy = load_json("scripts/architecture-dependency-policy.json")
        entries = policy["client_policy"]["target_manifest_allowlist"]
        identities = {(entry["caller"], entry["kind"]) for entry in entries}

        self.assertEqual(
            {
                ("rocketmq-admin-core", "normal"),
                ("rocketmq-proxy-cluster", "normal"),
                ("rocketmq-example", "dev"),
            },
            identities,
        )

    def test_client_source_allowlist_is_scoped_to_owned_adapters(self) -> None:
        policy = load_json("scripts/architecture-dependency-policy.json")
        entries = policy["client_policy"]["target_source_allowlist"]

        self.assertEqual(
            {"rocketmq-admin-core", "rocketmq-proxy-cluster", "rocketmq-example"},
            {entry["caller"] for entry in entries},
        )
        self.assertTrue(all(set(entry["aliases"]) == {"rocketmq_client_rust"} for entry in entries))

    def test_target_dependency_guard_accepts_current_edges(self) -> None:
        result = run_dependency_guard("target")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
