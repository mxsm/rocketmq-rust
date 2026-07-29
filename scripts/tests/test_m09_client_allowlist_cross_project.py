# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

import unittest

from scripts.tests.architecture_contract_helpers import load_json
from scripts.tests.architecture_contract_helpers import run_dependency_guard


class ClientAllowlistCrossProjectTests(unittest.TestCase):
    def test_client_edges_are_exactly_two_production_and_one_example(self) -> None:
        policy = load_json("scripts/architecture-dependency-policy.json")
        entries = policy["client_policy"]["target_manifest_allowlist"]

        self.assertEqual(3, len(entries))
        self.assertEqual(2, sum(entry["kind"] == "normal" for entry in entries))
        self.assertEqual(1, sum(entry["kind"] == "dev" for entry in entries))

    def test_target_ledgers_use_current_counts(self) -> None:
        result = run_dependency_guard("target")

        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("TARGET_COMPATIBILITY_LEDGER active_edges=2 entries=2", result.stdout)
        self.assertIn("TARGET_TEST_DEPENDENCIES active_edges=2 entries=2", result.stdout)


if __name__ == "__main__":
    unittest.main()
