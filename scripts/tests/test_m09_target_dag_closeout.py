# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

import unittest

from scripts.tests.architecture_contract_helpers import load_json
from scripts.tests.architecture_contract_helpers import run_dependency_guard
from scripts.tests.architecture_contract_helpers import workspace_packages


class TargetDagCloseoutTests(unittest.TestCase):
    def test_workspace_and_target_dag_have_the_same_current_packages(self) -> None:
        packages = set(workspace_packages())
        policy = load_json("scripts/architecture-dependency-policy.json")

        self.assertEqual(packages, set(policy["target_dag"]))
        self.assertEqual(policy["package_counts"]["target"], len(packages))

    def test_temporary_and_compatibility_ledgers_are_exact(self) -> None:
        baseline = load_json("scripts/architecture-dependency-baseline.json")
        policy = load_json("scripts/architecture-dependency-policy.json")

        self.assertEqual([], baseline["manifest_exceptions"])
        self.assertEqual([], baseline["source_exceptions"])
        self.assertEqual(2, len(baseline["compatibility_manifest_exceptions"]))
        self.assertEqual(2, len(policy["test_dependency_policy"]["allowed_edges"]))

    def test_target_guard_closes_unapproved_edges(self) -> None:
        result = run_dependency_guard("target")

        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("TARGET_DEBT_LEDGER active_edges=0 entries=0", result.stdout)
        self.assertIn("ARCHITECTURE_DEPENDENCY_GUARD_OK mode=target", result.stdout)


if __name__ == "__main__":
    unittest.main()
