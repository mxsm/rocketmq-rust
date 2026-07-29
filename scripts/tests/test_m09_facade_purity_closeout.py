# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

import unittest

from scripts.tests.architecture_contract_helpers import ROOT
from scripts.tests.architecture_contract_helpers import load_json


class FacadePurityCloseoutTests(unittest.TestCase):
    def test_removed_internal_facades_and_source_contract_tests_stay_deleted(self) -> None:
        for relative in (
            "rocketmq-common",
            "rocketmq-remoting",
            "rocketmq",
            "rocketmq-proxy/tests/core_compatibility.rs",
            "rocketmq-store/tests/m06_store_local_compatibility.rs",
            "rocketmq-store/tests/m06_store_local_record_compatibility.rs",
            "rocketmq-store/tests/m06_store_local_commitlog_compatibility.rs",
        ):
            self.assertFalse((ROOT / relative).exists(), relative)

    def test_remaining_composition_facades_are_registered_for_removal(self) -> None:
        registry = load_json("scripts/architecture-debt-registry.json")
        facade_entries = [
            entry for entry in registry["entries"] if entry["class"] == "facade" and entry["status"] == "active"
        ]

        self.assertEqual({"ARC-FACADE-001", "ARC-FACADE-002"}, {entry["id"] for entry in facade_entries})
        self.assertTrue(all(entry["target_release"] == "2.0.0" for entry in facade_entries))


if __name__ == "__main__":
    unittest.main()
