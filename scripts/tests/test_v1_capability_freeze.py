# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

from copy import deepcopy
import tempfile
import unittest
from pathlib import Path

from scripts.tests.release_test_support import ROOT, load_module, read_json, write_json


class V1CapabilityFreezeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.freeze = load_module("v1_capability_freeze", "scripts/v1_capability_freeze.py")
        cls.guard = load_module("v1_capability_guard_for_freeze", "scripts/v1_capability_guard.py")

    def test_frozen_manifest_closes_every_active_capability_and_keeps_deferrals(self) -> None:
        manifest = read_json(ROOT / "scripts" / "v1-capability-manifest.json")
        contract = read_json(ROOT / "scripts" / "v1-capability-freeze.json")

        self.assertEqual([], self.freeze.validate_freeze(manifest, contract, root=ROOT))
        self.assertEqual([], self.guard.validate_manifest(manifest, root=ROOT, phase=6))
        active = {item["capability_id"]: item for item in manifest["capabilities"][:-2]}
        self.assertEqual(24, len(active))
        self.assertTrue(
            all(item["completion_status"] in {"equivalent", "alternative-equivalent"} for item in active.values())
        )
        self.assertTrue(all(item["implementation_status"] == "implemented" for item in active.values()))
        self.assertEqual(
            {"G-07": "deferred-by-scope", "G-08": "deferred-by-scope"},
            {item["capability_id"]: item["completion_status"] for item in manifest["capabilities"][-2:]},
        )

    def test_check_rejects_status_pr_and_denominator_drift(self) -> None:
        manifest = read_json(ROOT / "scripts" / "v1-capability-manifest.json")
        contract = read_json(ROOT / "scripts" / "v1-capability-freeze.json")
        changed = deepcopy(manifest)
        changed["capabilities"][0]["completion_status"] = "blocked"
        changed_contract = deepcopy(contract)
        changed_contract["active_capabilities"]["F-01"]["implementation_prs"] = []

        findings = self.freeze.validate_freeze(changed, changed_contract, root=ROOT)

        self.assertTrue(any("F-01 completion status" in finding for finding in findings))
        self.assertTrue(any("F-01 implementation PR" in finding for finding in findings))

    def test_apply_is_deterministic_and_preserves_the_closed_denominator(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            target = Path(temp_dir) / "v1-capability-manifest.json"
            target.write_text(
                (ROOT / "scripts" / "v1-capability-manifest.json").read_text(encoding="utf-8"),
                encoding="utf-8",
            )
            contract_path = ROOT / "scripts" / "v1-capability-freeze.json"
            unfrozen = read_json(target)
            unfrozen["capabilities"][0]["completion_status"] = "blocked"
            unfrozen["capabilities"][0]["evidence_status"] = "none"
            unfrozen["capabilities"][0]["artifacts"] = []
            unfrozen["capabilities"][8]["implementation_status"] = "partial"
            write_json(target, unfrozen)

            self.freeze.apply_freeze(target, contract_path)
            first = target.read_text(encoding="utf-8")
            self.freeze.apply_freeze(target, contract_path)

            self.assertEqual(first, target.read_text(encoding="utf-8"))
            self.assertEqual([], self.freeze.validate_freeze(read_json(target), read_json(contract_path), root=ROOT))


if __name__ == "__main__":
    unittest.main()
