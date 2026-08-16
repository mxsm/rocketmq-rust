# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from scripts.tests.release_test_support import ROOT, load_module


class CoreContainerImageGuardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.guard = load_module("core_container_image_guard", "scripts/core_container_image_guard.py")
        cls.policy = ROOT / "docker" / "core-container-policy.json"
        cls.chart = ROOT / "distribution" / "helm" / "rocketmq-rust-core"

    def test_repository_core_contract_passes(self) -> None:
        self.assertEqual([], self.guard.audit(self.policy, self.chart))

    def test_excluded_service_in_values_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            chart = Path(temporary) / "chart"
            import shutil

            shutil.copytree(self.chart, chart)
            values = chart / "values.yaml"
            values.write_text(values.read_text(encoding="utf-8") + "\n  mcp:\n    enabled: true\n", encoding="utf-8")

            findings = self.guard.audit(self.policy, chart)

            self.assertTrue(any("excluded capability" in finding for finding in findings))


if __name__ == "__main__":
    unittest.main()
