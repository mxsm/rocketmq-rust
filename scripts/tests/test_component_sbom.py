# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from scripts.tests.release_archive_test_support import create_candidate, seed_binary_partial
from scripts.tests.release_test_support import ROOT, load_module, read_json


class ComponentSbomTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.common = load_module("release_archive_common", "distribution/release_archive_common.py")
        cls.sbom = load_module("generate_component_sbom", "distribution/generate_component_sbom.py")

    def test_component_sbom_uses_staging_and_six_binary_contracts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            candidate = create_candidate(Path(temporary))
            target = "x86_64-unknown-linux-gnu"
            seed_binary_partial(self.common, candidate, target)
            staging = candidate.parent / "staging" / target / "rocketmq-rust-1.0.0"
            staging.mkdir(parents=True)
            (staging / "README.md").write_text("fixture\n", encoding="utf-8")

            output = self.sbom.generate_sbom(
                candidate, target, ROOT / "distribution" / "sbom-toolchain.json"
            )

            value = read_json(output)
            self.assertEqual("CycloneDX", value["bomFormat"])
            self.assertEqual(6, len(value["components"]))
            self.assertNotIn("hashes", value)


if __name__ == "__main__":
    unittest.main()
