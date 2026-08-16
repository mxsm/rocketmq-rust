# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts.tests.release_evidence_test_support import seed_complete_candidate
from scripts.tests.release_test_support import ROOT, load_module, read_json


class ReleaseSbomTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.generator = load_module("generate_release_sbom_test", "distribution/generate_release_sbom.py")

    def test_complete_candidate_generates_31_external_sboms(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            candidate, metadata = seed_complete_candidate(Path(temporary))
            with mock.patch.object(self.generator, "_cargo_metadata", return_value=metadata):
                index = self.generator.generate(candidate, ROOT / "distribution" / "sbom-toolchain.json")

            value = read_json(index)
            self.assertEqual(31, len(value["outputs"]))
            self.assertEqual("not-executed", value["remote_publication"])
            crate_sbom = read_json(candidate.parent / value["outputs"][0]["path"])
            self.assertEqual("CycloneDX", crate_sbom["bomFormat"])
            scopes = {entry["properties"][0]["value"] for entry in crate_sbom["components"]}
            self.assertEqual({"direct", "transitive"}, scopes)


if __name__ == "__main__":
    unittest.main()
