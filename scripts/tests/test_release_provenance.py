# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts.tests.release_evidence_test_support import seed_complete_candidate
from scripts.tests.release_test_support import ROOT, load_module, read_json


class ReleaseProvenanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.sbom = load_module("generate_release_sbom_provenance", "distribution/generate_release_sbom.py")
        cls.provenance = load_module(
            "generate_release_provenance_test", "distribution/generate_release_provenance.py"
        )

    def test_provenance_records_semantics_without_digest_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            candidate, metadata = seed_complete_candidate(Path(temporary))
            with mock.patch.object(self.sbom, "_cargo_metadata", return_value=metadata):
                self.sbom.generate(candidate, ROOT / "distribution" / "sbom-toolchain.json")

            output = self.provenance.generate(candidate)

            value = read_json(output)
            self.assertEqual("unofficial-community", value["distribution"])
            self.assertEqual("locked", value["lockfile_mode"])
            self.assertEqual("not-executed", value["remote_publication"])
            rendered = str(value).lower()
            for forbidden in ("'sha':", "'sha256':", "'digest':", "'checksum':"):
                self.assertNotIn(forbidden, rendered)
            self.assertGreater(len(value["outputs"]), 31)
            self.assertEqual("ARTIFACT_INDEX.json", read_json(candidate)["artifact_index"])


if __name__ == "__main__":
    unittest.main()
