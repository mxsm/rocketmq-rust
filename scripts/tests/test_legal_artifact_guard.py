# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

from pathlib import Path
import tarfile
import tempfile
import unittest

from scripts.tests.release_evidence_test_support import seed_complete_candidate
from scripts.tests.release_test_support import load_module, read_json


class LegalArtifactGuardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.guard = load_module("legal_artifact_guard_test", "scripts/legal_artifact_guard.py")

    def test_complete_candidate_legal_denominator_passes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            candidate, _metadata = seed_complete_candidate(Path(temporary))
            self.assertEqual([], self.guard.audit(candidate))

    def test_missing_crate_notice_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            candidate, _metadata = seed_complete_candidate(Path(temporary))
            root = candidate.parent
            package = read_json(root / "PACKAGE_PLAN.json")["staged_packages"][0]
            crate = root / package["crate_path"]
            replacement = crate.with_suffix(".tmp")
            with tarfile.open(crate, "r:gz") as source, tarfile.open(replacement, "w:gz") as output:
                for member in source.getmembers():
                    if member.name.endswith("/NOTICE"):
                        continue
                    output.addfile(member, source.extractfile(member) if member.isfile() else None)
            replacement.replace(crate)

            findings = self.guard.audit(candidate)

            self.assertTrue(any("crate legal files are incomplete" in finding for finding in findings))


if __name__ == "__main__":
    unittest.main()
