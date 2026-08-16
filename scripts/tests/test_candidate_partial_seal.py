# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from scripts.tests.release_archive_test_support import create_candidate, seed_binary_partial
from scripts.tests.release_test_support import load_module


class CandidatePartialSealTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.common = load_module("release_archive_common", "distribution/release_archive_common.py")
        cls.sealer = load_module("seal_candidate_partial", "distribution/seal_candidate_partial.py")

    def test_complete_partial_seals_once(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            candidate = create_candidate(Path(temporary))
            target = "x86_64-unknown-linux-gnu"
            partial = seed_binary_partial(self.common, candidate, target)
            root = candidate.parent
            for identifier in ("archive", "archive-manifest", "common-inputs", "component-sbom", "host-smoke"):
                path = root / "evidence" / target / f"{identifier}.json"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("{}\n", encoding="utf-8")
                partial["artifacts"].append(
                    {"id": identifier, "kind": identifier, "path": self.common.candidate_relative(root, path, identifier)}
                )
            self.common.save_draft(root, target, partial)

            sealed = self.sealer.seal_partial(candidate, target)

            self.assertTrue(sealed.is_file())
            with self.assertRaisesRegex(self.common.ArchiveError, "already sealed"):
                self.sealer.seal_partial(candidate, target)


if __name__ == "__main__":
    unittest.main()
