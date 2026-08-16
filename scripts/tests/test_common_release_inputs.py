# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from scripts.tests.release_archive_test_support import create_candidate
from scripts.tests.release_test_support import load_module, read_json


class CommonReleaseInputsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.notes = load_module(
            "render_candidate_release_notes_common", "distribution/render_candidate_release_notes.py"
        )
        cls.builder = load_module(
            "build_common_release_inputs", "distribution/build_common_release_inputs.py"
        )

    def test_common_inputs_are_candidate_scoped_and_local_only(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            candidate = create_candidate(Path(temporary))
            root = candidate.parent
            notes = root / "common-input-source" / "RELEASE_NOTES.md"
            self.assertEqual(0, self.notes.main(["--candidate-manifest", str(candidate), "--output", str(notes)]))

            output = self.builder.build_inputs(candidate, root / "common-inputs")

            manifest = read_json(output / "COMMON_RELEASE_INPUTS.json")
            self.assertEqual("not-executed", manifest["remote_publication"])
            names = {entry["path"] for entry in manifest["files"] if entry["type"] == "file"}
            self.assertTrue({"LICENSE-APACHE", "NOTICE", "README.md", "RELEASE_NOTES.md"}.issubset(names))
            with self.assertRaisesRegex(Exception, "already exists"):
                self.builder.build_inputs(candidate, output)


if __name__ == "__main__":
    unittest.main()
