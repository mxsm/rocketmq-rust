# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from scripts.tests.release_archive_test_support import create_candidate
from scripts.tests.release_test_support import load_module, read_json, write_json


class CandidateReleaseNotesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.renderer = load_module(
            "render_candidate_release_notes", "distribution/render_candidate_release_notes.py"
        )

    def test_notes_identify_unofficial_distribution_and_explicit_exclusions(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            candidate = create_candidate(Path(temporary))
            value = read_json(candidate)
            value["known_issues"] = [{"id": "KNOWN-1", "summary": "Documented limitation"}]
            write_json(candidate, value)

            notes = self.renderer.render_notes(value)

            self.assertIn("unofficial community distribution", notes.lower())
            self.assertIn("not an Apache Software Foundation release", notes)
            self.assertIn("OpenMessaging", notes)
            self.assertIn("KNOWN-1", notes)
            self.assertIn("not-executed", notes)


if __name__ == "__main__":
    unittest.main()
