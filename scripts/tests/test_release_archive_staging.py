# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

from pathlib import Path
import tempfile
import tomllib
import unittest

from scripts.tests.release_archive_test_support import create_candidate, seed_binary_partial
from scripts.tests.release_test_support import load_module


class ReleaseArchiveStagingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.common = load_module("release_archive_common_staging", "distribution/release_archive_common.py")
        cls.notes = load_module("render_notes_staging", "distribution/render_candidate_release_notes.py")
        cls.inputs = load_module("build_inputs_staging", "distribution/build_common_release_inputs.py")
        cls.staging = load_module("prepare_staging_test", "distribution/prepare_release_archive_staging.py")

    def test_staging_has_six_binaries_and_portable_configs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            candidate = create_candidate(Path(temporary))
            target = "x86_64-unknown-linux-gnu"
            seed_binary_partial(self.common, candidate, target)
            root = candidate.parent
            self.assertEqual(0, self.notes.main(["--candidate-manifest", str(candidate)]))
            common_inputs = self.inputs.build_inputs(candidate, root / "common-inputs")

            package = self.staging.prepare_staging(candidate, target, common_inputs)

            self.assertEqual(6, len(list((package / "bin").iterdir())))
            for config in (package / "conf").glob("*.toml"):
                source = config.read_text(encoding="utf-8")
                self.assertNotIn("${user.home}", source)
                self.assertNotIn("/opt/data", source)
                with config.open("rb") as handle:
                    tomllib.load(handle)
            for excluded in ("mcp", "dashboard", "sre", "openmessaging", "brokercontainer", "dledger"):
                self.assertNotIn(excluded, " ".join(path.name.lower() for path in package.rglob("*")))


if __name__ == "__main__":
    unittest.main()
