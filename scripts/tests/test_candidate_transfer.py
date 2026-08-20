# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from scripts.tests.release_test_support import load_module, read_json


class CandidateTransferTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.transfer = load_module("transfer_candidate_test", "distribution/transfer_candidate.py")
        cls.series = load_module("release_series_for_transfer_test", "distribution/release_series.py")
        cls.candidate = load_module("candidate_run_for_transfer_test", "distribution/candidate_run.py")

    def test_control_bundle_carries_candidate_and_external_series(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            series = self.series.create_series(base / "series", "1.0", "community-v1")
            candidate = self.candidate.create_candidate(
                base / "candidates", "1.0.0-rc.1", "local", 1, series
            )
            root = candidate.parent
            bundle = root / "transfer" / "CANDIDATE_BUILD_CONTROL_BUNDLE.tar"
            result = self.transfer.main(
                ["export-build-control", "--candidate-manifest", str(candidate), "--output", str(bundle)]
            )
            self.assertEqual(0, result)

            imported = self.transfer.import_bundle(bundle, base / "imported")
            manifest = read_json(imported / "CANDIDATE_TRANSFER.json")
            self.assertEqual("build-control", manifest["bundle_kind"])
            self.assertTrue((imported / "CANDIDATE_RUN.json").is_file())
            self.assertTrue((imported / "RELEASE_SERIES.json").is_file())


if __name__ == "__main__":
    unittest.main()
