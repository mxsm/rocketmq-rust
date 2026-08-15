# Copyright 2026 The RocketMQ Rust Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import io
import stat
import tarfile
import tempfile
import unittest
from pathlib import Path

from scripts.tests.release_test_support import create_source_bundle, load_module, read_json


class CandidateSourceSnapshotTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.series = load_module("release_series_for_snapshot", "distribution/release_series.py")
        cls.candidate = load_module("candidate_run_for_snapshot", "distribution/candidate_run.py")
        cls.snapshot = load_module(
            "create_candidate_source_snapshot",
            "distribution/create_candidate_source_snapshot.py",
        )

    def test_snapshot_comes_only_from_the_registered_source_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            candidate = self.candidate.create_candidate(root / "candidates", "1.0.0-rc.1", "rc1", 1, series)
            bundle = create_source_bundle(
                root / "CORE_SOURCE_TRANSFER.tar",
                version="1.0.0-rc.1",
                run_id="rc1",
                attempt=1,
            )
            self.candidate.record_build_source_bundle(candidate, bundle)

            snapshot = self.snapshot.create_snapshot(candidate)
            value = read_json(snapshot)
            self.assertTrue(value["sealed"])
            self.assertNotIn("sha", snapshot.read_text(encoding="utf-8").lower())
            copied = snapshot.parent / "source" / "src/lib.rs"
            self.assertEqual(copied.read_bytes(), b"pub fn ready() {}\n")
            self.assertFalse(copied.stat().st_mode & stat.S_IWUSR)

    def test_path_traversal_and_manifest_size_drift_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            candidate = self.candidate.create_candidate(root / "candidates", "1.0.0-rc.1", "rc1", 1, series)
            bundle = create_source_bundle(root / "bad.tar", version="1.0.0-rc.1", run_id="rc1", attempt=1)
            with tarfile.open(bundle, "a") as archive:
                content = b"escape"
                info = tarfile.TarInfo("source/../outside.txt")
                info.size = len(content)
                archive.addfile(info, io.BytesIO(content))
            self.candidate.record_build_source_bundle(candidate, bundle)

            with self.assertRaises(self.snapshot.SnapshotError):
                self.snapshot.create_snapshot(candidate)
            self.assertFalse((root / "outside.txt").exists())


if __name__ == "__main__":
    unittest.main()
