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

import json
import io
import tempfile
import tarfile
import threading
import unittest
from pathlib import Path

from scripts.tests.release_test_support import load_module, read_json


class ReleaseSeriesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.series = load_module("release_series", "distribution/release_series.py")
        cls.state = load_module("release_state_for_series", "distribution/release_state.py")
        cls.candidate = load_module("candidate_run_for_series", "distribution/candidate_run.py")
        cls.lifecycle = load_module("release_lifecycle_for_series", "scripts/release_lifecycle_guard.py")

    def test_create_export_and_explicit_generation_import(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manifest = self.series.create_series(root, "1.0", "community-v1")
            value = read_json(manifest)
            self.assertEqual(value["generation"], 0)
            self.assertIsNone(value["head"])

            bundle = self.series.export_control_bundle(manifest, root / "series-control.tar")
            imported = self.series.import_control_bundle(bundle, root / "imported", expected_generation=0)
            self.assertEqual(read_json(imported), value)

            with self.assertRaises(self.series.SeriesError):
                self.series.import_control_bundle(bundle, root / "stale", expected_generation=1)

    def test_duplicate_series_refuses_to_overwrite_existing_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self.series.create_series(root, "1.0", "community-v1")
            with self.assertRaises(self.series.SeriesError):
                self.series.create_series(root, "1.0", "community-v1")

    def test_control_bundle_rebases_the_head_and_parent_chain_for_a_new_worker(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manifest = self.series.create_series(root / "source-series", "1.0", "community-v1")
            rc1 = self.candidate.create_candidate(root / "source-candidates", "1.0.0-rc.1", "rc1", 1, manifest)
            self.lifecycle.transition_candidate(rc1, "rejected", phase=5, rejection_reason="fixture")
            bundle = self.series.export_control_bundle(manifest, root / "series-control.tar")
            imported = self.series.import_control_bundle(bundle, root / "worker", expected_generation=2)

            head = read_json(imported)["head"]
            imported_rc1 = Path(head["candidate_manifest"])
            self.assertTrue(imported_rc1.is_file())
            self.assertEqual(read_json(imported_rc1)["series_manifest"], str(imported.resolve()))
            rc2 = self.candidate.create_candidate(root / "worker-candidates", "1.0.0-rc.2", "rc2", 1, imported)
            self.assertEqual(read_json(rc2)["parent_manifest"], str(imported_rc1.resolve()))

    def test_control_bundle_export_waits_for_the_series_lock(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manifest = self.series.create_series(root, "1.0", "community-v1")
            output = root / "locked-export.tar"
            started = threading.Event()
            completed = threading.Event()
            failures: list[BaseException] = []

            def export() -> None:
                started.set()
                try:
                    self.series.export_control_bundle(manifest, output)
                except BaseException as error:
                    failures.append(error)
                finally:
                    completed.set()

            lock = self.state.series_lock_path(manifest)
            with self.state.exclusive_lock(lock):
                worker = threading.Thread(target=export)
                worker.start()
                self.assertTrue(started.wait(1))
                self.assertFalse(completed.wait(0.2))
            worker.join(2)

            self.assertFalse(failures)
            self.assertTrue(completed.is_set())
            self.assertTrue(output.is_file())

    def test_control_bundle_rejects_candidate_and_series_entry_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manifest = self.series.create_series(root / "series", "1.0", "community-v1")
            candidate = self.candidate.create_candidate(
                root / "candidates", "1.0.0-rc.1", "rc1", 1, manifest
            )
            value = read_json(candidate)
            value["state"] = "rejected"
            value["outcome"] = "rejected"
            value["sealed"] = True
            candidate.write_text(json.dumps(value), encoding="utf-8")

            with self.assertRaises(self.series.SeriesError):
                self.series.export_control_bundle(manifest, root / "inconsistent.tar")

    def test_series_validation_rejects_fields_outside_the_closed_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest = self.series.create_series(
                Path(temp_dir), "1.0", "community-v1"
            )
            value = read_json(manifest)
            value["unexpected"] = True

            with self.assertRaises(self.series.ReleaseStateError):
                self.series.validate_series(value)

    def test_control_bundle_import_rejects_duplicate_tar_members(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manifest = self.series.create_series(root / "series", "1.0", "community-v1")
            bundle = self.series.export_control_bundle(manifest, root / "valid.tar")
            duplicate = root / "duplicate.tar"
            with tarfile.open(bundle, "r") as source, tarfile.open(duplicate, "w") as output:
                payloads: list[tuple[str, bytes]] = []
                for member in source.getmembers():
                    stream = source.extractfile(member)
                    self.assertIsNotNone(stream)
                    payloads.append((member.name, stream.read()))
                payloads.append(payloads[0])
                for name, content in payloads:
                    info = tarfile.TarInfo(name)
                    info.size = len(content)
                    output.addfile(info, io.BytesIO(content))

            with self.assertRaises(self.series.SeriesError):
                self.series.import_control_bundle(
                    duplicate, root / "imported", expected_generation=0
                )

    def test_missing_control_bundle_uses_the_series_error_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            with self.assertRaises(self.series.SeriesError):
                self.series.import_control_bundle(
                    root / "missing.tar", root / "imported", expected_generation=0
                )


if __name__ == "__main__":
    unittest.main()
