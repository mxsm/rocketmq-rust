# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import json
from pathlib import Path
import shutil
import tempfile
import unittest

from scripts.tests.release_test_support import ROOT, load_module, read_json, write_json


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
            self.assertEqual(read_json(candidate)["series_generation"], manifest["series_generation"])
            self.assertTrue((imported / "RELEASE_SERIES_CONTROL_BUNDLE.tar").is_file())

    def test_control_bundle_import_relocates_the_committed_series(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            producer_root = base / "producer"
            series = self.series.create_series(
                producer_root / "series", "1.0", "community-v1"
            )
            candidate = self.candidate.create_candidate(
                producer_root / "candidates", "1.0.0-rc.1", "local", 1, series
            )
            candidate_id = read_json(candidate)["candidate_id"]
            produced_bundle = (
                candidate.parent / "transfer" / "CANDIDATE_BUILD_CONTROL_BUNDLE.tar"
            )
            self.transfer.export_build_control(candidate, produced_bundle)
            bundle = base / "CANDIDATE_BUILD_CONTROL_BUNDLE.tar"
            shutil.copy2(produced_bundle, bundle)

            shutil.rmtree(producer_root / "series")
            shutil.rmtree(producer_root / "candidates" / "1.0.0-rc.1" / "local" / "attempt-1")
            worker_root = base / "isolated-worker"

            imported_candidate = self.transfer.import_build_control(bundle, worker_root)

            imported = read_json(imported_candidate)
            imported_series_path = Path(imported["series_manifest"])
            imported_series = read_json(imported_series_path)
            self.assertEqual(candidate_id, imported["candidate_id"])
            self.assertEqual(imported_candidate.parent, Path(imported["candidate_root"]))
            self.assertEqual(
                imported_candidate.resolve(),
                Path(imported_series["head"]["candidate_manifest"]).resolve(),
            )
            for path in (imported_candidate, imported_series_path):
                self.assertTrue(path.resolve().is_relative_to(worker_root.resolve()))
            self.assertNotIn(str(producer_root.resolve()), json.dumps(imported_series))
            self.assertNotIn(str(producer_root.resolve()), json.dumps(imported))

    def test_common_inputs_bundle_preserves_closed_candidate_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            candidate = self.candidate.create_candidate(
                base / "candidates",
                "1.0.0-rc.1",
                "local",
                1,
                self.series.create_series(base / "series", "1.0", "community-v1"),
            )
            common = candidate.parent / "common-inputs-staging"
            common.mkdir(parents=True)
            (common / "COMMON_RELEASE_INPUTS.json").write_text(
                '{"schema_version":1}\n', encoding="utf-8"
            )
            (common / "RELEASE_NOTES.md").write_text("candidate notes\n", encoding="utf-8")
            bundle = candidate.parent / "common-inputs" / "COMMON_RELEASE_INPUTS.tar"

            result = self.transfer.main(
                [
                    "export-common-inputs",
                    "--candidate-manifest",
                    str(candidate),
                    "--input-root",
                    str(common),
                    "--output",
                    str(bundle),
                ]
            )

            self.assertEqual(0, result)
            imported = self.transfer.import_bundle(bundle, base / "imported-common")
            transfer = read_json(imported / "CANDIDATE_TRANSFER.json")
            self.assertEqual("common-inputs", transfer["bundle_kind"])
            self.assertEqual(read_json(candidate)["candidate_id"], transfer["candidate_id"])
            self.assertEqual(
                ["COMMON_RELEASE_INPUTS.json", "RELEASE_NOTES.md"],
                sorted(entry["path"] for entry in transfer["files"]),
            )

            payload_only = base / "payload-only"
            result = self.transfer.main(
                [
                    "import",
                    "--bundle",
                    str(bundle),
                    "--output",
                    str(payload_only),
                    "--payload-only",
                ]
            )
            self.assertEqual(0, result)
            self.assertEqual(
                ["COMMON_RELEASE_INPUTS.json", "RELEASE_NOTES.md"],
                sorted(path.name for path in payload_only.iterdir()),
            )

    def test_artifact_bundle_keeps_local_oci_layouts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            candidate = self.candidate.create_candidate(
                base / "candidates",
                "1.0.0-rc.1",
                "local",
                1,
                self.series.create_series(base / "series", "1.0", "community-v1"),
            )
            root = candidate.parent
            write_json(root / "ARTIFACT_INDEX.json", {"schema_version": 1, "artifacts": []})
            write_json(root / "evidence" / "EVIDENCE_INDEX.json", {"schema_version": 1})
            write_json(root / "evidence" / "NO_REMOTE_PUBLICATION.json", {"schema_version": 1})
            layout = root / "oci-layout" / "broker" / "oci-layout"
            write_json(layout, {"imageLayoutVersion": "1.0.0"})
            bundle = root / "transfer" / "CANDIDATE_SOURCE_BUNDLE.tar"

            result = self.transfer.main(
                [
                    "export-artifacts",
                    "--candidate-manifest",
                    str(candidate),
                    "--output",
                    str(bundle),
                    "--repository-source-root",
                    str(ROOT),
                ]
            )

            self.assertEqual(0, result)
            imported = self.transfer.import_bundle(bundle, base / "imported-artifacts")
            self.assertTrue((imported / "oci-layout" / "broker" / "oci-layout").is_file())


if __name__ == "__main__":
    unittest.main()
