# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

from pathlib import Path
import tarfile
import tempfile
import unittest

from scripts.tests.release_archive_test_support import create_candidate
from scripts.tests.release_test_support import ROOT, load_module, read_json, write_json


class PackageCoreHelmTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.packager = load_module("package_core_helm_test", "distribution/package_core_helm.py")

    def test_package_is_local_candidate_scoped_and_complete(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            candidate = create_candidate(Path(temporary), version="1.0.0-rc.1")
            root = candidate.parent
            write_json(
                root / "ARTIFACT_INDEX.json",
                {"schema_version": 1, "candidate_id": read_json(candidate)["candidate_id"], "artifacts": []},
            )

            package, manifest = self.packager.package_chart(
                candidate, ROOT / "distribution" / "helm" / "rocketmq-rust-core"
            )

            self.assertEqual("not-executed", read_json(manifest)["remote_publication"])
            with tarfile.open(package, "r:gz") as archive:
                chart = archive.extractfile("rocketmq-rust-core/Chart.yaml")
                self.assertIsNotNone(chart)
                self.assertIn("version: 1.0.0-rc.1", chart.read().decode())
            artifacts = read_json(root / "ARTIFACT_INDEX.json")["artifacts"]
            self.assertEqual({"helm-core", "helm-core-manifest"}, {entry["id"] for entry in artifacts})


if __name__ == "__main__":
    unittest.main()
