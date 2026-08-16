# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts.tests.release_archive_test_support import create_candidate, seed_binary_partial
from scripts.tests.release_test_support import ROOT, load_module, read_json


class ReleaseArchiveTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.common = load_module("release_archive_common_e2e", "distribution/release_archive_common.py")
        cls.notes = load_module("render_notes_e2e", "distribution/render_candidate_release_notes.py")
        cls.inputs = load_module("build_inputs_e2e", "distribution/build_common_release_inputs.py")
        cls.staging = load_module("prepare_staging_e2e", "distribution/prepare_release_archive_staging.py")
        cls.sbom = load_module("component_sbom_e2e", "distribution/generate_component_sbom.py")
        cls.builder = load_module("build_archive_e2e", "distribution/build_release_archive.py")
        cls.verifier = load_module("verify_archive_e2e", "distribution/verify_release_archive.py")

    def test_linux_archive_is_built_from_staging_and_smoked(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            candidate = create_candidate(Path(temporary))
            target = "x86_64-unknown-linux-gnu"
            seed_binary_partial(self.common, candidate, target)
            root = candidate.parent
            self.assertEqual(0, self.notes.main(["--candidate-manifest", str(candidate)]))
            common_inputs = self.inputs.build_inputs(candidate, root / "common-inputs")
            self.staging.prepare_staging(candidate, target, common_inputs)
            self.sbom.generate_sbom(candidate, target, ROOT / "distribution" / "sbom-toolchain.json")

            archive, manifest = self.builder.build_archive(candidate, target)
            layout = self.common.load_layout()
            by_name = {
                entry.get("archive_binary", entry["binary"]): entry for entry in layout["binaries"]
            }

            def version_result(command, **_kwargs):
                name = Path(command[0]).name
                binary = by_name[name]
                stdout = (
                    f"component={binary['id']}\n"
                    "version=1.0.0\n"
                    f"artifact_id={self.common.artifact_id(read_json(candidate), target, binary['id'])}\n"
                    f"requested_features={','.join(binary['requested_features'])}\n"
                    f"effective_features={','.join(binary['effective_features'])}\n"
                )
                return mock.Mock(returncode=0, stdout=stdout, stderr="")

            with mock.patch.object(self.verifier.subprocess, "run", side_effect=version_result):
                evidence = self.verifier.verify_archive(candidate, archive, smoke=True)

            self.assertTrue(archive.is_file())
            self.assertEqual("not-executed", read_json(manifest)["remote_publication"])
            self.assertEqual("passed", read_json(evidence)["status"])
            self.assertEqual(6, len(read_json(evidence)["results"]))


if __name__ == "__main__":
    unittest.main()
