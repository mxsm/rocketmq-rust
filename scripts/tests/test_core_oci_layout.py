# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts.tests.release_archive_test_support import create_candidate, seed_binary_partial
from scripts.tests.release_test_support import load_module, read_json, write_json


class CoreOciLayoutTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.common = load_module("release_archive_common_oci", "distribution/release_archive_common.py")
        cls.builder = load_module("build_core_oci_layout_test", "distribution/build_core_oci_layout.py")
        cls.verifier = load_module("verify_core_oci_layout_test", "distribution/verify_core_oci_layout.py")

    def test_four_local_layouts_preserve_linux_archive_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            candidate = create_candidate(Path(temporary))
            target = self.builder.LINUX_TARGET
            partial = seed_binary_partial(self.common, candidate, target)
            root = candidate.parent
            candidate_value = read_json(candidate)
            archive = root / "archives" / f"rocketmq-rust-1.0.0-{target}.manifest.json"
            write_json(
                archive,
                {
                    "schema_version": 1,
                    "candidate_id": candidate_value["candidate_id"],
                    "version": "1.0.0",
                    "run_id": candidate_value["run_id"],
                    "attempt": candidate_value["attempt"],
                    "target": target,
                    "binaries": [entry for entry in partial["artifacts"] if entry["kind"] == "binary"],
                },
            )
            staging = root / "staging" / target / "rocketmq-rust-1.0.0" / "conf"
            staging.mkdir(parents=True)
            for service in self.builder.SERVICES:
                (staging / f"{service}.toml").write_text('workDir = "./work"\n', encoding="utf-8")

            outputs = self.builder.build_layouts(candidate)

            self.assertEqual(4, len(outputs))
            for output in outputs:
                self.assertTrue((output / "oci-layout").is_file())
                self.assertEqual(
                    "not-executed",
                    read_json(output / "OCI_CANDIDATE_MANIFEST.json")["remote_publication"],
                )

            def version_result(command, **_kwargs):
                service = next(name for name in self.builder.SERVICES if name in Path(command[0]).name)
                spec = next(entry for entry in self.common.load_layout()["binaries"] if entry["id"] == service)
                stdout = (
                    "version=1.0.0\n"
                    f"artifact_id={self.common.artifact_id(candidate_value, target, service)}\n"
                    f"requested_features={','.join(spec['requested_features'])}\n"
                    f"effective_features={','.join(spec['effective_features'])}\n"
                )
                return mock.Mock(returncode=0, stdout=stdout, stderr="")

            with mock.patch.object(self.verifier.subprocess, "run", side_effect=version_result):
                evidence = self.verifier.verify_layouts(candidate, smoke=True)
            self.assertEqual("passed", read_json(evidence)["status"])


if __name__ == "__main__":
    unittest.main()
