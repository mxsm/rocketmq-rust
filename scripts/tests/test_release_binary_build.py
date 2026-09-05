# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import json
from pathlib import Path
import unittest

from scripts.tests.release_test_support import ROOT, load_module


class ReleaseBinaryBuildTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.common = load_module("release_archive_common", "distribution/release_archive_common.py")
        cls.builder = load_module("build_release_binaries", "distribution/build_release_binaries.py")

    def test_layout_declares_six_core_binaries_and_three_targets(self) -> None:
        layout = self.common.load_layout()
        self.assertEqual(6, len(layout["binaries"]))
        self.assertEqual(3, len(layout["targets"]))
        names = {entry["id"] for entry in layout["binaries"]}
        self.assertEqual({"admin", "broker", "controller", "namesrv", "proxy", "store-inspect"}, names)
        rendered = json.dumps(layout)
        for excluded in (
            "BrokerContainer",
            "Dashboard",
            "MCP",
            "SRE",
            "OpenMessaging",
            "DLedger CommitLog",
        ):
            self.assertIn(excluded, rendered)

    def test_build_commands_are_locked_target_scoped_and_local_only(self) -> None:
        layout = self.common.load_layout()
        root = Path("C:/candidate")
        for binary in layout["binaries"]:
            command = self.builder.build_command(root, "x86_64-pc-windows-msvc", binary)
            self.assertIn("--locked", command)
            self.assertIn("--release", command)
            self.assertIn("--target", command)
            self.assertNotIn("publish", command)

    def test_candidate_workflow_has_no_remote_publication_route(self) -> None:
        workflow = ROOT / ".github" / "workflows" / "release-candidate.yml"
        self.assertTrue(workflow.is_file())
        source = workflow.read_text(encoding="utf-8").lower()
        for forbidden in ("gh release", "cargo publish", "docker push", "helm push"):
            self.assertNotIn(forbidden, source)

    def test_candidate_workflow_executes_the_real_preparation_pipeline(self) -> None:
        workflow = ROOT / ".github" / "workflows" / "release-candidate.yml"
        source = workflow.read_text(encoding="utf-8")

        self.assertNotIn("--help", source)
        self.assertIn("-Mode PrepareCommon", source)
        self.assertIn("-Mode Target", source)
        self.assertIn("-Mode Aggregate", source)
        self.assertIn("import-build-control", source)
        self.assertIn("actions/upload-artifact@v7", source)
        self.assertIn("actions/download-artifact@v8", source)


if __name__ == "__main__":
    unittest.main()
