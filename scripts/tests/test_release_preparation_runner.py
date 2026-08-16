# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class ReleasePreparationRunnerTests(unittest.TestCase):
    def test_both_runners_are_fail_fast_and_never_publish(self) -> None:
        powershell = (ROOT / "scripts" / "run-release-preparation.ps1").read_text(encoding="utf-8")
        bash = (ROOT / "scripts" / "run-release-preparation.sh").read_text(encoding="utf-8")
        self.assertIn("$ErrorActionPreference = \"Stop\"", powershell)
        self.assertIn("$LASTEXITCODE", powershell)
        self.assertIn("set -euo pipefail", bash)
        for source in (powershell, bash):
            lowered = source.lower()
            self.assertNotIn("cargo publish", lowered)
            self.assertNotIn("docker push", lowered)
            self.assertNotIn("helm push", lowered)
            self.assertNotIn("gh release", lowered)
            self.assertIn("release_candidate_command.py", source)
            self.assertIn("no_remote_publication_guard.py", source)
            self.assertIn("release_evidence_guard.py", source)


if __name__ == "__main__":
    unittest.main()
