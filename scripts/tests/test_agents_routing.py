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

import os
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class AgentsRoutingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.shells: list[tuple[str, list[str]]] = []
        pwsh = os.environ.get("AGENTS_TEST_PWSH") or shutil.which("pwsh")
        if pwsh:
            cls.shells.append(("PowerShell", [
                pwsh, "-NoLogo", "-NoProfile", "-File",
                str(ROOT / "scripts/check-agents-routing.ps1"),
            ]))
        bash = os.environ.get("AGENTS_TEST_BASH")
        if not bash and os.name == "nt":
            git = shutil.which("git")
            if git:
                candidate = Path(git).resolve().parents[1] / "bin/bash.exe"
                if candidate.is_file():
                    bash = str(candidate)
        elif not bash:
            bash = shutil.which("bash")
        if bash:
            cls.shells.append(("Bash", [
                bash, (ROOT / "scripts/check-agents-routing.sh").as_posix(),
            ]))
        if not cls.shells:
            raise unittest.SkipTest("PowerShell or Bash is required for routing script tests")

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="agents-routing-")
        self.addCleanup(self.temporary.cleanup)
        self.repository = Path(self.temporary.name)
        # Preserve the actual project map while allowing arbitrary policy prose.
        routes = re.findall(
            r"\]\(([^)\n]+/AGENTS\.md)\)",
            (ROOT / "AGENTS.md").read_text(encoding="utf-8"),
        )
        self.assertTrue(routes, "Root project map must link its local guides")
        self.write("AGENTS.md", "# Local routing\n\n" + "\n".join(routes) + "\n")
        self.write("Cargo.toml", "[workspace]\nmembers = []\n")
        for route in routes:
            self.write(route, "# Local guide\n\nChoose the relevant check.\n")
        for workflow in (ROOT / ".github/workflows").iterdir():
            if workflow.suffix in {".yml", ".yaml"}:
                self.write(f".github/workflows/{workflow.name}", "name: fixture\n")
        for document in ("agents-routing-validation-adr.md", "agent-validation-reference.md"):
            self.write(f"rocketmq-doc/en/{document}", "# Select checks by impact\n")
        self.write(
            "scripts/standalone_workspace_trigger_guard.py",
            "from pathlib import Path\n"
            "Path(__file__).parents[1].joinpath('metadata-invoked').write_text('unexpected')\n"
            "raise SystemExit(97)\n",
        )

    def write(self, relative: str, content: str) -> None:
        path = self.repository / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    def assert_routing(self, expected_code: int, expected_message: str) -> None:
        for name, command in self.shells:
            with self.subTest(shell=name):
                argument = (["-RepoRoot", str(self.repository)] if name == "PowerShell"
                            else [self.repository.as_posix()])
                result = subprocess.run(
                    command + argument, capture_output=True, text=True,
                    encoding="utf-8", errors="replace", timeout=30, check=False,
                )
                output = result.stdout + result.stderr
                self.assertEqual(expected_code, result.returncode, output)
                self.assertIn(expected_message, output)
                self.assertFalse((self.repository / "metadata-invoked").exists(), output)

    def test_routing_accepts_new_wording_without_build_or_metadata_audits(self) -> None:
        self.write("standalone demo/Cargo.toml", "[workspace]\nmembers = []\n")
        self.write("standalone demo/AGENTS.md", "# Standalone guide\n")
        self.write("frontend demo/package.json", "{}\n")
        self.write("frontend demo/AGENTS.md", "# Frontend guide\n")
        root_guide = self.repository / "AGENTS.md"
        with root_guide.open("a", encoding="utf-8") as guide:
            guide.write("standalone demo/\nfrontend demo/\n")
        self.assert_routing(0, "AGENTS_ROUTING_CHECK_OK standalone_cargo=1 node_projects=1")

    def test_new_standalone_project_requires_local_instructions(self) -> None:
        self.write("new-standalone/Cargo.toml", "[workspace]\nmembers = []\n")
        self.assert_routing(
            1, "Standalone Cargo project at 'new-standalone' has no same-directory AGENTS.md",
        )

    def test_new_node_project_requires_local_instructions(self) -> None:
        self.write("new-ui/package.json", "{}\n")
        self.assert_routing(1, "Node project at 'new-ui' has no same-directory AGENTS.md")

    def test_new_project_requires_root_route_even_with_local_instructions(self) -> None:
        self.write("new-standalone/Cargo.toml", "[workspace]\nmembers = []\n")
        self.write("new-standalone/AGENTS.md", "# Guide\n")
        self.assert_routing(1, "Root AGENTS.md standalone Cargo routing does not mention 'new-standalone/'")

    def test_missing_workflow_is_reported(self) -> None:
        (self.repository / ".github/workflows/rocketmq-rust-ci.yaml").unlink()
        self.assert_routing(1, "Missing required workflow: .github/workflows/rocketmq-rust-ci.yaml")

    def test_missing_root_guide_is_reported(self) -> None:
        (self.repository / "AGENTS.md").unlink()
        self.assert_routing(1, "Missing required file: AGENTS.md")


if __name__ == "__main__":
    unittest.main()
