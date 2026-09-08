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
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from scripts import runtime_audit as audit


class RuntimeAuditTests(unittest.TestCase):
    def test_comments_strings_and_test_only_items_are_excluded(self):
        source = '''
// tokio::spawn(fake);
const EXAMPLE: &str = "Runtime::new()";
#[cfg(test)]
fn test_helper() { tokio::spawn(async {}); }
fn production() {
    tokio::spawn(async {});
    worker.cancel();
}
'''
        sites = audit.scan_source(source, "src/lib.rs")
        self.assertEqual(["task-spawn", "shutdown"], [site["kind"] for site in sites])

    def test_mixed_cfg_remains_in_production_review(self):
        sites = audit.scan_source(
            "#[cfg(any(test, feature = \"live\"))]\nfn worker() { handle.block_on(job); }",
            "src/lib.rs",
        )
        self.assertEqual(["blocking"], [site["kind"] for site in sites])

    def test_cli_reports_production_modules_without_a_baseline(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "src").mkdir()
            (root / "Cargo.toml").write_text(
                '[package]\nname = "audit-fixture"\nversion = "0.1.0"\n', encoding="utf-8",
            )
            (root / "src/lib.rs").write_text("mod worker;\n", encoding="utf-8")
            (root / "src/worker.rs").write_text("fn work() { tokio::spawn(async {}); }\n", encoding="utf-8")
            result = subprocess.run(
                [sys.executable, str(Path(audit.__file__)), "--root", str(root)],
                capture_output=True, text=True, encoding="utf-8", check=False,
            )
            self.assertEqual(0, result.returncode, result.stdout + result.stderr)
            report = json.loads((root / "target/runtime-audit/runtime-sites.json").read_text(encoding="utf-8"))
            self.assertEqual([{"path": "src/worker.rs", "line": 1, "kind": "task-spawn"}], report["sites"])


if __name__ == "__main__":
    unittest.main()
