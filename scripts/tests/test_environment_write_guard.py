# Copyright 2023 The RocketMQ Rust Authors
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

import importlib.util
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "environment_write_guard.py"


def load_guard():
    spec = importlib.util.spec_from_file_location("environment_write_guard", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(module)
    return module


class EnvironmentWriteGuardTests(unittest.TestCase):
    def setUp(self):
        self.guard = load_guard()

    def test_rejects_production_environment_writes(self):
        findings = self.guard.scan_source(
            "fn configure() { unsafe { std::env::set_var(\"KEY\", \"value\") } }",
            "crate/src/lib.rs",
        )

        self.assertEqual(1, len(findings))
        self.assertEqual("set_var", findings[0].operation)

    def test_rejects_environment_removal_through_imported_module(self):
        findings = self.guard.scan_source(
            "use std::env;\nfn clear() { unsafe { env::remove_var(\"KEY\") } }",
            "crate/src/lib.rs",
        )

        self.assertEqual(1, len(findings))
        self.assertEqual("remove_var", findings[0].operation)

    def test_ignores_test_only_modules(self):
        source = """
#[cfg(test)]
mod tests {
    #[test]
    fn controls_process_input() {
        unsafe { std::env::set_var("KEY", "value") }
    }
}
"""

        self.assertEqual([], self.guard.scan_source(source, "crate/src/lib.rs"))

    def test_ignores_comments_and_literals(self):
        source = """
// std::env::set_var("KEY", "value");
const EXAMPLE: &str = r#"env::remove_var("KEY")"#;
"""

        self.assertEqual([], self.guard.scan_source(source, "crate/src/lib.rs"))

    def test_lifetimes_do_not_hide_test_module_boundaries(self):
        source = """
#[cfg(test)]
mod tests {
    fn with_environment(key: &'static str) {
        unsafe { std::env::set_var(key, "value") }
    }
}
"""

        self.assertEqual([], self.guard.scan_source(source, "crate/src/lib.rs"))

    def test_current_tree_has_no_production_environment_writes(self):
        self.assertEqual([], self.guard.scan_tree(ROOT))


if __name__ == "__main__":
    unittest.main()
