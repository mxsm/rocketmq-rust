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
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "rust_hygiene_guard.py"


def load_guard():
    sys.path.insert(0, str(SCRIPT.parent))
    spec = importlib.util.spec_from_file_location("rust_hygiene_guard", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(module)
    return module


class RustHygieneGuardTests(unittest.TestCase):
    def setUp(self):
        self.guard = load_guard()

    def test_rejects_unsafe_without_safety_comment(self):
        safety, _ = self.guard.scan_source(
            "fn read(pointer: *const u8) { unsafe { pointer.read(); } }",
            "crate/src/lib.rs",
        )

        self.assertEqual(1, len(safety))

    def test_accepts_adjacent_safety_comment(self):
        safety, _ = self.guard.scan_source(
            """
fn read(pointer: *const u8) {
    // SAFETY: the caller validated that pointer addresses an initialized byte.
    unsafe { pointer.read(); }
}
""",
            "crate/src/lib.rs",
        )

        self.assertEqual([], safety)

    def test_accepts_multiline_safety_comment(self):
        safety, _ = self.guard.scan_source(
            """
fn read(pointer: *const u8) {
    // SAFETY: the caller validated the pointer.
    // The initialized allocation remains alive for the read.
    unsafe { pointer.read(); }
}
""",
            "crate/src/lib.rs",
        )

        self.assertEqual([], safety)

    def test_inventory_is_item_scoped_and_stable_across_line_shifts(self):
        first = "fn decode(value: Option<u8>) { value.unwrap(); }\n"
        shifted = "\n\nfn decode(value: Option<u8>) { value.unwrap(); }\n"

        _, first_debt = self.guard.scan_source(first, "crate/src/lib.rs")
        _, shifted_debt = self.guard.scan_source(shifted, "crate/src/lib.rs")

        self.assertEqual(first_debt[0]["identity"], shifted_debt[0]["identity"])

    def test_ignores_test_module_debt_and_unsafe(self):
        source = """
#[cfg(test)]
mod tests {
    fn probe(value: Option<u8>) {
        unsafe { std::ptr::read(&0) };
        value.unwrap();
    }
}
"""

        self.assertEqual(([], []), self.guard.scan_source(source, "crate/src/lib.rs"))

    def test_detects_manual_pin_projection(self):
        _, debt = self.guard.scan_source(
            "fn poll(value: Pin<&mut Value>) { value.get_unchecked_mut(); }\n",
            "crate/src/lib.rs",
        )

        self.assertEqual("manual_pin", debt[0]["kind"])


if __name__ == "__main__":
    unittest.main()
