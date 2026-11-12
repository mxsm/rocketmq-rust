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

import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "check_read_gateway_boundary.py"
SPEC = importlib.util.spec_from_file_location("check_read_gateway_boundary", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ReadGatewayBoundaryTests(unittest.TestCase):
    def test_only_a_leading_file_level_cfg_excludes_adapter_fixtures(self) -> None:
        cases = (
            ("// License\n\n#![cfg(test)]\nMcpSource", []),
            ("McpSource", ["connector-adapter-bypass:fixture_tests.rs:McpSource"]),
            ("#[cfg(test)]\nmod tests {}\nMcpSource", ["connector-adapter-bypass:fixture_tests.rs:McpSource"]),
            ("/*\n#![cfg(test)]\n*/\nMcpSource", ["connector-adapter-bypass:fixture_tests.rs:McpSource"]),
            ('const TEXT: &str = r#"\n#![cfg(test)]\n"#;\nMcpSource', ["connector-adapter-bypass:fixture_tests.rs:McpSource"]),
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_root = root / "src"
            source_root.mkdir()
            fixture = source_root / "fixture_tests.rs"
            with patch.object(MODULE, "CONNECTOR", root):
                for source, expected in cases:
                    with self.subTest(source=source):
                        fixture.write_text(source, encoding="utf-8")
                        self.assertEqual(MODULE.connector_adapter_bypasses(), expected)


if __name__ == "__main__":
    unittest.main()
