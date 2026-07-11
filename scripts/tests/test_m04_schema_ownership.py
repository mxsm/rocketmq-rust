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

from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
LEGACY_PROTOCOL = ROOT / "rocketmq-remoting" / "src" / "protocol"
CANONICAL_PROTOCOL = ROOT / "rocketmq-protocol" / "src" / "protocol"
DECLARATION = re.compile(r"^\s*pub\s+(?:struct|enum|trait)\s+", re.MULTILINE)


class SchemaOwnershipTests(unittest.TestCase):
    def test_declarative_wire_schema_has_no_legacy_definitions(self) -> None:
        owners = ("admin", "body", "filter", "header", "heartbeat", "route", "static_topic", "subscription")
        remaining = []
        for owner in owners:
            for path in (LEGACY_PROTOCOL / owner).rglob("*.rs"):
                if path.name == "topic_queue_mapping_utils.rs":
                    continue
                if DECLARATION.search(path.read_text(encoding="utf-8")):
                    remaining.append(path.relative_to(ROOT).as_posix())
        self.assertEqual([], remaining, "legacy schema definitions remain:\n" + "\n".join(remaining))

    def test_canonical_schema_sources_have_no_owner_state_imports(self) -> None:
        forbidden = (
            "rocketmq_common",
            "rocketmq_rust::ArcMut",
            "std::env",
            "std::fs",
            "tokio",
            "TimeUtils",
            "EnvUtils",
            "FileUtils",
        )
        findings = []
        for path in CANONICAL_PROTOCOL.rglob("*.rs"):
            source = path.read_text(encoding="utf-8")
            for token in forbidden:
                if token in source:
                    findings.append(f"{path.relative_to(ROOT).as_posix()}: {token}")
        self.assertEqual([], findings, "canonical owner leaks:\n" + "\n".join(findings))


if __name__ == "__main__":
    unittest.main()
