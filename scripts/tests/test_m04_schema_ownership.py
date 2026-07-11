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
LEGACY_FACADES = {
    "rocketmq-remoting/src/protocol/body/subscription_group_wrapper.rs",
    "rocketmq-remoting/src/protocol/body/topic_info_wrapper/topic_config_wrapper.rs",
}


class SchemaOwnershipTests(unittest.TestCase):
    def test_declarative_wire_schema_has_no_legacy_definitions(self) -> None:
        owners = ("admin", "body", "filter", "header", "heartbeat", "route", "static_topic", "subscription")
        remaining = []
        for owner in owners:
            for path in (LEGACY_PROTOCOL / owner).rglob("*.rs"):
                if path.name == "topic_queue_mapping_utils.rs":
                    continue
                relative = path.relative_to(ROOT).as_posix()
                source = path.read_text(encoding="utf-8")
                if DECLARATION.search(source) and relative not in LEGACY_FACADES:
                    remaining.append(relative)
                if relative in LEGACY_FACADES and ("Canonical" not in source or "impl From" not in source):
                    remaining.append(f"{relative}: missing canonical conversion")
        self.assertEqual([], remaining, "legacy schema definitions remain:\n" + "\n".join(remaining))

    def test_canonical_schema_sources_have_no_owner_state_imports(self) -> None:
        forbidden = (
            "rocketmq_common",
            "rocketmq_rust::ArcMut",
            "std::env",
            "std::fs",
            "SystemTime",
            "Instant",
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

    def test_model_protocol_dependency_direction_and_version_exception(self) -> None:
        model_manifest = (ROOT / "rocketmq-model" / "Cargo.toml").read_text(encoding="utf-8")
        protocol_manifest = (ROOT / "rocketmq-protocol" / "Cargo.toml").read_text(encoding="utf-8")
        protocol_lib = (ROOT / "rocketmq-protocol" / "src" / "lib.rs").read_text(encoding="utf-8")
        common_version = (ROOT / "rocketmq-common" / "src" / "common" / "mq_version.rs").read_text(encoding="utf-8")

        self.assertNotIn("rocketmq-protocol", model_manifest)
        self.assertIn("rocketmq-model.workspace = true", protocol_manifest)
        self.assertIn("pub use rocketmq_model::version;", protocol_lib)
        self.assertIn("pub use rocketmq_model::version::*;", common_version)


if __name__ == "__main__":
    unittest.main()
