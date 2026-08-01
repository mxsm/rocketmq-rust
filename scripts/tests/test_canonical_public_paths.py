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

import re
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import public_api_intent_guard as intent_guard  # noqa: E402


CRATES = {
    "rocketmq-client-rust": (
        "rocketmq-client/src/lib.rs",
        "rocketmq-client/src/public_api.rs",
    ),
    "rocketmq-runtime": (
        "rocketmq-runtime/src/lib.rs",
        "rocketmq-runtime/src/public_api.rs",
    ),
    "rocketmq-transport": (
        "rocketmq-transport/src/lib.rs",
        "rocketmq-transport/src/public_api.rs",
    ),
    "rocketmq-store": (
        "rocketmq-store/src/lib.rs",
        "rocketmq-store/src/public_api.rs",
    ),
}

FORBIDDEN_ROOT_SYMBOLS = {
    "rocketmq-client/src/lib.rs": (
        "proxy_adapter_compat",
        "MQClientAPIExt",
        "MqClientAdminImpl",
    ),
    "rocketmq-store/src/lib.rs": ("bench_support",),
}

TEST_SUPPORT_CRATES = (
    "rocketmq-client/src/lib.rs",
    "rocketmq-transport/src/lib.rs",
    "rocketmq-store/src/lib.rs",
)


def exported_name(declaration: str) -> str | None:
    use_match = re.fullmatch(r"pub use (.+)", declaration)
    if use_match:
        target = use_match.group(1).strip()
        if target.endswith("::*") or "{" in target:
            return None
        if " as " in target:
            return target.rsplit(" as ", 1)[1]
        return target.rsplit("::", 1)[-1]
    item_match = re.match(
        r"pub (?:type|struct|enum|trait|fn|const|static|mod) ([A-Za-z_][A-Za-z0-9_]*)",
        declaration,
    )
    return item_match.group(1) if item_match else None


class CanonicalPublicPathTests(unittest.TestCase):
    def test_public_api_exports_are_not_duplicated_in_root_handwritten_exports(self) -> None:
        duplicates: list[str] = []
        for crate, (root_path, canonical_path) in CRATES.items():
            root_source = (ROOT / root_path).read_text(encoding="utf-8")
            canonical_source = (ROOT / canonical_path).read_text(encoding="utf-8")
            root_names = {
                name
                for entry in intent_guard.inventory_source(root_path, root_source, crate)
                if entry["declaration"] != "pub use public_api::*"
                for name in [exported_name(entry["declaration"])]
                if name is not None
            }
            canonical_names = {
                name
                for entry in intent_guard.inventory_source(canonical_path, canonical_source, crate)
                for name in [exported_name(entry["declaration"])]
                if name is not None
            }
            duplicates.extend(f"{crate}:{name}" for name in sorted(root_names & canonical_names))

        self.assertEqual([], duplicates, "\n".join(duplicates))

    def test_retired_compat_aliases_and_production_bench_support_are_absent(self) -> None:
        findings: list[str] = []
        for relative, symbols in FORBIDDEN_ROOT_SYMBOLS.items():
            source = (ROOT / relative).read_text(encoding="utf-8")
            findings.extend(
                f"{relative}:{symbol}" for symbol in symbols if re.search(rf"\b{re.escape(symbol)}\b", source)
            )

        self.assertEqual([], findings, "\n".join(findings))

    def test_root_crates_have_no_doc_hidden_public_exports(self) -> None:
        findings = [
            relative
            for relative, _ in CRATES.values()
            if "#[doc(hidden)]" in (ROOT / relative).read_text(encoding="utf-8")
        ]
        self.assertEqual([], findings, "\n".join(findings))

    def test_test_support_is_explicit_and_feature_gated(self) -> None:
        expected = '#[cfg(any(test, feature = "test-support"))]\npub mod test_support;'
        findings = [
            relative
            for relative in TEST_SUPPORT_CRATES
            if expected not in (ROOT / relative).read_text(encoding="utf-8")
        ]
        self.assertEqual([], findings, "\n".join(findings))

    def test_each_crate_root_reexports_one_canonical_surface(self) -> None:
        findings: list[str] = []
        for crate, (root_path, _) in CRATES.items():
            source = (ROOT / root_path).read_text(encoding="utf-8")
            if source.count("pub use public_api::*;") != 1:
                findings.append(crate)
        self.assertEqual([], findings, "\n".join(findings))


if __name__ == "__main__":
    unittest.main()
