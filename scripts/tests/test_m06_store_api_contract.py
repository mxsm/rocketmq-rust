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
import tomllib
import unittest
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
CRATE = ROOT / "rocketmq-store-api"
CAPABILITIES = {
    "StoreLifecycle",
    "MessageAppender",
    "MessageReader",
    "OffsetIndex",
    "StoreHealth",
    "ReplicationControl",
    "DerivedRecordSink",
    "AdminStore",
}
ALLOWED_DEPENDENCIES = {"rocketmq-model", "rocketmq-error", "bytes"}
M06_02_TYPES = {
    "AppendReceipt",
    "AppendReceiptError",
    "AppendStatus",
    "DerivedProgress",
    "Durability",
    "FlushBacklog",
    "GetResult",
    "GetStatus",
    "LeasedBytes",
    "QueryResult",
    "ReadCacheState",
    "SelectResult",
    "StoreHealthSnapshot",
}
DEFERRED_TYPES = {
    "ReadRequest",
    "ReadResult",
    "StoredMessage",
    "OffsetRange",
    "ReplicationState",
    "DerivedRecord",
    "AdminRequest",
    "AdminResponse",
    "StoreFuture",
}
FORBIDDEN_NORMALIZED_TOKENS = (
    "observability",
    "mappedfile",
    "remoting",
    "rocksdb",
    "runtime",
    "broker",
    "native",
    "tiered",
    "timer",
    "tokio",
    "ha",
)
COMPOUND_FORBIDDEN_FIXTURES = {
    "HaState": "ha",
    "TimerWheel": "timer",
    "RocksDbBackend": "rocksdb",
    "MappedFileHandle": "mappedfile",
    "NativeStore": "native",
}


def dependency_tables(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    tables = [
        manifest.get("dependencies", {}),
        manifest.get("dev-dependencies", {}),
        manifest.get("build-dependencies", {}),
    ]
    for target in manifest.get("target", {}).values():
        tables.extend(
            [
                target.get("dependencies", {}),
                target.get("dev-dependencies", {}),
                target.get("build-dependencies", {}),
            ]
        )
    return tables


def normalized(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def normalized_segments(identifier: str) -> set[str]:
    parts = re.findall(r"[A-Z]+(?=[A-Z][a-z]|[0-9_]|$)|[A-Z]?[a-z]+|[0-9]+", identifier)
    return {normalized(part) for part in parts}


def forbidden_identifiers(source: str) -> dict[str, str]:
    code = re.sub(r"/\*.*?\*/", " ", source, flags=re.DOTALL)
    code = re.sub(r"//[^\r\n]*", " ", code)
    code = re.sub(r'(?s)(?:br|r|b)?(?:#+)?".*?"(?:#+)?', " ", code)
    forbidden: dict[str, str] = {}
    for identifier in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", code):
        candidate = normalized(identifier)
        segments = normalized_segments(identifier)
        for token in FORBIDDEN_NORMALIZED_TOKENS:
            if (token == "ha" and token in segments) or (token != "ha" and token in candidate):
                forbidden[identifier] = token
                break
    return forbidden


def function_body(source: str, signature: str) -> str:
    start = source.index(signature)
    brace = source.index("{", start)
    depth = 0
    for index in range(brace, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[brace + 1 : index]
    raise AssertionError(f"unterminated function: {signature}")


def item_body(source: str, signature: str) -> str:
    start = source.index(signature)
    brace = source.index("{", start)
    depth = 0
    for index in range(brace, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[brace + 1 : index]
    raise AssertionError(f"unterminated item: {signature}")


def arc_dyn_aliases(source: str) -> set[str]:
    return set(re.findall(r"(?:pub\s+)?type\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*Arc\s*<\s*dyn\b", source))


class StoreApiContractTests(unittest.TestCase):
    def test_workspace_contains_minimal_runtime_neutral_store_api_crate(self) -> None:
        root_manifest = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
        self.assertIn("rocketmq-store-api", root_manifest["workspace"]["members"])
        self.assertIn("rocketmq-store-api", root_manifest["workspace"]["dependencies"])

        manifest = tomllib.loads((CRATE / "Cargo.toml").read_text(encoding="utf-8"))
        self.assertEqual([], manifest["features"]["default"])
        dependencies: set[str] = set()
        for table in dependency_tables(manifest):
            for alias, spec in table.items():
                dependencies.add(alias)
                self.assertIn(alias, ALLOWED_DEPENDENCIES)
                if isinstance(spec, dict):
                    self.assertNotIn("package", spec, f"dependency alias is forbidden: {alias}")
        self.assertEqual(ALLOWED_DEPENDENCIES, dependencies)

    def test_public_contracts_include_m06_02_results_and_remain_backend_neutral(self) -> None:
        source = (CRATE / "src" / "lib.rs").read_text(encoding="utf-8")
        for capability in CAPABILITIES:
            self.assertIn(f"pub trait {capability}", source)
        for required in M06_02_TYPES:
            self.assertRegex(source, rf"pub (?:struct|enum) {required}(?:<L>)?")
        for removed in DEFERRED_TYPES:
            self.assertNotIn(f"pub struct {removed}", source)
            self.assertNotIn(f"pub enum {removed}", source)
            self.assertNotIn(f"pub type {removed}", source)
        self.assertNotIn("Pin<Box", source)
        self.assertNotIn("dyn Future", source)
        self.assertNotIn("Box::pin", source)
        self.assertNotIn("Arc<dyn", source)
        self.assertNotIn("trait MessageStore", source)
        self.assertIn("pub enum StoreOperation", source)
        self.assertIn("pub struct LeasedBytes<L>", source)
        self.assertIn("pub struct SelectResult<L>", source)
        self.assertIn("pub struct GetResult<L>", source)
        self.assertIn("pub struct QueryResult<L>", source)
        self.assertEqual({}, forbidden_identifiers(source))

    def test_legacy_read_results_and_native_lease_stay_in_the_store_adapter(self) -> None:
        source = (ROOT / "rocketmq-store" / "src" / "store_api_adapter.rs").read_text(encoding="utf-8")
        filter_source = (ROOT / "rocketmq-store" / "src" / "filter.rs").read_text(encoding="utf-8")
        request = item_body(source, "pub enum LegacyReadRequest")
        hidden_dyn_aliases = arc_dyn_aliases(filter_source)
        request_identifiers = set(re.findall(r"[A-Za-z_][A-Za-z0-9_]*", request))
        self.assertIn("impl<MS> MessageReader for LegacyMessageStoreReadAdapter", source)
        self.assertIn("pub(crate) trait LegacyReadCallBoundary", source)
        self.assertNotIn("pub trait LegacyReadCallBoundary", source)
        self.assertIn("impl<MS: MessageStore> LegacyReadCallBoundary for MS", source)
        self.assertIn("type Output = Option<LegacyReadResult>", source)
        self.assertIn("_selected: SelectMappedBufferResult", source)
        self.assertIn("map(selected_result_from_legacy)", source)
        self.assertNotIn("impl MessageStore for LegacyMessageStore", source)
        self.assertIn("pub enum LegacyReadRequest", source)
        self.assertNotIn("filter", request)
        self.assertEqual(set(), hidden_dyn_aliases & request_identifiers)
        self.assertNotIn("ArcMessageFilter", source)

    def test_arc_dyn_alias_detection_catches_hidden_hot_path_types(self) -> None:
        fixture = "pub type HiddenFilter = Arc<dyn Filter>; pub enum Request { Get(Option<HiddenFilter>) }"
        self.assertEqual({"HiddenFilter"}, arc_dyn_aliases(fixture))

    def test_compound_backend_identifiers_are_rejected(self) -> None:
        for identifier, token in COMPOUND_FORBIDDEN_FIXTURES.items():
            with self.subTest(identifier=identifier):
                self.assertEqual(token, forbidden_identifiers(f"pub struct {identifier};").get(identifier))

    def test_forbidden_token_priority_is_deterministic_and_specific_first(self) -> None:
        expected = tuple(sorted(FORBIDDEN_NORMALIZED_TOKENS, key=lambda token: (-len(token), token)))
        self.assertEqual(expected, FORBIDDEN_NORMALIZED_TOKENS)

    def test_short_ha_token_only_matches_an_identifier_segment(self) -> None:
        for identifier in ("HashMap", "HandleState", "Chart"):
            with self.subTest(identifier=identifier):
                self.assertEqual({}, forbidden_identifiers(f"pub struct {identifier};"))

    def test_real_broker_send_and_reject_paths_traverse_capabilities(self) -> None:
        manifest = tomllib.loads((ROOT / "rocketmq-broker" / "Cargo.toml").read_text(encoding="utf-8"))
        self.assertIn("rocketmq-store-api", manifest["dependencies"])
        source = (ROOT / "rocketmq-broker" / "src" / "processor" / "send_message_processor.rs").read_text(
            encoding="utf-8"
        )

        batch = function_body(source, "async fn send_batch_message")
        self.assertGreaterEqual(batch.count("append_message_with_store("), 2)
        self.assertNotIn(".put_message(", batch)
        self.assertNotIn(".put_messages(", batch)

        single = function_body(source, "async fn send_message<F>")
        self.assertGreaterEqual(single.count("append_message_with_store("), 2)
        self.assertNotIn(".put_message(", single)
        self.assertNotIn(".prepare_message(", single)
        self.assertIn("TransactionalMessageAppender::new", single)

        reject = function_body(source, "fn reject_request")
        self.assertIn("store_health_reject_remark_from", reject)
        self.assertNotIn(".health_snapshot(", reject)

        helper = function_body(source, "fn append_message_with_store")
        self.assertIn("store.append_message(message)", helper)
        self.assertNotIn("Box::pin", helper)


if __name__ == "__main__":
    unittest.main()
