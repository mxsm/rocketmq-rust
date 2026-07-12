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
LOCAL_CRATE = ROOT / "rocketmq-store-local"
STORE_CRATE = ROOT / "rocketmq-store"
LEAF_FILES = {
    "direct_io.rs",
    "flush_strategy.rs",
    "io_uring_impl.rs",
    "mapped_buffer.rs",
    "mapped_file_error.rs",
    "metrics.rs",
}
CANONICAL_ITEMS = {
    "DirectIoBuffer": "direct_io.rs",
    "DirectIoRequest": "direct_io.rs",
    "DirectIoValidationError": "direct_io.rs",
    "FlushStrategy": "flush_strategy.rs",
    "IoUringBackendStatus": "io_uring_impl.rs",
    "IoUringFallbackReason": "io_uring_impl.rs",
    "IoUringOpcodeSupport": "io_uring_impl.rs",
    "IoUringRuntimeCapability": "io_uring_impl.rs",
    "LinuxKernelVersion": "io_uring_impl.rs",
    "MappedBuffer": "mapped_buffer.rs",
    "MappedFileError": "mapped_file_error.rs",
    "MappedFileMetrics": "metrics.rs",
    "MappedFileResult": "mapped_file_error.rs",
}
FACADE_ROOT_ITEMS = {
    "DirectIoBuffer",
    "DirectIoRequest",
    "DirectIoValidationError",
    "FlushStrategy",
    "IoUringBackendStatus",
    "MappedBuffer",
    "MappedFileError",
    "MappedFileMetrics",
    "MappedFileResult",
}
FORBIDDEN_DEPENDENCIES = {
    "rocketmq-common",
    "rocketmq-rust",
    "rocketmq-remoting",
    "rocketmq-store",
    "rocketmq-broker",
    "rocksdb",
    "rocketmq-store-rocksdb",
    "rocketmq-tieredstore",
}
FORBIDDEN_SOURCE_TOKENS = (
    "rocketmq_common",
    "rocketmq_rust",
    "rocketmq_remoting",
    "rocketmq_store",
    "rocketmq_broker",
    "rocksdb",
    "rocketmq_store_rocksdb",
    "rocketmq_tieredstore",
)


def dependency_tables(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    tables = [
        manifest.get("dependencies", {}),
        manifest.get("build-dependencies", {}),
    ]
    for target in manifest.get("target", {}).values():
        tables.extend(
            [
                target.get("dependencies", {}),
                target.get("build-dependencies", {}),
            ]
        )
    return tables


class StoreLocalContractTests(unittest.TestCase):
    def assert_local_crate_exists(self) -> None:
        self.assertTrue(LOCAL_CRATE.is_dir(), "canonical rocketmq-store-local crate is missing")

    def test_workspace_and_feature_ownership_are_exact(self) -> None:
        self.assert_local_crate_exists()
        root_manifest = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
        self.assertIn("rocketmq-store-local", root_manifest["workspace"]["members"])
        workspace_dependency = root_manifest["workspace"]["dependencies"]["rocketmq-store-local"]
        self.assertFalse(workspace_dependency["default-features"])

        local_manifest = tomllib.loads((LOCAL_CRATE / "Cargo.toml").read_text(encoding="utf-8"))
        self.assertEqual(
            {
                "default": [],
                "fast-load": [],
                "safe-load": [],
                "io_uring": ["dep:tokio-uring"],
            },
            local_manifest["features"],
        )
        store_manifest = tomllib.loads((STORE_CRATE / "Cargo.toml").read_text(encoding="utf-8"))
        self.assertEqual(["local_file_store", "fast-load"], store_manifest["features"]["default"])
        self.assertEqual(["rocketmq-store-local/fast-load"], store_manifest["features"]["fast-load"])
        self.assertEqual(["rocketmq-store-local/safe-load"], store_manifest["features"]["safe-load"])
        self.assertEqual(["rocketmq-store-local/io_uring"], store_manifest["features"]["io_uring"])
        self.assertIn("rocketmq-store-local", store_manifest["dependencies"])
        self.assertNotIn("tokio-uring", store_manifest.get("target", {}).get("cfg(target_os = \"linux\")", {}).get("dependencies", {}))

    def test_local_manifest_and_sources_have_no_forbidden_owner_edges(self) -> None:
        self.assert_local_crate_exists()
        manifest = tomllib.loads((LOCAL_CRATE / "Cargo.toml").read_text(encoding="utf-8"))
        dependencies = {
            alias
            for table in dependency_tables(manifest)
            for alias in table
        }
        self.assertTrue(FORBIDDEN_DEPENDENCIES.isdisjoint(dependencies), dependencies)

        findings: list[str] = []
        for path in sorted((LOCAL_CRATE / "src").rglob("*.rs")):
            source = path.read_text(encoding="utf-8")
            source = re.sub(r"/\*.*?\*/", " ", source, flags=re.DOTALL)
            source = re.sub(r"//[^\r\n]*", " ", source)
            source = re.sub(r'(?s)(?:br|r|b)?(?:#+)?".*?"(?:#+)?', " ", source)
            for token in FORBIDDEN_SOURCE_TOKENS:
                if re.search(rf"\b{re.escape(token)}\b", source):
                    findings.append(f"{path.relative_to(ROOT)}: {token}")
        self.assertEqual([], findings)

    def test_six_leaf_files_have_one_canonical_definition_and_facade_reexports(self) -> None:
        self.assert_local_crate_exists()
        canonical_dir = LOCAL_CRATE / "src" / "mapped_file"
        facade_dir = STORE_CRATE / "src" / "log_file" / "mapped_file"
        self.assertEqual(LEAF_FILES, {path.name for path in canonical_dir.glob("*.rs")})
        self.assertTrue(all(not (facade_dir / name).exists() for name in LEAF_FILES))

        rust_sources = list(ROOT.glob("rocketmq-*/src/**/*.rs"))
        for item, expected_file in CANONICAL_ITEMS.items():
            pattern = re.compile(rf"\bpub\s+(?:struct|enum|type)\s+{item}\b")
            definitions = [path for path in rust_sources if pattern.search(path.read_text(encoding="utf-8"))]
            self.assertEqual([canonical_dir / expected_file], definitions, item)

        facade = (STORE_CRATE / "src" / "log_file" / "mapped_file.rs").read_text(encoding="utf-8")
        for item in FACADE_ROOT_ITEMS:
            self.assertIn(f"pub use rocketmq_store_local::mapped_file::{item};", facade)
        self.assertIn("pub use rocketmq_store_local::mapped_file::io_uring_impl;", facade)


if __name__ == "__main__":
    unittest.main()
