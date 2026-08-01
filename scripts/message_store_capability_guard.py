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

"""Enforce the completed Store capability cutover."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
STORE_PORTS = Path("rocketmq-store/src/store_ports.rs")
STORE_FACTORY = Path("rocketmq-store/src/factory.rs")
PUBLIC_API = Path("rocketmq-store/src/public_api.rs")
BASE_MODULE = Path("rocketmq-store/src/base.rs")
BACKEND_OPS = Path("rocketmq-store/src/base/backend_ops.rs")
LEGACY_BACKEND = Path("rocketmq-store/src/base/message_store.rs")
CAPABILITIES = Path("rocketmq-store/src/capability.rs")
BROKER_ROOT = Path("rocketmq-broker/src")
ROCKSDB_ROOT = Path("rocketmq-store-rocksdb")

SUBSYSTEM_CAPABILITIES = {
    Path("processor/send_message_processor.rs"): "SendMessageProcessor<MS: BrokerWriteStore",
    Path("processor/pull_message_processor.rs"): "PullMessageProcessor<MS: BrokerReadStore",
    Path("processor/pop_message_processor.rs"): "PopMessageProcessor<MS: BrokerReadWriteStore",
    Path("processor/admin_broker_processor.rs"): "AdminBrokerProcessor<MS: BrokerAdminStore",
    Path("failover/escape_bridge.rs"): "impl<MS: BrokerReadStore> EscapeBridge<MS>",
}


def rust_sources(root: Path) -> list[Path]:
    sources: list[Path] = []
    for crate in sorted(root.glob("rocketmq-*")):
        if not crate.is_dir():
            continue
        sources.extend(path for path in crate.rglob("*.rs") if "target" not in path.parts)
    return sorted(sources)


def read(root: Path, relative: Path) -> str:
    return (root / relative).read_text(encoding="utf-8")


def validate(root: Path) -> list[str]:
    findings: list[str] = []
    legacy_identifier = re.compile(r"\bMessage" + r"Store\b")
    compatibility_names = (
        "Legacy" + "MessageStore",
        "Deprecated" + "MessageStore",
    )

    for path in rust_sources(root):
        source = path.read_text(encoding="utf-8")
        if legacy_identifier.search(source):
            findings.append(f"legacy wide Store identifier remains in {path.relative_to(root).as_posix()}")
        for name in compatibility_names:
            if name in source:
                findings.append(f"compatibility Store facade {name} remains in {path.relative_to(root).as_posix()}")

    try:
        store_ports = read(root, STORE_PORTS)
        factory = read(root, STORE_FACTORY)
        public_api = read(root, PUBLIC_API)
        base_module = read(root, BASE_MODULE)
        backend_ops = read(root, BACKEND_OPS)
        capabilities = read(root, CAPABILITIES)
    except OSError as error:
        findings.append(f"Store composition source is missing: {error}")
        return findings

    if "pub enum StorePorts" not in store_ports:
        findings.append("StorePorts must be the closed selected-backend composition root")
    for variant in ("LocalFileStore", "RocksDBStore"):
        if variant not in store_ports:
            findings.append(f"StorePorts is missing backend variant {variant}")
    for constructor in ("StorePorts::local_file", "StorePorts::rocksdb"):
        if factory.count(constructor) != 1:
            findings.append(f"Store factory must select {constructor} exactly once")
    if "pub use crate::store_ports::StorePorts;" not in public_api:
        findings.append("StorePorts is missing from the intentional public API")
    if "BackendOps" in public_api:
        findings.append("the backend implementation trait leaked into the intentional public API")
    if (root / LEGACY_BACKEND).exists():
        findings.append("the legacy base/message_store.rs backend trait file still exists")
    if "pub(crate) mod backend_ops;" not in base_module:
        findings.append("the backend implementation adapter must remain crate-private")
    if "pub trait BackendOps" not in backend_ops:
        findings.append("the private backend implementation adapter is missing")
    for capability in (
        "BrokerReadStore",
        "BrokerWriteStore",
        "BrokerMasterAddressStore",
        "BrokerAdminStore",
        "BrokerReplicationStore",
        "BrokerStorePort",
    ):
        declaration = re.search(
            rf"pub trait {capability}\b[^{{]*\{{(?P<body>.*?)\n\}}",
            capabilities,
            re.DOTALL,
        )
        if declaration is None or "fn " not in declaration.group("body"):
            findings.append(f"{capability} must declare real operations, not be an empty marker")
    if re.search(r"pub trait Broker\w+Store\s*:\s*BackendOps\b", capabilities):
        findings.append("a Broker capability directly extends the broad backend adapter")

    broker_root = root / BROKER_ROOT
    if broker_root.is_dir():
        direct_bound = re.compile(r"\bMS\s*:\s*BackendOps\b")
        for path in sorted(broker_root.rglob("*.rs")):
            if direct_bound.search(path.read_text(encoding="utf-8")):
                findings.append(
                    f"Broker subsystem directly binds BackendOps in {path.relative_to(root).as_posix()}"
                )
        for relative, expected in SUBSYSTEM_CAPABILITIES.items():
            path = broker_root / relative
            if not path.is_file() or expected not in path.read_text(encoding="utf-8"):
                findings.append(f"Broker capability boundary is missing: {relative.as_posix()} -> {expected}")
        escape_bridge = read(root, BROKER_ROOT / "failover/escape_bridge.rs")
        if "MS: BrokerReplicationStore" not in escape_bridge:
            findings.append("EscapeBridge replication operations are missing their explicit capability bound")

    rocksdb_manifest = root / ROCKSDB_ROOT / "Cargo.toml"
    rocksdb_sources = root / ROCKSDB_ROOT / "src"
    if not rocksdb_manifest.is_file():
        findings.append("rocketmq-store-rocksdb/Cargo.toml is missing")
    elif "rocketmq-store-local" in rocksdb_manifest.read_text(encoding="utf-8"):
        findings.append("RocksDB manifest directly depends on the Local Store backend")
    if rocksdb_sources.is_dir():
        for path in sorted(rocksdb_sources.rglob("*.rs")):
            if "rocketmq_store_local" in path.read_text(encoding="utf-8"):
                findings.append(f"RocksDB source directly imports Local Store in {path.relative_to(root).as_posix()}")

    store_api_root = root / "rocketmq-store-api" / "src"
    if store_api_root.is_dir():
        for path in sorted(store_api_root.rglob("*.rs")):
            if "async_trait" in path.read_text(encoding="utf-8"):
                findings.append(f"store-api uses async_trait in {path.relative_to(root).as_posix()}")
    return findings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=ROOT)
    return parser.parse_args()


def main() -> int:
    root = parse_args().root.resolve()
    findings = validate(root)
    if findings:
        for finding in findings:
            print(f"message-store-capability-guard: {finding}", file=sys.stderr)
        return 1
    broker_paths = sum(1 for _ in (root / BROKER_ROOT).rglob("*.rs"))
    print(
        "message-store-capability-guard: "
        f"legacy_identifiers=0 broker_backend_bounds=0 rocksdb_local_refs=0 "
        f"subsystem_capabilities={len(SUBSYSTEM_CAPABILITIES)} broker_source_files={broker_paths}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
