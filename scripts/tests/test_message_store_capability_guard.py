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

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
GUARD = REPO_ROOT / "scripts" / "message_store_capability_guard.py"


class StoreCapabilityCutoverGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="store-capability-cutover-")
        self.root = Path(self.temporary.name)
        self.write(
            "rocketmq-store/src/store_ports.rs",
            "pub enum StorePorts { LocalFileStore, RocksDBStore }",
        )
        self.write(
            "rocketmq-store/src/factory.rs",
            "StorePorts::local_file(local); StorePorts::rocksdb(rocks);",
        )
        self.write(
            "rocketmq-store/src/public_api.rs",
            "pub use crate::store_ports::StorePorts;",
        )
        self.write("rocketmq-store/src/base.rs", "pub(crate) mod backend_ops;")
        self.write("rocketmq-store/src/base/backend_ops.rs", "pub trait BackendOps {}")
        self.write(
            "rocketmq-store/src/capability.rs",
            """
pub trait BrokerReadStore {
    fn read(&self);
}
pub trait BrokerWriteStore: BrokerReadStore {
    fn write(&self);
}
pub trait BrokerMasterAddressStore {
    fn update_master_address(&self);
}
pub trait BrokerAdminStore {
    fn administer(&self);
}
pub trait BrokerReplicationStore {
    fn replicate(&self);
}
pub trait BrokerStorePort {
    fn start(&self);
}
""",
        )
        self.write(
            "rocketmq-store-api/src/lib.rs",
            "pub trait MessageReader {}",
        )
        self.write("rocketmq-store-rocksdb/Cargo.toml", "[dependencies]\nrocketmq-store-api = \"1\"")
        self.write("rocketmq-store-rocksdb/src/lib.rs", "pub struct RocksDbStore;")
        boundaries = {
            "processor/send_message_processor.rs": "SendMessageProcessor<MS: BrokerWriteStore",
            "processor/pull_message_processor.rs": "PullMessageProcessor<MS: BrokerReadStore",
            "processor/pop_message_processor.rs": "PopMessageProcessor<MS: BrokerReadWriteStore",
            "processor/admin_broker_processor.rs": "AdminBrokerProcessor<MS: BrokerAdminStore",
            "failover/escape_bridge.rs": (
                "impl<MS: BrokerReadStore> EscapeBridge<MS> {} "
                "impl<MS> EscapeBridge<MS> where MS: BrokerReplicationStore {}"
            ),
        }
        for path, source in boundaries.items():
            self.write(f"rocketmq-broker/src/{path}", source)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def write(self, relative: str, source: str) -> None:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source, encoding="utf-8")

    def run_guard(self) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(GUARD), "--root", str(self.root)],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )

    def test_complete_cutover_fixture_passes(self) -> None:
        result = self.run_guard()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("legacy_identifiers=0", result.stdout)
        self.assertIn("rocksdb_local_refs=0", result.stdout)

    def test_legacy_wide_identifier_fails(self) -> None:
        self.write(
            "rocketmq-broker/src/legacy.rs",
            "use rocketmq_store::MessageStore;",
        )
        result = self.run_guard()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("legacy wide Store identifier remains", result.stderr)

    def test_direct_backend_bound_fails(self) -> None:
        self.write(
            "rocketmq-broker/src/direct.rs",
            "fn process<MS: BackendOps>(store: &MS) {}",
        )
        result = self.run_guard()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("directly binds BackendOps", result.stderr)

    def test_rocksdb_local_dependency_fails(self) -> None:
        self.write(
            "rocketmq-store-rocksdb/Cargo.toml",
            "[dependencies]\nrocketmq-store-local = \"1\"",
        )
        result = self.run_guard()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("RocksDB manifest directly depends", result.stderr)

    def test_missing_use_case_capability_fails(self) -> None:
        self.write(
            "rocketmq-broker/src/processor/send_message_processor.rs",
            "SendMessageProcessor<MS: BrokerStorePort",
        )
        result = self.run_guard()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Broker capability boundary is missing", result.stderr)

    def test_empty_capability_marker_fails(self) -> None:
        self.write(
            "rocketmq-store/src/capability.rs",
            """
pub trait BrokerReadStore {}
pub trait BrokerWriteStore { fn write(&self); }
pub trait BrokerMasterAddressStore { fn update_master_address(&self); }
pub trait BrokerAdminStore { fn administer(&self); }
pub trait BrokerReplicationStore { fn replicate(&self); }
pub trait BrokerStorePort { fn start(&self); }
""",
        )
        result = self.run_guard()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must declare real operations", result.stderr)


class RepositoryStoreCapabilityCutoverContracts(unittest.TestCase):
    def test_repository_cutover_is_complete(self) -> None:
        result = subprocess.run(
            [sys.executable, str(GUARD), "--root", str(REPO_ROOT)],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("legacy_identifiers=0", result.stdout)
        self.assertIn("subsystem_capabilities=5", result.stdout)


if __name__ == "__main__":
    unittest.main()
