// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Source contracts for the atomic Store capability cutover.

use std::fs;
use std::path::Path;

fn read(path: impl AsRef<Path>) -> String {
    fs::read_to_string(path.as_ref()).unwrap_or_else(|error| panic!("read {}: {error}", path.as_ref().display()))
}

#[test]
fn public_store_surface_exposes_capabilities_without_the_wide_facade() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let public_api = read(root.join("src/public_api.rs"));
    let store_ports = root.join("src/store_ports.rs");

    assert!(store_ports.is_file(), "the StorePorts composition root must exist");
    assert!(
        !public_api.contains("BackendOps"),
        "the internal BackendOps adapter must not remain in the intentional public API"
    );
    assert!(
        !root.join("src/base/message_store.rs").exists(),
        "the legacy broad backend trait file must be deleted"
    );
    assert!(
        public_api.contains("StorePorts"),
        "the selected StorePorts composition root must be intentional public API"
    );
}

#[test]
fn rocksdb_backend_has_no_local_backend_dependency() {
    let store_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let repository_root = store_root.parent().expect("repository root");
    let manifest = read(repository_root.join("rocketmq-store-rocksdb/Cargo.toml"));
    assert!(
        !manifest.contains("rocketmq-store-local"),
        "RocksDB must depend on neutral Store contracts, not the Local backend"
    );

    let source_root = repository_root.join("rocketmq-store-rocksdb/src");
    let mut pending = vec![source_root];
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display())) {
            let entry = entry.expect("RocksDB source entry");
            let entry_path = entry.path();
            if entry_path.is_dir() {
                pending.push(entry_path);
            } else if entry_path.extension().is_some_and(|extension| extension == "rs") {
                let source = read(&entry_path);
                assert!(
                    !source.contains("rocketmq_store_local"),
                    "{} directly imports the Local backend",
                    entry_path.display()
                );
            }
        }
    }
}

#[test]
fn store_factory_selects_backends_only_through_store_ports() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let factory = read(root.join("src/factory.rs"));
    let store_ports = read(root.join("src/store_ports.rs"));
    let legacy_root = ["Owned", "Message", "Store"].concat();

    assert!(factory.contains("StorePorts"));
    assert!(!factory.contains(&legacy_root));
    assert!(store_ports.contains("pub enum StorePorts"));
    assert!(store_ports.contains("LocalFileStore"));
    assert!(store_ports.contains("RocksDBStore"));
}

#[test]
fn broker_subsystems_declare_use_case_capability_bounds() {
    let store_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let broker_root = store_root
        .parent()
        .expect("repository root")
        .join("rocketmq-broker/src");
    let cases = [
        (
            "processor/send_message_processor.rs",
            "SendMessageProcessor<MS: BrokerWriteStore",
        ),
        (
            "processor/pull_message_processor.rs",
            "PullMessageProcessor<MS: BrokerReadStore",
        ),
        (
            "processor/pop_message_processor.rs",
            "PopMessageProcessor<MS: BrokerReadWriteStore",
        ),
        (
            "processor/admin_broker_processor.rs",
            "AdminBrokerProcessor<MS: BrokerAdminStore",
        ),
        (
            "failover/escape_bridge.rs",
            "impl<MS: BrokerReadStore> EscapeBridge<MS>",
        ),
    ];
    for (relative, expected) in cases {
        let source = read(broker_root.join(relative));
        assert!(source.contains(expected), "{relative} must declare {expected}");
    }

    let mut pending = vec![broker_root.clone()];
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display())) {
            let entry = entry.expect("Broker source entry");
            let entry_path = entry.path();
            if entry_path.is_dir() {
                pending.push(entry_path);
            } else if entry_path.extension().is_some_and(|extension| extension == "rs") {
                let source = read(&entry_path);
                assert!(
                    !source.contains("MS: BackendOps"),
                    "{} still binds a Broker subsystem directly to the backend implementation trait",
                    entry_path.display()
                );
            }
        }
    }

    let escape_bridge = read(broker_root.join("failover/escape_bridge.rs"));
    assert!(
        escape_bridge.contains("MS: BrokerReplicationStore"),
        "EscapeBridge replication operations must declare their replication capability"
    );
}
