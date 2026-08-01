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

//! Recovery-contract registration for every selectable Store backend.
//!
//! The heavy recovery scenarios remain separate test binaries so crash helpers
//! can safely terminate subprocesses. These guards prevent a backend from being
//! selectable through `StorePorts` without retaining its recovery suite.

const FACTORY: &str = include_str!("../src/factory.rs");
const STORE_PORTS: &str = include_str!("../src/store_ports.rs");
const LOCAL_RECOVERY: &str = include_str!("architecture_correctness.rs");
const ROCKSDB_RECOVERY: &str = include_str!("rocksdb_store_semantics_tests.rs");

#[test]
fn local_backend_selection_retains_crash_and_derived_recovery_contracts() {
    assert!(FACTORY.contains("StorePorts::local_file"));
    assert!(STORE_PORTS.contains("LocalFileStore"));
    assert!(LOCAL_RECOVERY.contains("sync_flush_crash_recovery"));
    assert!(LOCAL_RECOVERY.contains("derived_replay_no_holes"));
}

#[test]
fn rocksdb_backend_selection_retains_wal_and_offset_recovery_contracts() {
    assert!(FACTORY.contains("StorePorts::rocksdb"));
    assert!(STORE_PORTS.contains("RocksDBStore"));
    assert!(ROCKSDB_RECOVERY.contains("rocksdb_store_load_start_recover_round_trip"));
    assert!(ROCKSDB_RECOVERY.contains("rocksdb_recovery_skips_dirty_tail"));
    assert!(ROCKSDB_RECOVERY.contains("restart_reput_advances_the_single_local_wal_queue_offset"));
}

#[test]
fn both_backends_share_the_same_store_ports_lifecycle_boundary() {
    assert!(STORE_PORTS.contains("pub enum StorePorts"));
    assert!(FACTORY.contains("pub fn into_parts(self) -> (StorePorts"));
}
