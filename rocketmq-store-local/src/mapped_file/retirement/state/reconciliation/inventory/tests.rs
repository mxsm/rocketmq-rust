// Copyright 2026 The RocketMQ Rust Authors
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

use std::fs;
use std::fs::File;
use std::io;
use std::path::Path;

use tempfile::tempdir;

use super::*;
use crate::mapped_file::retirement::codec::crc32;
use crate::mapped_file::retirement::codec::QuarantineEntityKind;
use crate::mapped_file::retirement::codec::QuarantineReason;
use crate::mapped_file::retirement::identity::FileIncarnationId;
use crate::mapped_file::retirement::identity::PhysicalFileKey;
use crate::mapped_file::retirement::platform::physical_file_key;
use crate::mapped_file::retirement::sidecar::IncarnationPhase;
use crate::mapped_file::retirement::sidecar::IncarnationSnapshotEntry;
use crate::mapped_file::retirement::sidecar::LifecycleSnapshot;
use crate::mapped_file::retirement::sidecar::QuarantineSnapshotEntry;
use crate::mapped_file::retirement::sidecar::SnapshotEntry;
use crate::mapped_file::retirement::sidecar::SnapshotMode;
use crate::mapped_file::retirement::state::reconciliation::reconcile;
use crate::mapped_file::retirement::state::reconciliation::ReconciliationDisposition;
use crate::mapped_file::retirement::state::LedgerStateMachine;
use crate::mapped_file::retirement::state::NeedsReconciliation;

#[test]
fn retained_root_scans_nested_unicode_directory_with_exact_a_b_c_inventory() {
    let directory = tempdir().expect("create temporary Store root");
    let parent = directory.path().join("commitlog").join("主题");
    fs::create_dir_all(&parent).expect("create nested segment directory");
    let canonical = parent.join("00000000000000000000");
    let file = File::create(&canonical).expect("create segment");
    file.set_len(1_024).expect("size segment");
    let key = physical_file_key(&file).expect("capture physical key");
    drop(file);

    let incarnation = incarnation("commitlog/主题/00000000000000000000", key);
    let needs = recovered(vec![SnapshotEntry::Incarnation(incarnation.clone())]);
    let root = open_root(directory.path()).expect("open retained root");
    let inventory = scan(&root, store_uuid(), needs.recovered(), limits()).expect("scan stable namespace");

    let ReconciliationDisposition::Ready(ready) = reconcile(needs, inventory).expect("reconcile exact segment") else {
        panic!("exact stable inventory must be ready");
    };
    assert_eq!(
        ready
            .active_incarnation(&incarnation.canonical_path)
            .expect("active binding")
            .physical_key(),
        key
    );
}

#[test]
fn external_hard_link_is_rejected_before_reconciliation() {
    let directory = tempdir().expect("create temporary Store root");
    let parent = directory.path().join("commitlog");
    fs::create_dir_all(&parent).expect("create segment directory");
    let canonical = parent.join("00000000000000000000");
    let alias = parent.join("00000000000000001024");
    let file = File::create(&canonical).expect("create segment");
    file.set_len(1_024).expect("size segment");
    let key = physical_file_key(&file).expect("capture physical key");
    drop(file);
    fs::hard_link(&canonical, &alias).expect("create hard-link alias");

    let incarnation = incarnation("commitlog/00000000000000000000", key);
    let needs = recovered(vec![SnapshotEntry::Incarnation(incarnation)]);
    let root = open_root(directory.path()).expect("open retained root");
    assert!(matches!(
        scan(&root, store_uuid(), needs.recovered(), limits()),
        Err(ReconciliationInventoryFailure::HardLinkAlias { .. })
    ));
}

#[test]
fn quarantine_fingerprint_is_read_positionally_and_bound_to_the_retained_handle() {
    let directory = tempdir().expect("create temporary Store root");
    let parent = directory.path().join("quarantine");
    fs::create_dir_all(&parent).expect("create quarantine directory");
    let quarantined = parent.join("unknown.bin");
    let bytes = b"retained quarantine bytes";
    fs::write(&quarantined, bytes).expect("write quarantined content");
    let file = File::open(&quarantined).expect("open quarantined file");
    let key = physical_file_key(&file).expect("capture quarantine key");
    drop(file);
    let source_path = StoreRelativePath::new("quarantine/unknown.bin").expect("canonical path");
    let needs = recovered(vec![SnapshotEntry::Quarantine(QuarantineSnapshotEntry {
        entity_kind: QuarantineEntityKind::Canonical,
        reason: QuarantineReason::UnknownOwner,
        sequence_at_observation: 10,
        physical_key: Some(key),
        content_fingerprint: Some(ContentFingerprint {
            length: bytes.len() as u64,
            crc32: crc32(bytes),
        }),
        source_path,
        destination_path: None,
    })]);
    let root = open_root(directory.path()).expect("open retained root");
    let inventory = scan(&root, store_uuid(), needs.recovered(), limits()).expect("scan quarantine");

    assert!(matches!(
        reconcile(needs, inventory).expect("reconcile exact fingerprint"),
        ReconciliationDisposition::Ready(_)
    ));
}

#[test]
fn missing_parent_and_zero_limits_fail_closed() {
    let directory = tempdir().expect("create temporary Store root");
    let key = PhysicalFileKey::unix(7, 9);
    let needs = recovered(vec![SnapshotEntry::Incarnation(incarnation(
        "commitlog/00000000000000000000",
        key,
    ))]);
    let root = open_root(directory.path()).expect("open retained root");
    assert!(matches!(
        scan(&root, store_uuid(), needs.recovered(), limits()),
        Err(ReconciliationInventoryFailure::MissingDirectory { .. })
    ));
    assert!(matches!(
        scan(
            &root,
            store_uuid(),
            needs.recovered(),
            ReconciliationInventoryLimits {
                max_directories: 0,
                ..limits()
            },
        ),
        Err(ReconciliationInventoryFailure::InvalidLimits)
    ));
}

fn limits() -> ReconciliationInventoryLimits {
    ReconciliationInventoryLimits {
        max_directories: 8,
        max_entries: 32,
        max_fingerprint_bytes: 4_096,
    }
}

fn recovered(entries: Vec<SnapshotEntry>) -> NeedsReconciliation {
    let state = LedgerStateMachine::from_snapshot(LifecycleSnapshot {
        mode: SnapshotMode::OrdinaryCompaction,
        store_uuid: store_uuid(),
        generation: 1,
        log_generation: 1,
        predecessor_log_generation: 0,
        base_sequence: 10,
        create_high_water: entries
            .iter()
            .filter_map(|entry| match entry {
                SnapshotEntry::Incarnation(entry) => Some(entry.incarnation.create_seq()),
                _ => None,
            })
            .max()
            .unwrap_or(0),
        ticket_high_water: 0,
        entries,
    })
    .expect("test snapshot is valid")
    .finish(10, 1, 1)
    .expect("test replay epochs are nonzero");
    NeedsReconciliation::for_test(state)
}

fn incarnation(canonical: &str, key: PhysicalFileKey) -> IncarnationSnapshotEntry {
    let canonical_path = StoreRelativePath::new(canonical).expect("canonical segment path");
    let (parent, _) = canonical.rsplit_once('/').expect("test segment has parent");
    IncarnationSnapshotEntry {
        incarnation: FileIncarnationId::new(store_uuid(), 1).expect("test incarnation is nonzero"),
        phase: IncarnationPhase::Published,
        segment_offset: 0,
        expected_file_length: 1_024,
        create_nonce: [0x11; 16],
        physical_key: Some(key),
        canonical_path,
        create_file_path: StoreRelativePath::new(&format!(
            "{parent}/.create.i0000000000000001.s00000000000000000000.n{}",
            "11".repeat(16)
        ))
        .expect("canonical create path"),
    }
}

fn store_uuid() -> StoreUuid {
    StoreUuid::new([1; 16]).expect("test UUID is nonzero")
}

#[cfg(windows)]
fn open_root(path: &Path) -> io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt;

    use windows::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS;

    fs::OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS.0)
        .open(path)
}

#[cfg(not(windows))]
fn open_root(path: &Path) -> io::Result<File> {
    File::open(path)
}
