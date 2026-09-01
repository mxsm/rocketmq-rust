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

#[cfg(target_os = "linux")]
use std::fs;
#[cfg(not(windows))]
use std::fs::File;

#[cfg(target_os = "linux")]
use crate::mapped_file::retirement::bootstrap::inventory::preflight_bootstrap_namespace;
#[cfg(not(windows))]
use crate::mapped_file::retirement::bootstrap::inventory::scan_bootstrap_inventory;
#[cfg(not(windows))]
use crate::mapped_file::retirement::bootstrap::inventory::BootstrapInventoryLimits;
#[cfg(target_os = "linux")]
use crate::mapped_file::retirement::sidecar::IncarnationPhase;
#[cfg(target_os = "linux")]
use crate::mapped_file::retirement::sidecar::SnapshotEntry;
#[cfg(target_os = "linux")]
use crate::mapped_file::retirement::sidecar::StoreMeta;

#[cfg(target_os = "linux")]
use super::bootstrap_managed_lifecycle_under_exclusive_lock;
#[cfg(target_os = "linux")]
use super::support::store_uuid;
#[cfg(target_os = "linux")]
fn meta() -> StoreMeta {
    StoreMeta {
        store_uuid: store_uuid(),
        creation_time_ns: 17,
        bootstrap_id: [0x61; 16],
    }
}

#[cfg(target_os = "linux")]
#[test]
fn recursive_inventory_is_complete_canonical_and_retains_numeric_segments() {
    let root = tempfile::tempdir().expect("temporary Store root");
    fs::create_dir(root.path().join("commitlog")).expect("commitlog directory");
    fs::create_dir_all(root.path().join("consumequeue/topic/0")).expect("consume queue directory");
    fs::create_dir(root.path().join(".rocketmq-lifecycle")).expect("lifecycle directory");
    fs::write(root.path().join("commitlog/00000000000000000000"), [1_u8; 16]).expect("first segment");
    fs::write(root.path().join("commitlog/00000000000000000016"), [2_u8; 16]).expect("second segment");
    fs::write(root.path().join("consumequeue/topic/0/00000000000000000000"), [3_u8; 8]).expect("consume queue segment");
    fs::write(root.path().join("checkpoint"), b"ignored non-segment").expect("ordinary Store file");
    fs::write(
        root.path()
            .join(".rocketmq-lifecycle/retirement.log.g00000000000000000000"),
        b"not a legacy segment",
    )
    .expect("lifecycle log");

    let root_file = File::open(root.path()).expect("open root");
    let inventory = scan_bootstrap_inventory(&root_file, &meta(), BootstrapInventoryLimits::default())
        .expect("stable complete inventory");
    let snapshot = inventory.snapshot_for_test();

    assert_eq!(snapshot.entries.len(), 3);
    assert_eq!(snapshot.create_high_water, 3);
    assert_eq!(snapshot.ticket_high_water, 0);
    assert_eq!(inventory.retained_file_count_for_test(), 3);
    let paths = snapshot
        .entries
        .iter()
        .map(|entry| match entry {
            SnapshotEntry::Incarnation(entry) => {
                assert_eq!(entry.phase, IncarnationPhase::Published);
                assert!(entry.physical_key.is_some());
                assert_ne!(entry.create_nonce, [0; 16]);
                entry.canonical_path.as_bytes().to_owned()
            }
            _ => panic!("bootstrap inventory contains only incarnations"),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        paths,
        [
            b"commitlog/00000000000000000000".to_vec(),
            b"commitlog/00000000000000000016".to_vec(),
            b"consumequeue/topic/0/00000000000000000000".to_vec(),
        ]
    );
}

#[cfg(target_os = "linux")]
#[test]
fn inventory_rejects_hardlinks_and_zero_length_segments() {
    use std::os::unix::fs::symlink;

    let hardlink = tempfile::tempdir().expect("hardlink root");
    fs::create_dir(hardlink.path().join("commitlog")).expect("commitlog directory");
    let segment = hardlink.path().join("commitlog/00000000000000000000");
    fs::write(&segment, [1_u8; 16]).expect("segment");
    fs::hard_link(&segment, hardlink.path().join("alias")).expect("hard link");
    let error = scan_bootstrap_inventory(
        &File::open(hardlink.path()).expect("open hardlink root"),
        &meta(),
        BootstrapInventoryLimits::default(),
    )
    .expect_err("external hardlink aliases are unsafe");
    assert_eq!(error.category_for_test(), "unsafe-namespace");

    let empty = tempfile::tempdir().expect("empty root");
    fs::create_dir(empty.path().join("commitlog")).expect("commitlog directory");
    fs::write(empty.path().join("commitlog/00000000000000000000"), []).expect("empty segment");
    let error = scan_bootstrap_inventory(
        &File::open(empty.path()).expect("open empty root"),
        &meta(),
        BootstrapInventoryLimits::default(),
    )
    .expect_err("zero-length segments are not active incarnations");
    assert_eq!(error.category_for_test(), "invalid-segment");

    let linked = tempfile::tempdir().expect("symlink root");
    fs::create_dir(linked.path().join("commitlog")).expect("commitlog directory");
    symlink("commitlog", linked.path().join("hidden")).expect("directory symlink");
    let error = scan_bootstrap_inventory(
        &File::open(linked.path()).expect("open symlink root"),
        &meta(),
        BootstrapInventoryLimits::default(),
    )
    .expect_err("a symlink can hide additional numeric segments");
    assert_eq!(error.category_for_test(), "unsafe-namespace");
}

#[cfg(target_os = "linux")]
#[test]
fn unsupported_numeric_store_files_are_rejected_before_bootstrap_artifacts_exist() {
    let root = tempfile::tempdir().expect("temporary Store root");
    fs::create_dir(root.path().join("timerlog")).expect("timer log directory");
    fs::write(root.path().join("timerlog/00000000000000000000"), [1_u8; 16]).expect("timer segment");
    fs::write(root.path().join("checkpoint"), b"ordinary Store file").expect("checkpoint");

    let error = preflight_bootstrap_namespace(
        &File::open(root.path()).expect("open Store root"),
        BootstrapInventoryLimits::default(),
    )
    .expect_err("numeric files outside mapped-file queues are not lifecycle incarnations");

    assert_eq!(error.category_for_test(), "invalid-segment");
    assert!(!root.path().join(".rocketmq-lifecycle").exists());
}

#[cfg(target_os = "linux")]
#[test]
fn public_bootstrap_entry_runs_namespace_preflight_before_any_write() {
    let root = tempfile::tempdir().expect("temporary Store root");
    fs::create_dir(root.path().join("timerlog")).expect("timer log directory");
    fs::write(root.path().join("timerlog/00000000000000000000"), [1_u8; 16]).expect("timer segment");
    let root_file = File::open(root.path()).expect("open Store root");

    // SAFETY: the temporary root is private to this test and no Store component is running.
    let error = unsafe { bootstrap_managed_lifecycle_under_exclusive_lock(&root_file) }
        .expect_err("unsupported numeric files must block bootstrap before mutation");

    assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_READ_FAILED);
    assert!(!root.path().join(".rocketmq-lifecycle").exists());
}

#[cfg(target_os = "linux")]
#[test]
fn mixed_queue_lengths_are_rejected_before_bootstrap_artifacts_exist() {
    let root = tempfile::tempdir().expect("temporary Store root");
    fs::create_dir(root.path().join("commitlog")).expect("commit log directory");
    fs::write(root.path().join("commitlog/00000000000000000000"), [1_u8; 16]).expect("first segment");
    fs::write(root.path().join("commitlog/00000000000000000016"), [2_u8; 8]).expect("second segment");
    let root_file = File::open(root.path()).expect("open Store root");

    // SAFETY: the temporary root is private to this test and no Store component is running.
    let error = unsafe { bootstrap_managed_lifecycle_under_exclusive_lock(&root_file) }
        .expect_err("mixed queue lengths must block bootstrap before mutation");

    assert_eq!(error.descriptor(), &rocketmq_error::STORAGE_READ_FAILED);
    assert!(!root.path().join(".rocketmq-lifecycle").exists());
}

#[cfg(not(any(target_os = "linux", windows)))]
#[test]
fn bootstrap_inventory_is_unsupported_without_a_qualified_writer_platform() {
    let root = tempfile::tempdir().expect("temporary Store root");
    let root_file = File::open(root.path()).expect("open Store root");

    let error = scan_bootstrap_inventory(&root_file, &meta(), BootstrapInventoryLimits::default())
        .expect_err("unsupported platforms cannot mint bootstrap inventory evidence");
    assert_eq!(error.category_for_test(), "unsupported-platform");
}
