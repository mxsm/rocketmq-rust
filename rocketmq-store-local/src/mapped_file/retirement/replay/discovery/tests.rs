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

use std::collections::BTreeMap;
use std::fs;
use std::fs::File;
use std::io;
use std::path::Path;

use super::quarantine::TailEvidenceCoordinates;
use super::*;
use crate::mapped_file::retirement::codec::crc32;
use crate::mapped_file::retirement::codec::encode_acknowledgement_slot;
use crate::mapped_file::retirement::codec::encode_commit_seal;
use crate::mapped_file::retirement::codec::encode_ledger_frame;
use crate::mapped_file::retirement::codec::AcknowledgementSlot;
use crate::mapped_file::retirement::codec::CommitSeal;
use crate::mapped_file::retirement::codec::LedgerRecord;
use crate::mapped_file::retirement::codec::OpenReason;
use crate::mapped_file::retirement::identity::StoreUuid;
use crate::mapped_file::retirement::sidecar::encode_enabled_marker_file;
use crate::mapped_file::retirement::sidecar::encode_enabled_marker_slot;
use crate::mapped_file::retirement::sidecar::encode_snapshot;
use crate::mapped_file::retirement::sidecar::encode_store_meta;
use crate::mapped_file::retirement::sidecar::EnabledMarkerFile;
use crate::mapped_file::retirement::sidecar::EnabledMarkerSlot;
use crate::mapped_file::retirement::sidecar::LifecycleSnapshot;
use crate::mapped_file::retirement::sidecar::SnapshotMode;
use crate::mapped_file::retirement::sidecar::StoreMeta;

#[path = "tests/bootstrap.rs"]
mod bootstrap_cases;
#[path = "tests/quarantine.rs"]
mod quarantine_cases;

#[test]
fn absent_lifecycle_directory_is_legacy_without_writes() {
    let root = tempfile::tempdir().expect("temporary Store root");
    let before = tree_bytes(root.path());
    let handle = open_root(root.path()).expect("Store-root handle");

    assert_eq!(
        inspect_managed_lifecycle_read_only(&handle).expect("legacy inspection succeeds"),
        ManagedLifecycleReadOutcome::LegacyAbsent
    );
    assert_eq!(tree_bytes(root.path()), before);
}

#[test]
fn complete_generation_inventory_replays_and_performs_zero_writes() {
    let fixture = DiskFixture::new();
    let before = tree_bytes(fixture.root.path());
    let handle = open_root(fixture.root.path()).expect("Store-root handle");

    assert_eq!(
        inspect_managed_lifecycle_read_only(&handle).expect("managed inspection succeeds"),
        ManagedLifecycleReadOutcome::ManagedNeedsReconciliation
    );
    assert_eq!(tree_bytes(fixture.root.path()), before);
}

#[test]
fn missing_generation_half_and_unexplained_higher_pair_fail_closed() {
    let missing_half = DiskFixture::new();
    fs::remove_file(missing_half.lifecycle().join(log_name(0))).expect("remove log half");
    let handle = open_root(missing_half.root.path()).expect("Store-root handle");
    assert_eq!(
        inspect_managed_lifecycle_read_only(&handle)
            .expect_err("half pair must fail")
            .kind(),
        ManagedLifecycleReadErrorKind::Corruption
    );

    let higher = DiskFixture::new();
    fs::copy(
        higher.lifecycle().join(snapshot_name(0)),
        higher.lifecycle().join(snapshot_name(2)),
    )
    .expect("copy higher snapshot");
    fs::copy(
        higher.lifecycle().join(log_name(0)),
        higher.lifecycle().join(log_name(2)),
    )
    .expect("copy higher log");
    let handle = open_root(higher.root.path()).expect("Store-root handle");
    assert_eq!(
        inspect_managed_lifecycle_read_only(&handle)
            .expect_err("unexplained higher pair must fail")
            .kind(),
        ManagedLifecycleReadErrorKind::Corruption
    );
}

#[test]
fn unknown_version_truncation_and_crc_corruption_are_distinct() {
    let unknown = DiskFixture::new();
    let marker_path = unknown.lifecycle().join("ENABLED.v1");
    let mut marker = fs::read(&marker_path).expect("read marker");
    marker[4..6].copy_from_slice(&2_u16.to_le_bytes());
    fs::write(&marker_path, marker).expect("write unknown marker version");
    let handle = open_root(unknown.root.path()).expect("Store-root handle");
    assert_eq!(
        inspect_managed_lifecycle_read_only(&handle)
            .expect_err("unknown version must fail")
            .kind(),
        ManagedLifecycleReadErrorKind::UnknownVersionCorruption
    );

    for corrupt in [Corruption::Truncated, Corruption::Checksum] {
        let fixture = DiskFixture::new();
        let snapshot_path = fixture.lifecycle().join(snapshot_name(0));
        let mut snapshot = fs::read(&snapshot_path).expect("read snapshot");
        match corrupt {
            Corruption::Truncated => snapshot.truncate(snapshot.len() - 1),
            Corruption::Checksum => *snapshot.last_mut().expect("snapshot CRC") ^= 1,
        }
        fs::write(snapshot_path, snapshot).expect("write corrupt snapshot");
        let handle = open_root(fixture.root.path()).expect("Store-root handle");
        assert_eq!(
            inspect_managed_lifecycle_read_only(&handle)
                .expect_err("snapshot corruption must fail")
                .kind(),
            ManagedLifecycleReadErrorKind::Corruption
        );
    }
}

#[test]
fn mutation_between_complete_enumerations_invalidates_inventory_proof() {
    let fixture = DiskFixture::new();
    let lifecycle = fixture.lifecycle();
    let handle = open_root(fixture.root.path()).expect("Store-root handle");

    let error = inspect_with_hook(&handle, || {
        fs::write(lifecycle.join("appeared-during-enumeration"), b"race").expect("inject inventory mutation");
    })
    .expect_err("changed inventory must not mint a proof");
    assert_eq!(error.kind(), ManagedLifecycleReadErrorKind::InventoryChanged);
}

#[test]
fn lifecycle_directory_rebinding_after_first_inventory_is_rejected() {
    let fixture = DiskFixture::new();
    let lifecycle = fixture.lifecycle();
    let displaced = fixture.root.path().join("displaced-lifecycle");
    let handle = open_root(fixture.root.path()).expect("Store-root handle");

    let error = inspect_with_hook(&handle, || {
        fs::rename(&lifecycle, &displaced).expect("move retained lifecycle directory aside");
        fs::create_dir(&lifecycle).expect("install replacement lifecycle directory");
    })
    .expect_err("stable old handle must not prove a replaced parent binding");
    assert_eq!(error.kind(), ManagedLifecycleReadErrorKind::InventoryChanged);
}

#[test]
fn mutation_before_third_enumeration_invalidates_inventory_proof() {
    let fixture = DiskFixture::new();
    let lifecycle = fixture.lifecycle();
    let handle = open_root(fixture.root.path()).expect("Store-root handle");

    let error = inspect_with_hooks(
        &handle,
        || {},
        || {
            fs::write(lifecycle.join("appeared-before-third-scan"), b"race").expect("inject late inventory mutation");
        },
    )
    .expect_err("late inventory change must not mint a proof");
    assert_eq!(error.kind(), ManagedLifecycleReadErrorKind::InventoryChanged);
}

#[test]
fn case_fold_collisions_and_unknown_artifacts_fail_closed() {
    let stamp = platform::FileStamp {
        volume: 1,
        file_id: [1; 16],
        link_count: 1,
        length: 64,
        allocation_size: 64,
        created: [1, 0],
        modified: [1, 0],
        changed: [1, 0],
        attributes: 0,
        reparse_tag: 0,
        kind: platform::EntryKind::File,
    };
    let inventory = platform::InventorySnapshot {
        directory_stamp: stamp.clone(),
        entries: vec![
            platform::InventoryEntry {
                name: "STORE.META".to_owned(),
                kind: platform::EntryKind::File,
                stamp: stamp.clone(),
            },
            platform::InventoryEntry {
                name: "store.meta".to_owned(),
                kind: platform::EntryKind::File,
                stamp,
            },
        ],
    };
    assert_eq!(
        InventoryPlan::parse(&inventory, ManagedLifecycleReadLimits::default())
            .expect_err("case-fold aliases are corrupt")
            .kind(),
        ManagedLifecycleReadErrorKind::Corruption
    );

    let fixture = DiskFixture::new();
    fs::write(fixture.lifecycle().join("unknown.future"), b"unknown").expect("write unknown artifact");
    let handle = open_root(fixture.root.path()).expect("Store-root handle");
    assert_eq!(
        inspect_managed_lifecycle_read_only(&handle)
            .expect_err("unknown artifact must fail closed")
            .kind(),
        ManagedLifecycleReadErrorKind::Corruption
    );
}

#[test]
fn hard_link_aliases_between_durable_roles_are_rejected() {
    let fixture = DiskFixture::new();
    let marker = fixture.lifecycle().join("ENABLED.v1");
    let acknowledgement = fixture.lifecycle().join("ACKNOWLEDGED.v1");
    fs::remove_file(&acknowledgement).expect("remove independent acknowledgement");
    if let Err(error) = fs::hard_link(&marker, &acknowledgement) {
        if matches!(
            error.kind(),
            io::ErrorKind::Unsupported | io::ErrorKind::PermissionDenied
        ) {
            return;
        }
        panic!("create hard-link alias: {error}");
    }
    let handle = open_root(fixture.root.path()).expect("Store-root handle");

    assert_eq!(
        inspect_managed_lifecycle_read_only(&handle)
            .expect_err("hard-link aliases must fail closed")
            .kind(),
        ManagedLifecycleReadErrorKind::UnsafeNamespace
    );
}

#[test]
fn durable_sidecar_with_an_external_hard_link_is_rejected() {
    let fixture = DiskFixture::new();
    let meta = fixture.lifecycle().join("store.meta");
    let external_alias = fixture.root.path().join("store-meta-alias");
    if let Err(error) = fs::hard_link(&meta, &external_alias) {
        if matches!(
            error.kind(),
            io::ErrorKind::Unsupported | io::ErrorKind::PermissionDenied
        ) {
            return;
        }
        panic!("create external hard-link alias: {error}");
    }
    let handle = open_root(fixture.root.path()).expect("Store-root handle");

    assert_eq!(
        inspect_managed_lifecycle_read_only(&handle)
            .expect_err("every durable sidecar must be singly linked")
            .kind(),
        ManagedLifecycleReadErrorKind::UnsafeNamespace
    );
}

#[test]
fn torn_nonzero_acknowledgement_slot_is_reconstructed_from_the_unique_seal() {
    let fixture = DiskFixture::new();
    let acknowledgement_path = fixture.lifecycle().join("ACKNOWLEDGED.v1");
    let mut acknowledgement = fs::read(&acknowledgement_path).expect("read acknowledgement");
    acknowledgement[ACKNOWLEDGEMENT_SLOT_LENGTH + 100] ^= 1;
    fs::write(&acknowledgement_path, acknowledgement).expect("write torn acknowledgement slot");
    let handle = open_root(fixture.root.path()).expect("Store-root handle");

    assert_eq!(
        inspect_managed_lifecycle_read_only(&handle).expect("seal reconstructs the torn older slot"),
        ManagedLifecycleReadOutcome::ManagedNeedsReconciliation
    );
}

#[test]
fn valid_temporary_artifact_requires_write_side_recovery() {
    let fixture = DiskFixture::new();
    fs::write(
        fixture
            .lifecycle()
            .join("retirement.log.g00000000000000000001.tmp.0123456789abcdef0123456789abcdef"),
        b"partial",
    )
    .expect("write valid temporary artifact");
    let handle = open_root(fixture.root.path()).expect("Store-root handle");

    assert_eq!(
        inspect_managed_lifecycle_read_only(&handle).expect("temporary is classified"),
        ManagedLifecycleReadOutcome::RecoveryWriteRequired(ManagedLifecycleRecoveryReason::TemporaryArtifact)
    );
}

#[test]
fn temporary_artifact_cannot_mask_authoritative_log_corruption() {
    let fixture = DiskFixture::new();
    fs::write(
        fixture
            .lifecycle()
            .join("retirement.log.g00000000000000000001.tmp.0123456789abcdef0123456789abcdef"),
        b"partial",
    )
    .expect("write valid temporary artifact");
    let log_path = fixture.lifecycle().join(log_name(0));
    let mut log = fs::read(&log_path).expect("read selected log");
    *log.last_mut().expect("selected log seal CRC") ^= 1;
    fs::write(log_path, log).expect("corrupt selected log");
    let handle = open_root(fixture.root.path()).expect("Store-root handle");

    assert_eq!(
        inspect_managed_lifecycle_read_only(&handle)
            .expect_err("temporary file must not mask authoritative corruption")
            .kind(),
        ManagedLifecycleReadErrorKind::Corruption
    );
}

#[test]
#[cfg(any(unix, windows))]
fn symlink_or_reparse_generation_is_rejected_without_following() {
    let fixture = DiskFixture::new();
    let target = fixture.root.path().join("outside-log");
    fs::write(&target, b"not a ledger").expect("write external target");
    let link = fixture.lifecycle().join(log_name(0));
    fs::remove_file(&link).expect("remove canonical log");
    if let Err(error) = create_file_symlink(&target, &link) {
        if cfg!(windows) && (error.kind() == io::ErrorKind::PermissionDenied || error.raw_os_error() == Some(1314)) {
            return;
        }
        panic!("create test symlink: {error}");
    }
    let handle = open_root(fixture.root.path()).expect("Store-root handle");

    assert_eq!(
        inspect_managed_lifecycle_read_only(&handle)
            .expect_err("symlink must fail closed")
            .kind(),
        ManagedLifecycleReadErrorKind::UnsafeNamespace
    );
}

#[test]
fn oversized_log_is_rejected_before_file_sized_allocation() {
    let fixture = DiskFixture::new();
    let log = fs::OpenOptions::new()
        .write(true)
        .open(fixture.lifecycle().join(log_name(0)))
        .expect("open test log");
    log.set_len(MAX_LOG_FILE_LENGTH + 1).expect("make sparse oversized log");
    let handle = open_root(fixture.root.path()).expect("Store-root handle");

    assert_eq!(
        inspect_managed_lifecycle_read_only(&handle)
            .expect_err("oversized log must fail before allocation")
            .kind(),
        ManagedLifecycleReadErrorKind::LimitExceeded
    );
}

#[test]
fn explicit_limits_accept_exact_boundaries_and_reject_one_less() {
    let fixture = DiskFixture::new();
    let lifecycle = fixture.lifecycle();
    let log_length = fs::metadata(lifecycle.join(log_name(0))).expect("log metadata").len();
    let total_length = [
        "store.meta".to_owned(),
        "ENABLED.v1".to_owned(),
        "ACKNOWLEDGED.v1".to_owned(),
        snapshot_name(0),
        log_name(0),
    ]
    .iter()
    .map(|name| fs::metadata(lifecycle.join(name)).expect("sidecar metadata").len())
    .sum();
    let exact = ManagedLifecycleReadLimits {
        max_directory_entries: 5,
        max_generations: 1,
        max_sealed_units: 3,
        max_log_file_length: log_length,
        max_total_read_bytes: total_length,
    };
    let handle = open_root(fixture.root.path()).expect("Store-root handle");
    assert_eq!(
        inspect_managed_lifecycle_read_only_with_limits(&handle, exact).expect("exact limits succeed"),
        ManagedLifecycleReadOutcome::ManagedNeedsReconciliation
    );

    let too_short_log = ManagedLifecycleReadLimits {
        max_log_file_length: log_length - 1,
        ..exact
    };
    assert_eq!(
        inspect_managed_lifecycle_read_only_with_limits(&handle, too_short_log)
            .expect_err("one-byte-short log limit fails")
            .kind(),
        ManagedLifecycleReadErrorKind::LimitExceeded
    );
    let too_small_total = ManagedLifecycleReadLimits {
        max_total_read_bytes: total_length - 1,
        ..exact
    };
    assert_eq!(
        inspect_managed_lifecycle_read_only_with_limits(&handle, too_small_total)
            .expect_err("one-byte-short total limit fails")
            .kind(),
        ManagedLifecycleReadErrorKind::LimitExceeded
    );
    let invalid = ManagedLifecycleReadLimits {
        max_generations: 0,
        ..exact
    };
    assert_eq!(
        inspect_managed_lifecycle_read_only_with_limits(&handle, invalid)
            .expect_err("zero limit is rejected")
            .kind(),
        ManagedLifecycleReadErrorKind::LimitExceeded
    );
}

#[derive(Debug, Clone, Copy)]
enum Corruption {
    Truncated,
    Checksum,
}

struct DiskFixture {
    root: tempfile::TempDir,
}

impl DiskFixture {
    fn preactivation() -> Self {
        let root = tempfile::tempdir().expect("temporary Store root");
        let lifecycle = root.path().join(".rocketmq-lifecycle");
        fs::create_dir(&lifecycle).expect("create lifecycle directory");
        let meta = StoreMeta {
            store_uuid: StoreUuid::new([1; 16]).expect("test UUID"),
            creation_time_ns: 7,
            bootstrap_id: [2; 16],
        };
        fs::write(
            lifecycle.join("store.meta"),
            encode_store_meta(&meta).expect("meta encodes"),
        )
        .expect("write meta");
        fs::write(lifecycle.join("ACKNOWLEDGED.v1"), [0_u8; 208]).expect("write empty acknowledgement");
        Self { root }
    }

    fn new() -> Self {
        let root = tempfile::tempdir().expect("temporary Store root");
        let lifecycle = root.path().join(".rocketmq-lifecycle");
        fs::create_dir(&lifecycle).expect("create lifecycle directory");

        let store_uuid = StoreUuid::new([1; 16]).expect("test UUID");
        let bootstrap_id = [2; 16];
        let meta = StoreMeta {
            store_uuid,
            creation_time_ns: 7,
            bootstrap_id,
        };
        let snapshot = encode_snapshot(&LifecycleSnapshot {
            mode: SnapshotMode::BootstrapInventory,
            store_uuid,
            generation: 0,
            log_generation: 0,
            predecessor_log_generation: u64::MAX,
            base_sequence: 1,
            create_high_water: 0,
            ticket_high_water: 0,
            entries: Vec::new(),
        })
        .expect("snapshot encodes");
        let first = encode_ledger_frame(
            &LedgerRecord::StoreInitialized {
                store_uuid,
                bootstrap_id,
                creation_time_ns: 7,
            },
            1,
            0,
        )
        .expect("StoreInitialized encodes");
        let second = encode_ledger_frame(
            &LedgerRecord::BootstrapInstalled {
                store_uuid,
                bootstrap_id,
                snapshot_generation: 0,
                snapshot_base_sequence: 1,
                snapshot_file_length: snapshot.len() as u64,
                snapshot_file_crc32: crc32(&snapshot),
                inventory_count: 0,
                create_high_water: 0,
                ticket_high_water: 0,
            },
            2,
            0,
        )
        .expect("BootstrapInstalled encodes");
        let marker_slot = EnabledMarkerSlot {
            slot_index: 0,
            store_uuid,
            bootstrap_id,
            marker_epoch: 1,
            snapshot_generation: 0,
            log_generation: 0,
            anchor_sequence: 2,
            snapshot_file_length: snapshot.len() as u64,
            snapshot_file_crc32: crc32(&snapshot),
            anchor_frame_crc32: crc32(&second),
        };
        let marker_slot_bytes = encode_enabled_marker_slot(&marker_slot).expect("marker slot encodes");
        let marker_slot_crc32 = u32::from_le_bytes(marker_slot_bytes[100..104].try_into().expect("slot CRC"));
        let third = encode_ledger_frame(
            &LedgerRecord::MarkerCommitted {
                store_uuid,
                marker_epoch: 1,
                snapshot_generation: 0,
                log_generation: 0,
                anchor_sequence: 2,
                slot_index: 0,
                slot_crc32: marker_slot_crc32,
            },
            3,
            0,
        )
        .expect("MarkerCommitted encodes");
        let marker = encode_enabled_marker_file(&EnabledMarkerFile {
            slots: [Some(marker_slot), None],
        })
        .expect("marker file encodes");
        let (log, acknowledgement) = build_log(&meta, &[first, second, third]);

        fs::write(
            lifecycle.join("store.meta"),
            encode_store_meta(&meta).expect("meta encodes"),
        )
        .expect("write meta");
        fs::write(lifecycle.join("ACKNOWLEDGED.v1"), acknowledgement).expect("write acknowledgement");
        fs::write(lifecycle.join(snapshot_name(0)), snapshot).expect("write snapshot");
        fs::write(lifecycle.join(log_name(0)), log).expect("write log");
        fs::write(lifecycle.join("ENABLED.v1"), marker).expect("write marker");
        Self { root }
    }

    fn lifecycle(&self) -> std::path::PathBuf {
        self.root.path().join(".rocketmq-lifecycle")
    }
}

fn fixture_meta() -> StoreMeta {
    StoreMeta {
        store_uuid: StoreUuid::new([1; 16]).expect("test UUID"),
        creation_time_ns: 7,
        bootstrap_id: [2; 16],
    }
}

fn bootstrap_snapshot(meta: &StoreMeta) -> Vec<u8> {
    encode_snapshot(&LifecycleSnapshot {
        mode: SnapshotMode::BootstrapInventory,
        store_uuid: meta.store_uuid,
        generation: 0,
        log_generation: 0,
        predecessor_log_generation: u64::MAX,
        base_sequence: 1,
        create_high_water: 0,
        ticket_high_water: 0,
        entries: Vec::new(),
    })
    .expect("bootstrap snapshot encodes")
}

fn store_initialized_frame(meta: &StoreMeta) -> Vec<u8> {
    encode_ledger_frame(
        &LedgerRecord::StoreInitialized {
            store_uuid: meta.store_uuid,
            bootstrap_id: meta.bootstrap_id,
            creation_time_ns: meta.creation_time_ns,
        },
        1,
        0,
    )
    .expect("StoreInitialized encodes")
}

fn bootstrap_installed_frame(meta: &StoreMeta, snapshot: &[u8]) -> Vec<u8> {
    encode_ledger_frame(
        &LedgerRecord::BootstrapInstalled {
            store_uuid: meta.store_uuid,
            bootstrap_id: meta.bootstrap_id,
            snapshot_generation: 0,
            snapshot_base_sequence: 1,
            snapshot_file_length: snapshot.len() as u64,
            snapshot_file_crc32: crc32(snapshot),
            inventory_count: 0,
            create_high_water: 0,
            ticket_high_water: 0,
        },
        2,
        0,
    )
    .expect("BootstrapInstalled encodes")
}

fn build_log(meta: &StoreMeta, frames: &[Vec<u8>]) -> (Vec<u8>, [u8; 208]) {
    let mut log = Vec::new();
    let mut slots = [[0_u8; 104]; 2];
    for (index, frame) in frames.iter().enumerate() {
        let sequence = index as u64 + 1;
        let epoch = sequence;
        let slot_index = ((epoch - 1) & 1) as u8;
        let slot = AcknowledgementSlot {
            slot_index,
            activated: sequence >= 3,
            store_uuid: meta.store_uuid,
            bootstrap_id: meta.bootstrap_id,
            acknowledgement_epoch: epoch,
            marker_epoch: u64::from(sequence >= 3),
            log_generation: 0,
            frame_sequence: sequence,
            frame_end_offset: (log.len() + frame.len()) as u64,
            frame_crc32: crc32(frame),
        };
        let encoded_slot = encode_acknowledgement_slot(&slot).expect("acknowledgement encodes");
        let seal = CommitSeal::from_acknowledgement_slot(&slot, &encoded_slot).expect("seal derives");
        log.extend_from_slice(frame);
        log.extend_from_slice(&encode_commit_seal(&seal).expect("seal encodes"));
        slots[slot_index as usize] = encoded_slot;
    }
    let mut acknowledgement = [0_u8; 208];
    acknowledgement[..104].copy_from_slice(&slots[0]);
    acknowledgement[104..].copy_from_slice(&slots[1]);
    (log, acknowledgement)
}

fn snapshot_name(generation: u64) -> String {
    format!("manifest.snapshot.g{generation:020}")
}

fn log_name(generation: u64) -> String {
    format!("retirement.log.g{generation:020}")
}

fn tree_bytes(root: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut result = BTreeMap::new();
    collect_tree(root, root, &mut result);
    result
}

fn collect_tree(root: &Path, current: &Path, output: &mut BTreeMap<String, Vec<u8>>) {
    let mut entries = fs::read_dir(current)
        .expect("read fixture tree")
        .map(|entry| entry.expect("fixture entry"))
        .collect::<Vec<_>>();
    entries.sort_by_key(fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        let relative = path.strip_prefix(root).expect("relative fixture path");
        let name = relative.to_string_lossy().replace('\\', "/");
        if entry.file_type().expect("fixture file type").is_dir() {
            output.insert(format!("{name}/"), Vec::new());
            collect_tree(root, &path, output);
        } else {
            output.insert(name, fs::read(path).expect("read fixture file"));
        }
    }
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

#[cfg(windows)]
fn create_file_symlink(target: &Path, link: &Path) -> io::Result<()> {
    std::os::windows::fs::symlink_file(target, link)
}

#[cfg(unix)]
fn create_file_symlink(target: &Path, link: &Path) -> io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}
