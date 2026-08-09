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

use super::*;

#[test]
fn marker_absent_managed_artifacts_require_bootstrap_recovery() {
    let activated = DiskFixture::new();
    fs::remove_file(activated.lifecycle().join("ENABLED.v1")).expect("remove marker");
    let handle = open_root(activated.root.path()).expect("Store-root handle");
    assert_eq!(
        inspect_managed_lifecycle_read_only(&handle)
            .expect_err("activated acknowledgement without marker is corruption")
            .kind(),
        ManagedLifecycleReadErrorKind::Corruption
    );

    let preactivation = DiskFixture::preactivation();
    let handle = open_root(preactivation.root.path()).expect("Store-root handle");
    assert_eq!(
        inspect_managed_lifecycle_read_only(&handle).expect("preactivation inspection succeeds"),
        ManagedLifecycleReadOutcome::RecoveryWriteRequired(ManagedLifecycleRecoveryReason::BootstrapResume)
    );
}

#[test]
fn marker_absent_accepts_the_exact_log_only_store_initialized_checkpoint() {
    let fixture = DiskFixture::preactivation();
    let meta = fixture_meta();
    let initialized = store_initialized_frame(&meta);
    let (log, acknowledgement) = build_log(&meta, &[initialized]);
    fs::write(fixture.lifecycle().join(log_name(0)), log).expect("write generation-0 log");
    fs::write(fixture.lifecycle().join("ACKNOWLEDGED.v1"), acknowledgement)
        .expect("write StoreInitialized acknowledgement");
    let handle = open_root(fixture.root.path()).expect("Store-root handle");

    assert_eq!(
        inspect_managed_lifecycle_read_only(&handle).expect("exact bootstrap checkpoint is resumable"),
        ManagedLifecycleReadOutcome::RecoveryWriteRequired(ManagedLifecycleRecoveryReason::BootstrapResume)
    );
}

#[test]
fn marker_absent_rejects_noncanonical_bootstrap_frontiers() {
    let missing_acknowledgement = DiskFixture::preactivation();
    fs::remove_file(missing_acknowledgement.lifecycle().join("ACKNOWLEDGED.v1")).expect("remove acknowledgement");
    fs::write(missing_acknowledgement.lifecycle().join(log_name(0)), b"").expect("write empty log");
    let handle = open_root(missing_acknowledgement.root.path()).expect("Store-root handle");
    assert_eq!(
        inspect_managed_lifecycle_read_only(&handle)
            .expect_err("a log cannot precede the fixed acknowledgement file")
            .kind(),
        ManagedLifecycleReadErrorKind::Corruption
    );

    let zero_acknowledgement = DiskFixture::preactivation();
    let meta = fixture_meta();
    let (sealed_initialized, _) = build_log(&meta, &[store_initialized_frame(&meta)]);
    fs::write(zero_acknowledgement.lifecycle().join(log_name(0)), sealed_initialized)
        .expect("write sealed StoreInitialized");
    let handle = open_root(zero_acknowledgement.root.path()).expect("Store-root handle");
    assert_eq!(
        inspect_managed_lifecycle_read_only(&handle)
            .expect_err("a sealed unit cannot coexist with zero acknowledgement slots")
            .kind(),
        ManagedLifecycleReadErrorKind::Corruption
    );

    let wrong_first_record = DiskFixture::preactivation();
    let wrong_frame = encode_ledger_frame(
        &LedgerRecord::StoreInitialized {
            creation_time_ns: meta.creation_time_ns + 1,
            store_uuid: meta.store_uuid,
            bootstrap_id: meta.bootstrap_id,
        },
        1,
        0,
    )
    .expect("mismatched StoreInitialized encodes");
    let (wrong_log, wrong_acknowledgement) = build_log(&meta, &[wrong_frame]);
    fs::write(wrong_first_record.lifecycle().join(log_name(0)), wrong_log).expect("write wrong log");
    fs::write(
        wrong_first_record.lifecycle().join("ACKNOWLEDGED.v1"),
        wrong_acknowledgement,
    )
    .expect("write wrong acknowledgement");
    let handle = open_root(wrong_first_record.root.path()).expect("Store-root handle");
    assert_eq!(
        inspect_managed_lifecycle_read_only(&handle)
            .expect_err("StoreInitialized must match immutable store.meta byte-for-byte")
            .kind(),
        ManagedLifecycleReadErrorKind::Corruption
    );
}

#[test]
fn marker_absent_requires_store_initialized_before_the_bootstrap_snapshot() {
    let fixture = DiskFixture::preactivation();
    let snapshot = bootstrap_snapshot(&fixture_meta());
    fs::write(fixture.lifecycle().join(snapshot_name(0)), snapshot).expect("write bootstrap snapshot");
    fs::write(fixture.lifecycle().join(log_name(0)), b"").expect("write empty generation-0 log");
    let handle = open_root(fixture.root.path()).expect("Store-root handle");

    assert_eq!(
        inspect_managed_lifecycle_read_only(&handle)
            .expect_err("snapshot publication cannot precede durable StoreInitialized")
            .kind(),
        ManagedLifecycleReadErrorKind::Corruption
    );
}

#[test]
fn marker_absent_reconstructs_each_torn_acknowledgement_slot_from_seals() {
    let fixture = DiskFixture::preactivation();
    let meta = fixture_meta();
    let snapshot = bootstrap_snapshot(&meta);
    let frames = [
        store_initialized_frame(&meta),
        bootstrap_installed_frame(&meta, &snapshot),
    ];
    let (log, mut acknowledgement) = build_log(&meta, &frames);
    acknowledgement[100] ^= 1;
    fs::write(fixture.lifecycle().join(snapshot_name(0)), snapshot).expect("write bootstrap snapshot");
    fs::write(fixture.lifecycle().join(log_name(0)), log).expect("write preactivation log");
    fs::write(fixture.lifecycle().join("ACKNOWLEDGED.v1"), acknowledgement).expect("write torn slot");
    let handle = open_root(fixture.root.path()).expect("Store-root handle");

    assert_eq!(
        inspect_managed_lifecycle_read_only(&handle).expect("the unique seals reconstruct the torn slot"),
        ManagedLifecycleReadOutcome::RecoveryWriteRequired(ManagedLifecycleRecoveryReason::BootstrapResume)
    );
}

#[test]
fn marker_absent_rejects_any_marker_committed_prefix() {
    let fixture = DiskFixture::preactivation();
    let meta = fixture_meta();
    let snapshot = bootstrap_snapshot(&meta);
    let installed = bootstrap_installed_frame(&meta, &snapshot);
    let marker_slot = EnabledMarkerSlot {
        slot_index: 0,
        store_uuid: meta.store_uuid,
        bootstrap_id: meta.bootstrap_id,
        marker_epoch: 1,
        snapshot_generation: 0,
        log_generation: 0,
        anchor_sequence: 2,
        snapshot_file_length: snapshot.len() as u64,
        snapshot_file_crc32: crc32(&snapshot),
        anchor_frame_crc32: crc32(&installed),
    };
    let marker_slot_bytes = encode_enabled_marker_slot(&marker_slot).expect("marker slot encodes");
    let marker_committed = encode_ledger_frame(
        &LedgerRecord::MarkerCommitted {
            store_uuid: meta.store_uuid,
            marker_epoch: 1,
            snapshot_generation: 0,
            log_generation: 0,
            anchor_sequence: 2,
            slot_index: 0,
            slot_crc32: u32::from_le_bytes(marker_slot_bytes[100..104].try_into().expect("slot CRC")),
        },
        3,
        0,
    )
    .expect("MarkerCommitted encodes");
    let (mut log, acknowledgement) = build_log(&meta, &[store_initialized_frame(&meta), installed]);
    log.extend_from_slice(&marker_committed[..17]);
    fs::write(fixture.lifecycle().join(snapshot_name(0)), snapshot).expect("write bootstrap snapshot");
    fs::write(fixture.lifecycle().join(log_name(0)), log).expect("write partial marker witness");
    fs::write(fixture.lifecycle().join("ACKNOWLEDGED.v1"), acknowledgement)
        .expect("write preactivation acknowledgement");
    let handle = open_root(fixture.root.path()).expect("Store-root handle");

    assert_eq!(
        inspect_managed_lifecycle_read_only(&handle)
            .expect_err("marker witness bytes prove missing activation state")
            .kind(),
        ManagedLifecycleReadErrorKind::Corruption
    );
}
