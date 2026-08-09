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
fn quarantine_cannot_appear_before_activation() {
    let fixture = DiskFixture::preactivation();
    fs::create_dir(fixture.lifecycle().join("quarantine")).expect("create quarantine");
    let handle = open_root(fixture.root.path()).expect("Store-root handle");

    assert_eq!(
        inspect_managed_lifecycle_read_only(&handle)
            .expect_err("quarantine is not an initial-bootstrap artifact")
            .kind(),
        ManagedLifecycleReadErrorKind::Corruption
    );
}

#[test]
fn mutation_inside_quarantine_invalidates_the_nested_inventory_proof() {
    let fixture = DiskFixture::new();
    let quarantine = fixture.lifecycle().join("quarantine");
    fs::create_dir(&quarantine).expect("create quarantine");
    let evidence = quarantine.join("reviewed-evidence.bin");
    fs::write(&evidence, b"before").expect("write reviewed evidence");
    let handle = open_root(fixture.root.path()).expect("Store-root handle");

    let error = inspect_with_hooks(
        &handle,
        || {},
        || fs::write(&evidence, b"after!!").expect("mutate nested evidence"),
    )
    .expect_err("quarantine must have its own A/B/C proof");
    assert_eq!(error.kind(), ManagedLifecycleReadErrorKind::InventoryChanged);
}

#[test]
fn quarantine_tail_filename_length_and_crc_are_authoritative() {
    let fixture = DiskFixture::new();
    let quarantine = fixture.lifecycle().join("quarantine");
    fs::create_dir(&quarantine).expect("create quarantine");
    fs::write(
        quarantine.join(
            "retirement.log.g00000000000000000000.tail.o00000000000000000000.l00000000000000000003.c00000000.bin",
        ),
        b"abc",
    )
    .expect("write incorrectly named tail evidence");
    let handle = open_root(fixture.root.path()).expect("Store-root handle");

    let error = inspect_managed_lifecycle_read_only(&handle)
        .expect_err("tail content must match the CRC embedded in its exact filename");
    assert_eq!(
        error.kind(),
        ManagedLifecycleReadErrorKind::Corruption,
        "unexpected failure: {error}"
    );
}

#[test]
fn every_tail_repair_snapshot_requires_exact_quarantine_evidence() {
    let store_uuid = StoreUuid::new([1; 16]).expect("test UUID");
    let snapshot = encode_snapshot(&LifecycleSnapshot {
        mode: SnapshotMode::TailRepair,
        store_uuid,
        generation: 1,
        log_generation: 1,
        predecessor_log_generation: 0,
        base_sequence: 3,
        create_high_water: 0,
        ticket_high_water: 0,
        entries: Vec::new(),
    })
    .expect("tail-repair snapshot encodes");
    let suffix = b"tail";
    let log = encode_ledger_frame(
        &LedgerRecord::LogOpened {
            store_uuid,
            generation: 1,
            snapshot_generation: 1,
            predecessor_log_generation: 0,
            predecessor_terminal_acknowledged_sequence: 3,
            snapshot_base_sequence: 3,
            snapshot_file_length: snapshot.len() as u64,
            snapshot_file_crc32: crc32(&snapshot),
            predecessor_prefix_crc32: 9,
            validated_prefix_length: 100,
            unacknowledged_suffix_length: suffix.len() as u32,
            unacknowledged_suffix_crc32: crc32(suffix),
            open_reason: OpenReason::TailRepair,
            predecessor_acknowledgement_epoch: 3,
        },
        4,
        1,
    )
    .expect("tail-repair LogOpened encodes");
    let generations = [OwnedGeneration {
        generation: 1,
        snapshot,
        log,
    }];

    assert_eq!(
        validate_required_tail_evidence(&generations, &[])
            .expect_err("tail repair without copied suffix evidence is corrupt")
            .kind(),
        ManagedLifecycleReadErrorKind::Corruption
    );
    validate_required_tail_evidence(
        &generations,
        &[OwnedTailEvidence {
            coordinates: TailEvidenceCoordinates {
                predecessor_generation: 0,
                offset: 100,
                length: suffix.len() as u64,
                crc32: crc32(suffix),
            },
            bytes: suffix.to_vec(),
        }],
    )
    .expect("the exact named suffix evidence satisfies the tail-repair proof");
}
