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

use rocketmq_store_api::CursorAdvanceDisposition;
use rocketmq_store_api::CursorAdvanceError;
use rocketmq_store_api::DerivedCheckpoint;
use rocketmq_store_api::DerivedCheckpointDecodeError;
use rocketmq_store_api::DerivedCursor;
use rocketmq_store_api::DerivedEngine;
use rocketmq_store_api::DerivedProgress;
use rocketmq_store_api::DerivedRecordId;
use rocketmq_store_api::DerivedRecordIdError;
use rocketmq_store_api::LegacyDerivedCursorV0;
use rocketmq_store_api::DERIVED_CHECKPOINT_ENCODED_LEN;
use rocketmq_store_api::DERIVED_CHECKPOINT_FORMAT_VERSION;

#[test]
fn cursor_accepts_only_contiguous_records_and_classifies_complete_duplicates() {
    let cursor = DerivedCursor::genesis(7);
    let first = DerivedRecordId::try_new(7, 0, 12).expect("first record is valid");
    let first_advance = match cursor.prepare(first).expect("first record is contiguous") {
        CursorAdvanceDisposition::Advance(advance) => advance,
        CursorAdvanceDisposition::AlreadyCommitted => panic!("genesis cursor cannot contain the first record"),
    };
    assert_eq!(DerivedCursor::restore(7, 12), first_advance.next_cursor());

    let cursor = first_advance.next_cursor();
    assert_eq!(
        CursorAdvanceDisposition::AlreadyCommitted,
        cursor.prepare(first).expect("complete replay is idempotent")
    );

    let second = DerivedRecordId::try_new(7, 12, 8).expect("second record is valid");
    let second_advance = match cursor.prepare(second).expect("second record is contiguous") {
        CursorAdvanceDisposition::Advance(advance) => advance,
        CursorAdvanceDisposition::AlreadyCommitted => panic!("second record is not committed yet"),
    };
    assert_eq!(20, second_advance.next_cursor().next_offset());
}

#[test]
fn cursor_fails_closed_on_epoch_gap_or_partial_overlap() {
    let cursor = DerivedCursor::restore(4, 20);
    let other_epoch = DerivedRecordId::try_new(5, 20, 4).expect("record is valid");
    assert_eq!(
        CursorAdvanceError::SourceEpochMismatch { expected: 4, actual: 5 },
        cursor.prepare(other_epoch).expect_err("epoch change must fail")
    );

    let gap = DerivedRecordId::try_new(4, 24, 4).expect("record is valid");
    assert_eq!(
        CursorAdvanceError::Gap {
            expected: 20,
            actual: 24,
        },
        cursor.prepare(gap).expect_err("physical hole must fail")
    );

    let overlap = DerivedRecordId::try_new(4, 16, 8).expect("record is valid");
    assert_eq!(
        CursorAdvanceError::PartialOverlap {
            committed: 20,
            record_start: 16,
            record_end: 24,
        },
        cursor.prepare(overlap).expect_err("partial record replay must fail")
    );
}

#[test]
fn record_identity_rejects_empty_and_overflowing_ranges() {
    assert_eq!(
        DerivedRecordIdError::EmptyRecord,
        DerivedRecordId::try_new(1, 0, 0).expect_err("zero-length record must fail")
    );
    assert_eq!(
        DerivedRecordIdError::RangeOverflow,
        DerivedRecordId::try_new(1, u64::MAX, 1).expect_err("overflowing range must fail")
    );
}

#[test]
fn checkpoint_round_trip_is_fixed_versioned_and_payload_free() {
    let checkpoint = DerivedCheckpoint::new(DerivedEngine::Tiered, DerivedCursor::restore(9, 4_096));
    let encoded = checkpoint.encode();

    assert_eq!(DERIVED_CHECKPOINT_ENCODED_LEN, encoded.len());
    assert_eq!(DERIVED_CHECKPOINT_FORMAT_VERSION, checkpoint.format_version());
    assert_eq!(
        checkpoint,
        DerivedCheckpoint::decode(&encoded, DerivedEngine::Tiered).expect("checkpoint must round trip")
    );
    assert!(!encoded
        .windows(b"message-payload".len())
        .any(|window| window == b"message-payload"));
}

#[test]
fn checkpoint_rejects_corruption_wrong_owner_and_future_version() {
    let checkpoint = DerivedCheckpoint::new(DerivedEngine::Index, DerivedCursor::restore(3, 64));
    let encoded = checkpoint.encode();

    assert_eq!(
        DerivedCheckpointDecodeError::EngineMismatch {
            expected: DerivedEngine::Tiered,
            actual: DerivedEngine::Index,
        },
        DerivedCheckpoint::decode(&encoded, DerivedEngine::Tiered).expect_err("owner mismatch must fail")
    );

    let mut corrupted = encoded;
    corrupted[20] ^= 0x40;
    assert_eq!(
        DerivedCheckpointDecodeError::ChecksumMismatch,
        DerivedCheckpoint::decode(&corrupted, DerivedEngine::Index).expect_err("corruption must fail")
    );

    let mut future = encoded;
    future[8..10].copy_from_slice(&(DERIVED_CHECKPOINT_FORMAT_VERSION + 1).to_be_bytes());
    assert_eq!(
        DerivedCheckpointDecodeError::UnsupportedVersion(DERIVED_CHECKPOINT_FORMAT_VERSION + 1),
        DerivedCheckpoint::decode(&future, DerivedEngine::Index).expect_err("future version must fail")
    );
}

#[test]
fn legacy_offset_upgrade_requires_an_explicit_source_epoch() {
    let legacy = LegacyDerivedCursorV0::new(128);
    let upgraded = DerivedCheckpoint::upgrade_legacy(DerivedEngine::ConsumeQueue, 77, legacy);

    assert_eq!(DERIVED_CHECKPOINT_FORMAT_VERSION, upgraded.format_version());
    assert_eq!(DerivedCursor::restore(77, 128), upgraded.cursor());
    assert_eq!(
        upgraded,
        DerivedCheckpoint::decode(&upgraded.encode(), DerivedEngine::ConsumeQueue)
            .expect("upgraded checkpoint must use current format")
    );
}

#[test]
fn derived_progress_remains_separate_from_primary_acknowledgement() {
    let progress = DerivedProgress::new(80, 64);
    assert!(!progress.acknowledges_primary_append());
    assert!(!progress.satisfies_primary_durability());
}
