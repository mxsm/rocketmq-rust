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

use std::path::PathBuf;

use rocketmq_error::STORAGE_IO_FAILED;
use rocketmq_error::STORAGE_WRITE_FAILED;
use rocketmq_store_api::file_uri_to_path;
use rocketmq_store_api::hash_checkpoint_directory;
use rocketmq_store_api::AppendReceipt;
use rocketmq_store_api::AppendStatus;
use rocketmq_store_api::CheckpointOffsets;
use rocketmq_store_api::DerivedCheckpoint;
use rocketmq_store_api::DerivedCursor;
use rocketmq_store_api::DerivedEngine;
use rocketmq_store_api::DerivedRecordId;
use rocketmq_store_api::Durability;
use rocketmq_store_api::MasterEpoch;
use rocketmq_store_api::ReleaseCheckpointCreateOutcome;
use rocketmq_store_api::ReleaseCheckpointCreateRejection;
use rocketmq_store_api::ReleaseCheckpointRestoreOutcome;
use rocketmq_store_api::ReleaseCheckpointRestoreRejection;
use rocketmq_store_api::StoreComponent;
use rocketmq_store_api::StoreContractViolation;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;
use rocketmq_store_api::TimerEngineId;
use rocketmq_store_api::TimerSnapshotFile;
use rocketmq_store_api::TimerSnapshotManifest;
use rocketmq_store_api::TimerTimelineIndexKind;
use rocketmq_store_api::DERIVED_CHECKPOINT_ENCODED_LEN;
use rocketmq_store_api::TIMER_SNAPSHOT_SCHEMA_VERSION;

#[test]
fn storage_api_is_consumed_only_through_root_exports() {
    let source = include_str!("../src/lib.rs");
    let error_source = include_str!("../src/error.rs");
    for module in ["capability", "contract", "error", "progress"] {
        assert!(
            !source.contains(&format!("pub mod {module};")),
            "`rocketmq-store-api` implementation module `{module}` must remain private"
        );
    }

    let error = StoreError::new(&STORAGE_WRITE_FAILED, StoreOperation::Append).in_component(StoreComponent::CommitLog);
    assert_eq!("storage.write.failed", error.code().as_str());
    assert!(!source.contains(concat!("StoreError", "Kind")));
    assert!(!error_source.contains(concat!("StoreError", "Kind")));
    assert!(!error_source.contains("impl DomainError for StoreError"));
}

#[test]
fn operational_capabilities_expose_one_error_identity_and_exact_checkpoint_outcomes() {
    let capability_sources = [
        include_str!("../src/capability/admin.rs"),
        include_str!("../src/capability/appender.rs"),
        include_str!("../src/capability/derived.rs"),
        include_str!("../src/capability/lifecycle.rs"),
        include_str!("../src/capability/offset.rs"),
        include_str!("../src/capability/reader.rs"),
        include_str!("../src/capability/release_checkpoint.rs"),
        include_str!("../src/capability/replication.rs"),
    ];
    assert!(capability_sources.iter().all(|source| !source.contains("type Error:")));

    let create_rejections = [
        ReleaseCheckpointCreateRejection::AuthorizationExpired,
        ReleaseCheckpointCreateRejection::CapabilityNotGranted,
        ReleaseCheckpointCreateRejection::AlreadyExists,
        ReleaseCheckpointCreateRejection::CapacityExceeded {
            actual_bytes: 2,
            maximum_bytes: 1,
        },
    ];
    assert_eq!(
        ["authorization", "capability", "exists", "capacity"],
        create_rejections.map(create_rejection_name)
    );

    let restore_rejections = [
        ReleaseCheckpointRestoreRejection::AuthorizationExpired,
        ReleaseCheckpointRestoreRejection::CapabilityNotGranted,
    ];
    assert_eq!(
        ["authorization", "capability"],
        restore_rejections.map(restore_rejection_name)
    );
    let _create_classifier: fn(ReleaseCheckpointCreateOutcome) -> &'static str = create_outcome_name;
    let _restore_classifier: fn(ReleaseCheckpointRestoreOutcome) -> &'static str = restore_outcome_name;
}

fn create_outcome_name(outcome: ReleaseCheckpointCreateOutcome) -> &'static str {
    match outcome {
        ReleaseCheckpointCreateOutcome::Created(_) => "created",
        ReleaseCheckpointCreateOutcome::Rejected(rejection) => create_rejection_name(rejection),
    }
}

fn create_rejection_name(rejection: ReleaseCheckpointCreateRejection) -> &'static str {
    match rejection {
        ReleaseCheckpointCreateRejection::AuthorizationExpired => "authorization",
        ReleaseCheckpointCreateRejection::CapabilityNotGranted => "capability",
        ReleaseCheckpointCreateRejection::AlreadyExists => "exists",
        ReleaseCheckpointCreateRejection::CapacityExceeded {
            actual_bytes: _,
            maximum_bytes: _,
        } => "capacity",
    }
}

fn restore_outcome_name(outcome: ReleaseCheckpointRestoreOutcome) -> &'static str {
    match outcome {
        ReleaseCheckpointRestoreOutcome::Verified(_) => "verified",
        ReleaseCheckpointRestoreOutcome::Rejected(rejection) => restore_rejection_name(rejection),
    }
}

fn restore_rejection_name(rejection: ReleaseCheckpointRestoreRejection) -> &'static str {
    match rejection {
        ReleaseCheckpointRestoreRejection::AuthorizationExpired => "authorization",
        ReleaseCheckpointRestoreRejection::CapabilityNotGranted => "capability",
    }
}

#[test]
fn one_contract_type_owns_every_deterministic_store_api_violation() {
    let path = PathBuf::from("checkpoint");
    let violations = vec![
        StoreContractViolation::CheckpointArtifactTooLarge { actual: 2, maximum: 1 },
        StoreContractViolation::CheckpointArtifactEmpty,
        StoreContractViolation::CheckpointArtifactSymbolicLink(path.clone()),
        StoreContractViolation::CheckpointArtifactPathEscaped(path.clone()),
        StoreContractViolation::CheckpointArtifactUnsupportedFileType(path),
        StoreContractViolation::CheckpointArtifactUnsupportedUri("memory://checkpoint".into()),
        StoreContractViolation::CheckpointSchemaVersion { expected: 1, actual: 2 },
        StoreContractViolation::CheckpointInvalidField {
            field: "checkpoint_id",
            reason: "empty".into(),
        },
        StoreContractViolation::CheckpointInvalidOffsets("reversed".into()),
        StoreContractViolation::CheckpointDestructiveRollback,
        StoreContractViolation::CheckpointRestoreVerificationIncomplete,
        StoreContractViolation::HaInvalidMasterEpoch(0),
        StoreContractViolation::HaInvalidSyncStateSetEpoch(0),
        StoreContractViolation::HaInvalidBrokerId(-1),
        StoreContractViolation::HaBrokerIdOutOfRange(u64::MAX),
        StoreContractViolation::HaInvalidReplicaCount(1),
        StoreContractViolation::HaInvalidAckPolicy(0),
        StoreContractViolation::HaInvalidOffset(-1),
        StoreContractViolation::HaInvalidLeaseGeneration(0),
        StoreContractViolation::HaEmptySyncStateSet,
        StoreContractViolation::HaLeaderMissingFromSyncStateSet(1),
        StoreContractViolation::AppendReceiptEmptyRange,
        StoreContractViolation::AppendReceiptReversedRange,
        StoreContractViolation::AppendReceiptRejectedStatusWithRange,
        StoreContractViolation::AppendReceiptAcceptedStatusWithoutRange,
        StoreContractViolation::AppendReceiptAppendedWatermarkBehindRange,
        StoreContractViolation::AppendReceiptDurableWatermarkBehindRange,
        StoreContractViolation::AppendReceiptDurableWatermarkAheadOfAppended,
        StoreContractViolation::AppendReceiptMemoryDurabilityAlreadyCovered,
        StoreContractViolation::AppendReceiptReplicatedDurabilityRequiresDecision,
        StoreContractViolation::AppendReceiptReplicationDecisionBehindRange,
        StoreContractViolation::DerivedRecordEmpty,
        StoreContractViolation::DerivedRecordRangeOverflow,
        StoreContractViolation::DerivedCursorSourceEpochMismatch { expected: 1, actual: 2 },
        StoreContractViolation::DerivedCursorGap { expected: 1, actual: 2 },
        StoreContractViolation::DerivedCursorPartialOverlap {
            committed: 2,
            record_start: 1,
            record_end: 3,
        },
        StoreContractViolation::DerivedCheckpointInvalidLength {
            expected: DERIVED_CHECKPOINT_ENCODED_LEN,
            actual: 0,
        },
        StoreContractViolation::DerivedCheckpointInvalidMagic,
        StoreContractViolation::DerivedCheckpointUnsupportedVersion(2),
        StoreContractViolation::DerivedCheckpointUnknownEngine(0),
        StoreContractViolation::DerivedCheckpointEngineMismatch {
            expected: DerivedEngine::Tiered,
            actual: DerivedEngine::Index,
        },
        StoreContractViolation::DerivedCheckpointInvalidReservedByte(1),
        StoreContractViolation::DerivedCheckpointChecksumMismatch,
        StoreContractViolation::TimerSnapshotUnsupportedVersion(2),
        StoreContractViolation::TimerSnapshotInvalidMetadata,
        StoreContractViolation::TimerSnapshotInvalidFile,
        StoreContractViolation::TimerSnapshotChecksumMismatch,
        StoreContractViolation::TimerSnapshotInvalidNativeBinding,
        StoreContractViolation::TimerSnapshotArtifactDigestMismatch,
        StoreContractViolation::TimerUnknownEngine("unknown".into()),
        StoreContractViolation::TimerInvalidPayloadLocator,
        StoreContractViolation::TimerInvalidPayloadStoreLocator,
        StoreContractViolation::TimerInvalidRoute,
        StoreContractViolation::TimerConfigurationOutOfRange {
            field: "timer_precision_ms",
            actual: 0,
            minimum: 1,
            maximum: 1_000,
        },
    ];

    for expected in [
        ContractFamily::CheckpointArtifact,
        ContractFamily::Checkpoint,
        ContractFamily::Ha,
        ContractFamily::AppendReceipt,
        ContractFamily::DerivedRecord,
        ContractFamily::DerivedCursor,
        ContractFamily::DerivedCheckpoint,
        ContractFamily::TimerSnapshot,
        ContractFamily::Timer,
    ] {
        assert!(violations
            .iter()
            .any(|violation| contract_family(violation) == expected));
    }
}

#[test]
fn public_entry_points_emit_every_contract_family() {
    assert_eq!(
        Err(StoreContractViolation::CheckpointArtifactUnsupportedUri(
            "memory://checkpoint".into()
        )),
        file_uri_to_path("memory://checkpoint")
    );
    assert_eq!(
        Err(StoreContractViolation::CheckpointInvalidOffsets(
            "checkpoint offsets cannot be negative".into()
        )),
        CheckpointOffsets {
            appended_offset: -1,
            durable_offset: 0,
            consume_queue_offset: 0,
            index_offset: 0,
        }
        .validate()
    );
    assert_eq!(
        Err(StoreContractViolation::HaInvalidMasterEpoch(0)),
        MasterEpoch::try_from(0)
    );
    assert_eq!(
        Err(StoreContractViolation::AppendReceiptEmptyRange),
        AppendReceipt::try_new(AppendStatus::PutOk, 1..1, 1, 0, Durability::Memory)
    );
    assert_eq!(
        Err(StoreContractViolation::DerivedRecordEmpty),
        DerivedRecordId::try_new(1, 0, 0)
    );
    let gapped_record = DerivedRecordId::try_new(1, 2, 1).expect("record identity is valid");
    assert_eq!(
        Err(StoreContractViolation::DerivedCursorGap { expected: 0, actual: 2 }),
        DerivedCursor::genesis(1).prepare(gapped_record)
    );
    assert_eq!(
        Err(StoreContractViolation::DerivedCheckpointInvalidLength {
            expected: DERIVED_CHECKPOINT_ENCODED_LEN,
            actual: 0,
        }),
        DerivedCheckpoint::decode(&[], DerivedEngine::Tiered)
    );
    let invalid_snapshot = TimerSnapshotManifest {
        schema_version: 0,
        generation: 0,
        source_cq_cursor: 0,
        source_physical_cursor: 0,
        due_time_cursor_ms: 0,
        completion_physical_cursor: 0,
        timeline_sequence: 0,
        timeline_index_kind: TimerTimelineIndexKind::RocksDb,
        native_manifest_generation: None,
        native_durable_end: None,
        native_manifest_checksum: None,
        native_files: Vec::new(),
        role_epoch: 0,
        activation_epoch: 0,
        format_fingerprint: 0,
        timeline_checkpoint_uri: String::new(),
        payload_files: Vec::new(),
        checksum: String::new(),
    };
    assert_eq!(
        Err(StoreContractViolation::TimerSnapshotUnsupportedVersion(0)),
        invalid_snapshot.validate()
    );
    assert_eq!(
        Err(StoreContractViolation::TimerUnknownEngine("?".into())),
        TimerEngineId::parse("?")
    );
}

#[test]
fn filesystem_failures_retain_io_sources_behind_store_error() {
    let missing_checkpoint =
        std::env::temp_dir().join(format!("rocketmq-store-api-missing-checkpoint-{}", std::process::id()));
    let checkpoint_error =
        hash_checkpoint_directory(&missing_checkpoint, 1).expect_err("a missing checkpoint directory must fail");
    assert_eq!(&STORAGE_IO_FAILED, checkpoint_error.descriptor());
    assert!(source_chain_contains_io(&checkpoint_error));
    assert!(!checkpoint_error.to_string().contains("missing-checkpoint"));
    assert!(!format!("{checkpoint_error:?}").contains("missing-checkpoint"));

    let mut manifest = TimerSnapshotManifest {
        schema_version: TIMER_SNAPSHOT_SCHEMA_VERSION,
        generation: 1,
        source_cq_cursor: 0,
        source_physical_cursor: 0,
        due_time_cursor_ms: 0,
        completion_physical_cursor: 0,
        timeline_sequence: 1,
        timeline_index_kind: TimerTimelineIndexKind::RocksDb,
        native_manifest_generation: None,
        native_durable_end: None,
        native_manifest_checksum: None,
        native_files: Vec::new(),
        role_epoch: 1,
        activation_epoch: 1,
        format_fingerprint: 1,
        timeline_checkpoint_uri: "file:///timeline".into(),
        payload_files: vec![TimerSnapshotFile {
            relative_path: format!("missing-timer-artifact-{}", std::process::id()),
            length: 1,
            sha256: "00".repeat(32),
        }],
        checksum: String::new(),
    };
    manifest.seal().expect("the manifest shape is valid");
    let timer_error = manifest
        .validate_artifact_files(std::env::temp_dir())
        .expect_err("a missing timer artifact must fail");
    assert_eq!(&STORAGE_IO_FAILED, timer_error.descriptor());
    assert!(source_chain_contains_io(&timer_error));
    assert!(!timer_error.to_string().contains("missing-timer"));
    assert!(!format!("{timer_error:?}").contains("missing-timer"));
}

#[test]
fn removed_error_names_are_absent_from_the_public_surface() {
    let sources = [
        include_str!("../src/lib.rs"),
        include_str!("../src/checkpoint.rs"),
        include_str!("../src/checkpoint_artifact.rs"),
        include_str!("../src/ha_contract.rs"),
        include_str!("../src/progress.rs"),
        include_str!("../src/timer.rs"),
        include_str!("../src/timer_snapshot.rs"),
    ];
    let removed = [
        concat!("CheckpointArtifact", "Error"),
        concat!("CheckpointValidation", "Error"),
        concat!("HaContract", "Error"),
        concat!("AppendReceipt", "Error"),
        concat!("DerivedRecordId", "Error"),
        concat!("CursorAdvance", "Error"),
        concat!("DerivedCheckpointDecode", "Error"),
        concat!("TimerSnapshotValidation", "Error"),
        concat!("TimerContract", "Error"),
    ];

    for name in removed {
        assert!(
            sources.iter().all(|source| !source.contains(name)),
            "removed name remains: {name}"
        );
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ContractFamily {
    CheckpointArtifact,
    Checkpoint,
    Ha,
    AppendReceipt,
    DerivedRecord,
    DerivedCursor,
    DerivedCheckpoint,
    TimerSnapshot,
    Timer,
}

fn contract_family(violation: &StoreContractViolation) -> ContractFamily {
    match violation {
        StoreContractViolation::CheckpointArtifactTooLarge { .. }
        | StoreContractViolation::CheckpointArtifactEmpty
        | StoreContractViolation::CheckpointArtifactSymbolicLink(_)
        | StoreContractViolation::CheckpointArtifactPathEscaped(_)
        | StoreContractViolation::CheckpointArtifactUnsupportedFileType(_)
        | StoreContractViolation::CheckpointArtifactUnsupportedUri(_) => ContractFamily::CheckpointArtifact,
        StoreContractViolation::CheckpointSchemaVersion { .. }
        | StoreContractViolation::CheckpointInvalidField { .. }
        | StoreContractViolation::CheckpointInvalidOffsets(_)
        | StoreContractViolation::CheckpointDestructiveRollback
        | StoreContractViolation::CheckpointRestoreVerificationIncomplete => ContractFamily::Checkpoint,
        StoreContractViolation::HaInvalidMasterEpoch(_)
        | StoreContractViolation::HaInvalidSyncStateSetEpoch(_)
        | StoreContractViolation::HaInvalidBrokerId(_)
        | StoreContractViolation::HaBrokerIdOutOfRange(_)
        | StoreContractViolation::HaInvalidReplicaCount(_)
        | StoreContractViolation::HaInvalidAckPolicy(_)
        | StoreContractViolation::HaInvalidOffset(_)
        | StoreContractViolation::HaInvalidLeaseGeneration(_)
        | StoreContractViolation::HaEmptySyncStateSet
        | StoreContractViolation::HaLeaderMissingFromSyncStateSet(_) => ContractFamily::Ha,
        StoreContractViolation::AppendReceiptEmptyRange
        | StoreContractViolation::AppendReceiptReversedRange
        | StoreContractViolation::AppendReceiptRejectedStatusWithRange
        | StoreContractViolation::AppendReceiptAcceptedStatusWithoutRange
        | StoreContractViolation::AppendReceiptAppendedWatermarkBehindRange
        | StoreContractViolation::AppendReceiptDurableWatermarkBehindRange
        | StoreContractViolation::AppendReceiptDurableWatermarkAheadOfAppended
        | StoreContractViolation::AppendReceiptMemoryDurabilityAlreadyCovered
        | StoreContractViolation::AppendReceiptReplicatedDurabilityRequiresDecision
        | StoreContractViolation::AppendReceiptReplicationDecisionBehindRange => ContractFamily::AppendReceipt,
        StoreContractViolation::DerivedRecordEmpty | StoreContractViolation::DerivedRecordRangeOverflow => {
            ContractFamily::DerivedRecord
        }
        StoreContractViolation::DerivedCursorSourceEpochMismatch { .. }
        | StoreContractViolation::DerivedCursorGap { .. }
        | StoreContractViolation::DerivedCursorPartialOverlap { .. } => ContractFamily::DerivedCursor,
        StoreContractViolation::DerivedCheckpointInvalidLength { .. }
        | StoreContractViolation::DerivedCheckpointInvalidMagic
        | StoreContractViolation::DerivedCheckpointUnsupportedVersion(_)
        | StoreContractViolation::DerivedCheckpointUnknownEngine(_)
        | StoreContractViolation::DerivedCheckpointEngineMismatch { .. }
        | StoreContractViolation::DerivedCheckpointInvalidReservedByte(_)
        | StoreContractViolation::DerivedCheckpointChecksumMismatch => ContractFamily::DerivedCheckpoint,
        StoreContractViolation::TimerSnapshotUnsupportedVersion(_)
        | StoreContractViolation::TimerSnapshotInvalidMetadata
        | StoreContractViolation::TimerSnapshotInvalidFile
        | StoreContractViolation::TimerSnapshotChecksumMismatch
        | StoreContractViolation::TimerSnapshotInvalidNativeBinding
        | StoreContractViolation::TimerSnapshotArtifactDigestMismatch => ContractFamily::TimerSnapshot,
        StoreContractViolation::TimerUnknownEngine(_)
        | StoreContractViolation::TimerInvalidPayloadLocator
        | StoreContractViolation::TimerInvalidPayloadStoreLocator
        | StoreContractViolation::TimerInvalidRoute
        | StoreContractViolation::TimerConfigurationOutOfRange { .. } => ContractFamily::Timer,
    }
}

fn source_chain_contains_io(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(source) = current {
        if source.downcast_ref::<std::io::Error>().is_some() {
            return true;
        }
        current = source.source();
    }
    false
}
