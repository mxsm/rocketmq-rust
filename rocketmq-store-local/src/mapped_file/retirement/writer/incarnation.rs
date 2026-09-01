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

use thiserror::Error;

use super::{LedgerIo, ManagedLedgerWriter, WriterError};
use crate::mapped_file::retirement::codec::LedgerRecord;
use crate::mapped_file::retirement::identity::{
    FileIncarnationId, IdentityViolation, PhysicalFileKey, StoreRelativePath,
};

/// Fully validated durable allocation coordinates for one new segment incarnation.
#[derive(Debug)]
pub(in crate::mapped_file::retirement) struct IncarnationAllocationPlan {
    binding: IncarnationBinding,
}

impl IncarnationAllocationPlan {
    /// Validates the canonical segment and create-file names before any ledger I/O is possible.
    pub(in crate::mapped_file::retirement) fn new(
        incarnation: FileIncarnationId,
        segment_offset: u64,
        expected_length: u64,
        create_nonce: [u8; 16],
        canonical_path: StoreRelativePath,
        create_file_path: StoreRelativePath,
    ) -> Result<Self, IncarnationWriteError> {
        if expected_length == 0 {
            return Err(IncarnationWriteError::new(
                IncarnationWriteErrorSource::ZeroExpectedLength,
            ));
        }
        if create_nonce == [0; 16] {
            return Err(IncarnationWriteError::new(IncarnationWriteErrorSource::ZeroCreateNonce));
        }
        canonical_path.validate_create_binding(&create_file_path, incarnation, segment_offset, &create_nonce)?;
        Ok(Self {
            binding: IncarnationBinding {
                incarnation,
                segment_offset,
                expected_length,
                create_nonce,
                canonical_path,
                create_file_path,
            },
        })
    }
}

#[derive(Debug)]
struct IncarnationBinding {
    incarnation: FileIncarnationId,
    segment_offset: u64,
    expected_length: u64,
    create_nonce: [u8; 16],
    canonical_path: StoreRelativePath,
    create_file_path: StoreRelativePath,
}

impl IncarnationBinding {
    fn allocate_record(&self) -> LedgerRecord {
        LedgerRecord::AllocateIncarnation {
            incarnation: self.incarnation,
            segment_offset: self.segment_offset,
            expected_length: self.expected_length,
            create_nonce: self.create_nonce,
            canonical_path: self.canonical_path.clone(),
            create_file_path: self.create_file_path.clone(),
        }
    }

    fn bind_record(&self, physical_key: PhysicalFileKey) -> LedgerRecord {
        LedgerRecord::BindIncarnation {
            incarnation: self.incarnation,
            expected_length: self.expected_length,
            physical_key,
            canonical_path: self.canonical_path.clone(),
            create_file_path: self.create_file_path.clone(),
        }
    }

    fn publish_record(&self, physical_key: PhysicalFileKey) -> LedgerRecord {
        LedgerRecord::PublishIncarnation {
            incarnation: self.incarnation,
            expected_length: self.expected_length,
            physical_key,
            canonical_path: self.canonical_path.clone(),
            create_file_path: self.create_file_path.clone(),
        }
    }
}

/// Non-clone proof that the exact `AllocateIncarnation` unit is durable.
#[derive(Debug)]
pub(in crate::mapped_file::retirement) struct AllocatedIncarnationReceipt {
    binding: IncarnationBinding,
    allocate_sequence: u64,
}

impl AllocatedIncarnationReceipt {
    pub(in crate::mapped_file::retirement) const fn incarnation(&self) -> FileIncarnationId {
        self.binding.incarnation
    }

    pub(in crate::mapped_file::retirement) const fn segment_offset(&self) -> u64 {
        self.binding.segment_offset
    }

    pub(in crate::mapped_file::retirement) const fn expected_length(&self) -> u64 {
        self.binding.expected_length
    }

    pub(in crate::mapped_file::retirement) const fn create_nonce(&self) -> &[u8; 16] {
        &self.binding.create_nonce
    }

    pub(in crate::mapped_file::retirement) const fn canonical_path(&self) -> &StoreRelativePath {
        &self.binding.canonical_path
    }

    pub(in crate::mapped_file::retirement) const fn create_file_path(&self) -> &StoreRelativePath {
        &self.binding.create_file_path
    }

    pub(in crate::mapped_file::retirement) const fn allocate_sequence(&self) -> u64 {
        self.allocate_sequence
    }
}

/// Non-clone proof that `BindIncarnation` immediately followed the matching allocation.
#[derive(Debug)]
pub(in crate::mapped_file::retirement) struct BoundIncarnationReceipt {
    binding: IncarnationBinding,
    physical_key: PhysicalFileKey,
    allocate_sequence: u64,
    bind_sequence: u64,
}

impl BoundIncarnationReceipt {
    pub(in crate::mapped_file::retirement) const fn incarnation(&self) -> FileIncarnationId {
        self.binding.incarnation
    }

    pub(in crate::mapped_file::retirement) const fn segment_offset(&self) -> u64 {
        self.binding.segment_offset
    }

    pub(in crate::mapped_file::retirement) const fn expected_length(&self) -> u64 {
        self.binding.expected_length
    }

    pub(in crate::mapped_file::retirement) const fn create_nonce(&self) -> &[u8; 16] {
        &self.binding.create_nonce
    }

    pub(in crate::mapped_file::retirement) const fn physical_key(&self) -> PhysicalFileKey {
        self.physical_key
    }

    pub(in crate::mapped_file::retirement) const fn canonical_path(&self) -> &StoreRelativePath {
        &self.binding.canonical_path
    }

    pub(in crate::mapped_file::retirement) const fn create_file_path(&self) -> &StoreRelativePath {
        &self.binding.create_file_path
    }

    pub(in crate::mapped_file::retirement) const fn allocate_sequence(&self) -> u64 {
        self.allocate_sequence
    }

    pub(in crate::mapped_file::retirement) const fn bind_sequence(&self) -> u64 {
        self.bind_sequence
    }
}

/// Non-clone proof that allocation, physical-key binding, and publication are all durable.
#[derive(Debug)]
pub(in crate::mapped_file::retirement) struct PublishedIncarnationReceipt {
    binding: IncarnationBinding,
    physical_key: PhysicalFileKey,
    allocate_sequence: u64,
    bind_sequence: u64,
    publish_sequence: u64,
}

impl PublishedIncarnationReceipt {
    pub(in crate::mapped_file::retirement) const fn incarnation(&self) -> FileIncarnationId {
        self.binding.incarnation
    }

    pub(in crate::mapped_file::retirement) const fn segment_offset(&self) -> u64 {
        self.binding.segment_offset
    }

    pub(in crate::mapped_file::retirement) const fn expected_length(&self) -> u64 {
        self.binding.expected_length
    }

    pub(in crate::mapped_file::retirement) const fn create_nonce(&self) -> &[u8; 16] {
        &self.binding.create_nonce
    }

    pub(in crate::mapped_file::retirement) const fn physical_key(&self) -> PhysicalFileKey {
        self.physical_key
    }

    pub(in crate::mapped_file::retirement) const fn canonical_path(&self) -> &StoreRelativePath {
        &self.binding.canonical_path
    }

    pub(in crate::mapped_file::retirement) const fn create_file_path(&self) -> &StoreRelativePath {
        &self.binding.create_file_path
    }

    pub(in crate::mapped_file::retirement) const fn allocate_sequence(&self) -> u64 {
        self.allocate_sequence
    }

    pub(in crate::mapped_file::retirement) const fn bind_sequence(&self) -> u64 {
        self.bind_sequence
    }

    pub(in crate::mapped_file::retirement) const fn publish_sequence(&self) -> u64 {
        self.publish_sequence
    }
}

impl<I: LedgerIo> ManagedLedgerWriter<I> {
    /// Appends the exact allocation coordinates after binding them to this Store writer.
    pub(in crate::mapped_file::retirement) fn append_allocate_incarnation(
        &mut self,
        plan: IncarnationAllocationPlan,
    ) -> Result<AllocatedIncarnationReceipt, IncarnationWriteError> {
        if plan.binding.incarnation.store_uuid() != self.writer.cursor.store_uuid {
            return Err(IncarnationWriteError::new(
                IncarnationWriteErrorSource::StoreUuidMismatch,
            ));
        }
        let receipt = self.writer.append(&plan.binding.allocate_record())?;
        Ok(AllocatedIncarnationReceipt {
            binding: plan.binding,
            allocate_sequence: receipt.sequence(),
        })
    }

    /// Appends the handle-captured physical key only at the allocation's immediate successor.
    pub(in crate::mapped_file::retirement) fn append_bind_incarnation(
        &mut self,
        allocated: AllocatedIncarnationReceipt,
        physical_key: PhysicalFileKey,
    ) -> Result<BoundIncarnationReceipt, IncarnationWriteError> {
        let expected_sequence = immediate_successor(allocated.allocate_sequence)?;
        self.require_next_sequence(expected_sequence)?;
        let receipt = self.writer.append(&allocated.binding.bind_record(physical_key))?;
        Ok(BoundIncarnationReceipt {
            binding: allocated.binding,
            physical_key,
            allocate_sequence: allocated.allocate_sequence,
            bind_sequence: receipt.sequence(),
        })
    }

    /// Appends publication only at the exact binding's immediate successor.
    pub(in crate::mapped_file::retirement) fn append_publish_incarnation(
        &mut self,
        bound: BoundIncarnationReceipt,
    ) -> Result<PublishedIncarnationReceipt, IncarnationWriteError> {
        let expected_sequence = immediate_successor(bound.bind_sequence)?;
        self.require_next_sequence(expected_sequence)?;
        let receipt = self.writer.append(&bound.binding.publish_record(bound.physical_key))?;
        Ok(PublishedIncarnationReceipt {
            binding: bound.binding,
            physical_key: bound.physical_key,
            allocate_sequence: bound.allocate_sequence,
            bind_sequence: bound.bind_sequence,
            publish_sequence: receipt.sequence(),
        })
    }

    fn require_next_sequence(&self, expected: u64) -> Result<(), IncarnationWriteError> {
        let actual = self.writer.cursor.next_sequence();
        if actual != expected {
            return Err(IncarnationWriteError::new(
                IncarnationWriteErrorSource::InterleavedLedgerRecord { expected, actual },
            ));
        }
        Ok(())
    }
}

fn immediate_successor(sequence: u64) -> Result<u64, IncarnationWriteError> {
    sequence
        .checked_add(1)
        .ok_or_else(|| IncarnationWriteError::new(IncarnationWriteErrorSource::SequenceDomainExhausted))
}

/// Failure while validating or durably advancing a creation typestate.
#[derive(Debug, Error)]
#[error(transparent)]
pub(crate) struct IncarnationWriteError {
    source: IncarnationWriteErrorSource,
}

impl IncarnationWriteError {
    fn new(source: IncarnationWriteErrorSource) -> Self {
        Self { source }
    }
}

#[derive(Debug, Error)]
enum IncarnationWriteErrorSource {
    #[error("new incarnation expected length is zero")]
    ZeroExpectedLength,
    #[error("new incarnation create nonce is zero")]
    ZeroCreateNonce,
    #[error("new incarnation belongs to a different Store UUID")]
    StoreUuidMismatch,
    #[error("creation chain sequence domain is exhausted")]
    SequenceDomainExhausted,
    #[error("another ledger record interrupted the creation chain: expected sequence {expected}, found {actual}")]
    InterleavedLedgerRecord { expected: u64, actual: u64 },
    #[error(transparent)]
    Identity(#[from] IdentityViolation),
    #[error(transparent)]
    Writer(#[from] WriterError),
}

impl From<IdentityViolation> for IncarnationWriteError {
    fn from(source: IdentityViolation) -> Self {
        Self::new(IncarnationWriteErrorSource::Identity(source))
    }
}

impl From<WriterError> for IncarnationWriteError {
    fn from(source: WriterError) -> Self {
        Self::new(IncarnationWriteErrorSource::Writer(source))
    }
}

#[cfg(test)]
mod tests {
    use crate::mapped_file::retirement::codec::{decode_next_frame, DecodeOutcome, LedgerRecord, COMMIT_SEAL_LENGTH};
    use crate::mapped_file::retirement::identity::{
        FileIncarnationId, PhysicalFileKey, StoreRelativePath, StoreUuid, TicketId,
    };
    use crate::mapped_file::retirement::writer::model_io::ModelLedgerIo;
    use crate::mapped_file::retirement::writer::{IncarnationAllocationPlan, ManagedLedgerWriter};

    #[test]
    fn allocate_bind_publish_is_an_exact_non_skippable_durable_chain() {
        let mut writer = managed_writer();
        let plan = allocation_plan();
        let physical_key = PhysicalFileKey::unix(17, 29);

        let allocated = writer.append_allocate_incarnation(plan).expect("Allocate is durable");
        let bound = writer
            .append_bind_incarnation(allocated, physical_key)
            .expect("Bind is durable");
        let published = writer.append_publish_incarnation(bound).expect("Publish is durable");

        assert_eq!(published.incarnation(), incarnation());
        assert_eq!(published.segment_offset(), 1_000_000);
        assert_eq!(published.expected_length(), 4096);
        assert_eq!(published.physical_key(), physical_key);
        assert_eq!(published.canonical_path().as_str(), "commitlog/00000000000001000000");

        assert_eq!(
            decode_records(writer.io_for_test().log()),
            [
                LedgerRecord::AllocateIncarnation {
                    incarnation: incarnation(),
                    segment_offset: 1_000_000,
                    expected_length: 4096,
                    create_nonce: [0x5a; 16],
                    canonical_path: canonical_path(),
                    create_file_path: create_path(),
                },
                LedgerRecord::BindIncarnation {
                    incarnation: incarnation(),
                    expected_length: 4096,
                    physical_key,
                    canonical_path: canonical_path(),
                    create_file_path: create_path(),
                },
                LedgerRecord::PublishIncarnation {
                    incarnation: incarnation(),
                    expected_length: 4096,
                    physical_key,
                    canonical_path: canonical_path(),
                    create_file_path: create_path(),
                },
            ]
        );
    }

    #[test]
    fn invalid_allocation_bindings_are_rejected_before_writer_io() {
        let cases = [
            IncarnationAllocationPlan::new(incarnation(), 1_000_000, 0, [0x5a; 16], canonical_path(), create_path()),
            IncarnationAllocationPlan::new(incarnation(), 1_000_000, 4096, [0; 16], canonical_path(), create_path()),
            IncarnationAllocationPlan::new(
                incarnation(),
                2_000_000,
                4096,
                [0x5a; 16],
                canonical_path(),
                create_path(),
            ),
        ];

        for case in cases {
            assert!(case.is_err());
        }

        let foreign = FileIncarnationId::new(StoreUuid::new([0x44; 16]).expect("uuid"), 7).expect("incarnation");
        let plan = IncarnationAllocationPlan::new(
            foreign,
            1_000_000,
            4096,
            [0x5a; 16],
            canonical_path(),
            StoreRelativePath::new(
                "commitlog/.create.i0000000000000007.s00000000000001000000.n5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a",
            )
            .expect("foreign create path"),
        )
        .expect("the plan is internally valid");
        let mut writer = managed_writer();

        assert!(writer.append_allocate_incarnation(plan).is_err());
        assert!(writer.io_for_test().events().is_empty());
    }

    #[test]
    fn an_interleaved_ledger_record_blocks_bind_and_publish_before_io() {
        let physical_key = PhysicalFileKey::unix(17, 29);

        let mut bind_writer = managed_writer();
        let allocated = bind_writer
            .append_allocate_incarnation(allocation_plan())
            .expect("Allocate is durable");
        bind_writer
            .writer
            .append(&unrelated_record())
            .expect("unrelated record is durable");
        let before_bind = bind_writer.io_for_test().events().len();
        assert!(bind_writer.append_bind_incarnation(allocated, physical_key).is_err());
        assert_eq!(bind_writer.io_for_test().events().len(), before_bind);

        let mut publish_writer = managed_writer();
        let allocated = publish_writer
            .append_allocate_incarnation(allocation_plan())
            .expect("Allocate is durable");
        let bound = publish_writer
            .append_bind_incarnation(allocated, physical_key)
            .expect("Bind is durable");
        publish_writer
            .writer
            .append(&unrelated_record())
            .expect("unrelated record is durable");
        let before_publish = publish_writer.io_for_test().events().len();
        assert!(publish_writer.append_publish_incarnation(bound).is_err());
        assert_eq!(publish_writer.io_for_test().events().len(), before_publish);
    }

    #[test]
    fn creation_receipts_are_non_clone_typestate_capabilities() {
        let source = include_str!("incarnation.rs");
        let production = source
            .split_once("#[cfg(test)]")
            .expect("tests follow the production capability code")
            .0;

        for capability in [
            "AllocatedIncarnationReceipt",
            "BoundIncarnationReceipt",
            "PublishedIncarnationReceipt",
        ] {
            let marker = format!("struct {capability}");
            let declaration = production
                .split_once(&marker)
                .unwrap_or_else(|| panic!("missing capability declaration {capability}"))
                .0
                .lines()
                .rev()
                .take(3)
                .collect::<Vec<_>>()
                .join("\n");
            assert!(!declaration.contains("Clone"), "{capability} must not be Clone");
            assert!(!production.contains(&format!("pub(crate) struct {capability}")));
        }
        assert!(production.contains("bound: BoundIncarnationReceipt"));
        assert!(!production.contains("plan: IncarnationAllocationPlan,\n    ) -> Result<PublishedIncarnationReceipt"));
    }

    fn managed_writer() -> ManagedLedgerWriter<ModelLedgerIo> {
        ManagedLedgerWriter::for_test(ModelLedgerIo::empty(), store_uuid(), [0x33; 16], 2, 100, 77, 0, true, 5)
            .expect("managed writer")
    }

    fn allocation_plan() -> IncarnationAllocationPlan {
        IncarnationAllocationPlan::new(
            incarnation(),
            1_000_000,
            4096,
            [0x5a; 16],
            canonical_path(),
            create_path(),
        )
        .expect("valid allocation plan")
    }

    fn store_uuid() -> StoreUuid {
        StoreUuid::new([0x22; 16]).expect("uuid")
    }

    fn incarnation() -> FileIncarnationId {
        FileIncarnationId::new(store_uuid(), 7).expect("incarnation")
    }

    fn canonical_path() -> StoreRelativePath {
        StoreRelativePath::new("commitlog/00000000000001000000").expect("canonical path")
    }

    fn create_path() -> StoreRelativePath {
        StoreRelativePath::new(
            "commitlog/.create.i0000000000000007.s00000000000001000000.n5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a",
        )
        .expect("create path")
    }

    fn unrelated_record() -> LedgerRecord {
        LedgerRecord::Completed {
            ticket_id: TicketId::new(41).expect("ticket"),
            incarnation: incarnation(),
            completion_time_ns: 43,
            namespace_absent_sequence: 9,
        }
    }

    fn decode_records(log: &[u8]) -> Vec<LedgerRecord> {
        let mut records = Vec::new();
        let mut offset = 0;
        let mut sequence = 100;
        while offset < log.len() {
            let DecodeOutcome::Frame(frame) =
                decode_next_frame(&log[offset..], sequence, 2).expect("frame envelope decodes")
            else {
                panic!("writer log must contain a complete frame");
            };
            let frame_len = frame.encoded_len();
            records.push(
                frame
                    .decode_record()
                    .expect("typed record decodes")
                    .expect("writer only emits known records"),
            );
            offset += frame_len + COMMIT_SEAL_LENGTH;
            sequence += 1;
        }
        records
    }
}
