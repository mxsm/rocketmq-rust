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

use std::error::Error as StdError;
use std::fmt;

use rocketmq_store_api::CursorAdvanceDisposition;
use rocketmq_store_api::DerivedCheckpoint;
use rocketmq_store_api::DerivedCursor;
use rocketmq_store_api::DerivedEngine;
use rocketmq_store_api::DerivedRecordId;
use rocketmq_store_api::LegacyDerivedCursorV0;
use rocketmq_store_api::StoreContractViolation;
use rocketmq_store_api::StoreError;
use rocketmq_store_api::StoreOperation;
use rocketmq_store_api::DERIVED_CHECKPOINT_ENCODED_LEN;

/// Persistence boundary owned independently by one derived engine.
///
/// Implementations must atomically and durably replace only the metadata bytes for `engine`
/// before returning `Ok(())`. They must not copy message payload or create a second WAL. Blocking
/// implementations must be invoked from the repository's owned blocking boundary.
pub trait CheckpointPersistence {
    type Error: StdError + Send + Sync + 'static;

    /// Loads the last durably committed metadata for `engine`.
    ///
    /// # Errors
    ///
    /// Returns a typed storage error without guessing or repairing the cursor.
    fn load(&self, engine: DerivedEngine) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Atomically replaces the durable metadata for `engine`.
    ///
    /// # Errors
    ///
    /// Returns a typed storage error if durability cannot be established. A caller must not
    /// publish the next cursor after an error, even when the write outcome is uncertain.
    fn persist(
        &mut self,
        engine: DerivedEngine,
        checkpoint: &[u8; DERIVED_CHECKPOINT_ENCODED_LEN],
    ) -> Result<(), Self::Error>;
}

/// Sole publisher of one engine's in-memory and durable cursor.
pub struct DerivedCursorOwner<P> {
    engine: DerivedEngine,
    cursor: DerivedCursor,
    persistence: P,
}

impl<P: CheckpointPersistence> DerivedCursorOwner<P> {
    /// Loads a current checkpoint or starts at the genesis cursor for `source_epoch`.
    ///
    /// # Errors
    ///
    /// Fails closed on persistence or checkpoint validation errors.
    pub fn open(engine: DerivedEngine, source_epoch: u64, persistence: P) -> Result<Self, StoreError> {
        Self::open_or_upgrade(engine, source_epoch, persistence, None)
    }

    /// Loads the current checkpoint, or durably upgrades a proven version-zero offset once.
    ///
    /// The legacy value is ignored when a current checkpoint already exists.
    ///
    /// # Errors
    ///
    /// Fails closed on persistence or checkpoint validation errors. The upgraded cursor is not
    /// published until the current checkpoint bytes have been durably persisted.
    #[allow(dead_code, reason = "exercised by the in-crate replay harness")]
    pub(crate) fn open_or_upgrade(
        engine: DerivedEngine,
        source_epoch: u64,
        persistence: P,
        legacy: Option<LegacyDerivedCursorV0>,
    ) -> Result<Self, StoreError> {
        Self::open_or_upgrade_checked(engine, source_epoch, persistence, legacy).map_err(owner_store_error)
    }

    fn open_or_upgrade_checked(
        engine: DerivedEngine,
        source_epoch: u64,
        mut persistence: P,
        legacy: Option<LegacyDerivedCursorV0>,
    ) -> Result<Self, DerivedCursorOwnerFailure<P::Error>> {
        let cursor = match persistence.load(engine).map_err(DerivedCursorOwnerFailure::read)? {
            Some(encoded) => DerivedCheckpoint::decode(&encoded, engine)
                .map_err(DerivedCursorOwnerFailure::Checkpoint)?
                .cursor(),
            None => match legacy {
                Some(legacy) => {
                    let checkpoint = DerivedCheckpoint::upgrade_legacy(engine, source_epoch, legacy);
                    persistence
                        .persist(engine, &checkpoint.encode())
                        .map_err(DerivedCursorOwnerFailure::persist)?;
                    checkpoint.cursor()
                }
                None => DerivedCursor::genesis(source_epoch),
            },
        };

        if cursor.source_epoch() != source_epoch {
            return Err(DerivedCursorOwnerFailure::SourceEpochMismatch {
                expected: source_epoch,
                actual: cursor.source_epoch(),
            });
        }

        Ok(Self {
            engine,
            cursor,
            persistence,
        })
    }

    /// Returns the engine that exclusively owns this checkpoint.
    pub const fn engine(&self) -> DerivedEngine {
        self.engine
    }

    /// Returns the last cursor known to have completed durable metadata commit.
    pub const fn cursor(&self) -> DerivedCursor {
        self.cursor
    }

    /// Returns the persistence adapter for diagnostics and controlled shutdown.
    pub const fn persistence(&self) -> &P {
        &self.persistence
    }

    /// Consumes the owner and returns its persistence adapter.
    pub fn into_persistence(self) -> P {
        self.persistence
    }

    /// Durably commits one contiguous record and only then publishes the new cursor.
    ///
    /// # Errors
    ///
    /// Returns `Ok(None)` for a deterministic cursor invariant rejection. Persistence failures are
    /// operational errors. In either case the in-memory cursor remains unchanged; an uncertain
    /// persistence outcome is resolved by reloading on restart.
    pub fn commit(&mut self, record: DerivedRecordId) -> Result<Option<DerivedCommitOutcome>, StoreError> {
        match self.commit_checked(record) {
            Ok(outcome) => Ok(Some(outcome)),
            Err(DerivedCursorOwnerFailure::Cursor(_)) => Ok(None),
            Err(error) => Err(owner_store_error(error)),
        }
    }

    pub(super) fn commit_checked(
        &mut self,
        record: DerivedRecordId,
    ) -> Result<DerivedCommitOutcome, DerivedCursorOwnerFailure<P::Error>> {
        let advance = match self.cursor.prepare(record).map_err(DerivedCursorOwnerFailure::Cursor)? {
            CursorAdvanceDisposition::AlreadyCommitted => return Ok(DerivedCommitOutcome::AlreadyCommitted),
            CursorAdvanceDisposition::Advance(advance) => advance,
        };

        let next_cursor = advance.next_cursor();
        let checkpoint = DerivedCheckpoint::new(self.engine, next_cursor).encode();
        self.persistence
            .persist(self.engine, &checkpoint)
            .map_err(DerivedCursorOwnerFailure::persist)?;
        self.cursor = next_cursor;
        Ok(DerivedCommitOutcome::Committed(next_cursor))
    }
}

/// Outcome of a durable cursor commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DerivedCommitOutcome {
    AlreadyCommitted,
    Committed(DerivedCursor),
}

/// Failure while loading, validating, or committing a derived cursor.
#[derive(Debug)]
pub(crate) enum DerivedCursorOwnerFailure<E> {
    Read(E),
    Persist(E),
    Store(StoreError),
    Checkpoint(StoreContractViolation),
    Cursor(StoreContractViolation),
    SourceEpochMismatch { expected: u64, actual: u64 },
}

impl<E: fmt::Display> fmt::Display for DerivedCursorOwnerFailure<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(error) => write!(formatter, "derived checkpoint load failed: {error}"),
            Self::Persist(error) => write!(formatter, "derived checkpoint persistence failed: {error}"),
            Self::Store(error) => write!(formatter, "derived checkpoint storage failed: {error}"),
            Self::Checkpoint(error) => write!(formatter, "derived checkpoint validation failed: {error}"),
            Self::Cursor(error) => write!(formatter, "derived cursor advance failed: {error}"),
            Self::SourceEpochMismatch { expected, actual } => write!(
                formatter,
                "derived checkpoint source epoch mismatch: expected {expected}, got {actual}"
            ),
        }
    }
}

impl<E: StdError + 'static> StdError for DerivedCursorOwnerFailure<E> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Read(error) | Self::Persist(error) => Some(error),
            Self::Store(error) => Some(error),
            Self::Checkpoint(error) => Some(error),
            Self::Cursor(error) => Some(error),
            Self::SourceEpochMismatch { .. } => None,
        }
    }
}

impl<E: StdError + Send + Sync + 'static> DerivedCursorOwnerFailure<E> {
    fn read(error: E) -> Self {
        Self::external(error, Self::Read)
    }

    fn persist(error: E) -> Self {
        Self::external(error, Self::Persist)
    }

    fn external(error: E, wrap: impl FnOnce(E) -> Self) -> Self {
        let error: Box<dyn StdError + Send + Sync> = Box::new(error);
        match error.downcast::<StoreError>() {
            Ok(error) => Self::Store(*error),
            Err(error) => wrap(*error.downcast::<E>().expect("boxed source retains its concrete type")),
        }
    }
}

fn owner_store_error<E>(error: DerivedCursorOwnerFailure<E>) -> StoreError
where
    E: StdError + Send + Sync + 'static,
{
    if let DerivedCursorOwnerFailure::Store(error) = error {
        return error;
    }
    let (descriptor, operation) = match &error {
        DerivedCursorOwnerFailure::Read(_) => (&rocketmq_error::STORAGE_READ_FAILED, StoreOperation::Read),
        DerivedCursorOwnerFailure::Persist(_) => (&rocketmq_error::STORAGE_WRITE_FAILED, StoreOperation::AppendDerived),
        DerivedCursorOwnerFailure::Checkpoint(_) | DerivedCursorOwnerFailure::SourceEpochMismatch { .. } => {
            (&rocketmq_error::STORAGE_STATE_CORRUPTED, StoreOperation::Read)
        }
        DerivedCursorOwnerFailure::Cursor(_) => {
            unreachable!("derived cursor contract violations remain on the contract channel")
        }
        DerivedCursorOwnerFailure::Store(_) => unreachable!("contained StoreError returned above"),
    };
    StoreError::new(descriptor, operation)
        .in_component(rocketmq_store_api::StoreComponent::Store)
        .with_source(error)
}
