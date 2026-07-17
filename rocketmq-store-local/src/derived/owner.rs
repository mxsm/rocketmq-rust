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
use rocketmq_store_api::CursorAdvanceError;
use rocketmq_store_api::DerivedCheckpoint;
use rocketmq_store_api::DerivedCheckpointDecodeError;
use rocketmq_store_api::DerivedCursor;
use rocketmq_store_api::DerivedEngine;
use rocketmq_store_api::DerivedRecordId;
use rocketmq_store_api::LegacyDerivedCursorV0;
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
    pub fn open(
        engine: DerivedEngine,
        source_epoch: u64,
        persistence: P,
    ) -> Result<Self, DerivedCursorOwnerError<P::Error>> {
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
    pub fn open_or_upgrade(
        engine: DerivedEngine,
        source_epoch: u64,
        mut persistence: P,
        legacy: Option<LegacyDerivedCursorV0>,
    ) -> Result<Self, DerivedCursorOwnerError<P::Error>> {
        let cursor = match persistence.load(engine).map_err(DerivedCursorOwnerError::Persistence)? {
            Some(encoded) => DerivedCheckpoint::decode(&encoded, engine)
                .map_err(DerivedCursorOwnerError::Checkpoint)?
                .cursor(),
            None => match legacy {
                Some(legacy) => {
                    let checkpoint = DerivedCheckpoint::upgrade_legacy(engine, source_epoch, legacy);
                    persistence
                        .persist(engine, &checkpoint.encode())
                        .map_err(DerivedCursorOwnerError::Persistence)?;
                    checkpoint.cursor()
                }
                None => DerivedCursor::genesis(source_epoch),
            },
        };

        if cursor.source_epoch() != source_epoch {
            return Err(DerivedCursorOwnerError::SourceEpochMismatch {
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
    /// Fails for a cursor invariant or persistence error. On failure the in-memory cursor remains
    /// unchanged; an uncertain persistence outcome is resolved by reloading on restart.
    pub fn commit(
        &mut self,
        record: DerivedRecordId,
    ) -> Result<DerivedCommitOutcome, DerivedCursorOwnerError<P::Error>> {
        let advance = match self.cursor.prepare(record).map_err(DerivedCursorOwnerError::Cursor)? {
            CursorAdvanceDisposition::AlreadyCommitted => return Ok(DerivedCommitOutcome::AlreadyCommitted),
            CursorAdvanceDisposition::Advance(advance) => advance,
        };

        let next_cursor = advance.next_cursor();
        let checkpoint = DerivedCheckpoint::new(self.engine, next_cursor).encode();
        self.persistence
            .persist(self.engine, &checkpoint)
            .map_err(DerivedCursorOwnerError::Persistence)?;
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
pub enum DerivedCursorOwnerError<E> {
    Persistence(E),
    Checkpoint(DerivedCheckpointDecodeError),
    Cursor(CursorAdvanceError),
    SourceEpochMismatch { expected: u64, actual: u64 },
}

impl<E: fmt::Display> fmt::Display for DerivedCursorOwnerError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Persistence(error) => write!(formatter, "derived checkpoint persistence failed: {error}"),
            Self::Checkpoint(error) => write!(formatter, "derived checkpoint validation failed: {error}"),
            Self::Cursor(error) => write!(formatter, "derived cursor advance failed: {error}"),
            Self::SourceEpochMismatch { expected, actual } => write!(
                formatter,
                "derived checkpoint source epoch mismatch: expected {expected}, got {actual}"
            ),
        }
    }
}

impl<E: StdError + 'static> StdError for DerivedCursorOwnerError<E> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Persistence(error) => Some(error),
            Self::Checkpoint(error) => Some(error),
            Self::Cursor(error) => Some(error),
            Self::SourceEpochMismatch { .. } => None,
        }
    }
}
