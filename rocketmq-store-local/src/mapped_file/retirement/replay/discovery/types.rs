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

use std::fs::File;
use std::sync::Arc;

use thiserror::Error;

use super::platform;
use super::RecoveryDecision;
use super::ReplayLimits;
use super::MAX_DIRECTORY_ENTRIES;
use super::MAX_LOG_FILE_LENGTH;
use super::MAX_TOTAL_READ_BYTES;
use crate::mapped_file::retirement::codec::CodecError;
use crate::mapped_file::retirement::identity::StoreUuid;
use crate::mapped_file::retirement::replay::ReplayError;
use crate::mapped_file::retirement::sidecar::SidecarError;

/// Store-local, read-only classification of managed lifecycle evidence.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedLifecycleReadOutcome {
    LegacyAbsent,
    ManagedNeedsReconciliation,
    RecoveryWriteRequired(ManagedLifecycleRecoveryReason),
}

/// A write-side recovery protocol identified without executing it.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedLifecycleRecoveryReason {
    BootstrapResume,
    AcknowledgeSelectedAnchor,
    CompleteSeal,
    CompleteMarkerWitness,
    TailRepair,
    ResumeGeneration,
    TemporaryArtifact,
}

/// Stable managed-lifecycle evidence that owns its exclusive Store-root lease proof.
///
/// The session is deliberately non-Clone and does not expose decoded sidecars, replay state, or
/// the retained root handle. The opaque keepalive is supplied only through the unsafe inspection
/// boundary and keeps the exact root lock alive until the session and every derived capability are
/// dropped.
#[doc(hidden)]
pub struct ManagedLifecycleSession {
    retained_root: File,
    _exclusive_lease: Arc<dyn Send + Sync>,
    outcome: ManagedLifecycleReadOutcome,
    store_uuid: Option<StoreUuid>,
    decision: Option<RecoveryDecision>,
}

impl std::fmt::Debug for ManagedLifecycleSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ManagedLifecycleSession")
            .field("outcome", &self.outcome)
            .field("has_store_uuid", &self.store_uuid.is_some())
            .field("has_replay_decision", &self.decision.is_some())
            .finish_non_exhaustive()
    }
}

impl ManagedLifecycleSession {
    pub(super) fn new(
        retained_root: File,
        exclusive_lease: Arc<dyn Send + Sync>,
        outcome: ManagedLifecycleReadOutcome,
        store_uuid: Option<StoreUuid>,
        decision: Option<RecoveryDecision>,
    ) -> Self {
        Self {
            retained_root,
            _exclusive_lease: exclusive_lease,
            outcome,
            store_uuid,
            decision,
        }
    }

    /// Returns the read-only disposition without exposing replay or namespace authority.
    #[doc(hidden)]
    pub const fn outcome(&self) -> ManagedLifecycleReadOutcome {
        self.outcome
    }

    pub(in crate::mapped_file::retirement) const fn store_uuid(&self) -> Option<StoreUuid> {
        self.store_uuid
    }

    pub(in crate::mapped_file::retirement) const fn decision(&self) -> Option<&RecoveryDecision> {
        self.decision.as_ref()
    }

    pub(in crate::mapped_file::retirement) const fn retained_root(&self) -> &File {
        &self.retained_root
    }
}

/// Result of inspecting lifecycle state while an exclusive Store-root lease is retained.
#[doc(hidden)]
#[derive(Debug)]
pub enum LockedManagedLifecycleInspection {
    LegacyAbsent,
    Managed(Box<ManagedLifecycleSession>),
}

impl LockedManagedLifecycleInspection {
    /// Returns the same stable disposition as the read-only inspector.
    #[doc(hidden)]
    pub const fn outcome(&self) -> ManagedLifecycleReadOutcome {
        match self {
            Self::LegacyAbsent => ManagedLifecycleReadOutcome::LegacyAbsent,
            Self::Managed(session) => session.outcome(),
        }
    }
}

/// Stable category for a failed read-only lifecycle inspection.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedLifecycleReadErrorKind {
    Io,
    UnsafeNamespace,
    InventoryChanged,
    LimitExceeded,
    Corruption,
    UnknownVersionCorruption,
    UnsupportedPlatform,
}

/// Caller-selected work bounds for read-only discovery and replay.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManagedLifecycleReadLimits {
    pub max_directory_entries: usize,
    pub max_generations: usize,
    pub max_sealed_units: usize,
    pub max_log_file_length: u64,
    pub max_total_read_bytes: u64,
}

impl Default for ManagedLifecycleReadLimits {
    fn default() -> Self {
        Self {
            max_directory_entries: MAX_DIRECTORY_ENTRIES,
            max_generations: ReplayLimits::default().max_generations,
            max_sealed_units: ReplayLimits::default().max_sealed_units,
            max_log_file_length: MAX_LOG_FILE_LENGTH,
            max_total_read_bytes: MAX_TOTAL_READ_BYTES,
        }
    }
}

impl ManagedLifecycleReadLimits {
    pub(super) fn validate(self) -> Result<Self, ManagedLifecycleReadError> {
        for (name, value) in [
            ("max_directory_entries", self.max_directory_entries),
            ("max_generations", self.max_generations),
            ("max_sealed_units", self.max_sealed_units),
        ] {
            if value == 0 {
                return Err(limit_error(name, 0, 1));
            }
        }
        if self.max_log_file_length == 0 {
            return Err(limit_error("max_log_file_length", 0, 1));
        }
        if self.max_total_read_bytes == 0 {
            return Err(limit_error("max_total_read_bytes", 0, 1));
        }
        Ok(self)
    }
}

#[derive(Debug, Error)]
pub(super) enum ManagedLifecycleReadSource {
    #[error(transparent)]
    Platform(#[from] platform::PlatformError),
    #[error("filesystem read failed: {0}")]
    Io(#[source] std::io::Error),
    #[error("sidecar decode failed: {0}")]
    Sidecar(#[source] SidecarError),
    #[error("ledger codec failed: {0}")]
    Codec(#[source] CodecError),
    #[error("ledger replay failed: {0}")]
    Replay(#[source] ReplayError),
    #[error("lifecycle evidence is corrupt: {0}")]
    Corruption(String),
    #[error("unsafe lifecycle namespace: {0}")]
    UnsafeNamespace(String),
    #[error("unsupported lifecycle format version: {0}")]
    UnknownVersion(String),
    #[error("lifecycle inventory changed: {0}")]
    InventoryChanged(String),
    #[error("lifecycle discovery limit exceeded: {0}")]
    Limit(String),
}

/// Failure from bounded, handle-relative lifecycle discovery and replay.
#[doc(hidden)]
#[derive(Debug, Error)]
#[error("managed lifecycle read failed ({kind:?}): {source}")]
pub struct ManagedLifecycleReadError {
    kind: ManagedLifecycleReadErrorKind,
    #[source]
    source: ManagedLifecycleReadSource,
}

impl ManagedLifecycleReadError {
    /// Returns the stable failure category without exposing decoded sidecars or inventory proof.
    #[doc(hidden)]
    pub const fn kind(&self) -> ManagedLifecycleReadErrorKind {
        self.kind
    }

    pub(super) fn new(kind: ManagedLifecycleReadErrorKind, source: ManagedLifecycleReadSource) -> Self {
        Self { kind, source }
    }
}

pub(super) fn corruption(detail: impl Into<String>) -> ManagedLifecycleReadError {
    ManagedLifecycleReadError::new(
        ManagedLifecycleReadErrorKind::Corruption,
        ManagedLifecycleReadSource::Corruption(detail.into()),
    )
}

pub(super) fn io_error(error: std::io::Error) -> ManagedLifecycleReadError {
    ManagedLifecycleReadError::new(ManagedLifecycleReadErrorKind::Io, ManagedLifecycleReadSource::Io(error))
}

pub(super) fn limit_error(
    resource: &'static str,
    actual: impl std::fmt::Display,
    maximum: impl std::fmt::Display,
) -> ManagedLifecycleReadError {
    ManagedLifecycleReadError::new(
        ManagedLifecycleReadErrorKind::LimitExceeded,
        ManagedLifecycleReadSource::Limit(format!("{resource} bound exceeded: actual {actual}, maximum {maximum}")),
    )
}
