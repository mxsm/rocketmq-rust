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
use std::io;

use thiserror::Error;

use super::native;
use super::NamespaceVerificationError;
use super::VerifiedNamespaceRoot;
use crate::mapped_file::retirement::identity::PhysicalFileKey;
use crate::mapped_file::retirement::writer::{AllocatedIncarnationReceipt, BoundIncarnationReceipt};

/// Durable-creation filesystem stage associated with a typed failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::mapped_file::retirement) enum IncarnationCreationStage {
    OpenParent,
    VerifyNames,
    CreateTemp,
    SizeTemp,
    SyncTemp,
    CapturePhysicalKey,
    RenameNoReplace,
    #[allow(
        dead_code,
        reason = "Linux fsyncs the retained parent; Windows uses reopen verification"
    )]
    SyncParent,
    ReopenCanonical,
    VerifyCanonical,
}

/// Handle-relative creation failure that never loses the underlying OS error.
#[derive(Debug, Error)]
#[error("managed incarnation creation failed during {stage:?}: {source}")]
pub(in crate::mapped_file::retirement) struct IncarnationCreationError {
    stage: IncarnationCreationStage,
    #[source]
    source: IncarnationCreationErrorSource,
}

#[derive(Debug, Error)]
enum IncarnationCreationErrorSource {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Namespace(#[from] NamespaceVerificationError),
    #[error("{0}")]
    Policy(&'static str),
    #[allow(
        dead_code,
        reason = "the typed unsupported source is constructed only by non-Linux native backends"
    )]
    #[error("platform {platform} does not support managed creation: {reason}")]
    Unsupported {
        platform: &'static str,
        reason: &'static str,
    },
}

impl IncarnationCreationError {
    pub(super) fn io(stage: IncarnationCreationStage, source: io::Error) -> Self {
        Self {
            stage,
            source: source.into(),
        }
    }

    pub(super) fn policy(stage: IncarnationCreationStage, reason: &'static str) -> Self {
        Self {
            stage,
            source: IncarnationCreationErrorSource::Policy(reason),
        }
    }

    pub(super) fn namespace(stage: IncarnationCreationStage, source: NamespaceVerificationError) -> Self {
        Self {
            stage,
            source: IncarnationCreationErrorSource::Namespace(source),
        }
    }

    #[allow(
        dead_code,
        reason = "the typed unsupported constructor is used only by non-Linux native backends"
    )]
    pub(super) fn unsupported(stage: IncarnationCreationStage, platform: &'static str, reason: &'static str) -> Self {
        Self {
            stage,
            source: IncarnationCreationErrorSource::Unsupported { platform, reason },
        }
    }

    #[cfg(test)]
    pub(in crate::mapped_file::retirement) const fn stage(&self) -> IncarnationCreationStage {
        self.stage
    }
}

/// Non-clone native temporary-file capability created only after durable allocation.
pub(in crate::mapped_file::retirement) struct CreatedIncarnationTemp {
    native: native::CreatedIncarnationTemp,
}

impl std::fmt::Debug for CreatedIncarnationTemp {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("CreatedIncarnationTemp").finish_non_exhaustive()
    }
}

impl CreatedIncarnationTemp {
    /// Returns the key captured from the still-open, synced create-file handle.
    pub(in crate::mapped_file::retirement) const fn physical_key(&self) -> PhysicalFileKey {
        self.native.physical_key()
    }
}

/// Canonical handle reopened and verified after the no-replace namespace transition.
#[derive(Debug)]
pub(in crate::mapped_file::retirement) struct VerifiedCreatedIncarnation {
    file: File,
    physical_key: PhysicalFileKey,
}

impl VerifiedCreatedIncarnation {
    pub(in crate::mapped_file::retirement) const fn physical_key(&self) -> PhysicalFileKey {
        self.physical_key
    }

    pub(in crate::mapped_file::retirement) fn into_file(self) -> File {
        self.file
    }
}

impl VerifiedNamespaceRoot {
    /// Creates, sizes, syncs, and key-binds the unique create-file name.
    pub(in crate::mapped_file::retirement) fn create_incarnation_temp(
        &self,
        allocated: &AllocatedIncarnationReceipt,
    ) -> Result<CreatedIncarnationTemp, IncarnationCreationError> {
        if allocated.incarnation().store_uuid() != self.store_uuid {
            return Err(IncarnationCreationError::policy(
                IncarnationCreationStage::VerifyNames,
                "allocation belongs to a different Store UUID",
            ));
        }
        self.native
            .create_incarnation_temp(allocated)
            .map(|native| CreatedIncarnationTemp { native })
    }

    /// Publishes the exact bound temp file and returns a reopened canonical handle.
    pub(in crate::mapped_file::retirement) fn publish_bound_incarnation(
        &self,
        created: CreatedIncarnationTemp,
        bound: &BoundIncarnationReceipt,
    ) -> Result<VerifiedCreatedIncarnation, IncarnationCreationError> {
        if bound.incarnation().store_uuid() != self.store_uuid {
            return Err(IncarnationCreationError::policy(
                IncarnationCreationStage::VerifyNames,
                "binding belongs to a different Store UUID",
            ));
        }
        let (file, physical_key) = self.native.publish_bound_incarnation(created.native, bound)?;
        Ok(VerifiedCreatedIncarnation { file, physical_key })
    }
}
