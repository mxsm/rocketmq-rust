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

use super::engine::BackendFailure;
use super::engine::NamespaceIo;
use super::engine::NamespaceSnapshot;
use super::types::NamespaceEntry;
use super::types::NamespaceFailureClass;
use super::types::NamespaceOperation;
use super::types::NamespaceRetirementRequest;
use super::types::NamespaceTransition;
use super::types::NamespaceVerificationError;
use crate::mapped_file::retirement::identity::PhysicalFileKey;
use crate::mapped_file::retirement::identity::StoreRelativePath;
use crate::mapped_file::retirement::writer::AllocatedIncarnationReceipt;
use crate::mapped_file::retirement::writer::BoundIncarnationReceipt;

use super::creation::IncarnationCreationError;
use super::creation::IncarnationCreationStage;

const REASON: &str = "no audited handle-relative managed-retirement backend exists for this target";

pub(super) struct NamespaceRoot;

impl NamespaceRoot {
    pub(super) fn open(_file: File) -> Result<Self, NamespaceVerificationError> {
        Err(NamespaceVerificationError::Unsupported {
            platform: "unsupported target",
            reason: REASON,
        })
    }

    pub(super) fn open_active_segment(
        &self,
        _path: &StoreRelativePath,
        _expected_key: PhysicalFileKey,
        _expected_length: u64,
    ) -> Result<File, NamespaceVerificationError> {
        Err(NamespaceVerificationError::Unsupported {
            platform: "unsupported target",
            reason: REASON,
        })
    }

    pub(super) fn reserve(
        &self,
        _request: &NamespaceRetirementRequest,
        _transition: NamespaceTransition,
    ) -> Result<NamespaceReservation, NamespaceVerificationError> {
        Err(NamespaceVerificationError::Unsupported {
            platform: "unsupported target",
            reason: REASON,
        })
    }

    pub(super) fn create_incarnation_temp(
        &self,
        _allocated: &AllocatedIncarnationReceipt,
    ) -> Result<CreatedIncarnationTemp, IncarnationCreationError> {
        Err(IncarnationCreationError::unsupported(
            IncarnationCreationStage::CreateTemp,
            "unsupported target",
            REASON,
        ))
    }

    pub(super) fn publish_bound_incarnation(
        &self,
        _created: CreatedIncarnationTemp,
        _bound: &BoundIncarnationReceipt,
    ) -> Result<(File, PhysicalFileKey), IncarnationCreationError> {
        Err(IncarnationCreationError::unsupported(
            IncarnationCreationStage::RenameNoReplace,
            "unsupported target",
            REASON,
        ))
    }
}

pub(super) enum CreatedIncarnationTemp {}

impl CreatedIncarnationTemp {
    pub(super) const fn physical_key(&self) -> PhysicalFileKey {
        match *self {}
    }
}

pub(super) struct NamespaceReservation;

impl NamespaceIo for NamespaceReservation {
    fn snapshot(
        &mut self,
        _expected_key: PhysicalFileKey,
        _expected_length: u64,
    ) -> Result<NamespaceSnapshot, BackendFailure> {
        Err(unsupported_failure(NamespaceOperation::Reverify))
    }

    fn rename_to_tombstone(&mut self) -> Result<(), BackendFailure> {
        Err(unsupported_failure(NamespaceOperation::Rename))
    }

    fn unlink(&mut self, _entry: NamespaceEntry) -> Result<(), BackendFailure> {
        Err(unsupported_failure(NamespaceOperation::Unlink))
    }

    fn sync_after_namespace(&mut self, _transition: NamespaceTransition) -> Result<(), BackendFailure> {
        Err(unsupported_failure(NamespaceOperation::SyncParentOrHandle))
    }

    fn release_for_reverification(&mut self) {}
}

const fn unsupported_failure(operation: NamespaceOperation) -> BackendFailure {
    BackendFailure::failed(operation, NamespaceFailureClass::OtherIo, None)
}
