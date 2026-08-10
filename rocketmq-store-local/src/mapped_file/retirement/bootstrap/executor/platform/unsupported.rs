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

use super::InitialBootstrapFoundationError;
use super::PreparedInitialBootstrapFoundation;
use crate::mapped_file::retirement::bootstrap::types::ImmutableArtifactProgress;
use crate::mapped_file::retirement::bootstrap::types::ImmutableArtifactStep;
use crate::mapped_file::retirement::bootstrap::types::InitialMarkerProgress;
use crate::mapped_file::retirement::bootstrap::types::InitialMarkerStep;
use crate::mapped_file::retirement::bootstrap::types::PlannedInitialMarker;
use crate::mapped_file::retirement::bootstrap::types::PlannedSnapshot;
use crate::mapped_file::retirement::sidecar::StoreMeta;

pub(super) struct InitialArtifactStore;

impl InitialArtifactStore {
    pub(super) fn inspect_snapshot(
        &self,
        _planned: &PlannedSnapshot,
    ) -> Result<ImmutableArtifactProgress, InitialBootstrapFoundationError> {
        Err(unsupported())
    }

    pub(super) fn advance_snapshot(
        &mut self,
        _planned: &PlannedSnapshot,
        _step: ImmutableArtifactStep,
    ) -> Result<(), InitialBootstrapFoundationError> {
        Err(unsupported())
    }

    pub(super) fn inspect_initial_marker(
        &self,
        _planned: &PlannedInitialMarker,
    ) -> Result<InitialMarkerProgress, InitialBootstrapFoundationError> {
        Err(unsupported())
    }

    pub(super) fn advance_initial_marker(
        &mut self,
        _planned: &PlannedInitialMarker,
        _step: InitialMarkerStep,
    ) -> Result<(), InitialBootstrapFoundationError> {
        Err(unsupported())
    }
}

pub(super) fn prepare(
    _store_root: File,
    _expected_meta: &StoreMeta,
) -> Result<PreparedInitialBootstrapFoundation, InitialBootstrapFoundationError> {
    Err(unsupported())
}

fn unsupported() -> InitialBootstrapFoundationError {
    InitialBootstrapFoundationError::unsupported(
        "this target lacks a qualified handle-relative initial-bootstrap writer",
    )
}
