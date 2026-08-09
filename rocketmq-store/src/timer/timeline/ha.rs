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

use std::sync::Arc;

use parking_lot::RwLock;
use rocketmq_store_api::TimerSnapshotManifest;
use thiserror::Error;

use crate::timer::clock::TimerClockSafety;
use crate::timer::clock::TimerClockState;

/// All replicated and derived boundaries required before a follower may deliver.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct TimelinePromotionObservation {
    pub(crate) source_retention_start: i64,
    pub(crate) source_replay_cursor: i64,
    pub(crate) replicated_source_end: i64,
    pub(crate) final_retention_start: i64,
    pub(crate) completion_replay_cursor: i64,
    pub(crate) replicated_final_end: i64,
    pub(crate) materialization_backlog: u64,
    pub(crate) due_backlog: u64,
    pub(crate) completion_backlog: u64,
    pub(crate) role_epoch: u64,
    pub(crate) activation_epoch: u64,
    pub(crate) format_fingerprint: u64,
    pub(crate) capability_version: u16,
}

/// Fail-closed Extended promotion decision, including both retention streams.
pub(crate) struct TimelinePromotionGate {
    expected_activation_epoch: u64,
    expected_format_fingerprint: u64,
    expected_capability_version: u16,
    clock: Arc<TimerClockSafety>,
    installed_snapshot: RwLock<Option<TimerSnapshotManifest>>,
}

impl TimelinePromotionGate {
    pub(crate) fn new(
        expected_activation_epoch: u64,
        expected_format_fingerprint: u64,
        expected_capability_version: u16,
        clock: Arc<TimerClockSafety>,
    ) -> Self {
        Self {
            expected_activation_epoch,
            expected_format_fingerprint,
            expected_capability_version,
            clock,
            installed_snapshot: RwLock::new(None),
        }
    }

    pub(crate) fn mark_snapshot_installed(
        &self,
        manifest: TimerSnapshotManifest,
    ) -> Result<(), TimelinePromotionError> {
        manifest.validate()?;
        if manifest.activation_epoch != self.expected_activation_epoch
            || manifest.format_fingerprint != self.expected_format_fingerprint
        {
            return Err(TimelinePromotionError::SnapshotCompatibility);
        }
        let mut installed = self.installed_snapshot.write();
        if installed
            .as_ref()
            .is_some_and(|current| current.generation >= manifest.generation)
        {
            return Err(TimelinePromotionError::StaleSnapshot);
        }
        *installed = Some(manifest);
        Ok(())
    }

    pub(crate) fn snapshot_generation(&self) -> u64 {
        self.installed_snapshot
            .read()
            .as_ref()
            .map_or(0, |manifest| manifest.generation)
    }

    pub(crate) fn evaluate(&self, observation: TimelinePromotionObservation) -> Result<(), TimelinePromotionError> {
        if self.clock.state() == TimerClockState::Unsafe {
            return Err(TimelinePromotionError::ClockUnsafe);
        }
        let snapshot = self
            .installed_snapshot
            .read()
            .clone()
            .ok_or(TimelinePromotionError::SnapshotMissing)?;
        if observation.activation_epoch != self.expected_activation_epoch
            || observation.format_fingerprint != self.expected_format_fingerprint
            || observation.capability_version != self.expected_capability_version
            || observation.role_epoch < snapshot.role_epoch
        {
            return Err(TimelinePromotionError::Compatibility);
        }
        if observation.source_replay_cursor < observation.source_retention_start
            || observation.completion_replay_cursor < observation.final_retention_start
            || observation.source_replay_cursor < snapshot.source_physical_cursor
            || observation.completion_replay_cursor < snapshot.completion_physical_cursor
        {
            return Err(TimelinePromotionError::RetentionGap);
        }
        if observation.source_replay_cursor < observation.replicated_source_end
            || observation.completion_replay_cursor < observation.replicated_final_end
            || observation.materialization_backlog != 0
            || observation.due_backlog != 0
            || observation.completion_backlog != 0
        {
            return Err(TimelinePromotionError::NotCaughtUp);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum TimelinePromotionError {
    #[error(transparent)]
    Manifest(#[from] rocketmq_store_api::TimerSnapshotValidationError),
    #[error("Extended promotion requires an installed snapshot")]
    SnapshotMissing,
    #[error("Extended snapshot format or activation epoch is incompatible")]
    SnapshotCompatibility,
    #[error("Extended snapshot generation is not newer than the installed artifact")]
    StaleSnapshot,
    #[error("Extended member capability, epoch, or format is incompatible")]
    Compatibility,
    #[error("Extended source or final-fact replay has crossed retention")]
    RetentionGap,
    #[error("Extended source, due, or completion replay is not caught up")]
    NotCaughtUp,
    #[error("CLOCK_UNSAFE prevents Extended promotion")]
    ClockUnsafe,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timer::clock::SystemTimerClock;

    #[test]
    fn empty_or_retention_gapped_follower_cannot_promote() {
        let clock = Arc::new(TimerClockSafety::new(Arc::new(SystemTimerClock::default()), 1_000));
        let gate = TimelinePromotionGate::new(2, 7, 1, clock);
        let observation = TimelinePromotionObservation {
            source_replay_cursor: 100,
            replicated_source_end: 100,
            completion_replay_cursor: 200,
            replicated_final_end: 200,
            role_epoch: 4,
            activation_epoch: 2,
            format_fingerprint: 7,
            capability_version: 1,
            ..TimelinePromotionObservation::default()
        };
        assert_eq!(gate.evaluate(observation), Err(TimelinePromotionError::SnapshotMissing));
    }
}
