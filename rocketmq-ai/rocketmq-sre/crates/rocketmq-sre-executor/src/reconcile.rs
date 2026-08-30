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

/// Typed live-state result returned by an exact read-side action handler.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveEffectState {
    AppliedAndVerified,
    NotAppliedCompensationRequired,
    Ambiguous,
}

/// Fail-closed recovery choice for an intent without a durable result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconcileDisposition {
    MarkCompleted,
    ContinueCompensation,
    EscalateManual,
}

/// Deterministic recovery classifier. It never retries an external write.
pub struct ReconcilePlanner;

impl ReconcilePlanner {
    /// Maps observed live state to a recovery disposition.
    #[must_use]
    pub const fn classify(state: LiveEffectState) -> ReconcileDisposition {
        match state {
            LiveEffectState::AppliedAndVerified => ReconcileDisposition::MarkCompleted,
            LiveEffectState::NotAppliedCompensationRequired => ReconcileDisposition::ContinueCompensation,
            LiveEffectState::Ambiguous => ReconcileDisposition::EscalateManual,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ambiguous_live_state_never_retries_a_write() {
        assert_eq!(
            ReconcilePlanner::classify(LiveEffectState::Ambiguous),
            ReconcileDisposition::EscalateManual
        );
    }

    #[test]
    fn observed_effect_and_compensation_paths_are_explicit() {
        assert_eq!(
            ReconcilePlanner::classify(LiveEffectState::AppliedAndVerified),
            ReconcileDisposition::MarkCompleted
        );
        assert_eq!(
            ReconcilePlanner::classify(LiveEffectState::NotAppliedCompensationRequired),
            ReconcileDisposition::ContinueCompensation
        );
    }
}
