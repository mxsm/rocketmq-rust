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

//! Explicit cleanup accounting for bounded synthetic probes.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

/// Cleanup result. Topic and Group deletion are permanently outside the probe.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ProbeCleanupResult {
    pub producer_stopped: bool,
    pub consumer_stopped: bool,
    pub local_run_metadata_removed: bool,
    pub topic_deleted: bool,
    pub group_deleted: bool,
    pub partial: bool,
    pub warnings: Vec<String>,
}

impl ProbeCleanupResult {
    /// Creates a safe cleanup result and derives its partial state.
    #[must_use]
    pub fn bounded(
        producer_stopped: bool,
        consumer_stopped: bool,
        local_run_metadata_removed: bool,
        warnings: Vec<String>,
    ) -> Self {
        Self {
            producer_stopped,
            consumer_stopped,
            local_run_metadata_removed,
            topic_deleted: false,
            group_deleted: false,
            partial: !(producer_stopped && consumer_stopped && local_run_metadata_removed && warnings.is_empty()),
            warnings,
        }
    }
}

impl Default for ProbeCleanupResult {
    fn default() -> Self {
        Self::bounded(true, true, true, Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_never_claims_topic_or_group_deletion() {
        let cleanup = ProbeCleanupResult::default();

        assert!(!cleanup.topic_deleted);
        assert!(!cleanup.group_deleted);
        assert!(!cleanup.partial);
    }
}
