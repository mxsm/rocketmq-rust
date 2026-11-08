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

//! Fractional current metrics. The original integer Topic DTO remains compatible.

use serde::{Deserialize, Serialize};

use crate::Observed;

/// One application's Topic observations. Unknown rates never become idle zeroes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicCurrentObservation {
    /// Stable Topic name.
    pub topic: String,
    /// Stored messages from complete queue-offset evidence.
    pub total_messages: Observed<u64>,
    /// Last-minute production rate in messages per second.
    pub produce_tps: Observed<f64>,
    /// Last-minute pull rate summed across Consumer groups, in messages per second.
    pub consume_tps: Observed<f64>,
}

/// Independent query stage that failed for one candidate Topic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopicCurrentStage {
    Rates,
    Offsets,
}

/// Body-free failure evidence retained alongside successful Topic observations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicCurrentFailure {
    pub topic: String,
    pub stage: TopicCurrentStage,
    pub failed_targets: usize,
}

/// Bounded application Topic ranking with explicit sampling coverage.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicCurrentLoad {
    /// Total registered Topics, including system, retry and DLQ Topics.
    pub total_topics: usize,
    /// Number of application Topics available to select.
    pub application_topics: usize,
    /// Application names available to the history selector, including unsampled Topics.
    pub available_topics: Vec<String>,
    /// Candidate names in the current bounded collection window.
    pub candidates: Vec<String>,
    /// At most twenty observations, ranked by stored messages.
    pub items: Vec<TopicCurrentObservation>,
    /// Candidate Topics with unavailable or incomplete offset/rate observations.
    pub failed_topics: usize,
    /// Per-Topic rate or offset failures, including rate candidates outside the selected twenty.
    pub failures: Vec<TopicCurrentFailure>,
    /// Application Topics outside the bounded rate collection window.
    pub omitted_topics: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fractional_rates_round_trip_without_merging_zero_and_unknown() {
        let observation = TopicCurrentObservation {
            topic: "orders".into(),
            total_messages: Observed::Observed(0),
            produce_tps: Observed::Observed(0.5),
            consume_tps: Observed::Unknown,
        };
        let encoded = serde_json::to_vec(&observation).unwrap();
        let decoded: TopicCurrentObservation = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded.produce_tps, Observed::Observed(0.5));
        assert_eq!(decoded.total_messages, Observed::Observed(0));
        assert_eq!(decoded.consume_tps, Observed::Unknown);
    }
}
