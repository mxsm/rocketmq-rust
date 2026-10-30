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

//! Current collector evidence, scoped to the environment that started each attempt.

#[derive(Clone, Copy, Debug)]
pub enum CollectionSide {
    Topics,
    Brokers,
}

impl CollectionSide {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Topics => "topic",
            Self::Brokers => "broker",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CollectionSideHealth {
    pub last_success_epoch_ms: Option<u64>,
    pub failed_resources: usize,
    pub failed: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HistoryCollectionHealth {
    pub enabled: bool,
    pub interval_seconds: u64,
    pub topics: CollectionSideHealth,
    pub brokers: CollectionSideHealth,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CollectionStatus {
    Disabled,
    Warming,
    Normal,
    Stale,
    Partial,
}

impl HistoryCollectionHealth {
    pub fn status(self, now_epoch_ms: u64) -> CollectionStatus {
        if !self.enabled || self.interval_seconds == 0 {
            return CollectionStatus::Disabled;
        }
        let successes = [self.topics.last_success_epoch_ms, self.brokers.last_success_epoch_ms];
        let oldest = successes.into_iter().flatten().min();
        if oldest.is_some_and(|last| now_epoch_ms.saturating_sub(last) > self.interval_seconds.saturating_mul(3_000)) {
            return CollectionStatus::Stale;
        }
        if self.topics.failed
            || self.brokers.failed
            || self.topics.failed_resources > 0
            || self.brokers.failed_resources > 0
        {
            return CollectionStatus::Partial;
        }
        if successes.into_iter().any(|success| success.is_none()) {
            CollectionStatus::Warming
        } else {
            CollectionStatus::Normal
        }
    }

    pub fn latest_success(self) -> Option<u64> {
        [self.topics.last_success_epoch_ms, self.brokers.last_success_epoch_ms]
            .into_iter()
            .flatten()
            .max()
    }
}
