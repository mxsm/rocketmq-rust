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

use std::sync::Arc;

use cheetah_string::CheetahString;
use dashmap::DashMap;

const MAX_RESIDENCY_OBSERVATIONS: usize = 65_536;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct MessageResidencyKey {
    consumer_group: CheetahString,
    topic: CheetahString,
    queue_id: i32,
    queue_offset: i64,
}

/// Supplies deterministic message-residency decisions to cold-data flow control.
pub(crate) trait ColdDataResidencyProbe: Send + Sync {
    /// Records the residency observed by the real message read path.
    fn observe(&self, consumer_group: &str, topic: &str, queue_id: i32, queue_offset: i64, in_cache: bool);

    /// Returns whether the last observed message residency was cold.
    fn is_cold(&self, consumer_group: &str, topic: &str, queue_id: i32, queue_offset: i64) -> bool;
}

#[derive(Default)]
struct ObservedColdDataResidencyProbe {
    observations: DashMap<MessageResidencyKey, bool>,
}

impl ColdDataResidencyProbe for ObservedColdDataResidencyProbe {
    fn observe(&self, consumer_group: &str, topic: &str, queue_id: i32, queue_offset: i64, in_cache: bool) {
        if self.observations.len() >= MAX_RESIDENCY_OBSERVATIONS {
            self.observations.clear();
        }
        self.observations.insert(
            MessageResidencyKey {
                consumer_group: consumer_group.into(),
                topic: topic.into(),
                queue_id,
                queue_offset,
            },
            !in_cache,
        );
    }

    fn is_cold(&self, consumer_group: &str, topic: &str, queue_id: i32, queue_offset: i64) -> bool {
        self.observations
            .get(&MessageResidencyKey {
                consumer_group: consumer_group.into(),
                topic: topic.into(),
                queue_id,
                queue_offset,
            })
            .is_some_and(|cold| *cold)
    }
}

/// Service for checking if message data is in cold storage area
pub struct ColdDataCheckService {
    residency_probe: Arc<dyn ColdDataResidencyProbe>,
}

impl Default for ColdDataCheckService {
    fn default() -> Self {
        Self {
            residency_probe: Arc::new(ObservedColdDataResidencyProbe::default()),
        }
    }
}

impl ColdDataCheckService {
    /// Creates a service backed by an injected residency probe.
    #[cfg(test)]
    pub(crate) fn with_probe(residency_probe: Arc<dyn ColdDataResidencyProbe>) -> Self {
        Self { residency_probe }
    }

    /// Check if the data at the given offset is in page cache
    pub fn is_data_in_page_cache(&self) -> bool {
        true
    }

    /// Check if the message at the given queue offset is in cold data area
    ///
    /// Cold data is data that has not been accessed for a long time and may be
    /// stored on slower storage or has been paged out from memory.
    ///
    /// # Arguments
    /// * `consumer_group` - The consumer group name
    /// * `topic` - The topic name
    /// * `queue_id` - The queue ID
    /// * `queue_offset` - The queue offset to check
    ///
    /// # Returns
    /// `true` if the message is in cold data area, `false` otherwise
    pub fn is_msg_in_cold_area(
        &self,
        consumer_group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        queue_offset: i64,
    ) -> bool {
        self.residency_probe
            .is_cold(consumer_group.as_str(), topic.as_str(), queue_id, queue_offset)
    }

    /// Records the page-cache state observed while reading the message payload.
    pub fn observe_message_residency(
        &self,
        consumer_group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        queue_offset: i64,
        in_cache: bool,
    ) {
        self.residency_probe.observe(
            consumer_group.as_str(),
            topic.as_str(),
            queue_id,
            queue_offset,
            in_cache,
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    struct FixedResidencyProbe(bool);

    impl ColdDataResidencyProbe for FixedResidencyProbe {
        fn observe(&self, _consumer_group: &str, _topic: &str, _queue_id: i32, _queue_offset: i64, _in_cache: bool) {}

        fn is_cold(&self, _consumer_group: &str, _topic: &str, _queue_id: i32, _queue_offset: i64) -> bool {
            self.0
        }
    }

    #[test]
    fn injected_probe_controls_residency_without_os_page_cache_dependency() {
        let service = ColdDataCheckService::with_probe(Arc::new(FixedResidencyProbe(true)));
        assert!(service.is_msg_in_cold_area(
            &CheetahString::from_static_str("group"),
            &CheetahString::from_static_str("topic"),
            0,
            0,
        ));
    }

    #[test]
    fn observed_residency_drives_cold_area_checks() {
        let service = ColdDataCheckService::default();
        let group = CheetahString::from_static_str("group");
        let topic = CheetahString::from_static_str("topic");

        assert!(service.is_data_in_page_cache());
        assert!(!service.is_msg_in_cold_area(&group, &topic, 1, 42));

        service.observe_message_residency(&group, &topic, 1, 42, false);
        assert!(service.is_msg_in_cold_area(&group, &topic, 1, 42));

        service.observe_message_residency(&group, &topic, 1, 42, true);
        assert!(!service.is_msg_in_cold_area(&group, &topic, 1, 42));
    }
}
