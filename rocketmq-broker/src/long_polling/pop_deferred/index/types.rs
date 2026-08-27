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

use cheetah_string::CheetahString;
use rocketmq_model::common::key_builder::KeyBuilder;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_store::ArcMessageFilter;
use rocketmq_store::CqExtUnit;
use std::collections::HashMap;
use std::num::NonZeroUsize;

/// Fixed global and per-criteria admission limits for the POP business index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PopCriteriaLimits {
    pub(super) max_entries: NonZeroUsize,
    pub(super) max_entries_per_key: NonZeroUsize,
}

impl PopCriteriaLimits {
    pub(crate) const fn new(max_entries: NonZeroUsize, max_entries_per_key: NonZeroUsize) -> Self {
        Self {
            max_entries,
            max_entries_per_key,
        }
    }
}

/// Exact topic/group/queue membership key for a deferred POP request.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct PopCriteriaKey {
    pub(super) topic: CheetahString,
    pub(super) consumer_group: CheetahString,
    pub(super) queue_id: i32,
}

impl PopCriteriaKey {
    pub(crate) fn new(topic: CheetahString, consumer_group: CheetahString, queue_id: i32) -> Self {
        Self {
            topic,
            consumer_group,
            queue_id,
        }
    }

    pub(crate) fn from_parts(topic: &CheetahString, consumer_group: &CheetahString, queue_id: i32) -> Self {
        Self::new(topic.clone(), consumer_group.clone(), queue_id)
    }

    #[must_use]
    pub(crate) const fn topic(&self) -> &CheetahString {
        &self.topic
    }

    #[must_use]
    pub(crate) const fn consumer_group(&self) -> &CheetahString {
        &self.consumer_group
    }

    #[must_use]
    pub(crate) const fn queue_id(&self) -> i32 {
        self.queue_id
    }
}

/// Filter state shared between the index record and affine resume ownership.
pub(crate) struct PopMatchCriteria {
    subscription: Option<SubscriptionData>,
    filter: Option<ArcMessageFilter>,
}

impl PopMatchCriteria {
    pub(crate) fn new(subscription: Option<SubscriptionData>, filter: Option<ArcMessageFilter>) -> Self {
        Self { subscription, filter }
    }

    #[must_use]
    pub(crate) const fn subscription(&self) -> Option<&SubscriptionData> {
        self.subscription.as_ref()
    }

    #[must_use]
    pub(crate) const fn filter(&self) -> Option<&ArcMessageFilter> {
        self.filter.as_ref()
    }

    pub(super) fn matches(&self, arrival: &PopArrival) -> bool {
        if arrival.force {
            return true;
        }
        let (Some(filter), Some(_subscription)) = (&self.filter, &self.subscription) else {
            return true;
        };
        let cq = CqExtUnit::new(
            arrival.tags_code.unwrap_or_default(),
            arrival.message_store_time,
            arrival.filter_bitmap.clone(),
        );
        if !filter.is_matched_by_consume_queue(arrival.tags_code, Some(&cq)) {
            return false;
        }
        arrival
            .properties
            .as_ref()
            .is_none_or(|properties| filter.is_matched_by_commit_log(None, Some(properties)))
    }
}

/// Owned message-arrival facts used before any registry claim is attempted.
pub(crate) struct PopArrival {
    pub(super) topic: CheetahString,
    pub(super) consumer_group: CheetahString,
    pub(super) queue_id: i32,
    tags_code: Option<i64>,
    message_store_time: i64,
    filter_bitmap: Option<Vec<u8>>,
    properties: Option<HashMap<CheetahString, CheetahString>>,
    force: bool,
}

impl PopArrival {
    pub(crate) fn new(topic: CheetahString, consumer_group: CheetahString, queue_id: i32) -> Self {
        Self {
            topic,
            consumer_group,
            queue_id,
            tags_code: None,
            message_store_time: 0,
            filter_bitmap: None,
            properties: None,
            force: false,
        }
    }

    /// Normalizes either POP retry-topic version with its trusted consumer group.
    pub(crate) fn from_retry_topic(topic: CheetahString, consumer_group: CheetahString, queue_id: i32) -> Self {
        let topic = KeyBuilder::parse_pop_retry_topic(topic.as_str(), consumer_group.as_str())
            .map(|(_, normal)| CheetahString::from(normal))
            .unwrap_or_else(|| topic.clone());
        Self::new(topic, consumer_group, queue_id)
    }

    #[must_use]
    pub(crate) fn with_filter_metadata(
        mut self,
        tags_code: Option<i64>,
        message_store_time: i64,
        filter_bitmap: Option<Vec<u8>>,
        properties: Option<HashMap<CheetahString, CheetahString>>,
    ) -> Self {
        self.tags_code = tags_code;
        self.message_store_time = message_store_time;
        self.filter_bitmap = filter_bitmap;
        self.properties = properties;
        self
    }

    #[must_use]
    pub(crate) fn forced(mut self) -> Self {
        self.force = true;
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PopSelectionOrder {
    Oldest,
    Newest,
}
