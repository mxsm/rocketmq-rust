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

//! Immutable topic-route snapshots and their single-writer publication gate.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use dashmap::DashMap;
use parking_lot::Mutex;
use parking_lot::MutexGuard;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_runtime::common::time_utils::current_millis;

use crate::route::types::route_topic_name;
use crate::route::types::RouteTopicName;
use crate::route::types::TopicName;

/// One coherent, immutable view of all route data exposed for a topic.
#[derive(Clone, Debug)]
pub(crate) struct TopicRouteSnapshot {
    pub(crate) route_data: TopicRouteData,
    pub(crate) version: u64,
    pub(crate) published_at: u64,
}

/// Serializes source-table mutations and atomically publishes their derived views.
///
/// The gate is always acquired before existing broker/topic segmented locks. Route
/// readers never acquire it; they load one immutable per-topic snapshot instead.
pub(crate) struct RouteMutationCoordinator {
    mutation_gate: Mutex<()>,
    snapshots: DashMap<RouteTopicName, Arc<ArcSwapOption<TopicRouteSnapshot>>>,
    next_version: AtomicU64,
}

impl RouteMutationCoordinator {
    pub(crate) fn new() -> Self {
        Self {
            mutation_gate: Mutex::new(()),
            snapshots: DashMap::new(),
            next_version: AtomicU64::new(0),
        }
    }

    pub(crate) fn begin_mutation(&self) -> RouteMutationGuard<'_> {
        RouteMutationGuard {
            coordinator: self,
            _gate: self.mutation_gate.lock(),
        }
    }

    /// Loads exactly one published pointer for the requested topic.
    pub(crate) fn load(&self, topic: &str) -> Option<Arc<TopicRouteSnapshot>> {
        let publisher = self.snapshots.get(topic).map(|entry| Arc::clone(entry.value()))?;
        publisher.load_full()
    }
}

impl Default for RouteMutationCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) struct RouteMutationGuard<'a> {
    coordinator: &'a RouteMutationCoordinator,
    _gate: MutexGuard<'a, ()>,
}

impl RouteMutationGuard<'_> {
    /// Publishes a complete route, or an explicit absence, while the mutation gate is held.
    pub(crate) fn publish(&self, topic: TopicName, route_data: Option<TopicRouteData>) -> u64 {
        let version = self.coordinator.next_version.fetch_add(1, Ordering::Relaxed) + 1;
        let topic = route_topic_name(topic);
        if let Some(route_data) = route_data {
            let publisher = self
                .coordinator
                .snapshots
                .entry(topic)
                .or_insert_with(|| Arc::new(ArcSwapOption::empty()))
                .clone();
            publisher.store(Some(Arc::new(TopicRouteSnapshot {
                route_data,
                version,
                published_at: current_millis(),
            })));
        } else if let Some(publisher) = self
            .coordinator
            .snapshots
            .get(&topic)
            .map(|entry| Arc::clone(entry.value()))
        {
            // Publish absence before removing the index entry so readers that already cloned this
            // publisher cannot retain a stale route after a delete/recreate cycle.
            publisher.store(None);
            self.coordinator.snapshots.remove(&topic);
        }
        version
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn route_snapshot_versions_are_monotonic_for_the_same_topic() {
        let coordinator = RouteMutationCoordinator::new();
        let topic = CheetahString::from_static_str("snapshot-topic");

        let first_version = coordinator
            .begin_mutation()
            .publish(topic.clone(), Some(TopicRouteData::default()));
        let first = coordinator.load(topic.as_str()).expect("first snapshot should exist");
        let second_version = coordinator
            .begin_mutation()
            .publish(topic.clone(), Some(TopicRouteData::default()));
        let second = coordinator.load(topic.as_str()).expect("second snapshot should exist");

        assert_eq!(first.version, first_version);
        assert_eq!(second.version, second_version);
        assert!(second.version > first.version);
        assert!(second.published_at >= first.published_at);
    }

    #[test]
    fn publishing_none_removes_the_visible_topic_snapshot() {
        let coordinator = RouteMutationCoordinator::new();
        let topic = CheetahString::from_static_str("snapshot-topic");

        coordinator
            .begin_mutation()
            .publish(topic.clone(), Some(TopicRouteData::default()));
        assert_eq!(coordinator.snapshots.len(), 1);
        coordinator.begin_mutation().publish(topic.clone(), None);

        assert!(coordinator.load(topic.as_str()).is_none());
        assert!(coordinator.snapshots.is_empty());
    }

    #[test]
    fn deleted_publisher_cannot_observe_a_recreated_topic() {
        let coordinator = RouteMutationCoordinator::new();
        let topic = CheetahString::from_static_str("snapshot-topic");

        let first_version = coordinator
            .begin_mutation()
            .publish(topic.clone(), Some(TopicRouteData::default()));
        let deleted_publisher = coordinator
            .snapshots
            .get(topic.as_str())
            .map(|entry| Arc::clone(entry.value()))
            .expect("published topic should have an indexed publisher");

        coordinator.begin_mutation().publish(topic.clone(), None);
        let recreated_version = coordinator
            .begin_mutation()
            .publish(topic.clone(), Some(TopicRouteData::default()));

        assert!(deleted_publisher.load_full().is_none());
        assert!(coordinator.load(topic.as_str()).is_some());
        assert!(recreated_version > first_version);
        assert_eq!(coordinator.snapshots.len(), 1);
    }
}
