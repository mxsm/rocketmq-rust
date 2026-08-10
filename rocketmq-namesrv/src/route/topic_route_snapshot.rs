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
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;

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
#[derive(Debug)]
pub(crate) struct TopicRouteSnapshot {
    route_data: Arc<TopicRouteData>,
    acting_master_route_data: OnceLock<Option<Arc<TopicRouteData>>>,
    pub(crate) version: u64,
    pub(crate) published_at: u64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum RouteVariant {
    Base,
    ActingMaster,
}

/// A cheap, generation-stamped reference to one immutable route representation.
#[derive(Clone, Debug)]
pub(crate) struct TopicRouteView {
    route_data: Arc<TopicRouteData>,
    version: u64,
    variant: RouteVariant,
}

impl TopicRouteSnapshot {
    fn new(route_data: TopicRouteData, version: u64) -> Self {
        Self {
            route_data: Arc::new(route_data),
            acting_master_route_data: OnceLock::new(),
            version,
            published_at: current_millis(),
        }
    }

    pub(crate) fn base_view(&self) -> TopicRouteView {
        TopicRouteView {
            route_data: Arc::clone(&self.route_data),
            version: self.version,
            variant: RouteVariant::Base,
        }
    }

    pub(crate) fn acting_master_view(
        &self,
        build: impl FnOnce(&TopicRouteData) -> Option<TopicRouteData>,
    ) -> TopicRouteView {
        let acting_master_route_data = self
            .acting_master_route_data
            .get_or_init(|| build(&self.route_data).map(Arc::new));
        match acting_master_route_data {
            Some(route_data) => TopicRouteView {
                route_data: Arc::clone(route_data),
                version: self.version,
                variant: RouteVariant::ActingMaster,
            },
            None => self.base_view(),
        }
    }
}

impl TopicRouteView {
    pub(crate) fn route_data(&self) -> &Arc<TopicRouteData> {
        &self.route_data
    }

    pub(crate) fn version(&self) -> u64 {
        self.version
    }

    pub(crate) fn variant(&self) -> RouteVariant {
        self.variant
    }
}

/// Serializes source-table mutations and atomically publishes their derived views.
///
/// The gate is always acquired before existing broker/topic segmented locks. Route
/// readers never acquire it; they load one immutable per-topic snapshot instead.
pub(crate) struct RouteMutationCoordinator {
    mutation_gate: Mutex<()>,
    snapshots: DashMap<RouteTopicName, Arc<ArcSwapOption<TopicRouteSnapshot>>>,
    next_version: AtomicU64,
    metrics: rocketmq_observability::metrics::namesrv::NameServerMetrics,
}

impl RouteMutationCoordinator {
    pub(crate) fn new() -> Self {
        Self::with_metrics(rocketmq_observability::metrics::namesrv::NameServerMetrics::noop())
    }

    pub(crate) fn with_metrics(metrics: rocketmq_observability::metrics::namesrv::NameServerMetrics) -> Self {
        Self {
            mutation_gate: Mutex::new(()),
            snapshots: DashMap::new(),
            next_version: AtomicU64::new(0),
            metrics,
        }
    }

    pub(crate) fn begin_mutation(&self) -> RouteMutationGuard<'_> {
        let wait_started = Instant::now();
        let gate = self.mutation_gate.lock();
        self.metrics.record_mutation_wait(wait_started.elapsed());
        RouteMutationGuard {
            coordinator: self,
            _gate: gate,
            hold_started: Instant::now(),
        }
    }

    /// Pins the mutable source tables to one completed mutation generation.
    ///
    /// Management endpoints use this guard while assembling DTOs that span
    /// multiple DashMap tables. Topic-route reads intentionally remain on the
    /// independently published ArcSwap snapshots and never acquire this gate.
    pub(crate) fn begin_management_read(&self) -> ManagementReadGuard<'_> {
        ManagementReadGuard {
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
    hold_started: Instant,
}

pub(crate) struct ManagementReadGuard<'a> {
    _gate: MutexGuard<'a, ()>,
}

impl RouteMutationGuard<'_> {
    pub(crate) fn record_snapshot_rebuild(&self, elapsed: Duration, present: bool) {
        self.coordinator.metrics.record_snapshot_rebuild(elapsed, present);
    }

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
            publisher.store(Some(Arc::new(TopicRouteSnapshot::new(route_data, version))));
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

impl Drop for RouteMutationGuard<'_> {
    fn drop(&mut self) {
        self.coordinator
            .metrics
            .record_mutation_hold(self.hold_started.elapsed());
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

    #[test]
    fn repeated_base_views_share_the_published_route_allocation() {
        let coordinator = RouteMutationCoordinator::new();
        let topic = CheetahString::from_static_str("snapshot-topic");

        coordinator
            .begin_mutation()
            .publish(topic.clone(), Some(TopicRouteData::default()));
        let snapshot = coordinator.load(topic.as_str()).expect("snapshot should exist");

        let first = snapshot.base_view();
        let second = snapshot.base_view();

        assert_eq!(first.variant(), RouteVariant::Base);
        assert_eq!(first.version(), second.version());
        assert!(Arc::ptr_eq(first.route_data(), second.route_data()));
    }

    #[test]
    fn acting_master_view_is_lazy_shared_and_does_not_mutate_the_base_view() {
        let coordinator = RouteMutationCoordinator::new();
        let topic = CheetahString::from_static_str("snapshot-topic");
        coordinator
            .begin_mutation()
            .publish(topic.clone(), Some(TopicRouteData::default()));
        let snapshot = coordinator.load(topic.as_str()).expect("snapshot should exist");

        let first = snapshot.acting_master_view(|base| {
            let mut acting = base.clone();
            acting.order_topic_conf = Some(CheetahString::from_static_str("acting"));
            Some(acting)
        });
        let second = snapshot.acting_master_view(|_| panic!("the acting view must initialize only once"));
        let base = snapshot.base_view();

        assert_eq!(first.variant(), RouteVariant::ActingMaster);
        assert_eq!(first.route_data().order_topic_conf.as_deref(), Some("acting"));
        assert!(base.route_data().order_topic_conf.is_none());
        assert!(Arc::ptr_eq(first.route_data(), second.route_data()));
        assert!(!Arc::ptr_eq(first.route_data(), base.route_data()));
    }
}
