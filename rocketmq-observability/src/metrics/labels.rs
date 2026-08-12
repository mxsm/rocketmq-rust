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

use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// Fixed low-cardinality value used when a metric label is disabled or exceeds its limit.
pub const METRIC_LABEL_SENTINEL: &str = "other";

/// Instance-scoped policy for metric labels whose values can grow without bound.
///
/// Clones share the admitted values of one telemetry runtime. Policies created separately never
/// share state, so one broker or embedded runtime cannot consume another runtime's budget.
#[derive(Debug, Clone)]
pub struct MetricLabelPolicy {
    state: Option<Arc<MetricLabelPolicyState>>,
}

impl MetricLabelPolicy {
    /// Creates an independent bounded-cardinality policy.
    #[must_use]
    pub fn new(cardinality_limit: usize, topic_enabled: bool, consumer_group_enabled: bool) -> Self {
        Self {
            state: Some(Arc::new(MetricLabelPolicyState {
                topics: topic_enabled.then(|| BoundedLabelSet::new(cardinality_limit)),
                consumer_groups: consumer_group_enabled.then(|| BoundedLabelSet::new(cardinality_limit)),
                dropped_labels: AtomicU64::new(0),
            })),
        }
    }

    /// Creates a policy that rejects topic and consumer-group labels without allocating state.
    #[must_use]
    pub const fn disabled() -> Self {
        Self { state: None }
    }

    /// Normalizes a topic label according to this instance's configured policy.
    #[must_use]
    #[inline]
    pub fn normalize_topic<'a>(&self, value: &'a str) -> Cow<'a, str> {
        self.normalize_metric_label("topic", value)
    }

    /// Normalizes a consumer-group label according to this instance's configured policy.
    #[must_use]
    #[inline]
    pub fn normalize_consumer_group<'a>(&self, value: &'a str) -> Cow<'a, str> {
        self.normalize_metric_label("consumer_group", value)
    }

    /// Normalizes a metric label, mapping rejected values to [`METRIC_LABEL_SENTINEL`].
    #[must_use]
    #[inline]
    pub fn normalize_metric_label<'a>(&self, key: &str, value: &'a str) -> Cow<'a, str> {
        self.normalize_metric_label_with_outcome(key, value).0
    }

    /// Normalizes a metric label and reports whether it was mapped to the sentinel.
    #[must_use]
    pub fn normalize_metric_label_with_outcome<'a>(&self, key: &str, value: &'a str) -> (Cow<'a, str>, bool) {
        match key {
            "address"
            | "address_family"
            | "aggregation"
            | "broker_set"
            | "cluster"
            | "consume_mode"
            | "dLedger_operation_status"
            | "dledger_operation"
            | "election_result"
            | "file_type"
            | "freshness"
            | "invocation_status"
            | "is_long_polling"
            | "is_retry"
            | "is_system"
            | "language"
            | "message_type"
            | "node_id"
            | "node_type"
            | "operation"
            | "path"
            | "peer_id"
            | "processor"
            | "protocol_type"
            | "proxy_mode"
            | "put_status"
            | "queue_id"
            | "request_code"
            | "request_handle_status"
            | "request_type"
            | "reason"
            | "response_code"
            | "result"
            | "revive_message_type"
            | "storage_medium"
            | "storage_type"
            | "source_kind"
            | "success"
            | "timer_bound_s"
            | "version" => (Cow::Borrowed(value), false),
            "group" | "consumer_group" => self.normalize_bounded_value(value, LabelKind::ConsumerGroup),
            "topic" => self.normalize_bounded_value(value, LabelKind::Topic),
            _ => self.dropped(),
        }
    }

    /// Returns whether a label was admitted without sentinel mapping.
    #[must_use]
    #[inline]
    pub fn allow_metric_label(&self, key: &str, value: &str) -> bool {
        !self.normalize_metric_label_with_outcome(key, value).1
    }

    /// Returns the number of label observations mapped to the sentinel.
    #[must_use]
    #[inline]
    pub fn dropped_labels(&self) -> u64 {
        self.state
            .as_ref()
            .map_or(0, |state| state.dropped_labels.load(Ordering::Relaxed))
    }

    #[inline]
    fn normalize_bounded_value<'a>(&self, value: &'a str, kind: LabelKind) -> (Cow<'a, str>, bool) {
        let Some(state) = self.state.as_ref() else {
            return self.dropped();
        };
        let values = match kind {
            LabelKind::Topic => state.topics.as_ref(),
            LabelKind::ConsumerGroup => state.consumer_groups.as_ref(),
        };
        let Some(values) = values else {
            return self.dropped();
        };

        if values.admit(value) {
            return (Cow::Borrowed(value), false);
        }

        self.dropped()
    }

    #[inline]
    fn dropped<'a>(&self) -> (Cow<'a, str>, bool) {
        if let Some(state) = self.state.as_ref() {
            state.dropped_labels.fetch_add(1, Ordering::Relaxed);
        }
        (Cow::Borrowed(METRIC_LABEL_SENTINEL), true)
    }
}

impl Default for MetricLabelPolicy {
    fn default() -> Self {
        Self::new(10_000, true, true)
    }
}

#[derive(Debug)]
struct MetricLabelPolicyState {
    topics: Option<BoundedLabelSet>,
    consumer_groups: Option<BoundedLabelSet>,
    dropped_labels: AtomicU64,
}

#[derive(Debug)]
struct BoundedLabelSet {
    limit: usize,
    values: parking_lot::RwLock<HashSet<Box<str>>>,
    saturated: AtomicBool,
}

impl BoundedLabelSet {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            values: parking_lot::RwLock::new(HashSet::new()),
            saturated: AtomicBool::new(limit == 0),
        }
    }

    #[inline]
    fn admit(&self, value: &str) -> bool {
        if self.values.read().contains(value) {
            return true;
        }
        if self.saturated.load(Ordering::Relaxed) {
            return false;
        }

        let mut values = self.values.write();
        if values.contains(value) {
            return true;
        }
        if values.len() >= self.limit {
            self.saturated.store(true, Ordering::Relaxed);
            return false;
        }

        values.insert(value.into());
        if values.len() >= self.limit {
            self.saturated.store(true, Ordering::Relaxed);
        }
        true
    }
}

/// Backward-compatible mutable facade for callers that have not yet adopted telemetry handles.
#[derive(Debug, Clone)]
pub struct LabelGuard {
    policy: MetricLabelPolicy,
}

impl LabelGuard {
    pub fn new(cardinality_limit: usize, topic_enabled: bool, consumer_group_enabled: bool) -> Self {
        Self {
            policy: MetricLabelPolicy::new(cardinality_limit, topic_enabled, consumer_group_enabled),
        }
    }

    pub fn normalize_metric_label<'a>(&mut self, key: &str, value: &'a str) -> Cow<'a, str> {
        self.policy.normalize_metric_label(key, value)
    }

    pub fn normalize_metric_label_with_outcome<'a>(&mut self, key: &str, value: &'a str) -> (Cow<'a, str>, bool) {
        self.policy.normalize_metric_label_with_outcome(key, value)
    }

    pub fn allow_metric_label(&mut self, key: &str, value: &str) -> bool {
        self.policy.allow_metric_label(key, value)
    }

    pub fn dropped_labels(&self) -> u64 {
        self.policy.dropped_labels()
    }
}

impl Default for LabelGuard {
    fn default() -> Self {
        Self::new(10_000, true, true)
    }
}

#[derive(Debug, Clone, Copy)]
enum LabelKind {
    Topic,
    ConsumerGroup,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_high_cardinality_keys() {
        let mut guard = LabelGuard::default();

        assert_eq!(guard.normalize_metric_label("message_id", "abc"), "other");
        assert_eq!(guard.normalize_metric_label("trace_id", "trace"), "other");
        assert_eq!(guard.dropped_labels(), 2);
    }

    #[test]
    fn reports_when_label_is_dropped() {
        let mut guard = LabelGuard::new(1, true, true);

        assert_eq!(
            guard.normalize_metric_label_with_outcome("topic", "topic-a"),
            (Cow::Borrowed("topic-a"), false)
        );
        assert_eq!(
            guard.normalize_metric_label_with_outcome("topic", "topic-b"),
            (Cow::Borrowed("other"), true)
        );
        assert_eq!(
            guard.normalize_metric_label_with_outcome("message_id", "msg-1"),
            (Cow::Borrowed("other"), true)
        );
    }

    #[test]
    fn bounds_topic_cardinality() {
        let mut guard = LabelGuard::new(1, true, true);

        assert_eq!(guard.normalize_metric_label("topic", "topic-a"), "topic-a");
        assert_eq!(guard.normalize_metric_label("topic", "topic-b"), "other");
        assert_eq!(guard.normalize_metric_label("topic", "topic-a"), "topic-a");
    }

    #[test]
    fn can_disable_topic_labels() {
        let mut guard = LabelGuard::new(10, false, true);

        assert_eq!(guard.normalize_metric_label("topic", "topic-a"), "other");
    }

    #[test]
    fn allows_java_compatible_low_cardinality_labels() {
        let mut guard = LabelGuard::default();

        for key in [
            "protocol_type",
            "source_kind",
            "address_family",
            "freshness",
            "reason",
            "request_code",
            "response_code",
            "is_long_polling",
            "result",
            "storage_type",
            "storage_medium",
            "timer_bound_s",
            "proxy_mode",
            "operation",
            "success",
            "queue_id",
            "file_type",
            "request_type",
            "dledger_operation",
            "dLedger_operation_status",
            "election_result",
        ] {
            assert_eq!(guard.normalize_metric_label(key, "value"), "value", "{key}");
        }

        assert_eq!(guard.dropped_labels(), 0);
    }

    #[test]
    fn nameserver_discovery_rejects_endpoint_identity_as_a_label_key() {
        let mut guard = LabelGuard::default();

        for key in ["fqdn", "ip", "pod", "namespace", "nameserver_endpoint"] {
            assert_eq!(guard.normalize_metric_label(key, "namesrv-0.default.svc"), "other");
        }
        assert_eq!(guard.dropped_labels(), 5);
    }

    #[test]
    fn bounds_java_group_alias_like_consumer_group() {
        let mut guard = LabelGuard::new(1, true, true);

        assert_eq!(guard.normalize_metric_label("group", "group-a"), "group-a");
        assert_eq!(guard.normalize_metric_label("group", "group-b"), "other");
        assert_eq!(guard.dropped_labels(), 1);
    }

    #[test]
    fn policy_switches_map_disabled_dimensions_to_sentinel() {
        let policy = MetricLabelPolicy::new(10, false, false);

        assert_eq!(policy.normalize_topic("topic-a"), METRIC_LABEL_SENTINEL);
        assert_eq!(policy.normalize_consumer_group("group-a"), METRIC_LABEL_SENTINEL);
        assert_eq!(policy.dropped_labels(), 2);
    }

    #[test]
    fn policy_enforces_independent_topic_and_group_limits() {
        let policy = MetricLabelPolicy::new(1, true, true);

        assert_eq!(policy.normalize_topic("topic-a"), "topic-a");
        assert_eq!(policy.normalize_topic("topic-b"), METRIC_LABEL_SENTINEL);
        assert_eq!(policy.normalize_topic("topic-a"), "topic-a");
        assert_eq!(policy.normalize_consumer_group("group-a"), "group-a");
        assert_eq!(policy.normalize_consumer_group("group-b"), METRIC_LABEL_SENTINEL);
        assert_eq!(policy.dropped_labels(), 2);
    }

    #[test]
    fn policy_clones_share_budget_but_independent_instances_do_not() {
        let first = MetricLabelPolicy::new(1, true, true);
        let first_clone = first.clone();
        let second = MetricLabelPolicy::new(1, true, true);

        assert_eq!(first.normalize_topic("topic-a"), "topic-a");
        assert_eq!(first_clone.normalize_topic("topic-b"), METRIC_LABEL_SENTINEL);
        assert_eq!(second.normalize_topic("topic-b"), "topic-b");
        assert_eq!(first.dropped_labels(), 1);
        assert_eq!(second.dropped_labels(), 0);
    }
}
