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

#[derive(Debug, Clone)]
pub struct LabelGuard {
    cardinality_limit: usize,
    topic_enabled: bool,
    consumer_group_enabled: bool,
    seen_topics: HashSet<String>,
    seen_consumer_groups: HashSet<String>,
    dropped_labels: u64,
}

impl LabelGuard {
    pub fn new(cardinality_limit: usize, topic_enabled: bool, consumer_group_enabled: bool) -> Self {
        Self {
            cardinality_limit,
            topic_enabled,
            consumer_group_enabled,
            seen_topics: HashSet::new(),
            seen_consumer_groups: HashSet::new(),
            dropped_labels: 0,
        }
    }

    pub fn normalize_metric_label<'a>(&mut self, key: &str, value: &'a str) -> Cow<'a, str> {
        self.normalize_metric_label_with_outcome(key, value).0
    }

    pub fn normalize_metric_label_with_outcome<'a>(&mut self, key: &str, value: &'a str) -> (Cow<'a, str>, bool) {
        match key {
            "address"
            | "aggregation"
            | "broker_set"
            | "cluster"
            | "consume_mode"
            | "dLedger_operation_status"
            | "dledger_operation"
            | "election_result"
            | "file_type"
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
            | "response_code"
            | "result"
            | "revive_message_type"
            | "storage_medium"
            | "storage_type"
            | "success"
            | "timer_bound_s"
            | "version" => (Cow::Borrowed(value), false),
            "group" if self.consumer_group_enabled => self.normalize_bounded_value(value, LabelKind::ConsumerGroup),
            "topic" if self.topic_enabled => self.normalize_bounded_value(value, LabelKind::Topic),
            "consumer_group" if self.consumer_group_enabled => {
                self.normalize_bounded_value(value, LabelKind::ConsumerGroup)
            }
            _ => {
                self.dropped_labels += 1;
                (Cow::Borrowed("other"), true)
            }
        }
    }

    pub fn allow_metric_label(&mut self, key: &str, value: &str) -> bool {
        !matches!(self.normalize_metric_label(key, value), Cow::Borrowed("other"))
    }

    pub fn dropped_labels(&self) -> u64 {
        self.dropped_labels
    }

    fn normalize_bounded_value<'a>(&mut self, value: &'a str, kind: LabelKind) -> (Cow<'a, str>, bool) {
        let values = match kind {
            LabelKind::Topic => &mut self.seen_topics,
            LabelKind::ConsumerGroup => &mut self.seen_consumer_groups,
        };

        if values.contains(value) {
            return (Cow::Borrowed(value), false);
        }

        if values.len() < self.cardinality_limit {
            values.insert(value.to_string());
            return (Cow::Borrowed(value), false);
        }

        self.dropped_labels += 1;
        (Cow::Borrowed("other"), true)
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
    fn bounds_java_group_alias_like_consumer_group() {
        let mut guard = LabelGuard::new(1, true, true);

        assert_eq!(guard.normalize_metric_label("group", "group-a"), "group-a");
        assert_eq!(guard.normalize_metric_label("group", "group-b"), "other");
        assert_eq!(guard.dropped_labels(), 1);
    }
}
