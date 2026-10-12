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

//! Evidence that explains a backlog: who consumes, who may read, and what failed.
//!
//! Each type summarizes its complete source result. A report lists Broker names only up to
//! `LISTED_BROKER_LIMIT` and states the full count next to the list, so a diagnosis of a large
//! cluster stays inside the row limit of a Tool result. The rules still see every Broker.

use std::collections::BTreeSet;

use serde::Serialize;

use crate::model::contract::millis_from_observed_at;
use crate::model::contract::observed_at;
use crate::tools::config_tools::GetTopicConfigOutput;
use crate::tools::config_tools::TopicConfigDifferenceField;
use crate::tools::consumer_tools::ConsumerConnectionState;
use crate::tools::consumer_tools::ConsumerConsumeType;
use crate::tools::consumer_tools::ConsumerGroupConfigPresence;
use crate::tools::consumer_tools::ConsumerMessageModel;
use crate::tools::consumer_tools::GetConsumerGroupDetailsOutput;
use crate::tools::topic_tools::TopicStatsQueueRow;

/// Prefix of the Topic that holds the messages a Consumer Group failed to consume.
pub(crate) const RETRY_TOPIC_PREFIX: &str = "%RETRY%";

/// Prefix of the Topic that holds the messages a Consumer Group gave up on.
pub(crate) const DEAD_LETTER_TOPIC_PREFIX: &str = "%DLQ%";

/// How many Broker names one list of an evidence summary holds.
pub(crate) const LISTED_BROKER_LIMIT: usize = 10;

/// The read bit of a RocketMQ Topic permission.
const PERM_READ: u32 = 0b100;

/// The write bit of a RocketMQ Topic permission.
const PERM_WRITE: u32 = 0b010;

/// Whether a Topic permission lets consumers read.
pub(crate) fn permits_read(perm: u32) -> bool {
    perm & PERM_READ != 0
}

/// Whether a Topic permission lets producers write.
pub(crate) fn permits_write(perm: u32) -> bool {
    perm & PERM_WRITE != 0
}

/// The first [`LISTED_BROKER_LIMIT`] names of a sorted set.
fn listed(names: &BTreeSet<String>) -> Vec<String> {
    names.iter().take(LISTED_BROKER_LIMIT).cloned().collect()
}

/// Connections and subscription configuration of one Consumer Group, over every Broker that holds
/// its configuration.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct ConsumerGroupState {
    pub cluster: String,
    pub consumer_group: String,
    /// Brokers that hold the subscription configuration of the group.
    pub configured_broker_count: usize,
    pub online_consumer_count: u64,
    /// Brokers that at least one consumer of the group is connected to.
    pub online_broker_count: usize,
    /// Brokers that report no consumer of the group.
    pub offline_broker_count: usize,
    pub offline_brokers: Vec<String>,
    /// Brokers whose configuration disables consumption for the group.
    pub consume_disabled_broker_count: usize,
    pub consume_disabled_brokers: Vec<String>,
    /// Whether every configured Broker reported its connections. Only then does an online count of
    /// zero establish that no consumer is online.
    pub connections_complete: bool,
    pub consume_types: Vec<ConsumerConsumeType>,
    pub message_models: Vec<ConsumerMessageModel>,
    pub generated_at: String,
    #[serde(skip)]
    offline: BTreeSet<String>,
    #[serde(skip)]
    consume_disabled: BTreeSet<String>,
}

impl ConsumerGroupState {
    /// Summarizes the details of a group. `sources_complete` is `false` when a Broker did not
    /// answer, in which case its row is missing and the connection view is incomplete.
    pub(crate) fn from_details(details: GetConsumerGroupDetailsOutput, sources_complete: bool) -> Self {
        let mut offline = BTreeSet::new();
        let mut consume_disabled = BTreeSet::new();
        let mut configured_broker_count = 0;
        let mut online_broker_count = 0;
        let mut connections_complete = sources_complete;
        for row in &details.brokers {
            if row.config_state != ConsumerGroupConfigPresence::Present {
                continue;
            }
            configured_broker_count += 1;
            if row.consume_enable == Some(false) {
                consume_disabled.insert(row.broker_name.clone());
            }
            match row.connection_state {
                Some(ConsumerConnectionState::Online) => online_broker_count += 1,
                Some(ConsumerConnectionState::Offline) => {
                    offline.insert(row.broker_name.clone());
                }
                None => connections_complete = false,
            }
        }
        let consume_types = [
            ConsumerConsumeType::Push,
            ConsumerConsumeType::Pull,
            ConsumerConsumeType::Pop,
            ConsumerConsumeType::Unknown,
        ]
        .into_iter()
        .filter(|kind| {
            details
                .brokers
                .iter()
                .any(|row| row.consume_type.as_ref() == Some(kind))
        })
        .collect();
        let message_models = [
            ConsumerMessageModel::Clustering,
            ConsumerMessageModel::Broadcasting,
            ConsumerMessageModel::Unknown,
        ]
        .into_iter()
        .filter(|model| {
            details
                .brokers
                .iter()
                .any(|row| row.message_model.as_ref() == Some(model))
        })
        .collect();
        Self {
            cluster: details.cluster,
            consumer_group: details.consumer_group,
            configured_broker_count,
            online_consumer_count: details.total_connection_count,
            online_broker_count,
            offline_broker_count: offline.len(),
            offline_brokers: listed(&offline),
            consume_disabled_broker_count: consume_disabled.len(),
            consume_disabled_brokers: listed(&consume_disabled),
            connections_complete,
            consume_types,
            message_models,
            generated_at: details.generated_at,
            offline,
            consume_disabled,
        }
    }

    /// Whether the evidence establishes that no consumer of the group is connected anywhere.
    pub(crate) fn no_consumer_online(&self) -> bool {
        self.connections_complete && self.configured_broker_count > 0 && self.online_broker_count == 0
    }

    /// Whether `broker_name` reported that no consumer of the group is connected to it.
    pub(crate) fn is_offline_on(&self, broker_name: &str) -> bool {
        self.offline.contains(broker_name)
    }

    /// Whether every Broker that holds the configuration disables consumption.
    pub(crate) fn consumption_disabled_everywhere(&self) -> bool {
        self.configured_broker_count > 0 && self.consume_disabled.len() == self.configured_broker_count
    }

    pub(crate) fn uses_pop(&self) -> bool {
        self.consume_types.contains(&ConsumerConsumeType::Pop)
    }
}

/// The parts of a Topic configuration that decide whether it can be consumed, over every Broker
/// that holds the Topic.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct TopicConfigSummary {
    pub cluster: String,
    pub topic: String,
    pub broker_count: usize,
    /// Brokers whose configuration of the Topic denies reads.
    pub read_denied_broker_count: usize,
    pub read_denied_brokers: Vec<String>,
    /// Configuration fields that differ between Brokers.
    pub inconsistent_fields: Vec<TopicConfigDifferenceField>,
    pub generated_at: String,
    #[serde(skip)]
    read_denied: BTreeSet<String>,
    #[serde(skip)]
    readable: BTreeSet<String>,
}

impl TopicConfigSummary {
    pub(crate) fn from_config(config: GetTopicConfigOutput) -> Self {
        let mut readable = BTreeSet::new();
        let mut read_denied = BTreeSet::new();
        for row in &config.brokers {
            if permits_read(row.perm) {
                readable.insert(row.broker_name.clone());
            } else {
                read_denied.insert(row.broker_name.clone());
            }
        }
        Self {
            cluster: config.cluster,
            topic: config.topic,
            broker_count: config.brokers.len(),
            read_denied_broker_count: read_denied.len(),
            read_denied_brokers: listed(&read_denied),
            inconsistent_fields: config.inconsistent_fields,
            generated_at: config.generated_at,
            read_denied,
            readable,
        }
    }

    /// Whether the Topic configuration on `broker_name` denies reads.
    pub(crate) fn denies_read_on(&self, broker_name: &str) -> bool {
        self.read_denied.contains(broker_name)
    }

    /// Whether the Topic configuration on `broker_name` allows reads.
    pub(crate) fn allows_read_on(&self, broker_name: &str) -> bool {
        self.readable.contains(broker_name)
    }

    /// Every Broker whose configuration of the Topic denies reads.
    pub(crate) fn read_denied(&self) -> &BTreeSet<String> {
        &self.read_denied
    }
}

/// What a Topic holds and when it last grew.
///
/// The diagnoses read it for the retry and the dead-letter Topic of a Consumer Group, which
/// exist only once a message was written to them, and for a Topic that is diagnosed itself.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct TopicActivity {
    pub cluster: String,
    pub topic: String,
    /// `false` when the Topic does not exist: nothing was ever written to it.
    pub exists: bool,
    pub queue_count: usize,
    pub message_count: u64,
    /// Store time of the newest message, which is when the Topic last grew.
    pub last_message_at: Option<String>,
    /// How long before this observation that message was stored.
    pub last_message_age_ms: Option<u64>,
    pub generated_at: String,
}

impl TopicActivity {
    /// The summary of a Topic that a completed lookup did not find.
    pub(crate) fn absent(cluster: &str, topic: String) -> Self {
        Self {
            cluster: cluster.to_string(),
            topic,
            exists: false,
            queue_count: 0,
            message_count: 0,
            last_message_at: None,
            last_message_age_ms: None,
            generated_at: observed_at(),
        }
    }

    /// Summarizes the statistics of an existing Topic. `queues` are the rows the source returned;
    /// the counts cover every queue, including rows beyond the source's row limit.
    pub(crate) fn from_stats(
        cluster: &str,
        topic: String,
        queue_count: usize,
        message_count: u64,
        queues: &[TopicStatsQueueRow],
        now_millis: i64,
    ) -> Self {
        // A queue that never held a message reports the Unix epoch, which is not a store time.
        let newest = queues
            .iter()
            .filter_map(|queue| {
                let stored_at = queue.last_update_at.as_deref()?;
                let millis = millis_from_observed_at(stored_at).filter(|millis| *millis > 0)?;
                Some((millis, stored_at))
            })
            .max_by_key(|(millis, _)| *millis);
        Self {
            cluster: cluster.to_string(),
            topic,
            exists: true,
            queue_count,
            message_count,
            last_message_at: newest.map(|(_, stored_at)| stored_at.to_string()),
            last_message_age_ms: newest.map(|(millis, _)| elapsed_millis(millis, now_millis)),
            generated_at: observed_at(),
        }
    }

    /// Whether the Topic holds messages and received one within `window_ms`.
    pub(crate) fn grew_within(&self, window_ms: u64) -> bool {
        self.message_count > 0 && self.last_message_age_ms.is_some_and(|age| age <= window_ms)
    }
}

/// Milliseconds from `earlier` to `now`. A Broker clock ahead of this server yields zero.
pub(crate) fn elapsed_millis(earlier: i64, now: i64) -> u64 {
    u64::try_from(now.saturating_sub(earlier)).unwrap_or_default()
}

/// A duration in the largest unit that keeps it readable: `850 ms`, `45 s`, `12 min`, `3.5 h`, `2.1 d`.
pub(crate) fn duration_text(millis: u64) -> String {
    const SECOND: u64 = 1_000;
    const MINUTE: u64 = 60 * SECOND;
    const HOUR: u64 = 60 * MINUTE;
    const DAY: u64 = 24 * HOUR;
    if millis < SECOND {
        format!("{millis} ms")
    } else if millis < 2 * MINUTE {
        format!("{} s", millis / SECOND)
    } else if millis < 2 * HOUR {
        format!("{} min", millis / MINUTE)
    } else if millis < 2 * DAY {
        format!("{:.1} h", millis as f64 / HOUR as f64)
    } else {
        format!("{:.1} d", millis as f64 / DAY as f64)
    }
}

/// A message rate in the unit that keeps it readable: per second, per minute or per hour.
pub(crate) fn rate_text(messages_per_second: f64) -> String {
    if messages_per_second >= 1.0 {
        format!("{messages_per_second:.1} messages per second")
    } else if messages_per_second * 60.0 >= 1.0 {
        format!("{:.1} messages per minute", messages_per_second * 60.0)
    } else {
        format!("{:.1} messages per hour", messages_per_second * 3_600.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::config_tools::TopicConfigObservationRow;
    use crate::tools::consumer_tools::ConsumerGroupDetailsBrokerRow;

    fn group_row(broker_name: &str, state: Option<ConsumerConnectionState>) -> ConsumerGroupDetailsBrokerRow {
        ConsumerGroupDetailsBrokerRow {
            broker_name: broker_name.to_string(),
            config_state: ConsumerGroupConfigPresence::Present,
            config_version: Some(1),
            consume_enable: Some(true),
            consume_from_min_enable: Some(true),
            consume_broadcast_enable: Some(false),
            consume_message_orderly: Some(false),
            retry_queue_nums: Some(1),
            retry_max_times: Some(16),
            notify_consumer_ids_changed_enable: Some(true),
            consume_timeout_minutes: Some(15),
            connection_count: u64::from(state == Some(ConsumerConnectionState::Online)),
            consume_type: (state == Some(ConsumerConnectionState::Online)).then_some(ConsumerConsumeType::Push),
            message_model: None,
            consume_from_where: None,
            connection_state: state,
        }
    }

    fn details(rows: Vec<ConsumerGroupDetailsBrokerRow>) -> GetConsumerGroupDetailsOutput {
        GetConsumerGroupDetailsOutput {
            cluster: "local-dev".to_string(),
            consumer_group: "order-service".to_string(),
            total_connection_count: rows.iter().map(|row| row.connection_count).sum(),
            brokers: rows,
            generated_at: "2026-10-10T00:00:00.000Z".to_string(),
        }
    }

    #[test]
    fn group_state_counts_only_brokers_that_hold_the_configuration() {
        let mut absent = group_row("broker-c", None);
        absent.config_state = ConsumerGroupConfigPresence::Absent;
        let mut disabled = group_row("broker-b", Some(ConsumerConnectionState::Offline));
        disabled.consume_enable = Some(false);
        let state = ConsumerGroupState::from_details(
            details(vec![
                group_row("broker-a", Some(ConsumerConnectionState::Online)),
                disabled,
                absent,
            ]),
            true,
        );

        assert_eq!(state.configured_broker_count, 2);
        assert_eq!(state.online_broker_count, 1);
        assert_eq!(state.online_consumer_count, 1);
        assert_eq!(state.offline_brokers, ["broker-b"]);
        assert_eq!(state.consume_disabled_brokers, ["broker-b"]);
        assert!(state.connections_complete);
        assert!(!state.no_consumer_online());
        assert!(state.is_offline_on("broker-b"));
        assert!(!state.is_offline_on("broker-a"));
        assert!(!state.consumption_disabled_everywhere());
        assert_eq!(state.consume_types, [ConsumerConsumeType::Push]);
    }

    #[test]
    fn group_state_does_not_claim_offline_without_a_complete_connection_view() {
        let offline = || group_row("broker-a", Some(ConsumerConnectionState::Offline));

        assert!(ConsumerGroupState::from_details(details(vec![offline()]), true).no_consumer_online());
        // A Broker that did not answer has no row, and one whose connection lookup failed has no state.
        assert!(!ConsumerGroupState::from_details(details(vec![offline()]), false).no_consumer_online());
        assert!(
            !ConsumerGroupState::from_details(details(vec![offline(), group_row("broker-b", None)]), true)
                .no_consumer_online()
        );
        assert!(!ConsumerGroupState::from_details(details(Vec::new()), true).no_consumer_online());
    }

    #[test]
    fn group_state_lists_a_bounded_number_of_brokers_and_counts_all_of_them() {
        let state = ConsumerGroupState::from_details(
            details(
                (0..25)
                    .map(|index| group_row(&format!("broker-{index:02}"), Some(ConsumerConnectionState::Offline)))
                    .collect(),
            ),
            true,
        );

        assert_eq!(state.offline_broker_count, 25);
        assert_eq!(state.offline_brokers.len(), LISTED_BROKER_LIMIT);
        assert_eq!(state.offline_brokers[0], "broker-00");
        assert!(state.is_offline_on("broker-24"), "the rules see Brokers the list omits");
    }

    #[test]
    fn topic_config_summary_separates_readable_from_read_denied_brokers() {
        let row = |broker_name: &str, perm: u32| TopicConfigObservationRow {
            broker_name: broker_name.to_string(),
            version: 1,
            read_queue_nums: 4,
            write_queue_nums: 4,
            perm,
            order: false,
            message_type: "NORMAL".to_string(),
        };
        let summary = TopicConfigSummary::from_config(GetTopicConfigOutput {
            cluster: "local-dev".to_string(),
            topic: "orders".to_string(),
            brokers: vec![row("broker-a", 6), row("broker-b", 2), row("broker-c", 4)],
            inconsistent_fields: vec![TopicConfigDifferenceField::Perm],
            generated_at: "2026-10-10T00:00:00.000Z".to_string(),
        });

        assert_eq!(summary.broker_count, 3);
        assert_eq!(summary.read_denied_brokers, ["broker-b"]);
        assert!(summary.denies_read_on("broker-b"));
        assert!(summary.allows_read_on("broker-a"));
        assert!(summary.allows_read_on("broker-c"), "a read-only Topic is readable");
        assert!(!summary.allows_read_on("broker-z") && !summary.denies_read_on("broker-z"));
    }

    #[test]
    fn system_topic_activity_reports_when_the_topic_last_grew() {
        let queue = |queue_id: i32, last_update_at: Option<&str>| TopicStatsQueueRow {
            broker_name: "broker-a".to_string(),
            queue_id,
            min_offset: 0,
            max_offset: 3,
            message_count: 3,
            last_update_at: last_update_at.map(str::to_string),
        };
        let now = millis_from_observed_at("2026-10-10T00:10:00.000Z").unwrap();
        let activity = TopicActivity::from_stats(
            "local-dev",
            "%RETRY%order-service".to_string(),
            3,
            9,
            &[
                queue(0, Some("2026-10-10T00:01:00.000Z")),
                queue(1, Some("2026-10-10T00:08:00.000Z")),
                queue(2, Some("1970-01-01T00:00:00.000Z")),
            ],
            now,
        );

        assert!(activity.exists);
        assert_eq!(activity.message_count, 9);
        assert_eq!(activity.last_message_at.as_deref(), Some("2026-10-10T00:08:00.000Z"));
        assert_eq!(activity.last_message_age_ms, Some(120_000));
        assert!(activity.grew_within(120_000));
        assert!(!activity.grew_within(119_999));

        let absent = TopicActivity::absent("local-dev", "%DLQ%order-service".to_string());
        assert!(!absent.exists);
        assert!(!absent.grew_within(u64::MAX));
    }

    #[test]
    fn elapsed_time_is_never_negative() {
        assert_eq!(elapsed_millis(1_000, 4_000), 3_000);
        assert_eq!(elapsed_millis(4_000, 1_000), 0);
        assert_eq!(elapsed_millis(i64::MIN, i64::MAX), u64::MAX / 2);
    }

    #[test]
    fn durations_are_written_in_a_readable_unit() {
        let text = [850, 45_000, 12 * 60_000, 210 * 60_000, 50 * 3_600_000 + 24 * 60_000].map(duration_text);
        assert_eq!(text, ["850 ms", "45 s", "12 min", "3.5 h", "2.1 d"]);
        assert_eq!(
            [100_100.0, 1.3, 0.5, 0.000_278].map(rate_text),
            [
                "100100.0 messages per second",
                "1.3 messages per second",
                "30.0 messages per minute",
                "1.0 messages per hour"
            ]
        );
    }
}
