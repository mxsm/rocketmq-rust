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

//! The diagnosis of the path a message takes from a Topic to a Consumer Group.
//!
//! It asks, in the order a message travels: can producers write to the Topic, can consumers read
//! it, is the group there and allowed to consume, has it made progress on the Topic, and does it
//! keep up. With a message identifier it also says where that message stands. The diagnosis never
//! reads or reports a message body, and it names a message by its pseudonym only.

use serde::Serialize;
use serde_json::json;
use serde_json::Value;

use crate::model::contract::millis_from_observed_at;
use crate::model::contract::observed_at;
use crate::model::diagnosis::ConfidenceBand;
use crate::model::diagnosis::DiagnosisReport;
use crate::model::diagnosis::EvidenceKind;
use crate::model::diagnosis::EvidenceSnapshot;
use crate::model::diagnosis::MetricWatchItem;
use crate::model::diagnosis::Recommendation;
use crate::model::diagnosis::Severity;
use crate::service::diagnosis_collector::evidence_id::CONSUMER_GROUP;
use crate::service::diagnosis_collector::evidence_id::CONSUMER_LAG;
use crate::service::diagnosis_collector::evidence_id::TOPIC_ROUTE;
use crate::service::diagnosis_collector::ConsumerLagSummary;
use crate::service::diagnosis_collector::TopicRouteSummary;
use crate::service::diagnosis_evidence::duration_text;
use crate::service::diagnosis_evidence::elapsed_millis;
use crate::service::diagnosis_evidence::permits_read;
use crate::service::diagnosis_evidence::permits_write;
use crate::service::diagnosis_evidence::ConsumerGroupState;
use crate::service::diagnosis_evidence::TopicActivity;
use crate::service::diagnosis_evidence::DEAD_LETTER_TOPIC_PREFIX;
use crate::service::diagnosis_evidence::LISTED_BROKER_LIMIT;
use crate::service::diagnosis_evidence::RETRY_TOPIC_PREFIX;
use crate::service::diagnosis_rules::conclusion;
use crate::service::diagnosis_rules::severity;
use crate::service::diagnosis_rules::ConsumerLagPolicy;
use crate::service::diagnosis_rules::Finding;
use crate::service::diagnosis_rules::Thresholds;
use crate::service::diagnosis_service;
use crate::service::diagnosis_service::HealthFinding;
use crate::service::diagnosis_service::HealthReport;
use crate::service::diagnosis_snapshot;
use crate::tools::consumer_tools::QueueLag;
use crate::tools::executor::NotFoundEntity;
use crate::tools::executor::ToolFailure;
use crate::tools::message_tools::MessageMetadataOutput;

pub(crate) const MESSAGE_DELIVERY_EVIDENCE_VERSION: &str = "rocketmq-mcp.evidence.message-delivery.v1";
pub(crate) const MESSAGE_DELIVERY_RULES_VERSION: &str = "rocketmq-mcp.rules.message-delivery.v1";

const TOPIC_STATS: &str = "topic_stats";
const MESSAGE_METADATA: &str = "message_metadata";

/// What a delivery diagnosis is about. The message is named by its pseudonym: the identifier the
/// caller sent never reaches a report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeliveryTarget {
    pub cluster: String,
    pub topic: String,
    pub consumer_group: String,
    pub message_alias: Option<String>,
}

impl DeliveryTarget {
    /// What the report is about, as its summary names it.
    fn subject(&self) -> String {
        format!("Delivery from topic {} to group {}", self.topic, self.consumer_group)
    }

    fn json(&self) -> Value {
        json!({
            "topic": self.topic,
            "consumer_group": self.consumer_group,
            "message_alias": self.message_alias,
        })
    }

    /// The Topic that holds the messages the group failed to consume.
    fn retry_topic(&self) -> String {
        format!("{RETRY_TOPIC_PREFIX}{}", self.consumer_group)
    }

    /// The Topic that holds the messages the group gave up on.
    fn dead_letter_topic(&self) -> String {
        format!("{DEAD_LETTER_TOPIC_PREFIX}{}", self.consumer_group)
    }
}

/// Where a message stands relative to the offset of a Consumer Group on its queue.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MessagePosition {
    /// The offset of the group is past the message.
    Consumed,
    /// The offset of the group has not reached the message.
    Waiting,
    /// The offsets of the group do not settle it.
    Undetermined,
}

impl MessagePosition {
    /// Compares a message with the offsets of a group on the Topic of the message.
    ///
    /// A message names its queue by number but not by Broker, and every Broker that hosts the
    /// Topic has a queue with that number. A queue that ends at or before the offset of the
    /// message cannot hold it. The position is stated when the queues that can hold it agree.
    pub(crate) fn of(queue_id: i32, queue_offset: i64, queues: &[QueueLag]) -> Self {
        let (consumed, waiting) = queues
            .iter()
            .filter(|queue| queue.queue_id == queue_id && queue_offset < queue.broker_offset)
            .fold((0_usize, 0_usize), |(consumed, waiting), queue| {
                if queue_offset < queue.consumer_offset {
                    (consumed + 1, waiting)
                } else {
                    (consumed, waiting + 1)
                }
            });
        match (consumed, waiting) {
            (1.., 0) => Self::Consumed,
            (0, 1..) => Self::Waiting,
            _ => Self::Undetermined,
        }
    }
}

/// One message as a lookup found it, without its body and under its pseudonyms.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct MessageObservation {
    pub message_alias: String,
    pub unique_message_alias: Option<String>,
    /// The Topic the message is stored on, which need not be the diagnosed one.
    pub topic: String,
    pub queue_id: i32,
    pub queue_offset: i64,
    pub born_at: Option<String>,
    pub stored_at: Option<String>,
    /// How long before this observation the message was stored.
    pub stored_age_ms: Option<u64>,
    /// How many consumption attempts failed before this copy of the message was stored.
    pub reconsume_times: i32,
    /// Where the message stands for the diagnosed group. `None` for a message of another Topic,
    /// which the offsets of the group on the diagnosed Topic say nothing about.
    pub position: Option<MessagePosition>,
    pub generated_at: String,
}

impl MessageObservation {
    /// Describes a message that a lookup found.
    ///
    /// `group_queues` are the offsets of the diagnosed group on the diagnosed Topic, or `None`
    /// when they were not read from every Broker: a queue that is missing from them could be the
    /// one that holds the message.
    pub(crate) fn observe(
        metadata: MessageMetadataOutput,
        diagnosed_topic: &str,
        group_queues: Option<&[QueueLag]>,
        now_millis: i64,
    ) -> Self {
        let position = (metadata.topic == diagnosed_topic).then(|| match group_queues {
            Some(queues) => MessagePosition::of(metadata.queue_id, metadata.queue_offset, queues),
            None => MessagePosition::Undetermined,
        });
        // A message without a store time reports the Unix epoch, which is not one.
        let stored_age_ms = metadata
            .stored_at
            .as_deref()
            .and_then(millis_from_observed_at)
            .filter(|stored| *stored > 0)
            .map(|stored| elapsed_millis(stored, now_millis));
        Self {
            message_alias: metadata.message_alias,
            unique_message_alias: metadata.unique_message_alias,
            topic: metadata.topic,
            queue_id: metadata.queue_id,
            queue_offset: metadata.queue_offset,
            born_at: metadata.born_at,
            stored_at: metadata.stored_at,
            stored_age_ms,
            reconsume_times: metadata.reconsume_times,
            position,
            generated_at: observed_at(),
        }
    }
}

/// The evidence one delivery diagnosis is evaluated on.
///
/// A part that could not be read is an error here and missing evidence in the report.
pub(crate) struct MessageDeliveryEvidence {
    /// Whether the Topic exists, and who may write to it and read it on each Broker.
    pub route: Result<TopicRouteSummary, ToolFailure>,
    pub group: Result<ConsumerGroupState, ToolFailure>,
    /// The progress of the group on the Topic.
    pub lag: Result<ConsumerLagSummary, ToolFailure>,
    /// What the Topic holds and when it last received a message.
    pub activity: Result<TopicActivity, ToolFailure>,
    /// The message the caller asked about, or `None` when none was named.
    pub message: Option<Result<MessageObservation, ToolFailure>>,
}

impl MessageDeliveryEvidence {
    fn snapshot(&self, target: &DeliveryTarget) -> EvidenceSnapshot {
        let query_hash = diagnosis_snapshot::query_hash(&[
            &target.cluster,
            &target.topic,
            &target.consumer_group,
            target.message_alias.as_deref().unwrap_or_default(),
        ]);
        let mut items = vec![
            diagnosis_snapshot::item(
                TOPIC_ROUTE,
                "rocketmq_get_topic_route",
                EvidenceKind::TopicRoute,
                &query_hash,
                &self.route,
            ),
            diagnosis_snapshot::item(
                CONSUMER_GROUP,
                "rocketmq_get_consumer_group_details",
                EvidenceKind::ConsumerGroup,
                &query_hash,
                &self.group,
            ),
            diagnosis_snapshot::item(
                CONSUMER_LAG,
                "rocketmq_get_consumer_lag",
                EvidenceKind::ConsumerLag,
                &query_hash,
                &self.lag,
            ),
            diagnosis_snapshot::item(
                TOPIC_STATS,
                "rocketmq_get_topic_stats",
                EvidenceKind::TopicStats,
                &query_hash,
                &self.activity,
            ),
        ];
        // A message that was not asked about is not evidence that is missing.
        if let Some(message) = &self.message {
            items.push(diagnosis_snapshot::item(
                MESSAGE_METADATA,
                "rocketmq_get_message_metadata",
                EvidenceKind::MessageMetadata,
                &query_hash,
                message,
            ));
        }
        diagnosis_snapshot::snapshot(
            MESSAGE_DELIVERY_EVIDENCE_VERSION,
            query_hash,
            &target.cluster,
            target.json(),
            items,
        )
    }

    /// The targets that a completed lookup established do not exist, the Topic first.
    fn absent_targets(&self) -> Vec<AbsentTarget> {
        let absent = |error: Option<&ToolFailure>| error.and_then(ToolFailure::not_found_entity);
        let mut targets = Vec::new();
        if absent(self.route.as_ref().err()) == Some(NotFoundEntity::Topic) {
            targets.push(AbsentTarget {
                entity: NotFoundEntity::Topic,
                evidence: TOPIC_ROUTE,
                source_tool: "rocketmq_get_topic_route",
            });
        }
        if absent(self.group.as_ref().err()) == Some(NotFoundEntity::ConsumerGroup) {
            targets.push(AbsentTarget {
                entity: NotFoundEntity::ConsumerGroup,
                evidence: CONSUMER_GROUP,
                source_tool: "rocketmq_get_consumer_group_details",
            });
        }
        targets
    }
}

/// A target of the diagnosis that does not exist, and the evidence that says so.
struct AbsentTarget {
    entity: NotFoundEntity,
    evidence: &'static str,
    source_tool: &'static str,
}

/// What the rules read: the evidence that was collected.
struct RuleInput<'a> {
    target: &'a DeliveryTarget,
    route: Option<&'a TopicRouteSummary>,
    group: Option<&'a ConsumerGroupState>,
    lag: Option<&'a ConsumerLagSummary>,
    activity: Option<&'a TopicActivity>,
    message: Option<&'a Result<MessageObservation, ToolFailure>>,
    thresholds: &'a Thresholds,
}

impl<'a> RuleInput<'a> {
    /// The Brokers whose route row fails `permits`, by name, and how many Brokers host the Topic.
    /// `None` when every Broker passes.
    fn denied(&self, permits: fn(u32) -> bool) -> Option<(Vec<&'a str>, usize)> {
        let route = self.route?;
        let denied = route
            .queues
            .iter()
            .filter(|queue| !permits(queue.perm))
            .map(|queue| queue.broker_name.as_str())
            .collect::<Vec<_>>();
        (!denied.is_empty()).then_some((denied, route.queues.len()))
    }

    /// Whether the evidence establishes that the group cannot consume the Topic at all.
    fn consumption_stopped(&self) -> bool {
        self.group
            .is_some_and(|group| group.no_consumer_online() || group.consumption_disabled_everywhere())
            || self
                .denied(permits_read)
                .is_some_and(|(denied, hosting)| denied.len() == hosting)
    }

    /// The message, when the lookup found it on `topic`.
    fn message_on(&self, topic: &str) -> Option<&'a MessageObservation> {
        self.message?.as_ref().ok().filter(|message| message.topic == topic)
    }

    /// The message, when the lookup found it on the diagnosed Topic at `position`.
    fn message_at(&self, position: MessagePosition) -> Option<&'a MessageObservation> {
        self.message_on(&self.target.topic)
            .filter(|message| message.position == Some(position))
    }
}

type Rule = fn(&RuleInput<'_>) -> Option<HealthFinding>;

/// The rule table, in the order a message travels. The identifier of a rule is the `rule_id` of
/// the finding it returns; it is part of the report contract and never changes once released.
const RULES: &[Rule] = &[
    write_permission_missing,
    read_permission_missing,
    no_online_consumer,
    consumption_disabled,
    group_without_progress,
    consumer_backlog,
    topic_has_no_messages,
    message_not_found,
    message_dead_lettered,
    message_waiting_for_retry,
    message_on_other_topic,
    message_waiting,
    message_delivered,
    message_position_undetermined,
];

/// Evaluates the rules on `evidence` and writes the report.
///
/// The report is as severe as its worst finding. Without a finding it is `healthy`, unless a half
/// of the path was not observed: the Topic route, or everything about the group. Then nothing was
/// found on a path that was not looked at, and the report is `unknown`.
pub(crate) fn evaluate(
    target: &DeliveryTarget,
    evidence: MessageDeliveryEvidence,
    policy: &ConsumerLagPolicy,
) -> DiagnosisReport {
    let thresholds = policy.thresholds_for(Some(&target.topic), &target.consumer_group);
    let snapshot = evidence.snapshot(target);
    let input = RuleInput {
        target,
        route: evidence.route.as_ref().ok(),
        group: evidence.group.as_ref().ok(),
        lag: evidence.lag.as_ref().ok(),
        activity: evidence.activity.as_ref().ok(),
        message: evidence.message.as_ref(),
        thresholds: &thresholds,
    };
    let findings = RULES.iter().filter_map(|rule| rule(&input)).collect::<Vec<_>>();
    let unobserved = match (findings.is_empty(), input.route, input.group, input.lag) {
        (false, ..) => None,
        (true, None, ..) => Some("Topic route"),
        (true, Some(_), None, None) => Some("Consumer Group"),
        (true, ..) => None,
    };
    // What each item adds to the confidence, in thousandths. A message that was not asked about
    // is not missing, so the route carries its share.
    let (route_weight, message_weight) = if evidence.message.is_some() {
        (400, 50)
    } else {
        (450, 0)
    };
    let weights = [
        (TOPIC_ROUTE, route_weight),
        (CONSUMER_GROUP, 250),
        (CONSUMER_LAG, 200),
        (TOPIC_STATS, 50),
        (MESSAGE_METADATA, message_weight),
    ];
    let mut target_value = target.json();
    target_value["lag_threshold"] = json!(thresholds.lag);
    target_value["consume_delay_threshold_ms"] = json!(thresholds.consume_delay_ms);
    let subject = target.subject();
    let mut report = diagnosis_service::build_health_report(HealthReport {
        report_type: "message_delivery",
        rules_version: MESSAGE_DELIVERY_RULES_VERSION,
        target: target_value,
        subject: subject.clone(),
        policy_profile: &policy.profile,
        missing_primary: unobserved,
        healthy_summary: healthy_summary(&input),
        confidence: diagnosis_service::weighted_confidence(&snapshot, 0, &weights).max(0.20),
        findings,
        risks: vec![
            "A Topic permission and a subscription configuration apply to every producer and consumer that uses them."
                .to_string(),
            "Resetting offsets to make one message deliver again redelivers every message after it too.".to_string(),
        ],
        follow_up_metrics: vec![
            MetricWatchItem {
                name: "total_lag".to_string(),
                target: "consumer_group/topic".to_string(),
                reason: "Shows whether the group keeps up with the topic.".to_string(),
            },
            MetricWatchItem {
                name: "online_consumer_count".to_string(),
                target: "consumer_group".to_string(),
                reason: "Shows whether consumers of the group are connected.".to_string(),
            },
        ],
        snapshot,
    });
    if let Some(unobserved) = unobserved {
        report.summary = format!("{subject} is Unknown because {unobserved} evidence is missing.");
    }
    let absent = evidence.absent_targets();
    if !absent.is_empty() {
        report_absent_targets(target, &absent, &mut report);
    }
    report
}

/// What a report without a finding says: only what the collected evidence showed.
fn healthy_summary(input: &RuleInput<'_>) -> String {
    let mut parts = vec!["the topic can be written and read on every Broker that hosts it"];
    if input.group.is_some_and(|group| group.online_broker_count > 0) {
        parts.push("consumers of the group are online");
    }
    if input.lag.is_some() {
        parts.push("the lag of the group is within the threshold");
    }
    format!("{}.", parts.join(", "))
}

/// Leads the report with the definite conclusion: a target does not exist.
///
/// What the rules found on the rest of the path stays in the report, after that conclusion.
fn report_absent_targets(target: &DeliveryTarget, absent: &[AbsentTarget], report: &mut DiagnosisReport) {
    const ABSENT_TARGET_CONFIDENCE: f32 = 0.90;

    let name = |entity: NotFoundEntity| match entity {
        NotFoundEntity::Topic => target.topic.as_str(),
        _ => target.consumer_group.as_str(),
    };
    report.severity = Severity::Unknown;
    report.summary = format!(
        "{} is Unknown because the {} does not exist in the selected cluster.",
        target.subject(),
        absent[0].entity.label()
    );
    report.confidence = ABSENT_TARGET_CONFIDENCE;
    report.confidence_band = ConfidenceBand::High;
    if report.root_causes.is_empty() {
        // Only the step for a report without a finding is listed, and it does not apply.
        report.recommendations.clear();
    }
    let mut root_causes = absent
        .iter()
        .map(|missing| {
            diagnosis_service::conclude(
                conclusion::TARGET_NOT_FOUND,
                missing.evidence,
                &format!("The {} does not exist in the selected cluster", missing.entity.label()),
                ABSENT_TARGET_CONFIDENCE,
                &format!(
                    "{} reported that {} {} was not found.",
                    missing.source_tool,
                    missing.entity.label(),
                    name(missing.entity)
                ),
            )
        })
        .collect::<Vec<_>>();
    root_causes.append(&mut report.root_causes);
    report.root_causes = root_causes;
    let mut recommendations = absent
        .iter()
        .map(|missing| Recommendation {
            action: missing.entity.suggestion().to_string(),
            priority: "high".to_string(),
            rationale: format!(
                "The {} was not found, so nothing can be delivered.",
                missing.entity.label()
            ),
            risk: "None. Listing names is read-only.".to_string(),
            verification: "Run the diagnosis again with the confirmed name.".to_string(),
        })
        .collect::<Vec<_>>();
    recommendations.append(&mut report.recommendations);
    report.recommendations = recommendations;
}

fn finding(
    rule_id: &'static str,
    severity: Severity,
    area: &str,
    evidence_refs: &[&'static str],
    cause: &str,
    reasoning: String,
    recommendation: Recommendation,
) -> HealthFinding {
    HealthFinding {
        rule_id,
        severity,
        area: area.to_string(),
        finding: Finding {
            cause: cause.to_string(),
            // A rule reads a state the cluster reports about itself, so what fires is established.
            confidence: 0.90,
            reasoning,
            evidence_refs: evidence_refs.to_vec(),
            stops_consumption: false,
            recommendation,
        },
    }
}

fn recommend(action: &str, priority: &str, rationale: &str, risk: &str, verification: &str) -> Recommendation {
    Recommendation {
        action: action.to_string(),
        priority: priority.to_string(),
        rationale: rationale.to_string(),
        risk: risk.to_string(),
        verification: verification.to_string(),
    }
}

/// Writes up to [`LISTED_BROKER_LIMIT`] names and says how many more there are.
fn names(all: &[&str]) -> String {
    let listed = all.iter().take(LISTED_BROKER_LIMIT).copied().collect::<Vec<_>>();
    match all.len() - listed.len() {
        0 => listed.join(", "),
        omitted => format!("{} and {omitted} more", listed.join(", ")),
    }
}

fn write_permission_missing(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let (denied, hosting) = input.denied(permits_write)?;
    let everywhere = denied.len() == hosting;
    Some(finding(
        "write_permission_missing",
        if everywhere { Severity::Critical } else { Severity::High },
        "produce",
        &[TOPIC_ROUTE],
        if everywhere {
            "No Broker that hosts the topic accepts writes"
        } else {
            "Not every Broker that hosts the topic accepts writes"
        },
        format!(
            "Writes are denied on {} of {hosting} Brokers that host the topic: {}.",
            denied.len(),
            names(&denied)
        ),
        recommend(
            "Restore the write permission where it was not removed on purpose. Plan a Topic permission change with \
             a dry run of rocketmq_upsert_topic on the control MCP server; a Broker permission is part of the Broker \
             configuration.",
            "high",
            "A producer cannot send to a queue that denies writes, so fewer or no messages reach the topic.",
            "Write permission is removed on purpose while a Broker is drained.",
            "rocketmq_get_topic_route reports a writable permission on every Broker.",
        ),
    ))
}

fn read_permission_missing(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let (denied, hosting) = input.denied(permits_read)?;
    let everywhere = denied.len() == hosting;
    Some(finding(
        "read_permission_missing",
        if everywhere { Severity::Critical } else { Severity::High },
        "consume",
        &[TOPIC_ROUTE],
        if everywhere {
            "No Broker that hosts the topic serves reads"
        } else {
            "Not every Broker that hosts the topic serves reads"
        },
        format!(
            "Reads are denied on {} of {hosting} Brokers that host the topic: {}.",
            denied.len(),
            names(&denied)
        ),
        recommend(
            "Restore the read permission where it was not removed on purpose. Plan a Topic permission change with a \
             dry run of rocketmq_upsert_topic on the control MCP server; a Broker permission is part of the Broker \
             configuration.",
            "high",
            "Consumers cannot fetch from a queue that denies reads, so its messages wait.",
            "Read permission is sometimes removed on purpose while a Broker is taken out of service.",
            "rocketmq_get_topic_route reports a readable permission on every Broker.",
        ),
    ))
}

fn no_online_consumer(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let group = input.group?;
    group.no_consumer_online().then(|| {
        finding(
            "no_online_consumer",
            Severity::Critical,
            "consume",
            &[CONSUMER_GROUP],
            "No consumer of the group is online",
            format!(
                "All {} Brokers that hold the configuration of the group report no connected consumer.",
                group.configured_broker_count
            ),
            recommend(
                "Start the consumers of the group, or find out why they cannot connect.",
                "high",
                "Nothing consumes for this group, so no message of the topic is delivered to it.",
                "Consumers that start on a large backlog fetch at full speed; watch Broker latency.",
                "rocketmq_list_consumer_connections lists the consumers.",
            ),
        )
    })
}

fn consumption_disabled(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let group = input.group?;
    (group.consume_disabled_broker_count > 0).then(|| {
        finding(
            "consumption_disabled",
            if group.consumption_disabled_everywhere() {
                Severity::Critical
            } else {
                Severity::High
            },
            "consume",
            &[CONSUMER_GROUP],
            "Consumption is disabled for the group",
            format!(
                "consume_enable is false on {} of {} Brokers that hold the configuration of the group, among them {}.",
                group.consume_disabled_broker_count,
                group.configured_broker_count,
                group.consume_disabled_brokers.join(", ")
            ),
            recommend(
                "Find out why consumption was disabled. To enable it again, plan the change with a dry run of \
                 rocketmq_upsert_consumer_group on the control MCP server.",
                "high",
                "A Broker refuses to serve a group whose consumption is disabled.",
                "Consumption may have been disabled on purpose, for example to stop a faulty consumer.",
                "rocketmq_get_consumer_group_details reports consume_enable=true.",
            ),
        )
    })
}

fn group_without_progress(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let lag = input.lag?;
    (lag.queues.queue_count == 0).then(|| {
        finding(
            "group_without_progress",
            Severity::High,
            "consume",
            &[CONSUMER_LAG],
            "The group has no consume progress on the topic",
            format!(
                "No Broker holds an offset of group {} for topic {}: the group is not subscribed to the topic, or \
                 it has not consumed from it yet.",
                input.target.consumer_group, input.target.topic
            ),
            recommend(
                "Check the subscription of the consumers: the topic name, and that they run the code that subscribes \
                 to it.",
                "high",
                "A group that never consumed a topic receives nothing from it until a consumer subscribes.",
                "A new group starts at the newest or the oldest message, depending on how its consumers are set up.",
                "rocketmq_get_consumer_lag lists queues for the group and the topic.",
            ),
        )
    })
}

fn consumer_backlog(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let lag = input.lag?;
    let severity = severity::grade(lag.figures(), input.thresholds, input.consumption_stopped());
    if matches!(severity, Severity::Healthy | Severity::Unknown) {
        return None;
    }
    let delay = lag
        .delay
        .consume_delay_ms
        .map(|delay| format!(", and the group is {} behind production", duration_text(delay)))
        .unwrap_or_default();
    Some(finding(
        "consumer_backlog",
        severity,
        "consume",
        &[CONSUMER_LAG],
        "The group is behind on the topic",
        format!(
            "total_lag={} is above the threshold of {}{delay}.",
            lag.total_lag, input.thresholds.lag
        ),
        recommend(
            "Run rocketmq_diagnose_consumer_lag for this topic and group; it explains the backlog.",
            "medium",
            "A message waits behind the backlog before it is delivered.",
            "None. The diagnosis is read-only.",
            "The lag diagnosis reports a backlog within the threshold.",
        ),
    ))
}

fn topic_has_no_messages(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let activity = input.activity?;
    (activity.exists && activity.message_count == 0).then(|| {
        finding(
            "topic_has_no_messages",
            Severity::Low,
            "produce",
            &[TOPIC_STATS],
            "The topic holds no messages",
            format!(
                "No queue of topic {} holds a message: nothing was produced to it, or everything expired.",
                input.target.topic
            ),
            recommend(
                "Check that the producers send to this topic name and that their sends succeed.",
                "low",
                "There is nothing to deliver until a message is stored.",
                "None. Checking is read-only.",
                "rocketmq_get_topic_stats reports a message count above zero.",
            ),
        )
    })
}

fn message_not_found(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let failure = input.message?.as_ref().err()?;
    (failure.not_found_entity() == Some(NotFoundEntity::Message)).then(|| {
        finding(
            "message_not_found",
            Severity::Medium,
            "message",
            &[MESSAGE_METADATA],
            "The message was not found",
            "No Broker of the cluster holds a message with the given identifier.".to_string(),
            recommend(
                "Verify the message identifier and the cluster. A message is removed once its retention time passed.",
                "medium",
                "A message that is not stored cannot be delivered.",
                "None. Checking is read-only.",
                "rocketmq_get_message_metadata finds the message.",
            ),
        )
    })
}

/// How many consumption attempts the Broker counts for a message, as a sentence that follows another.
fn failed_attempts(message: &MessageObservation) -> String {
    match message.reconsume_times {
        ..=0 => String::new(),
        1 => " The Broker counts 1 failed consumption attempt for it.".to_string(),
        attempts => format!(" The Broker counts {attempts} failed consumption attempts for it."),
    }
}

fn message_dead_lettered(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let dead_letter_topic = input.target.dead_letter_topic();
    let message = input.message_on(&dead_letter_topic)?;
    Some(finding(
        "message_dead_lettered",
        Severity::High,
        "message",
        &[MESSAGE_METADATA],
        "The group gave up on the message",
        format!(
            "The message is on {dead_letter_topic}, the dead-letter topic of the group: its consumption failed until \
             the retries were exhausted, and it is not delivered again.{}",
            failed_attempts(message)
        ),
        recommend(
            "Find the consumption error for this message in the consumer logs, and fix its cause before the message \
             is sent again.",
            "high",
            "A dead-lettered message stays on the dead-letter topic until someone handles it.",
            "A message that is sent again before its cause is fixed is dead-lettered again.",
            "rocketmq_diagnose_consumer_group reports no growing dead-letter topic.",
        ),
    ))
}

fn message_waiting_for_retry(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let retry_topic = input.target.retry_topic();
    let message = input.message_on(&retry_topic)?;
    Some(finding(
        "message_waiting_for_retry",
        Severity::Medium,
        "message",
        &[MESSAGE_METADATA],
        "The message is retried after a failed consumption",
        format!(
            "The message is on {retry_topic}, the retry topic of the group: a consumer of the group failed to \
             consume it or did not answer in time.{}",
            failed_attempts(message)
        ),
        recommend(
            "Find the consumption error for this message in the consumer logs.",
            "medium",
            "A message is retried with growing delays and dead-lettered once the retries are exhausted.",
            "None. Reading logs is read-only.",
            "rocketmq_diagnose_consumer_group reports no growing retry topic.",
        ),
    ))
}

fn message_on_other_topic(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let message = input.message?.as_ref().ok()?;
    let related = [
        input.target.topic.clone(),
        input.target.retry_topic(),
        input.target.dead_letter_topic(),
    ];
    (!related.contains(&message.topic)).then(|| {
        finding(
            "message_on_other_topic",
            Severity::Low,
            "message",
            &[MESSAGE_METADATA],
            "The message belongs to another topic",
            format!(
                "The message is stored on topic {}, not on topic {}.",
                message.topic, input.target.topic
            ),
            recommend(
                "Run the diagnosis again with the topic the message is stored on.",
                "low",
                "The offsets of the group on this topic say nothing about a message of another topic. A delayed \
                 message stays on a system topic until it is due.",
                "None. The diagnosis is read-only.",
                "The diagnosis of that topic states where the message stands.",
            ),
        )
    })
}

fn message_waiting(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let message = input.message_at(MessagePosition::Waiting)?;
    let overdue = message
        .stored_age_ms
        .is_some_and(|age| age >= input.thresholds.consume_delay_ms);
    let stored = message
        .stored_age_ms
        .map(|age| format!(" It was stored {} ago.", duration_text(age)))
        .unwrap_or_default();
    Some(finding(
        "message_waiting",
        if overdue { Severity::Medium } else { Severity::Low },
        "message",
        &[MESSAGE_METADATA, CONSUMER_LAG],
        "The group has not consumed the message yet",
        format!(
            "The message is at offset {} of queue {}, and the offset of the group on that queue has not passed \
             it.{stored}",
            message.queue_offset, message.queue_id
        ),
        recommend(
            "Run rocketmq_diagnose_consumer_lag for this topic and group to see why the group has not reached the \
             message.",
            "medium",
            "The message is delivered once the group reaches its offset.",
            "None. The diagnosis is read-only.",
            "The next diagnosis reports that the group consumed past the message.",
        ),
    ))
}

fn message_delivered(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let message = input.message_at(MessagePosition::Consumed)?;
    Some(finding(
        "message_delivered",
        Severity::Healthy,
        "message",
        &[MESSAGE_METADATA, CONSUMER_LAG],
        "The group consumed past the message",
        format!(
            "The message is at offset {} of queue {}, and the offset of the group on that queue is past it: the \
             message was delivered to a consumer of the group, or the subscription of the group filtered it out.",
            message.queue_offset, message.queue_id
        ),
        recommend(
            "If the application did not process the message, check the tag or SQL filter of its subscription and \
             look for the message in the consumer logs.",
            "low",
            "The cluster did its part; what happened to the message next is in the consumer.",
            "None. Checking is read-only.",
            "The consumer logs show the message, or the filter explains why it was skipped.",
        ),
    ))
}

fn message_position_undetermined(input: &RuleInput<'_>) -> Option<HealthFinding> {
    let message = input.message_at(MessagePosition::Undetermined)?;
    Some(finding(
        "message_position_undetermined",
        Severity::Unknown,
        "message",
        &[MESSAGE_METADATA, CONSUMER_LAG],
        "Where the message stands could not be determined",
        format!(
            "The message is at offset {offset} of queue {queue}, and the offsets of the group do not settle whether \
             it passed that offset: they were not read from every Broker, no queue {queue} of the group reaches the \
             offset, or Brokers that host a queue {queue} disagree.",
            offset = message.queue_offset,
            queue = message.queue_id
        ),
        recommend(
            "Run the diagnosis again. If the position stays undetermined, compare the offset of the message with \
             the offsets rocketmq_get_consumer_lag lists for the queue on each Broker.",
            "low",
            "A message names its queue by number, and every Broker that hosts the topic has a queue with that number.",
            "None. Both are read-only.",
            "The diagnosis reports that the group consumed past the message, or that the message waits.",
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::diagnosis::EvidenceStatus;
    use crate::service::diagnosis_collector::fixtures as lag;
    use crate::service::diagnosis_collector::fixtures::queue;
    use crate::tools::executor::ToolRejection;

    const ALIAS: &str = "message-00000000000000000000000000000001";

    fn target(message_alias: Option<&str>) -> DeliveryTarget {
        DeliveryTarget {
            cluster: "local-dev".to_string(),
            topic: "orders".to_string(),
            consumer_group: "order-service".to_string(),
            message_alias: message_alias.map(str::to_string),
        }
    }

    /// Complete evidence of an open path: writable and readable, consumers online, a small lag.
    fn evidence() -> MessageDeliveryEvidence {
        MessageDeliveryEvidence {
            route: Ok(lag::route(&[("broker-a", 6), ("broker-b", 6)])),
            group: Ok(lag::group(&[("broker-a", true), ("broker-b", true)])),
            lag: Ok(lag::lag_summary(
                vec![queue("broker-a", 0, 10), queue("broker-b", 0, 20)],
                50.0,
            )),
            activity: Ok(lag::system_topic("orders", 500, 5)),
            message: None,
        }
    }

    /// A queue of `broker_name` that ends at `broker_offset`, which the group read to `consumer_offset`.
    fn offsets(broker_name: &str, queue_id: i32, broker_offset: i64, consumer_offset: i64) -> QueueLag {
        QueueLag {
            broker_offset,
            consumer_offset,
            lag: broker_offset - consumer_offset,
            ..queue(broker_name, queue_id, 0)
        }
    }

    /// The metadata of a message at offset 42 of queue 0, stored a minute ago.
    fn metadata(topic: &str) -> MessageMetadataOutput {
        MessageMetadataOutput {
            cluster: "local-dev".to_string(),
            message_alias: ALIAS.to_string(),
            unique_message_alias: None,
            topic: topic.to_string(),
            born_at: lag::seconds_ago(61),
            stored_at: lag::seconds_ago(60),
            queue_id: 0,
            queue_offset: 42,
            store_size: 128,
            reconsume_times: 0,
            sys_flag: 0,
            flag: 0,
            prepared_transaction_offset: 0,
        }
    }

    /// A message of the diagnosed Topic at `position`.
    fn message(position: MessagePosition) -> MessageObservation {
        MessageObservation {
            position: Some(position),
            ..MessageObservation::observe(metadata("orders"), "orders", None, lag::now_millis())
        }
    }

    fn diagnose(evidence: MessageDeliveryEvidence) -> DiagnosisReport {
        let alias = evidence.message.as_ref().map(|_| ALIAS);
        evaluate(&target(alias), evidence, &ConsumerLagPolicy::default())
    }

    fn rule_ids(report: &DiagnosisReport) -> Vec<&str> {
        report.root_causes.iter().map(|cause| cause.rule_id.as_str()).collect()
    }

    #[test]
    fn an_open_path_is_healthy() {
        let report = diagnose(evidence());

        assert_eq!(report.report_type, "message_delivery");
        assert_eq!(report.rules_version, MESSAGE_DELIVERY_RULES_VERSION);
        assert_eq!(report.evidence_version, MESSAGE_DELIVERY_EVIDENCE_VERSION);
        assert_eq!(report.severity, Severity::Healthy);
        assert!(!report.partial);
        assert_eq!(report.confidence, 0.95);
        assert!(report.root_causes.is_empty());
        assert_eq!(
            report.summary,
            "Delivery from topic orders to group order-service is Healthy: the topic can be written and read on \
             every Broker that hosts it, consumers of the group are online, the lag of the group is within the \
             threshold."
        );
        assert_eq!(
            report.evidence_refs,
            ["topic_route", "consumer_group", "consumer_lag", "topic_stats"]
        );
        assert_eq!(report.target["message_alias"], Value::Null);
        assert_eq!(report.target["lag_threshold"], 1_000);
        assert_eq!(report.target["consume_delay_threshold_ms"], 300_000);
    }

    #[test]
    fn write_permission_missing_is_critical_when_no_broker_accepts_writes() {
        let mut evidence = evidence();
        evidence.route = Ok(lag::route(&[("broker-a", 4), ("broker-b", 6)]));
        let some = diagnose(evidence);
        assert_eq!(rule_ids(&some), ["write_permission_missing"]);
        assert_eq!(some.severity, Severity::High);
        assert_eq!(
            some.root_causes[0].cause,
            "Not every Broker that hosts the topic accepts writes"
        );
        assert_eq!(
            some.root_causes[0].reasoning,
            "Writes are denied on 1 of 2 Brokers that host the topic: broker-a."
        );

        let mut evidence = self::evidence();
        evidence.route = Ok(lag::route(&[("broker-a", 4), ("broker-b", 4)]));
        let all = diagnose(evidence);
        assert_eq!(all.severity, Severity::Critical);
        assert_eq!(
            all.root_causes[0].cause,
            "No Broker that hosts the topic accepts writes"
        );
        assert_eq!(all.impacts[0].area, "produce");
    }

    #[test]
    fn read_permission_missing_is_critical_when_no_broker_serves_reads() {
        let mut evidence = evidence();
        evidence.route = Ok(lag::route(&[("broker-a", 2), ("broker-b", 2)]));

        let report = diagnose(evidence);

        assert_eq!(rule_ids(&report), ["read_permission_missing"]);
        assert_eq!(report.severity, Severity::Critical);
        assert_eq!(report.impacts[0].area, "consume");
        assert_eq!(
            report.summary,
            "Delivery from topic orders to group order-service is Critical: No Broker that hosts the topic serves \
             reads."
        );
    }

    #[test]
    fn no_online_consumer_and_consumption_disabled_stop_the_delivery() {
        let mut offline = evidence();
        offline.group = Ok(lag::group(&[("broker-a", false), ("broker-b", false)]));
        let offline = diagnose(offline);
        assert_eq!(rule_ids(&offline), ["no_online_consumer"]);
        assert_eq!(offline.severity, Severity::Critical);

        let mut disabled = evidence();
        disabled.group = Ok(lag::group_with(&[("broker-a", true), ("broker-b", true)], |row| {
            row.consume_enable = Some(row.broker_name != "broker-b");
        }));
        let disabled = diagnose(disabled);
        assert_eq!(rule_ids(&disabled), ["consumption_disabled"]);
        assert_eq!(disabled.severity, Severity::High, "broker-a still serves the group");
        assert_eq!(
            disabled.root_causes[0].reasoning,
            "consume_enable is false on 1 of 2 Brokers that hold the configuration of the group, among them \
             broker-b."
        );
    }

    #[test]
    fn group_without_progress_fires_when_no_broker_holds_an_offset_of_the_group() {
        let mut evidence = evidence();
        evidence.lag = Ok(lag::lag_summary(Vec::new(), 0.0));

        let report = diagnose(evidence);

        assert_eq!(rule_ids(&report), ["group_without_progress"]);
        assert_eq!(report.severity, Severity::High);
        assert!(report.root_causes[0]
            .reasoning
            .starts_with("No Broker holds an offset of group order-service for topic orders"));
    }

    #[test]
    fn consumer_backlog_carries_the_grade_of_the_lag_diagnosis() {
        let mut evidence = evidence();
        evidence.lag = Ok(lag::lag_summary(vec![queue("broker-a", 0, 12_000)], 50.0));
        let report = diagnose(evidence);
        assert_eq!(rule_ids(&report), ["consumer_backlog"]);
        assert_eq!(report.severity, Severity::High);
        assert_eq!(
            report.root_causes[0].reasoning,
            "total_lag=12000 is above the threshold of 1000."
        );
        assert!(report.recommendations[0]
            .action
            .starts_with("Run rocketmq_diagnose_consumer_lag"));

        // A large backlog that is seconds of traffic is the low grade it is in the lag diagnosis.
        let mut evidence = self::evidence();
        evidence.lag = Ok(lag::lag_summary(
            vec![lag::queue_behind("broker-a", 0, 12_000, 30)],
            400.0,
        ));
        let report = diagnose(evidence);
        assert_eq!(report.severity, Severity::Low);
        assert_eq!(
            report.root_causes[0].reasoning,
            "total_lag=12000 is above the threshold of 1000, and the group is 30 s behind production."
        );
    }

    #[test]
    fn topic_has_no_messages_fires_for_an_empty_topic() {
        let mut evidence = evidence();
        evidence.activity = Ok(lag::system_topic("orders", 0, 5));

        let report = diagnose(evidence);

        assert_eq!(rule_ids(&report), ["topic_has_no_messages"]);
        assert_eq!(report.severity, Severity::Low);
    }

    #[test]
    fn a_message_position_is_stated_when_the_queues_that_can_hold_the_message_agree() {
        let position = |queues: &[QueueLag]| MessagePosition::of(0, 42, queues);

        assert_eq!(position(&[offsets("broker-a", 0, 100, 43)]), MessagePosition::Consumed);
        // The offset of a group is the next one it reads, so it has not read the message at it.
        assert_eq!(position(&[offsets("broker-a", 0, 100, 42)]), MessagePosition::Waiting);
        // The queue of broker-b ends before the message, so the message is not on broker-b.
        assert_eq!(
            position(&[offsets("broker-a", 0, 100, 90), offsets("broker-b", 0, 42, 42)]),
            MessagePosition::Consumed
        );
        assert_eq!(
            position(&[offsets("broker-a", 0, 100, 90), offsets("broker-b", 0, 100, 90)]),
            MessagePosition::Consumed
        );
        // Both Brokers can hold the message, and the group passed its offset on one of them only.
        assert_eq!(
            position(&[offsets("broker-a", 0, 100, 90), offsets("broker-b", 0, 100, 7)]),
            MessagePosition::Undetermined
        );
        // No queue 0 reaches the offset of the message, or the group reports none at all.
        assert_eq!(
            position(&[offsets("broker-a", 0, 42, 42)]),
            MessagePosition::Undetermined
        );
        assert_eq!(
            position(&[offsets("broker-a", 3, 100, 90)]),
            MessagePosition::Undetermined
        );
        assert_eq!(position(&[]), MessagePosition::Undetermined);
    }

    #[test]
    fn a_message_is_observed_against_the_offsets_of_the_group_on_the_diagnosed_topic() {
        let queues = [offsets("broker-a", 0, 100, 90)];
        let now = lag::now_millis();

        let consumed = MessageObservation::observe(metadata("orders"), "orders", Some(&queues), now);
        assert_eq!(consumed.position, Some(MessagePosition::Consumed));
        assert_eq!(consumed.stored_age_ms, Some(60_000));
        assert_eq!(consumed.message_alias, ALIAS);

        // Offsets that were not read from every Broker settle nothing.
        let incomplete = MessageObservation::observe(metadata("orders"), "orders", None, now);
        assert_eq!(incomplete.position, Some(MessagePosition::Undetermined));

        // The offsets of the group on the diagnosed Topic say nothing about another Topic.
        let elsewhere = MessageObservation::observe(metadata("payments"), "orders", Some(&queues), now);
        assert_eq!(elsewhere.position, None);

        let never_stored = MessageMetadataOutput {
            stored_at: crate::model::contract::observed_at_from_millis(0),
            ..metadata("orders")
        };
        assert_eq!(
            MessageObservation::observe(never_stored, "orders", Some(&queues), now).stored_age_ms,
            None
        );
    }

    #[test]
    fn message_delivered_is_a_healthy_conclusion() {
        let mut evidence = evidence();
        evidence.message = Some(Ok(message(MessagePosition::Consumed)));

        let report = diagnose(evidence);

        assert_eq!(rule_ids(&report), ["message_delivered"]);
        assert_eq!(report.severity, Severity::Healthy);
        assert_eq!(
            report.root_causes[0].evidence_refs,
            ["message_metadata", "consumer_lag"]
        );
        assert!(report.root_causes[0]
            .reasoning
            .ends_with("or the subscription of the group filtered it out."));
        assert_eq!(
            report.summary,
            "Delivery from topic orders to group order-service is Healthy: The group consumed past the message."
        );
        assert!(report.impacts.is_empty(), "a delivered message is no impact");
        assert_eq!(report.target["message_alias"], ALIAS);
        assert_eq!(report.confidence, 0.95);
    }

    #[test]
    fn message_waiting_is_low_until_the_message_is_older_than_the_delay_threshold() {
        let waiting = |stored_age_ms: u64| {
            let mut evidence = evidence();
            evidence.message = Some(Ok(MessageObservation {
                stored_age_ms: Some(stored_age_ms),
                ..message(MessagePosition::Waiting)
            }));
            diagnose(evidence)
        };

        let fresh = waiting(60_000);
        assert_eq!(rule_ids(&fresh), ["message_waiting"]);
        assert_eq!(fresh.severity, Severity::Low);
        assert_eq!(
            fresh.root_causes[0].reasoning,
            "The message is at offset 42 of queue 0, and the offset of the group on that queue has not passed it. \
             It was stored 60 s ago."
        );

        assert_eq!(waiting(300_000).severity, Severity::Medium);
    }

    #[test]
    fn message_position_undetermined_leaves_the_question_about_the_message_open() {
        let mut evidence = evidence();
        evidence.message = Some(Ok(message(MessagePosition::Undetermined)));

        let report = diagnose(evidence);

        assert_eq!(rule_ids(&report), ["message_position_undetermined"]);
        assert_eq!(report.severity, Severity::Unknown);
        assert_eq!(
            report.summary,
            "Delivery from topic orders to group order-service is Unknown: Where the message stands could not be \
             determined."
        );
        assert!(report.impacts.is_empty());

        // A problem on the path outranks the open question.
        let mut evidence = self::evidence();
        evidence.group = Ok(lag::group(&[("broker-a", false), ("broker-b", false)]));
        evidence.message = Some(Ok(message(MessagePosition::Undetermined)));
        let report = diagnose(evidence);
        assert_eq!(
            rule_ids(&report),
            ["no_online_consumer", "message_position_undetermined"]
        );
        assert_eq!(report.severity, Severity::Critical);
    }

    #[test]
    fn a_message_on_the_retry_or_the_dead_letter_topic_of_the_group_is_named_as_such() {
        let stored_on = |topic: &str, reconsume_times: i32| {
            let mut evidence = evidence();
            evidence.message = Some(Ok(MessageObservation {
                reconsume_times,
                ..MessageObservation::observe(metadata(topic), "orders", None, lag::now_millis())
            }));
            diagnose(evidence)
        };

        let retried = stored_on("%RETRY%order-service", 3);
        assert_eq!(rule_ids(&retried), ["message_waiting_for_retry"]);
        assert_eq!(retried.severity, Severity::Medium);
        assert_eq!(
            retried.root_causes[0].reasoning,
            "The message is on %RETRY%order-service, the retry topic of the group: a consumer of the group failed \
             to consume it or did not answer in time. The Broker counts 3 failed consumption attempts for it."
        );

        let dead = stored_on("%DLQ%order-service", 16);
        assert_eq!(rule_ids(&dead), ["message_dead_lettered"]);
        assert_eq!(dead.severity, Severity::High);
        assert!(dead.root_causes[0]
            .reasoning
            .ends_with("The Broker counts 16 failed consumption attempts for it."));

        // The retry topic of another group is another topic.
        let other_group = stored_on("%RETRY%billing", 1);
        assert_eq!(rule_ids(&other_group), ["message_on_other_topic"]);
    }

    #[test]
    fn message_on_other_topic_fires_for_a_message_of_another_topic() {
        let mut evidence = evidence();
        evidence.message = Some(Ok(MessageObservation::observe(
            metadata("payments"),
            "orders",
            None,
            lag::now_millis(),
        )));

        let report = diagnose(evidence);

        assert_eq!(rule_ids(&report), ["message_on_other_topic"]);
        assert_eq!(report.severity, Severity::Low);
        assert_eq!(
            report.root_causes[0].reasoning,
            "The message is stored on topic payments, not on topic orders."
        );
    }

    #[test]
    fn message_not_found_fires_for_a_lookup_that_found_nothing() {
        let mut evidence = evidence();
        evidence.message = Some(Err(ToolFailure::not_found(NotFoundEntity::Message)));

        let report = diagnose(evidence);

        assert_eq!(rule_ids(&report), ["message_not_found"]);
        assert_eq!(report.severity, Severity::Medium);
        assert!(report.partial);
        assert_eq!(report.missing_evidence, ["message_metadata"]);

        // A lookup that failed says nothing about the message: the path is judged without it.
        let mut evidence = self::evidence();
        evidence.message = Some(lag::unavailable());
        let report = diagnose(evidence);
        assert!(report.root_causes.is_empty());
        assert_eq!(report.severity, Severity::Healthy);
        assert!(report.partial);
        assert_eq!(report.confidence, 0.9);
    }

    #[test]
    fn a_report_never_holds_the_identifier_of_the_message() {
        let mut evidence = evidence();
        evidence.message = Some(Ok(message(MessagePosition::Waiting)));

        let report = serde_json::to_value(diagnose(evidence)).unwrap();

        let item = report["evidence_snapshot"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["id"] == "message_metadata")
            .unwrap();
        let data = &item["payload"]["data"];
        assert_eq!(data["message_alias"], ALIAS);
        assert_eq!(data["position"], "waiting");
        let mut fields = data.as_object().unwrap().keys().map(String::as_str).collect::<Vec<_>>();
        fields.sort_unstable();
        assert_eq!(
            fields,
            [
                "born_at",
                "generated_at",
                "message_alias",
                "position",
                "queue_id",
                "queue_offset",
                "reconsume_times",
                "stored_age_ms",
                "stored_at",
                "topic",
                "unique_message_alias",
            ]
        );
    }

    #[test]
    fn a_failed_evidence_source_silences_its_rules_and_leaves_a_partial_report() {
        let mut evidence = evidence();
        evidence.lag = Err(ToolFailure::Rejected(ToolRejection::TimedOut { timeout_ms: 5_000 }));
        evidence.activity = lag::unavailable();

        let report = diagnose(evidence);

        assert!(report.partial);
        assert_eq!(report.missing_evidence, ["consumer_lag", "topic_stats"]);
        assert_eq!(report.severity, Severity::Healthy);
        assert_eq!(report.confidence, 0.7);
        assert_eq!(
            report.summary,
            "Delivery from topic orders to group order-service is Healthy: the topic can be written and read on \
             every Broker that hosts it, consumers of the group are online."
        );
        let status = |id: &str| {
            report
                .evidence_snapshot
                .items
                .iter()
                .find(|item| item.id == id)
                .unwrap()
                .status
        };
        assert_eq!(status("consumer_lag"), EvidenceStatus::Timeout);
        assert_eq!(status("consumer_group"), EvidenceStatus::Present);
    }

    #[test]
    fn a_path_whose_consuming_half_was_not_observed_is_unknown() {
        let mut evidence = evidence();
        evidence.group = lag::unavailable();
        evidence.lag = lag::unavailable();

        let report = diagnose(evidence);

        assert_eq!(report.severity, Severity::Unknown);
        assert_eq!(
            report.summary,
            "Delivery from topic orders to group order-service is Unknown because Consumer Group evidence is \
             missing."
        );
        assert_eq!(report.recommendations[0].action, "Collect the evidence again.");

        // What was found on the half that was observed stands.
        let mut evidence = self::evidence();
        evidence.route = Ok(lag::route(&[("broker-a", 4), ("broker-b", 4)]));
        evidence.group = lag::unavailable();
        evidence.lag = lag::unavailable();
        let report = diagnose(evidence);
        assert_eq!(rule_ids(&report), ["write_permission_missing"]);
        assert_eq!(report.severity, Severity::Critical);
    }

    #[test]
    fn a_path_without_the_route_is_unknown_unless_a_rule_found_a_problem() {
        let mut evidence = evidence();
        evidence.route = lag::unavailable();
        let report = diagnose(evidence);
        assert_eq!(report.severity, Severity::Unknown);
        assert!(report.root_causes.is_empty());
        assert_eq!(report.confidence, 0.5);
        assert_eq!(
            report.summary,
            "Delivery from topic orders to group order-service is Unknown because Topic route evidence is missing."
        );

        let mut evidence = self::evidence();
        evidence.route = lag::unavailable();
        evidence.group = Ok(lag::group(&[("broker-a", false), ("broker-b", false)]));
        let report = diagnose(evidence);
        assert_eq!(rule_ids(&report), ["no_online_consumer"]);
        assert_eq!(report.severity, Severity::Critical);
        assert!(report.partial);
    }

    #[test]
    fn a_topic_or_a_group_that_does_not_exist_leads_the_report() {
        let mut evidence = evidence();
        evidence.route = Err(ToolFailure::not_found(NotFoundEntity::Topic));
        evidence.lag = Err(ToolFailure::not_found(NotFoundEntity::Topic));
        evidence.activity = Err(ToolFailure::not_found(NotFoundEntity::Topic));
        let topic = diagnose(evidence);
        assert_eq!(topic.severity, Severity::Unknown);
        assert_eq!(rule_ids(&topic), ["target_not_found"]);
        assert_eq!(
            topic.summary,
            "Delivery from topic orders to group order-service is Unknown because the topic does not exist in the \
             selected cluster."
        );
        assert_eq!(topic.root_causes[0].evidence_refs, ["topic_route"]);
        assert_eq!(topic.recommendations.len(), 1);
        assert!(topic.recommendations[0].action.contains("rocketmq_list_topics"));
        assert_eq!(topic.confidence, 0.9);

        // Neither exists, and the message the caller asked about is on another topic.
        let mut evidence = self::evidence();
        evidence.route = Err(ToolFailure::not_found(NotFoundEntity::Topic));
        evidence.group = Err(ToolFailure::not_found(NotFoundEntity::ConsumerGroup));
        evidence.lag = Err(ToolFailure::not_found(NotFoundEntity::Topic));
        evidence.activity = Err(ToolFailure::not_found(NotFoundEntity::Topic));
        evidence.message = Some(Ok(MessageObservation::observe(
            metadata("payments"),
            "orders",
            None,
            lag::now_millis(),
        )));
        let both = diagnose(evidence);
        assert_eq!(
            rule_ids(&both),
            ["target_not_found", "target_not_found", "message_on_other_topic"]
        );
        assert_eq!(both.root_causes[1].evidence_refs, ["consumer_group"]);
        assert!(both.summary.contains("the topic does not exist"));
        assert_eq!(both.recommendations.len(), 3);
        assert!(both.recommendations[1].action.contains("rocketmq_list_consumer_groups"));
    }

    #[test]
    fn more_brokers_than_a_reasoning_lists_are_counted() {
        let brokers = (0..12).map(|index| format!("broker-{index:02}")).collect::<Vec<_>>();
        let rows = brokers.iter().map(|name| (name.as_str(), 4)).collect::<Vec<_>>();
        let mut evidence = evidence();
        evidence.route = Ok(lag::route(&rows));

        let report = diagnose(evidence);

        assert!(report.root_causes[0]
            .reasoning
            .ends_with("broker-08, broker-09 and 2 more."));
    }
}
