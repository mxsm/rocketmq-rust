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

//! The rules of a consumer lag diagnosis.
//!
//! A rule reads evidence and either draws one conclusion or stays silent. It has a stable
//! identifier, which a report carries as `root_causes[].rule_id`, and it names the evidence it
//! cannot be evaluated without, so that missing evidence silences the rule instead of turning into
//! a conclusion. Rules never read the clock: every duration they compare was measured when the
//! evidence was collected.

use std::collections::BTreeSet;

use super::Thresholds;
use crate::model::diagnosis::Recommendation;
use crate::service::diagnosis_collector::evidence_id::CONSUMER_GROUP;
use crate::service::diagnosis_collector::evidence_id::CONSUMER_LAG;
use crate::service::diagnosis_collector::evidence_id::DEAD_LETTER_TOPIC_STATS;
use crate::service::diagnosis_collector::evidence_id::RETRY_TOPIC_STATS;
use crate::service::diagnosis_collector::evidence_id::TOPIC_CONFIG;
use crate::service::diagnosis_collector::evidence_id::TOPIC_ROUTE;
use crate::service::diagnosis_collector::ConsumerLagEvidence;
use crate::service::diagnosis_collector::LagFigures;
use crate::service::diagnosis_collector::TopicRouteSummary;
use crate::service::diagnosis_evidence::duration_text;
use crate::service::diagnosis_evidence::permits_read;
use crate::service::diagnosis_evidence::rate_text;
use crate::service::diagnosis_evidence::ConsumerGroupState;
use crate::service::diagnosis_evidence::TopicActivity;
use crate::service::diagnosis_evidence::TopicConfigSummary;
use crate::service::diagnosis_evidence::LISTED_BROKER_LIMIT;

/// Share of the backlog from which one Topic, one Broker or one queue counts as holding it.
pub(crate) const CONCENTRATION_RATIO: f64 = 0.70;

/// Consumption rate, in messages per second, below which a group with a backlog counts as slow.
const LOW_CONSUME_TPS: f64 = 1.0;

/// When a rule is evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Scope {
    /// With or without a backlog: the condition matters before messages pile up.
    Always,
    /// Only while the backlog is above the lag threshold: the rule explains a backlog.
    Backlog,
}

/// One row of the rule table.
pub(crate) struct Rule {
    /// Part of the report contract: the identifier never changes once released.
    pub id: &'static str,
    pub scope: Scope,
    /// Evidence, besides consumer lag, that the rule cannot be evaluated without.
    pub requires: &'static [&'static str],
    pub evaluate: fn(&RuleInput<'_>) -> Option<Finding>,
}

/// The evidence the rules read. A part that could not be collected is `None`.
pub(crate) struct RuleInput<'a> {
    pub lag: LagFigures<'a>,
    pub route: Option<&'a TopicRouteSummary>,
    pub group: Option<&'a ConsumerGroupState>,
    pub topic_config: Option<&'a TopicConfigSummary>,
    pub retry: Option<&'a TopicActivity>,
    pub dead_letter: Option<&'a TopicActivity>,
    pub thresholds: &'a Thresholds,
}

impl<'a> RuleInput<'a> {
    /// The rule input of `evidence`, or `None` without consumer lag: no rule runs without it.
    pub(crate) fn new(evidence: &'a ConsumerLagEvidence, thresholds: &'a Thresholds) -> Option<Self> {
        Some(Self {
            lag: evidence.lag.as_ref().ok()?.figures(),
            route: evidence.route.as_ref().ok(),
            group: evidence.group.as_ref().ok(),
            topic_config: evidence.topic_config.as_ref().ok(),
            retry: evidence.retry.as_ref().ok(),
            dead_letter: evidence.dead_letter.as_ref().ok(),
            thresholds,
        })
    }

    /// Whether the backlog is above the lag threshold.
    pub(crate) fn has_backlog(&self) -> bool {
        self.lag.total_lag > self.thresholds.lag
    }

    fn has(&self, evidence: &str) -> bool {
        match evidence {
            CONSUMER_LAG => true,
            TOPIC_ROUTE => self.route.is_some(),
            CONSUMER_GROUP => self.group.is_some(),
            TOPIC_CONFIG => self.topic_config.is_some(),
            RETRY_TOPIC_STATS => self.retry.is_some(),
            DEAD_LETTER_TOPIC_STATS => self.dead_letter.is_some(),
            _ => false,
        }
    }

    /// Brokers that host the Topic and deny reads on it, by the route or the Topic configuration.
    ///
    /// A Broker registers a Topic with the Topic permission masked by its own, so the route shows
    /// the permission that is in effect; the Topic configuration shows where it comes from.
    fn read_denied_brokers(&self) -> BTreeSet<&'a str> {
        let mut denied = BTreeSet::new();
        if let Some(route) = self.route {
            denied.extend(
                route
                    .queues
                    .iter()
                    .filter(|queue| !permits_read(queue.perm))
                    .map(|queue| queue.broker_name.as_str()),
            );
        }
        if let Some(config) = self.topic_config {
            denied.extend(config.read_denied().iter().map(String::as_str));
        }
        denied
    }

    /// Whether every Broker that hosts the Topic denies reads.
    fn reads_denied_everywhere(&self) -> bool {
        self.route
            .is_some_and(|route| !route.queues.is_empty() && self.read_denied_brokers().len() >= route.queues.len())
    }

    /// Whether the evidence establishes that the group cannot consume at all. Such a group is not
    /// slow, and its backlog is never harmless.
    pub(crate) fn consumption_stopped(&self) -> bool {
        self.group
            .is_some_and(|group| group.no_consumer_online() || group.consumption_disabled_everywhere())
            || self.reads_denied_everywhere()
    }
}

/// What a rule concluded.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Finding {
    pub cause: String,
    pub confidence: f32,
    pub reasoning: String,
    /// The evidence items the conclusion rests on.
    pub evidence_refs: Vec<&'static str>,
    /// Whether the condition stops the group from consuming at all.
    pub stops_consumption: bool,
    pub recommendation: Recommendation,
}

/// Conclusions that are not the result of a rule firing.
pub(crate) mod conclusion {
    pub(crate) const BACKLOG_WITHIN_THRESHOLD: &str = "backlog_within_threshold";
    pub(crate) const CAUSE_NOT_ISOLATED: &str = "cause_not_isolated";
    pub(crate) const NO_QUEUE_ROWS: &str = "no_queue_rows";
    pub(crate) const TARGET_NOT_FOUND: &str = "target_not_found";
}

pub(crate) const RULES: &[Rule] = &[
    Rule {
        id: "no_online_consumer",
        scope: Scope::Always,
        requires: &[CONSUMER_GROUP],
        evaluate: no_online_consumer,
    },
    Rule {
        id: "consumption_disabled",
        scope: Scope::Always,
        requires: &[CONSUMER_GROUP],
        evaluate: consumption_disabled,
    },
    Rule {
        id: "read_permission_missing",
        scope: Scope::Always,
        requires: &[TOPIC_ROUTE],
        evaluate: read_permission_missing,
    },
    Rule {
        id: "consumer_missing_on_broker",
        scope: Scope::Backlog,
        requires: &[CONSUMER_GROUP],
        evaluate: consumer_missing_on_broker,
    },
    Rule {
        id: "consume_throughput_low",
        scope: Scope::Backlog,
        requires: &[],
        evaluate: consume_throughput_low,
    },
    Rule {
        id: "lag_concentrated_on_broker",
        scope: Scope::Backlog,
        requires: &[],
        evaluate: lag_concentrated_on_broker,
    },
    Rule {
        id: "lag_concentrated_on_queue",
        scope: Scope::Backlog,
        requires: &[],
        evaluate: lag_concentrated_on_queue,
    },
    Rule {
        id: "consume_delay",
        scope: Scope::Backlog,
        requires: &[],
        evaluate: consume_delay,
    },
    Rule {
        id: "pop_inflight_high",
        scope: Scope::Backlog,
        requires: &[CONSUMER_GROUP],
        evaluate: pop_inflight_high,
    },
    Rule {
        id: "retry_queue_growing",
        scope: Scope::Backlog,
        requires: &[RETRY_TOPIC_STATS],
        evaluate: retry_queue_growing,
    },
    Rule {
        id: "dead_letter_queue_growing",
        scope: Scope::Backlog,
        requires: &[DEAD_LETTER_TOPIC_STATS],
        evaluate: dead_letter_queue_growing,
    },
];

/// Evaluates every rule that applies and returns what fired, most confident first.
pub(crate) fn evaluate(input: &RuleInput<'_>) -> Vec<(&'static str, Finding)> {
    let backlog = input.has_backlog();
    let mut fired = RULES
        .iter()
        .filter(|rule| backlog || rule.scope == Scope::Always)
        .filter(|rule| rule.requires.iter().all(|evidence| input.has(evidence)))
        .filter_map(|rule| (rule.evaluate)(input).map(|finding| (rule.id, finding)))
        .collect::<Vec<_>>();
    // The sort is stable, so equally confident conclusions keep the order of the table.
    fired.sort_by(|left, right| right.1.confidence.total_cmp(&left.1.confidence));
    fired
}

fn no_online_consumer(input: &RuleInput<'_>) -> Option<Finding> {
    let group = input.group?;
    if !group.no_consumer_online() {
        return None;
    }
    Some(Finding {
        cause: "No consumer of the group is online".to_string(),
        confidence: 0.90,
        reasoning: format!(
            "All {} Brokers that hold the configuration of the group report no connected consumer.",
            group.configured_broker_count
        ),
        evidence_refs: vec![CONSUMER_GROUP],
        stops_consumption: true,
        recommendation: Recommendation {
            action: "Start the consumers of the group, or find out why they cannot connect.".to_string(),
            priority: "high".to_string(),
            rationale: "Nothing consumes for this group, so every message that is produced adds to the backlog."
                .to_string(),
            risk: "Consumers that start on a large backlog fetch at full speed; watch Broker latency.".to_string(),
            verification: "rocketmq_list_consumer_connections lists the consumers and total_lag declines.".to_string(),
        },
    })
}

fn consumption_disabled(input: &RuleInput<'_>) -> Option<Finding> {
    let group = input.group?;
    if group.consume_disabled_broker_count == 0 {
        return None;
    }
    Some(Finding {
        cause: "Consumption is disabled for the group".to_string(),
        confidence: 0.92,
        reasoning: format!(
            "consume_enable is false on {} of {} Brokers that hold the configuration of the group: {}.",
            group.consume_disabled_broker_count,
            group.configured_broker_count,
            names(
                group.consume_disabled_brokers.iter().map(String::as_str),
                group.consume_disabled_broker_count
            )
        ),
        evidence_refs: vec![CONSUMER_GROUP],
        stops_consumption: group.consumption_disabled_everywhere(),
        recommendation: Recommendation {
            action: "Find out why consumption was disabled. To enable it again, plan the change with a dry run of \
                     rocketmq_upsert_consumer_group on the control MCP server."
                .to_string(),
            priority: "high".to_string(),
            rationale: "A Broker refuses to serve a group whose consumption is disabled.".to_string(),
            risk: "Consumption may have been disabled on purpose, for example to stop a faulty consumer.".to_string(),
            verification: "rocketmq_get_consumer_group_details reports consume_enable=true and total_lag declines."
                .to_string(),
        },
    })
}

fn read_permission_missing(input: &RuleInput<'_>) -> Option<Finding> {
    let route = input.route?;
    let denied = input.read_denied_brokers();
    if denied.is_empty() {
        return None;
    }
    let mut reasoning = format!(
        "Reads are denied on {} of {} Brokers that host the Topic: {}.",
        denied.len(),
        route.queues.len(),
        names(denied.iter().copied(), denied.len())
    );
    let mut evidence_refs = vec![TOPIC_ROUTE];
    if let Some(config) = input.topic_config {
        evidence_refs.push(TOPIC_CONFIG);
        let by_topic = denied.iter().filter(|broker| config.denies_read_on(broker)).count();
        let by_broker = denied.iter().filter(|broker| config.allows_read_on(broker)).count();
        if by_topic > 0 {
            reasoning.push_str(&format!(" The Topic permission denies reads on {by_topic} of them."));
        }
        if by_broker > 0 {
            reasoning.push_str(&format!(
                " On {by_broker} of them the Topic permission allows reads, so the Broker permission denies them."
            ));
        }
    }
    Some(Finding {
        cause: "The Topic cannot be read on every Broker that hosts it".to_string(),
        confidence: 0.90,
        reasoning,
        evidence_refs,
        stops_consumption: input.reads_denied_everywhere(),
        recommendation: Recommendation {
            action: "Restore the read permission where it was not removed on purpose. Plan a Topic permission change \
                     with a dry run of rocketmq_upsert_topic on the control MCP server; a Broker permission is part \
                     of the Broker configuration."
                .to_string(),
            priority: "high".to_string(),
            rationale: "Consumers cannot fetch from a queue that denies reads, so its messages wait.".to_string(),
            risk: "Read permission is sometimes removed on purpose while a Broker is taken out of service.".to_string(),
            verification: "rocketmq_get_topic_route reports a readable permission on every Broker and total_lag \
                           declines."
                .to_string(),
        },
    })
}

fn consumer_missing_on_broker(input: &RuleInput<'_>) -> Option<Finding> {
    let group = input.group?;
    // With no consumer anywhere, `no_online_consumer` is the conclusion.
    if group.no_consumer_online() {
        return None;
    }
    let unattended = input
        .lag
        .queues
        .brokers
        .iter()
        .filter(|broker| broker.lag > 0 && group.is_offline_on(&broker.broker_name))
        .collect::<Vec<_>>();
    if unattended.is_empty() {
        return None;
    }
    let lag = unattended
        .iter()
        .fold(0_i64, |total, broker| total.saturating_add(broker.lag));
    Some(Finding {
        cause: "No consumer of the group is connected to a Broker that holds lag".to_string(),
        confidence: 0.85,
        reasoning: format!(
            "{} of total_lag={} sits on {} Brokers that report no connected consumer of the group: {}.",
            lag,
            input.lag.total_lag,
            unattended.len(),
            names(
                unattended.iter().map(|broker| broker.broker_name.as_str()),
                unattended.len()
            )
        ),
        evidence_refs: vec![CONSUMER_LAG, CONSUMER_GROUP],
        stops_consumption: false,
        recommendation: Recommendation {
            action: "Check that the consumers can reach those Brokers and that their route to the Topic includes them."
                .to_string(),
            priority: "high".to_string(),
            rationale: "A consumer fetches only from Brokers it is connected to.".to_string(),
            risk: "A network or address problem between consumers and one Broker often affects producers too."
                .to_string(),
            verification: "rocketmq_list_consumer_connections lists consumers on those Brokers and their lag declines."
                .to_string(),
        },
    })
}

fn consume_throughput_low(input: &RuleInput<'_>) -> Option<Finding> {
    // A group that cannot consume has no throughput to judge; the rule that says why it cannot
    // consume is the conclusion.
    if input.consumption_stopped() || input.lag.consume_tps >= LOW_CONSUME_TPS {
        return None;
    }
    Some(Finding {
        cause: "Consumer throughput is too low for the backlog".to_string(),
        confidence: 0.72,
        reasoning: format!(
            "consume_tps={} while total_lag={}.",
            input.lag.consume_tps, input.lag.total_lag
        ),
        evidence_refs: vec![CONSUMER_LAG],
        stops_consumption: false,
        recommendation: Recommendation {
            action: "Scale or speed up the consumer group before changing offsets.".to_string(),
            priority: "high".to_string(),
            rationale: format!("total_lag={} is above the configured threshold.", input.lag.total_lag),
            risk: "Scaling consumers can increase broker fetch pressure; watch broker latency.".to_string(),
            verification: "Confirm consume_tps rises and total_lag declines after scaling.".to_string(),
        },
    })
}

fn lag_concentrated_on_broker(input: &RuleInput<'_>) -> Option<Finding> {
    let lag = input.lag;
    let top = lag.queues.brokers.first()?;
    // One queue that holds the share is a queue finding, whichever Broker hosts it.
    if lag.queues.brokers.len() < 2
        || share(top.lag, lag.total_lag) < CONCENTRATION_RATIO
        || share(lag.queues.max_queue_lag, lag.total_lag) >= CONCENTRATION_RATIO
    {
        return None;
    }
    Some(Finding {
        cause: format!("Lag is concentrated on Broker {}", top.broker_name),
        confidence: 0.70,
        reasoning: format!(
            "{} holds lag={} on {} queues, {:.0}% of total_lag={} across {} Brokers.",
            top.broker_name,
            top.lag,
            top.queue_count,
            share(top.lag, lag.total_lag) * 100.0,
            lag.total_lag,
            lag.queues.brokers.len()
        ),
        evidence_refs: vec![CONSUMER_LAG],
        stops_consumption: false,
        recommendation: Recommendation {
            action: format!(
                "Compare Broker {} with the others: rocketmq_describe_broker and rocketmq_get_broker_diagnostics \
                 show its load and store health.",
                top.broker_name
            ),
            priority: "medium".to_string(),
            rationale: "Lag that follows one Broker points at that Broker or at the path to it, not at the consumers."
                .to_string(),
            risk: "Moving traffic away from a Broker shifts its load to the others.".to_string(),
            verification: "Confirm the lag of that Broker converges with the others.".to_string(),
        },
    })
}

fn lag_concentrated_on_queue(input: &RuleInput<'_>) -> Option<Finding> {
    let lag = input.lag;
    let ratio = share(lag.queues.max_queue_lag, lag.total_lag);
    if lag.queues.queue_count < 2 || ratio < CONCENTRATION_RATIO {
        return None;
    }
    Some(Finding {
        cause: "Lag is concentrated on a small subset of queues".to_string(),
        confidence: 0.68,
        reasoning: format!(
            "max_queue_lag={} accounts for {:.0}% of total_lag={}.",
            lag.queues.max_queue_lag,
            ratio * 100.0,
            lag.total_lag
        ),
        evidence_refs: vec![CONSUMER_LAG],
        stops_consumption: false,
        recommendation: Recommendation {
            action: "Inspect queues with the highest lag and their broker placement.".to_string(),
            priority: "medium".to_string(),
            rationale: format!(
                "max_queue_lag={} may identify a localized queue or broker issue.",
                lag.queues.max_queue_lag
            ),
            risk: "Queue-level findings require route evidence before rebalancing decisions.".to_string(),
            verification: "Confirm max_queue_lag converges after the placement review.".to_string(),
        },
    })
}

fn consume_delay(input: &RuleInput<'_>) -> Option<Finding> {
    let delay = input.lag.delay.consume_delay_ms?;
    if delay < input.thresholds.consume_delay_ms {
        return None;
    }
    let mut reasoning = format!(
        "At least half of the backlog sits on queues whose last consumed message was stored {} ago or earlier; the \
         threshold is {}.",
        duration_text(delay),
        duration_text(input.thresholds.consume_delay_ms)
    );
    if let Some(rate) = input.lag.delay.estimated_produce_tps {
        reasoning.push_str(&format!(" The backlog built up at about {}.", rate_text(rate)));
    }
    Some(Finding {
        cause: format!("Consumption is {} behind production", duration_text(delay)),
        confidence: 0.60,
        reasoning,
        evidence_refs: vec![CONSUMER_LAG],
        stops_consumption: false,
        recommendation: Recommendation {
            action: "Check whether the group still makes progress: read rocketmq_get_consumer_progress twice and \
                     compare the consumer offsets."
                .to_string(),
            priority: "medium".to_string(),
            rationale: "A group that is this far behind is either stuck or consumes more slowly than messages arrive."
                .to_string(),
            risk: "Skipping the backlog to catch up loses the messages that are waiting.".to_string(),
            verification: "Confirm the consume delay of the next diagnosis is shorter.".to_string(),
        },
    })
}

fn pop_inflight_high(input: &RuleInput<'_>) -> Option<Finding> {
    let group = input.group?;
    if !group.uses_pop() || input.lag.inflight_total < input.thresholds.pop_inflight {
        return None;
    }
    Some(Finding {
        cause: "POP consumers hold many messages without acknowledging them".to_string(),
        confidence: 0.70,
        reasoning: format!(
            "The group consumes in POP mode and inflight_total={} is at or above the threshold of {}.",
            input.lag.inflight_total, input.thresholds.pop_inflight
        ),
        evidence_refs: vec![CONSUMER_LAG, CONSUMER_GROUP],
        stops_consumption: false,
        recommendation: Recommendation {
            action: "Check how long the consumers take to process a message and whether their acknowledgements fail."
                .to_string(),
            priority: "medium".to_string(),
            rationale: "A message that is fetched but not acknowledged becomes visible again and is delivered once \
                        more, so the backlog does not shrink."
                .to_string(),
            risk: "Redelivered messages are processed more than once.".to_string(),
            verification: "Confirm inflight_total falls below the threshold.".to_string(),
        },
    })
}

fn retry_queue_growing(input: &RuleInput<'_>) -> Option<Finding> {
    let retry = input.retry?;
    if !retry.grew_within(input.thresholds.retry_activity_window_ms) {
        return None;
    }
    Some(Finding {
        cause: "Consumption is failing and messages are being retried".to_string(),
        confidence: 0.60,
        reasoning: system_topic_reasoning("retry", retry, input.thresholds.retry_activity_window_ms),
        evidence_refs: vec![RETRY_TOPIC_STATS],
        stops_consumption: false,
        recommendation: Recommendation {
            action: "Find the consumption errors in the consumer logs and fix what makes messages fail.".to_string(),
            priority: "medium".to_string(),
            rationale: "A message that fails is consumed again later, which takes consumer capacity from new messages."
                .to_string(),
            risk: "Messages that keep failing are dead-lettered once their retries are exhausted.".to_string(),
            verification: "Confirm the retry Topic stops receiving messages.".to_string(),
        },
    })
}

fn dead_letter_queue_growing(input: &RuleInput<'_>) -> Option<Finding> {
    let dead_letter = input.dead_letter?;
    if !dead_letter.grew_within(input.thresholds.retry_activity_window_ms) {
        return None;
    }
    Some(Finding {
        cause: "Messages exhausted their retries and were dead-lettered".to_string(),
        confidence: 0.65,
        reasoning: system_topic_reasoning("dead-letter", dead_letter, input.thresholds.retry_activity_window_ms),
        evidence_refs: vec![DEAD_LETTER_TOPIC_STATS],
        stops_consumption: false,
        recommendation: Recommendation {
            action: "Find out why those messages fail every time; the consumer logs name the error.".to_string(),
            priority: "high".to_string(),
            rationale: "A dead-lettered message is no longer delivered to the group.".to_string(),
            risk: "Dead-lettered messages stay unprocessed until someone handles them.".to_string(),
            verification: "Confirm the dead-letter Topic stops receiving messages.".to_string(),
        },
    })
}

fn system_topic_reasoning(kind: &str, activity: &TopicActivity, window_ms: u64) -> String {
    format!(
        "The {kind} Topic of the group holds {} messages and received one {} ago, within the window of {}.",
        activity.message_count,
        duration_text(activity.last_message_age_ms.unwrap_or_default()),
        duration_text(window_ms)
    )
}

/// The share of `total` that `part` is, or zero when there is no total to share.
pub(crate) fn share(part: i64, total: i64) -> f64 {
    if total <= 0 {
        return 0.0;
    }
    part as f64 / total as f64
}

/// Writes up to [`LISTED_BROKER_LIMIT`] names and says how many more there are.
fn names<'a>(listed: impl Iterator<Item = &'a str>, total: usize) -> String {
    let listed = listed.take(LISTED_BROKER_LIMIT).collect::<Vec<_>>();
    let omitted = total.saturating_sub(listed.len());
    if omitted == 0 {
        listed.join(", ")
    } else {
        format!("{} and {omitted} more", listed.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::diagnosis_collector::fixtures;
    use crate::service::diagnosis_collector::fixtures::queue;
    use crate::service::diagnosis_collector::fixtures::queue_behind;
    use crate::tools::consumer_tools::ConsumerConsumeType;

    const THRESHOLDS: Thresholds = Thresholds {
        lag: 1_000,
        consume_delay_ms: 300_000,
        pop_inflight: 1_000,
        retry_activity_window_ms: 600_000,
    };

    /// The identifiers of the rules that fire on `evidence`, most confident first.
    fn fired(evidence: &ConsumerLagEvidence) -> Vec<&'static str> {
        let input = RuleInput::new(evidence, &THRESHOLDS).unwrap();
        evaluate(&input).into_iter().map(|(id, _)| id).collect()
    }

    fn finding(evidence: &ConsumerLagEvidence, id: &str) -> Finding {
        let input = RuleInput::new(evidence, &THRESHOLDS).unwrap();
        evaluate(&input)
            .into_iter()
            .find(|(fired, _)| *fired == id)
            .map(|(_, finding)| finding)
            .unwrap_or_else(|| panic!("{id} did not fire"))
    }

    /// A backlog of 4,000 spread evenly over two Brokers, consumed at a healthy rate.
    fn backlog() -> ConsumerLagEvidence {
        fixtures::evidence(fixtures::lag_summary(
            vec![
                queue("broker-a", 0, 1_000),
                queue("broker-a", 1, 1_000),
                queue("broker-b", 0, 1_000),
                queue("broker-b", 1, 1_000),
            ],
            500.0,
        ))
    }

    #[test]
    fn rule_identifiers_are_unique_and_name_known_evidence() {
        let mut ids = RULES.iter().map(|rule| rule.id).collect::<Vec<_>>();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), RULES.len());

        let evidence = backlog();
        let input = RuleInput::new(&evidence, &THRESHOLDS).unwrap();
        for rule in RULES {
            for required in rule.requires {
                assert!(input.has(required), "{} requires unknown evidence {required}", rule.id);
            }
        }
    }

    #[test]
    fn a_healthy_backlog_with_complete_evidence_fires_no_rule() {
        assert!(fired(&backlog()).is_empty());
    }

    #[test]
    fn no_online_consumer_fires_when_every_broker_reports_the_group_offline() {
        let mut evidence = backlog();
        evidence.group = Ok(fixtures::group(&[("broker-a", false), ("broker-b", false)]));

        let finding = finding(&evidence, "no_online_consumer");
        assert!(finding.stops_consumption);
        assert_eq!(finding.evidence_refs, [CONSUMER_GROUP]);
        assert!(finding.reasoning.contains("All 2 Brokers"));

        evidence.group = Ok(fixtures::group(&[("broker-a", true), ("broker-b", false)]));
        assert!(!fired(&evidence).contains(&"no_online_consumer"));
    }

    #[test]
    fn consumption_disabled_fires_for_a_broker_whose_configuration_disables_it() {
        let mut evidence = backlog();
        evidence.group = Ok(fixtures::group_with(&[("broker-a", true), ("broker-b", true)], |row| {
            row.consume_enable = Some(row.broker_name != "broker-b");
        }));

        let partly = finding(&evidence, "consumption_disabled");
        assert!(partly.reasoning.contains("1 of 2 Brokers"));
        assert!(partly.reasoning.contains("broker-b"));
        assert!(!partly.stops_consumption, "broker-a still serves the group");

        evidence.group = Ok(fixtures::group_with(&[("broker-a", true), ("broker-b", true)], |row| {
            row.consume_enable = Some(false);
        }));
        assert!(finding(&evidence, "consumption_disabled").stops_consumption);
    }

    #[test]
    fn read_permission_missing_fires_and_says_whether_the_topic_or_the_broker_denies_reads() {
        let mut evidence = backlog();
        // broker-a: the Topic itself is write-only. broker-b: the Topic allows reads but the route does not.
        evidence.route = Ok(fixtures::route(&[("broker-a", 2), ("broker-b", 2), ("broker-c", 6)]));
        evidence.topic_config = Ok(fixtures::topic_config(&[
            ("broker-a", 2),
            ("broker-b", 6),
            ("broker-c", 6),
        ]));

        let finding = finding(&evidence, "read_permission_missing");
        assert!(finding.reasoning.contains("2 of 3 Brokers"));
        assert!(finding
            .reasoning
            .contains("The Topic permission denies reads on 1 of them."));
        assert!(finding.reasoning.contains("the Broker permission denies them"));
        assert_eq!(finding.evidence_refs, [TOPIC_ROUTE, TOPIC_CONFIG]);
        assert!(!finding.stops_consumption, "broker-c is readable");

        // Without the Topic configuration the route still shows the denial, but not its origin.
        evidence.topic_config = fixtures::unavailable();
        evidence.route = Ok(fixtures::route(&[("broker-a", 2), ("broker-b", 0)]));
        let finding = finding_without_attribution(&evidence);
        assert!(finding.stops_consumption);
        assert_eq!(finding.evidence_refs, [TOPIC_ROUTE]);
    }

    fn finding_without_attribution(evidence: &ConsumerLagEvidence) -> Finding {
        let finding = finding(evidence, "read_permission_missing");
        assert!(!finding.reasoning.contains("permission denies reads on"));
        assert!(!finding.reasoning.contains("Broker permission"));
        finding
    }

    #[test]
    fn consumer_missing_on_broker_fires_for_a_lagging_broker_without_consumers() {
        let mut evidence = backlog();
        evidence.group = Ok(fixtures::group(&[("broker-a", true), ("broker-b", false)]));

        let finding = finding(&evidence, "consumer_missing_on_broker");
        assert!(finding.reasoning.contains("2000 of total_lag=4000"));
        assert!(finding.reasoning.contains("broker-b"));
        assert_eq!(finding.evidence_refs, [CONSUMER_LAG, CONSUMER_GROUP]);

        // An offline Broker that holds no lag explains nothing.
        evidence.lag = Ok(fixtures::lag_summary(
            vec![queue("broker-a", 0, 4_000), queue("broker-b", 0, 0)],
            500.0,
        ));
        assert!(!fired(&evidence).contains(&"consumer_missing_on_broker"));
    }

    #[test]
    fn consume_throughput_low_fires_only_for_a_group_that_can_consume() {
        let mut evidence = backlog();
        evidence.lag = Ok(fixtures::lag_summary(vec![queue("broker-a", 0, 4_000)], 0.5));
        assert_eq!(fired(&evidence), ["consume_throughput_low"]);

        evidence.group = Ok(fixtures::group(&[("broker-a", false)]));
        assert_eq!(fired(&evidence), ["no_online_consumer"]);
    }

    #[test]
    fn lag_concentrated_on_broker_fires_when_one_broker_holds_most_of_a_spread_backlog() {
        let mut evidence = backlog();
        evidence.lag = Ok(fixtures::lag_summary(
            vec![
                queue("broker-a", 0, 100),
                queue("broker-a", 1, 100),
                queue("broker-b", 0, 1_900),
                queue("broker-b", 1, 1_900),
            ],
            500.0,
        ));

        let finding = finding(&evidence, "lag_concentrated_on_broker");
        assert_eq!(finding.cause, "Lag is concentrated on Broker broker-b");
        assert!(finding.reasoning.contains("95% of total_lag=4000 across 2 Brokers"));
        assert_eq!(fired(&evidence), ["lag_concentrated_on_broker"]);
    }

    #[test]
    fn lag_concentrated_on_queue_fires_instead_when_one_queue_holds_the_share() {
        let mut evidence = backlog();
        evidence.lag = Ok(fixtures::lag_summary(
            vec![
                queue("broker-a", 0, 100),
                queue("broker-a", 1, 100),
                queue("broker-b", 0, 3_600),
                queue("broker-b", 1, 200),
            ],
            500.0,
        ));

        assert_eq!(fired(&evidence), ["lag_concentrated_on_queue"]);
        assert!(finding(&evidence, "lag_concentrated_on_queue")
            .reasoning
            .contains("max_queue_lag=3600 accounts for 90% of total_lag=4000"));
    }

    #[test]
    fn consume_delay_fires_from_the_delay_threshold_and_states_the_produce_rate() {
        let mut evidence = backlog();
        let behind = |seconds| {
            fixtures::lag_summary(
                vec![
                    queue_behind("broker-a", 0, 2_000, seconds),
                    queue_behind("broker-b", 0, 2_000, seconds),
                ],
                500.0,
            )
        };

        evidence.lag = Ok(behind(299));
        assert!(fired(&evidence).is_empty());

        evidence.lag = Ok(behind(3_600));
        let finding = finding(&evidence, "consume_delay");
        assert_eq!(finding.cause, "Consumption is 60 min behind production");
        assert!(finding.reasoning.contains("the threshold is 5 min"));
        assert!(finding.reasoning.contains("about 1.1 messages per second"));
    }

    #[test]
    fn pop_inflight_high_fires_only_for_a_pop_group_above_the_threshold() {
        let mut evidence = backlog();
        let inflight = |inflight| {
            let mut row = queue("broker-a", 0, 4_000);
            row.inflight = inflight;
            fixtures::lag_summary(vec![row], 500.0)
        };
        let pop = || {
            fixtures::group_with(&[("broker-a", true)], |row| {
                row.consume_type = Some(ConsumerConsumeType::Pop);
            })
        };

        evidence.lag = Ok(inflight(1_000));
        assert!(fired(&evidence).is_empty(), "a push group fetches ahead by design");

        evidence.group = Ok(pop());
        let finding = finding(&evidence, "pop_inflight_high");
        assert!(finding.reasoning.contains("inflight_total=1000"));
        assert_eq!(finding.evidence_refs, [CONSUMER_LAG, CONSUMER_GROUP]);

        evidence.lag = Ok(inflight(999));
        assert!(fired(&evidence).is_empty());
    }

    #[test]
    fn retry_queue_growing_fires_when_the_retry_topic_received_a_message_within_the_window() {
        let mut evidence = backlog();
        evidence.retry = Ok(fixtures::system_topic("%RETRY%order-service", 40, 30));

        let finding = finding(&evidence, "retry_queue_growing");
        assert!(finding
            .reasoning
            .contains("holds 40 messages and received one 30 s ago"));
        assert_eq!(finding.evidence_refs, [RETRY_TOPIC_STATS]);

        // Old retries are history, not an explanation of the present backlog.
        evidence.retry = Ok(fixtures::system_topic("%RETRY%order-service", 40, 601));
        assert!(fired(&evidence).is_empty());
    }

    #[test]
    fn dead_letter_queue_growing_fires_when_the_dead_letter_topic_received_a_message_within_the_window() {
        let mut evidence = backlog();
        evidence.dead_letter = Ok(fixtures::system_topic("%DLQ%order-service", 3, 600));

        let finding = finding(&evidence, "dead_letter_queue_growing");
        assert!(finding
            .reasoning
            .contains("The dead-letter Topic of the group holds 3 messages"));
        assert_eq!(finding.evidence_refs, [DEAD_LETTER_TOPIC_STATS]);

        evidence.dead_letter = Ok(fixtures::system_topic("%DLQ%order-service", 0, 30));
        assert!(fired(&evidence).is_empty(), "an empty Topic did not grow");
    }

    #[test]
    fn only_rules_about_stopped_consumption_run_without_a_backlog() {
        // 900 is below the lag threshold; everything that could fire is set up to fire.
        let mut evidence = backlog();
        let mut lagging = queue_behind("broker-a", 0, 900, 86_400);
        lagging.inflight = 5_000;
        evidence.lag = Ok(fixtures::lag_summary(vec![lagging, queue("broker-b", 0, 0)], 0.0));
        evidence.group = Ok(fixtures::group_with(
            &[("broker-a", false), ("broker-b", false)],
            |row| {
                row.consume_enable = Some(false);
                row.consume_type = Some(ConsumerConsumeType::Pop);
            },
        ));
        evidence.route = Ok(fixtures::route(&[("broker-a", 2), ("broker-b", 2)]));
        evidence.retry = Ok(fixtures::system_topic("%RETRY%order-service", 40, 30));
        evidence.dead_letter = Ok(fixtures::system_topic("%DLQ%order-service", 3, 30));

        assert_eq!(
            fired(&evidence),
            ["consumption_disabled", "no_online_consumer", "read_permission_missing"]
        );
    }

    #[test]
    fn a_rule_is_silent_without_the_evidence_it_requires() {
        let mut evidence = backlog();
        evidence.lag = Ok(fixtures::lag_summary(vec![queue("broker-a", 0, 4_000)], 500.0));
        evidence.route = fixtures::unavailable();
        evidence.group = fixtures::unavailable();
        evidence.topic_config = fixtures::unavailable();
        evidence.retry = fixtures::unavailable();
        evidence.dead_letter = fixtures::unavailable();

        // Nothing is known about consumers, permissions or retries, so nothing is concluded.
        assert!(fired(&evidence).is_empty());
    }

    #[test]
    fn long_broker_lists_are_shortened_in_the_reasoning() {
        let rows = (0..25)
            .map(|index| (format!("broker-{index:02}"), true))
            .collect::<Vec<_>>();
        let rows = rows
            .iter()
            .map(|(name, online)| (name.as_str(), *online))
            .collect::<Vec<_>>();
        let mut evidence = backlog();
        evidence.group = Ok(fixtures::group_with(&rows, |row| row.consume_enable = Some(false)));

        let reasoning = finding(&evidence, "consumption_disabled").reasoning;
        assert!(reasoning.contains("25 of 25 Brokers"));
        assert!(reasoning.contains("broker-09 and 15 more"));
        assert!(!reasoning.contains("broker-10"));
    }
}
