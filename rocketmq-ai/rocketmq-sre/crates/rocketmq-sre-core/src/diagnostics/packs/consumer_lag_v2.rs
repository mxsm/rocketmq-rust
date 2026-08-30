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

use super::super::DiagnosticContext;
use super::super::DiagnosticError;
use super::super::DiagnosticPack;
use super::super::EvidenceRequirement;
use super::super::FindingOutcome;
use super::super::FollowUpQuery;
use super::super::PackVersion;
use super::super::RuleMatch;
use super::super::Severity;
use super::common;

const REQUIRED: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "consumer-lag",
    source: "rocketmq-mcp",
    resource_prefix: "consumer-lag/",
    purpose: "Lag total, slope, per-queue skew, and production/consumption rates",
}];

const OPTIONAL: &[EvidenceRequirement] = &[
    EvidenceRequirement {
        key: "consumer-runtime",
        source: "admin-query",
        resource_prefix: "consumer-runtime/",
        purpose: "Connections, pause state, and queue allocation",
    },
    EvidenceRequirement {
        key: "broker-store",
        source: "rocketmq-mcp",
        resource_prefix: "broker-runtime/",
        purpose: "Route, Broker, and Store health used for attribution",
    },
];

const RULES: &[&str] = &[
    "CONSUMER_THROUGHPUT_DEFICIT",
    "CONSUMER_QUEUE_SKEW",
    "CONSUMER_RUNTIME_UNAVAILABLE",
    "CONSUMER_BROKER_STORE_DEPENDENCY",
    "CONSUMER_LAG_HEALTHY",
    "CONSUMER_LAG_EVIDENCE_INCOMPLETE",
];

const FOLLOW_UP: &[FollowUpQuery] = &[
    FollowUpQuery {
        source: "admin-query",
        resource_template: "consumer-runtime/{group}",
        reason: "Distinguish client runtime and allocation problems from load",
    },
    FollowUpQuery {
        source: "rocketmq-mcp",
        resource_template: "broker-runtime/{broker}",
        reason: "Check Broker and Store dependencies for lag attribution",
    },
];

/// Consumer lag slope, skew, throughput, runtime, and dependency pack.
#[derive(Debug)]
pub struct ConsumerLagV2;

impl DiagnosticPack for ConsumerLagV2 {
    fn id(&self) -> &'static str {
        "consumer-lag"
    }

    fn version(&self) -> PackVersion {
        PackVersion::new(2, 0, 0)
    }

    fn applicable_components(&self) -> &'static [&'static str] {
        &["consumer", "broker", "store", "nameserver"]
    }

    fn required_evidence(&self) -> &'static [EvidenceRequirement] {
        REQUIRED
    }

    fn optional_evidence(&self) -> &'static [EvidenceRequirement] {
        OPTIONAL
    }

    fn rule_codes(&self) -> &'static [&'static str] {
        RULES
    }

    fn follow_up_queries(&self) -> &'static [FollowUpQuery] {
        FOLLOW_UP
    }

    fn evaluate(&self, context: &DiagnosticContext<'_>) -> Result<Vec<RuleMatch>, DiagnosticError> {
        if !context.is_available("consumer-lag") {
            return Ok(Vec::new());
        }
        let total_lag = context.number("consumer-lag", "total_lag");
        let slope = context.number("consumer-lag", "lag_slope_per_min");
        let skew = context.number("consumer-lag", "queue_skew_ratio");
        let consume_rate = context.number("consumer-lag", "consume_rate_per_sec");
        let produce_rate = context.number("consumer-lag", "produce_rate_per_sec");
        if [total_lag, slope, skew, consume_rate, produce_rate]
            .iter()
            .any(Option::is_none)
        {
            return Ok(common::incomplete(
                context,
                "consumer-lag",
                "CONSUMER_LAG_EVIDENCE_INCOMPLETE",
                &[
                    "total_lag",
                    "lag_slope_per_min",
                    "queue_skew_ratio",
                    "consume_rate_per_sec",
                    "produce_rate_per_sec",
                ],
            )
            .into_iter()
            .collect());
        }

        let mut findings = Vec::new();
        if total_lag.is_some_and(|value| value > 0.0)
            && slope.is_some_and(|value| value > 0.0)
            && produce_rate
                .zip(consume_rate)
                .is_some_and(|(produced, consumed)| produced > consumed)
            && let Some(mut finding) = common::conclusion(
                context,
                "consumer-lag",
                "CONSUMER_THROUGHPUT_DEFICIT",
                "Consumer throughput is below production rate while lag is growing",
                Severity::Critical,
                FindingOutcome::Fault,
                "Lag, slope, and production/consumption rates jointly match the rule",
            )
        {
            finding = finding.with_matched_signals(3);
            if context.bool("broker-store", "broker_ready") == Some(true)
                && context.bool("broker-store", "store_healthy") == Some(true)
                && let Some(counter) = context.cite(
                    "broker-store",
                    "Broker and Store report healthy, countering a server dependency root cause",
                )
            {
                finding = finding.with_counter(counter);
            }
            findings.push(finding);
        }
        if skew.is_some_and(|value| value >= 2.0) {
            findings.extend(
                common::conclusion(
                    context,
                    "consumer-lag",
                    "CONSUMER_QUEUE_SKEW",
                    "Lag is concentrated on a subset of queues",
                    Severity::Warning,
                    FindingOutcome::Fault,
                    "The maximum-to-median queue lag ratio crosses the deterministic threshold",
                )
                .map(|finding| finding.with_matched_signals(2)),
            );
        }
        if context.bool("consumer-runtime", "paused") == Some(true)
            || context
                .number("consumer-runtime", "connected_clients")
                .is_some_and(|value| value == 0.0)
        {
            findings.extend(common::conclusion(
                context,
                "consumer-runtime",
                "CONSUMER_RUNTIME_UNAVAILABLE",
                "The consumer runtime is paused or has no active connection",
                Severity::Critical,
                FindingOutcome::Fault,
                "The runtime snapshot reports a paused or disconnected consumer",
            ));
        }
        if context.bool("broker-store", "broker_ready") == Some(false)
            || context.bool("broker-store", "store_healthy") == Some(false)
        {
            findings.extend(common::conclusion(
                context,
                "broker-store",
                "CONSUMER_BROKER_STORE_DEPENDENCY",
                "Broker or Store health is contributing to consumer lag",
                Severity::Critical,
                FindingOutcome::Fault,
                "The Broker runtime snapshot reports an unhealthy dependency",
            ));
        }

        if findings.is_empty() {
            findings.extend(common::conclusion(
                context,
                "consumer-lag",
                "CONSUMER_LAG_HEALTHY",
                "Lag slope, queue skew, and throughput are within the Wave A baseline",
                Severity::Info,
                FindingOutcome::Healthy,
                "The complete consumer lag snapshot crosses no deterministic threshold",
            ));
        }
        Ok(findings)
    }
}
