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
    key: "producer",
    source: "admin-query",
    resource_prefix: "producer-connectivity/",
    purpose: "Connection, route refresh, send/retry/timeout, queue selection, backpressure, and version",
}];

const OPTIONAL: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "topic-route",
    source: "rocketmq-mcp",
    resource_prefix: "topic-route/",
    purpose: "Independent confirmation that a route is available",
}];

const RULES: &[&str] = &[
    "PRODUCER_CONNECTION_LOST",
    "PRODUCER_ROUTE_STALE",
    "PRODUCER_SEND_FAILURES",
    "PRODUCER_QUEUE_SELECTION_FAILED",
    "PRODUCER_BACKPRESSURE",
    "PRODUCER_CLIENT_VERSION_UNSUPPORTED",
    "PRODUCER_CONNECTIVITY_HEALTHY",
    "PRODUCER_CONNECTIVITY_EVIDENCE_INCOMPLETE",
];

const FOLLOW_UP: &[FollowUpQuery] = &[FollowUpQuery {
    source: "rocketmq-mcp",
    resource_template: "topic-route/{topic}",
    reason: "Confirm NameServer route availability independently of the producer runtime",
}];

/// Producer route, send, retry, timeout, queue, and backpressure pack.
#[derive(Debug)]
pub struct ProducerConnectivityV1;

impl DiagnosticPack for ProducerConnectivityV1 {
    fn id(&self) -> &'static str {
        "producer-connectivity"
    }

    fn version(&self) -> PackVersion {
        PackVersion::new(1, 0, 0)
    }

    fn applicable_components(&self) -> &'static [&'static str] {
        &["producer", "nameserver", "broker"]
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
        if !context.is_available("producer") {
            return Ok(Vec::new());
        }
        let required_fields = [
            context.bool("producer", "connected").is_some(),
            context.number("producer", "route_age_seconds").is_some(),
            context.number("producer", "send_error_rate_percent").is_some(),
            context.number("producer", "retry_rate_percent").is_some(),
            context.number("producer", "timeout_rate_percent").is_some(),
            context.number("producer", "queue_selection_failures").is_some(),
            context.bool("producer", "backpressure").is_some(),
            context.bool("producer", "client_version_supported").is_some(),
        ];
        if required_fields.contains(&false) {
            return Ok(common::incomplete(
                context,
                "producer",
                "PRODUCER_CONNECTIVITY_EVIDENCE_INCOMPLETE",
                &[
                    "connected",
                    "route_age_seconds",
                    "send_error_rate_percent",
                    "retry_rate_percent",
                    "timeout_rate_percent",
                    "queue_selection_failures",
                    "backpressure",
                    "client_version_supported",
                ],
            )
            .into_iter()
            .collect());
        }

        let mut findings = Vec::new();
        if context.bool("producer", "connected") == Some(false) {
            findings.extend(common::conclusion(
                context,
                "producer",
                "PRODUCER_CONNECTION_LOST",
                "The producer has no active Broker connection",
                Severity::Critical,
                FindingOutcome::Fault,
                "The producer runtime snapshot reports a disconnected client",
            ));
        }
        if context
            .number("producer", "route_age_seconds")
            .is_some_and(|value| value >= 300.0)
            || context.bool("topic-route", "route_available") == Some(false)
        {
            let requirement = if context.bool("topic-route", "route_available") == Some(false) {
                "topic-route"
            } else {
                "producer"
            };
            findings.extend(common::conclusion(
                context,
                requirement,
                "PRODUCER_ROUTE_STALE",
                "The producer route is stale or unavailable",
                Severity::Critical,
                FindingOutcome::Fault,
                "Route freshness or independent route availability crosses the fail-closed threshold",
            ));
        }
        if context
            .number("producer", "send_error_rate_percent")
            .is_some_and(|value| value >= 5.0)
            || context
                .number("producer", "retry_rate_percent")
                .is_some_and(|value| value >= 10.0)
            || context
                .number("producer", "timeout_rate_percent")
                .is_some_and(|value| value >= 2.0)
        {
            findings.extend(
                common::conclusion(
                    context,
                    "producer",
                    "PRODUCER_SEND_FAILURES",
                    "Send errors, retries, or timeouts exceed the Wave A baseline",
                    Severity::Critical,
                    FindingOutcome::Fault,
                    "The producer snapshot reports elevated send outcome rates",
                )
                .map(|finding| finding.with_matched_signals(3)),
            );
        }
        if context
            .number("producer", "queue_selection_failures")
            .is_some_and(|value| value > 0.0)
        {
            findings.extend(common::conclusion(
                context,
                "producer",
                "PRODUCER_QUEUE_SELECTION_FAILED",
                "The producer cannot select a writable message queue",
                Severity::Critical,
                FindingOutcome::Fault,
                "The producer snapshot reports queue-selection failures",
            ));
        }
        if context.bool("producer", "backpressure") == Some(true) {
            findings.extend(common::conclusion(
                context,
                "producer",
                "PRODUCER_BACKPRESSURE",
                "Producer backpressure is limiting send progress",
                Severity::Warning,
                FindingOutcome::Fault,
                "The producer runtime marks bounded backpressure active",
            ));
        }
        if context.bool("producer", "client_version_supported") == Some(false) {
            findings.extend(common::conclusion(
                context,
                "producer",
                "PRODUCER_CLIENT_VERSION_UNSUPPORTED",
                "The producer client version is outside the supported compatibility range",
                Severity::Warning,
                FindingOutcome::Fault,
                "The producer runtime marks the client version unsupported",
            ));
        }

        if findings.is_empty() {
            findings.extend(common::conclusion(
                context,
                "producer",
                "PRODUCER_CONNECTIVITY_HEALTHY",
                "Connection, route, send outcomes, queue selection, and backpressure are healthy",
                Severity::Info,
                FindingOutcome::Healthy,
                "The complete producer snapshot crosses no deterministic threshold",
            ));
        }
        Ok(findings)
    }
}
