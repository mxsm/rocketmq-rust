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
    key: "broker-metrics",
    source: "prometheus",
    resource_prefix: "broker-health/",
    purpose: "Readiness, errors, connections, CPU/RSS, disk, flush, dispatch, and HA lag",
}];

const OPTIONAL: &[EvidenceRequirement] = &[
    EvidenceRequirement {
        key: "broker-runtime",
        source: "rocketmq-mcp",
        resource_prefix: "broker-runtime/",
        purpose: "Independent Broker runtime readiness",
    },
    EvidenceRequirement {
        key: "connections",
        source: "admin-query",
        resource_prefix: "broker-connections/",
        purpose: "Read-only client and peer connection inventory",
    },
];

const RULES: &[&str] = &[
    "BROKER_NOT_READY",
    "BROKER_REQUEST_ERRORS",
    "BROKER_RESOURCE_PRESSURE",
    "BROKER_DISK_PRESSURE",
    "BROKER_FLUSH_DISPATCH_STALL",
    "BROKER_HA_LAG",
    "BROKER_HEALTHY",
    "BROKER_HEALTH_EVIDENCE_INCOMPLETE",
];

const FOLLOW_UP: &[FollowUpQuery] = &[
    FollowUpQuery {
        source: "rocketmq-mcp",
        resource_template: "broker-runtime/{broker}",
        reason: "Confirm runtime readiness independently of telemetry",
    },
    FollowUpQuery {
        source: "admin-query",
        resource_template: "broker-connections/{broker}",
        reason: "Confirm client and HA connection state",
    },
];

/// Broker readiness, resource, Store pipeline, and HA health pack.
#[derive(Debug)]
pub struct BrokerHealthV1;

impl DiagnosticPack for BrokerHealthV1 {
    fn id(&self) -> &'static str {
        "broker-health"
    }

    fn version(&self) -> PackVersion {
        PackVersion::new(1, 0, 0)
    }

    fn applicable_components(&self) -> &'static [&'static str] {
        &["broker", "store"]
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
        if !context.is_available("broker-metrics") {
            return Ok(Vec::new());
        }
        let required_fields = [
            context.bool("broker-metrics", "ready").is_some(),
            context.number("broker-metrics", "request_error_rate_percent").is_some(),
            context.number("broker-metrics", "active_connections").is_some(),
            context.number("broker-metrics", "cpu_percent").is_some(),
            context.number("broker-metrics", "rss_bytes").is_some(),
            context.number("broker-metrics", "disk_used_percent").is_some(),
            context.number("broker-metrics", "flush_latency_ms").is_some(),
            context.number("broker-metrics", "dispatch_backlog").is_some(),
            context.number("broker-metrics", "ha_lag_bytes").is_some(),
        ];
        if required_fields.contains(&false) {
            return Ok(common::incomplete(
                context,
                "broker-metrics",
                "BROKER_HEALTH_EVIDENCE_INCOMPLETE",
                &[
                    "ready",
                    "request_error_rate_percent",
                    "active_connections",
                    "cpu_percent",
                    "rss_bytes",
                    "disk_used_percent",
                    "flush_latency_ms",
                    "dispatch_backlog",
                    "ha_lag_bytes",
                ],
            )
            .into_iter()
            .collect());
        }

        let mut findings = Vec::new();
        if context.bool("broker-metrics", "ready") == Some(false)
            || context.bool("broker-runtime", "broker_up") == Some(false)
        {
            let requirement = if context.bool("broker-runtime", "broker_up") == Some(false) {
                "broker-runtime"
            } else {
                "broker-metrics"
            };
            findings.extend(common::conclusion(
                context,
                requirement,
                "BROKER_NOT_READY",
                "The Broker is not ready to serve requests",
                Severity::Critical,
                FindingOutcome::Fault,
                "Readiness evidence reports the Broker down or not ready",
            ));
        }
        if context
            .number("broker-metrics", "request_error_rate_percent")
            .is_some_and(|value| value >= 5.0)
        {
            findings.extend(common::conclusion(
                context,
                "broker-metrics",
                "BROKER_REQUEST_ERRORS",
                "Broker request error rate exceeds five percent",
                Severity::Critical,
                FindingOutcome::Fault,
                "The bounded request error metric crosses the deterministic threshold",
            ));
        }
        if context
            .number("broker-metrics", "cpu_percent")
            .is_some_and(|value| value >= 90.0)
        {
            findings.extend(common::conclusion(
                context,
                "broker-metrics",
                "BROKER_RESOURCE_PRESSURE",
                "Broker CPU is under sustained resource pressure",
                Severity::Warning,
                FindingOutcome::Fault,
                "The Broker CPU metric crosses ninety percent",
            ));
        }
        if context
            .number("broker-metrics", "disk_used_percent")
            .is_some_and(|value| value >= 85.0)
        {
            findings.extend(common::conclusion(
                context,
                "broker-metrics",
                "BROKER_DISK_PRESSURE",
                "Broker storage usage is approaching the operational limit",
                Severity::Critical,
                FindingOutcome::Fault,
                "The Broker disk metric crosses eighty-five percent",
            ));
        }
        if context
            .number("broker-metrics", "flush_latency_ms")
            .is_some_and(|value| value >= 500.0)
            || context
                .number("broker-metrics", "dispatch_backlog")
                .is_some_and(|value| value >= 1_000.0)
        {
            findings.extend(
                common::conclusion(
                    context,
                    "broker-metrics",
                    "BROKER_FLUSH_DISPATCH_STALL",
                    "Store flush latency or dispatch backlog indicates a persistence pipeline stall",
                    Severity::Critical,
                    FindingOutcome::Fault,
                    "Flush and dispatch metrics cross the Wave A thresholds",
                )
                .map(|finding| finding.with_matched_signals(2)),
            );
        }
        if context
            .number("broker-metrics", "ha_lag_bytes")
            .is_some_and(|value| value >= 10_485_760.0)
        {
            findings.extend(common::conclusion(
                context,
                "broker-metrics",
                "BROKER_HA_LAG",
                "Broker HA replication lag exceeds ten MiB",
                Severity::Critical,
                FindingOutcome::Fault,
                "The HA lag metric crosses the deterministic byte threshold",
            ));
        }

        if findings.is_empty() {
            findings.extend(common::conclusion(
                context,
                "broker-metrics",
                "BROKER_HEALTHY",
                "Broker readiness, requests, resources, Store pipeline, and HA are healthy",
                Severity::Info,
                FindingOutcome::Healthy,
                "The complete Broker metric snapshot crosses no deterministic threshold",
            ));
        }
        Ok(findings)
    }
}
