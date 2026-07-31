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
    key: "runtime",
    source: "admin-query",
    resource_prefix: "consumer-runtime/",
    purpose: "Connection, running info, rebalance, allocation, process RT, offset controls, and version",
}];

const OPTIONAL: &[EvidenceRequirement] = &[EvidenceRequirement {
    key: "runtime-metrics",
    source: "prometheus",
    resource_prefix: "consumer-runtime/",
    purpose: "Time-series corroboration for processing latency and rebalance activity",
}];

const RULES: &[&str] = &[
    "CONSUMER_CONNECTION_LOST",
    "CONSUMER_REBALANCE_STALLED",
    "CONSUMER_PROCESSING_SLOW",
    "CONSUMER_OFFSET_CONTROL_ANOMALY",
    "CONSUMER_CLIENT_VERSION_UNSUPPORTED",
    "CONSUMER_RUNTIME_HEALTHY",
    "CONSUMER_RUNTIME_EVIDENCE_INCOMPLETE",
];

const FOLLOW_UP: &[FollowUpQuery] = &[FollowUpQuery {
    source: "prometheus",
    resource_template: "consumer-runtime/{group}",
    reason: "Confirm duration and trend of process latency or rebalance activity",
}];

/// Consumer connection and runtime behavior pack.
#[derive(Debug)]
pub struct ConsumerRuntimeV1;

impl DiagnosticPack for ConsumerRuntimeV1 {
    fn id(&self) -> &'static str {
        "consumer-runtime"
    }

    fn version(&self) -> PackVersion {
        PackVersion::new(1, 0, 0)
    }

    fn applicable_components(&self) -> &'static [&'static str] {
        &["consumer", "broker"]
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
        if !context.is_available("runtime") {
            return Ok(Vec::new());
        }
        let required_fields = [
            context.bool("runtime", "connected").is_some(),
            context.bool("runtime", "running_info_available").is_some(),
            context.bool("runtime", "rebalance_in_progress").is_some(),
            context.number("runtime", "unassigned_queues").is_some(),
            context.number("runtime", "process_rt_ms").is_some(),
            context.bool("runtime", "paused").is_some(),
            context.number("runtime", "commit_failures").is_some(),
            context.number("runtime", "seek_operations").is_some(),
            context.bool("runtime", "client_version_supported").is_some(),
        ];
        if required_fields.contains(&false) {
            return Ok(common::incomplete(
                context,
                "runtime",
                "CONSUMER_RUNTIME_EVIDENCE_INCOMPLETE",
                &[
                    "connected",
                    "running_info_available",
                    "rebalance_in_progress",
                    "unassigned_queues",
                    "process_rt_ms",
                    "paused",
                    "commit_failures",
                    "seek_operations",
                    "client_version_supported",
                ],
            )
            .into_iter()
            .collect());
        }

        let mut findings = Vec::new();
        if context.bool("runtime", "connected") == Some(false)
            || context.bool("runtime", "running_info_available") == Some(false)
        {
            findings.extend(common::conclusion(
                context,
                "runtime",
                "CONSUMER_CONNECTION_LOST",
                "The consumer connection or running information is unavailable",
                Severity::Critical,
                FindingOutcome::Fault,
                "The read-only runtime snapshot reports a disconnected client",
            ));
        }
        if context.bool("runtime", "rebalance_in_progress") == Some(true)
            && context
                .number("runtime", "unassigned_queues")
                .is_some_and(|value| value > 0.0)
        {
            findings.extend(
                common::conclusion(
                    context,
                    "runtime",
                    "CONSUMER_REBALANCE_STALLED",
                    "Rebalance is in progress while queues remain unassigned",
                    Severity::Critical,
                    FindingOutcome::Fault,
                    "Rebalance and allocation fields jointly match the stalled rule",
                )
                .map(|finding| finding.with_matched_signals(2)),
            );
        }
        if context
            .number("runtime", "process_rt_ms")
            .is_some_and(|value| value >= 1_000.0)
        {
            findings.extend(common::conclusion(
                context,
                "runtime",
                "CONSUMER_PROCESSING_SLOW",
                "Consumer processing latency exceeds the one-second Wave A threshold",
                Severity::Warning,
                FindingOutcome::Fault,
                "The runtime snapshot reports elevated process RT",
            ));
        }
        if context.bool("runtime", "paused") == Some(true)
            || context
                .number("runtime", "commit_failures")
                .is_some_and(|value| value > 0.0)
            || context
                .number("runtime", "seek_operations")
                .is_some_and(|value| value > 0.0)
        {
            findings.extend(common::conclusion(
                context,
                "runtime",
                "CONSUMER_OFFSET_CONTROL_ANOMALY",
                "Pause, commit failure, or seek activity can interrupt normal offset progress",
                Severity::Warning,
                FindingOutcome::Fault,
                "The runtime snapshot reports offset-control activity",
            ));
        }
        if context.bool("runtime", "client_version_supported") == Some(false) {
            findings.extend(common::conclusion(
                context,
                "runtime",
                "CONSUMER_CLIENT_VERSION_UNSUPPORTED",
                "The consumer client version is outside the supported compatibility range",
                Severity::Warning,
                FindingOutcome::Fault,
                "The runtime snapshot marks the client version unsupported",
            ));
        }

        if findings.is_empty() {
            findings.extend(common::conclusion(
                context,
                "runtime",
                "CONSUMER_RUNTIME_HEALTHY",
                "Connection, rebalance, allocation, processing, and offset controls are healthy",
                Severity::Info,
                FindingOutcome::Healthy,
                "The complete consumer runtime snapshot crosses no deterministic threshold",
            ));
        }
        Ok(findings)
    }
}
