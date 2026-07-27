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
    key: "runtime-observability",
    source: "runtime",
    resource_prefix: "observability/",
    purpose: "Build feature, exporter, last success, queue/drop, collector, and backend state",
}];

const OPTIONAL: &[EvidenceRequirement] = &[
    EvidenceRequirement {
        key: "collector-metrics",
        source: "prometheus",
        resource_prefix: "telemetry-pipeline/",
        purpose: "Collector queue and export telemetry",
    },
    EvidenceRequirement {
        key: "collector-workload",
        source: "kubernetes",
        resource_prefix: "otel-collector/",
        purpose: "Collector workload readiness and restarts",
    },
];

const RULES: &[&str] = &[
    "TELEMETRY_BUILD_FEATURE_DISABLED",
    "TELEMETRY_EXPORTER_DISABLED",
    "TELEMETRY_EXPORT_STALE",
    "TELEMETRY_QUEUE_DROPPING",
    "TELEMETRY_COLLECTOR_UNAVAILABLE",
    "TELEMETRY_BACKEND_UNAVAILABLE",
    "TELEMETRY_PIPELINE_HEALTHY",
    "TELEMETRY_EVIDENCE_INCOMPLETE",
];

const FOLLOW_UP: &[FollowUpQuery] = &[
    FollowUpQuery {
        source: "prometheus",
        resource_template: "telemetry-pipeline/{collector}",
        reason: "Distinguish exporter queue pressure from collector export failure",
    },
    FollowUpQuery {
        source: "kubernetes",
        resource_template: "otel-collector/{namespace}",
        reason: "Confirm Collector readiness and restart state",
    },
];

/// Telemetry build-to-backend pipeline pack.
#[derive(Debug)]
pub struct TelemetryPipelineV1;

impl DiagnosticPack for TelemetryPipelineV1 {
    fn id(&self) -> &'static str {
        "telemetry-pipeline"
    }

    fn version(&self) -> PackVersion {
        PackVersion::new(1, 0, 0)
    }

    fn applicable_components(&self) -> &'static [&'static str] {
        &[
            "broker",
            "nameserver",
            "controller",
            "proxy",
            "mcp",
            "runtime",
            "otel-collector",
        ]
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
        if !context.is_available("runtime-observability") {
            return Ok(Vec::new());
        }
        let required_fields = [
            context.bool("runtime-observability", "build_feature_enabled").is_some(),
            context.bool("runtime-observability", "exporter_enabled").is_some(),
            context
                .number("runtime-observability", "last_export_success_age_seconds")
                .is_some(),
            context.number("runtime-observability", "queue_depth").is_some(),
            context.number("runtime-observability", "dropped_items").is_some(),
            context.bool("runtime-observability", "collector_reachable").is_some(),
            context.bool("runtime-observability", "backend_reachable").is_some(),
        ];
        if required_fields.contains(&false) {
            return Ok(common::incomplete(
                context,
                "runtime-observability",
                "TELEMETRY_EVIDENCE_INCOMPLETE",
                &[
                    "build_feature_enabled",
                    "exporter_enabled",
                    "last_export_success_age_seconds",
                    "queue_depth",
                    "dropped_items",
                    "collector_reachable",
                    "backend_reachable",
                ],
            )
            .into_iter()
            .collect());
        }

        let mut findings = Vec::new();
        if context.bool("runtime-observability", "build_feature_enabled") == Some(false) {
            findings.extend(common::conclusion(
                context,
                "runtime-observability",
                "TELEMETRY_BUILD_FEATURE_DISABLED",
                "The required telemetry exporter was not compiled into the component",
                Severity::Critical,
                FindingOutcome::Fault,
                "The runtime diagnostics view reports the build feature disabled",
            ));
        }
        if context.bool("runtime-observability", "exporter_enabled") == Some(false) {
            findings.extend(common::conclusion(
                context,
                "runtime-observability",
                "TELEMETRY_EXPORTER_DISABLED",
                "The telemetry exporter is disabled at runtime",
                Severity::Critical,
                FindingOutcome::Fault,
                "The runtime diagnostics view reports the exporter disabled",
            ));
        }
        if context
            .number("runtime-observability", "last_export_success_age_seconds")
            .is_some_and(|value| value >= 300.0)
        {
            findings.extend(common::conclusion(
                context,
                "runtime-observability",
                "TELEMETRY_EXPORT_STALE",
                "No successful telemetry export has completed in five minutes",
                Severity::Critical,
                FindingOutcome::Fault,
                "Last export success age crosses the deterministic freshness threshold",
            ));
        }
        if context
            .number("runtime-observability", "queue_depth")
            .is_some_and(|value| value >= 1_000.0)
            || context
                .number("runtime-observability", "dropped_items")
                .is_some_and(|value| value > 0.0)
        {
            findings.extend(
                common::conclusion(
                    context,
                    "runtime-observability",
                    "TELEMETRY_QUEUE_DROPPING",
                    "Exporter queue pressure is causing telemetry loss",
                    Severity::Critical,
                    FindingOutcome::Fault,
                    "Queue depth or dropped item count crosses the Wave A threshold",
                )
                .map(|finding| finding.with_matched_signals(2)),
            );
        }
        if context.bool("runtime-observability", "collector_reachable") == Some(false) {
            findings.extend(common::conclusion(
                context,
                "runtime-observability",
                "TELEMETRY_COLLECTOR_UNAVAILABLE",
                "The component cannot reach the configured telemetry Collector",
                Severity::Critical,
                FindingOutcome::Fault,
                "The runtime diagnostics view reports Collector reachability false",
            ));
        }
        if context.bool("runtime-observability", "backend_reachable") == Some(false) {
            findings.extend(common::conclusion(
                context,
                "runtime-observability",
                "TELEMETRY_BACKEND_UNAVAILABLE",
                "The telemetry backend is unavailable behind the Collector",
                Severity::Warning,
                FindingOutcome::Fault,
                "The runtime diagnostics view reports backend reachability false",
            ));
        }

        if findings.is_empty() {
            findings.extend(common::conclusion(
                context,
                "runtime-observability",
                "TELEMETRY_PIPELINE_HEALTHY",
                "Telemetry build features, exporter, queue, Collector, and backend are healthy",
                Severity::Info,
                FindingOutcome::Healthy,
                "The complete observability snapshot crosses no deterministic threshold",
            ));
        }
        Ok(findings)
    }
}
