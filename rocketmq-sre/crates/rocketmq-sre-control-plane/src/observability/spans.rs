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

use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use crate::observability::CorrelationContext;
use crate::observability::DiagnosticPackLabel;
use crate::observability::EvidenceSourceLabel;
use crate::observability::IncidentOutcome;
use crate::observability::ModelPurposeLabel;
use crate::observability::ModelTokenDirection;
use crate::observability::ProviderFamilyLabel;
use crate::observability::ResultClass;
use crate::observability::SreMetricSink;
use crate::observability::SreMetrics;
use crate::observability::ToolClassLabel;

pub const SPAN_INCIDENT_RUN: &str = "sre.incident.run";
pub const SPAN_EVIDENCE_COLLECT: &str = "sre.evidence.collect";
pub const SPAN_DIAGNOSTIC_EVALUATE: &str = "sre.diagnostic.evaluate";
pub const SPAN_MODEL_INVOKE: &str = "sre.model.invoke";

// One control-plane process has one Prometheus surface. Domain services are
// constructed before the HTTP router creates its exporter handle, so both
// paths resolve this same collector without passing endpoint state into the
// domain layer.
static PROCESS_METRICS: OnceLock<Arc<SreMetrics>> = OnceLock::new();

fn process_metrics() -> Arc<SreMetrics> {
    PROCESS_METRICS.get_or_init(|| Arc::new(SreMetrics::new())).clone()
}

/// Injectable observability facade shared by workflow, evidence, diagnostic,
/// tool, and model services.
///
/// The facade intentionally exposes no methods that accept prompt text,
/// evidence content, tool arguments, tokens, secrets, endpoints, or arbitrary
/// label strings.
#[derive(Clone)]
pub struct SreObservability {
    metrics: Arc<dyn SreMetricSink>,
}

impl SreObservability {
    #[must_use]
    pub fn new(metrics: Arc<dyn SreMetricSink>) -> Self {
        Self { metrics }
    }

    #[must_use]
    pub fn with_prometheus_metrics() -> (Self, Arc<SreMetrics>) {
        let metrics = process_metrics();
        (Self::new(metrics.clone()), metrics)
    }

    #[must_use]
    pub fn incident_run_span(&self, correlation: CorrelationContext) -> tracing::Span {
        tracing::info_span!(
            target: "rocketmq_sre::operations",
            "sre.incident.run",
            correlation_id = %correlation.id(),
            component = "control_plane"
        )
    }

    #[must_use]
    pub fn evidence_collect_span(&self, correlation: CorrelationContext, source: EvidenceSourceLabel) -> tracing::Span {
        tracing::info_span!(
            target: "rocketmq_sre::operations",
            "sre.evidence.collect",
            correlation_id = %correlation.id(),
            source = source.as_str(),
            component = "control_plane"
        )
    }

    #[must_use]
    pub fn diagnostic_evaluate_span(
        &self,
        correlation: CorrelationContext,
        pack: DiagnosticPackLabel,
    ) -> tracing::Span {
        tracing::info_span!(
            target: "rocketmq_sre::operations",
            "sre.diagnostic.evaluate",
            correlation_id = %correlation.id(),
            pack = pack.as_str(),
            component = "control_plane"
        )
    }

    #[must_use]
    pub fn model_invoke_span(
        &self,
        correlation: CorrelationContext,
        provider: ProviderFamilyLabel,
        purpose: ModelPurposeLabel,
    ) -> tracing::Span {
        tracing::info_span!(
            target: "rocketmq_sre::operations",
            "sre.model.invoke",
            correlation_id = %correlation.id(),
            provider = provider.as_str(),
            purpose = purpose.as_str(),
            component = "model_gateway"
        )
    }

    pub fn record_incident(&self, outcome: IncidentOutcome) {
        self.metrics.record_incident(outcome);
    }

    pub fn record_evidence_query(&self, source: EvidenceSourceLabel, result: ResultClass, elapsed: Duration) {
        self.metrics.record_evidence_query(source, result, elapsed);
    }

    pub fn record_diagnostic(&self, pack: DiagnosticPackLabel, result: ResultClass, elapsed: Duration) {
        self.metrics.record_diagnostic(pack, result, elapsed);
    }

    pub fn record_model_request(
        &self,
        provider: ProviderFamilyLabel,
        purpose: ModelPurposeLabel,
        result: ResultClass,
        elapsed: Duration,
    ) {
        self.metrics.record_model_request(provider, purpose, result, elapsed);
    }

    pub fn record_model_tokens(&self, provider: ProviderFamilyLabel, direction: ModelTokenDirection, tokens: u64) {
        self.metrics.record_model_tokens(provider, direction, tokens);
    }

    pub fn record_model_cost_microusd(&self, provider: ProviderFamilyLabel, cost_microusd: u64) {
        self.metrics.record_model_cost_microusd(provider, cost_microusd);
    }

    pub fn record_tool_call(&self, tool_class: ToolClassLabel, result: ResultClass) {
        self.metrics.record_tool_call(tool_class, result);
    }
}

impl Default for SreObservability {
    fn default() -> Self {
        Self::new(process_metrics())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_span_names_are_stable() {
        assert_eq!(SPAN_INCIDENT_RUN, "sre.incident.run");
        assert_eq!(SPAN_EVIDENCE_COLLECT, "sre.evidence.collect");
        assert_eq!(SPAN_DIAGNOSTIC_EVALUATE, "sre.diagnostic.evaluate");
        assert_eq!(SPAN_MODEL_INVOKE, "sre.model.invoke");
    }

    #[test]
    fn model_services_and_prometheus_exporter_share_process_metrics() {
        let first = process_metrics();
        let second = process_metrics();

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn spans_only_accept_bounded_fields_and_correlation_id() {
        let telemetry = SreObservability::default();
        let correlation = CorrelationContext::default();

        let _incident = telemetry.incident_run_span(correlation);
        let _evidence = telemetry.evidence_collect_span(correlation, EvidenceSourceLabel::Mcp);
        let _diagnostic = telemetry.diagnostic_evaluate_span(correlation, DiagnosticPackLabel::ConsumerLag);
        let _model =
            telemetry.model_invoke_span(correlation, ProviderFamilyLabel::DeepSeek, ModelPurposeLabel::Diagnosis);
    }
}
