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

use std::fmt::Write as _;
use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

const LATENCY_BUCKETS_MS: [u64; 10] = [1, 5, 10, 25, 50, 100, 250, 500, 1_000, 5_000];

trait BoundedLabel: Copy {
    const COUNT: usize;

    fn index(self) -> usize;
    fn as_str(self) -> &'static str;
    fn all() -> &'static [Self];
}

macro_rules! bounded_label {
    (
        $(#[$meta:meta])*
        $visibility:vis enum $name:ident {
            $($variant:ident => $value:literal),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        $visibility enum $name {
            $($variant),+
        }

        impl $name {
            $visibility const ALL: &'static [Self] = &[$(Self::$variant),+];

            #[must_use]
            $visibility const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value),+
                }
            }
        }

        impl BoundedLabel for $name {
            const COUNT: usize = [$(Self::$variant),+].len();

            fn index(self) -> usize {
                Self::ALL
                    .iter()
                    .position(|candidate| *candidate == self)
                    .unwrap_or(Self::COUNT - 1)
            }

            fn as_str(self) -> &'static str {
                self.as_str()
            }

            fn all() -> &'static [Self] {
                Self::ALL
            }
        }
    };
}

bounded_label! {
    /// Finite incident lifecycle outcomes used by the incident counters.
    pub enum IncidentOutcome {
        Started => "started",
        Completed => "completed",
        Failed => "failed",
        RulesOnly => "rules_only",
        Cancelled => "cancelled",
    }
}

impl IncidentOutcome {
    const fn closes_active_run(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::RulesOnly | Self::Cancelled)
    }
}

bounded_label! {
    /// Canonical evidence source families. Unknown source IDs collapse to
    /// `other`; the original string is never retained.
    pub enum EvidenceSourceLabel {
        Mcp => "mcp",
        AdminQuery => "admin_query",
        Prometheus => "prometheus",
        Alertmanager => "alertmanager",
        Loki => "loki",
        Tempo => "tempo",
        Kubernetes => "kubernetes",
        Runtime => "runtime",
        Topology => "topology",
        Other => "other",
    }
}

impl EvidenceSourceLabel {
    #[must_use]
    pub fn from_source_id(value: &str) -> Self {
        match value {
            "mcp" | "rocketmq-mcp" => Self::Mcp,
            "admin-query" | "admin_query" | "admin-read" => Self::AdminQuery,
            "prometheus" => Self::Prometheus,
            "alertmanager" => Self::Alertmanager,
            "loki" => Self::Loki,
            "tempo" => Self::Tempo,
            "kubernetes" => Self::Kubernetes,
            "runtime" => Self::Runtime,
            "topology" => Self::Topology,
            _ => Self::Other,
        }
    }
}

bounded_label! {
    /// Phase 1 diagnostic pack labels. Arbitrary pack IDs never become metric
    /// labels.
    pub enum DiagnosticPackLabel {
        ConsumerLag => "consumer_lag",
        ConsumerRuntime => "consumer_runtime",
        ProducerConnectivity => "producer_connectivity",
        BrokerHealth => "broker_health",
        MessagePath => "message_path",
        TelemetryPipeline => "telemetry_pipeline",
        DeploymentDrift => "deployment_drift",
        ClusterTopology => "cluster_topology",
        Other => "other",
    }
}

impl DiagnosticPackLabel {
    #[must_use]
    pub fn from_pack_id(value: &str) -> Self {
        match value {
            "consumer-lag.v2" | "consumer_lag" => Self::ConsumerLag,
            "consumer-runtime.v1" | "consumer_runtime" => Self::ConsumerRuntime,
            "producer-connectivity.v1" | "producer_connectivity" => Self::ProducerConnectivity,
            "broker-health.v1" | "broker_health" => Self::BrokerHealth,
            "message-path.v1" | "message_path" => Self::MessagePath,
            "telemetry-pipeline.v1" | "telemetry_pipeline" => Self::TelemetryPipeline,
            "deployment-drift.v1" | "deployment_drift" => Self::DeploymentDrift,
            "cluster-topology.v1" | "cluster_topology" => Self::ClusterTopology,
            _ => Self::Other,
        }
    }
}

bounded_label! {
    /// Provider protocol families supported by the model gateway.
    pub enum ProviderFamilyLabel {
        OpenAiCompatible => "openai_compatible",
        Anthropic => "anthropic",
        Gemini => "gemini",
        Bedrock => "bedrock",
        DeepSeek => "deepseek",
        ZhipuGlm => "zhipu_glm",
        MoonshotKimi => "moonshot_kimi",
        Local => "local",
        Spi => "spi",
        Other => "other",
    }
}

impl ProviderFamilyLabel {
    #[must_use]
    pub fn from_provider_id(value: &str) -> Self {
        match value {
            "openai" | "openai-compatible" | "openai_compatible" => Self::OpenAiCompatible,
            "anthropic" => Self::Anthropic,
            "gemini" => Self::Gemini,
            "bedrock" => Self::Bedrock,
            "deepseek" => Self::DeepSeek,
            "zhipu" | "zhipu-glm" | "zhipu_glm" => Self::ZhipuGlm,
            "kimi" | "moonshot" | "moonshot-kimi" | "moonshot_kimi" => Self::MoonshotKimi,
            "local" => Self::Local,
            "spi" => Self::Spi,
            _ => Self::Other,
        }
    }
}

bounded_label! {
    /// Stable model invocation purposes.
    pub enum ModelPurposeLabel {
        Diagnosis => "diagnosis",
        Critic => "critic",
        Retrieval => "retrieval",
        Summarization => "summarization",
        Classification => "classification",
        Other => "other",
    }
}

bounded_label! {
    /// Model token direction.
    pub enum ModelTokenDirection {
        Input => "input",
        Output => "output",
    }
}

bounded_label! {
    /// Stable, transport-independent result classes.
    pub enum ResultClass {
        Success => "success",
        Timeout => "timeout",
        RateLimited => "rate_limited",
        Unauthorized => "unauthorized",
        Unavailable => "unavailable",
        InvalidResponse => "invalid_response",
        Cancelled => "cancelled",
        OtherError => "other_error",
    }
}

impl ResultClass {
    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(self, Self::Success)
    }
}

bounded_label! {
    /// Tool categories rather than concrete tool names or arguments.
    pub enum ToolClassLabel {
        Cluster => "cluster",
        Topic => "topic",
        Broker => "broker",
        Consumer => "consumer",
        MessageMetadata => "message_metadata",
        Runtime => "runtime",
        Observability => "observability",
        Other => "other",
    }
}

/// Injectable metric contract for SRE domain services.
pub trait SreMetricSink: Send + Sync {
    fn record_incident(&self, outcome: IncidentOutcome);
    fn record_evidence_query(&self, source: EvidenceSourceLabel, result: ResultClass, elapsed: Duration);
    fn record_diagnostic(&self, pack: DiagnosticPackLabel, result: ResultClass, elapsed: Duration);
    fn record_model_request(
        &self,
        provider: ProviderFamilyLabel,
        purpose: ModelPurposeLabel,
        result: ResultClass,
        elapsed: Duration,
    );
    fn record_model_tokens(&self, provider: ProviderFamilyLabel, direction: ModelTokenDirection, tokens: u64);
    fn record_model_cost_microusd(&self, provider: ProviderFamilyLabel, cost_microusd: u64);
    fn record_tool_call(&self, tool_class: ToolClassLabel, result: ResultClass);
}

/// Dependency-free bounded collector with a Prometheus text projection.
///
/// The collector can be injected now and later bridged to the meter created by
/// `rocketmq-observability` without changing domain service call sites.
pub struct SreMetrics {
    incidents: CounterVec<IncidentOutcome>,
    incidents_active: AtomicU64,
    evidence_queries: CounterMatrix<EvidenceSourceLabel, ResultClass>,
    evidence_errors: CounterMatrix<EvidenceSourceLabel, ResultClass>,
    evidence_latency: HistogramVec<EvidenceSourceLabel>,
    diagnostic_evaluations: CounterMatrix<DiagnosticPackLabel, ResultClass>,
    diagnostic_latency: HistogramVec<DiagnosticPackLabel>,
    model_requests: Counter3<ProviderFamilyLabel, ModelPurposeLabel, ResultClass>,
    model_errors: CounterMatrix<ProviderFamilyLabel, ResultClass>,
    model_latency: HistogramVec<ProviderFamilyLabel>,
    model_tokens: CounterMatrix<ProviderFamilyLabel, ModelTokenDirection>,
    model_cost_microusd: CounterVec<ProviderFamilyLabel>,
    tool_calls: CounterMatrix<ToolClassLabel, ResultClass>,
}

impl SreMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self {
            incidents: CounterVec::new(),
            incidents_active: AtomicU64::new(0),
            evidence_queries: CounterMatrix::new(),
            evidence_errors: CounterMatrix::new(),
            evidence_latency: HistogramVec::new(),
            diagnostic_evaluations: CounterMatrix::new(),
            diagnostic_latency: HistogramVec::new(),
            model_requests: Counter3::new(),
            model_errors: CounterMatrix::new(),
            model_latency: HistogramVec::new(),
            model_tokens: CounterMatrix::new(),
            model_cost_microusd: CounterVec::new(),
            tool_calls: CounterMatrix::new(),
        }
    }

    /// Returns a Prometheus text exposition with only finite label values.
    #[must_use]
    pub fn render_prometheus(&self) -> String {
        let mut output = String::with_capacity(32 * 1024);
        write_counter_vec(
            &mut output,
            "rocketmq_sre_incidents_total",
            "SRE incident runs by bounded outcome.",
            "outcome",
            &self.incidents,
        );
        write_metric_header(
            &mut output,
            "rocketmq_sre_incidents_active",
            "Current SRE incident runs.",
            "gauge",
        );
        let _ = writeln!(
            output,
            "rocketmq_sre_incidents_active {}",
            self.incidents_active.load(Ordering::Relaxed)
        );
        write_counter_matrix(
            &mut output,
            "rocketmq_sre_evidence_queries_total",
            "Evidence collection queries.",
            ("source", "result"),
            &self.evidence_queries,
        );
        write_counter_matrix(
            &mut output,
            "rocketmq_sre_evidence_query_errors_total",
            "Evidence collection errors.",
            ("source", "result"),
            &self.evidence_errors,
        );
        write_histogram_vec(
            &mut output,
            "rocketmq_sre_evidence_query_duration_seconds",
            "Evidence query duration.",
            "source",
            &self.evidence_latency,
        );
        write_counter_matrix(
            &mut output,
            "rocketmq_sre_diagnostic_evaluations_total",
            "Diagnostic pack evaluations.",
            ("pack", "result"),
            &self.diagnostic_evaluations,
        );
        write_histogram_vec(
            &mut output,
            "rocketmq_sre_diagnostic_evaluation_duration_seconds",
            "Diagnostic pack evaluation duration.",
            "pack",
            &self.diagnostic_latency,
        );
        write_counter3(
            &mut output,
            "rocketmq_sre_model_requests_total",
            "Model requests.",
            ("provider", "purpose", "result"),
            &self.model_requests,
        );
        write_counter_matrix(
            &mut output,
            "rocketmq_sre_model_errors_total",
            "Model request errors.",
            ("provider", "result"),
            &self.model_errors,
        );
        write_histogram_vec(
            &mut output,
            "rocketmq_sre_model_request_duration_seconds",
            "Model request duration.",
            "provider",
            &self.model_latency,
        );
        write_counter_matrix(
            &mut output,
            "rocketmq_sre_model_tokens_total",
            "Model tokens by direction.",
            ("provider", "direction"),
            &self.model_tokens,
        );
        write_counter_vec(
            &mut output,
            "rocketmq_sre_model_cost_microusd_total",
            "Estimated model cost in micro US dollars.",
            "provider",
            &self.model_cost_microusd,
        );
        write_counter_matrix(
            &mut output,
            "rocketmq_sre_tool_calls_total",
            "Read-only tool calls by category.",
            ("tool_class", "result"),
            &self.tool_calls,
        );
        output
    }

    #[cfg(test)]
    fn active_incidents(&self) -> u64 {
        self.incidents_active.load(Ordering::Relaxed)
    }
}

impl Default for SreMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl SreMetricSink for SreMetrics {
    fn record_incident(&self, outcome: IncidentOutcome) {
        self.incidents.add(outcome, 1);
        if outcome == IncidentOutcome::Started {
            self.incidents_active.fetch_add(1, Ordering::Relaxed);
        } else if outcome.closes_active_run() {
            let _ = self
                .incidents_active
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(current.saturating_sub(1))
                });
        }
    }

    fn record_evidence_query(&self, source: EvidenceSourceLabel, result: ResultClass, elapsed: Duration) {
        self.evidence_queries.add(source, result, 1);
        if result.is_error() {
            self.evidence_errors.add(source, result, 1);
        }
        self.evidence_latency.observe(source, elapsed);
    }

    fn record_diagnostic(&self, pack: DiagnosticPackLabel, result: ResultClass, elapsed: Duration) {
        self.diagnostic_evaluations.add(pack, result, 1);
        self.diagnostic_latency.observe(pack, elapsed);
    }

    fn record_model_request(
        &self,
        provider: ProviderFamilyLabel,
        purpose: ModelPurposeLabel,
        result: ResultClass,
        elapsed: Duration,
    ) {
        self.model_requests.add(provider, purpose, result, 1);
        if result.is_error() {
            self.model_errors.add(provider, result, 1);
        }
        self.model_latency.observe(provider, elapsed);
    }

    fn record_model_tokens(&self, provider: ProviderFamilyLabel, direction: ModelTokenDirection, tokens: u64) {
        self.model_tokens.add(provider, direction, tokens);
    }

    fn record_model_cost_microusd(&self, provider: ProviderFamilyLabel, cost_microusd: u64) {
        self.model_cost_microusd.add(provider, cost_microusd);
    }

    fn record_tool_call(&self, tool_class: ToolClassLabel, result: ResultClass) {
        self.tool_calls.add(tool_class, result, 1);
    }
}

struct CounterVec<L> {
    values: Box<[AtomicU64]>,
    marker: PhantomData<L>,
}

impl<L: BoundedLabel + 'static> CounterVec<L> {
    fn new() -> Self {
        Self {
            values: atomic_values(L::COUNT),
            marker: PhantomData,
        }
    }

    fn add(&self, label: L, value: u64) {
        self.values[label.index()].fetch_add(value, Ordering::Relaxed);
    }

    fn get(&self, label: L) -> u64 {
        self.values[label.index()].load(Ordering::Relaxed)
    }
}

struct CounterMatrix<L, R> {
    values: Box<[AtomicU64]>,
    marker: PhantomData<(L, R)>,
}

impl<L: BoundedLabel + 'static, R: BoundedLabel + 'static> CounterMatrix<L, R> {
    fn new() -> Self {
        Self {
            values: atomic_values(L::COUNT * R::COUNT),
            marker: PhantomData,
        }
    }

    fn add(&self, left: L, right: R, value: u64) {
        let index = left.index() * R::COUNT + right.index();
        self.values[index].fetch_add(value, Ordering::Relaxed);
    }

    fn get(&self, left: L, right: R) -> u64 {
        let index = left.index() * R::COUNT + right.index();
        self.values[index].load(Ordering::Relaxed)
    }
}

struct Counter3<A, B, C> {
    values: Box<[AtomicU64]>,
    marker: PhantomData<(A, B, C)>,
}

impl<A: BoundedLabel + 'static, B: BoundedLabel + 'static, C: BoundedLabel + 'static> Counter3<A, B, C> {
    fn new() -> Self {
        Self {
            values: atomic_values(A::COUNT * B::COUNT * C::COUNT),
            marker: PhantomData,
        }
    }

    fn add(&self, first: A, second: B, third: C, value: u64) {
        let index = (first.index() * B::COUNT + second.index()) * C::COUNT + third.index();
        self.values[index].fetch_add(value, Ordering::Relaxed);
    }

    fn get(&self, first: A, second: B, third: C) -> u64 {
        let index = (first.index() * B::COUNT + second.index()) * C::COUNT + third.index();
        self.values[index].load(Ordering::Relaxed)
    }
}

struct HistogramVec<L> {
    bins: Box<[AtomicU64]>,
    counts: Box<[AtomicU64]>,
    sum_micros: Box<[AtomicU64]>,
    marker: PhantomData<L>,
}

impl<L: BoundedLabel + 'static> HistogramVec<L> {
    fn new() -> Self {
        Self {
            bins: atomic_values(L::COUNT * (LATENCY_BUCKETS_MS.len() + 1)),
            counts: atomic_values(L::COUNT),
            sum_micros: atomic_values(L::COUNT),
            marker: PhantomData,
        }
    }

    fn observe(&self, label: L, elapsed: Duration) {
        let millis = elapsed.as_millis().min(u128::from(u64::MAX)) as u64;
        let bin = LATENCY_BUCKETS_MS
            .iter()
            .position(|upper_bound| millis <= *upper_bound)
            .unwrap_or(LATENCY_BUCKETS_MS.len());
        let category = label.index();
        self.bins[category * (LATENCY_BUCKETS_MS.len() + 1) + bin].fetch_add(1, Ordering::Relaxed);
        self.counts[category].fetch_add(1, Ordering::Relaxed);
        let micros = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
        self.sum_micros[category].fetch_add(micros, Ordering::Relaxed);
    }

    fn bin(&self, label: L, bin: usize) -> u64 {
        self.bins[label.index() * (LATENCY_BUCKETS_MS.len() + 1) + bin].load(Ordering::Relaxed)
    }

    fn count(&self, label: L) -> u64 {
        self.counts[label.index()].load(Ordering::Relaxed)
    }

    fn sum_micros(&self, label: L) -> u64 {
        self.sum_micros[label.index()].load(Ordering::Relaxed)
    }
}

fn atomic_values(count: usize) -> Box<[AtomicU64]> {
    (0..count)
        .map(|_| AtomicU64::new(0))
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

fn write_metric_header(output: &mut String, name: &str, help: &str, metric_type: &str) {
    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} {metric_type}");
}

fn write_counter_vec<L: BoundedLabel + 'static>(
    output: &mut String,
    name: &str,
    help: &str,
    label_name: &str,
    counter: &CounterVec<L>,
) {
    write_metric_header(output, name, help, "counter");
    for label in L::all() {
        let _ = writeln!(
            output,
            "{name}{{{label_name}=\"{}\"}} {}",
            label.as_str(),
            counter.get(*label)
        );
    }
}

fn write_counter_matrix<L: BoundedLabel + 'static, R: BoundedLabel + 'static>(
    output: &mut String,
    name: &str,
    help: &str,
    label_names: (&str, &str),
    counter: &CounterMatrix<L, R>,
) {
    write_metric_header(output, name, help, "counter");
    for left in L::all() {
        for right in R::all() {
            let _ = writeln!(
                output,
                "{name}{{{}=\"{}\",{}=\"{}\"}} {}",
                label_names.0,
                left.as_str(),
                label_names.1,
                right.as_str(),
                counter.get(*left, *right)
            );
        }
    }
}

fn write_counter3<A: BoundedLabel + 'static, B: BoundedLabel + 'static, C: BoundedLabel + 'static>(
    output: &mut String,
    name: &str,
    help: &str,
    label_names: (&str, &str, &str),
    counter: &Counter3<A, B, C>,
) {
    write_metric_header(output, name, help, "counter");
    for first in A::all() {
        for second in B::all() {
            for third in C::all() {
                let _ = writeln!(
                    output,
                    "{name}{{{}=\"{}\",{}=\"{}\",{}=\"{}\"}} {}",
                    label_names.0,
                    first.as_str(),
                    label_names.1,
                    second.as_str(),
                    label_names.2,
                    third.as_str(),
                    counter.get(*first, *second, *third)
                );
            }
        }
    }
}

fn write_histogram_vec<L: BoundedLabel + 'static>(
    output: &mut String,
    name: &str,
    help: &str,
    label_name: &str,
    histogram: &HistogramVec<L>,
) {
    write_metric_header(output, name, help, "histogram");
    for label in L::all() {
        let mut cumulative = 0;
        for (bin, upper_bound_ms) in LATENCY_BUCKETS_MS.iter().enumerate() {
            cumulative += histogram.bin(*label, bin);
            let upper_bound_seconds = *upper_bound_ms as f64 / 1_000.0;
            let _ = writeln!(
                output,
                "{name}_bucket{{{label_name}=\"{}\",le=\"{upper_bound_seconds}\"}} {cumulative}",
                label.as_str()
            );
        }
        cumulative += histogram.bin(*label, LATENCY_BUCKETS_MS.len());
        let _ = writeln!(
            output,
            "{name}_bucket{{{label_name}=\"{}\",le=\"+Inf\"}} {cumulative}",
            label.as_str()
        );
        let _ = writeln!(
            output,
            "{name}_sum{{{label_name}=\"{}\"}} {}",
            label.as_str(),
            histogram.sum_micros(*label) as f64 / 1_000_000.0
        );
        let _ = writeln!(
            output,
            "{name}_count{{{label_name}=\"{}\"}} {}",
            label.as_str(),
            histogram.count(*label)
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_identifiers_collapse_to_other_without_retaining_input() {
        let secret_like = "cluster-a/token=super-secret";

        assert_eq!(
            EvidenceSourceLabel::from_source_id(secret_like),
            EvidenceSourceLabel::Other
        );
        assert_eq!(
            DiagnosticPackLabel::from_pack_id(secret_like),
            DiagnosticPackLabel::Other
        );
        assert_eq!(
            ProviderFamilyLabel::from_provider_id(secret_like),
            ProviderFamilyLabel::Other
        );
        assert_eq!(EvidenceSourceLabel::from_source_id(secret_like).as_str(), "other");
    }

    #[test]
    fn phase_two_source_ids_have_finite_canonical_labels() {
        assert_eq!(
            EvidenceSourceLabel::from_source_id("admin-query"),
            EvidenceSourceLabel::AdminQuery
        );
        assert_eq!(
            EvidenceSourceLabel::from_source_id("alertmanager"),
            EvidenceSourceLabel::Alertmanager
        );
    }

    #[test]
    fn metrics_have_finite_labels_and_no_sensitive_or_identity_values() {
        let metrics = SreMetrics::new();
        let secret_like = "cluster-a/token=super-secret";
        metrics.record_incident(IncidentOutcome::Started);
        metrics.record_evidence_query(
            EvidenceSourceLabel::from_source_id(secret_like),
            ResultClass::Unauthorized,
            Duration::from_millis(17),
        );
        metrics.record_diagnostic(
            DiagnosticPackLabel::from_pack_id(secret_like),
            ResultClass::Success,
            Duration::from_millis(3),
        );
        metrics.record_model_request(
            ProviderFamilyLabel::from_provider_id(secret_like),
            ModelPurposeLabel::Diagnosis,
            ResultClass::Unavailable,
            Duration::from_millis(23),
        );
        metrics.record_tool_call(ToolClassLabel::Consumer, ResultClass::Success);

        let rendered = metrics.render_prometheus();
        for forbidden in [
            secret_like,
            "super-secret",
            "incident_id=",
            "evidence_id=",
            "cluster_id=",
            "tenant_id=",
            "prompt=",
            "arguments=",
            "token=",
            "endpoint=",
        ] {
            assert!(!rendered.contains(forbidden), "rendered metric leaked `{forbidden}`");
        }
        assert!(rendered.contains("source=\"other\""));
        assert!(rendered.contains("provider=\"other\""));
        assert!(rendered.contains("pack=\"other\""));
    }

    #[test]
    fn error_and_latency_metrics_are_recorded_consistently() {
        let metrics = SreMetrics::new();
        metrics.record_evidence_query(
            EvidenceSourceLabel::Mcp,
            ResultClass::Timeout,
            Duration::from_millis(25),
        );
        metrics.record_model_tokens(ProviderFamilyLabel::DeepSeek, ModelTokenDirection::Input, 20);
        metrics.record_model_tokens(ProviderFamilyLabel::DeepSeek, ModelTokenDirection::Output, 7);
        metrics.record_model_cost_microusd(ProviderFamilyLabel::DeepSeek, 13);

        let rendered = metrics.render_prometheus();
        assert!(rendered.contains("rocketmq_sre_evidence_query_errors_total{source=\"mcp\",result=\"timeout\"} 1"));
        assert!(rendered.contains("rocketmq_sre_evidence_query_duration_seconds_count{source=\"mcp\"} 1"));
        assert!(rendered.contains("rocketmq_sre_model_tokens_total{provider=\"deepseek\",direction=\"input\"} 20"));
        assert!(rendered.contains("rocketmq_sre_model_cost_microusd_total{provider=\"deepseek\"} 13"));
    }

    #[test]
    fn active_incident_gauge_saturates_at_zero() {
        let metrics = SreMetrics::new();
        metrics.record_incident(IncidentOutcome::Completed);
        assert_eq!(metrics.active_incidents(), 0);
        metrics.record_incident(IncidentOutcome::Started);
        metrics.record_incident(IncidentOutcome::RulesOnly);
        assert_eq!(metrics.active_incidents(), 0);
    }
}
