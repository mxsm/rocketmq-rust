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

use std::collections::BTreeSet;

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::EvidenceExposure;
use rocketmq_sre_contracts::REQUIRED_SIGNALS_EVIDENCE_SCHEMA_VERSION;
use rocketmq_sre_contracts::RequiredSignalObservation;
use rocketmq_sre_contracts::RequiredSignalStatus;
use rocketmq_sre_contracts::RequiredSignalType;
use rocketmq_sre_contracts::RequiredSignalsEvidenceV1;
use serde::Deserialize;
use serde_json::Value;

use super::common::CancelSignal;
use super::common::SourceOutput;
use super::loki::LokiSource;
use super::mcp::McpSource;
use super::prometheus::PrometheusSource;
use super::runtime_diagnostics::RuntimeDiagnosticsSource;
use super::tempo::TempoSource;
use crate::ConnectorError;
use crate::ConnectorErrorCode;
use crate::mcp::McpGateway;

const MANIFEST_SCHEMA_VERSION: &str = "rocketmq.sre.required-signals.v1";
const BROKER_MANIFEST: &str = include_str!("../../../../config/observability/required-signals/broker.yaml");
const NAMESERVER_MANIFEST: &str = include_str!("../../../../config/observability/required-signals/nameserver.yaml");
const CONTROLLER_MANIFEST: &str = include_str!("../../../../config/observability/required-signals/controller.yaml");
const PROXY_MANIFEST: &str = include_str!("../../../../config/observability/required-signals/proxy.yaml");
const MCP_MANIFEST: &str = include_str!("../../../../config/observability/required-signals/mcp.yaml");
const RUNTIME_MANIFEST: &str = include_str!("../../../../config/observability/required-signals/runtime.yaml");

#[derive(Debug, Deserialize)]
struct RequiredSignalManifest {
    schema_version: String,
    component: String,
    signals: Vec<ManifestSignal>,
}

#[derive(Clone, Debug, Deserialize)]
struct ManifestSignal {
    requirement_id: String,
    registry_reference: String,
    signal_type: RequiredSignalType,
    status: String,
    #[serde(default)]
    query_resource: Option<String>,
}

#[derive(Clone, Debug)]
enum FixedQuery {
    Prometheus(String),
    Loki(String),
    Tempo(String),
    Runtime(String),
    NotProductionVerified(&'static str),
}

impl FixedQuery {
    const fn source(&self) -> &'static str {
        match self {
            Self::Prometheus(_) => "prometheus",
            Self::Loki(_) => "loki",
            Self::Tempo(_) => "tempo",
            Self::Runtime(_) => "runtime",
            Self::NotProductionVerified(_) => "manifest",
        }
    }
}

#[derive(Clone, Debug)]
enum CachedRead {
    Output(SourceOutput),
    Missing {
        source: &'static str,
        reason_code: &'static str,
    },
}

#[derive(Default)]
struct SharedReads {
    logs: Option<CachedRead>,
    spans: Option<CachedRead>,
}

pub(super) struct RequiredSignalsSource;

impl RequiredSignalsSource {
    #[allow(
        clippy::too_many_arguments,
        reason = "the composite source keeps every backend and security bound explicit"
    )]
    pub(super) async fn query<G>(
        prometheus: &PrometheusSource,
        loki: &LokiSource,
        tempo: &TempoSource,
        mcp: &McpSource<G>,
        runtime: &RuntimeDiagnosticsSource,
        external_cluster: &str,
        resource: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        max_rows: usize,
        max_bytes: usize,
        deadline: DateTime<Utc>,
        cancel: &CancelSignal,
    ) -> Result<SourceOutput, ConnectorError>
    where
        G: McpGateway,
    {
        let component = normalize_component(resource)?;
        let manifest = parse_manifest(component, manifest_source(component)?)?;
        let mut shared = SharedReads::default();
        let mut observations = Vec::with_capacity(manifest.signals.len());

        for signal in &manifest.signals {
            let query = fixed_query(&manifest.component, signal)?;
            let observation = match &query {
                FixedQuery::NotProductionVerified(reason_code) => {
                    not_production_verified(signal, query.source(), reason_code)
                }
                FixedQuery::Prometheus(metric_resource) => {
                    let read = normalize_read(
                        "prometheus",
                        prometheus
                            .query(
                                external_cluster,
                                metric_resource,
                                start,
                                end,
                                max_rows,
                                max_bytes,
                                deadline,
                                cancel,
                            )
                            .await,
                    )?;
                    observation_from_read(signal, read)
                }
                FixedQuery::Loki(log_resource) => {
                    if shared.logs.is_none() {
                        shared.logs = Some(normalize_read(
                            "loki",
                            loki.query(
                                external_cluster,
                                log_resource,
                                start,
                                end,
                                max_rows,
                                max_bytes,
                                deadline,
                                cancel,
                            )
                            .await,
                        )?);
                    }
                    let read = shared
                        .logs
                        .clone()
                        .ok_or_else(|| ConnectorError::source("Required Signals log read was not initialized"))?;
                    observation_from_read(signal, read)
                }
                FixedQuery::Tempo(trace_resource) => {
                    if shared.spans.is_none() {
                        shared.spans = Some(normalize_read(
                            "tempo",
                            tempo
                                .query(
                                    external_cluster,
                                    trace_resource,
                                    start,
                                    end,
                                    max_rows,
                                    max_bytes,
                                    deadline,
                                    cancel,
                                )
                                .await,
                        )?);
                    }
                    let read = shared
                        .spans
                        .clone()
                        .ok_or_else(|| ConnectorError::source("Required Signals span read was not initialized"))?;
                    observation_from_read(signal, read)
                }
                FixedQuery::Runtime(runtime_resource) => {
                    let read = normalize_read("runtime", runtime.query(mcp, runtime_resource, deadline, cancel).await)?;
                    observation_from_read(signal, read)
                }
            };
            observations.push(observation);
        }

        aggregate(manifest.component, observations)
    }
}

fn normalize_component(resource: &str) -> Result<&'static str, ConnectorError> {
    let component = resource
        .strip_prefix("component/")
        .or_else(|| resource.strip_prefix("required-signals/"))
        .unwrap_or(resource);
    match component {
        "broker" => Ok("broker"),
        "nameserver" | "name_server" | "namesrv" => Ok("nameserver"),
        "controller" => Ok("controller"),
        "proxy" => Ok("proxy"),
        "mcp" => Ok("mcp"),
        "runtime" => Ok("runtime"),
        _ => Err(ConnectorError::new(
            ConnectorErrorCode::InvalidEvidenceQuery,
            false,
            "Required Signals component is not registered",
        )),
    }
}

fn manifest_source(component: &str) -> Result<&'static str, ConnectorError> {
    match component {
        "broker" => Ok(BROKER_MANIFEST),
        "nameserver" => Ok(NAMESERVER_MANIFEST),
        "controller" => Ok(CONTROLLER_MANIFEST),
        "proxy" => Ok(PROXY_MANIFEST),
        "mcp" => Ok(MCP_MANIFEST),
        "runtime" => Ok(RUNTIME_MANIFEST),
        _ => Err(ConnectorError::new(
            ConnectorErrorCode::InvalidEvidenceQuery,
            false,
            "Required Signals component is not registered",
        )),
    }
}

fn parse_manifest(expected_component: &str, input: &str) -> Result<RequiredSignalManifest, ConnectorError> {
    let manifest: RequiredSignalManifest = serde_yaml::from_str(input).map_err(|_| {
        ConnectorError::capability(
            ConnectorErrorCode::CapabilityMismatch,
            "Required Signals manifest is invalid",
        )
    })?;
    let component_matches = manifest.component == expected_component
        || (expected_component == "nameserver" && manifest.component == "name_server");
    if manifest.schema_version != MANIFEST_SCHEMA_VERSION || !component_matches || manifest.signals.is_empty() {
        return Err(ConnectorError::capability(
            ConnectorErrorCode::CapabilityMismatch,
            "Required Signals manifest identity is incompatible",
        ));
    }

    let mut ids = BTreeSet::new();
    if manifest.signals.iter().any(|signal| {
        signal.requirement_id.trim().is_empty()
            || signal.registry_reference.trim().is_empty()
            || !ids.insert(signal.requirement_id.as_str())
    }) {
        return Err(ConnectorError::capability(
            ConnectorErrorCode::CapabilityMismatch,
            "Required Signals manifest contains an invalid requirement",
        ));
    }
    Ok(manifest)
}

fn fixed_query(component: &str, signal: &ManifestSignal) -> Result<FixedQuery, ConnectorError> {
    match signal.status.as_str() {
        "missing_instrumentation" => {
            return Ok(FixedQuery::NotProductionVerified(
                "required_signal_instrumentation_missing",
            ));
        }
        "in_process_only" => {
            return Ok(FixedQuery::NotProductionVerified(
                "required_signal_remote_adapter_missing",
            ));
        }
        "existing" | "queryable" => {}
        _ => {
            return Err(ConnectorError::capability(
                ConnectorErrorCode::CapabilityMismatch,
                "Required Signal status is unsupported",
            ));
        }
    }

    let service_name = service_name(component)?;
    match signal.signal_type {
        RequiredSignalType::Metric => {
            let resource = signal
                .query_resource
                .clone()
                .unwrap_or_else(|| format!("metrics/{}", signal.registry_reference));
            if !resource.starts_with("metrics/") {
                return Err(ConnectorError::capability(
                    ConnectorErrorCode::CapabilityMismatch,
                    "Required Signal metric route is not a fixed metric resource",
                ));
            }
            Ok(FixedQuery::Prometheus(resource))
        }
        RequiredSignalType::Log => Ok(FixedQuery::Loki(format!("logs/{service_name}"))),
        RequiredSignalType::Span => Ok(FixedQuery::Tempo(format!("traces/service/{service_name}"))),
        RequiredSignalType::Resource => match signal.registry_reference.as_str() {
            "rocketmq://system/runtime/v1" => Ok(FixedQuery::Runtime("runtime".to_owned())),
            "rocketmq.runtime-diagnostics.v1" if component == "runtime" => {
                Ok(FixedQuery::Runtime("runtime/components".to_owned()))
            }
            "rocketmq.runtime-diagnostics.v1" => Ok(FixedQuery::Runtime(format!("runtime/{component}"))),
            "rocketmq://system/observability/v1" => Ok(FixedQuery::Runtime("observability".to_owned())),
            _ => Ok(FixedQuery::NotProductionVerified(
                "required_signal_resource_adapter_missing",
            )),
        },
    }
}

fn service_name(component: &str) -> Result<&'static str, ConnectorError> {
    match component {
        "broker" => Ok("rocketmq-broker"),
        "name_server" | "nameserver" => Ok("rocketmq-namesrv"),
        "controller" => Ok("rocketmq-controller"),
        "proxy" => Ok("rocketmq-proxy"),
        "mcp" => Ok("rocketmq-mcp"),
        "runtime" => Ok("rocketmq-mcp"),
        _ => Err(ConnectorError::capability(
            ConnectorErrorCode::CapabilityMismatch,
            "Required Signals manifest component has no bounded service identity",
        )),
    }
}

fn normalize_read(
    source: &'static str,
    result: Result<SourceOutput, ConnectorError>,
) -> Result<CachedRead, ConnectorError> {
    match result {
        Ok(output) => Ok(CachedRead::Output(output)),
        Err(error) if error.code == ConnectorErrorCode::SourceUnavailable => Ok(CachedRead::Missing {
            source,
            reason_code: "required_signal_source_unavailable",
        }),
        Err(error) => Err(error),
    }
}

fn observation_from_read(signal: &ManifestSignal, read: CachedRead) -> RequiredSignalObservation {
    match read {
        CachedRead::Missing { source, reason_code } => missing(signal, source, reason_code),
        CachedRead::Output(output) => match output.coverage {
            CoverageStatus::Missing => missing(
                signal,
                source_for_type(signal.signal_type),
                "required_signal_source_missing",
            ),
            CoverageStatus::NotProductionVerified => not_production_verified(
                signal,
                source_for_type(signal.signal_type),
                "required_signal_not_production_verified",
            ),
            CoverageStatus::Available | CoverageStatus::Partial
                if has_measurement(signal.signal_type, &output.content) =>
            {
                RequiredSignalObservation {
                    requirement_id: signal.requirement_id.clone(),
                    registry_reference: signal.registry_reference.clone(),
                    signal_type: signal.signal_type,
                    query_source: source_for_type(signal.signal_type).to_owned(),
                    status: RequiredSignalStatus::Available,
                    observed_at: Some(output.observed_at),
                    partial: output.partial || output.coverage == CoverageStatus::Partial,
                    warnings: output.warnings,
                    content: Some(output.content),
                    reason_code: None,
                }
            }
            CoverageStatus::Available | CoverageStatus::Partial => missing(
                signal,
                source_for_type(signal.signal_type),
                "required_signal_result_empty",
            ),
        },
    }
}

fn source_for_type(signal_type: RequiredSignalType) -> &'static str {
    match signal_type {
        RequiredSignalType::Metric => "prometheus",
        RequiredSignalType::Log => "loki",
        RequiredSignalType::Span => "tempo",
        RequiredSignalType::Resource => "runtime",
    }
}

fn has_measurement(signal_type: RequiredSignalType, content: &Value) -> bool {
    match signal_type {
        RequiredSignalType::Metric => content.get("series").and_then(Value::as_array).is_some_and(|series| {
            series.iter().any(|item| {
                item.get("samples")
                    .and_then(Value::as_array)
                    .is_some_and(|samples| !samples.is_empty())
            })
        }),
        RequiredSignalType::Log => content
            .pointer("/data/result")
            .and_then(Value::as_array)
            .is_some_and(|streams| {
                streams.iter().any(|stream| {
                    stream
                        .get("values")
                        .and_then(Value::as_array)
                        .is_some_and(|values| !values.is_empty())
                })
            }),
        RequiredSignalType::Span => content
            .get("traces")
            .and_then(Value::as_array)
            .is_some_and(|traces| !traces.is_empty()),
        RequiredSignalType::Resource => true,
    }
}

fn missing(signal: &ManifestSignal, source: &'static str, reason_code: &'static str) -> RequiredSignalObservation {
    RequiredSignalObservation {
        requirement_id: signal.requirement_id.clone(),
        registry_reference: signal.registry_reference.clone(),
        signal_type: signal.signal_type,
        query_source: source.to_owned(),
        status: RequiredSignalStatus::Missing,
        observed_at: None,
        partial: false,
        warnings: Vec::new(),
        content: None,
        reason_code: Some(reason_code.to_owned()),
    }
}

fn not_production_verified(
    signal: &ManifestSignal,
    source: &'static str,
    reason_code: &'static str,
) -> RequiredSignalObservation {
    RequiredSignalObservation {
        requirement_id: signal.requirement_id.clone(),
        registry_reference: signal.registry_reference.clone(),
        signal_type: signal.signal_type,
        query_source: source.to_owned(),
        status: RequiredSignalStatus::NotProductionVerified,
        observed_at: None,
        partial: false,
        warnings: Vec::new(),
        content: None,
        reason_code: Some(reason_code.to_owned()),
    }
}

fn aggregate(component: String, observations: Vec<RequiredSignalObservation>) -> Result<SourceOutput, ConnectorError> {
    let available = observations
        .iter()
        .filter(|observation| observation.status == RequiredSignalStatus::Available)
        .count();
    let missing = observations
        .iter()
        .filter(|observation| observation.status == RequiredSignalStatus::Missing)
        .count();
    let unverified = observations.len().saturating_sub(available + missing);
    let observed_at = observations
        .iter()
        .filter_map(|observation| observation.observed_at)
        .max()
        .unwrap_or_else(Utc::now);
    let partial = available != observations.len() || observations.iter().any(|observation| observation.partial);
    let evidence = RequiredSignalsEvidenceV1 {
        schema_version: REQUIRED_SIGNALS_EVIDENCE_SCHEMA_VERSION.to_owned(),
        component,
        observed_at,
        partial,
        observations,
    };
    evidence.validate().map_err(|_| {
        ConnectorError::capability(
            ConnectorErrorCode::CapabilityMismatch,
            "Required Signals evidence violates its public contract",
        )
    })?;
    let content = serde_json::to_value(evidence)
        .map_err(|_| ConnectorError::source("Required Signals evidence cannot be encoded"))?;
    let mut output = SourceOutput::available(content, observed_at).with_exposure(EvidenceExposure::RequiredSignals);
    output.partial = partial;
    output.coverage = if available == 0 && missing > 0 {
        CoverageStatus::Missing
    } else if available == 0 && unverified > 0 {
        CoverageStatus::NotProductionVerified
    } else if partial {
        CoverageStatus::Partial
    } else {
        CoverageStatus::Available
    };
    if missing > 0 {
        output.warnings.push("required_signals_missing".to_owned());
    }
    if unverified > 0 {
        output
            .warnings
            .push("required_signals_not_production_verified".to_owned());
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn embedded_manifests_have_fixed_fail_closed_routes() {
        for component in ["broker", "nameserver", "controller", "proxy", "mcp", "runtime"] {
            let manifest = parse_manifest(component, manifest_source(component).expect("registered manifest"))
                .expect("valid Required Signals manifest");
            assert!(!manifest.signals.is_empty());
            for signal in &manifest.signals {
                let query =
                    fixed_query(&manifest.component, signal).expect("manifest signal must resolve deterministically");
                match query {
                    FixedQuery::Prometheus(resource) => {
                        assert!(resource.starts_with("metrics/"));
                    }
                    FixedQuery::Loki(resource) => {
                        assert!(resource.starts_with("logs/rocketmq-"));
                    }
                    FixedQuery::Tempo(resource) => {
                        assert!(resource.starts_with("traces/service/rocketmq-"));
                    }
                    FixedQuery::Runtime(resource) => {
                        assert!(matches!(
                            resource.as_str(),
                            "runtime" | "runtime/components" | "observability"
                        ));
                    }
                    FixedQuery::NotProductionVerified(reason_code) => {
                        assert!(!reason_code.is_empty());
                    }
                }
            }
        }
    }

    #[test]
    fn empty_metric_series_is_missing_and_never_fabricated_as_zero() {
        let signal = ManifestSignal {
            requirement_id: "broker.availability".to_owned(),
            registry_reference: "rocketmq_broker_up".to_owned(),
            signal_type: RequiredSignalType::Metric,
            status: "existing".to_owned(),
            query_resource: None,
        };
        let output = SourceOutput::available(
            json!({
                "schema_version": "rocketmq.prometheus-evidence.v1",
                "series": []
            }),
            Utc::now(),
        );

        let observation = observation_from_read(&signal, CachedRead::Output(output));

        assert_eq!(observation.status, RequiredSignalStatus::Missing);
        assert!(observation.content.is_none());
        assert_eq!(observation.reason_code.as_deref(), Some("required_signal_result_empty"));
    }

    #[test]
    fn aggregate_preserves_explicit_missing_and_unverified_status() {
        let metric = ManifestSignal {
            requirement_id: "broker.availability".to_owned(),
            registry_reference: "rocketmq_broker_up".to_owned(),
            signal_type: RequiredSignalType::Metric,
            status: "existing".to_owned(),
            query_resource: None,
        };
        let lifecycle = ManifestSignal {
            requirement_id: "broker.lifecycle".to_owned(),
            registry_reference: "rocketmq.broker.lifecycle".to_owned(),
            signal_type: RequiredSignalType::Log,
            status: "existing".to_owned(),
            query_resource: None,
        };
        let output = aggregate(
            "broker".to_owned(),
            vec![
                missing(&metric, "prometheus", "required_signal_result_empty"),
                not_production_verified(&lifecycle, "loki", "required_signal_not_production_verified"),
            ],
        )
        .expect("aggregate evidence");

        assert_eq!(output.coverage, CoverageStatus::Missing);
        assert!(output.partial);
        assert!(output.content.to_string().contains("\"content\":null"));
        assert!(!output.content.to_string().contains("\"value\":0"));
    }
}
