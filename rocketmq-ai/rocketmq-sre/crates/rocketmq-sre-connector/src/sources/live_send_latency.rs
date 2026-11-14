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

//! Opt-in, isolated-source regression through the normal Connector composer.
//! The external fixture owns workload, scrape configuration and source faults.

use std::error::Error;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use rocketmq_runtime::{RuntimeConfig, RuntimeOwner};
use rocketmq_sre_contracts::{
    ClusterId, CorrelationId, CoverageStatus, EvidenceContent, EvidenceExposure, EvidenceQuery, EvidenceSnapshot,
    QueryId, TenantId, TimeRange,
};
use rocketmq_sre_core::diagnostics::{DiagnosticEngine, DiagnosticStatus, FindingOutcome, PackVersion, full_registry};
use serde_json::json;

use super::CancelSignal;
use crate::{ConnectorAdmissionOutcome, ConnectorAdmissionRejection, ConnectorConfig, ConnectorEngine, RmcpGateway};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const CLUSTER: &str = "Local-c03-latency";
const TOPIC: &str = "ProducerSimpleTest";
const RESOURCE: &str = "send-latency/ProducerSimpleTest";

/// Uses fixed canonical Prometheus collection and a CA-verified MCP activity
/// prerequisite. This test does not add a target mutation or arbitrary query API.
#[test]
#[ignore = "requires an explicitly configured isolated Broker, Prometheus and TLS MCP fixture"]
fn live_send_latency_uses_fixed_canonical_read_and_owned_shutdown() -> TestResult<()> {
    if std::env::var("ROCKETMQ_SRE_LIVE_SEND_LATENCY").as_deref() != Ok("1") {
        return Err(io::Error::other("live latency regression requires explicit opt-in").into());
    }
    let config = Arc::new(ConnectorConfig::from_env()?);
    if config.mcp_url.scheme() != "https"
        || config.mcp_ca_pem.is_empty()
        || config.cluster_allowlist.len() != 1
        || !config.cluster_allowlist.contains(CLUSTER)
        || config.prometheus_url.is_none()
        || config.control_plane.is_some()
    {
        return Err(
            io::Error::other("live fixture requires one isolated Prometheus cluster and verified MCP TLS").into(),
        );
    }
    let range = completed_window()?;
    let expected = expected_availability()?;
    let output = output_path()?;
    let mut runtime_config = RuntimeConfig::server_default("send-latency-source-regression");
    runtime_config.shutdown_timeout = config.shutdown_timeout;
    let owner = RuntimeOwner::plan(runtime_config)?.build()?;
    let context = owner.root_context().component("send-latency-source-regression");
    let captured = owner.block_on(Box::pin(async {
        let gateway = Arc::new(RmcpGateway::new(config.clone())?);
        let engine = Arc::new(ConnectorEngine::new(config.clone(), gateway)?);
        let result = Box::pin(async {
            let deadline = Utc::now() + Duration::from_std(config.request_timeout)?;
            let query = EvidenceQuery {
                query_id: QueryId::new(),
                correlation_id: CorrelationId::new(),
                tenant_id: config.tenant_id,
                cluster_id: config.cluster_ids[CLUSTER],
                source: "prometheus".to_owned(),
                resource: RESOURCE.to_owned(),
                time_range: range.clone(),
            };
            // Authorization rejection must not depend on a reachable source.
            let mut wrong_tenant = query.clone();
            wrong_tenant.tenant_id = TenantId::new();
            let tenant_rejection = Box::pin(engine.collect_contract_query(
                wrong_tenant,
                "isolated-latency-source-regression",
                deadline,
                &CancelSignal::default(),
            ))
            .await?;
            let mut wrong_cluster = query.clone();
            wrong_cluster.cluster_id = ClusterId::new();
            let cluster_rejection = Box::pin(engine.collect_contract_query(
                wrong_cluster,
                "isolated-latency-source-regression",
                deadline,
                &CancelSignal::default(),
            ))
            .await?;
            Box::pin(engine.initialize_sources(context.component("evidence-sources"))).await;
            if Box::pin(engine.reconcile()).await? != ConnectorAdmissionOutcome::Accepted(()) {
                return Err(io::Error::other("live MCP compatibility prerequisite was rejected").into());
            }
            let deadline = Utc::now() + Duration::from_std(config.request_timeout)?;
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let collecting = engine.clone();
            let (_, read) = context.task_group().spawn_service_with_handle(
                "send-latency-source-regression.fixed-read",
                Box::pin(async move {
                    let result = Box::pin(collecting.collect_contract_query(
                        query,
                        "isolated-latency-source-regression",
                        deadline,
                        &CancelSignal::default(),
                    ))
                    .await;
                    let _ = sender.send(result);
                }),
            )?;
            read.await?;
            let snapshot = match receiver.await?? {
                ConnectorAdmissionOutcome::Accepted(snapshot) => snapshot,
                ConnectorAdmissionOutcome::Rejected(_) => {
                    return Err(io::Error::other("fixed live latency source read was rejected").into());
                }
            };
            Ok::<_, Box<dyn Error + Send + Sync>>((snapshot, tenant_rejection, cluster_rejection))
        })
        .await;
        // Drain source/client ownership before assertions, including errors.
        context.task_group().cancel();
        Box::pin(engine.close()).await;
        result
    }));
    let shutdown = owner.shutdown_runtime_blocking();
    let (snapshot, tenant_rejection, cluster_rejection) = captured?;
    let shutdown = shutdown?;
    let registry = full_registry().map_err(|_| io::Error::other("compiled latency registry was rejected"))?;
    let report = DiagnosticEngine::new(registry).evaluate_version(
        "send-latency",
        PackVersion::new(2, 0, 0),
        std::slice::from_ref(&snapshot),
    )?;
    let result = json!({
        "schema_version": "rocketmq.sre-live-send-latency-regression.v1",
        "provenance": "isolated_same_tree_source_sample",
        "production_verified": false,
        "real_idp_verified": false,
        "data_read_path": "prometheus_http_then_normal_canonical_source_composer",
        "mcp_usage": "authenticated_tls_compatibility_and_cluster_activity_prerequisite",
        "selected_window_kind": "completed_requested_window_with_actual_scrape_proof",
        "expected_broker_availability": expected,
        "captured_at": Utc::now(),
        "evidence": snapshot,
        "diagnosis": {
            "pack_id": report.pack_id,
            "pack_version": report.pack_version.to_string(),
            "status": match report.status {
                DiagnosticStatus::Healthy => "healthy",
                DiagnosticStatus::Fault => "fault",
                DiagnosticStatus::Inconclusive => "inconclusive",
                DiagnosticStatus::Unsupported => "unsupported",
            },
            "reason_codes": report.findings.iter().map(|finding| &finding.reason_code).collect::<Vec<_>>(),
            "missing_required_evidence": report.missing_required_evidence,
        },
        "tenant_boundary_rejected": matches!(tenant_rejection, ConnectorAdmissionOutcome::Rejected(ConnectorAdmissionRejection::TenantMismatch)),
        "cluster_boundary_rejected": matches!(cluster_rejection, ConnectorAdmissionOutcome::Rejected(ConnectorAdmissionRejection::ClusterNotAllowed)),
        "owned_runtime_shutdown_completed": shutdown.is_healthy(),
        "shutdown": shutdown,
    });
    let mut file = OpenOptions::new().write(true).create_new(true).open(output)?;
    file.write_all(&serde_json::to_vec_pretty(&result)?)?;
    file.write_all(b"\n")?;
    file.sync_all()?;

    assert!(
        shutdown.is_healthy(),
        "isolated source runtime did not shut down cleanly"
    );
    assert!(matches!(
        tenant_rejection,
        ConnectorAdmissionOutcome::Rejected(ConnectorAdmissionRejection::TenantMismatch)
    ));
    assert!(matches!(
        cluster_rejection,
        ConnectorAdmissionOutcome::Rejected(ConnectorAdmissionRejection::ClusterNotAllowed)
    ));
    assert_eq!(snapshot.tenant_id, config.tenant_id);
    assert_eq!(snapshot.cluster_id, config.cluster_ids[CLUSTER]);
    assert_eq!(snapshot.source, "prometheus");
    assert_eq!(snapshot.resource, RESOURCE);
    assert_eq!(snapshot.time_range, range);
    assert_eq!(snapshot.exposure, EvidenceExposure::PrometheusApi);
    assert!(snapshot.partial);
    assert_ne!(snapshot.coverage, CoverageStatus::Available);
    assert_eq!(report.status, DiagnosticStatus::Inconclusive);
    assert!(
        report
            .findings
            .iter()
            .all(|finding| finding.outcome != FindingOutcome::Healthy)
    );
    if matches!(expected, "available" | "no_traffic" | "insufficient_data") {
        assert!(report.missing_required_evidence.len() >= 5);
    } else {
        // Unusable proof is excluded before rule evaluation. The engine then
        // names the missing input rather than manufacturing evaluated findings.
        assert!(!report.missing_required_evidence.is_empty());
        assert!(
            report
                .findings
                .iter()
                .all(|finding| finding.outcome != FindingOutcome::Fault)
        );
    }
    check_observations(&snapshot, expected);
    Ok(())
}

fn completed_window() -> TestResult<TimeRange> {
    let parse = |name| -> TestResult<DateTime<Utc>> { Ok(std::env::var(name)?.parse()?) };
    let range = TimeRange::new(
        parse("ROCKETMQ_SRE_LIVE_SEND_LATENCY_START")?,
        parse("ROCKETMQ_SRE_LIVE_SEND_LATENCY_END")?,
    )?;
    if range.end > Utc::now() {
        return Err(io::Error::other("live latency window must already be completed").into());
    }
    Ok(range)
}

fn expected_availability() -> TestResult<&'static str> {
    match std::env::var("ROCKETMQ_SRE_LIVE_SEND_LATENCY_EXPECT").as_deref() {
        Ok("available") => Ok("available"),
        Ok("no_traffic") => Ok("no_traffic"),
        Ok("stale") => Ok("stale"),
        Ok("missing") => Ok("missing"),
        Ok("unknown") => Ok("unknown"),
        Ok("insufficient_data") => Ok("insufficient_data"),
        _ => {
            Err(io::Error::other("live latency regression requires an explicit closed availability expectation").into())
        }
    }
}

fn output_path() -> TestResult<PathBuf> {
    let path = PathBuf::from(std::env::var("ROCKETMQ_SRE_LIVE_SEND_LATENCY_OUTPUT")?);
    let parent = path
        .parent()
        .filter(|_| path.is_absolute() && path.file_name().is_some())
        .ok_or_else(|| io::Error::other("live output must be an absolute new file outside the repository"))?;
    let parent = std::fs::canonicalize(parent)?;
    let repository = std::fs::canonicalize(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../.."))?;
    if parent.starts_with(repository) {
        return Err(io::Error::other("live source captures must remain outside the repository").into());
    }
    Ok(parent.join(
        path.file_name()
            .ok_or_else(|| io::Error::other("missing output filename"))?,
    ))
}

fn check_observations(snapshot: &EvidenceSnapshot, expected: &str) {
    let EvidenceContent::Inline(content) = &snapshot.content else {
        panic!("fixed live source must capture inline safe histogram proof");
    };
    assert_eq!(content["observation_schema"], "rocketmq.send-latency-window.v1");
    assert_eq!(content["scope"]["topic"], TOPIC);
    assert_eq!(content["scope"]["canonical_resource"], RESOURCE);
    assert_eq!(content["window"]["interval"], "left_open_right_closed");
    assert_eq!(content["unit"], "milliseconds");
    assert_eq!(
        content["missing_dimensions"]
            .as_array()
            .expect("explicit path gaps")
            .len(),
        5
    );
    let rows = content["segments"]
        .as_array()
        .expect("five independent population rows");
    assert_eq!(rows.len(), 5);
    let broker = rows.iter().find(|row| row["id"] == "broker").expect("Broker segment");
    assert_eq!(broker["availability"], expected);
    for row in rows.iter().filter(|row| row["id"] != "broker") {
        assert_eq!(row["availability"], "unsupported_scope");
        assert!(row["observation"].is_null());
    }
    let requests = content["http_statistics"]["physical_requests"]
        .as_u64()
        .expect("observed request count");
    assert!((1..=3).contains(&requests));
    let histograms = content["raw_histograms"].as_array().expect("safe histogram proof");
    if !histograms.is_empty() {
        assert_eq!(
            requests, 3,
            "qualified proof requires all three consistent physical reads"
        );
        assert!(content["source_age_seconds"].as_u64().is_some());
    }
    if expected == "available" {
        let observation = &broker["observation"];
        assert!(
            observation["observed_increments"]
                .as_f64()
                .is_some_and(|value| value >= 100.0)
        );
        assert!(
            observation["estimate_millis"]
                .as_f64()
                .is_some_and(|value| value.is_finite() && value >= 0.0)
        );
        assert!(!histograms.is_empty());
    }
    if expected == "no_traffic" {
        assert!(
            broker["observation"].is_null(),
            "no traffic must not become zero-millisecond latency"
        );
    }
}
