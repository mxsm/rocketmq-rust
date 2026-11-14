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

//! Opt-in regression for an isolated, same-tree Broker pair and authenticated
//! TLS MCP server. This is a source observation, not production qualification.

use std::error::Error;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{Duration, Utc};
use rocketmq_runtime::{RuntimeConfig, RuntimeOwner};
use rocketmq_sre_contracts::{
    ClusterId, CorrelationId, CoverageStatus, EvidenceContent, EvidenceExposure, EvidenceQuery, EvidenceSnapshot,
    QueryId, TenantId, TimeRange,
};
use rocketmq_sre_core::diagnostics::{DiagnosticEngine, DiagnosticStatus, FindingOutcome, PackVersion, full_registry};
use serde_json::{Value, json};

use super::CancelSignal;
use crate::{ConnectorAdmissionOutcome, ConnectorAdmissionRejection, ConnectorConfig, ConnectorEngine, RmcpGateway};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const CLUSTER: &str = "Local-c02-pressure";
const RESOURCE: &str = "store-pressure/broker-a";

/// Requires a task-owned master/replica pair, a CA-verified HTTPS MCP endpoint,
/// and the usual ConnectorConfig environment. The external fixture owns node
/// setup and any replica fault; this test exposes no target mutation API.
#[test]
#[ignore = "requires an explicitly configured isolated Broker/MCP source fixture"]
fn live_store_pressure_uses_fixed_canonical_read_and_owned_shutdown() -> TestResult<()> {
    eprintln!("live Store source stage: configuration");
    if std::env::var("ROCKETMQ_SRE_LIVE_STORE_PRESSURE").as_deref() != Ok("1") {
        return Err(io::Error::other("live Store source regression requires explicit opt-in").into());
    }
    let config = Arc::new(ConnectorConfig::from_env()?);
    if config.mcp_url.scheme() != "https"
        || config.mcp_ca_pem.is_empty()
        || config.cluster_allowlist.len() != 1
        || !config.cluster_allowlist.contains(CLUSTER)
        || config.admin_source.is_none()
        || config.control_plane.is_some()
    {
        return Err(
            io::Error::other("live fixture must use one isolated Admin cluster and CA-verified MCP TLS").into(),
        );
    }
    let unavailable = match std::env::var("ROCKETMQ_SRE_LIVE_STORE_PRESSURE_EXPECT_UNAVAILABLE").as_deref() {
        Err(std::env::VarError::NotPresent) | Ok("0") => 0,
        Ok("1") => 1,
        _ => return Err(io::Error::other("live fixture permits only zero or one unavailable replica").into()),
    };
    let output = output_path()?;
    let mut runtime_config = RuntimeConfig::server_default("store-pressure-source-regression");
    runtime_config.shutdown_timeout = config.shutdown_timeout;
    let owner = RuntimeOwner::plan(runtime_config)?.build()?;
    eprintln!("live Store source stage: owned runtime ready");
    let context = owner.root_context().component("store-pressure-source-regression");
    let captured = owner.block_on(Box::pin(async {
        eprintln!("live Store source stage: constructing adapters");
        let gateway = Arc::new(RmcpGateway::new(config.clone())?);
        let engine = Arc::new(ConnectorEngine::new(config.clone(), gateway)?);
        let result = Box::pin(async {
            let now = Utc::now();
            // This live point read includes collection time explicitly. It is
            // not evidence of range coverage for a completed historical window.
            let deadline = now + Duration::from_std(config.request_timeout)?;
            let query = EvidenceQuery {
                query_id: QueryId::new(),
                correlation_id: CorrelationId::new(),
                tenant_id: config.tenant_id,
                cluster_id: config.cluster_ids[CLUSTER],
                source: "admin-query".to_owned(),
                resource: RESOURCE.to_owned(),
                time_range: TimeRange::new(now - Duration::minutes(5), deadline)?,
            };
            // Check boundary rejection before any source is initialized. A
            // reachable MCP server must not be necessary to reject these IDs.
            let mut wrong_tenant = query.clone();
            wrong_tenant.tenant_id = TenantId::new();
            let tenant_rejection = Box::pin(engine.collect_contract_query(
                wrong_tenant,
                "isolated-source-regression",
                deadline,
                &CancelSignal::default(),
            ))
            .await?;
            let mut wrong_cluster = query.clone();
            wrong_cluster.cluster_id = ClusterId::new();
            let cluster_rejection = Box::pin(engine.collect_contract_query(
                wrong_cluster,
                "isolated-source-regression",
                deadline,
                &CancelSignal::default(),
            ))
            .await?;
            Box::pin(engine.initialize_sources(context.component("evidence-sources"))).await;
            eprintln!("live Store source stage: sources initialized");
            if Box::pin(engine.reconcile()).await? != ConnectorAdmissionOutcome::Accepted(()) {
                return Err(io::Error::other("live MCP compatibility handshake was rejected").into());
            }
            eprintln!("live Store source stage: MCP compatibility verified");
            // Give this single bounded read its own deadline after startup;
            // the selected live window also includes its actual sample point.
            let now = Utc::now();
            let deadline = now + Duration::from_std(config.request_timeout)?;
            let query = EvidenceQuery {
                time_range: TimeRange::new(now - Duration::minutes(5), deadline)?,
                ..query
            };
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let collecting = engine.clone();
            let (_, read) = context.task_group().spawn_service_with_handle(
                "store-pressure-source-regression.fixed-read",
                Box::pin(async move {
                    let result = Box::pin(collecting.collect_contract_query(
                        query,
                        "isolated-source-regression",
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
                    return Err(io::Error::other("fixed live Store source read was rejected").into());
                }
            };
            Ok::<_, Box<dyn Error + Send + Sync>>((snapshot, tenant_rejection, cluster_rejection))
        })
        .await;
        // Match the production shutdown order even on a failed read. Assertions
        // run only after Admin/client tasks and their runtime have been awaited.
        context.task_group().cancel();
        Box::pin(engine.close()).await;
        result
    }));
    let shutdown = owner.shutdown_runtime_blocking();
    let (snapshot, tenant_rejection, cluster_rejection) = captured?;
    let shutdown = shutdown?;

    let registry = full_registry().map_err(|_| io::Error::other("compiled Store pressure registry was rejected"))?;
    let report = DiagnosticEngine::new(registry).evaluate_version(
        "store-pressure",
        PackVersion::new(2, 0, 0),
        std::slice::from_ref(&snapshot),
    )?;
    // Only the canonical, sanitized snapshot is exported. In particular the
    // Broker runtime KV table, filesystem roots and TLS/auth configuration never
    // become report fields. Exclusive creation preserves previous captures.
    let result = json!({
        "schema_version": "rocketmq.sre-live-store-pressure-regression.v1",
        "provenance": "isolated_same_tree_source_sample",
        "production_verified": false,
        "data_read_path": "public_read_only_admin_rpc_then_canonical_projection",
        "mcp_usage": "authenticated_tls_compatibility_and_cluster_activity_checks",
        "selected_window_kind": "live_point_collection_period_not_historical_range_proof",
        "expected_unavailable_replicas": unavailable,
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
    assert_eq!(snapshot.source, "admin-query");
    assert_eq!(snapshot.resource, RESOURCE);
    assert_eq!(snapshot.exposure, EvidenceExposure::AdminRpc);
    assert!(snapshot.partial);
    assert_eq!(snapshot.coverage, CoverageStatus::Partial);
    assert_eq!(report.status, DiagnosticStatus::Inconclusive);
    assert!(
        report
            .findings
            .iter()
            .all(|finding| finding.outcome != FindingOutcome::Healthy)
    );
    assert!(
        report
            .missing_required_evidence
            .iter()
            .any(|item| item.contains("monotonic_progress"))
    );
    check_observations(&snapshot, unavailable);
    Ok(())
}

fn output_path() -> TestResult<PathBuf> {
    let path = PathBuf::from(std::env::var("ROCKETMQ_SRE_LIVE_STORE_PRESSURE_OUTPUT")?);
    let parent = path
        .parent()
        .filter(|_| path.is_absolute() && path.file_name().is_some())
        .ok_or_else(|| io::Error::other("live source output must be an absolute new file outside the repository"))?;
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

fn check_observations(snapshot: &EvidenceSnapshot, unavailable: u64) {
    let EvidenceContent::Inline(content) = &snapshot.content else {
        panic!("fixed live Store source must capture inline safe metadata");
    };
    assert_eq!(content["schema_version"], "rocketmq.sre-diagnostic-source.v1");
    assert_eq!(content["profile"], "store_pressure");
    assert_eq!(content["unavailable_brokers"], unavailable);
    let rows = content["brokers"].as_array().expect("actual per-instance rows");
    assert_eq!(rows.len() as u64, 2 - unavailable);
    assert_eq!(content["pressure_observed_brokers"], rows.len());
    let mut ids = std::collections::BTreeSet::new();
    for row in rows {
        assert_eq!(row["broker_name"], "broker-a");
        assert!(ids.insert(row["broker_id"].as_u64().expect("actual instance ID")));
        let pressure = &row["store_pressure"];
        assert_eq!(pressure["schema_version"], "rocketmq.store-pressure-observation.v1");
        assert_eq!(pressure["source"], "broker_runtime_info");
        assert_eq!(pressure["generation_status"], "matched_runtime_configuration");
        assert!(
            pressure["configuration_generation"]
                .as_u64()
                .is_some_and(|value| value > 0)
        );
        assert!(pressure["observed_at_millis"].as_u64().is_some_and(|value| value > 0));
        let disk = &pressure["disk"];
        assert_eq!(disk["commit_log_samples_complete"], true);
        assert_eq!(disk["commit_log_unknown_paths"], 0);
        assert!(
            disk["commit_log_expected_paths"]
                .as_u64()
                .is_some_and(|value| value > 0)
        );
        assert_eq!(disk["commit_log_expected_paths"], disk["commit_log_observed_paths"]);
        for field in ["commit_log_max_used_ratio", "consume_queue_used_ratio"] {
            assert!(
                disk[field]
                    .as_f64()
                    .is_some_and(|ratio| ratio.is_finite() && ratio > 0.0 && ratio <= 1.0)
            );
        }
        let runtime = &pressure["runtime"];
        for field in [
            "sync_flush_queue_depth",
            "sync_flush_timeout_total",
            "sync_flush_oldest_wait_millis",
            "dispatch_behind_bytes",
        ] {
            // A quiet source may truthfully report zero. These checks require
            // real counter presence, never a synthetic nonzero workload claim.
            assert!(runtime[field].as_u64().is_some(), "missing native counter: {field}");
        }
        for field in [
            "disk_warning_used_ratio",
            "disk_clean_forcibly_used_ratio",
            "disk_max_used_ratio",
        ] {
            assert!(
                pressure["policy"][field]
                    .as_f64()
                    .is_some_and(|ratio| ratio.is_finite() && (0.0..=1.0).contains(&ratio))
            );
        }
        assert_eq!(pressure["tiered_configured"], false);
        let signals = row["signals"].as_array().expect("derived safe predicates");
        assert!(
            signals
                .iter()
                .any(|signal| signal["key"] == "flush_dispatch_progress" && signal["state"] == "unknown")
        );
        assert!(!pressure.to_string().contains("commitLogDiskRatio_"));
        assert!(!pressure.to_string().contains("/task/"));
    }
    assert!(ids.contains(&0));
    if unavailable == 0 {
        assert!(ids.contains(&1));
    }
    // The content is a closed sanitized projection, not a raw runtime table.
    assert!(content.get("runtime_info").is_none());
    assert!(content.get("disk_pressure").is_none());
    assert!(content.get("flush_stalled").is_none());
    assert!(content.get("dispatch_stalled").is_none());
    assert!(matches!(content.get("missing_dimensions"), Some(Value::Array(items)) if !items.is_empty()));
}
