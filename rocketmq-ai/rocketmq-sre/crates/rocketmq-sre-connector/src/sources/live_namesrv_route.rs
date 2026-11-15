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

//! Opt-in real-source regression. The external fixture owns the two independent
//! native processes and any topology faults; this module only uses fixed reads.

use std::error::Error;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{Duration, Utc};
use rocketmq_runtime::{RuntimeConfig, RuntimeOwner};
use rocketmq_sre_contracts::{
    ClusterId, CorrelationId, CoverageStatus, EvidenceContent, EvidenceExposure, EvidenceQuery, QueryId, TenantId,
    TimeRange,
};
use rocketmq_sre_core::diagnostics::{DiagnosticEngine, DiagnosticStatus, FindingOutcome, PackVersion, full_registry};
use serde_json::json;

use super::CancelSignal;
use crate::{ConnectorAdmissionOutcome, ConnectorAdmissionRejection, ConnectorConfig, ConnectorEngine, RmcpGateway};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
const CLUSTER: &str = "Local-c04-namesrv";
const TOPIC: &str = "ProducerSimpleTest";
const RESOURCE: &str = "namesrv-route/ProducerSimpleTest";

/// Exercises the production composer on its default owned worker. MCP supplies
/// authenticated TLS/protocol/cluster-capability checks; route data is AdminRpc.
/// There is no target mutation, arbitrary request, clock rewrite or stack override.
#[test]
#[ignore = "requires an explicitly configured isolated two-NameServer/TLS MCP fixture"]
fn live_namesrv_route_uses_fixed_peer_reads_and_owned_shutdown() -> TestResult<()> {
    if std::env::var("ROCKETMQ_SRE_LIVE_NAMESRV_ROUTE").as_deref() != Ok("1") {
        return Err(io::Error::other("live NameServer regression requires explicit opt-in").into());
    }
    let config = Arc::new(ConnectorConfig::from_env()?);
    if config.mcp_url.scheme() != "https"
        || config.mcp_ca_pem.is_empty()
        || config.cluster_allowlist.len() != 1
        || !config.cluster_allowlist.contains(CLUSTER)
        || config.admin_source.is_none()
        || config.control_plane.is_some()
    {
        return Err(io::Error::other("live fixture requires one isolated Admin cluster and verified MCP TLS").into());
    }
    let cluster_id = *config
        .cluster_ids
        .get(CLUSTER)
        .ok_or_else(|| io::Error::other("fixed live cluster mapping is missing"))?;
    let expected = expected_state()?;
    let output = output_path()?;
    // Reserve a new capture before requesting source data. Existing captures,
    // hard links and symlink aliases cannot be overwritten by another phase.
    let mut file = OpenOptions::new().write(true).create_new(true).open(output)?;
    let mut runtime_config = RuntimeConfig::server_default("namesrv-route-source-regression");
    runtime_config.shutdown_timeout = config.shutdown_timeout;
    let owner = RuntimeOwner::plan(runtime_config)?.build()?;
    let context = owner.root_context().component("namesrv-route-source-regression");
    let captured = owner.block_on(Box::pin(async {
        let gateway = Arc::new(RmcpGateway::new(config.clone())?);
        let engine = Arc::new(ConnectorEngine::new(config.clone(), gateway)?);
        let result = Box::pin(async {
            let now = Utc::now();
            let query = EvidenceQuery {
                query_id: QueryId::new(),
                correlation_id: CorrelationId::new(),
                tenant_id: config.tenant_id,
                cluster_id,
                source: "admin-query".to_owned(),
                resource: RESOURCE.to_owned(),
                time_range: TimeRange::new(now - Duration::minutes(5), now)?,
            };
            let deadline = now + Duration::from_std(config.request_timeout)?;
            let mut wrong_tenant = query.clone();
            wrong_tenant.tenant_id = TenantId::new();
            let tenant_rejection = Box::pin(engine.collect_contract_query(
                wrong_tenant,
                "isolated-namesrv-route-regression",
                deadline,
                &CancelSignal::default(),
            ))
            .await?;
            let mut wrong_cluster = query.clone();
            wrong_cluster.cluster_id = ClusterId::new();
            let cluster_rejection = Box::pin(engine.collect_contract_query(
                wrong_cluster,
                "isolated-namesrv-route-regression",
                deadline,
                &CancelSignal::default(),
            ))
            .await?;
            Box::pin(engine.initialize_sources(context.component("evidence-sources"))).await;
            if Box::pin(engine.reconcile()).await? != ConnectorAdmissionOutcome::Accepted(()) {
                return Err(io::Error::other("live MCP compatibility prerequisite was rejected").into());
            }
            // The ordinary ended window is retained. Later live observations
            // are current context, not a fabricated historical/range read.
            let end = Utc::now();
            let range = TimeRange::new(end - Duration::minutes(5), end)?;
            let query = EvidenceQuery {
                time_range: range.clone(),
                ..query
            };
            let deadline = end + Duration::from_std(config.request_timeout)?;
            let cancel = CancelSignal::for_owner(context.task_group());
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let collecting = engine.clone();
            let (_, read) = context.task_group().spawn_service_with_handle(
                "namesrv-route-source-regression.fixed-read",
                Box::pin(async move {
                    let result = Box::pin(collecting.collect_contract_query(
                        query,
                        "isolated-namesrv-route-regression",
                        deadline,
                        &cancel,
                    ))
                    .await;
                    let _ = sender.send(result);
                }),
            )?;
            read.await?;
            let snapshot = match receiver.await?? {
                ConnectorAdmissionOutcome::Accepted(snapshot) => snapshot,
                ConnectorAdmissionOutcome::Rejected(_) => {
                    return Err(io::Error::other("fixed live route read was rejected").into());
                }
            };
            Ok::<_, Box<dyn Error + Send + Sync>>((snapshot, range, tenant_rejection, cluster_rejection))
        })
        .await;
        // Match normal ownership on every outcome; no assertion can skip drain.
        context.task_group().cancel();
        Box::pin(engine.close()).await;
        result
    }));
    let shutdown = owner.shutdown_runtime_blocking();
    let (snapshot, range, tenant_rejection, cluster_rejection) = captured?;
    let shutdown = shutdown?;
    assert!(shutdown.is_healthy(), "isolated route source did not shut down cleanly");
    assert!(matches!(
        tenant_rejection,
        ConnectorAdmissionOutcome::Rejected(ConnectorAdmissionRejection::TenantMismatch)
    ));
    assert!(matches!(
        cluster_rejection,
        ConnectorAdmissionOutcome::Rejected(ConnectorAdmissionRejection::ClusterNotAllowed)
    ));
    assert_eq!(snapshot.tenant_id, config.tenant_id);
    assert_eq!(snapshot.cluster_id, cluster_id);
    assert_eq!(snapshot.source, "admin-query");
    assert_eq!(snapshot.resource, RESOURCE);
    assert_eq!(snapshot.time_range, range);
    assert_eq!(snapshot.exposure, EvidenceExposure::AdminRpc);
    assert!(snapshot.partial);
    assert_ne!(snapshot.coverage, CoverageStatus::Available);
    snapshot.verify_content_hash()?;
    let EvidenceContent::Inline(content) = &snapshot.content else {
        return Err(io::Error::other("fixed route source must capture inline safe proof").into());
    };
    assert!(content["schema_version"] == "rocketmq.sre-namesrv-route-observation.v1");
    assert!(content["profile"] == "nameserver_route_comparison");
    assert!(content["topic"] == TOPIC);
    assert!(content["temporal_semantics"] == "current_context_only");
    assert!(content["historical_coverage"] == "unknown");
    assert!(
        content["comparison"]["state"].as_str() == Some(expected),
        "actual comparison differs from the declared fixture phase"
    );
    assert!(
        content["configuration"]["peers"]
            .as_array()
            .is_some_and(|peers| peers.len() == 2)
    );
    assert!(content["rounds"].as_array().is_some_and(|rounds| rounds.len() == 2));
    assert!(
        content["missing_facts"]
            .as_array()
            .is_some_and(|facts| facts.len() >= 9)
    );
    let registry = full_registry().map_err(|_| io::Error::other("compiled route registry was rejected"))?;
    let report = DiagnosticEngine::new(registry).evaluate_version(
        "namesrv-route",
        PackVersion::new(2, 0, 0),
        std::slice::from_ref(&snapshot),
    )?;
    assert_eq!(report.status, DiagnosticStatus::Inconclusive);
    assert!(
        report
            .findings
            .iter()
            .all(|finding| finding.outcome == FindingOutcome::Inconclusive)
    );
    assert!(
        report.findings.iter().any(|finding| finding
            .supporting_evidence
            .iter()
            .any(|evidence| evidence.evidence_id == snapshot.evidence_id)),
        "real current context must retain its actual Evidence citation"
    );
    let result = json!({
        "schema_version": "rocketmq.sre-live-namesrv-route-regression.v1",
        "provenance": "isolated_same_tree_source_sample",
        "production_verified": false,
        "real_idp_verified": false,
        "data_read_path": "public_read_only_exact_admin_rpc_then_normal_canonical_source_composer",
        "mcp_usage": "authenticated_tls_protocol_and_fixed_cluster_capability_manifest_prerequisite",
        "control_plane_active_check": "not_exercised_in_this_source_only_fixture",
        "independence_provenance": "operator_declared_direct_peers_with_external_fixture_process_manifest",
        "protocol_instance_identity": "unknown",
        "selected_window_kind": "completed_requested_window_current_context_only",
        "expected_comparison": expected,
        "captured_at": Utc::now(),
        "evidence": snapshot,
        "diagnosis": {
            "pack_id": report.pack_id, "pack_version": report.pack_version.to_string(),
            "status": "inconclusive",
            "reason_codes": report.findings.iter().map(|finding| &finding.reason_code).collect::<Vec<_>>(),
            "cited_evidence_ids": report.findings.iter().flat_map(|finding| &finding.supporting_evidence)
                .map(|evidence| evidence.evidence_id).collect::<Vec<_>>(),
            "missing_required_evidence": report.missing_required_evidence,
        },
        "tenant_boundary_rejected": true,
        "cluster_boundary_rejected": true,
        "owned_runtime_shutdown_completed": shutdown.is_healthy(),
        "shutdown": shutdown,
    });
    file.write_all(&serde_json::to_vec_pretty(&result)?)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

fn expected_state() -> TestResult<&'static str> {
    match std::env::var("ROCKETMQ_SRE_LIVE_NAMESRV_ROUTE_EXPECT").as_deref() {
        Ok("compared_equal") => Ok("compared_equal"),
        Ok("stable_observed_difference") => Ok("stable_observed_difference"),
        Ok("transient_change_observed") => Ok("transient_change_observed"),
        Ok("incomparable") => Ok("incomparable"),
        _ => Err(io::Error::other("live fixture must declare one supported comparison state").into()),
    }
}

fn output_path() -> TestResult<PathBuf> {
    let path = PathBuf::from(std::env::var("ROCKETMQ_SRE_LIVE_NAMESRV_ROUTE_OUTPUT")?);
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
