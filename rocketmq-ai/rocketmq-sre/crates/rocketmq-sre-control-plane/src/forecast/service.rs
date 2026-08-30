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

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use chrono::Duration;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterForecastReport;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::DrReadinessReport;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceId;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::ForecastStatus;
use rocketmq_sre_contracts::HealthDataQuality;
use rocketmq_sre_contracts::HealthStatus;
use rocketmq_sre_contracts::QueryId;
use rocketmq_sre_contracts::ReadinessReportId;
use rocketmq_sre_contracts::SimulationId;
use rocketmq_sre_contracts::SimulationKind;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::TimeRange;
use rocketmq_sre_contracts::UpgradeReadinessReport;
use rocketmq_sre_contracts::WhatIfSimulation;
use rocketmq_sre_contracts::WhatIfSimulationRequest;
use rocketmq_sre_core::prediction::readiness::ReadinessSignals;
use rocketmq_sre_core::prediction::readiness::evaluate_dr;
use rocketmq_sre_core::prediction::readiness::evaluate_upgrade;
use rocketmq_sre_core::prediction::what_if::simulate;
use serde_json::Value;
use serde_json::json;

use super::ForecastConfiguration;
use super::policy::ForecastTarget;
use super::policy::ForecastTargetKind;
use super::policy::ForecastWindowPolicy;
use super::projection::backlog_forecast;
use super::projection::baseline_artifacts;
use super::projection::capacity_forecast;
use super::projection::change_point_artifact;
use super::projection::parse_prometheus_points;
use crate::ControlPlaneError;
use crate::OnboardingState;
use crate::Phase2Repository;
use crate::PostgresRepository;
use crate::assets::AssetKind;
use crate::assets::AssetTopologyService;
use crate::assets::InventorySnapshot;
use crate::auth::AuthContext;
use crate::connector_channel::PostgresConnectorChannelService;
use crate::evidence::EvidenceService;
use crate::repository::ClusterRepository;
use crate::slo::SloService;

const MAX_SIMULATION_RESOURCES: usize = 128;
const MAX_READINESS_TEXT: usize = 128;
const READINESS_TTL_SECONDS: i64 = 3_600;

/// Capacity forecasting, deterministic simulation, and readiness facade.
#[derive(Clone)]
pub(crate) struct ForecastService {
    repository: PostgresRepository,
    connector: PostgresConnectorChannelService,
    evidence: EvidenceService,
    assets: AssetTopologyService,
    slo: SloService,
    config: Arc<ForecastConfiguration>,
    worker_cursor: Arc<AtomicUsize>,
}

impl ForecastService {
    pub(crate) fn new(
        repository: PostgresRepository,
        connector: PostgresConnectorChannelService,
        evidence: EvidenceService,
        assets: AssetTopologyService,
        slo: SloService,
    ) -> Result<Self, ControlPlaneError> {
        Ok(Self {
            repository,
            connector,
            evidence,
            assets,
            slo,
            config: Arc::new(ForecastConfiguration::embedded()?),
            worker_cursor: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub(crate) fn worker_interval(&self) -> std::time::Duration {
        self.config.worker_interval
    }

    pub(crate) async fn cluster_report(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
    ) -> Result<ClusterForecastReport, ControlPlaneError> {
        self.validate_cluster(auth, cluster_id).await?;
        self.repository.cluster_forecast_report(auth, cluster_id).await
    }

    #[tracing::instrument(
        name = "sre.forecast.evaluate",
        skip_all,
        fields(cluster_id = %cluster_id, target = %target.id, window = ?window.window, access = "read_only")
    )]
    async fn evaluate_target(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
        target: &ForecastTarget,
        window: &ForecastWindowPolicy,
    ) -> Result<(), ControlPlaneError> {
        self.validate_cluster(auth, cluster_id).await?;
        let now = Utc::now();
        let evidence = self.collect_evidence(auth, cluster_id, target, window, now).await?;
        let evidence_ids = evidence
            .as_ref()
            .map(|snapshot| vec![snapshot.evidence_id])
            .unwrap_or_default();
        let content = match evidence.as_ref() {
            Some(snapshot) => Some(self.evidence_content(auth, snapshot).await?),
            None => None,
        };
        let points = content
            .as_ref()
            .map(|content| {
                parse_prometheus_points(
                    content,
                    &target.metric,
                    window.window,
                    target.aggregation,
                    self.config.max_response_points,
                )
            })
            .transpose()?
            .unwrap_or_default();
        let actual_points = points
            .iter()
            .filter_map(|point| chrono::DateTime::from_timestamp(point.at_seconds, 0).map(|at| (at, point.value)))
            .collect::<Vec<_>>();
        self.repository
            .record_forecast_outcomes(auth, cluster_id, &target.metric, &actual_points)
            .await?;
        match target.kind {
            ForecastTargetKind::Capacity | ForecastTargetKind::Expiry => {
                let forecast = capacity_forecast(
                    auth.tenant_id,
                    cluster_id,
                    target,
                    window,
                    &points,
                    evidence_ids.clone(),
                    &self.config.algorithm_version,
                    now,
                )?;
                self.repository.store_capacity_forecast(&forecast).await?;
            }
            ForecastTargetKind::Backlog => {
                let forecast = backlog_forecast(
                    auth.tenant_id,
                    cluster_id,
                    target,
                    window,
                    &points,
                    evidence_ids.clone(),
                    &self.config.algorithm_version,
                    now,
                )?;
                self.repository.store_backlog_eta(&forecast).await?;
            }
        }
        if window.window == rocketmq_sre_contracts::ForecastWindow::ThirtyDays {
            for policy in &self.config.baselines {
                let (baseline, anomaly) = baseline_artifacts(
                    auth.tenant_id,
                    cluster_id,
                    target,
                    *policy,
                    &points,
                    &evidence_ids,
                    &self.config.algorithm_version,
                    now,
                )?;
                if let Some(baseline) = baseline {
                    self.repository.store_anomaly_baseline(auth, &baseline).await?;
                }
                self.repository.store_anomaly_assessment(auth, &anomaly).await?;
            }
            if let Some(change) = change_point_artifact(
                auth.tenant_id,
                cluster_id,
                target,
                &points,
                self.config.change_point_window_samples,
                self.config.change_point_score_threshold,
                evidence_ids,
                &self.config.algorithm_version,
            )? {
                self.repository.store_change_point(auth, &change).await?;
            }
        }
        Ok(())
    }

    /// Evaluates a rotating bounded set of cluster/target/window tuples. A
    /// failed data source is isolated and cannot stop the scheduler.
    pub(crate) async fn run_due(&self) {
        let clusters = match self.repository.list().await {
            Ok(clusters) => clusters
                .into_iter()
                .filter(|cluster| {
                    matches!(
                        cluster.state,
                        OnboardingState::ReadyReadOnly | OnboardingState::ReadOnlyDegraded
                    )
                })
                .collect::<Vec<_>>(),
            Err(error) => {
                tracing::warn!(error = %error, "forecast cluster scan failed");
                return;
            }
        };
        let tuples_per_cluster = self.config.targets.len().saturating_mul(self.config.windows.len());
        let total = clusters.len().saturating_mul(tuples_per_cluster);
        if total == 0 {
            return;
        }
        let start = self
            .worker_cursor
            .fetch_add(self.config.max_evaluations_per_run, Ordering::Relaxed);
        for offset in 0..self.config.max_evaluations_per_run.min(total) {
            let flat = start.wrapping_add(offset) % total;
            let cluster_index = flat / tuples_per_cluster;
            let tuple_index = flat % tuples_per_cluster;
            let target_index = tuple_index / self.config.windows.len();
            let window_index = tuple_index % self.config.windows.len();
            let cluster = &clusters[cluster_index];
            let Ok(tenant_id) = cluster.tenant_id.parse::<TenantId>() else {
                tracing::warn!(cluster_id = %cluster.id, "forecast cluster tenant identifier is invalid");
                continue;
            };
            let auth = AuthContext {
                tenant_id,
                subject: "rocketmq-sre-forecast-worker".to_owned(),
                clusters: BTreeSet::from([cluster.id]),
                roles: BTreeSet::from(["diagnose".to_owned()]),
            };
            if let Err(error) = self
                .evaluate_target(
                    &auth,
                    cluster.id,
                    &self.config.targets[target_index],
                    &self.config.windows[window_index],
                )
                .await
            {
                tracing::warn!(
                    cluster_id = %cluster.id,
                    target = %self.config.targets[target_index].id,
                    window = ?self.config.windows[window_index].window,
                    error = %error,
                    "forecast evaluation failed"
                );
            }
        }
    }

    pub(crate) async fn run_simulation(
        &self,
        auth: &AuthContext,
        request: WhatIfSimulationRequest,
    ) -> Result<WhatIfSimulation, ControlPlaneError> {
        self.validate_cluster(auth, request.cluster_id).await?;
        validate_simulation_request(&request)?;
        let inventory = self.assets.latest(auth, request.cluster_id).await?;
        let report = self
            .repository
            .cluster_forecast_report(auth, request.cluster_id)
            .await?;
        let enriched = enrich_simulation(request, inventory.as_ref(), &report);
        let projection = simulate(&enriched);
        let result = WhatIfSimulation {
            id: SimulationId::new(),
            tenant_id: auth.tenant_id,
            cluster_id: enriched.cluster_id,
            kind: enriched.kind,
            status: projection.status,
            input: serde_json::to_value(&enriched).map_err(|_| {
                ControlPlaneError::validation("invalid_request", "simulation request cannot be serialized")
            })?,
            assumptions: projection.assumptions,
            projected_utilization: json!({
                "current": enriched.current_utilization,
                "projected": projection.projected_utilization,
                "unit": "ratio"
            }),
            bottlenecks: projection.bottlenecks,
            blast_radius: projection.blast_radius,
            missing_assumptions: projection.missing_assumptions,
            evidence_ids: enriched.evidence_ids,
            algorithm_version: self.config.algorithm_version.clone(),
            created_by: auth.subject.clone(),
            execution_eligible: false,
            created_at: Utc::now(),
        };
        self.repository.store_simulation(&result).await?;
        Ok(result)
    }

    pub(crate) async fn upgrade_readiness(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
        target_version: &str,
    ) -> Result<UpgradeReadinessReport, ControlPlaneError> {
        validate_bounded_text("target version", target_version, MAX_READINESS_TEXT)?;
        self.validate_cluster(auth, cluster_id).await?;
        let health = self.slo.cluster_report(auth, cluster_id).await?;
        let forecasts = self.repository.cluster_forecast_report(auth, cluster_id).await?;
        let inventory = self.assets.latest(auth, cluster_id).await?;
        let signals = readiness_signals(&health, &forecasts, inventory.as_ref(), false);
        let mut evaluation = evaluate_upgrade(signals);
        attach_readiness_evidence(&mut evaluation.findings, &health.evidence_ids, &forecasts);
        let now = Utc::now();
        let report = UpgradeReadinessReport {
            id: ReadinessReportId::new(),
            tenant_id: auth.tenant_id,
            cluster_id,
            target_version: target_version.trim().to_owned(),
            status: evaluation.status,
            findings: evaluation.findings,
            pack_versions: vec![
                "upgrade-readiness.v1".to_owned(),
                "capacity-runway.v1".to_owned(),
                "broker-ha.v1".to_owned(),
                "store-recovery.v1".to_owned(),
                "deployment-drift.v1".to_owned(),
            ],
            execution_eligible: false,
            observed_at: now,
            expires_at: now + Duration::seconds(READINESS_TTL_SECONDS),
        };
        self.repository.store_upgrade_readiness(&report).await?;
        Ok(report)
    }

    pub(crate) async fn dr_readiness(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
        target_region: Option<&str>,
        requested_rto_seconds: u64,
        requested_rpo_seconds: u64,
    ) -> Result<DrReadinessReport, ControlPlaneError> {
        if let Some(region) = target_region {
            validate_bounded_text("target region", region, MAX_READINESS_TEXT)?;
        }
        if requested_rto_seconds > 30 * 86_400 || requested_rpo_seconds > 30 * 86_400 {
            return Err(ControlPlaneError::validation(
                "invalid_request",
                "requested RTO and RPO must not exceed 30 days",
            ));
        }
        self.validate_cluster(auth, cluster_id).await?;
        let health = self.slo.cluster_report(auth, cluster_id).await?;
        let forecasts = self.repository.cluster_forecast_report(auth, cluster_id).await?;
        let inventory = self.assets.latest(auth, cluster_id).await?;
        let signals = readiness_signals(&health, &forecasts, inventory.as_ref(), true);
        let mut evaluation = evaluate_dr(signals);
        attach_readiness_evidence(&mut evaluation.findings, &health.evidence_ids, &forecasts);
        let now = Utc::now();
        let report = DrReadinessReport {
            id: ReadinessReportId::new(),
            tenant_id: auth.tenant_id,
            cluster_id,
            target_region: target_region.map(str::trim).map(str::to_owned),
            requested_rto_seconds,
            requested_rpo_seconds,
            status: evaluation.status,
            findings: evaluation.findings,
            execution_eligible: false,
            observed_at: now,
            expires_at: now + Duration::seconds(READINESS_TTL_SECONDS),
        };
        self.repository.store_dr_readiness(&report).await?;
        Ok(report)
    }

    async fn validate_cluster(&self, auth: &AuthContext, cluster_id: ClusterId) -> Result<(), ControlPlaneError> {
        if !auth.clusters.contains(&cluster_id) {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "forecast cluster is outside the authenticated scope",
            ));
        }
        let cluster = self.repository.get(cluster_id).await?;
        if cluster.tenant_id != auth.tenant_id.to_string() || cluster.state == OnboardingState::Offboarded {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "forecast cluster is offboarded or belongs to another tenant",
            ));
        }
        Ok(())
    }

    async fn collect_evidence(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
        target: &ForecastTarget,
        window: &ForecastWindowPolicy,
        now: chrono::DateTime<Utc>,
    ) -> Result<Option<EvidenceSnapshot>, ControlPlaneError> {
        let window_name = match window.window {
            rocketmq_sre_contracts::ForecastWindow::SevenDays => "7d",
            rocketmq_sre_contracts::ForecastWindow::ThirtyDays => "30d",
        };
        let resource = format!("trend/{window_name}/{}", target.metric);
        let start = now
            - Duration::seconds(
                i64::try_from(window.trend.window_seconds)
                    .map_err(|_| ControlPlaneError::configuration("forecast query window cannot be represented"))?,
            );
        let query = EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id: auth.tenant_id,
            cluster_id,
            source: "prometheus".to_owned(),
            resource: resource.clone(),
            time_range: TimeRange::new(start, now)
                .map_err(|_| ControlPlaneError::configuration("forecast evidence range cannot be constructed"))?,
        };
        let deadline = now
            + Duration::from_std(self.config.query_timeout)
                .map_err(|_| ControlPlaneError::configuration("forecast query timeout cannot be represented"))?;
        match self
            .connector
            .query_and_wait(auth.tenant_id, cluster_id, query, deadline)
            .await
        {
            Ok(response) => {
                if let Some(snapshot) = response.evidence {
                    if snapshot.source != "prometheus" || snapshot.resource != resource {
                        return Err(ControlPlaneError::validation(
                            "invalid_forecast_evidence",
                            "connector returned forecast evidence from an unexpected source or resource",
                        ));
                    }
                    return self.evidence.persist_cluster(auth, snapshot).await.map(Some);
                }
            }
            Err(error) => {
                tracing::debug!(
                    cluster_id = %cluster_id,
                    target = %target.id,
                    error = %error,
                    "live forecast query unavailable; using persisted evidence"
                );
            }
        }
        self.evidence
            .latest_cluster_source(auth, cluster_id, "prometheus", &resource)
            .await
    }

    async fn evidence_content(
        &self,
        auth: &AuthContext,
        snapshot: &EvidenceSnapshot,
    ) -> Result<Value, ControlPlaneError> {
        match &snapshot.content {
            EvidenceContent::Inline(content) => Ok(content.clone()),
            EvidenceContent::Reference(_) => {
                let bytes = self.evidence.content(auth, snapshot.evidence_id).await?;
                serde_json::from_slice(&bytes).map_err(|_| {
                    ControlPlaneError::validation(
                        "invalid_forecast_evidence",
                        "externalized forecast evidence is not valid JSON",
                    )
                })
            }
        }
    }
}

fn validate_simulation_request(request: &WhatIfSimulationRequest) -> Result<(), ControlPlaneError> {
    if request.configuration_changes.len() > 64
        || request.affected_resource_keys.len() > MAX_SIMULATION_RESOURCES
        || request.evidence_ids.len() > 64
        || request
            .configuration_changes
            .iter()
            .chain(request.affected_resource_keys.iter())
            .any(|value| value.trim().is_empty() || value.len() > 512 || value.chars().any(char::is_control))
        || request
            .target_version
            .as_deref()
            .is_some_and(|value| validate_bounded_text("target version", value, MAX_READINESS_TEXT).is_err())
    {
        return Err(ControlPlaneError::validation(
            "invalid_request",
            "simulation request contains an invalid or unbounded field",
        ));
    }
    Ok(())
}

fn enrich_simulation(
    mut request: WhatIfSimulationRequest,
    inventory: Option<&InventorySnapshot>,
    report: &ClusterForecastReport,
) -> WhatIfSimulationRequest {
    let component = simulation_asset_kind(request.kind);
    if request.current_instances.is_none() {
        request.current_instances = inventory.map(|inventory| {
            u32::try_from(
                inventory
                    .assets
                    .iter()
                    .filter(|asset| Some(asset.key.kind) == component)
                    .count(),
            )
            .unwrap_or(u32::MAX)
        });
    }
    if request.current_queue_count.is_none() && request.kind == SimulationKind::TopicQueueExpand {
        request.current_queue_count = inventory.map(|inventory| {
            u32::try_from(
                inventory
                    .assets
                    .iter()
                    .filter(|asset| asset.key.kind == AssetKind::Queue)
                    .count(),
            )
            .unwrap_or(u32::MAX)
        });
    }
    if request.current_utilization.is_none() {
        request.current_utilization = inferred_utilization(request.kind, report);
    }
    if request.affected_resource_keys.is_empty() {
        request.affected_resource_keys = inventory
            .map(|inventory| dependency_blast_radius(inventory, component))
            .unwrap_or_default();
    }
    request
}

fn simulation_asset_kind(kind: SimulationKind) -> Option<AssetKind> {
    match kind {
        SimulationKind::BrokerOffline | SimulationKind::BrokerScaleOut => Some(AssetKind::Broker),
        SimulationKind::ProxyOffline | SimulationKind::ProxyScaleOut => Some(AssetKind::Proxy),
        SimulationKind::TopicQueueExpand => Some(AssetKind::Topic),
        SimulationKind::TrafficIncrease | SimulationKind::VersionUpgrade | SimulationKind::ConfigurationDiff => None,
    }
}

fn inferred_utilization(kind: SimulationKind, report: &ClusterForecastReport) -> Option<f64> {
    report
        .forecasts
        .iter()
        .filter(|forecast| forecast.status == ForecastStatus::Ready)
        .filter(|forecast| match kind {
            SimulationKind::BrokerOffline | SimulationKind::BrokerScaleOut => {
                forecast.resource.display_name.as_deref() == Some("broker_capacity")
            }
            SimulationKind::ProxyOffline | SimulationKind::ProxyScaleOut => {
                forecast.resource.display_name.as_deref() == Some("proxy_capacity")
            }
            SimulationKind::TrafficIncrease
            | SimulationKind::TopicQueueExpand
            | SimulationKind::VersionUpgrade
            | SimulationKind::ConfigurationDiff => {
                matches!(
                    forecast.resource.display_name.as_deref(),
                    Some("broker_capacity" | "proxy_capacity")
                )
            }
        })
        .filter_map(|forecast| {
            let current = forecast
                .points
                .iter()
                .rev()
                .find(|point| !point.projected)
                .map(|point| point.value)?;
            if (0.0..=2.0).contains(&current) {
                Some(current)
            } else {
                forecast
                    .threshold
                    .filter(|threshold| *threshold > 0.0)
                    .map(|threshold| current / threshold)
            }
        })
        .filter(|value| value.is_finite())
        .max_by(f64::total_cmp)
}

fn dependency_blast_radius(inventory: &InventorySnapshot, component: Option<AssetKind>) -> Vec<String> {
    let mut keys = BTreeSet::new();
    for edge in &inventory.edges {
        if component.is_none_or(|kind| edge.from.kind == kind || edge.to.kind == kind) {
            keys.insert(format!("{}:{}", edge.from.kind, edge.from.external_key));
            keys.insert(format!("{}:{}", edge.to.kind, edge.to.external_key));
        }
        if keys.len() >= MAX_SIMULATION_RESOURCES {
            break;
        }
    }
    if keys.is_empty() {
        for asset in inventory
            .assets
            .iter()
            .filter(|asset| component.is_none_or(|kind| asset.key.kind == kind))
            .take(MAX_SIMULATION_RESOURCES)
        {
            keys.insert(format!("{}:{}", asset.key.kind, asset.key.external_key));
        }
    }
    keys.into_iter().take(MAX_SIMULATION_RESOURCES).collect()
}

fn readiness_signals(
    health: &rocketmq_sre_contracts::ClusterHealthReport,
    forecasts: &ClusterForecastReport,
    inventory: Option<&InventorySnapshot>,
    dr: bool,
) -> ReadinessSignals {
    let health_acceptable = match health.status {
        HealthStatus::Healthy | HealthStatus::Degraded => Some(true),
        HealthStatus::Critical => Some(false),
        HealthStatus::Unknown => None,
    };
    let ready_forecasts = forecasts
        .forecasts
        .iter()
        .filter(|forecast| forecast.status == ForecastStatus::Ready)
        .collect::<Vec<_>>();
    let capacity_runway_acceptable = (!ready_forecasts.is_empty()).then(|| {
        let horizon = Utc::now() + Duration::days(7);
        ready_forecasts
            .iter()
            .all(|forecast| forecast.exhaustion_at.is_none_or(|at| at > horizon))
    });
    let quorum_ready = inventory.map(|inventory| {
        let controllers = inventory
            .assets
            .iter()
            .filter(|asset| asset.key.kind == AssetKind::Controller)
            .count();
        let brokers = inventory
            .assets
            .iter()
            .filter(|asset| asset.key.kind == AssetKind::Broker)
            .count();
        controllers >= 3 && !controllers.is_multiple_of(2) && brokers >= 2
    });
    let recovery_verified = inventory.and_then(|inventory| {
        let stores = inventory
            .assets
            .iter()
            .filter(|asset| asset.key.kind == AssetKind::Store)
            .collect::<Vec<_>>();
        (!stores.is_empty()).then(|| {
            stores.iter().all(|asset| {
                let field = if dr {
                    "backup_restore_verified"
                } else {
                    "recovery_verified"
                };
                asset.attributes.get(field).and_then(Value::as_bool) == Some(true)
            })
        })
    });
    let telemetry_fresh = match health.data_quality {
        HealthDataQuality::Complete => Some(true),
        HealthDataQuality::Partial => Some(false),
        HealthDataQuality::Stale => Some(false),
        HealthDataQuality::Missing => None,
    };
    let rollback_or_failback_defined = inventory.map(|inventory| {
        inventory
            .assets
            .iter()
            .any(|asset| asset.key.kind == AssetKind::ConfigVersion)
            && !inventory.partial
    });
    ReadinessSignals {
        health_acceptable,
        capacity_runway_acceptable,
        quorum_ready,
        recovery_verified,
        telemetry_fresh,
        rollback_or_failback_defined,
    }
}

fn attach_readiness_evidence(
    findings: &mut [rocketmq_sre_contracts::ReadinessFinding],
    health_evidence: &[EvidenceId],
    forecasts: &ClusterForecastReport,
) {
    let capacity_evidence = forecasts
        .forecasts
        .iter()
        .flat_map(|forecast| forecast.evidence_ids.iter().copied())
        .collect::<BTreeSet<_>>();
    for finding in findings {
        finding.evidence_ids = match finding.component.as_str() {
            "capacity" => capacity_evidence.iter().copied().collect(),
            "cluster" | "telemetry" => health_evidence.to_vec(),
            _ => Vec::new(),
        };
    }
}

fn validate_bounded_text(name: &str, value: &str, max: usize) -> Result<(), ControlPlaneError> {
    let value = value.trim();
    if value.is_empty() || value.len() > max || value.chars().any(char::is_control) {
        return Err(ControlPlaneError::validation(
            "invalid_request",
            format!("{name} must be non-empty, bounded, and contain no control characters"),
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;
