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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::BurnRateWindowResult;
use rocketmq_sre_contracts::ClusterHealthReport;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::CoverageStatus;
use rocketmq_sre_contracts::EvidenceContent;
use rocketmq_sre_contracts::EvidenceQuery;
use rocketmq_sre_contracts::EvidenceSnapshot;
use rocketmq_sre_contracts::FleetClusterHealth;
use rocketmq_sre_contracts::FleetHealthReport;
use rocketmq_sre_contracts::HealthDataQuality;
use rocketmq_sre_contracts::HealthDimensionScore;
use rocketmq_sre_contracts::HealthOperationalState;
use rocketmq_sre_contracts::HealthRecentChange;
use rocketmq_sre_contracts::HealthSnapshotId;
use rocketmq_sre_contracts::HealthStatus;
use rocketmq_sre_contracts::IncidentHealthSummary;
use rocketmq_sre_contracts::QueryId;
use rocketmq_sre_contracts::SliHealth;
use rocketmq_sre_contracts::SloDimension;
use rocketmq_sre_contracts::TenantId;
use rocketmq_sre_contracts::TimeRange;
use rocketmq_sre_core::health::evaluate_health_score;
use rocketmq_sre_core::slo::BurnRatePoint;
use rocketmq_sre_core::slo::SliEvaluation;
use rocketmq_sre_core::slo::WindowRole;
use rocketmq_sre_core::slo::evaluate_burn_rates;
use serde_json::Value;

use super::SloConfiguration;
use super::repository::FleetHealthRecord;
use crate::ControlPlaneError;
use crate::OnboardingState;
use crate::PostgresRepository;
use crate::alerting::AlertingService;
use crate::auth::AuthContext;
use crate::connector_channel::PostgresConnectorChannelService;
use crate::evidence::EvidenceService;
use crate::repository::ClusterRepository;

const HEALTH_SCHEMA_VERSION: &str = "rocketmq-sre.cluster-health.v1";
const FLEET_SCHEMA_VERSION: &str = "rocketmq-sre.fleet-health.v1";
const PROMETHEUS_SCHEMA_VERSION: &str = "rocketmq.prometheus-evidence.v1";
const MAX_FLEET_CLUSTERS: usize = 500;
const MAX_WORKER_CLUSTERS: usize = 16;

/// Deterministic multi-window SLO evaluation and persisted health facade.
#[derive(Clone)]
pub(crate) struct SloService {
    repository: PostgresRepository,
    connector: PostgresConnectorChannelService,
    evidence: EvidenceService,
    alerting: AlertingService,
    config: Arc<SloConfiguration>,
    worker_cursor: Arc<AtomicUsize>,
}

impl SloService {
    pub(crate) fn new(
        repository: PostgresRepository,
        connector: PostgresConnectorChannelService,
        evidence: EvidenceService,
        alerting: AlertingService,
    ) -> Result<Self, ControlPlaneError> {
        Ok(Self {
            repository,
            connector,
            evidence,
            alerting,
            config: Arc::new(SloConfiguration::embedded()?),
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
    ) -> Result<ClusterHealthReport, ControlPlaneError> {
        if let Some(report) = self.repository.latest_health_snapshot(auth, cluster_id).await?
            && Utc::now()
                .signed_duration_since(report.observed_at)
                .to_std()
                .is_ok_and(|age| age <= self.config.worker_interval.saturating_mul(2))
        {
            return Ok(report);
        }
        self.evaluate_cluster(auth, cluster_id).await
    }

    #[tracing::instrument(
        name = "sre.slo.evaluate",
        skip_all,
        fields(cluster_id = %cluster_id, access = "read_only", algorithm = %self.config.algorithm_version)
    )]
    pub(crate) async fn evaluate_cluster(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
    ) -> Result<ClusterHealthReport, ControlPlaneError> {
        if !auth.clusters.contains(&cluster_id) {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "SLO cluster is outside the authenticated scope",
            ));
        }
        let cluster = self.repository.get(cluster_id).await?;
        if cluster.tenant_id != auth.tenant_id.to_string() || cluster.state == OnboardingState::Offboarded {
            return Err(ControlPlaneError::forbidden(
                "cluster_not_allowed",
                "SLO cluster is offboarded or belongs to another tenant",
            ));
        }
        let previous = self.repository.latest_health_snapshot(auth, cluster_id).await?;
        let evidence = self.collect_evidence(auth, cluster_id).await?;
        let now = Utc::now();
        let points = match &evidence {
            Some(snapshot) => {
                let content = match &snapshot.content {
                    EvidenceContent::Inline(content) => content.clone(),
                    EvidenceContent::Reference(_) => {
                        let bytes = self.evidence.content(auth, snapshot.evidence_id).await?;
                        serde_json::from_slice(&bytes).map_err(|_| {
                            ControlPlaneError::validation(
                                "invalid_slo_evidence",
                                "externalized SLO evidence is not valid JSON",
                            )
                        })?
                    }
                };
                parse_burn_rate_points(snapshot, &content, &self.config)?
            }
            None => Vec::new(),
        };
        let slis = evaluate_burn_rates(&self.config.slo_policy, &points, now.timestamp()).map_err(|reason| {
            ControlPlaneError::validation(
                "invalid_slo_evidence",
                format!("SLO burn-rate evidence is invalid: {reason}"),
            )
        })?;
        let score = evaluate_health_score(&self.config.score_policy, &slis).map_err(|reason| {
            ControlPlaneError::configuration(format!("health-score policy cannot be evaluated: {reason}"))
        })?;
        let incident = self.alerting.cluster_health(auth, cluster_id).await?;
        let evidence_ids = score.evidence_ids.clone();
        let report = ClusterHealthReport {
            schema_version: HEALTH_SCHEMA_VERSION.to_owned(),
            id: HealthSnapshotId::new(),
            tenant_id: auth.tenant_id,
            cluster_id,
            score: score.score,
            status: score.status,
            data_quality: score.data_quality,
            operational_state: self.config.operational_state(&cluster.environment),
            dimensions: score
                .dimensions
                .into_iter()
                .map(|dimension| HealthDimensionScore {
                    dimension: dimension.dimension,
                    weight: dimension.weight,
                    score: dimension.score,
                    status: dimension.status,
                    data_quality: dimension.data_quality,
                    triggered_sli_ids: dimension.triggered_sli_ids,
                    evidence_ids: dimension.evidence_ids,
                    reason_codes: dimension.reason_codes,
                })
                .collect(),
            slis: slis.into_iter().map(contract_sli).collect(),
            incident_summary: IncidentHealthSummary {
                active_incidents: incident.active_incidents,
                critical_incidents: incident.critical_incidents,
                unassigned_incidents: incident.unassigned_incidents,
                last_alert_at: incident.last_alert_at,
            },
            triggered_sli_ids: score.triggered_sli_ids,
            evidence_ids,
            recent_changes: health_change(previous.as_ref(), score.score, score.status, now)
                .into_iter()
                .collect(),
            algorithm_version: self.config.algorithm_version.clone(),
            model_adjustment_supported: false,
            execution_eligible: false,
            observed_at: now,
        };
        self.repository.store_health_snapshot(auth, &report).await
    }

    pub(crate) async fn fleet_report(
        &self,
        auth: &AuthContext,
        region: Option<&str>,
    ) -> Result<FleetHealthReport, ControlPlaneError> {
        let region = region.map(str::trim).filter(|value| !value.is_empty());
        if region.is_some_and(|value| value.len() > 128 || value.chars().any(char::is_control)) {
            return Err(ControlPlaneError::validation(
                "invalid_request",
                "fleet region must be a bounded plain-text value",
            ));
        }
        let records = self.repository.fleet_health_records(auth, region).await?;
        if records.len() > MAX_FLEET_CLUSTERS {
            return Err(ControlPlaneError::validation(
                "output_too_large",
                "fleet health is bounded to 500 clusters",
            ));
        }
        Ok(aggregate_fleet(auth.tenant_id, region, records, Utc::now()))
    }

    /// Evaluates a bounded batch using synthetic internal identities. Errors
    /// are isolated per cluster and never stop RocketMQ or the scheduler.
    pub(crate) async fn run_due(&self) {
        let clusters = match self.repository.list().await {
            Ok(clusters) => clusters,
            Err(error) => {
                tracing::warn!(error = %error, "SLO cluster scan failed");
                return;
            }
        };
        let eligible = clusters
            .into_iter()
            .filter(|cluster| {
                matches!(
                    cluster.state,
                    OnboardingState::ReadyReadOnly | OnboardingState::ReadOnlyDegraded
                )
            })
            .collect::<Vec<_>>();
        for cluster in bounded_worker_batch(eligible, self.worker_cursor.as_ref()) {
            let Ok(tenant_id) = cluster.tenant_id.parse::<TenantId>() else {
                tracing::warn!(cluster_id = %cluster.id, "SLO cluster tenant identifier is invalid");
                continue;
            };
            let auth = AuthContext {
                tenant_id,
                subject: "rocketmq-sre-slo-worker".to_owned(),
                clusters: BTreeSet::from([cluster.id]),
                roles: BTreeSet::from(["diagnose".to_owned()]),
            };
            if let Err(error) = self.evaluate_cluster(&auth, cluster.id).await {
                tracing::warn!(cluster_id = %cluster.id, error = %error, "SLO evaluation failed");
            }
        }
    }

    async fn collect_evidence(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
    ) -> Result<Option<EvidenceSnapshot>, ControlPlaneError> {
        let now = Utc::now();
        let resource = self.config.resource();
        let query = EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id: auth.tenant_id,
            cluster_id,
            source: "prometheus".to_owned(),
            resource: resource.clone(),
            time_range: TimeRange::new(now, now)
                .map_err(|_| ControlPlaneError::configuration("SLO evidence time range cannot be constructed"))?,
        };
        let deadline = now
            + chrono::Duration::from_std(self.config.query_timeout)
                .map_err(|_| ControlPlaneError::configuration("SLO query timeout cannot be represented"))?;
        match self
            .connector
            .query_and_wait(auth.tenant_id, cluster_id, query, deadline)
            .await
        {
            Ok(response) => {
                if let Some(snapshot) = response.evidence {
                    if snapshot.source != "prometheus" || snapshot.resource != resource {
                        return Err(ControlPlaneError::validation(
                            "invalid_slo_evidence",
                            "connector returned evidence from an unexpected source or resource",
                        ));
                    }
                    return self.evidence.persist_cluster(auth, snapshot).await.map(Some);
                }
            }
            Err(error) => {
                tracing::debug!(cluster_id = %cluster_id, error = %error, "live SLO query unavailable; using persisted evidence");
            }
        }
        self.evidence
            .latest_cluster_source(auth, cluster_id, "prometheus", &resource)
            .await
    }
}

fn bounded_worker_batch<T>(mut items: Vec<T>, cursor: &AtomicUsize) -> Vec<T> {
    if items.is_empty() {
        return items;
    }
    let batch_len = items.len().min(MAX_WORKER_CLUSTERS);
    let start = cursor.fetch_add(batch_len, Ordering::Relaxed) % items.len();
    items.rotate_left(start);
    items.truncate(batch_len);
    items
}

fn parse_burn_rate_points(
    snapshot: &EvidenceSnapshot,
    content: &Value,
    config: &SloConfiguration,
) -> Result<Vec<BurnRatePoint>, ControlPlaneError> {
    if content.get("schema_version").and_then(Value::as_str) != Some(PROMETHEUS_SCHEMA_VERSION)
        || content.get("query_kind").and_then(Value::as_str) != Some("instant")
        || content.get("metric").and_then(Value::as_str) != Some(config.recording_metric.as_str())
    {
        return Err(ControlPlaneError::validation(
            "invalid_slo_evidence",
            "SLO Prometheus evidence schema or metric is incompatible",
        ));
    }
    let policies = config
        .slo_policy
        .slis
        .iter()
        .map(|sli| (sli.id.as_str(), sli.dimension))
        .collect::<BTreeMap<_, _>>();
    let series = content
        .get("series")
        .and_then(Value::as_array)
        .ok_or_else(|| ControlPlaneError::validation("invalid_slo_evidence", "SLO Prometheus series are missing"))?;
    let mut points = Vec::new();
    for item in series {
        let labels = item.get("labels").and_then(Value::as_object).ok_or_else(|| {
            ControlPlaneError::validation("invalid_slo_evidence", "SLO Prometheus labels are missing")
        })?;
        let sli_id = label(labels, "sli")?;
        let dimension = parse_dimension(label(labels, "dimension")?)?;
        if policies.get(sli_id).copied() != Some(dimension) {
            return Err(ControlPlaneError::validation(
                "invalid_slo_evidence",
                "SLO series references an unknown SLI or mismatched dimension",
            ));
        }
        let window_id = label(labels, "window_pair")?;
        let role = match label(labels, "window_role")? {
            "short" => WindowRole::Short,
            "long" => WindowRole::Long,
            _ => {
                return Err(ControlPlaneError::validation(
                    "invalid_slo_evidence",
                    "SLO series window role is unsupported",
                ));
            }
        };
        let samples = item.get("samples").and_then(Value::as_array).ok_or_else(|| {
            ControlPlaneError::validation("invalid_slo_evidence", "SLO Prometheus samples are missing")
        })?;
        for sample in samples {
            let observed_at = sample
                .get("observed_at")
                .and_then(Value::as_str)
                .and_then(|value| value.parse::<DateTime<Utc>>().ok())
                .ok_or_else(|| {
                    ControlPlaneError::validation("invalid_slo_evidence", "SLO sample timestamp is invalid")
                })?;
            let value = sample
                .get("value")
                .and_then(Value::as_f64)
                .ok_or_else(|| ControlPlaneError::validation("invalid_slo_evidence", "SLO sample value is invalid"))?;
            points.push(BurnRatePoint {
                sli_id: sli_id.to_owned(),
                window_id: window_id.to_owned(),
                role,
                value,
                observed_epoch_seconds: observed_at.timestamp(),
                evidence_id: snapshot.evidence_id,
                partial: snapshot.partial || snapshot.coverage != CoverageStatus::Available,
            });
        }
    }
    Ok(points)
}

fn label<'a>(labels: &'a serde_json::Map<String, Value>, key: &'static str) -> Result<&'a str, ControlPlaneError> {
    labels.get(key).and_then(Value::as_str).ok_or_else(|| {
        ControlPlaneError::validation(
            "invalid_slo_evidence",
            format!("SLO Prometheus label `{key}` is missing"),
        )
    })
}

fn parse_dimension(value: &str) -> Result<SloDimension, ControlPlaneError> {
    match value {
        "traffic" => Ok(SloDimension::Traffic),
        "consumer" => Ok(SloDimension::Consumer),
        "broker" => Ok(SloDimension::Broker),
        "store" => Ok(SloDimension::Store),
        "ha_controller" => Ok(SloDimension::HaController),
        "routing_proxy" => Ok(SloDimension::RoutingProxy),
        "security" => Ok(SloDimension::Security),
        "platform" => Ok(SloDimension::Platform),
        _ => Err(ControlPlaneError::validation(
            "invalid_slo_evidence",
            "SLO Prometheus dimension is unsupported",
        )),
    }
}

fn contract_sli(sli: SliEvaluation) -> SliHealth {
    SliHealth {
        id: sli.id,
        display_name: sli.display_name,
        dimension: sli.dimension,
        objective: sli.objective,
        status: sli.status,
        data_quality: sli.data_quality,
        windows: sli
            .windows
            .into_iter()
            .map(|window| BurnRateWindowResult {
                window_id: window.window_id,
                short_window_seconds: window.short_window_seconds,
                long_window_seconds: window.long_window_seconds,
                short_burn_rate: window.short_burn_rate,
                long_burn_rate: window.long_burn_rate,
                threshold: window.threshold,
                severity: window.severity,
                triggered: window.triggered,
                data_quality: window.data_quality,
                observed_at: window.observed_epoch_seconds.and_then(DateTime::from_timestamp_secs),
                evidence_ids: window.evidence_ids,
                reason_codes: window.reason_codes,
            })
            .collect(),
        evidence_ids: sli.evidence_ids,
        reason_codes: sli.reason_codes,
    }
}

fn health_change(
    previous: Option<&ClusterHealthReport>,
    score: Option<u8>,
    status: HealthStatus,
    occurred_at: DateTime<Utc>,
) -> Option<HealthRecentChange> {
    let previous = previous?;
    if previous.score == score && previous.status == status {
        return None;
    }
    Some(HealthRecentChange {
        previous_score: previous.score,
        current_score: score,
        score_delta: previous
            .score
            .zip(score)
            .map(|(before, after)| i16::from(after) - i16::from(before)),
        previous_status: previous.status,
        current_status: status,
        occurred_at,
    })
}

fn aggregate_fleet(
    tenant_id: TenantId,
    region: Option<&str>,
    records: Vec<FleetHealthRecord>,
    observed_at: DateTime<Utc>,
) -> FleetHealthReport {
    let clusters = records
        .into_iter()
        .map(|record| fleet_cluster(record, observed_at))
        .collect::<Vec<_>>();
    let critical_clusters = ids_for(&clusters, |cluster| cluster.status == HealthStatus::Critical);
    let unknown_clusters = ids_for(&clusters, |cluster| cluster.status == HealthStatus::Unknown);
    let maintenance_clusters = ids_for(&clusters, |cluster| {
        cluster.operational_state == HealthOperationalState::Maintenance
    });
    let fault_drill_clusters = ids_for(&clusters, |cluster| {
        cluster.operational_state == HealthOperationalState::FaultDrill
    });
    let status = worst_status(clusters.iter().map(|cluster| cluster.status));
    let data_quality = worst_quality(clusters.iter().map(|cluster| cluster.data_quality));
    let score = if clusters.iter().all(|cluster| cluster.score.is_some()) {
        clusters.iter().filter_map(|cluster| cluster.score).min()
    } else {
        None
    };
    let worst_cluster_id = clusters
        .iter()
        .max_by_key(|cluster| {
            (
                status_rank(cluster.status),
                quality_rank(cluster.data_quality),
                cluster.score.map_or(101, |score| 100_u8.saturating_sub(score)),
            )
        })
        .map(|cluster| cluster.cluster_id);
    FleetHealthReport {
        schema_version: FLEET_SCHEMA_VERSION.to_owned(),
        tenant_id,
        region: region.map(str::to_owned),
        score,
        status,
        data_quality,
        worst_cluster_id,
        cluster_count: u32::try_from(clusters.len()).unwrap_or(u32::MAX),
        healthy_clusters: u32::try_from(
            clusters
                .iter()
                .filter(|cluster| cluster.status == HealthStatus::Healthy)
                .count(),
        )
        .unwrap_or(u32::MAX),
        degraded_clusters: u32::try_from(
            clusters
                .iter()
                .filter(|cluster| cluster.status == HealthStatus::Degraded)
                .count(),
        )
        .unwrap_or(u32::MAX),
        critical_clusters,
        unknown_clusters,
        maintenance_clusters,
        fault_drill_clusters,
        clusters,
        aggregation: "worst_cluster_no_average_masking".to_owned(),
        observed_at,
    }
}

fn fleet_cluster(record: FleetHealthRecord, now: DateTime<Utc>) -> FleetClusterHealth {
    match record.report {
        Some(report) => FleetClusterHealth {
            cluster_id: record.cluster_id,
            external_cluster_key: record.external_cluster_key,
            region: record.region,
            score: report.score,
            status: report.status,
            data_quality: report.data_quality,
            operational_state: report.operational_state,
            critical_incidents: report.incident_summary.critical_incidents,
            triggered_sli_ids: report.triggered_sli_ids,
            observed_at: report.observed_at,
        },
        None => FleetClusterHealth {
            cluster_id: record.cluster_id,
            external_cluster_key: record.external_cluster_key,
            region: record.region,
            score: None,
            status: HealthStatus::Unknown,
            data_quality: HealthDataQuality::Missing,
            operational_state: HealthOperationalState::Normal,
            critical_incidents: 0,
            triggered_sli_ids: Vec::new(),
            observed_at: now,
        },
    }
}

fn ids_for(clusters: &[FleetClusterHealth], predicate: impl Fn(&FleetClusterHealth) -> bool) -> Vec<ClusterId> {
    clusters
        .iter()
        .filter(|cluster| predicate(cluster))
        .map(|cluster| cluster.cluster_id)
        .collect()
}

fn worst_status(values: impl IntoIterator<Item = HealthStatus>) -> HealthStatus {
    values
        .into_iter()
        .max_by_key(|status| status_rank(*status))
        .unwrap_or(HealthStatus::Unknown)
}

const fn status_rank(status: HealthStatus) -> u8 {
    match status {
        HealthStatus::Healthy => 0,
        HealthStatus::Unknown => 1,
        HealthStatus::Degraded => 2,
        HealthStatus::Critical => 3,
    }
}

fn worst_quality(values: impl IntoIterator<Item = HealthDataQuality>) -> HealthDataQuality {
    values
        .into_iter()
        .max_by_key(|quality| quality_rank(*quality))
        .unwrap_or(HealthDataQuality::Missing)
}

const fn quality_rank(quality: HealthDataQuality) -> u8 {
    match quality {
        HealthDataQuality::Complete => 0,
        HealthDataQuality::Partial => 1,
        HealthDataQuality::Stale => 2,
        HealthDataQuality::Missing => 3,
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_sre_contracts::SchemaVersion;
    use serde_json::json;

    use super::*;

    fn record(status: HealthStatus, score: Option<u8>, quality: HealthDataQuality) -> FleetHealthRecord {
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        FleetHealthRecord {
            cluster_id,
            external_cluster_key: format!("cluster-{cluster_id}"),
            region: "test".to_owned(),
            report: Some(ClusterHealthReport {
                schema_version: HEALTH_SCHEMA_VERSION.to_owned(),
                id: HealthSnapshotId::new(),
                tenant_id,
                cluster_id,
                score,
                status,
                data_quality: quality,
                operational_state: HealthOperationalState::Normal,
                dimensions: Vec::new(),
                slis: Vec::new(),
                incident_summary: IncidentHealthSummary {
                    active_incidents: 0,
                    critical_incidents: 0,
                    unassigned_incidents: 0,
                    last_alert_at: None,
                },
                triggered_sli_ids: Vec::new(),
                evidence_ids: Vec::new(),
                recent_changes: Vec::new(),
                algorithm_version: "test".to_owned(),
                model_adjustment_supported: false,
                execution_eligible: false,
                observed_at: Utc::now(),
            }),
        }
    }

    #[test]
    fn fleet_never_hides_a_critical_cluster_behind_healthy_scores() {
        let tenant_id = TenantId::new();
        let report = aggregate_fleet(
            tenant_id,
            None,
            vec![
                record(HealthStatus::Healthy, Some(100), HealthDataQuality::Complete),
                record(HealthStatus::Critical, Some(25), HealthDataQuality::Complete),
            ],
            Utc::now(),
        );

        assert_eq!(report.status, HealthStatus::Critical);
        assert_eq!(report.score, Some(25));
        assert_eq!(report.critical_clusters.len(), 1);
        assert_eq!(report.aggregation, "worst_cluster_no_average_masking");
    }

    #[test]
    fn missing_cluster_data_makes_fleet_score_unknown_without_zero_fabrication() {
        let tenant_id = TenantId::new();
        let missing_id = ClusterId::new();
        let report = aggregate_fleet(
            tenant_id,
            None,
            vec![
                record(HealthStatus::Healthy, Some(100), HealthDataQuality::Complete),
                FleetHealthRecord {
                    cluster_id: missing_id,
                    external_cluster_key: "missing".to_owned(),
                    region: "test".to_owned(),
                    report: None,
                },
            ],
            Utc::now(),
        );

        assert_eq!(report.score, None);
        assert_eq!(report.status, HealthStatus::Unknown);
        assert_eq!(report.unknown_clusters, vec![missing_id]);
    }

    #[test]
    fn parser_requires_all_control_labels_and_preserves_evidence_identity() {
        let config = SloConfiguration::embedded().expect("SLO config");
        let cluster_id = ClusterId::new();
        let tenant_id = TenantId::new();
        let at = Utc::now();
        let query = EvidenceQuery {
            query_id: QueryId::new(),
            correlation_id: CorrelationId::new(),
            tenant_id,
            cluster_id,
            source: "prometheus".to_owned(),
            resource: config.resource(),
            time_range: TimeRange::new(at, at).expect("time range"),
        };
        let snapshot = EvidenceSnapshot::capture(
            query,
            SchemaVersion::new("rocketmq-sre.evidence", 1, 0),
            at,
            EvidenceContent::Inline(json!({
                "schema_version": PROMETHEUS_SCHEMA_VERSION,
                "query_kind": "instant",
                "metric": config.recording_metric,
                "series": [{
                    "labels": {
                        "sli": "delivery_ratio",
                        "dimension": "traffic",
                        "window_pair": "fast",
                        "window_role": "short"
                    },
                    "samples": [{"observed_at": at, "value": 15.0}]
                }]
            })),
        )
        .expect("snapshot");
        let content = match &snapshot.content {
            EvidenceContent::Inline(content) => content,
            EvidenceContent::Reference(_) => panic!("fixture must remain inline"),
        };
        let points = parse_burn_rate_points(&snapshot, content, &config).expect("points");

        assert_eq!(points.len(), 1);
        assert_eq!(points[0].evidence_id, snapshot.evidence_id);
        assert_eq!(points[0].window_id, "fast");
        assert_eq!(points[0].role, WindowRole::Short);
    }

    #[test]
    fn health_change_is_only_emitted_for_a_real_transition() {
        let previous = record(HealthStatus::Healthy, Some(100), HealthDataQuality::Complete)
            .report
            .expect("report");

        assert!(health_change(Some(&previous), Some(100), HealthStatus::Healthy, Utc::now()).is_none());
        assert_eq!(
            health_change(Some(&previous), Some(65), HealthStatus::Degraded, Utc::now())
                .expect("change")
                .score_delta,
            Some(-35)
        );
    }

    #[test]
    fn worker_batches_rotate_without_starving_later_clusters() {
        let cursor = AtomicUsize::new(0);
        let all = (0..20).collect::<Vec<_>>();

        let first = bounded_worker_batch(all.clone(), &cursor);
        let second = bounded_worker_batch(all, &cursor);

        assert_eq!(first, (0..16).collect::<Vec<_>>());
        assert_eq!(second, (16..20).chain(0..12).collect::<Vec<_>>());
    }
}
