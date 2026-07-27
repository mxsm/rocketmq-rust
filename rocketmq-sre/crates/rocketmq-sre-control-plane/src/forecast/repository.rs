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

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::AnomalyAssessment;
use rocketmq_sre_contracts::AnomalyBaseline;
use rocketmq_sre_contracts::BacklogEta;
use rocketmq_sre_contracts::CapacityForecast;
use rocketmq_sre_contracts::ChangePoint;
use rocketmq_sre_contracts::ClusterForecastReport;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::ForecastAccuracy;
use rocketmq_sre_contracts::ForecastWindow;
use serde::de::DeserializeOwned;
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

#[cfg(test)]
use super::policy::ForecastConfiguration;
use crate::ControlPlaneError;
use crate::PostgresRepository;
use crate::auth::AuthContext;

const FORECAST_REPORT_SCHEMA: &str = "rocketmq-sre.cluster-forecast.v1";
const MAX_REPORT_ITEMS: usize = 256;
const OUTCOME_MATCH_TOLERANCE_SECONDS: i64 = 1_800;

impl PostgresRepository {
    pub(crate) async fn store_anomaly_baseline(
        &self,
        auth: &AuthContext,
        baseline: &AnomalyBaseline,
    ) -> Result<(), ControlPlaneError> {
        enforce_scope(auth, baseline.tenant_id, baseline.cluster_id)?;
        sqlx::query(
            "INSERT INTO anomaly_baselines (
                id, tenant_id, cluster_id, resource, metric, period_seconds,
                median, median_absolute_deviation, sample_count, coverage_ratio,
                algorithm_version, valid_from, valid_until, report
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
             ) ON CONFLICT (id) DO NOTHING",
        )
        .bind(baseline.id.as_uuid())
        .bind(baseline.tenant_id.as_uuid())
        .bind(baseline.cluster_id.as_uuid())
        .bind(serialize(&baseline.resource)?)
        .bind(&baseline.metric)
        .bind(i64::try_from(baseline.period_seconds).map_err(|_| {
            ControlPlaneError::validation("invalid_forecast", "baseline period exceeds PostgreSQL BIGINT")
        })?)
        .bind(baseline.median)
        .bind(baseline.median_absolute_deviation)
        .bind(i32::try_from(baseline.sample_count).map_err(|_| {
            ControlPlaneError::validation("invalid_forecast", "baseline sample count exceeds PostgreSQL INTEGER")
        })?)
        .bind(baseline.coverage_ratio)
        .bind(&baseline.algorithm_version)
        .bind(baseline.valid_from)
        .bind(baseline.valid_until)
        .bind(serialize(baseline)?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(crate) async fn store_anomaly_assessment(
        &self,
        auth: &AuthContext,
        assessment: &AnomalyAssessment,
    ) -> Result<(), ControlPlaneError> {
        enforce_scope(auth, assessment.tenant_id, assessment.cluster_id)?;
        sqlx::query(
            "INSERT INTO anomaly_assessments (
                id, tenant_id, cluster_id, metric, seasonality, anomaly, report, observed_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(Uuid::new_v4())
        .bind(assessment.tenant_id.as_uuid())
        .bind(assessment.cluster_id.as_uuid())
        .bind(&assessment.metric)
        .bind(seasonality_name(assessment.seasonality))
        .bind(assessment.anomaly)
        .bind(serialize(assessment)?)
        .bind(assessment.observed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(crate) async fn store_change_point(
        &self,
        auth: &AuthContext,
        change: &ChangePoint,
    ) -> Result<(), ControlPlaneError> {
        enforce_scope(auth, change.tenant_id, change.cluster_id)?;
        sqlx::query(
            "INSERT INTO change_points (
                id, tenant_id, cluster_id, resource, metric, detected_at,
                before_value, after_value, score, algorithm_version,
                evidence_ids, report
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(change.id.as_uuid())
        .bind(change.tenant_id.as_uuid())
        .bind(change.cluster_id.as_uuid())
        .bind(serialize(&change.resource)?)
        .bind(&change.metric)
        .bind(change.detected_at)
        .bind(change.before_value)
        .bind(change.after_value)
        .bind(change.score)
        .bind(&change.algorithm_version)
        .bind(change.evidence_ids.iter().map(|id| id.as_uuid()).collect::<Vec<_>>())
        .bind(serialize(change)?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(crate) async fn cluster_forecast_report(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
    ) -> Result<ClusterForecastReport, ControlPlaneError> {
        enforce_cluster(auth, cluster_id)?;
        let forecasts = self
            .latest_reports::<CapacityForecast>(
                auth,
                cluster_id,
                "SELECT DISTINCT ON (metric, report->>'window') report
                 FROM capacity_forecasts
                 WHERE tenant_id = $1 AND cluster_id = $2 AND report IS NOT NULL
                 ORDER BY metric, report->>'window', observed_at DESC, id DESC
                 LIMIT 256",
            )
            .await?;
        let backlog_etas = self
            .latest_reports::<BacklogEta>(
                auth,
                cluster_id,
                "SELECT DISTINCT ON (backlog_kind, report->>'window') report
                 FROM backlog_eta_forecasts
                 WHERE tenant_id = $1 AND cluster_id = $2 AND report IS NOT NULL
                 ORDER BY backlog_kind, report->>'window', observed_at DESC, id DESC
                 LIMIT 256",
            )
            .await?;
        let baselines = self
            .latest_reports::<AnomalyBaseline>(
                auth,
                cluster_id,
                "SELECT DISTINCT ON (metric, report->>'seasonality') report
                 FROM anomaly_baselines
                 WHERE tenant_id = $1 AND cluster_id = $2 AND report IS NOT NULL
                 ORDER BY metric, report->>'seasonality', valid_until DESC, id DESC
                 LIMIT 256",
            )
            .await?;
        let anomalies = self
            .latest_reports::<AnomalyAssessment>(
                auth,
                cluster_id,
                "SELECT DISTINCT ON (metric, seasonality) report
                 FROM anomaly_assessments
                 WHERE tenant_id = $1 AND cluster_id = $2
                 ORDER BY metric, seasonality, observed_at DESC, id DESC
                 LIMIT 256",
            )
            .await?;
        let change_points = self
            .latest_reports::<ChangePoint>(
                auth,
                cluster_id,
                "SELECT report
                 FROM change_points
                 WHERE tenant_id = $1 AND cluster_id = $2 AND report IS NOT NULL
                 ORDER BY detected_at DESC, id DESC
                 LIMIT 256",
            )
            .await?;
        let accuracy = self.forecast_accuracy(auth, cluster_id).await?;
        let observed_at = latest_observed_at(&forecasts, &backlog_etas).unwrap_or_else(Utc::now);
        let partial = forecasts.is_empty()
            || forecasts
                .iter()
                .any(|forecast| !matches!(forecast.status, rocketmq_sre_contracts::ForecastStatus::Ready));
        let warnings = if forecasts.is_empty() {
            vec!["forecast_worker_has_not_persisted_capacity_data".to_owned()]
        } else if partial {
            vec!["one_or_more_forecasts_have_insufficient_or_stale_data".to_owned()]
        } else {
            Vec::new()
        };
        Ok(ClusterForecastReport {
            schema_version: FORECAST_REPORT_SCHEMA.to_owned(),
            tenant_id: auth.tenant_id,
            cluster_id,
            forecasts,
            backlog_etas,
            baselines,
            anomalies,
            change_points,
            accuracy,
            partial,
            warnings,
            execution_eligible: false,
            observed_at,
        })
    }

    async fn latest_reports<T>(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
        statement: &str,
    ) -> Result<Vec<T>, ControlPlaneError>
    where
        T: DeserializeOwned,
    {
        let rows = sqlx::query(statement)
            .bind(auth.tenant_id.as_uuid())
            .bind(cluster_id.as_uuid())
            .fetch_all(&self.pool)
            .await?;
        if rows.len() > MAX_REPORT_ITEMS {
            return Err(ControlPlaneError::validation(
                "output_too_large",
                "forecast report exceeded the bounded repository result",
            ));
        }
        rows.iter()
            .map(|row| parse(row.try_get::<Value, _>("report")?))
            .collect()
    }

    pub(crate) async fn record_forecast_outcomes(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
        metric: &str,
        actual_points: &[(DateTime<Utc>, f64)],
    ) -> Result<(), ControlPlaneError> {
        enforce_cluster(auth, cluster_id)?;
        if actual_points.is_empty() {
            return Ok(());
        }
        let reports = sqlx::query_scalar::<_, Value>(
            "SELECT report
             FROM capacity_forecasts
             WHERE tenant_id = $1 AND cluster_id = $2 AND metric = $3
               AND report IS NOT NULL
             ORDER BY observed_at DESC, id DESC
             LIMIT 64",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .bind(metric)
        .fetch_all(&self.pool)
        .await?;
        for value in reports {
            let forecast: CapacityForecast = parse(value)?;
            for projected in forecast.points.iter().filter(|point| point.projected) {
                let Some((_, actual)) = actual_points
                    .iter()
                    .filter(|(at, value)| {
                        value.is_finite()
                            && at.signed_duration_since(projected.at).num_seconds().abs()
                                <= OUTCOME_MATCH_TOLERANCE_SECONDS
                    })
                    .min_by_key(|(at, _)| at.signed_duration_since(projected.at).num_seconds().abs())
                else {
                    continue;
                };
                let signed_error = projected.value - actual;
                let interval = forecast
                    .volatility
                    .map(|value| value * projected.value.abs().max(1.0) * 1.96);
                sqlx::query(
                    "INSERT INTO forecast_actual_outcomes (
                        forecast_id, tenant_id, cluster_id, metric, forecast_window,
                        projected_at, predicted_value, actual_value, absolute_error,
                        signed_error, covered_by_interval, recorded_at
                     ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
                     ) ON CONFLICT (forecast_id, projected_at) DO NOTHING",
                )
                .bind(forecast.id.as_uuid())
                .bind(auth.tenant_id.as_uuid())
                .bind(cluster_id.as_uuid())
                .bind(metric)
                .bind(window_name(forecast.window))
                .bind(projected.at)
                .bind(projected.value)
                .bind(actual)
                .bind(signed_error.abs())
                .bind(signed_error)
                .bind(interval.map(|width| signed_error.abs() <= width))
                .bind(Utc::now())
                .execute(&self.pool)
                .await?;
            }
        }
        Ok(())
    }

    async fn forecast_accuracy(
        &self,
        auth: &AuthContext,
        cluster_id: ClusterId,
    ) -> Result<Vec<ForecastAccuracy>, ControlPlaneError> {
        let rows = sqlx::query(
            "SELECT metric, forecast_window, COUNT(*) AS evaluated_points,
                    AVG(absolute_error) AS mae, AVG(signed_error) AS bias,
                    AVG(
                        CASE WHEN covered_by_interval
                        THEN 1.0::DOUBLE PRECISION
                        ELSE 0.0::DOUBLE PRECISION END
                    )
                        FILTER (WHERE covered_by_interval IS NOT NULL) AS coverage,
                    MAX(recorded_at) AS observed_at
             FROM forecast_actual_outcomes
             WHERE tenant_id = $1 AND cluster_id = $2
             GROUP BY metric, forecast_window
             ORDER BY metric, forecast_window
             LIMIT 256",
        )
        .bind(auth.tenant_id.as_uuid())
        .bind(cluster_id.as_uuid())
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                let count: i64 = row.try_get("evaluated_points")?;
                Ok(ForecastAccuracy {
                    metric: row.try_get("metric")?,
                    window: parse_window(row.try_get("forecast_window")?)?,
                    evaluated_points: u32::try_from(count).unwrap_or(u32::MAX),
                    mean_absolute_error: row.try_get("mae")?,
                    bias: row.try_get("bias")?,
                    interval_coverage_ratio: row.try_get("coverage")?,
                    observed_at: row.try_get("observed_at")?,
                })
            })
            .collect()
    }
}

fn enforce_scope(
    auth: &AuthContext,
    tenant_id: rocketmq_sre_contracts::TenantId,
    cluster_id: ClusterId,
) -> Result<(), ControlPlaneError> {
    if auth.tenant_id != tenant_id {
        return Err(ControlPlaneError::forbidden(
            "tenant_mismatch",
            "forecast tenant differs from the authenticated tenant",
        ));
    }
    enforce_cluster(auth, cluster_id)
}

fn enforce_cluster(auth: &AuthContext, cluster_id: ClusterId) -> Result<(), ControlPlaneError> {
    if !auth.clusters.contains(&cluster_id) {
        return Err(ControlPlaneError::forbidden(
            "cluster_not_allowed",
            "forecast cluster is outside the authenticated scope",
        ));
    }
    Ok(())
}

fn serialize<T: serde::Serialize>(value: &T) -> Result<Value, ControlPlaneError> {
    serde_json::to_value(value)
        .map_err(|_| ControlPlaneError::validation("invalid_forecast", "forecast value cannot be serialized"))
}

fn parse<T: DeserializeOwned>(value: Value) -> Result<T, ControlPlaneError> {
    serde_json::from_value(value)
        .map_err(|_| ControlPlaneError::configuration("database contains an invalid forecast report"))
}

fn window_name(window: ForecastWindow) -> &'static str {
    match window {
        ForecastWindow::SevenDays => "seven_days",
        ForecastWindow::ThirtyDays => "thirty_days",
    }
}

fn parse_window(value: &str) -> Result<ForecastWindow, ControlPlaneError> {
    match value {
        "seven_days" => Ok(ForecastWindow::SevenDays),
        "thirty_days" => Ok(ForecastWindow::ThirtyDays),
        _ => Err(ControlPlaneError::configuration(
            "database contains an invalid forecast window",
        )),
    }
}

fn seasonality_name(value: rocketmq_sre_contracts::Seasonality) -> &'static str {
    match value {
        rocketmq_sre_contracts::Seasonality::Hourly => "hourly",
        rocketmq_sre_contracts::Seasonality::Daily => "daily",
        rocketmq_sre_contracts::Seasonality::Weekly => "weekly",
    }
}

fn latest_observed_at(forecasts: &[CapacityForecast], backlogs: &[BacklogEta]) -> Option<DateTime<Utc>> {
    forecasts
        .iter()
        .map(|forecast| forecast.observed_at)
        .chain(backlogs.iter().map(|forecast| forecast.observed_at))
        .max()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use chrono::Duration;
    use rocketmq_sre_contracts::ForecastBacktest;
    use rocketmq_sre_contracts::ForecastId;
    use rocketmq_sre_contracts::ForecastPoint;
    use rocketmq_sre_contracts::ForecastQuality;
    use rocketmq_sre_contracts::ForecastStatus;
    use rocketmq_sre_contracts::ForecastTrend;
    use rocketmq_sre_contracts::ResourceKind;
    use rocketmq_sre_contracts::ResourceRef;
    use rocketmq_sre_contracts::TenantId;

    use super::*;
    use crate::Phase2Repository;

    #[test]
    fn window_and_seasonality_names_are_stable() {
        assert_eq!(window_name(ForecastWindow::SevenDays), "seven_days");
        assert_eq!(seasonality_name(rocketmq_sre_contracts::Seasonality::Weekly), "weekly");
        assert_eq!(parse_window("thirty_days").expect("window"), ForecastWindow::ThirtyDays);
    }

    #[test]
    fn configuration_type_is_used_by_repository_contract() {
        let config = ForecastConfiguration::embedded().expect("forecast configuration");
        assert!(!config.targets.is_empty());
    }

    #[tokio::test]
    #[ignore = "requires ROCKETMQ_SRE_TEST_DATABASE_URL pointing to an isolated PostgreSQL database"]
    async fn forecast_report_and_actual_outcome_round_trip() {
        let database_url = std::env::var("ROCKETMQ_SRE_TEST_DATABASE_URL").expect("test database URL must be explicit");
        let repository = PostgresRepository::connect(&database_url, 2)
            .await
            .expect("database and migrations");
        let tenant_id = TenantId::new();
        let cluster_id = ClusterId::new();
        sqlx::query(
            "INSERT INTO clusters (
                id, tenant_id, external_cluster_key, environment, region,
                rocketmq_version, deployment_mode, owner_name,
                requested_access_profile, effective_access_profile, onboarding_state
             ) VALUES (
                $1, $2, $3, 'test', 'forecast-test', 'test', 'test', 'forecast-test',
                'read_only', 'read_only', 'ready_read_only'
             )",
        )
        .bind(cluster_id.as_uuid())
        .bind(tenant_id.to_string())
        .bind(format!("forecast-test-{cluster_id}"))
        .execute(&repository.pool)
        .await
        .expect("test cluster");
        let auth = AuthContext {
            tenant_id,
            subject: "forecast-test".to_owned(),
            clusters: BTreeSet::from([cluster_id]),
            roles: BTreeSet::from(["diagnose".to_owned()]),
        };
        let observed_at = Utc::now();
        let projected_at = observed_at + Duration::hours(1);
        let forecast = CapacityForecast {
            id: ForecastId::new(),
            tenant_id,
            cluster_id,
            resource: ResourceRef {
                kind: ResourceKind::Broker,
                key: "broker-a".to_owned(),
                display_name: Some("Broker A".to_owned()),
            },
            metric: "rocketmq_broker_disk_used_ratio".to_owned(),
            window: ForecastWindow::SevenDays,
            trend: ForecastTrend::Increasing,
            status: ForecastStatus::Ready,
            quality: ForecastQuality::High,
            algorithm_version: "rocketmq-sre.explainable-forecast.v1".to_owned(),
            sample_start: observed_at - Duration::days(7),
            sample_end: observed_at,
            coverage_ratio: 0.95,
            slope_per_hour: Some(0.01),
            volatility: Some(0.01),
            threshold: Some(0.85),
            exhaustion_at: Some(observed_at + Duration::hours(10)),
            points: vec![
                ForecastPoint {
                    at: observed_at,
                    value: 0.70,
                    projected: false,
                },
                ForecastPoint {
                    at: projected_at,
                    value: 0.71,
                    projected: true,
                },
            ],
            backtest: ForecastBacktest {
                evaluated_points: 8,
                mean_absolute_error: Some(0.01),
                bias: Some(0.0),
                interval_coverage_ratio: Some(0.9),
            },
            advisories: vec!["review_capacity_before_projected_threshold".to_owned()],
            evidence_ids: Vec::new(),
            execution_eligible: false,
            observed_at,
        };

        repository
            .store_capacity_forecast(&forecast)
            .await
            .expect("store forecast");
        let report = repository
            .cluster_forecast_report(&auth, cluster_id)
            .await
            .expect("read forecast report");
        assert_eq!(report.forecasts, vec![forecast.clone()]);
        assert!(!report.execution_eligible);

        repository
            .record_forecast_outcomes(&auth, cluster_id, &forecast.metric, &[(projected_at, 0.72)])
            .await
            .expect("record actual outcome");
        let report = repository
            .cluster_forecast_report(&auth, cluster_id)
            .await
            .expect("read report with accuracy");
        assert_eq!(report.accuracy.len(), 1);
        assert_eq!(report.accuracy[0].evaluated_points, 1);
        let mean_absolute_error = report.accuracy[0]
            .mean_absolute_error
            .expect("persisted outcome should have an error");
        assert!((mean_absolute_error - 0.01).abs() < f64::EPSILON);
    }
}
