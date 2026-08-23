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
use crate::error::DashboardError;
use crate::model::DashboardHistoryHealth;
use crate::model::DashboardHistoryPoint;
use crate::model::DashboardHistoryQuery;
use crate::model::DashboardHistorySeries;
use crate::model::DashboardOverview;
use crate::model::DashboardTopicCurrent;
use crate::model::EnvironmentId;
use crate::model::MetricDimension;
use crate::model::MetricSample;
use crate::model::StorageBackend;
use crate::persistence::DashboardPersistence;
use crate::persistence::TimeRange;
use crate::persistence::history_repository::HistoryQuery;
use crate::persistence::lease_repository::HistoryLease;
use crate::state::AppState;
use crate::state::WebAdminFacade;
#[path = "history_collector_schedule.rs"]
mod history_collector_schedule;
use chrono::NaiveDate;
use chrono::Utc;
use history_collector_schedule::HistoryCollectorSchedule;
use history_collector_schedule::HistoryCollectorState;
use rocketmq_runtime::ChildServiceContext;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::MissedTickBehavior;

const DEFAULT_HISTORY_PAGE_SIZE: u32 = 1_440;

pub async fn overview(state: &AppState) -> Result<DashboardOverview, DashboardError> {
    state.admin_facade().dashboard_overview().await
}

pub async fn topic_current(state: &AppState) -> Result<DashboardTopicCurrent, DashboardError> {
    state.admin_client.topic_current().await
}

pub async fn broker_history(
    state: &AppState,
    query: DashboardHistoryQuery,
) -> Result<DashboardHistorySeries, DashboardError> {
    history_series(state, query, "broker-count", "broker", Vec::new()).await
}

pub async fn topic_history(
    state: &AppState,
    query: DashboardHistoryQuery,
) -> Result<DashboardHistorySeries, DashboardError> {
    let metric = if query.topic_name.is_some() {
        "topic-total-messages"
    } else {
        "topic-count"
    };
    let dimensions = query
        .topic_name
        .as_deref()
        .map(|topic| {
            vec![MetricDimension {
                key: "topic".to_string(),
                value: topic.to_string(),
            }]
        })
        .unwrap_or_default();
    history_series(state, query, metric, "topic", dimensions).await
}

async fn history_series(
    state: &AppState,
    query: DashboardHistoryQuery,
    storage_metric: &str,
    response_metric: &str,
    dimensions: Vec<MetricDimension>,
) -> Result<DashboardHistorySeries, DashboardError> {
    let history = state
        .persistence
        .query_history(HistoryQuery {
            environment_id: state.published().environment.environment_id,
            metric: storage_metric.to_string(),
            range: query_date_range(&query.date)?,
            dimensions,
            limit: query.limit.unwrap_or(DEFAULT_HISTORY_PAGE_SIZE),
            cursor: query.cursor,
        })
        .await?;
    let points = history
        .samples
        .into_iter()
        .map(|sample| DashboardHistoryPoint {
            timestamp: sample.bucket_ms,
            value: sample.value,
        })
        .collect::<Vec<_>>();
    Ok(DashboardHistorySeries {
        date: query.date,
        metric: response_metric.to_string(),
        topic_name: query.topic_name,
        collected: !points.is_empty(),
        points,
        next_cursor: history.next_cursor,
        health: state.history_runtime.health().await,
    })
}

fn query_date_range(value: &str) -> Result<TimeRange, DashboardError> {
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| DashboardError::Validation("history date must use YYYY-MM-DD".to_string()))?;
    let start_ms = date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| DashboardError::Validation("history date is invalid".to_string()))?
        .and_utc()
        .timestamp_millis();
    Ok(TimeRange {
        start_ms,
        end_ms: start_ms + 86_400_000 - 1,
    })
}

#[derive(Debug, Clone)]
pub struct DashboardHistoryRuntime {
    state: Arc<RwLock<DashboardHistoryHealth>>,
}

impl DashboardHistoryRuntime {
    pub fn new(backend: StorageBackend) -> Self {
        Self {
            state: Arc::new(RwLock::new(DashboardHistoryHealth {
                backend,
                connectivity: "available".to_string(),
                role: "standby".to_string(),
                lease_expires_at_ms: None,
                last_collection_at_ms: None,
                last_append_at_ms: None,
                last_retention_at_ms: None,
                recent_error: None,
            })),
        }
    }

    pub async fn health(&self) -> DashboardHistoryHealth {
        self.state.read().await.clone()
    }

    async fn set_leader(&self, expiry: Option<i64>) {
        let mut health = self.state.write().await;
        health.connectivity = "available".to_string();
        health.role = "leader".to_string();
        health.lease_expires_at_ms = expiry;
        health.recent_error = None;
    }

    async fn set_standby(&self) {
        let mut health = self.state.write().await;
        health.connectivity = "available".to_string();
        health.role = "standby".to_string();
        health.lease_expires_at_ms = None;
        health.recent_error = None;
    }

    /// Records a lease loss without hiding the storage failure that caused it.
    async fn set_standby_after_error(&self) {
        let mut health = self.state.write().await;
        health.role = "standby".to_string();
        health.lease_expires_at_ms = None;
    }

    async fn success(&self) {
        let now = Utc::now().timestamp_millis();
        let mut health = self.state.write().await;
        health.connectivity = "available".to_string();
        health.last_collection_at_ms = Some(now);
        health.last_append_at_ms = Some(now);
        health.recent_error = None;
    }

    async fn retention(&self) {
        self.state.write().await.last_retention_at_ms = Some(Utc::now().timestamp_millis());
    }

    async fn unavailable(&self, message: &'static str) {
        let mut health = self.state.write().await;
        health.connectivity = "unavailable".to_string();
        health.recent_error = Some(message.to_string());
    }
}

#[derive(Debug, Clone, Copy)]
pub struct HistoryCollectorConfig {
    pub interval_secs: u64,
    pub retention_days: u32,
    pub retention_batch_size: u32,
    pub lease_ttl_secs: u64,
}

pub fn start_dashboard_history_collector(
    service_context: ChildServiceContext,
    persistence: Arc<DashboardPersistence>,
    admin_facade: WebAdminFacade,
    environment_id: EnvironmentId,
    config: HistoryCollectorConfig,
    runtime: DashboardHistoryRuntime,
) -> Result<(), DashboardError> {
    if config.interval_secs == 0 {
        return Ok(());
    }
    let cancellation = service_context.task_group().cancellation_token();
    service_context
        .spawn_service("dashboard-history-collector", async move {
            let holder_id = uuid::Uuid::now_v7().to_string();
            let ttl_ms = i64::try_from(config.lease_ttl_secs)
                .ok()
                .and_then(|seconds| seconds.checked_mul(1_000))
                .filter(|ttl| *ttl > 0)
                .unwrap_or(30_000);
            let schedule = HistoryCollectorSchedule::new(config.interval_secs, ttl_ms);
            let mut renewal = tokio::time::interval(schedule.renewal_period());
            renewal.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut collection = tokio::time::interval(schedule.collection_period());
            collection.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut retention = tokio::time::interval(schedule.retention_period());
            retention.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut lease: Option<HistoryLease> = None;
            let mut collector = HistoryCollectorState::default();

            loop {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => break,
                    _ = renewal.tick() => {
                        let mut lease_failed = false;
                        if persistence.history_uses_sql_lease() {
                            lease = match lease.take() {
                                Some(current) => match persistence.renew_history_lease(&current, ttl_ms).await {
                                    Ok(renewed) => renewed,
                                    Err(_) => {
                                        runtime.unavailable("history lease is unavailable").await;
                                        lease_failed = true;
                                        None
                                    }
                                },
                                None => match persistence.acquire_history_lease(&environment_id, &holder_id, ttl_ms).await {
                                    Ok(acquired) => acquired,
                                    Err(_) => {
                                        runtime.unavailable("history lease is unavailable").await;
                                        lease_failed = true;
                                        None
                                    }
                                },
                            };
                        }
                        if persistence.history_uses_sql_lease() && lease.is_none() {
                            collector.lost_lease();
                            if lease_failed {
                                runtime.set_standby_after_error().await;
                            } else {
                                runtime.set_standby().await;
                            }
                            continue;
                        }
                        collector.became_leader();
                        runtime.set_leader(lease.as_ref().map(HistoryLease::expires_at_ms)).await;
                    }
                    _ = collection.tick(), if collector.can_collect() => {
                        match collect_history_sample(&persistence, &admin_facade, &environment_id, config.interval_secs, lease.as_ref()).await {
                            Ok(()) => {
                                runtime.success().await;
                            }
                            Err(_) => {
                                runtime.unavailable("history collection is unavailable").await;
                                if persistence.history_uses_sql_lease() {
                                    lease = None;
                                    collector.lost_lease();
                                    runtime.set_standby_after_error().await;
                                }
                            }
                        }
                    }
                    _ = retention.tick(), if collector.is_leader() => collector.retention_due(),
                    _ = tokio::task::yield_now(), if collector.can_retain() => {
                        let cutoff = Utc::now().timestamp_millis()
                            .saturating_sub(i64::from(config.retention_days) * 86_400_000);
                        match persistence
                            .delete_history_before(&environment_id, cutoff, config.retention_batch_size, lease.as_ref())
                            .await
                        {
                            Ok(result) => {
                                runtime.retention().await;
                                collector.completed_retention_batch(result.has_more);
                            }
                            Err(_) => {
                                collector.completed_retention_batch(false);
                                runtime.unavailable("history retention is unavailable").await;
                                if persistence.history_uses_sql_lease() {
                                    lease = None;
                                    collector.lost_lease();
                                    runtime.set_standby_after_error().await;
                                }
                            }
                        }
                    }
                }
            }
            runtime.set_standby().await;
            if let Some(lease) = collector.cancel(&mut lease) {
                let _ = persistence.release_history_lease(&lease).await;
            }
        })
        .map_err(|error| DashboardError::internal_source("Could not start history collector", error))?;
    Ok(())
}

async fn collect_history_sample(
    persistence: &DashboardPersistence,
    admin_facade: &WebAdminFacade,
    environment_id: &EnvironmentId,
    interval_secs: u64,
    lease: Option<&HistoryLease>,
) -> Result<(), DashboardError> {
    let overview = admin_facade.dashboard_overview().await?;
    let interval_ms = i64::try_from(interval_secs)
        .ok()
        .and_then(|seconds| seconds.checked_mul(1_000))
        .filter(|value| *value > 0)
        .ok_or_else(|| DashboardError::Config("history interval is invalid".to_string()))?;
    let now = Utc::now().timestamp_millis();
    let bucket_ms = now - now.rem_euclid(interval_ms);
    let mut samples = vec![MetricSample {
        environment_id: environment_id.clone(),
        metric: "broker-count".to_string(),
        bucket_ms,
        dimensions: Vec::new(),
        value: overview.broker_count as f64,
    }];
    if let Ok(topics) = admin_facade.provider().topic_current().await {
        samples.push(MetricSample {
            environment_id: environment_id.clone(),
            metric: "topic-count".to_string(),
            bucket_ms,
            dimensions: Vec::new(),
            value: topics.total_topics as f64,
        });
        samples.extend(topics.top_topics.into_iter().map(|topic| MetricSample {
            environment_id: environment_id.clone(),
            metric: "topic-total-messages".to_string(),
            bucket_ms,
            dimensions: vec![MetricDimension {
                key: "topic".to_string(),
                value: topic.topic,
            }],
            value: topic.total_msg as f64,
        }));
    }
    persistence.append_history(samples, lease).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::DashboardHistoryRuntime;
    use super::query_date_range;
    use crate::model::StorageBackend;

    #[test]
    fn history_date_is_a_single_utc_day() {
        let range = query_date_range("2026-08-24").expect("date range");
        assert_eq!(range.end_ms - range.start_ms, 86_399_999);
    }

    #[tokio::test]
    async fn lease_error_remains_visible_until_a_normal_standby_or_leader_transition() {
        let runtime = DashboardHistoryRuntime::new(StorageBackend::Sqlite);
        runtime.unavailable("history lease is unavailable").await;
        runtime.set_standby_after_error().await;
        let failed = runtime.health().await;
        assert_eq!(failed.connectivity, "unavailable");
        assert_eq!(failed.role, "standby");
        assert_eq!(failed.recent_error.as_deref(), Some("history lease is unavailable"));

        runtime.set_standby().await;
        let standby = runtime.health().await;
        assert_eq!(standby.connectivity, "available");
        assert!(standby.recent_error.is_none());

        runtime.unavailable("history collection is unavailable").await;
        runtime.set_leader(Some(1_000)).await;
        let leader = runtime.health().await;
        assert_eq!(leader.connectivity, "available");
        assert_eq!(leader.role, "leader");
        assert!(leader.recent_error.is_none());
    }
}
