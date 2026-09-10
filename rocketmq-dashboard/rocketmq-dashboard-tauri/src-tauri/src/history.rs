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

mod repository;
use crate::{
    auth::SessionState,
    cluster::ClusterManager,
    connection::ConnectionManager,
    error::{CommandResult, DashboardError, DashboardResult, authorize_command},
    persistence::StorageManager,
    topic::TopicManager,
};
use repository::{HistoryMetric, HistoryPage, HistoryQuery, Sample};
use rocketmq_dashboard_common::{ClusterHomePageRequest, TopicListRequest};
use serde::Serialize;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tauri::State;

#[derive(Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CollectorStatus {
    pub(crate) interval_seconds: u64,
    pub(crate) retention_days: u64,
    pub(crate) last_sample_ms: Option<i64>,
    pub(crate) last_write_ms: Option<i64>,
    pub(crate) last_error: Option<&'static str>,
}

#[derive(Clone)]
pub(crate) struct HistoryManager {
    storage: StorageManager,
    connections: ConnectionManager,
    cluster: ClusterManager,
    topic: TopicManager,
    status: Arc<Mutex<CollectorStatus>>,
    interval_seconds: u64,
    retention_days: u64,
}

impl HistoryManager {
    pub(crate) fn new(
        storage: StorageManager,
        connections: ConnectionManager,
        cluster: ClusterManager,
        topic: TopicManager,
    ) -> DashboardResult<Self> {
        let interval_seconds = setting("DASHBOARD_TAURI_HISTORY_INTERVAL_SECONDS", 60, 15, 3600)?;
        let retention_days = setting("DASHBOARD_TAURI_HISTORY_RETENTION_DAYS", 30, 1, 365)?;
        Ok(Self {
            storage,
            connections,
            cluster,
            topic,
            interval_seconds,
            retention_days,
            status: Arc::new(Mutex::new(CollectorStatus {
                interval_seconds,
                retention_days,
                ..CollectorStatus::default()
            })),
        })
    }

    pub(crate) fn status(&self) -> DashboardResult<CollectorStatus> {
        self.status
            .lock()
            .map(|status| status.clone())
            .map_err(|_| DashboardError::Internal("history state unavailable"))
    }

    pub(crate) fn start(&self) -> DashboardResult<()> {
        let manager = self.clone();
        // Storage shutdown cancels and awaits this loop before closing the admin managers.
        self.storage.start_background("history-collector", async move {
            let mut interval = tokio::time::interval(Duration::from_secs(manager.interval_seconds));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if manager.collect().await.is_err() {
                    manager.record_error("History sampling or storage is unavailable.");
                }
                let cutoff = chrono::Utc::now().timestamp_millis() - manager.retention_days as i64 * 86_400_000;
                loop {
                    match manager
                        .storage
                        .run("history-retention", move |connection| {
                            repository::cleanup(connection, cutoff)
                        })
                        .await
                    {
                        Ok(1000) => continue,
                        Ok(_) => break,
                        Err(_) => {
                            manager.record_error("History retention cleanup is unavailable.");
                            break;
                        }
                    }
                }
            }
        })
    }

    fn record_error(&self, error: &'static str) {
        if let Ok(mut status) = self.status.lock() {
            status.last_error = Some(error);
        }
    }

    async fn collect(&self) -> DashboardResult<()> {
        let snapshot = self.connections.snapshot()?;
        let Some(environment) = snapshot.environment_id else {
            return Ok(());
        };
        let revision = snapshot.revision;
        let timestamp_ms = chrono::Utc::now().timestamp_millis();
        let (brokers, topics, statistics) = tokio::join!(
            self.cluster
                .get_cluster_home_page(ClusterHomePageRequest { force_refresh: false }),
            self.topic.get_topic_list(TopicListRequest {
                skip_sys_process: false,
                skip_retry_and_dlq: false
            }),
            self.topic.get_topic_current_stats(),
        );
        let partial =
            brokers.is_err() || topics.is_err() || statistics.as_ref().map_or(true, |stats| !stats.failures.is_empty());
        let mut samples = Vec::new();
        if let Ok(brokers) = brokers {
            samples.push(Sample {
                metric: HistoryMetric::BrokerCount,
                dimension: String::new(),
                timestamp_ms,
                value: brokers.summary.total_brokers as f64,
            });
        }
        if let Ok(topics) = topics {
            samples.push(Sample {
                metric: HistoryMetric::TopicCount,
                dimension: String::new(),
                timestamp_ms,
                value: topics.total as f64,
            });
        }
        if let Ok(statistics) = statistics {
            samples.extend(statistics.items.into_iter().map(|topic| Sample {
                metric: HistoryMetric::TopicTotalMessages,
                dimension: topic.topic,
                timestamp_ms,
                value: topic.total_msg as f64,
            }));
        }
        if samples.is_empty() {
            self.record_error("No history metrics could be sampled.");
            return Ok(());
        }
        // IMMEDIATE checks the persisted revision in the same transaction as the sample writes.
        // A settings commit cannot race that check and attach old results to a new environment.
        let committed = self
            .storage
            .run("history-sample", move |connection| {
                repository::insert(connection, &environment, revision, &samples)
            })
            .await?;
        if committed {
            let mut status = self
                .status
                .lock()
                .map_err(|_| DashboardError::Internal("history state unavailable"))?;
            status.last_sample_ms = Some(timestamp_ms);
            status.last_write_ms = Some(chrono::Utc::now().timestamp_millis());
            status.last_error =
                partial.then_some("Some metrics were unavailable; only successful samples were stored.");
        } else {
            self.record_error("An obsolete sample was discarded after connection settings changed.");
        }
        Ok(())
    }

    async fn query(&self, metric: HistoryMetric, request: HistoryQuery) -> DashboardResult<HistoryPage> {
        let environment = self.connections.snapshot()?.environment_id.ok_or_else(|| {
            DashboardError::Configuration("Select a NameServer environment before reading history.".into())
        })?;
        self.storage
            .run("history-query", move |connection| {
                repository::query(connection, &environment, metric, request)
            })
            .await
    }
}

fn setting(name: &'static str, default: u64, minimum: u64, maximum: u64) -> DashboardResult<u64> {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|value| (minimum..=maximum).contains(value))
            .ok_or_else(|| DashboardError::Configuration(format!("Invalid {name}."))),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(_) => Err(DashboardError::Configuration(format!("Invalid {name}."))),
    }
}

#[tauri::command]
pub async fn query_broker_history(
    session_id: String,
    expected_revision: i64,
    request: HistoryQuery,
    session_state: State<'_, SessionState>,
    connection_manager: State<'_, ConnectionManager>,
    history: State<'_, HistoryManager>,
) -> CommandResult<HistoryPage> {
    authorize_command(&session_id, &session_state).await?;
    connection_manager.check_revision(expected_revision)?;
    let result = history
        .query(
            HistoryMetric::BrokerCount,
            HistoryQuery {
                topic_name: None,
                ..request
            },
        )
        .await
        .map_err(Into::into);
    connection_manager.check_revision(expected_revision)?;
    result
}

#[tauri::command]
pub async fn query_topic_history(
    session_id: String,
    expected_revision: i64,
    request: HistoryQuery,
    session_state: State<'_, SessionState>,
    connection_manager: State<'_, ConnectionManager>,
    history: State<'_, HistoryManager>,
) -> CommandResult<HistoryPage> {
    authorize_command(&session_id, &session_state).await?;
    connection_manager.check_revision(expected_revision)?;
    let metric = if request.topic_name.as_ref().is_some_and(|name| !name.is_empty()) {
        HistoryMetric::TopicTotalMessages
    } else {
        HistoryMetric::TopicCount
    };
    let result = history.query(metric, request).await.map_err(Into::into);
    connection_manager.check_revision(expected_revision)?;
    result
}

#[tauri::command]
pub async fn get_history_status(
    session_id: String,
    session_state: State<'_, SessionState>,
    history: State<'_, HistoryManager>,
) -> CommandResult<CollectorStatus> {
    session_state.authorize_read_only(&session_id).await?;
    history.status().map_err(Into::into)
}
