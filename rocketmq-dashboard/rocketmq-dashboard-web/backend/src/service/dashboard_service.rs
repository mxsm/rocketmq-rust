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
use crate::model::DashboardHistoryPoint;
use crate::model::DashboardHistoryQuery;
use crate::model::DashboardHistorySeries;
use crate::model::DashboardOverview;
use crate::model::DashboardTopicCurrent;
use crate::state::AppState;
use crate::state::WebAdminFacade;
use chrono::Utc;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::RwLock;
use tokio::task::AbortHandle;
use tokio::time::Duration;

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
    Ok(state.history_store.broker_series(query).await)
}

pub async fn topic_history(
    state: &AppState,
    query: DashboardHistoryQuery,
) -> Result<DashboardHistorySeries, DashboardError> {
    Ok(state.history_store.topic_series(query).await)
}

#[derive(Debug, Clone, Default)]
pub struct DashboardHistoryStore {
    samples: Arc<RwLock<VecDeque<DashboardHistorySample>>>,
}

#[derive(Debug, Clone, Default)]
pub struct DashboardTaskManager {
    inner: Arc<DashboardTaskManagerInner>,
}

#[derive(Debug, Default)]
struct DashboardTaskManagerInner {
    abort_handles: Mutex<Vec<AbortHandle>>,
}

impl DashboardTaskManager {
    pub fn spawn<F>(&self, task_name: &'static str, task: F) -> Result<(), DashboardError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut abort_handles = self.inner.abort_handles.lock().map_err(|_| {
            DashboardError::Internal(format!(
                "dashboard task manager lock poisoned while spawning {task_name}"
            ))
        })?;
        let handle = tokio::task::spawn(async move {
            tracing::debug!(task = task_name, "Dashboard background task started");
            task.await;
            tracing::debug!(task = task_name, "Dashboard background task stopped");
        });
        abort_handles.push(handle.abort_handle());
        drop(handle);
        Ok(())
    }
}

impl Drop for DashboardTaskManagerInner {
    fn drop(&mut self) {
        if let Ok(abort_handles) = self.abort_handles.get_mut() {
            for abort_handle in abort_handles.drain(..) {
                abort_handle.abort();
            }
        }
    }
}

#[derive(Debug, Clone)]
struct DashboardHistorySample {
    date: String,
    timestamp: i64,
    broker_count: f64,
    topic_count: f64,
    topics: Vec<TopicHistorySample>,
}

#[derive(Debug, Clone)]
struct TopicHistorySample {
    topic: String,
    total_msg: f64,
}

impl DashboardHistoryStore {
    const MAX_SAMPLES: usize = 2_880;

    pub async fn record(&self, overview: DashboardOverview, topic_current: DashboardTopicCurrent) {
        let now = Utc::now();
        let sample = DashboardHistorySample {
            date: now.format("%Y-%m-%d").to_string(),
            timestamp: now.timestamp_millis(),
            broker_count: overview.broker_count as f64,
            topic_count: topic_current.total_topics as f64,
            topics: topic_current
                .top_topics
                .into_iter()
                .map(|topic| TopicHistorySample {
                    topic: topic.topic,
                    total_msg: topic.total_msg as f64,
                })
                .collect(),
        };

        let mut samples = self.samples.write().await;
        samples.push_back(sample);
        while samples.len() > Self::MAX_SAMPLES {
            samples.pop_front();
        }
    }

    pub async fn broker_series(&self, query: DashboardHistoryQuery) -> DashboardHistorySeries {
        let samples = self.samples.read().await;
        let points = samples
            .iter()
            .filter(|sample| sample.date == query.date)
            .map(|sample| DashboardHistoryPoint {
                timestamp: sample.timestamp,
                value: sample.broker_count,
            })
            .collect::<Vec<_>>();
        DashboardHistorySeries {
            date: query.date,
            metric: "broker".to_string(),
            topic_name: None,
            collected: !points.is_empty(),
            points,
        }
    }

    pub async fn topic_series(&self, query: DashboardHistoryQuery) -> DashboardHistorySeries {
        let samples = self.samples.read().await;
        let points = samples
            .iter()
            .filter(|sample| sample.date == query.date)
            .filter_map(|sample| {
                let value = match query.topic_name.as_deref() {
                    Some(topic_name) => sample
                        .topics
                        .iter()
                        .find(|topic| topic.topic == topic_name)
                        .map(|topic| topic.total_msg),
                    None => Some(sample.topic_count),
                }?;
                Some(DashboardHistoryPoint {
                    timestamp: sample.timestamp,
                    value,
                })
            })
            .collect::<Vec<_>>();
        DashboardHistorySeries {
            date: query.date,
            metric: "topic".to_string(),
            topic_name: query.topic_name,
            collected: !points.is_empty(),
            points,
        }
    }
}

pub fn spawn_dashboard_history_collector(
    task_manager: &DashboardTaskManager,
    admin_facade: WebAdminFacade,
    history_store: DashboardHistoryStore,
    interval_secs: u64,
) -> Result<(), DashboardError> {
    task_manager.spawn("dashboard-history-collector", async move {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));
        loop {
            interval.tick().await;
            match collect_history_sample(&admin_facade, &history_store).await {
                Ok(()) => {}
                Err(error) => {
                    tracing::debug!(error = %error, "Dashboard history sample collection failed");
                }
            }
        }
    })
}

async fn collect_history_sample(
    admin_facade: &WebAdminFacade,
    history_store: &DashboardHistoryStore,
) -> Result<(), DashboardError> {
    let overview = admin_facade.dashboard_overview().await?;
    let topic_current = match admin_facade.provider().topic_current().await {
        Ok(topic_current) => topic_current,
        Err(error) => {
            tracing::debug!(error = %error, "Topic current collection failed; recording empty topic history sample");
            DashboardTopicCurrent {
                total_topics: 0,
                top_topics: Vec::new(),
            }
        }
    };
    history_store.record(overview, topic_current).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::DashboardHistoryStore;
    use super::DashboardTaskManager;
    use crate::model::DashboardHistoryQuery;
    use crate::model::DashboardOverview;
    use crate::model::DashboardTopicCurrent;
    use crate::model::TopicCurrentMetric;
    use chrono::Utc;
    use std::future;
    use tokio::sync::oneshot;
    use tokio::time::Duration;
    use tokio::time::timeout;

    struct DropSignal(Option<oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[tokio::test]
    async fn dashboard_task_manager_drop_aborts_spawned_tasks() {
        let manager = DashboardTaskManager::default();
        let (started_tx, started_rx) = oneshot::channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();

        manager
            .spawn("dashboard-test-task", async move {
                let _drop_signal = DropSignal(Some(dropped_tx));
                let _ = started_tx.send(());
                future::pending::<()>().await;
            })
            .expect("dashboard task should spawn");

        started_rx.await.expect("task should start");
        drop(manager);

        timeout(Duration::from_secs(1), dropped_rx)
            .await
            .expect("task should be aborted when manager is dropped")
            .expect("drop signal should be sent");
    }

    #[tokio::test]
    async fn history_store_returns_broker_and_topic_points() {
        let store = DashboardHistoryStore::default();
        let date = Utc::now().format("%Y-%m-%d").to_string();
        store
            .record(
                DashboardOverview {
                    current_namesrv: Some("127.0.0.1:9876".to_string()),
                    broker_count: 3,
                    topic_count: 2,
                    consumer_group_count: 1,
                    producer_count: 1,
                    message_backlog: 0,
                    system_status: "UP".to_string(),
                },
                DashboardTopicCurrent {
                    total_topics: 2,
                    top_topics: vec![TopicCurrentMetric {
                        topic: "TopicTest".to_string(),
                        total_msg: 42,
                        in_tps: 0.0,
                        out_tps: 0.0,
                    }],
                },
            )
            .await;

        let broker_series = store
            .broker_series(DashboardHistoryQuery {
                date: date.clone(),
                topic_name: None,
            })
            .await;
        assert!(broker_series.collected);
        assert_eq!(broker_series.points[0].value, 3.0);

        let topic_series = store
            .topic_series(DashboardHistoryQuery {
                date,
                topic_name: Some("TopicTest".to_string()),
            })
            .await;
        assert!(topic_series.collected);
        assert_eq!(topic_series.points[0].value, 42.0);
    }
}
