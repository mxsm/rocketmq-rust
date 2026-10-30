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

//! Narrow Dashboard query and History sampling service.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::SystemTime,
};

use rocketmq_dashboard_common::{
    BrokerCurrentMetric, BrokerIdentity, BrokerInventoryItem, BrokerRole, DashboardOverview, DashboardOverviewEvidence,
    HISTORY_CLUSTER_SERIES, HistoryMetricKind, HistoryPoint, Observed, TopicCurrentFailure, TopicCurrentLoad,
    TopicCurrentObservation, TopicCurrentStage, broker_history_series_identity, project_dashboard_overview,
    rank_brokers,
};
use rocketmq_runtime::ChildServiceContext;

use super::RuntimeBridge;

use crate::{
    infrastructure::{
        admin_provider::{GpuiAdminProvider, SafeBrokerInfo, SafeBrokerList, SafeTopicRate},
        history_collector::{
            EnvironmentHistorySample, EnvironmentHistorySampleFuture, HistorySampleFuture, HistorySampler,
        },
        history_store::HistoryStore,
    },
    state::{UiError, UiErrorCode},
};

/// Real Dashboard application service. Admin client types never cross this boundary.
pub struct DashboardService {
    provider: Option<Arc<GpuiAdminProvider>>,
    history: Option<Arc<HistoryStore>>,
    history_runtime: Option<RuntimeBridge>,
}

/// Overview data plus a low-cardinality partial-failure warning count.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DashboardOverviewLoad {
    pub overview: DashboardOverview,
    pub failed_resources: usize,
}

impl DashboardOverviewLoad {
    pub fn has_warning(&self) -> bool {
        self.failed_resources > 0
    }
}

impl DashboardService {
    pub(crate) fn new(
        provider: Arc<GpuiAdminProvider>,
        history: Arc<HistoryStore>,
        history_context: ChildServiceContext,
    ) -> Arc<Self> {
        Arc::new(Self {
            provider: Some(provider),
            history: Some(history),
            history_runtime: Some(RuntimeBridge {
                context: history_context,
                #[cfg(test)]
                completion: None,
            }),
        })
    }

    pub(crate) fn unavailable() -> Arc<Self> {
        Arc::new(Self {
            provider: None,
            history: None,
            history_runtime: None,
        })
    }

    /// Builds independently evidenced overview fields from real Admin responses.
    pub async fn overview(&self, revision: u64) -> Result<DashboardOverviewLoad, UiError> {
        let provider = self.provider()?;
        let (health, brokers, topics, consumers, producers) = tokio::join!(
            provider.check_health(),
            provider.list_brokers(revision),
            provider.list_topics(revision),
            provider.consumer_summary(revision),
            provider.list_producers(revision),
        );

        let successful_resources = [
            health.is_ok(),
            brokers.is_ok(),
            topics.is_ok(),
            consumers.is_ok(),
            producers.is_ok(),
        ]
        .into_iter()
        .filter(|successful| *successful)
        .count();
        let mapped_brokers = brokers.as_ref().ok().map(map_broker_inventory);
        let evidence = DashboardOverviewEvidence {
            broker_count: mapped_brokers
                .as_ref()
                .and_then(|items| u64::try_from(items.len()).ok()),
            topic_count: topics
                .as_ref()
                .ok()
                .and_then(|items| u64::try_from(items.topics.len()).ok()),
            consumer_group_count: consumers
                .as_ref()
                .ok()
                .and_then(|items| items.group_count.and_then(|count| u64::try_from(count).ok())),
            producer_group_count: producers
                .as_ref()
                .ok()
                .and_then(|items| u64::try_from(items.distinct_group_count).ok()),
            complete_consumer_backlog: consumers.as_ref().ok().and_then(|items| items.complete_backlog),
            nameserver_availability: health.as_ref().ok().map(|health| health.availability),
            broker_availability: mapped_brokers
                .unwrap_or_default()
                .into_iter()
                .map(|item| item.availability)
                .collect(),
        };
        let mut load = project_overview_load(evidence, successful_resources)?;
        load.failed_resources += consumers.as_ref().ok().map_or(0, |items| items.failed_targets);
        Ok(load)
    }

    /// Samples at most twenty application Topics, retaining independent rate/offset failures.
    pub async fn topic_current(&self, revision: u64) -> Result<TopicCurrentLoad, UiError> {
        let provider = self.provider()?;
        let topics = provider.list_topics(revision).await.map_err(query_error)?;
        let application = topics.application_topics.into_iter().collect::<BTreeSet<_>>();
        let application_topics = application.len();
        let available_topics = application.into_iter().collect::<Vec<_>>();
        let candidates = available_topics
            .iter()
            .take(rocketmq_admin_core::core::dashboard_metrics::DASHBOARD_RATE_CANDIDATES)
            .cloned()
            .collect::<Vec<_>>();
        let rates = provider.topic_rates(revision, candidates.clone()).await;
        let rates = rates
            .unwrap_or_default()
            .into_iter()
            .map(|rate| (rate.topic.clone(), rate))
            .collect::<BTreeMap<_, _>>();
        let sampled = select_topic_candidates(&candidates, &rates);
        let mut items = Vec::with_capacity(sampled.len());
        let mut failed = candidates
            .iter()
            .filter(|topic| {
                rates.get(*topic).is_none_or(|rate| {
                    rate.failed_targets > 0 || rate.produce_tps.is_none() || rate.consume_tps.is_none()
                })
            })
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut failures = failed
            .iter()
            .map(|topic| TopicCurrentFailure {
                topic: topic.clone(),
                stage: TopicCurrentStage::Rates,
                failed_targets: rates.get(topic).map_or(1, |rate| rate.failed_targets.max(1)),
            })
            .collect::<Vec<_>>();
        for topic in sampled {
            let rate = rates.get(&topic);
            let stats = match rocketmq_dashboard_common::topic::TopicIdentity::parse(topic.clone()) {
                Ok(identity) => provider.detailed_topic_stats(revision, identity).await.ok(),
                Err(_) => None,
            };
            let total_messages = stats
                .as_ref()
                .filter(|stats| stats.completeness.is_complete())
                .and_then(|stats| u64::try_from(stats.total_message_count).ok());
            if total_messages.is_none() {
                failures.push(TopicCurrentFailure {
                    topic: topic.clone(),
                    stage: TopicCurrentStage::Offsets,
                    failed_targets: stats.as_ref().map_or(1, |stats| stats.failures.len().max(1)),
                });
            }
            let produce_tps = rate.and_then(|rate| rate.produce_tps);
            let consume_tps = rate.and_then(|rate| rate.consume_tps);
            if total_messages.is_none() || produce_tps.is_none() || consume_tps.is_none() {
                failed.insert(topic.clone());
            }
            items.push(TopicCurrentObservation {
                topic,
                total_messages: total_messages.map_or(Observed::Unknown, Observed::Observed),
                produce_tps: produce_tps.map_or(Observed::Unknown, Observed::Observed),
                consume_tps: consume_tps.map_or(Observed::Unknown, Observed::Observed),
            });
        }
        items.sort_by(|left, right| {
            match (&left.total_messages, &right.total_messages) {
                (Observed::Observed(left), Observed::Observed(right)) => right.cmp(left),
                (Observed::Observed(_), Observed::Unknown) => std::cmp::Ordering::Less,
                (Observed::Unknown, Observed::Observed(_)) => std::cmp::Ordering::Greater,
                _ => std::cmp::Ordering::Equal,
            }
            .then(left.topic.cmp(&right.topic))
        });
        Ok(TopicCurrentLoad {
            total_topics: topics.topics.len(),
            application_topics,
            available_topics,
            omitted_topics: application_topics.saturating_sub(candidates.len()),
            candidates,
            items,
            failed_topics: failed.len(),
            failures,
        })
    }

    /// Returns current Broker metrics from real inventory runtime evidence.
    pub async fn broker_current(&self, revision: u64) -> Result<Vec<BrokerCurrentMetric>, UiError> {
        let inventory = self.broker_inventory(revision).await?;
        Ok(rank_brokers(inventory.into_iter().map(inventory_metric).collect()))
    }

    /// Returns complete inventory for the Brokers page.
    pub async fn broker_inventory(&self, revision: u64) -> Result<Vec<BrokerInventoryItem>, UiError> {
        let provider = self.provider()?;
        provider
            .list_brokers(revision)
            .await
            .map(|response| map_broker_inventory(&response))
            .map_err(query_error)
    }

    /// Reads only persisted real points for a Topic series.
    pub async fn topic_history(
        &self,
        revision: u64,
        topic: String,
        start_epoch_ms: u64,
        end_epoch_ms: u64,
    ) -> Result<Vec<HistoryPoint>, UiError> {
        self.query_history(
            revision,
            HistoryMetricKind::TopicMessages,
            topic,
            start_epoch_ms,
            end_epoch_ms,
        )
        .await
    }

    /// Reads only persisted real points for a complete Broker identity.
    pub async fn broker_history(
        &self,
        revision: u64,
        metric: HistoryMetricKind,
        identity: BrokerIdentity,
        start_epoch_ms: u64,
        end_epoch_ms: u64,
    ) -> Result<Vec<HistoryPoint>, UiError> {
        if !matches!(
            metric,
            HistoryMetricKind::BrokerProduceTps | HistoryMetricKind::BrokerConsumeTps
        ) {
            return Err(UiError::new(
                "The selected History metric does not belong to a Broker.",
                UiErrorCode::Validation,
                false,
            ));
        }
        let series = broker_history_series_identity(&identity);
        self.query_history(revision, metric, series, start_epoch_ms, end_epoch_ms)
            .await
    }

    async fn query_history(
        &self,
        revision: u64,
        metric: HistoryMetricKind,
        series: String,
        start_epoch_ms: u64,
        end_epoch_ms: u64,
    ) -> Result<Vec<HistoryPoint>, UiError> {
        let history = Arc::clone(self.history()?);
        // Capture attribution before runtime admission: a queued read must never follow a new profile.
        let snapshot = history.environment_snapshot().map_err(history_error)?;
        validate_history_revision(snapshot.as_ref(), revision)?;
        let environment_id = snapshot.as_ref().map(|snapshot| snapshot.environment_id.clone());
        let query_history = Arc::clone(&history);
        let runtime = self.history_runtime.as_ref().ok_or_else(capability_unavailable)?;
        // Storage executor waits require Tokio's timer driver; GPUI only polls the bridge receiver.
        let points = runtime
            .run("gpui-history-query", async move {
                match environment_id {
                    Some(environment_id) => {
                        query_history
                            .query_environment(environment_id, metric, series, start_epoch_ms, end_epoch_ms)
                            .await
                    }
                    None => query_history.query(metric, series, start_epoch_ms, end_epoch_ms).await,
                }
                .map_err(history_error)
            })
            .await?;
        if history.environment_snapshot().map_err(history_error)? != snapshot {
            return Err(UiError::new(
                "The connection changed while History was queried.",
                UiErrorCode::Connection,
                true,
            ));
        }
        let mut points = points;
        if points.len() > 1_440 {
            points.drain(..points.len() - 1_440);
        }
        Ok(points)
    }

    pub async fn count_history(
        &self,
        revision: u64,
        metric: HistoryMetricKind,
        start: u64,
        end: u64,
    ) -> Result<Vec<HistoryPoint>, UiError> {
        if !matches!(metric, HistoryMetricKind::BrokerCount | HistoryMetricKind::TopicCount) {
            return Err(UiError::new(
                "Select a count History metric.",
                UiErrorCode::Validation,
                false,
            ));
        }
        self.query_history(revision, metric, HISTORY_CLUSTER_SERIES.into(), start, end)
            .await
    }

    pub fn collection_health(
        &self,
        revision: u64,
    ) -> Result<crate::infrastructure::history_health::HistoryCollectionHealth, UiError> {
        let history = self.history()?;
        let snapshot = history.environment_snapshot().map_err(history_error)?;
        validate_history_revision(snapshot.as_ref(), revision)?;
        history.collection_health().map_err(history_error)
    }

    fn provider(&self) -> Result<&Arc<GpuiAdminProvider>, UiError> {
        self.provider.as_ref().ok_or_else(capability_unavailable)
    }

    fn history(&self) -> Result<&Arc<HistoryStore>, UiError> {
        self.history.as_ref().ok_or_else(capability_unavailable)
    }
}

impl HistorySampler for DashboardService {
    fn sample_topics_scoped(&self) -> EnvironmentHistorySampleFuture<'_> {
        Box::pin(async move {
            let snapshot = self.history()?.environment_snapshot().map_err(history_error)?;
            let revision = snapshot
                .as_ref()
                .map(|snapshot| snapshot.connection.revision)
                .or_else(|| self.provider.as_ref().and_then(|provider| provider.revision()))
                .ok_or_else(capability_unavailable)?;
            let topics = self.topic_current(revision).await?;
            let failed_resources = topics.failed_topics + topics.omitted_topics;
            let timestamp = sample_timestamp()?;
            let points = topic_sample_points(topics, revision, timestamp);
            let mut sample = scoped_sample(snapshot, points)?;
            sample.failed_resources = failed_resources;
            Ok(sample)
        })
    }

    fn sample_brokers_scoped(&self) -> EnvironmentHistorySampleFuture<'_> {
        Box::pin(async move {
            let snapshot = self.history()?.environment_snapshot().map_err(history_error)?;
            let revision = snapshot
                .as_ref()
                .map(|snapshot| snapshot.connection.revision)
                .or_else(|| self.provider.as_ref().and_then(|provider| provider.revision()))
                .ok_or_else(capability_unavailable)?;
            let brokers = self.broker_current(revision).await?;
            let failed_resources = brokers
                .iter()
                .filter(|broker| {
                    matches!(broker.produce_tps, Observed::Unknown) || matches!(broker.consume_tps, Observed::Unknown)
                })
                .count();
            let points = broker_sample_points(brokers, revision, sample_timestamp()?);
            let mut sample = scoped_sample(snapshot, points)?;
            sample.failed_resources = failed_resources;
            Ok(sample)
        })
    }
    fn sample_topics(&self) -> HistorySampleFuture<'_> {
        Box::pin(async move {
            let provider = self.provider()?;
            let revision = provider.revision().ok_or_else(capability_unavailable)?;
            let topics = self.topic_current(revision).await?;
            let points = topic_sample_points(topics, revision, sample_timestamp()?);
            Ok(points)
        })
    }

    fn sample_brokers(&self) -> HistorySampleFuture<'_> {
        Box::pin(async move {
            let provider = self.provider()?;
            let revision = provider.revision().ok_or_else(capability_unavailable)?;
            let brokers = self.broker_current(revision).await?;
            let points = broker_sample_points(brokers, revision, sample_timestamp()?);
            Ok(points)
        })
    }
}

fn topic_sample_points(topics: TopicCurrentLoad, revision: u64, timestamp_epoch_ms: u64) -> Vec<HistoryPoint> {
    let mut points = vec![HistoryPoint {
        metric: HistoryMetricKind::TopicCount,
        series_identity: HISTORY_CLUSTER_SERIES.into(),
        timestamp_epoch_ms,
        value: topics.total_topics as f64,
        source_revision: revision,
    }];
    for topic in topics.items {
        if let Observed::Observed(value) = topic.total_messages {
            points.push(HistoryPoint {
                metric: HistoryMetricKind::TopicMessages,
                series_identity: topic.topic,
                timestamp_epoch_ms,
                value: value as f64,
                source_revision: revision,
            });
        }
    }
    points
}

fn broker_sample_points(
    brokers: Vec<BrokerCurrentMetric>,
    revision: u64,
    timestamp_epoch_ms: u64,
) -> Vec<HistoryPoint> {
    let mut points = vec![HistoryPoint {
        metric: HistoryMetricKind::BrokerCount,
        series_identity: HISTORY_CLUSTER_SERIES.into(),
        timestamp_epoch_ms,
        value: brokers.len() as f64,
        source_revision: revision,
    }];
    for broker in brokers {
        let series_identity = broker_history_series_identity(&broker.identity);
        for (metric, observation) in [
            (HistoryMetricKind::BrokerProduceTps, broker.produce_tps),
            (HistoryMetricKind::BrokerConsumeTps, broker.consume_tps),
        ] {
            if let Observed::Observed(value) = observation {
                points.push(HistoryPoint {
                    metric,
                    series_identity: series_identity.clone(),
                    timestamp_epoch_ms,
                    value,
                    source_revision: revision,
                });
            }
        }
    }
    points
}

fn select_topic_candidates(candidates: &[String], rates: &BTreeMap<String, SafeTopicRate>) -> Vec<String> {
    let mut sampled = candidates.to_vec();
    sampled.sort_by(|left, right| {
        let activity = |topic: &String| {
            rates.get(topic).map_or((0, 0.0), |rate| {
                (rate.produced_messages_24h.unwrap_or(0), rate.produce_tps.unwrap_or(0.0))
            })
        };
        let (left_count, left_tps) = activity(left);
        let (right_count, right_tps) = activity(right);
        right_count
            .cmp(&left_count)
            .then(right_tps.total_cmp(&left_tps))
            .then(left.cmp(right))
    });
    sampled.truncate(20);
    sampled
}

fn scoped_sample(
    snapshot: Option<rocketmq_dashboard_common::EnvironmentConnectionSnapshot>,
    points: Vec<HistoryPoint>,
) -> Result<EnvironmentHistorySample, UiError> {
    if let Some(snapshot) = snapshot {
        if points
            .iter()
            .any(|point| point.source_revision != snapshot.connection.revision)
        {
            return Err(UiError::new(
                "The connection changed while metrics were sampled.",
                UiErrorCode::Connection,
                true,
            ));
        }
        Ok(EnvironmentHistorySample {
            failed_resources: 0,
            environment_id: Some(snapshot.environment_id),
            points,
        })
    } else {
        Ok(EnvironmentHistorySample {
            failed_resources: 0,
            environment_id: None,
            points,
        })
    }
}

fn validate_history_revision(
    snapshot: Option<&rocketmq_dashboard_common::EnvironmentConnectionSnapshot>,
    revision: u64,
) -> Result<(), UiError> {
    if snapshot.is_some_and(|snapshot| snapshot.connection.revision != revision) {
        return Err(UiError::new(
            "The connection changed before History was queried.",
            UiErrorCode::Connection,
            true,
        ));
    }
    Ok(())
}

fn sample_timestamp() -> Result<u64, UiError> {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .ok_or_else(|| UiError::new("System time is unavailable.", UiErrorCode::Unknown, true))
}

pub(crate) fn map_broker_inventory(response: &SafeBrokerList) -> Vec<BrokerInventoryItem> {
    response.items.iter().map(map_broker).collect()
}

fn map_broker(item: &SafeBrokerInfo) -> BrokerInventoryItem {
    let identity = BrokerIdentity {
        cluster: item.cluster_name.clone(),
        broker_name: item.broker_name.clone(),
        broker_id: item.broker_id,
        address: item.address.clone(),
    };
    BrokerInventoryItem {
        identity,
        role: BrokerRole::classify(&item.role),
        version: item.version.clone().map_or(Observed::Unknown, Observed::Observed),
        availability: item.availability,
        produce_tps: item.produce_tps.map_or(Observed::Unknown, Observed::Observed),
        consume_tps: item.consume_tps.map_or(Observed::Unknown, Observed::Observed),
    }
}

fn inventory_metric(item: BrokerInventoryItem) -> BrokerCurrentMetric {
    let combined_tps = match (&item.produce_tps, &item.consume_tps) {
        (Observed::Observed(produce), Observed::Observed(consume)) => Observed::Observed(produce + consume),
        _ => Observed::Unknown,
    };
    BrokerCurrentMetric {
        identity: item.identity,
        version: item.version,
        availability: item.availability,
        produce_tps: item.produce_tps,
        consume_tps: item.consume_tps,
        combined_tps,
    }
}

fn capability_unavailable() -> UiError {
    UiError::new(
        "Dashboard data is unavailable in this application configuration.",
        UiErrorCode::CapabilityUnavailable,
        false,
    )
}

fn project_overview_load(
    evidence: DashboardOverviewEvidence,
    successful_resources: usize,
) -> Result<DashboardOverviewLoad, UiError> {
    if successful_resources == 0 {
        return Err(UiError::new(
            "Unable to load Dashboard data from the selected connection.",
            UiErrorCode::Connection,
            true,
        ));
    }
    Ok(DashboardOverviewLoad {
        overview: project_dashboard_overview(evidence),
        failed_resources: 5usize.saturating_sub(successful_resources),
    })
}

fn query_error(source: crate::infrastructure::admin_provider::ProviderFailure) -> UiError {
    let retryable = source.is_retryable();
    match source.into_operational() {
        Some(source) => UiError::caused_by(
            "Unable to load Dashboard data from the selected connection.",
            UiErrorCode::Connection,
            retryable,
            source,
        ),
        None => UiError::new(
            "Unable to load Dashboard data from the selected connection.",
            UiErrorCode::Connection,
            retryable,
        ),
    }
}

fn history_error(source: crate::infrastructure::config_store::ConfigStoreFailure) -> UiError {
    let retryable = source.is_retryable();
    match source.into_operational() {
        Some(source) => UiError::caused_by(
            "Unable to load local metric History.",
            UiErrorCode::Configuration,
            retryable,
            source,
        ),
        None => UiError::new(
            "Unable to load local metric History.",
            UiErrorCode::Configuration,
            retryable,
        ),
    }
}

#[cfg(test)]
#[path = "dashboard_history_tests.rs"]
mod history_runtime_tests;

#[cfg(test)]
mod tests {
    use rocketmq_dashboard_common::EndpointAvailability;

    use super::*;

    #[test]
    fn history_service_rejects_old_requests_and_filters_the_selected_profile() {
        use rocketmq_dashboard_common::{OperationActor, OperationResource};
        use rocketmq_runtime::{ProcessMemoryLimit, RuntimeConfig, RuntimeOwner};

        use crate::infrastructure::{config_store::DesktopConfigStore, environment_store::EnvironmentChange};

        let directory = tempfile::tempdir().unwrap();
        let runtime = RuntimeOwner::plan(RuntimeConfig::for_parallelism("history-service-test", 1))
            .unwrap()
            .with_memory_limit(ProcessMemoryLimit::configured(256 * 1024 * 1024).unwrap())
            .build()
            .unwrap();
        let context = runtime.root_context().component("history");
        let configuration = DesktopConfigStore::new(directory.path().join("config.json"), context.clone());
        let history = HistoryStore::with_config_store(
            directory.path().join("history.json"),
            context.clone(),
            Arc::clone(&configuration),
        );
        let service = DashboardService {
            provider: None,
            history: Some(Arc::clone(&history)),
            history_runtime: Some(RuntimeBridge {
                context,
                completion: None,
            }),
        };
        runtime.block_on(async {
            let first = configuration.load().await.unwrap();
            let point = |revision, value| HistoryPoint {
                metric: HistoryMetricKind::TopicMessages,
                series_identity: "orders".into(),
                timestamp_epoch_ms: 1,
                value,
                source_revision: revision,
            };
            let retention = rocketmq_dashboard_common::HistoryRetention {
                max_points_per_series: 10,
                max_series: 10,
                max_total_points: 10,
            };
            let first_id = configuration.environments().unwrap().active_id;
            history
                .append_environment_sample(first_id, vec![point(first.revision, 3.0)], retention)
                .await
                .unwrap();
            assert_eq!(
                service
                    .topic_history(first.revision, "orders".into(), 0, 2)
                    .await
                    .unwrap()[0]
                    .value,
                3.0
            );
            let context = configuration
                .operation_context(
                    first.revision,
                    OperationActor::LocalDesktop,
                    OperationResource::Configuration,
                )
                .unwrap();
            let second = configuration
                .change_environment(EnvironmentChange::Create { name: "Staging".into() }, context)
                .await
                .unwrap();
            let second_id = configuration.environments().unwrap().active_id;
            history
                .append_environment_sample(second_id, vec![point(second.revision, 9.0)], retention)
                .await
                .unwrap();
            assert!(
                service
                    .topic_history(first.revision, "orders".into(), 0, 2)
                    .await
                    .is_err()
            );
            assert_eq!(
                service
                    .topic_history(second.revision, "orders".into(), 0, 2)
                    .await
                    .unwrap()[0]
                    .value,
                9.0
            );
        });
        runtime.shutdown_runtime_blocking().unwrap();
    }

    fn broker(
        version: Option<String>,
        produce_tps: Option<f64>,
        consume_tps: Option<f64>,
        availability: EndpointAvailability,
    ) -> SafeBrokerInfo {
        SafeBrokerInfo {
            cluster_name: "c".into(),
            broker_name: "b".into(),
            broker_id: 0,
            address: "127.0.0.1:10911".into(),
            role: "MASTER".into(),
            version,
            produce_tps,
            consume_tps,
            availability,
        }
    }

    #[test]
    fn absent_or_failed_runtime_never_maps_default_zero_to_observed_or_healthy() {
        let failed = map_broker(&broker(None, None, None, EndpointAvailability::Unavailable));
        assert_eq!(failed.availability, EndpointAvailability::Unavailable);
        assert_eq!(failed.produce_tps, Observed::Unknown);
        assert_eq!(failed.consume_tps, Observed::Unknown);
        assert_eq!(failed.version, Observed::Unknown);

        let missing = map_broker(&broker(None, None, None, EndpointAvailability::Available));
        assert_eq!(missing.availability, EndpointAvailability::Available);
        assert_eq!(missing.produce_tps, Observed::Unknown);
        assert_eq!(missing.consume_tps, Observed::Unknown);
    }

    #[test]
    fn only_present_valid_runtime_values_are_observed() {
        let item = map_broker(&broker(
            Some("V5_3_0".into()),
            Some(0.0),
            Some(9.5),
            EndpointAvailability::Available,
        ));
        assert_eq!(item.version, Observed::Observed("V5_3_0".into()));
        assert_eq!(item.produce_tps, Observed::Observed(0.0));
        assert_eq!(item.consume_tps, Observed::Observed(9.5));
    }

    #[test]
    fn overview_requires_one_success_and_marks_partial_data_without_inventing_values() {
        assert!(project_overview_load(DashboardOverviewEvidence::default(), 0).is_err());
        let partial = project_overview_load(
            DashboardOverviewEvidence {
                topic_count: Some(4),
                ..Default::default()
            },
            1,
        )
        .expect("partial overview");
        assert_eq!(partial.overview.topic_count, Observed::Observed(4));
        assert_eq!(partial.overview.broker_count, Observed::Unknown);
        assert_eq!(partial.failed_resources, 4);
        assert!(partial.has_warning());

        let complete = project_overview_load(DashboardOverviewEvidence::default(), 5).expect("complete overview");
        assert!(!complete.has_warning());
    }
}
