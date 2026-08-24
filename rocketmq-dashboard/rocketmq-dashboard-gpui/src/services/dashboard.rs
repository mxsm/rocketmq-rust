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

use std::{collections::BTreeSet, sync::Arc, time::SystemTime};

use rocketmq_dashboard_common::{
    BrokerCurrentMetric, BrokerIdentity, BrokerInventoryItem, BrokerRole, DashboardOverview, DashboardOverviewEvidence,
    HistoryMetricKind, HistoryPoint, Observed, TopicCurrentMetric, broker_history_series_identity,
    project_dashboard_overview, rank_brokers, rank_topics, topic_current_from_offsets,
};

use crate::{
    infrastructure::{
        admin_provider::{GpuiAdminProvider, SafeBrokerInfo, SafeBrokerList},
        history_collector::{HistorySampleFuture, HistorySampler},
        history_store::HistoryStore,
    },
    state::{UiError, UiErrorCode},
};

/// Real Dashboard application service. Admin client types never cross this boundary.
pub struct DashboardService {
    provider: Option<Arc<GpuiAdminProvider>>,
    history: Option<Arc<HistoryStore>>,
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
    pub(crate) fn new(provider: Arc<GpuiAdminProvider>, history: Arc<HistoryStore>) -> Arc<Self> {
        Arc::new(Self {
            provider: Some(provider),
            history: Some(history),
        })
    }

    pub(crate) fn unavailable() -> Arc<Self> {
        Arc::new(Self {
            provider: None,
            history: None,
        })
    }

    /// Builds independently evidenced overview fields from real Admin responses.
    pub async fn overview(&self, revision: u64) -> Result<DashboardOverviewLoad, UiError> {
        let provider = self.provider()?;
        let (health, brokers, topics, consumers, producers) = tokio::join!(
            provider.check_health(),
            provider.list_brokers(revision),
            provider.list_topics(revision),
            provider.list_consumers(revision),
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
                .and_then(|items| u64::try_from(items.group_count).ok()),
            producer_group_count: producers
                .as_ref()
                .ok()
                .and_then(|items| u64::try_from(items.distinct_group_count).ok()),
            // The list contract does not prove that every backlog partition was returned.
            complete_consumer_backlog: None,
            nameserver_availability: health.as_ref().ok().map(|health| health.availability),
            broker_availability: mapped_brokers
                .unwrap_or_default()
                .into_iter()
                .map(|item| item.availability)
                .collect(),
        };
        project_overview_load(evidence, successful_resources)
    }

    /// Returns current Topic metrics backed only by successful offset responses.
    pub async fn topic_current(&self, revision: u64) -> Result<Vec<TopicCurrentMetric>, UiError> {
        let provider = self.provider()?;
        let topics = provider.list_topics(revision).await.map_err(query_error)?;
        let names = topics.topics.into_iter().collect::<BTreeSet<_>>();
        let mut metrics = Vec::with_capacity(names.len());
        for topic in names {
            let stats = provider.topic_stats(revision, topic).await.map_err(query_error)?;
            metrics.push(topic_current_from_offsets(
                stats.topic,
                stats.total_min_offset,
                stats.total_max_offset,
            ));
        }
        Ok(rank_topics(metrics))
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
        topic: String,
        start_epoch_ms: u64,
        end_epoch_ms: u64,
    ) -> Result<Vec<HistoryPoint>, UiError> {
        self.history()?
            .query(HistoryMetricKind::TopicMessages, topic, start_epoch_ms, end_epoch_ms)
            .await
            .map_err(history_error)
    }

    /// Reads only persisted real points for a complete Broker identity.
    pub async fn broker_history(
        &self,
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
        self.history()?
            .query(
                metric,
                broker_history_series_identity(&identity),
                start_epoch_ms,
                end_epoch_ms,
            )
            .await
            .map_err(history_error)
    }

    fn provider(&self) -> Result<&Arc<GpuiAdminProvider>, UiError> {
        self.provider.as_ref().ok_or_else(capability_unavailable)
    }

    fn history(&self) -> Result<&Arc<HistoryStore>, UiError> {
        self.history.as_ref().ok_or_else(capability_unavailable)
    }
}

impl HistorySampler for DashboardService {
    fn sample_topics(&self) -> HistorySampleFuture<'_> {
        Box::pin(async move {
            let provider = self.provider()?;
            let revision = provider.revision().ok_or_else(capability_unavailable)?;
            let topics = self.topic_current(revision).await?;
            let timestamp_epoch_ms = sample_timestamp()?;
            let mut points = Vec::new();
            for topic in topics {
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
            Ok(points)
        })
    }

    fn sample_brokers(&self) -> HistorySampleFuture<'_> {
        Box::pin(async move {
            let provider = self.provider()?;
            let revision = provider.revision().ok_or_else(capability_unavailable)?;
            let brokers = self.broker_current(revision).await?;
            let timestamp_epoch_ms = sample_timestamp()?;
            let mut points = Vec::new();
            for broker in brokers {
                let series_identity = broker_history_series_identity(&broker.identity);
                if let Observed::Observed(value) = broker.produce_tps {
                    points.push(HistoryPoint {
                        metric: HistoryMetricKind::BrokerProduceTps,
                        series_identity: series_identity.clone(),
                        timestamp_epoch_ms,
                        value,
                        source_revision: revision,
                    });
                }
                if let Observed::Observed(value) = broker.consume_tps {
                    points.push(HistoryPoint {
                        metric: HistoryMetricKind::BrokerConsumeTps,
                        series_identity,
                        timestamp_epoch_ms,
                        value,
                        source_revision: revision,
                    });
                }
            }
            Ok(points)
        })
    }
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
        return Err(query_error("overview unavailable"));
    }
    Ok(DashboardOverviewLoad {
        overview: project_dashboard_overview(evidence),
        failed_resources: 5usize.saturating_sub(successful_resources),
    })
}

fn query_error(_error: impl std::fmt::Display) -> UiError {
    UiError::new(
        "Unable to load Dashboard data from the selected connection.",
        UiErrorCode::Connection,
        true,
    )
}

fn history_error(_error: impl std::fmt::Display) -> UiError {
    UiError::new("Unable to load local metric History.", UiErrorCode::Configuration, true)
}

#[cfg(test)]
mod tests {
    use rocketmq_dashboard_common::EndpointAvailability;

    use super::*;

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
