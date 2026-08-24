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

//! Delivery 03 Admin queries and the private, allowlisted DTOs returned to GPUI services.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

use rocketmq_admin_core::core::{
    broker::{PatchBrokerConfigOutcome, PatchBrokerConfigRequest, QueryBrokerConfigGenerationRequest},
    dashboard::{
        DashboardBrokerConfig, DashboardBrokerInfo, DashboardBrokerList, DashboardBrokerRuntime, DashboardBrokerTarget,
        DashboardConsumerList, DashboardProducerInfo, DashboardTopicInfo, DashboardTopicList, DashboardTopicStats,
    },
};
use rocketmq_dashboard_common::{EndpointAvailability, RuntimeEntry, redact_sensitive_entries, runtime_entries};

use super::{GpuiAdminProvider, ProviderError, mutation_for_revision, query_for_revision, select_admin};

/// Topic names required by the current Dashboard product surface.
#[derive(Clone, Default, PartialEq, Eq)]
pub(crate) struct SafeTopicList {
    pub topics: Vec<String>,
}

impl fmt::Debug for SafeTopicList {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SafeTopicList")
            .field("topic_count", &self.topics.len())
            .finish()
    }
}

/// Offset fields required to calculate one Topic's current message count.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct SafeTopicStats {
    pub topic: String,
    pub total_min_offset: i64,
    pub total_max_offset: i64,
}

impl fmt::Debug for SafeTopicStats {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SafeTopicStats")
            .field("topic_available", &!self.topic.is_empty())
            .field("total_min_offset", &self.total_min_offset)
            .field("total_max_offset", &self.total_max_offset)
            .finish()
    }
}

/// Consumer evidence required by Overview.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SafeConsumerList {
    pub group_count: usize,
}

/// Producer evidence required by Overview.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SafeProducerList {
    pub distinct_group_count: usize,
}

/// Broker inventory evidence safe to expose outside the infrastructure boundary.
#[derive(Clone, PartialEq)]
pub(crate) struct SafeBrokerInfo {
    pub cluster_name: String,
    pub broker_name: String,
    pub broker_id: u64,
    pub address: String,
    pub role: String,
    pub version: Option<String>,
    pub availability: EndpointAvailability,
    pub produce_tps: Option<f64>,
    pub consume_tps: Option<f64>,
}

impl fmt::Debug for SafeBrokerInfo {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SafeBrokerInfo")
            .field("broker_id", &self.broker_id)
            .field("version_available", &self.version.is_some())
            .field("availability", &self.availability)
            .field("produce_tps_available", &self.produce_tps.is_some())
            .field("consume_tps_available", &self.consume_tps.is_some())
            .finish()
    }
}

/// Complete safe Broker inventory.
#[derive(Clone, Default, PartialEq)]
pub(crate) struct SafeBrokerList {
    pub items: Vec<SafeBrokerInfo>,
}

impl fmt::Debug for SafeBrokerList {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SafeBrokerList")
            .field("broker_count", &self.items.len())
            .finish()
    }
}

/// Redaction-aware runtime values for one exact Broker.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct SafeBrokerRuntime {
    pub broker_name: String,
    pub address: String,
    pub entries: Vec<RuntimeEntry>,
}

impl fmt::Debug for SafeBrokerRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SafeBrokerRuntime")
            .field("entry_count", &self.entries.len())
            .finish()
    }
}

/// Redacted Broker configuration response.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct SafeBrokerConfig {
    pub broker_name: String,
    pub address: String,
    pub entries: BTreeMap<String, String>,
}

impl fmt::Debug for SafeBrokerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SafeBrokerConfig")
            .field("entry_count", &self.entries.len())
            .finish()
    }
}

/// Exact Broker selector independent from admin-core contracts.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct SafeBrokerTarget {
    pub broker_name: String,
    pub address: String,
}

impl fmt::Debug for SafeBrokerTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("SafeBrokerTarget").finish_non_exhaustive()
    }
}

/// Safe generation query result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SafeConfigGeneration {
    pub generation: u64,
}

/// Safe CAS patch request used by the application service seam.
pub(crate) struct SafeConfigPatchRequest {
    pub address: String,
    pub expected_generation: u64,
    pub entries: BTreeMap<String, String>,
}

impl fmt::Debug for SafeConfigPatchRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SafeConfigPatchRequest")
            .field("expected_generation", &self.expected_generation)
            .field("entry_count", &self.entries.len())
            .finish()
    }
}

/// Safe generation-CAS outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SafeConfigPatchOutcome {
    Applied {
        previous_generation: u64,
        generation: u64,
    },
    GenerationConflict {
        expected_generation: u64,
        actual_generation: u64,
    },
}

impl GpuiAdminProvider {
    /// Lists allowlisted Topic names using the revisioned concurrent query session.
    pub async fn list_topics(self: &Arc<Self>, revision: u64) -> Result<SafeTopicList, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-dashboard-topics", move |cancellation| async move {
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            select_admin(cancellation, session.list_topics())
                .await
                .map(sanitize_topics)
        })
        .await
    }

    /// Loads the allowlisted offsets for one Topic.
    pub async fn topic_stats(self: &Arc<Self>, revision: u64, topic: String) -> Result<SafeTopicStats, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-dashboard-topic-stats", move |cancellation| async move {
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            select_admin(cancellation, session.topic_stats(&topic))
                .await
                .map(sanitize_topic_stats)
        })
        .await
    }

    /// Counts Consumer groups without retaining raw group metadata.
    pub async fn list_consumers(self: &Arc<Self>, revision: u64) -> Result<SafeConsumerList, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-dashboard-consumers", move |cancellation| async move {
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            select_admin(cancellation, session.list_consumers())
                .await
                .map(sanitize_consumers)
        })
        .await
    }

    /// Counts distinct Producer groups without retaining raw producer metadata.
    pub async fn list_producers(self: &Arc<Self>, revision: u64) -> Result<SafeProducerList, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-dashboard-producers", move |cancellation| async move {
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            select_admin(cancellation, session.list_producers())
                .await
                .map(sanitize_producers)
        })
        .await
    }

    /// Lists the complete Broker inventory returned by Dashboard Admin.
    pub async fn list_brokers(self: &Arc<Self>, revision: u64) -> Result<SafeBrokerList, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-dashboard-brokers", move |cancellation| async move {
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            select_admin(cancellation, session.list_brokers())
                .await
                .map(sanitize_broker_list)
        })
        .await
    }

    /// Loads one Broker runtime response.
    pub async fn broker_runtime(
        self: &Arc<Self>,
        revision: u64,
        target: SafeBrokerTarget,
    ) -> Result<SafeBrokerRuntime, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-broker-runtime", move |cancellation| async move {
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            let target = DashboardBrokerTarget {
                broker_name: target.broker_name,
                broker_addr: Some(target.address),
            };
            select_admin(cancellation, session.broker_runtime(&target))
                .await
                .map(sanitize_broker_runtime)
        })
        .await
    }

    /// Loads one Broker configuration response.
    pub async fn broker_config(
        self: &Arc<Self>,
        revision: u64,
        target: SafeBrokerTarget,
    ) -> Result<SafeBrokerConfig, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-broker-config", move |cancellation| async move {
            let guard = this.query_session.read().await;
            let session = query_for_revision(&guard, revision)?;
            let target = DashboardBrokerTarget {
                broker_name: target.broker_name,
                broker_addr: Some(target.address),
            };
            select_admin(cancellation, session.broker_config(&target))
                .await
                .map(sanitize_broker_config)
        })
        .await
    }

    /// Queries the CAS generation through the serialized mutation session.
    pub async fn query_config_generation(
        self: &Arc<Self>,
        revision: u64,
        address: String,
    ) -> Result<SafeConfigGeneration, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-broker-config-generation", move |cancellation| async move {
            let mut guard = this.mutation_session.lock().await;
            this.ensure_mutation(&mut guard, revision, cancellation.clone()).await?;
            let session = mutation_for_revision(&mut guard, revision)?;
            let request = QueryBrokerConfigGenerationRequest { broker_addr: address };
            select_admin(cancellation, session.query_config_generation(&request))
                .await
                .map(|result| SafeConfigGeneration {
                    generation: result.generation,
                })
        })
        .await
    }

    /// Applies one reviewed generation-CAS Broker configuration patch.
    pub async fn patch_config_if_generation(
        self: &Arc<Self>,
        revision: u64,
        request: SafeConfigPatchRequest,
    ) -> Result<SafeConfigPatchOutcome, ProviderError> {
        let this = Arc::clone(self);
        self.run_owned("gpui-broker-config-patch", move |cancellation| async move {
            let mut guard = this.mutation_session.lock().await;
            this.ensure_mutation(&mut guard, revision, cancellation.clone()).await?;
            let session = mutation_for_revision(&mut guard, revision)?;
            let request = PatchBrokerConfigRequest {
                broker_addr: request.address,
                expected_generation: request.expected_generation,
                properties: request.entries,
            };
            select_admin(cancellation, session.patch_config_if_generation(&request))
                .await
                .map(sanitize_patch_outcome)
        })
        .await
    }
}

fn sanitize_topics(response: DashboardTopicList) -> SafeTopicList {
    let DashboardTopicList { items } = response;
    SafeTopicList {
        topics: items
            .into_iter()
            .map(|DashboardTopicInfo { topic, .. }| topic)
            .collect(),
    }
}

fn sanitize_topic_stats(response: DashboardTopicStats) -> SafeTopicStats {
    let DashboardTopicStats {
        topic,
        total_min_offset,
        total_max_offset,
        ..
    } = response;
    SafeTopicStats {
        topic,
        total_min_offset,
        total_max_offset,
    }
}

fn sanitize_consumers(response: DashboardConsumerList) -> SafeConsumerList {
    let DashboardConsumerList { items } = response;
    SafeConsumerList {
        group_count: items.len(),
    }
}

fn sanitize_producers(response: Vec<DashboardProducerInfo>) -> SafeProducerList {
    let distinct_group_count = response
        .into_iter()
        .map(|DashboardProducerInfo { producer_group, .. }| producer_group)
        .collect::<BTreeSet<_>>()
        .len();
    SafeProducerList { distinct_group_count }
}

fn sanitize_broker_list(response: DashboardBrokerList) -> SafeBrokerList {
    SafeBrokerList {
        items: response.items.into_iter().map(sanitize_broker_info).collect(),
    }
}

fn sanitize_broker_info(item: DashboardBrokerInfo) -> SafeBrokerInfo {
    let DashboardBrokerInfo {
        cluster_name,
        broker_name,
        broker_id,
        address,
        role,
        version: _,
        produce_tps: _,
        consume_tps: _,
        runtime_entries,
        runtime_error,
    } = item;
    let runtime_failed = runtime_error.is_some();
    drop(runtime_error);
    let (version, produce_tps, consume_tps, availability) = if runtime_failed {
        (None, None, None, EndpointAvailability::Unavailable)
    } else {
        let version = runtime_entries
            .get("brokerVersionDesc")
            .filter(|value| !value.trim().is_empty())
            .cloned();
        let produce_tps = safe_rate(runtime_entries.get("putTps"));
        let consume_tps = safe_rate(
            runtime_entries
                .get("getTransferedTps")
                .filter(|value| !value.trim().is_empty())
                .or_else(|| runtime_entries.get("getTransferredTps")),
        );
        (version, produce_tps, consume_tps, EndpointAvailability::Available)
    };
    SafeBrokerInfo {
        cluster_name,
        broker_name,
        broker_id,
        address,
        role,
        version,
        availability,
        produce_tps,
        consume_tps,
    }
}

fn safe_rate(value: Option<&String>) -> Option<f64> {
    value
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite())
}

fn sanitize_broker_runtime(response: DashboardBrokerRuntime) -> SafeBrokerRuntime {
    let DashboardBrokerRuntime {
        broker_name,
        address,
        entries,
    } = response;
    SafeBrokerRuntime {
        broker_name,
        address,
        entries: runtime_entries(entries),
    }
}

fn sanitize_broker_config(response: DashboardBrokerConfig) -> SafeBrokerConfig {
    let DashboardBrokerConfig {
        broker_name,
        address,
        entries,
    } = response;
    SafeBrokerConfig {
        broker_name,
        address,
        entries: redact_sensitive_entries(entries),
    }
}

fn sanitize_patch_outcome(outcome: PatchBrokerConfigOutcome) -> SafeConfigPatchOutcome {
    match outcome {
        PatchBrokerConfigOutcome::Applied {
            previous_generation,
            generation,
        } => SafeConfigPatchOutcome::Applied {
            previous_generation,
            generation,
        },
        PatchBrokerConfigOutcome::GenerationConflict {
            expected_generation,
            actual_generation,
        } => SafeConfigPatchOutcome::GenerationConflict {
            expected_generation,
            actual_generation,
        },
    }
}

#[cfg(test)]
mod tests {
    use rocketmq_admin_core::core::dashboard::{DashboardConsumerGroup, DashboardTopicInfo};

    use super::*;

    #[test]
    fn raw_dashboard_resources_are_allowlisted_and_debug_redacted_at_the_provider_boundary() {
        let topics = sanitize_topics(DashboardTopicList {
            items: vec![DashboardTopicInfo {
                topic: "orders-sensitive-tenant".into(),
                broker_name: Some("raw-broker".into()),
                read_queue_count: 8,
                write_queue_count: 8,
                perm: 6,
                category: "raw-category".into(),
            }],
        });
        assert_eq!(topics.topics, ["orders-sensitive-tenant"]);
        let topic_debug = format!("{topics:?}");
        for raw in ["orders-sensitive-tenant", "raw-broker", "raw-category"] {
            assert!(!topic_debug.contains(raw));
        }

        let stats = sanitize_topic_stats(DashboardTopicStats {
            topic: "orders-sensitive-tenant".into(),
            queue_count: 16,
            total_min_offset: 4,
            total_max_offset: 24,
        });
        assert_eq!(stats.total_max_offset, 24);
        assert!(!format!("{stats:?}").contains("orders-sensitive-tenant"));

        let consumers = sanitize_consumers(DashboardConsumerList {
            items: vec![DashboardConsumerGroup {
                group: "private-consumer".into(),
                consume_type: "PUSH".into(),
                message_model: "CLUSTERING".into(),
                client_count: 1,
                diff_total: 9,
            }],
        });
        assert_eq!(consumers.group_count, 1);
        assert!(!format!("{consumers:?}").contains("private-consumer"));

        let producers = sanitize_producers(vec![DashboardProducerInfo {
            topic: "private-topic".into(),
            producer_group: "private-producer".into(),
            connection_count: 3,
        }]);
        assert_eq!(producers.distinct_group_count, 1);
        let producer_debug = format!("{producers:?}");
        assert!(!producer_debug.contains("private-topic"));
        assert!(!producer_debug.contains("private-producer"));
    }
}
