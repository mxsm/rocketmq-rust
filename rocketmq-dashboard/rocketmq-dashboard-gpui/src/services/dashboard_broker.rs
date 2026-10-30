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

//! Private Dashboard/Broker operations and use-case forwarding.

#[cfg(test)]
#[path = "dashboard_broker_test_support.rs"]
pub(crate) mod test_support;

use std::sync::Arc;

use rocketmq_dashboard_common::{
    AuditAction, BrokerConfigPatch, BrokerConfigSnapshot, BrokerCurrentMetric, BrokerIdentity, BrokerInventoryItem,
    HistoryMetricKind, HistoryPoint, OperationResource, RuntimeEntry, TopicCurrentLoad, broker_history_series_identity,
};

use super::{
    AppServices, ServiceFuture,
    brokers::{BrokerConfigMutationResult, BrokerService},
    dashboard::{DashboardOverviewLoad, DashboardService},
};
use crate::state::UiError;

/// Crate-private seam used by feature entities and their product-path fakes.
///
/// Keeping this contract here intentionally avoids enabling the generic `admin` feature in
/// `rocketmq-dashboard-common`: every value crossing the seam is already a protocol-independent
/// common DTO or a redacted GPUI result. Raw `rocketmq-admin-core` responses remain inside
/// `infrastructure` and the concrete Dashboard/Broker services.
pub(crate) trait DashboardBrokerOperations: Send + Sync {
    fn dashboard_overview(&self, revision: u64) -> ServiceFuture<'_, Result<DashboardOverviewLoad, UiError>>;

    fn dashboard_topic_current(&self, revision: u64) -> ServiceFuture<'_, Result<TopicCurrentLoad, UiError>>;

    fn dashboard_broker_current(&self, revision: u64) -> ServiceFuture<'_, Result<Vec<BrokerCurrentMetric>, UiError>>;

    fn dashboard_topic_history(
        &self,
        revision: u64,
        topic: String,
        start_epoch_ms: u64,
        end_epoch_ms: u64,
    ) -> ServiceFuture<'_, Result<Vec<HistoryPoint>, UiError>>;

    fn dashboard_broker_history(
        &self,
        revision: u64,
        metric: HistoryMetricKind,
        identity: BrokerIdentity,
        start_epoch_ms: u64,
        end_epoch_ms: u64,
    ) -> ServiceFuture<'_, Result<Vec<HistoryPoint>, UiError>>;

    fn dashboard_count_history(
        &self,
        revision: u64,
        metric: HistoryMetricKind,
        start: u64,
        end: u64,
    ) -> ServiceFuture<'_, Result<Vec<HistoryPoint>, UiError>>;
    fn dashboard_history_health(
        &self,
        revision: u64,
    ) -> ServiceFuture<'_, Result<crate::infrastructure::history_health::HistoryCollectionHealth, UiError>>;

    fn broker_inventory(&self, revision: u64) -> ServiceFuture<'_, Result<Vec<BrokerInventoryItem>, UiError>>;

    fn broker_runtime(
        &self,
        revision: u64,
        identity: BrokerIdentity,
    ) -> ServiceFuture<'_, Result<Vec<RuntimeEntry>, UiError>>;

    fn broker_config(
        &self,
        revision: u64,
        identity: BrokerIdentity,
    ) -> ServiceFuture<'_, Result<BrokerConfigSnapshot, UiError>>;

    fn patch_broker_config(
        &self,
        revision: u64,
        patch: BrokerConfigPatch,
    ) -> ServiceFuture<'_, Result<BrokerConfigMutationResult, UiError>>;
}

pub(super) struct RealDashboardBrokerOperations {
    dashboard: Arc<DashboardService>,
    brokers: Arc<BrokerService>,
}

impl RealDashboardBrokerOperations {
    pub(super) fn new(dashboard: Arc<DashboardService>, brokers: Arc<BrokerService>) -> Arc<Self> {
        Arc::new(Self { dashboard, brokers })
    }
}

impl DashboardBrokerOperations for RealDashboardBrokerOperations {
    fn dashboard_overview(&self, revision: u64) -> ServiceFuture<'_, Result<DashboardOverviewLoad, UiError>> {
        Box::pin(self.dashboard.overview(revision))
    }

    fn dashboard_topic_current(&self, revision: u64) -> ServiceFuture<'_, Result<TopicCurrentLoad, UiError>> {
        Box::pin(self.dashboard.topic_current(revision))
    }

    fn dashboard_broker_current(&self, revision: u64) -> ServiceFuture<'_, Result<Vec<BrokerCurrentMetric>, UiError>> {
        Box::pin(self.dashboard.broker_current(revision))
    }

    fn dashboard_topic_history(
        &self,
        revision: u64,
        topic: String,
        start_epoch_ms: u64,
        end_epoch_ms: u64,
    ) -> ServiceFuture<'_, Result<Vec<HistoryPoint>, UiError>> {
        Box::pin(
            self.dashboard
                .topic_history(revision, topic, start_epoch_ms, end_epoch_ms),
        )
    }

    fn dashboard_broker_history(
        &self,
        revision: u64,
        metric: HistoryMetricKind,
        identity: BrokerIdentity,
        start_epoch_ms: u64,
        end_epoch_ms: u64,
    ) -> ServiceFuture<'_, Result<Vec<HistoryPoint>, UiError>> {
        Box::pin(
            self.dashboard
                .broker_history(revision, metric, identity, start_epoch_ms, end_epoch_ms),
        )
    }

    fn dashboard_count_history(
        &self,
        revision: u64,
        metric: HistoryMetricKind,
        start: u64,
        end: u64,
    ) -> ServiceFuture<'_, Result<Vec<HistoryPoint>, UiError>> {
        Box::pin(self.dashboard.count_history(revision, metric, start, end))
    }
    fn dashboard_history_health(
        &self,
        revision: u64,
    ) -> ServiceFuture<'_, Result<crate::infrastructure::history_health::HistoryCollectionHealth, UiError>> {
        Box::pin(std::future::ready(self.dashboard.collection_health(revision)))
    }

    fn broker_inventory(&self, revision: u64) -> ServiceFuture<'_, Result<Vec<BrokerInventoryItem>, UiError>> {
        Box::pin(self.brokers.inventory(revision))
    }

    fn broker_runtime(
        &self,
        revision: u64,
        identity: BrokerIdentity,
    ) -> ServiceFuture<'_, Result<Vec<RuntimeEntry>, UiError>> {
        Box::pin(self.brokers.runtime(revision, identity))
    }

    fn broker_config(
        &self,
        revision: u64,
        identity: BrokerIdentity,
    ) -> ServiceFuture<'_, Result<BrokerConfigSnapshot, UiError>> {
        Box::pin(self.brokers.config(revision, identity))
    }

    fn patch_broker_config(
        &self,
        revision: u64,
        patch: BrokerConfigPatch,
    ) -> ServiceFuture<'_, Result<BrokerConfigMutationResult, UiError>> {
        Box::pin(self.brokers.patch_config(revision, patch))
    }
}

impl AppServices {
    /// Loads the independently evidenced Dashboard overview.
    pub async fn dashboard_overview(&self, revision: u64) -> Result<DashboardOverviewLoad, UiError> {
        self.dashboard_broker.dashboard_overview(revision).await
    }

    /// Loads current Topic metrics without inferred rates.
    pub async fn dashboard_topic_current(&self, revision: u64) -> Result<TopicCurrentLoad, UiError> {
        self.dashboard_broker.dashboard_topic_current(revision).await
    }

    /// Loads current Broker metrics without converting missing runtime fields to zero.
    pub async fn dashboard_broker_current(&self, revision: u64) -> Result<Vec<BrokerCurrentMetric>, UiError> {
        self.dashboard_broker.dashboard_broker_current(revision).await
    }

    /// Loads actual Topic History points in the requested range.
    pub async fn dashboard_topic_history(
        &self,
        revision: u64,
        topic: String,
        start_epoch_ms: u64,
        end_epoch_ms: u64,
    ) -> Result<Vec<HistoryPoint>, UiError> {
        self.dashboard_broker
            .dashboard_topic_history(revision, topic, start_epoch_ms, end_epoch_ms)
            .await
    }

    /// Loads actual Broker History points in the requested range.
    pub async fn dashboard_broker_history(
        &self,
        revision: u64,
        metric: HistoryMetricKind,
        identity: BrokerIdentity,
        start_epoch_ms: u64,
        end_epoch_ms: u64,
    ) -> Result<Vec<HistoryPoint>, UiError> {
        self.dashboard_broker
            .dashboard_broker_history(revision, metric, identity, start_epoch_ms, end_epoch_ms)
            .await
    }

    pub async fn dashboard_count_history(
        &self,
        revision: u64,
        metric: HistoryMetricKind,
        start: u64,
        end: u64,
    ) -> Result<Vec<HistoryPoint>, UiError> {
        self.dashboard_broker
            .dashboard_count_history(revision, metric, start, end)
            .await
    }
    pub async fn dashboard_history_health(
        &self,
        revision: u64,
    ) -> Result<crate::infrastructure::history_health::HistoryCollectionHealth, UiError> {
        self.dashboard_broker.dashboard_history_health(revision).await
    }

    /// Loads the complete Broker inventory.
    pub async fn broker_inventory(&self, revision: u64) -> Result<Vec<BrokerInventoryItem>, UiError> {
        self.dashboard_broker.broker_inventory(revision).await
    }

    /// Loads redaction-aware runtime entries for one Broker.
    pub async fn broker_runtime(&self, revision: u64, identity: BrokerIdentity) -> Result<Vec<RuntimeEntry>, UiError> {
        self.dashboard_broker.broker_runtime(revision, identity).await
    }

    /// Loads Broker config paired with its generation CAS token.
    pub async fn broker_config(
        &self,
        revision: u64,
        identity: BrokerIdentity,
    ) -> Result<BrokerConfigSnapshot, UiError> {
        self.dashboard_broker.broker_config(revision, identity).await
    }

    /// Applies one reviewed Broker config patch with generation CAS and no replay.
    pub async fn patch_broker_config(
        &self,
        revision: u64,
        patch: BrokerConfigPatch,
    ) -> Result<BrokerConfigMutationResult, UiError> {
        let backend = Arc::clone(&self.dashboard_broker);
        let operation = self.accept_operation(
            revision,
            AuditAction::BrokerUpdated,
            OperationResource::Broker(broker_history_series_identity(&patch.identity)),
        )?;
        self.run_mutation("gpui-service-broker-patch", operation, async move {
            backend.patch_broker_config(revision, patch).await
        })
        .await
    }

    #[cfg(test)]
    pub(crate) fn with_dashboard_broker_operations(mut self, backend: Arc<dyn DashboardBrokerOperations>) -> Self {
        self.dashboard_broker = backend;
        self
    }
}
