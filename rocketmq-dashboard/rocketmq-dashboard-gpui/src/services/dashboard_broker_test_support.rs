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

//! Deterministic safe-DTO fake for Dashboard/Broker product-path tests.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use rocketmq_dashboard_common::{
    BrokerConfigPatch, BrokerConfigSnapshot, BrokerCurrentMetric, BrokerIdentity, BrokerInventoryItem,
    DashboardOverview, HistoryMetricKind, HistoryPoint, RuntimeEntry, TopicCurrentLoad, TopicCurrentObservation,
};

use super::{DashboardBrokerOperations, ServiceFuture};
use crate::{
    services::{brokers::BrokerConfigMutationResult, dashboard::DashboardOverviewLoad},
    state::{UiError, UiErrorCode},
};

#[derive(Clone, Debug, Default)]
pub(crate) struct DashboardBrokerCalls {
    pub count_history: Vec<(HistoryMetricKind, u64, u64)>,
    pub overview_revisions: Vec<u64>,
    pub topic_current_revisions: Vec<u64>,
    pub broker_current_revisions: Vec<u64>,
    pub topic_history: Vec<(String, u64, u64)>,
    pub broker_history: Vec<(HistoryMetricKind, BrokerIdentity, u64, u64)>,
    pub inventory_revisions: Vec<u64>,
    pub runtime: Vec<(u64, BrokerIdentity)>,
    pub config: Vec<(u64, BrokerIdentity)>,
    pub patches: Vec<(u64, BrokerConfigPatch)>,
}

#[derive(Default)]
struct Queues {
    topic_current: VecDeque<Result<TopicCurrentLoad, UiError>>,
    broker_current: VecDeque<Result<Vec<BrokerCurrentMetric>, UiError>>,
    topic_history: VecDeque<Result<Vec<HistoryPoint>, UiError>>,
    broker_history: VecDeque<Result<Vec<HistoryPoint>, UiError>>,
    inventory: VecDeque<Result<Vec<BrokerInventoryItem>, UiError>>,
    runtime: VecDeque<Result<Vec<RuntimeEntry>, UiError>>,
    config: VecDeque<Result<BrokerConfigSnapshot, UiError>>,
    patches: VecDeque<Result<BrokerConfigMutationResult, UiError>>,
}

/// Test fake that cannot accept or expose any raw admin-core response type.
#[derive(Default)]
pub(crate) struct FakeDashboardBrokerOperations {
    queues: Mutex<Queues>,
    calls: Mutex<DashboardBrokerCalls>,
    next_patch: Mutex<Option<(tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>)>>,
    history_service: Mutex<Option<Arc<crate::services::dashboard::DashboardService>>>,
}

impl FakeDashboardBrokerOperations {
    pub fn hold_next_patch(&self) -> (tokio::sync::oneshot::Receiver<()>, tokio::sync::oneshot::Sender<()>) {
        let (started, observed) = tokio::sync::oneshot::channel();
        let (release, gate) = tokio::sync::oneshot::channel();
        *self.next_patch.lock().expect("Broker patch gate") = Some((started, gate));
        (observed, release)
    }

    pub fn use_history_service(&self, service: Arc<crate::services::dashboard::DashboardService>) {
        *self.history_service.lock().unwrap() = Some(service);
    }

    pub fn queue_topic_load(&self, result: Result<TopicCurrentLoad, UiError>) {
        self.queues.lock().unwrap().topic_current.push_back(result);
    }

    pub fn queue_topic_current(&self, result: Result<Vec<TopicCurrentObservation>, UiError>) {
        self.queues
            .lock()
            .expect("dashboard_broker queues")
            .topic_current
            .push_back(result.map(|items| TopicCurrentLoad {
                total_topics: items.len(),
                application_topics: items.len(),
                candidates: items.iter().map(|item| item.topic.clone()).collect(),
                available_topics: items.iter().map(|item| item.topic.clone()).collect(),
                items,
                ..Default::default()
            }));
    }

    pub fn queue_topic_history(&self, result: Result<Vec<HistoryPoint>, UiError>) {
        self.queues
            .lock()
            .expect("dashboard_broker queues")
            .topic_history
            .push_back(result);
    }

    pub fn queue_broker_current(&self, result: Result<Vec<BrokerCurrentMetric>, UiError>) {
        self.queues
            .lock()
            .expect("dashboard_broker queues")
            .broker_current
            .push_back(result);
    }

    pub fn queue_broker_history(&self, result: Result<Vec<HistoryPoint>, UiError>) {
        self.queues
            .lock()
            .expect("dashboard_broker queues")
            .broker_history
            .push_back(result);
    }

    pub fn queue_inventory(&self, result: Result<Vec<BrokerInventoryItem>, UiError>) {
        self.queues
            .lock()
            .expect("dashboard_broker queues")
            .inventory
            .push_back(result);
    }

    pub fn queue_runtime(&self, result: Result<Vec<RuntimeEntry>, UiError>) {
        self.queues
            .lock()
            .expect("dashboard_broker queues")
            .runtime
            .push_back(result);
    }

    pub fn queue_config(&self, result: Result<BrokerConfigSnapshot, UiError>) {
        self.queues
            .lock()
            .expect("dashboard_broker queues")
            .config
            .push_back(result);
    }

    pub fn queue_patch(&self, result: Result<BrokerConfigMutationResult, UiError>) {
        self.queues
            .lock()
            .expect("dashboard_broker queues")
            .patches
            .push_back(result);
    }

    pub fn calls(&self) -> DashboardBrokerCalls {
        self.calls.lock().expect("dashboard_broker calls").clone()
    }
}

impl DashboardBrokerOperations for FakeDashboardBrokerOperations {
    fn dashboard_overview(&self, revision: u64) -> ServiceFuture<'_, Result<DashboardOverviewLoad, UiError>> {
        self.calls
            .lock()
            .expect("dashboard_broker calls")
            .overview_revisions
            .push(revision);
        Box::pin(std::future::ready(Ok(DashboardOverviewLoad {
            overview: DashboardOverview::default(),
            failed_resources: 0,
        })))
    }

    fn dashboard_topic_current(&self, revision: u64) -> ServiceFuture<'_, Result<TopicCurrentLoad, UiError>> {
        self.calls
            .lock()
            .expect("dashboard_broker calls")
            .topic_current_revisions
            .push(revision);
        let result = self
            .queues
            .lock()
            .expect("dashboard_broker queues")
            .topic_current
            .pop_front()
            .unwrap_or_else(|| Ok(TopicCurrentLoad::default()));
        Box::pin(std::future::ready(result))
    }

    fn dashboard_broker_current(&self, revision: u64) -> ServiceFuture<'_, Result<Vec<BrokerCurrentMetric>, UiError>> {
        self.calls
            .lock()
            .expect("dashboard_broker calls")
            .broker_current_revisions
            .push(revision);
        let result = self
            .queues
            .lock()
            .expect("dashboard_broker queues")
            .broker_current
            .pop_front()
            .unwrap_or_else(|| Ok(Vec::new()));
        Box::pin(std::future::ready(result))
    }

    fn dashboard_topic_history(
        &self,
        revision: u64,
        topic: String,
        start_epoch_ms: u64,
        end_epoch_ms: u64,
    ) -> ServiceFuture<'_, Result<Vec<HistoryPoint>, UiError>> {
        self.calls.lock().expect("dashboard_broker calls").topic_history.push((
            topic.clone(),
            start_epoch_ms,
            end_epoch_ms,
        ));
        if let Some(service) = self.history_service.lock().unwrap().clone() {
            return Box::pin(async move {
                service
                    .topic_history(revision, topic, start_epoch_ms, end_epoch_ms)
                    .await
            });
        }
        let result = self
            .queues
            .lock()
            .expect("dashboard_broker queues")
            .topic_history
            .pop_front()
            .unwrap_or_else(|| Ok(Vec::new()));
        Box::pin(std::future::ready(result))
    }

    fn dashboard_broker_history(
        &self,
        _revision: u64,
        metric: HistoryMetricKind,
        identity: BrokerIdentity,
        start_epoch_ms: u64,
        end_epoch_ms: u64,
    ) -> ServiceFuture<'_, Result<Vec<HistoryPoint>, UiError>> {
        self.calls.lock().expect("dashboard_broker calls").broker_history.push((
            metric,
            identity,
            start_epoch_ms,
            end_epoch_ms,
        ));
        let result = self
            .queues
            .lock()
            .expect("dashboard_broker queues")
            .broker_history
            .pop_front()
            .unwrap_or_else(|| Ok(Vec::new()));
        Box::pin(std::future::ready(result))
    }

    fn dashboard_count_history(
        &self,
        _revision: u64,
        metric: HistoryMetricKind,
        start: u64,
        end: u64,
    ) -> ServiceFuture<'_, Result<Vec<HistoryPoint>, UiError>> {
        self.calls.lock().unwrap().count_history.push((metric, start, end));
        let mut queues = self.queues.lock().unwrap();
        let queue = if metric == HistoryMetricKind::TopicCount {
            &mut queues.topic_history
        } else {
            &mut queues.broker_history
        };
        Box::pin(std::future::ready(queue.pop_front().unwrap_or_else(|| Ok(Vec::new()))))
    }
    fn dashboard_history_health(
        &self,
        _revision: u64,
    ) -> ServiceFuture<'_, Result<crate::infrastructure::history_health::HistoryCollectionHealth, UiError>> {
        Box::pin(std::future::ready(Ok(Default::default())))
    }

    fn broker_inventory(&self, revision: u64) -> ServiceFuture<'_, Result<Vec<BrokerInventoryItem>, UiError>> {
        self.calls
            .lock()
            .expect("dashboard_broker calls")
            .inventory_revisions
            .push(revision);
        let result = self
            .queues
            .lock()
            .expect("dashboard_broker queues")
            .inventory
            .pop_front()
            .unwrap_or_else(|| Ok(Vec::new()));
        Box::pin(std::future::ready(result))
    }

    fn broker_runtime(
        &self,
        revision: u64,
        identity: BrokerIdentity,
    ) -> ServiceFuture<'_, Result<Vec<RuntimeEntry>, UiError>> {
        self.calls
            .lock()
            .expect("dashboard_broker calls")
            .runtime
            .push((revision, identity));
        let result = self
            .queues
            .lock()
            .expect("dashboard_broker queues")
            .runtime
            .pop_front()
            .unwrap_or_else(|| Ok(Vec::new()));
        Box::pin(std::future::ready(result))
    }

    fn broker_config(
        &self,
        revision: u64,
        identity: BrokerIdentity,
    ) -> ServiceFuture<'_, Result<BrokerConfigSnapshot, UiError>> {
        self.calls
            .lock()
            .expect("dashboard_broker calls")
            .config
            .push((revision, identity));
        let result = self
            .queues
            .lock()
            .expect("dashboard_broker queues")
            .config
            .pop_front()
            .unwrap_or_else(|| Err(unexpected_call("Broker config")));
        Box::pin(std::future::ready(result))
    }

    fn patch_broker_config(
        &self,
        revision: u64,
        patch: BrokerConfigPatch,
    ) -> ServiceFuture<'_, Result<BrokerConfigMutationResult, UiError>> {
        self.calls
            .lock()
            .expect("dashboard_broker calls")
            .patches
            .push((revision, patch));
        let result = self
            .queues
            .lock()
            .expect("dashboard_broker queues")
            .patches
            .pop_front()
            .unwrap_or_else(|| Err(unexpected_call("Broker config mutation")));
        let gate = self.next_patch.lock().expect("Broker patch gate").take();
        Box::pin(async move {
            if let Some((started, gate)) = gate {
                let _ = started.send(());
                gate.await.map_err(|_| unexpected_call("cancelled Broker patch gate"))?;
            }
            result
        })
    }
}

fn unexpected_call(_operation: &str) -> UiError {
    UiError::new(
        "Unexpected Delivery test call.",
        UiErrorCode::CapabilityUnavailable,
        false,
    )
}
