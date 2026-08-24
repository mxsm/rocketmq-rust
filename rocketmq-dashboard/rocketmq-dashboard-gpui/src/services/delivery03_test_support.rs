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

//! Deterministic safe-DTO fake for Delivery 03 product-path tests.

use std::{collections::VecDeque, sync::Mutex};

use rocketmq_dashboard_common::{
    BrokerConfigPatch, BrokerConfigSnapshot, BrokerCurrentMetric, BrokerIdentity, BrokerInventoryItem,
    DashboardOverview, HistoryMetricKind, HistoryPoint, RuntimeEntry, TopicCurrentMetric,
};

use super::{Delivery03Backend, ServiceFuture};
use crate::{
    services::{brokers::BrokerConfigMutationResult, dashboard::DashboardOverviewLoad},
    state::{UiError, UiErrorCode},
};

#[derive(Clone, Debug, Default)]
pub(crate) struct Delivery03Calls {
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
    topic_current: VecDeque<Result<Vec<TopicCurrentMetric>, UiError>>,
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
pub(crate) struct FakeDelivery03Backend {
    queues: Mutex<Queues>,
    calls: Mutex<Delivery03Calls>,
}

impl FakeDelivery03Backend {
    pub fn queue_topic_current(&self, result: Result<Vec<TopicCurrentMetric>, UiError>) {
        self.queues
            .lock()
            .expect("delivery03 queues")
            .topic_current
            .push_back(result);
    }

    pub fn queue_topic_history(&self, result: Result<Vec<HistoryPoint>, UiError>) {
        self.queues
            .lock()
            .expect("delivery03 queues")
            .topic_history
            .push_back(result);
    }

    pub fn queue_broker_current(&self, result: Result<Vec<BrokerCurrentMetric>, UiError>) {
        self.queues
            .lock()
            .expect("delivery03 queues")
            .broker_current
            .push_back(result);
    }

    pub fn queue_broker_history(&self, result: Result<Vec<HistoryPoint>, UiError>) {
        self.queues
            .lock()
            .expect("delivery03 queues")
            .broker_history
            .push_back(result);
    }

    pub fn queue_inventory(&self, result: Result<Vec<BrokerInventoryItem>, UiError>) {
        self.queues
            .lock()
            .expect("delivery03 queues")
            .inventory
            .push_back(result);
    }

    pub fn queue_runtime(&self, result: Result<Vec<RuntimeEntry>, UiError>) {
        self.queues.lock().expect("delivery03 queues").runtime.push_back(result);
    }

    pub fn queue_config(&self, result: Result<BrokerConfigSnapshot, UiError>) {
        self.queues.lock().expect("delivery03 queues").config.push_back(result);
    }

    pub fn queue_patch(&self, result: Result<BrokerConfigMutationResult, UiError>) {
        self.queues.lock().expect("delivery03 queues").patches.push_back(result);
    }

    pub fn calls(&self) -> Delivery03Calls {
        self.calls.lock().expect("delivery03 calls").clone()
    }
}

impl Delivery03Backend for FakeDelivery03Backend {
    fn dashboard_overview(&self, revision: u64) -> ServiceFuture<'_, Result<DashboardOverviewLoad, UiError>> {
        self.calls
            .lock()
            .expect("delivery03 calls")
            .overview_revisions
            .push(revision);
        Box::pin(std::future::ready(Ok(DashboardOverviewLoad {
            overview: DashboardOverview::default(),
            failed_resources: 0,
        })))
    }

    fn dashboard_topic_current(&self, revision: u64) -> ServiceFuture<'_, Result<Vec<TopicCurrentMetric>, UiError>> {
        self.calls
            .lock()
            .expect("delivery03 calls")
            .topic_current_revisions
            .push(revision);
        let result = self
            .queues
            .lock()
            .expect("delivery03 queues")
            .topic_current
            .pop_front()
            .unwrap_or_else(|| Ok(Vec::new()));
        Box::pin(std::future::ready(result))
    }

    fn dashboard_broker_current(&self, revision: u64) -> ServiceFuture<'_, Result<Vec<BrokerCurrentMetric>, UiError>> {
        self.calls
            .lock()
            .expect("delivery03 calls")
            .broker_current_revisions
            .push(revision);
        let result = self
            .queues
            .lock()
            .expect("delivery03 queues")
            .broker_current
            .pop_front()
            .unwrap_or_else(|| Ok(Vec::new()));
        Box::pin(std::future::ready(result))
    }

    fn dashboard_topic_history(
        &self,
        topic: String,
        start_epoch_ms: u64,
        end_epoch_ms: u64,
    ) -> ServiceFuture<'_, Result<Vec<HistoryPoint>, UiError>> {
        self.calls
            .lock()
            .expect("delivery03 calls")
            .topic_history
            .push((topic, start_epoch_ms, end_epoch_ms));
        let result = self
            .queues
            .lock()
            .expect("delivery03 queues")
            .topic_history
            .pop_front()
            .unwrap_or_else(|| Ok(Vec::new()));
        Box::pin(std::future::ready(result))
    }

    fn dashboard_broker_history(
        &self,
        metric: HistoryMetricKind,
        identity: BrokerIdentity,
        start_epoch_ms: u64,
        end_epoch_ms: u64,
    ) -> ServiceFuture<'_, Result<Vec<HistoryPoint>, UiError>> {
        self.calls.lock().expect("delivery03 calls").broker_history.push((
            metric,
            identity,
            start_epoch_ms,
            end_epoch_ms,
        ));
        let result = self
            .queues
            .lock()
            .expect("delivery03 queues")
            .broker_history
            .pop_front()
            .unwrap_or_else(|| Ok(Vec::new()));
        Box::pin(std::future::ready(result))
    }

    fn broker_inventory(&self, revision: u64) -> ServiceFuture<'_, Result<Vec<BrokerInventoryItem>, UiError>> {
        self.calls
            .lock()
            .expect("delivery03 calls")
            .inventory_revisions
            .push(revision);
        let result = self
            .queues
            .lock()
            .expect("delivery03 queues")
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
            .expect("delivery03 calls")
            .runtime
            .push((revision, identity));
        let result = self
            .queues
            .lock()
            .expect("delivery03 queues")
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
            .expect("delivery03 calls")
            .config
            .push((revision, identity));
        let result = self
            .queues
            .lock()
            .expect("delivery03 queues")
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
            .expect("delivery03 calls")
            .patches
            .push((revision, patch));
        let result = self
            .queues
            .lock()
            .expect("delivery03 queues")
            .patches
            .pop_front()
            .unwrap_or_else(|| Err(unexpected_call("Broker config mutation")));
        Box::pin(std::future::ready(result))
    }
}

fn unexpected_call(operation: &str) -> UiError {
    UiError::new(
        format!("Unexpected {operation} test call."),
        UiErrorCode::CapabilityUnavailable,
        false,
    )
}
