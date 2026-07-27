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

use chrono::DateTime;
use chrono::Utc;
use rocketmq_sre_contracts::ClusterId;
use rocketmq_sre_contracts::CorrelationId;
use rocketmq_sre_contracts::TenantId;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::broadcast;

/// Bounded workflow event exposed over SSE.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct WorkflowStreamEvent {
    pub tenant_id: TenantId,
    pub cluster_id: ClusterId,
    pub aggregate_type: &'static str,
    pub aggregate_id: String,
    pub event_type: &'static str,
    pub payload: Value,
    pub correlation_id: CorrelationId,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Clone)]
pub(crate) struct WorkflowEventBus {
    sender: broadcast::Sender<WorkflowStreamEvent>,
}

impl WorkflowEventBus {
    pub(crate) fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity.max(16));
        Self { sender }
    }

    pub(crate) fn publish(&self, event: WorkflowStreamEvent) {
        let _ = self.sender.send(event);
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<WorkflowStreamEvent> {
        self.sender.subscribe()
    }
}
