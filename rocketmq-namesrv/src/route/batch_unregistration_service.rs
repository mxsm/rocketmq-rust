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

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashSet;
use rocketmq_observability::metrics::namesrv::NameServerMetrics;
use rocketmq_protocol::protocol::header::namesrv::broker_request::UnRegisterBrokerRequestHeader;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing::warn;

use crate::bootstrap::NameServerRuntimeHandle;
use crate::route::types::BrokerGeneration;
use crate::route::types::BrokerInstanceKey;
use crate::route::types::RemotingConnectionId;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub(crate) struct BrokerUnregistrationRequest {
    pub(crate) header: UnRegisterBrokerRequestHeader,
    pub(crate) expected_channel_id: Option<RemotingConnectionId>,
    pub(crate) expected_generation: Option<BrokerGeneration>,
    enqueued_at: Instant,
}

impl BrokerUnregistrationRequest {
    pub(crate) fn explicit(header: UnRegisterBrokerRequestHeader) -> Self {
        Self {
            header,
            expected_channel_id: None,
            expected_generation: None,
            enqueued_at: Instant::now(),
        }
    }

    pub(crate) fn channel_guarded(
        header: UnRegisterBrokerRequestHeader,
        channel_id: RemotingConnectionId,
        generation: BrokerGeneration,
    ) -> Self {
        Self {
            header,
            expected_channel_id: Some(channel_id),
            expected_generation: Some(generation),
            enqueued_at: Instant::now(),
        }
    }

    fn pending_key(&self) -> PendingUnregistrationKey {
        PendingUnregistrationKey {
            broker: BrokerInstanceKey::new(
                self.header.cluster_name.clone(),
                self.header.broker_name.clone(),
                self.header.broker_id,
                self.header.broker_addr.clone(),
            ),
            generation: self.expected_generation,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PendingUnregistrationKey {
    broker: BrokerInstanceKey,
    generation: Option<BrokerGeneration>,
}

pub(crate) struct BatchUnregistrationService {
    name_server_runtime_inner: NameServerRuntimeHandle,
    tx: tokio::sync::mpsc::Sender<BrokerUnregistrationRequest>,
    rx: parking_lot::Mutex<Option<tokio::sync::mpsc::Receiver<BrokerUnregistrationRequest>>>,
    task_group: parking_lot::Mutex<Option<TaskGroup>>,
    pending: Arc<DashSet<PendingUnregistrationKey>>,
    batch_size: usize,
    batch_time: Duration,
    metrics: NameServerMetrics,
}

impl BatchUnregistrationService {
    pub(crate) fn new(
        name_server_runtime_inner: NameServerRuntimeHandle,
        queue_capacity: usize,
        batch_size: usize,
        batch_time: Duration,
        metrics: NameServerMetrics,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel::<BrokerUnregistrationRequest>(queue_capacity);
        BatchUnregistrationService {
            name_server_runtime_inner,
            tx,
            rx: parking_lot::Mutex::new(Some(rx)),
            task_group: parking_lot::Mutex::new(None),
            pending: Arc::new(DashSet::new()),
            batch_size,
            batch_time,
            metrics,
        }
    }

    pub fn submit(&self, request: UnRegisterBrokerRequestHeader) -> bool {
        self.submit_request(BrokerUnregistrationRequest::explicit(request))
    }

    pub(crate) fn submit_channel_guarded(
        &self,
        request: UnRegisterBrokerRequestHeader,
        channel_id: RemotingConnectionId,
        generation: BrokerGeneration,
    ) -> bool {
        self.submit_request(BrokerUnregistrationRequest::channel_guarded(
            request, channel_id, generation,
        ))
    }

    fn submit_request(&self, request: BrokerUnregistrationRequest) -> bool {
        let pending_key = request.pending_key();
        if !self.pending.insert(pending_key.clone()) {
            self.metrics
                .record_unregistration_queue("coalesced", self.queue_length());
            return true;
        }
        match self.tx.try_send(request) {
            Ok(()) => {
                self.metrics.record_unregistration_queue("queued", self.queue_length());
                true
            }
            Err(TrySendError::Full(request) | TrySendError::Closed(request)) => {
                self.pending.remove(&pending_key);
                let Some(runtime) = self.name_server_runtime_inner.upgrade() else {
                    warn!("cannot process unregister request because NameServer runtime is unavailable");
                    return false;
                };
                warn!("unregister queue unavailable; applying one request synchronously");
                self.metrics
                    .record_unregistration_queue("synchronous-fallback", self.queue_length());
                runtime.route_info_manager().un_register_broker_requests(vec![request]);
                true
            }
        }
    }

    pub fn start(&self) {
        if self.task_group.lock().is_some() {
            return;
        }

        let task_group = self
            .name_server_runtime_inner
            .component_task_group("namesrv.batch-unregistration");

        let name_server_runtime_inner = self.name_server_runtime_inner.clone();
        let Some(mut rx) = self.rx.lock().take() else {
            warn!("BatchUnregistrationService receiver is unavailable");
            return;
        };
        let shutdown_token = task_group.cancellation_token();
        let pending = Arc::clone(&self.pending);
        let batch_size = self.batch_size;
        let batch_time = self.batch_time;
        let metrics = self.metrics.clone();
        if let Err(error) = task_group.spawn_service("namesrv.batch-unregistration", async move {
            info!("BatchUnregistrationService started");
            run_batch_unregistration_service(
                name_server_runtime_inner,
                &mut rx,
                shutdown_token,
                pending,
                batch_size,
                batch_time,
                metrics,
            )
            .await;
        }) {
            warn!("BatchUnregistrationService cannot start because task spawn failed: {error}");
            return;
        }

        *self.task_group.lock() = Some(task_group);
    }

    pub async fn shutdown(&self) -> Option<ShutdownReport> {
        let task_group = { self.task_group.lock().take() };
        if let Some(task_group) = task_group {
            let report = task_group.shutdown(SHUTDOWN_TIMEOUT).await;
            if let Err(error) = report.assert_no_task_leak() {
                warn!("BatchUnregistrationService shutdown report is unhealthy: {error}");
            }
            Some(report)
        } else {
            None
        }
    }

    /// Returns the number of pending unregister requests in the queue.
    /// For test only.
    #[allow(dead_code)]
    pub fn queue_length(&self) -> usize {
        self.tx.max_capacity() - self.tx.capacity()
    }
}

async fn run_batch_unregistration_service(
    name_server_runtime_inner: NameServerRuntimeHandle,
    rx: &mut tokio::sync::mpsc::Receiver<BrokerUnregistrationRequest>,
    shutdown_token: CancellationToken,
    pending: Arc<DashSet<PendingUnregistrationKey>>,
    batch_size: usize,
    batch_time: Duration,
    metrics: NameServerMetrics,
) {
    loop {
        let first_request = tokio::select! {
            biased;
            _ = shutdown_token.cancelled(), if !shutdown_token.is_cancelled() => {
                info!("BatchUnregistrationService draining for shutdown");
                rx.close();
                rx.recv().await
            }
            request = rx.recv() => request,
        };
        let Some(first_request) = first_request else {
            info!("BatchUnregistrationService channel drained");
            break;
        };

        let unregistration_requests = collect_batch(first_request, rx, &shutdown_token, batch_size, batch_time).await;
        if let Some(oldest_age) = unregistration_requests
            .iter()
            .map(|request| request.enqueued_at.elapsed())
            .max()
        {
            metrics.record_unregistration_oldest_age(oldest_age);
        }

        let pending_keys = unregistration_requests
            .iter()
            .map(BrokerUnregistrationRequest::pending_key)
            .collect::<Vec<_>>();
        let Some(runtime) = name_server_runtime_inner.upgrade() else {
            info!("BatchUnregistrationService stopped because NameServer runtime was released");
            for key in pending_keys {
                pending.remove(&key);
            }
            break;
        };
        runtime
            .route_info_manager()
            .un_register_broker_requests(unregistration_requests);
        metrics.record_unregistration_batch(pending_keys.len());
        for key in pending_keys {
            pending.remove(&key);
        }
        tokio::task::yield_now().await;
    }
}

async fn collect_batch(
    first_request: BrokerUnregistrationRequest,
    rx: &mut tokio::sync::mpsc::Receiver<BrokerUnregistrationRequest>,
    shutdown_token: &CancellationToken,
    batch_size: usize,
    batch_time: Duration,
) -> Vec<BrokerUnregistrationRequest> {
    let mut requests = Vec::with_capacity(batch_size);
    requests.push(first_request);
    let deadline = Instant::now() + batch_time;
    while requests.len() < batch_size {
        tokio::select! {
            biased;
            _ = shutdown_token.cancelled(), if !shutdown_token.is_cancelled() => {
                rx.close();
            }
            request = tokio::time::timeout_at(deadline, rx.recv()) => match request {
                Ok(Some(request)) => requests.push(request),
                Ok(None) | Err(_) => break,
            },
        }
    }
    requests
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    fn request(index: u16) -> BrokerUnregistrationRequest {
        BrokerUnregistrationRequest::explicit(UnRegisterBrokerRequestHeader {
            cluster_name: CheetahString::from_static_str("cluster"),
            broker_addr: CheetahString::from_string(format!("127.0.0.1:{index}")),
            broker_name: CheetahString::from_string(format!("broker-{index}")),
            broker_id: u64::from(index),
        })
    }

    #[tokio::test]
    async fn batch_collection_obeys_size_limit() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        for index in 0..5 {
            tx.send(request(index)).await.unwrap();
        }
        let first = rx.recv().await.unwrap();

        let batch = collect_batch(first, &mut rx, &CancellationToken::new(), 2, Duration::from_secs(1)).await;

        assert_eq!(batch.len(), 2);
        assert_eq!(rx.len(), 3);
    }
}
