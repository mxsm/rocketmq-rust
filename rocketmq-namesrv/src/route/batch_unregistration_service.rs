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

use std::time::Duration;

use rocketmq_observability::metrics::namesrv::NameServerMetrics;
use rocketmq_protocol::protocol::header::namesrv::broker_request::UnRegisterBrokerRequestHeader;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing::warn;

use crate::bootstrap::NameServerRuntimeHandle;
use crate::route::types::BrokerGeneration;
use crate::route::types::BrokerInstanceKey;
use crate::route::types::RemotingConnectionId;

use self::queue::Submission;
use self::queue::UnregistrationBatch;
use self::queue::UnregistrationQueue;
use self::queue::UnregistrationReceiver;

mod queue;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub(crate) struct BrokerUnregistrationRequest {
    pub(crate) header: UnRegisterBrokerRequestHeader,
    pub(crate) expected_channel_id: Option<RemotingConnectionId>,
    pub(crate) expected_generation: Option<BrokerGeneration>,
    enqueued_at: Option<Instant>,
}

impl BrokerUnregistrationRequest {
    pub(crate) fn explicit(header: UnRegisterBrokerRequestHeader) -> Self {
        Self {
            header,
            expected_channel_id: None,
            expected_generation: None,
            enqueued_at: None,
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
            enqueued_at: None,
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
    queue: UnregistrationQueue,
    lifecycle: parking_lot::Mutex<ServiceLifecycle>,
    batch_size: usize,
    batch_time: Duration,
    metrics: NameServerMetrics,
}

enum ServiceLifecycle {
    Created(UnregistrationReceiver),
    Running(TaskGroup),
    Draining(TaskGroup),
    Stopped(Option<TaskGroup>),
    Failed,
}

impl BatchUnregistrationService {
    pub(crate) fn new(
        name_server_runtime_inner: NameServerRuntimeHandle,
        queue_capacity: usize,
        batch_size: usize,
        batch_time: Duration,
        metrics: NameServerMetrics,
    ) -> Self {
        let (queue, rx) = UnregistrationQueue::new(queue_capacity, metrics.clone());
        BatchUnregistrationService {
            name_server_runtime_inner,
            queue,
            lifecycle: parking_lot::Mutex::new(ServiceLifecycle::Created(rx)),
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
        match self.queue.submit(request) {
            Submission::Queued | Submission::Coalesced => true,
            Submission::Fallback(reason, request) => {
                let Some(runtime) = self.name_server_runtime_inner.upgrade() else {
                    warn!("cannot process unregister request because NameServer runtime is unavailable");
                    return false;
                };
                // Fallback executes on the caller and is outside the worker's drain.
                warn!(
                    reason = reason.as_str(),
                    "unregister queue unavailable; applying one request synchronously"
                );
                self.queue.record_fallback(reason);
                runtime.route_info_manager().un_register_broker_requests(vec![request]);
                true
            }
        }
    }

    pub(crate) fn start(&self) -> crate::NameServerResult<()> {
        self.start_locked(&mut self.lifecycle.lock())
    }

    fn start_locked(&self, lifecycle: &mut ServiceLifecycle) -> crate::NameServerResult<()> {
        if matches!(lifecycle, ServiceLifecycle::Running(_)) {
            if self.queue.is_closed() {
                return Err(crate::namesrv_error::startup_state(
                    "batch unregistration worker already stopped",
                ));
            }
            return Ok(());
        }
        if !matches!(lifecycle, ServiceLifecycle::Created(_)) {
            return Err(crate::namesrv_error::startup_state(
                "restart stopped batch unregistration service",
            ));
        }
        let ServiceLifecycle::Created(rx) = std::mem::replace(lifecycle, ServiceLifecycle::Failed) else {
            return Err(crate::namesrv_error::invariant(
                "namesrv.batch-unregistration.start-state",
            ));
        };
        let runtime = self
            .name_server_runtime_inner
            .upgrade()
            .ok_or_else(|| crate::namesrv_error::not_initialized("namesrv.batch-unregistration.runtime"))?;
        let task_group = runtime.component_task_group("namesrv.batch-unregistration");
        let name_server_runtime_inner = self.name_server_runtime_inner.clone();
        let shutdown_token = task_group.cancellation_token();
        let batch_size = self.batch_size;
        let batch_time = self.batch_time;
        let metrics = self.metrics.clone();
        task_group
            .spawn_service("namesrv.batch-unregistration", async move {
                info!("BatchUnregistrationService started");
                run_batch_unregistration_service(
                    name_server_runtime_inner,
                    rx,
                    shutdown_token,
                    batch_size,
                    batch_time,
                    metrics,
                )
                .await;
            })
            .map_err(|error| crate::namesrv_error::startup("start batch unregistration service", error))?;

        *lifecycle = ServiceLifecycle::Running(task_group);
        Ok(())
    }

    pub async fn shutdown(&self) -> Option<ShutdownReport> {
        self.shutdown_until(ShutdownDeadline::after(SHUTDOWN_TIMEOUT)).await
    }

    pub(crate) async fn shutdown_until(&self, deadline: ShutdownDeadline) -> Option<ShutdownReport> {
        let task_group = {
            let mut lifecycle = self.lifecycle.lock();
            self.queue.close();
            if let ServiceLifecycle::Created(rx) = &mut *lifecycle {
                // Seal the actual channel as well as the fast admission flag.
                // A producer that reserved capacity before close must still be
                // received, even if it has not published its request yet.
                rx.close();
                if rx.is_drained() {
                    *lifecycle = ServiceLifecycle::Stopped(None);
                } else if let Err(error) = self.start_locked(&mut lifecycle) {
                    warn!("cannot drain unstarted batch unregistration service: {error}");
                }
            }
            match &*lifecycle {
                ServiceLifecycle::Running(group) | ServiceLifecycle::Draining(group) => {
                    let group = group.clone();
                    *lifecycle = ServiceLifecycle::Draining(group.clone());
                    group
                }
                ServiceLifecycle::Stopped(Some(group)) => group.clone(),
                ServiceLifecycle::Created(_) | ServiceLifecycle::Stopped(None) | ServiceLifecycle::Failed => {
                    return None
                }
            }
        };
        // Keep the group in the lifecycle so cancellation of this wait does not
        // lose ownership, and repeated callers share its retained shutdown report.
        let report = task_group
            .shutdown_until(ShutdownDeadline::at(
                deadline
                    .instant()
                    .min(ShutdownDeadline::after(SHUTDOWN_TIMEOUT).instant()),
            ))
            .await;
        *self.lifecycle.lock() = ServiceLifecycle::Stopped(Some(task_group));
        if let Err(error) = report.assert_no_task_leak() {
            warn!("BatchUnregistrationService shutdown report is unhealthy: {error}");
        }
        Some(report)
    }

    /// Returns the queued request count, excluding the batch being processed.
    #[cfg(test)]
    pub fn queue_length(&self) -> usize {
        self.queue.len()
    }
}

async fn run_batch_unregistration_service(
    name_server_runtime_inner: NameServerRuntimeHandle,
    mut rx: UnregistrationReceiver,
    shutdown_token: CancellationToken,
    batch_size: usize,
    batch_time: Duration,
    metrics: NameServerMetrics,
) {
    loop {
        let batch = tokio::select! {
            biased;
            _ = shutdown_token.cancelled(), if !rx.is_closed() => {
                info!("BatchUnregistrationService draining for shutdown");
                rx.close();
                rx.recv_batch(batch_size).await
            }
            batch = rx.recv_batch(batch_size) => batch,
        };
        let Some(batch) = batch else {
            info!("BatchUnregistrationService channel drained");
            break;
        };

        let batch = collect_batch(batch, &mut rx, &shutdown_token, batch_size, batch_time).await;
        let Some(runtime) = name_server_runtime_inner.upgrade() else {
            info!("BatchUnregistrationService stopped because NameServer runtime was released");
            break;
        };
        let (unregistration_requests, registrations) = batch.into_parts();
        let batch_len = unregistration_requests.len();
        runtime
            .route_info_manager()
            .un_register_broker_requests(unregistration_requests);
        metrics.record_unregistration_batch(batch_len);
        drop(registrations);
        tokio::task::yield_now().await;
    }
}

async fn collect_batch(
    mut batch: UnregistrationBatch,
    rx: &mut UnregistrationReceiver,
    shutdown_token: &CancellationToken,
    batch_size: usize,
    batch_time: Duration,
) -> UnregistrationBatch {
    let deadline = Instant::now() + batch_time;
    while batch.len() < batch_size {
        tokio::select! {
            biased;
            _ = shutdown_token.cancelled(), if !rx.is_closed() => {
                rx.close();
            }
            received = tokio::time::timeout_at(deadline, rx.recv_into(&mut batch)) => match received {
                Ok(true) => {}
                Ok(false) | Err(_) => break,
            },
        }
    }
    batch
}

#[cfg(test)]
mod tests;
