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

use rocketmq_remoting::protocol::header::namesrv::broker_request::UnRegisterBrokerRequestHeader;
use rocketmq_runtime::ShutdownReport;
use rocketmq_runtime::TaskGroup;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing::warn;

use crate::bootstrap::NameServerRuntimeHandle;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) struct BatchUnregistrationService {
    name_server_runtime_inner: NameServerRuntimeHandle,
    tx: tokio::sync::mpsc::Sender<UnRegisterBrokerRequestHeader>,
    rx: parking_lot::Mutex<Option<tokio::sync::mpsc::Receiver<UnRegisterBrokerRequestHeader>>>,
    task_group: parking_lot::Mutex<Option<TaskGroup>>,
}

impl BatchUnregistrationService {
    pub(crate) fn new(name_server_runtime_inner: NameServerRuntimeHandle, queue_capacity: usize) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel::<UnRegisterBrokerRequestHeader>(queue_capacity);
        BatchUnregistrationService {
            name_server_runtime_inner,
            tx,
            rx: parking_lot::Mutex::new(Some(rx)),
            task_group: parking_lot::Mutex::new(None),
        }
    }

    pub fn submit(&self, request: UnRegisterBrokerRequestHeader) -> bool {
        if let Err(e) = self.tx.try_send(request) {
            warn!("submit unregister broker request failed: {:?}", e);
            return false;
        }
        true
    }

    pub fn start(&self) {
        if self.task_group.lock().is_some() {
            return;
        }

        let Some(task_group) = self
            .name_server_runtime_inner
            .task_group()
            .map(|task_group| task_group.child("namesrv.batch-unregistration"))
        else {
            warn!("BatchUnregistrationService cannot start because NameServer task group is unavailable");
            return;
        };

        let name_server_runtime_inner = self.name_server_runtime_inner.clone();
        let Some(mut rx) = self.rx.lock().take() else {
            warn!("BatchUnregistrationService receiver is unavailable");
            return;
        };
        let shutdown_token = task_group.cancellation_token();
        if let Err(error) = task_group.spawn_service("namesrv.batch-unregistration", async move {
            info!("BatchUnregistrationService started");
            run_batch_unregistration_service(name_server_runtime_inner, &mut rx, shutdown_token).await;
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
    rx: &mut tokio::sync::mpsc::Receiver<UnRegisterBrokerRequestHeader>,
    shutdown_token: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            _ = shutdown_token.cancelled() => {
                info!("BatchUnregistrationService shutdown");
                break;
            }
            first_request = rx.recv() => {
                match first_request {
                    Some(request) => {
                        let mut unregistration_requests = vec![request];

                        while let Ok(req) = rx.try_recv() {
                            unregistration_requests.push(req);
                        }

                        let Some(runtime) = name_server_runtime_inner.upgrade() else {
                            info!("BatchUnregistrationService stopped because NameServer runtime was released");
                            break;
                        };
                        runtime.route_info_manager().un_register_broker(unregistration_requests);
                    }
                    None => {
                        info!("BatchUnregistrationService channel closed");
                        break;
                    }
                }
            }
        }
    }
}
