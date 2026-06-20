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

use rocketmq_remoting::protocol::header::namesrv::broker_request::UnRegisterBrokerRequestHeader;
use rocketmq_runtime::TaskGroup;
use rocketmq_rust::ArcMut;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing::warn;

use crate::bootstrap::NameServerRuntimeInner;

pub(crate) struct BatchUnregistrationService {
    name_server_runtime_inner: ArcMut<NameServerRuntimeInner>,
    tx: tokio::sync::mpsc::Sender<UnRegisterBrokerRequestHeader>,
    rx: Option<tokio::sync::mpsc::Receiver<UnRegisterBrokerRequestHeader>>,
    task_group: parking_lot::Mutex<Option<TaskGroup>>,
}

impl BatchUnregistrationService {
    pub(crate) fn new(name_server_runtime_inner: ArcMut<NameServerRuntimeInner>) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel::<UnRegisterBrokerRequestHeader>(
            name_server_runtime_inner
                .name_server_config()
                .unregister_broker_queue_capacity as usize,
        );
        BatchUnregistrationService {
            name_server_runtime_inner,
            tx,
            rx: Some(rx),
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

    pub fn start(&mut self) {
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

        let mut name_server_runtime_inner = self.name_server_runtime_inner.clone();
        let mut rx = self.rx.take().expect("rx is None");
        let shutdown_token = task_group.cancellation_token();
        if let Err(error) = task_group.spawn_service("namesrv.batch-unregistration", async move {
            info!("BatchUnregistrationService started");
            run_batch_unregistration_service(&mut name_server_runtime_inner, &mut rx, shutdown_token).await;
        }) {
            warn!("BatchUnregistrationService cannot start because task spawn failed: {error}");
            return;
        }

        *self.task_group.lock() = Some(task_group);
    }

    pub fn shutdown(&self) {
        if let Some(task_group) = self.task_group.lock().take() {
            let report = task_group.shutdown_now();
            if let Err(error) = report.assert_no_task_leak() {
                warn!("BatchUnregistrationService shutdown report is unhealthy: {error}");
            }
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
    name_server_runtime_inner: &mut ArcMut<NameServerRuntimeInner>,
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

                        name_server_runtime_inner
                            .route_info_manager_mut()
                            .un_register_broker(unregistration_requests);
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
