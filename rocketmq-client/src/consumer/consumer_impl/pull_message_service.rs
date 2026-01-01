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

use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_rust::ArcMut;
use rocketmq_rust::Shutdown;
use tracing::info;
use tracing::warn;

use crate::consumer::consumer_impl::message_request::MessageRequest;
use crate::consumer::consumer_impl::pop_request::PopRequest;
use crate::consumer::consumer_impl::pull_request::PullRequest;
use crate::factory::mq_client_instance::MQClientInstance;

#[derive(Clone)]
pub struct PullMessageService {
    tx: Option<tokio::sync::mpsc::Sender<Box<dyn MessageRequest + Send + 'static>>>,
    tx_shutdown: Option<tokio::sync::broadcast::Sender<()>>,
}

impl PullMessageService {
    pub fn new() -> Self {
        PullMessageService {
            tx: None,
            tx_shutdown: None,
        }
    }
    pub async fn start(&mut self, mut instance: ArcMut<MQClientInstance>) {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Box<dyn MessageRequest + Send + 'static>>(1024 * 4);
        let (mut shutdown, tx_shutdown) = Shutdown::new(1);
        self.tx = Some(tx);
        self.tx_shutdown = Some(tx_shutdown);
        tokio::spawn(async move {
            info!(">>>>>>>>>>>>>>>>>>>>>>>PullMessageService  started<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
            /*while let Some(request) = rx.recv().await {
                if request.get_message_request_mode() == MessageRequestMode::Pull {
                    let pull_request =
                        unsafe { *Box::from_raw(Box::into_raw(request) as *mut PullRequest) };
                    PullMessageService::pull_message(pull_request, instance.as_mut()).await;
                } else {
                    let pop_request =
                        unsafe { *Box::from_raw(Box::into_raw(request) as *mut PopRequest) };
                    PullMessageService::pop_message(pop_request, instance.as_mut()).await;
                }
            }*/
            if shutdown.is_shutdown() {
                info!("PullMessageService shutdown");
                return;
            }
            loop {
                tokio::select! {
                    _ = shutdown.recv() => {
                        info!("PullMessageService shutdown");
                    }
                    Some(request) = rx.recv() => {
                        if request.get_message_request_mode() == MessageRequestMode::Pull {
                            let pull_request =
                                unsafe { *Box::from_raw(Box::into_raw(request) as *mut PullRequest) };
                            PullMessageService::pull_message(pull_request, instance.as_mut()).await;
                        } else {
                            let pop_request =
                                unsafe { *Box::from_raw(Box::into_raw(request) as *mut PopRequest) };
                            PullMessageService::pop_message(pop_request, instance.as_mut()).await;
                        }
                    }
                }
                if shutdown.is_shutdown() {
                    info!("PullMessageService shutdown");
                    break;
                }
            }
        });
    }

    async fn pull_message(request: PullRequest, instance: &mut MQClientInstance) {
        if let Some(mut consumer) = instance.select_consumer(request.get_consumer_group()).await {
            consumer.pull_message(request).await;
        } else {
            warn!("No matched consumer for the PullRequest {},drop it", request)
        }
    }

    async fn pop_message(request: PopRequest, instance: &mut MQClientInstance) {
        if let Some(mut consumer) = instance.select_consumer(request.get_consumer_group()).await {
            consumer.pop_message(request).await;
        } else {
            warn!("No matched consumer for the PopRequest {}, drop it", request)
        }
    }

    pub fn execute_pull_request_later(&self, pull_request: PullRequest, time_delay: u64) {
        let this = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(time_delay)).await;
            if let Err(e) = this.tx.as_ref().unwrap().send(Box::new(pull_request)).await {
                warn!("Failed to send pull request to pull_tx, error: {:?}", e);
            }
        });
    }

    pub async fn execute_pull_request_immediately(&self, pull_request: PullRequest) {
        if let Err(e) = self.tx.as_ref().unwrap().send(Box::new(pull_request)).await {
            warn!("Failed to send pull request to pull_tx, error: {:?}", e);
        }
    }

    pub fn execute_pop_pull_request_later(&self, pop_request: PopRequest, time_delay: u64) {
        let this = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(time_delay)).await;
            if let Err(e) = this.tx.as_ref().unwrap().send(Box::new(pop_request)).await {
                warn!("Failed to send pull request to pull_tx, error: {:?}", e);
            }
        });
    }

    pub async fn execute_pop_pull_request_immediately(&self, pop_request: PopRequest) {
        if let Err(e) = self.tx.as_ref().unwrap().send(Box::new(pop_request)).await {
            warn!("Failed to send pull request to pull_tx, error: {:?}", e);
        }
    }

    pub fn shutdown(&self) {
        if let Some(tx_shutdown) = &self.tx_shutdown {
            if let Err(e) = tx_shutdown.send(()) {
                warn!("Failed to send shutdown signal to pull_tx, error: {:?}", e);
            }
        } else {
            warn!("Attempted to shutdown but tx_shutdown is None. Ensure `start` is called before `shutdown`.");
        }
    }
}
