/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use rocketmq_common::ArcRefCellWrapper;
use tracing::info;
use tracing::warn;

use crate::consumer::consumer_impl::pop_request::PopRequest;
use crate::consumer::consumer_impl::pull_request::PullRequest;
use crate::factory::mq_client_instance::MQClientInstance;

#[derive(Clone)]
pub struct PullMessageService {
    pop_tx: Option<tokio::sync::mpsc::Sender<PopRequest>>,
    pull_tx: Option<tokio::sync::mpsc::Sender<PullRequest>>,
}

impl PullMessageService {
    pub fn new() -> Self {
        PullMessageService {
            pop_tx: None,
            pull_tx: None,
        }
    }
    pub async fn start(&mut self, instance: ArcRefCellWrapper<MQClientInstance>) {
        let (pop_tx, mut pop_rx) = tokio::sync::mpsc::channel(1024 * 4);
        let (pull_tx, mut pull_rx) = tokio::sync::mpsc::channel(1024 * 4);
        self.pop_tx = Some(pop_tx);
        self.pull_tx = Some(pull_tx);
        //let instance_wrapper = ArcRefCellWrapper::new(instance);
        let instance_wrapper_clone = instance.clone();
        tokio::spawn(async move {
            info!(
                ">>>>>>>>>>>>>>>>>>>>>>>PullMessageService [PopRequest] \
                 started<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
            );
            while let Some(request) = pop_rx.recv().await {
                if let Some(mut consumer) =
                    instance.select_consumer(request.get_consumer_group()).await
                {
                    consumer.pop_message(request).await;
                } else {
                    warn!(
                        "No matched consumer for the PopRequest {}, drop it",
                        request
                    )
                }
            }
        });
        tokio::spawn(async move {
            info!(
                ">>>>>>>>>>>>>>>>>>>>>>>PullMessageService  [PullRequest] \
                 started<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
            );
            while let Some(request) = pull_rx.recv().await {
                if let Some(mut consumer) = instance_wrapper_clone
                    .select_consumer(request.get_consumer_group())
                    .await
                {
                    consumer.pull_message(request).await;
                } else {
                    warn!(
                        "No matched consumer for the PullRequest {},drop it",
                        request
                    )
                }
            }
        });
    }

    pub fn execute_pull_request_later(&self, pull_request: PullRequest, time_delay: u64) {
        let this = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(time_delay)).await;
            if let Err(e) = this.pull_tx.as_ref().unwrap().send(pull_request).await {
                warn!("Failed to send pull request to pull_tx, error: {:?}", e);
            }
        });
    }

    pub async fn execute_pull_request_immediately(&self, pull_request: PullRequest) {
        if let Err(e) = self.pull_tx.as_ref().unwrap().send(pull_request).await {
            warn!("Failed to send pull request to pull_tx, error: {:?}", e);
        }
    }

    pub fn execute_pop_pull_request_later(&self, pop_request: PopRequest, time_delay: u64) {
        let this = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(time_delay)).await;
            if let Err(e) = this.pop_tx.as_ref().unwrap().send(pop_request).await {
                warn!("Failed to send pull request to pull_tx, error: {:?}", e);
            }
        });
    }

    pub async fn execute_pop_pull_request_immediately(&self, pop_request: PopRequest) {
        if let Err(e) = self.pop_tx.as_ref().unwrap().send(pop_request).await {
            warn!("Failed to send pull request to pull_tx, error: {:?}", e);
        }
    }
}
