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

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use cheetah_string::CheetahString;
use parking_lot::Mutex;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_runtime::TaskGroup;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use rocketmq_store::consume_queue::cq_ext_unit::CqExtUnit;
use tokio::sync::Notify;
use tokio::time::Instant;
use tracing::info;
use tracing::warn;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::long_polling::many_pull_request::ManyPullRequest;
use crate::long_polling::pull_request::PullRequest;
use crate::processor::pull_message_processor::PullMessageProcessor;

const TOPIC_QUEUE_ID_SEPARATOR: &str = "@";

pub struct PullRequestHoldService<MS: MessageStore> {
    pull_request_table: Arc<parking_lot::RwLock<HashMap<String, ManyPullRequest>>>,
    pull_message_processor: ArcMut<PullMessageProcessor<MS>>,
    shutdown: Arc<Notify>,
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    running: AtomicBool,
    task_group: Mutex<Option<TaskGroup>>,
}

impl<MS> PullRequestHoldService<MS>
where
    MS: MessageStore + Send + Sync,
{
    pub fn new(
        pull_message_processor: ArcMut<PullMessageProcessor<MS>>,
        broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    ) -> Self {
        PullRequestHoldService {
            pull_request_table: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            pull_message_processor,
            shutdown: Arc::new(Default::default()),
            broker_runtime_inner,
            running: AtomicBool::new(false),
            task_group: Mutex::new(None),
        }
    }
}

#[allow(unused_variables)]
impl<MS> PullRequestHoldService<MS>
where
    MS: MessageStore + Send + Sync,
{
    pub fn start(&mut self, this: ArcMut<BrokerRuntimeInner<MS>>) {
        if self
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let Some(task_group) = this.broker_task_group_or_current(
            "rocketmq-broker.long-polling.pull-request-hold",
            "failed to start PullRequestHoldService outside Tokio runtime",
        ) else {
            self.running.store(false, Ordering::Release);
            return;
        };
        let cancellation_token = task_group.cancellation_token();

        if let Err(error) = task_group.spawn_service("broker.long-polling.pull-request-hold.scan", async move {
            loop {
                let handle_future = if this.broker_config().long_polling_enable {
                    tokio::time::sleep(tokio::time::Duration::from_secs(5))
                } else {
                    tokio::time::sleep(tokio::time::Duration::from_millis(
                        this.broker_config().short_polling_time_mills,
                    ))
                };
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        if let Some(service) = this.pull_request_hold_service() {
                            service.running.store(false, Ordering::Release);
                        }
                        info!("PullRequestHoldService: shutdown..........");
                        break;
                    }
                    _ = handle_future => {}
                    _ = this.pull_request_hold_service().as_ref().unwrap().shutdown.notified() => {
                        if let Some(service) = this.pull_request_hold_service() {
                            service.running.store(false, Ordering::Release);
                        }
                        info!("PullRequestHoldService: shutdown..........");
                        break;
                    }
                }
                let instant = Instant::now();
                this.pull_request_hold_service().as_ref().unwrap().check_hold_request();
                let elapsed = instant.elapsed().as_millis();
                if elapsed > 5000 {
                    warn!("PullRequestHoldService: check hold pull request cost {}ms", elapsed);
                }
            }
        }) {
            self.running.store(false, Ordering::Release);
            warn!(?error, "failed to spawn PullRequestHoldService scan task");
            return;
        }

        *self.task_group.lock() = Some(task_group);
    }

    pub async fn shutdown(&mut self) {
        self.running.store(false, Ordering::Release);
        self.shutdown.notify_waiters();
        let task_group = self.task_group.lock().take();
        if let Some(task_group) = task_group {
            let report = task_group.shutdown(Duration::from_secs(5)).await;
            if !report.is_healthy() {
                warn!(
                    report = %report.to_json(),
                    "PullRequestHoldService shutdown report is unhealthy"
                );
            }
        }
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    pub fn suspend_pull_request(&self, topic: &str, queue_id: i32, mut pull_request: PullRequest) {
        let key = build_key(topic, queue_id);
        let mut table = self.pull_request_table.write();
        let mpr = table.entry(key).or_insert_with(ManyPullRequest::new);
        pull_request.request_command_mut().set_suspended_ref(true);
        mpr.add_pull_request(pull_request);
    }

    fn check_hold_request(&self) {
        let binding = self.pull_request_table.read();
        let keys = binding.keys().cloned().collect::<Vec<String>>();
        drop(binding);
        for key in keys {
            let key_parts: Vec<&str> = key.split(TOPIC_QUEUE_ID_SEPARATOR).collect();
            if key_parts.len() != 2 {
                continue;
            }
            let topic = CheetahString::from(key_parts[0]);
            let queue_id = key_parts[1].parse::<i32>().unwrap();
            let max_offset = self
                .broker_runtime_inner
                .message_store()
                .unwrap()
                .get_max_offset_in_queue(&topic, queue_id);
            self.notify_message_arriving(&topic, queue_id, max_offset);
        }
    }

    pub fn notify_message_arriving(&self, topic: &CheetahString, queue_id: i32, max_offset: i64) {
        self.notify_message_arriving_ext(topic, queue_id, max_offset, None, 0, None, None);
    }

    pub fn notify_message_arriving_ext(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        max_offset: i64,
        tags_code: Option<i64>,
        msg_store_time: i64,
        filter_bit_map: Option<Vec<u8>>,
        properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) {
        let key = build_key(topic, queue_id);
        let mut table = self.pull_request_table.write();
        if let Some(mpr) = table.get_mut(&key) {
            if let Some(request_list) = mpr.clone_list_and_clear() {
                if request_list.is_empty() {
                    return;
                }
                drop(table);
                let mut replay_list = Vec::new();

                for request in request_list {
                    let mut newest_offset = max_offset;
                    if newest_offset <= request.pull_from_this_offset() {
                        newest_offset = self
                            .broker_runtime_inner
                            .message_store()
                            .unwrap()
                            .get_max_offset_in_queue(topic, queue_id);
                    }

                    if newest_offset > request.pull_from_this_offset() {
                        let match_by_consume_queue = request.message_filter().is_matched_by_consume_queue(
                            tags_code,
                            Some(&CqExtUnit::new(
                                tags_code.unwrap_or(0),
                                msg_store_time,
                                filter_bit_map.clone(),
                            )),
                        );
                        let mut match_by_commit_log = match_by_consume_queue;
                        if match_by_consume_queue && properties.is_some() {
                            match_by_commit_log = request.message_filter().is_matched_by_commit_log(None, properties);
                        }

                        if match_by_commit_log {
                            let pull_message_this = self.pull_message_processor.clone();
                            self.pull_message_processor.execute_request_when_wakeup(
                                pull_message_this,
                                request.client_channel().clone(),
                                request.connection_handler_context().clone(),
                                request.request_command().clone(),
                            );
                            continue;
                        }
                    }

                    if current_millis() >= (request.suspend_timestamp() + request.timeout_millis()) {
                        let pull_message_this = self.pull_message_processor.clone();
                        self.pull_message_processor.execute_request_when_wakeup(
                            pull_message_this,
                            request.client_channel().clone(),
                            request.connection_handler_context().clone(),
                            request.request_command().clone(),
                        );
                        continue;
                    }

                    replay_list.push(request);
                }

                if !replay_list.is_empty() {
                    let mut table = self.pull_request_table.write();
                    let mpr = table.entry(key).or_insert_with(ManyPullRequest::new);
                    mpr.add_pull_requests(replay_list);
                }
            }
        }
    }

    pub async fn notify_master_online(&self) {
        for mpr in self.pull_request_table.read().values() {
            if let Some(request_list) = mpr.clone_list_and_clear() {
                for request in request_list {
                    info!(
                        "notify master online, wakeup {}",
                        //  request.client_channel(),
                        request.request_command()
                    );
                    let pull_message_this = self.pull_message_processor.clone();
                    self.pull_message_processor.execute_request_when_wakeup(
                        pull_message_this,
                        request.client_channel().clone(),
                        request.connection_handler_context().clone(),
                        request.request_command().clone(),
                    );
                }
            }
        }
    }
}

fn build_key(topic: &str, queue_id: i32) -> String {
    format!("{topic}{TOPIC_QUEUE_ID_SEPARATOR}{queue_id}")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;

    use super::*;
    use crate::broker_runtime::BrokerRuntime;
    use crate::processor::default_pull_message_result_handler::DefaultPullMessageResultHandler;

    #[tokio::test]
    async fn start_is_idempotent_and_shutdown_stops_background_task() {
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let mut broker_runtime = BrokerRuntime::new(broker_config, message_store_config);
        let inner = broker_runtime.inner_for_test().clone();
        let result_handler = ArcMut::new(DefaultPullMessageResultHandler::new(
            Arc::new(Default::default()),
            inner.clone(),
        ));
        let pull_message_processor = ArcMut::new(PullMessageProcessor::new(result_handler, inner.clone()));
        let mut service = PullRequestHoldService::new(pull_message_processor, inner.clone());

        service.start(inner.clone());
        service.start(inner);
        assert!(service.is_running());

        service.shutdown().await;
        assert!(!service.is_running());
    }
}
