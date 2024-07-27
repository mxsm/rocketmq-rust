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

use std::collections::HashMap;
use std::sync::Arc;

use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_store::consume_queue::consume_queue_ext::CqExtUnit;
use rocketmq_store::log_file::MessageStore;
use tokio::sync::Notify;
use tokio::time::Instant;
use tracing::info;
use tracing::warn;

use crate::long_polling::many_pull_request::ManyPullRequest;
use crate::long_polling::pull_request::PullRequest;
use crate::processor::pull_message_processor::PullMessageProcessor;

const TOPIC_QUEUE_ID_SEPARATOR: &str = "@";

#[derive(Clone)]
pub struct PullRequestHoldService<MS> {
    pull_request_table: Arc<parking_lot::RwLock<HashMap<String, ManyPullRequest>>>,
    pull_message_processor: Arc<PullMessageProcessor<MS>>,
    message_store: Arc<MS>,
    broker_config: Arc<BrokerConfig>,
    shutdown: Arc<Notify>,
}

impl<MS> PullRequestHoldService<MS>
where
    MS: MessageStore + Send + Sync,
{
    pub fn new(
        message_store: Arc<MS>,
        pull_message_processor: Arc<PullMessageProcessor<MS>>,
        broker_config: Arc<BrokerConfig>,
    ) -> Self {
        PullRequestHoldService {
            pull_request_table: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            pull_message_processor,
            message_store,
            broker_config,
            shutdown: Arc::new(Default::default()),
        }
    }
}

#[allow(unused_variables)]
impl<MS> PullRequestHoldService<MS>
where
    MS: MessageStore + Send + Sync,
{
    pub fn start(&mut self) {
        let self_clone = self.clone();
        tokio::spawn(async move {
            loop {
                let handle_future = if self_clone.broker_config.long_polling_enable {
                    tokio::time::sleep(tokio::time::Duration::from_secs(5))
                } else {
                    tokio::time::sleep(tokio::time::Duration::from_millis(
                        self_clone.broker_config.short_polling_time_mills,
                    ))
                };
                tokio::select! {
                    _ = handle_future => {}
                    _ = self_clone.shutdown.notified() => {
                        info!("PullRequestHoldService: shutdown..........");
                        break;
                    }
                }
                let instant = Instant::now();
                self_clone.check_hold_request();
                let elapsed = instant.elapsed().as_millis();
                if elapsed > 5000 {
                    warn!(
                        "PullRequestHoldService: check hold pull request cost {}ms",
                        elapsed
                    );
                }
            }
        });
    }

    pub fn shutdown(&mut self) {
        self.shutdown.notify_waiters();
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
            let topic = key_parts[0];
            let queue_id = key_parts[1].parse::<i32>().unwrap();
            /*info!(
                "check hold request, topic: {}, queue_id: {}",
                topic, queue_id
            );*/
            let max_offset = self.message_store.get_max_offset_in_queue(topic, queue_id);
            self.notify_message_arriving(topic, queue_id, max_offset);
        }
    }

    pub fn notify_message_arriving(&self, topic: &str, queue_id: i32, max_offset: i64) {
        self.notify_message_arriving_ext(topic, queue_id, max_offset, None, 0, None, None);
    }

    pub fn notify_message_arriving_ext(
        &self,
        topic: &str,
        queue_id: i32,
        max_offset: i64,
        tags_code: Option<i64>,
        msg_store_time: i64,
        filter_bit_map: Option<Vec<u8>>,
        properties: Option<&HashMap<String, String>>,
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
                        newest_offset = self.message_store.get_max_offset_in_queue(topic, queue_id);
                    }

                    if newest_offset > request.pull_from_this_offset() {
                        let match_by_consume_queue =
                            request.message_filter().is_matched_by_consume_queue(
                                tags_code,
                                Some(&CqExtUnit::new(
                                    tags_code.unwrap_or(0),
                                    msg_store_time,
                                    filter_bit_map.clone(),
                                )),
                            );
                        let mut match_by_commit_log = match_by_consume_queue;
                        if match_by_consume_queue && properties.is_some() {
                            match_by_commit_log = request
                                .message_filter()
                                .is_matched_by_commit_log(None, properties);
                        }

                        if match_by_commit_log {
                            self.pull_message_processor.execute_request_when_wakeup(
                                request.client_channel().clone(),
                                request.connection_handler_context().clone(),
                                request.request_command().clone(),
                            );
                            continue;
                        }
                    }

                    if get_current_millis()
                        >= (request.suspend_timestamp() + request.timeout_millis())
                    {
                        self.pull_message_processor.execute_request_when_wakeup(
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
        for (_, mpr) in self.pull_request_table.read().iter() {
            if let Some(request_list) = mpr.clone_list_and_clear() {
                for request in request_list {
                    info!(
                        "notify master online, wakeup {:?} {}",
                        request.client_channel(),
                        request.request_command()
                    );
                    self.pull_message_processor.execute_request_when_wakeup(
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
    format!("{}{}{}", topic, TOPIC_QUEUE_ID_SEPARATOR, queue_id)
}
