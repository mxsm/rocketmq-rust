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
pub mod allocate_message_queue_averagely;
pub mod allocate_message_queue_averagely_by_circle;
pub mod allocate_message_queue_by_config;
pub mod allocate_message_queue_by_machine_room;

use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_queue::MessageQueue;
use tracing::info;

use crate::client_error::MQClientError::IllegalArgumentError;
use crate::Result;

pub fn check(
    consumer_group: &CheetahString,
    current_cid: &CheetahString,
    mq_all: &[MessageQueue],
    cid_all: &[CheetahString],
) -> Result<bool> {
    if current_cid.is_empty() {
        return Err(IllegalArgumentError("currentCID is empty".to_string()));
    }
    if mq_all.is_empty() {
        return Err(IllegalArgumentError(
            "mqAll is null or mqAll empty".to_string(),
        ));
    }
    if cid_all.is_empty() {
        return Err(IllegalArgumentError(
            "cidAll is null or cidAll empty".to_string(),
        ));
    }

    let cid_set: HashSet<_> = cid_all.iter().collect();
    if !cid_set.contains(current_cid) {
        info!(
            "[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {:?}",
            consumer_group, current_cid, cid_all
        );
        return Ok(false);
    }
    Ok(true)
}
