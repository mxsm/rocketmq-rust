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
use std::collections::HashSet;
use std::fmt::Display;

use rocketmq_common::common::message::message_queue::MessageQueue;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct UnlockBatchRequestBody {
    pub consumer_group: Option<String>,
    pub client_id: Option<String>,
    pub only_this_broker: bool,
    pub mq_set: HashSet<MessageQueue>,
}

impl Display for UnlockBatchRequestBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "UnlockBatchRequestBody [consumer_group={}, client_id={}, only_this_broker={}, \
             mq_set={:?}]",
            self.consumer_group.as_ref().unwrap_or(&"".to_string()),
            self.client_id.as_ref().unwrap_or(&"".to_string()),
            self.only_this_broker,
            self.mq_set
        )
    }
}
