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
use std::hash::Hash;

use cheetah_string::CheetahString;
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use serde::Deserialize;
use serde::Serialize;

use crate::protocol::heartbeat::consume_type::ConsumeType;
use crate::protocol::heartbeat::message_model::MessageModel;
use crate::protocol::heartbeat::subscription_data::SubscriptionData;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerData {
    pub group_name: CheetahString,
    pub consume_type: ConsumeType,
    pub message_model: MessageModel,
    pub consume_from_where: ConsumeFromWhere,
    pub subscription_data_set: HashSet<SubscriptionData>,
    pub unit_mode: bool,
}

impl Hash for ConsumerData {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.group_name.hash(state);
        self.consume_type.hash(state);
        self.message_model.hash(state);
        self.consume_from_where.hash(state);
        self.subscription_data_set
            .iter()
            .for_each(|code| code.hash(state));
        self.unit_mode.hash(state);
    }
}
