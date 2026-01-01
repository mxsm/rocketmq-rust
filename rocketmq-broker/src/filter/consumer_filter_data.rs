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

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_filter::expression::Expression;
use rocketmq_filter::utils::bloom_filter_data::BloomFilterData;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerFilterData {
    consumer_group: CheetahString,
    topic: CheetahString,
    expression: Option<CheetahString>,
    expression_type: Option<CheetahString>,
    #[serde(skip)]
    compiled_expression: Option<Arc<Box<dyn Expression + Send + Sync + 'static>>>,
    born_time: u64,
    dead_time: u64,
    bloom_filter_data: Option<BloomFilterData>,
    client_version: u64,
}

impl ConsumerFilterData {
    pub fn consumer_group(&self) -> &CheetahString {
        &self.consumer_group
    }

    pub fn topic(&self) -> &CheetahString {
        &self.topic
    }

    pub fn expression(&self) -> Option<&CheetahString> {
        self.expression.as_ref()
    }

    pub fn expression_type(&self) -> Option<&CheetahString> {
        self.expression_type.as_ref()
    }

    pub fn born_time(&self) -> u64 {
        self.born_time
    }

    pub fn dead_time(&self) -> u64 {
        self.dead_time
    }

    pub fn bloom_filter_data(&self) -> Option<&BloomFilterData> {
        self.bloom_filter_data.as_ref()
    }

    pub fn client_version(&self) -> u64 {
        self.client_version
    }

    pub fn set_consumer_group(&mut self, consumer_group: CheetahString) {
        self.consumer_group = consumer_group;
    }

    pub fn set_topic(&mut self, topic: CheetahString) {
        self.topic = topic;
    }

    pub fn set_expression(&mut self, expression: Option<CheetahString>) {
        self.expression = expression;
    }

    pub fn set_expression_type(&mut self, expression_type: Option<CheetahString>) {
        self.expression_type = expression_type;
    }

    pub fn set_born_time(&mut self, born_time: u64) {
        self.born_time = born_time;
    }

    pub fn set_dead_time(&mut self, dead_time: u64) {
        self.dead_time = dead_time;
    }

    pub fn set_bloom_filter_data(&mut self, bloom_filter_data: Option<BloomFilterData>) {
        self.bloom_filter_data = bloom_filter_data;
    }

    pub fn set_client_version(&mut self, client_version: u64) {
        self.client_version = client_version;
    }

    pub fn compiled_expression(&self) -> &Option<Arc<Box<dyn Expression + Send + Sync + 'static>>> {
        &self.compiled_expression
    }
}

impl Debug for ConsumerFilterData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumerFilterData")
            .field("consumer_group", &self.consumer_group)
            .field("topic", &self.topic)
            .field("expression", &self.expression)
            .field("expression_type", &self.expression_type)
            .field("compiled_expression", &self.compiled_expression.is_some())
            .field("born_time", &self.born_time)
            .field("dead_time", &self.dead_time)
            .field("bloom_filter_data", &self.bloom_filter_data)
            .field("client_version", &self.client_version)
            .finish()
    }
}

impl Display for ConsumerFilterData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ConsumerFilterData {{ group: {}, topic: {}, expression: {:?}, type: {:?}, has_compiled: {}, born: {}, \
             dead: {}, has_bloom: {}, client_ver: {} }}",
            self.consumer_group,
            self.topic,
            self.expression,
            self.expression_type,
            self.compiled_expression.is_some(),
            self.born_time,
            self.dead_time,
            self.bloom_filter_data.is_some(),
            self.client_version
        )
    }
}
