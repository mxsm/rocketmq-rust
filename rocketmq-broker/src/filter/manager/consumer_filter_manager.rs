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

use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::common::filter::expression_type::ExpressionType;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_filter::utils::bloom_filter::BloomFilter;
use rocketmq_store::config::message_store_config::MessageStoreConfig;

use crate::broker_path_config_helper::get_consumer_filter_path;
use crate::filter::consumer_filter_data::ConsumerFilterData;
use crate::filter::manager::consumer_filter_wrapper::ConsumerFilterWrapper;

const MS_24_HOUR: u64 = 24 * 60 * 60 * 1000;

#[derive(Clone)]
pub(crate) struct ConsumerFilterManager {
    broker_config: Arc<BrokerConfig>,
    message_store_config: Arc<MessageStoreConfig>,
    consumer_filter_wrapper: Arc<parking_lot::RwLock<ConsumerFilterWrapper>>,
    bloom_filter: Option<BloomFilter>,
}

impl ConsumerFilterManager {
    pub fn new(mut broker_config: Arc<BrokerConfig>, message_store_config: Arc<MessageStoreConfig>) -> Self {
        let consumer_filter_wrapper = Arc::new(parking_lot::RwLock::new(ConsumerFilterWrapper::default()));
        let bloom_filter = BloomFilter::create_by_fn(
            broker_config.max_error_rate_of_bloom_filter,
            broker_config.expect_consumer_num_use_filter,
        )
        .unwrap();
        let broker_config_mut = Arc::make_mut(&mut broker_config);
        broker_config_mut.bit_map_length_consume_queue_ext = bloom_filter.m();
        ConsumerFilterManager {
            broker_config,
            message_store_config,
            consumer_filter_wrapper,
            bloom_filter: Some(bloom_filter),
        }
    }
}

//Fully implemented will be removed
#[allow(unused_variables)]
impl ConfigManager for ConsumerFilterManager {
    fn decode0(&mut self, key: &[u8], body: &[u8]) {
        todo!()
    }

    fn stop(&mut self) -> bool {
        todo!()
    }

    fn config_file_path(&self) -> String {
        get_consumer_filter_path(self.message_store_config.store_path_root_dir.as_str())
    }

    fn encode_pretty(&self, pretty_format: bool) -> String {
        "".to_string()
    }

    fn decode(&self, json_string: &str) {}
}

#[allow(unused_variables)]
impl ConsumerFilterManager {
    pub fn build(
        topic: CheetahString,
        consumer_group: CheetahString,
        expression: Option<CheetahString>,
        type_: Option<CheetahString>,
        client_version: u64,
    ) -> Option<ConsumerFilterData> {
        if ExpressionType::is_tag_type(type_.as_deref()) {
            return None;
        }

        let mut consumer_filter_data = ConsumerFilterData::default();
        consumer_filter_data.set_topic(topic);
        consumer_filter_data.set_consumer_group(consumer_group);
        consumer_filter_data.set_born_time(get_current_millis());
        consumer_filter_data.set_dead_time(0);
        consumer_filter_data.set_expression(expression);
        consumer_filter_data.set_expression_type(type_);
        consumer_filter_data.set_client_version(client_version);

        /*        let filter_factory = FilterFactory;
                match filter_factory.get(&type_).compile(&expression) {
                    Ok(compiled_expression) => {
                        consumer_filter_data.set_compiled_expression(compiled_expression);
                    }
                    Err(e) => {
                        eprintln!(
                            "parse error: expr={}, topic={}, group={}, error={}",
                            expression, topic, consumer_group, e
                        );
                        return None;
                    }
                }
        */
        Some(consumer_filter_data)
    }

    pub fn get_consumer_filter_data(
        &self,
        topic: &CheetahString,
        consumer_group: &CheetahString,
    ) -> Option<ConsumerFilterData> {
        None
    }

    pub fn bloom_filter(&self) -> Option<&BloomFilter> {
        self.bloom_filter.as_ref()
    }

    pub fn get(&self, topic: &CheetahString) -> Option<Vec<ConsumerFilterData>> {
        unimplemented!("get method not implemented");
    }
}
