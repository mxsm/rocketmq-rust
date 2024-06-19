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

use std::sync::Arc;

use rocketmq_common::common::broker::broker_config::BrokerConfig;
use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_common::common::filter::expression_type::ExpressionType;
use rocketmq_common::TimeUtils::get_current_millis;

use crate::broker_path_config_helper::get_consumer_filter_path;
use crate::filter::consumer_filter_data::ConsumerFilterData;

#[derive(Default)]
pub(crate) struct ConsumerFilterManager {
    pub(crate) broker_config: Arc<BrokerConfig>,
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
        get_consumer_filter_path(self.broker_config.store_path_root_dir.as_str())
    }

    fn encode(&mut self) -> String {
        todo!()
    }

    fn encode_pretty(&self, pretty_format: bool) -> String {
        "".to_string()
    }

    fn decode(&self, json_string: &str) {}
}

impl ConsumerFilterManager {
    pub fn build(
        topic: &str,
        consumer_group: &str,
        expression: &str,
        type_: &str,
        client_version: u64,
    ) -> Option<ConsumerFilterData> {
        if ExpressionType::is_tag_type(Some(type_)) {
            return None;
        }

        let mut consumer_filter_data = ConsumerFilterData::default();
        consumer_filter_data.set_topic(topic.to_string());
        consumer_filter_data.set_consumer_group(consumer_group.to_string());
        consumer_filter_data.set_born_time(get_current_millis());
        consumer_filter_data.set_dead_time(0);
        consumer_filter_data.set_expression(expression.to_string());
        consumer_filter_data.set_expression_type(type_.to_string());
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
}
