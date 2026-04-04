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
use std::sync::Arc;

use bytes::Bytes;
use cheetah_string::CheetahString;
use rocketmq_common::common::filter::expression_type::ExpressionType;
use rocketmq_common::common::message::message_decoder;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_store::consume_queue::cq_ext_unit::CqExtUnit;
use rocketmq_store::filter::MessageFilter;

use crate::filter::consumer_filter_data::ConsumerFilterData;
use crate::filter::manager::consumer_filter_manager::ConsumerFilterManager;
use crate::filter::message_evaluation_context::MessageEvaluationContext;

pub struct ExpressionMessageFilter {
    subscription_data: Option<SubscriptionData>,
    consumer_filter_data: Option<ConsumerFilterData>,
    consumer_filter_manager: Arc<ConsumerFilterManager>,
    bloom_data_valid: bool,
}

impl ExpressionMessageFilter {
    pub fn new(
        subscription_data: Option<SubscriptionData>,
        consumer_filter_data: Option<ConsumerFilterData>,
        consumer_filter_manager: Arc<ConsumerFilterManager>,
    ) -> Self {
        let bloom_data_valid = match consumer_filter_data {
            None => false,
            Some(ref filter) => match consumer_filter_manager.bloom_filter() {
                None => false,
                Some(bloom_filter) => bloom_filter.is_valid(filter.bloom_filter_data()),
            },
        };

        ExpressionMessageFilter {
            subscription_data,
            consumer_filter_data,
            consumer_filter_manager,
            bloom_data_valid,
        }
    }
}

#[allow(unused_variables)]
impl MessageFilter for ExpressionMessageFilter {
    fn is_matched_by_consume_queue(&self, tags_code: Option<i64>, cq_ext_unit: Option<&CqExtUnit>) -> bool {
        if self.subscription_data.is_none() {
            return true;
        }
        let subscription_data = self.subscription_data.as_ref().unwrap();
        if subscription_data.class_filter_mode {
            return true;
        }
        if ExpressionType::is_tag_type(Some(subscription_data.expression_type.as_str())) {
            if tags_code.is_none() {
                return true;
            }
            if subscription_data.sub_string.as_str() == SubscriptionData::SUB_ALL {
                return true;
            }
            subscription_data.code_set.contains(&(tags_code.unwrap() as i32))
        } else {
            let Some(filter_data) = self.consumer_filter_data.as_ref() else {
                return true;
            };

            if filter_data.expression().is_none()
                || filter_data.expression_type().is_none()
                || filter_data.compiled_expression().is_none()
                || filter_data.bloom_filter_data().is_none()
            {
                return true;
            }

            let Some(cq_ext_unit) = cq_ext_unit else {
                return true;
            };

            if !filter_data.is_msg_in_live(cq_ext_unit.msg_store_time() as u64) {
                return true;
            }

            let Some(filter_bit_map) = cq_ext_unit.filter_bit_map() else {
                return true;
            };

            if !self.bloom_data_valid
                || filter_bit_map.len() * u8::BITS as usize
                    != filter_data.bloom_filter_data().unwrap().bit_num() as usize
            {
                return true;
            }

            let Ok(bits_array) = rocketmq_filter::utils::bits_array::BitsArray::from_bytes(filter_bit_map) else {
                return true;
            };

            self.consumer_filter_manager
                .bloom_filter()
                .and_then(|bloom_filter| {
                    bloom_filter
                        .is_hit(filter_data.bloom_filter_data().unwrap(), &bits_array)
                        .ok()
                })
                .unwrap_or(true)
        }
    }

    fn is_matched_by_commit_log(
        &self,
        msg_buffer: Option<&[u8]>,
        properties: Option<&HashMap<CheetahString, CheetahString>>,
    ) -> bool {
        if self.subscription_data.is_none() {
            return true;
        }
        let subscription_data = self.subscription_data.as_ref().unwrap();
        if subscription_data.class_filter_mode {
            return true;
        }
        if ExpressionType::is_tag_type(Some(subscription_data.expression_type.as_str())) {
            return true;
        }
        if self.consumer_filter_data.is_none() {
            return true;
        }
        let real_filter_data = self.consumer_filter_data.as_ref().unwrap();
        if real_filter_data.expression().is_none()
            || real_filter_data.expression_type().is_none()
            || real_filter_data.compiled_expression().is_none()
        {
            return true;
        }

        let temp_properties = match (properties, msg_buffer) {
            (None, Some(bytes)) => {
                let mut bytes_ = Bytes::copy_from_slice(bytes);
                message_decoder::decode_properties(&mut bytes_)
            }
            _ => None,
        };
        let context = MessageEvaluationContext::new(&temp_properties);
        if let Some(filter) = real_filter_data.compiled_expression() {
            match filter.evaluate(&context) {
                Ok(rocketmq_filter::expression::Value::Boolean(b)) => b,
                Ok(_) => false, // Non-boolean values are treated as false
                Err(_) => false,
            }
        } else {
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;
    use rocketmq_store::filter::MessageFilter;

    use super::*;

    fn new_manager() -> ConsumerFilterManager {
        ConsumerFilterManager::new(
            Arc::new(BrokerConfig::default()),
            Arc::new(MessageStoreConfig::default()),
        )
    }

    fn sql_subscription(topic: &str, expression: &str) -> SubscriptionData {
        SubscriptionData {
            topic: CheetahString::from_slice(topic),
            sub_string: CheetahString::from_slice(expression),
            expression_type: CheetahString::from_static_str(ExpressionType::SQL92),
            sub_version: 11,
            ..Default::default()
        }
    }

    #[test]
    fn sql_consume_queue_path_checks_bloom_bitmap() {
        let manager = new_manager();
        let subscriptions = HashSet::from([sql_subscription("TopicTest", "color = 'blue'")]);
        manager.register("GroupTest", &subscriptions);

        let filter_data = manager
            .get_consumer_filter_data(
                &CheetahString::from_slice("TopicTest"),
                &CheetahString::from_slice("GroupTest"),
            )
            .unwrap();

        let mut bits =
            rocketmq_filter::utils::bits_array::BitsArray::create(manager.bloom_filter().unwrap().m() as usize);
        manager
            .bloom_filter()
            .unwrap()
            .hash_to(filter_data.bloom_filter_data().unwrap(), &mut bits)
            .unwrap();

        let cq_ext_unit = CqExtUnit::new(0, filter_data.born_time() as i64 + 1, Some(bits.bytes().to_vec()));
        let filter = ExpressionMessageFilter::new(
            Some(sql_subscription("TopicTest", "color = 'blue'")),
            Some(filter_data.clone()),
            Arc::new(manager.clone()),
        );

        assert!(filter.is_matched_by_consume_queue(None, Some(&cq_ext_unit)));

        let miss = CqExtUnit::new(0, filter_data.born_time() as i64 + 1, Some(vec![0; bits.byte_length()]));
        assert!(!filter.is_matched_by_consume_queue(None, Some(&miss)));
    }
}
