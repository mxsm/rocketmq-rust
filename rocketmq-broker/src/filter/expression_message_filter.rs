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

use cheetah_string::CheetahString;
use rocketmq_common::common::filter::expression_type::ExpressionType;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_store::consume_queue::consume_queue_ext::CqExtUnit;
use rocketmq_store::filter::MessageFilter;

use crate::filter::consumer_filter_data::ConsumerFilterData;
use crate::filter::manager::consumer_filter_manager::ConsumerFilterManager;

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
            Some(ref filter) => match consumer_filter_manager.get_bloom_filter() {
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
    fn is_matched_by_consume_queue(
        &self,
        tags_code: Option<i64>,
        cq_ext_unit: Option<&CqExtUnit>,
    ) -> bool {
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
            subscription_data
                .code_set
                .contains(&(tags_code.unwrap() as i32))
        } else {
            unimplemented!("SQL92 expression type is not supported yet.")
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
        if real_filter_data.expression().is_none() || real_filter_data.expression_type().is_none() {
            return true;
        }
        unimplemented!("SQL92 expression type is not supported yet.")
    }
}
