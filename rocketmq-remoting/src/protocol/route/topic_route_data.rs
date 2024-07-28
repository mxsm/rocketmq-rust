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

use serde::Deserialize;
use serde::Serialize;

use crate::protocol::route::route_data_view::BrokerData;
use crate::protocol::route::route_data_view::QueueData;
use crate::protocol::static_topic::topic_queue_info::TopicQueueMappingInfo;

#[derive(Debug, Serialize, Deserialize, Clone, Default, Eq, PartialEq)]
pub struct TopicRouteData {
    #[serde(rename = "orderTopicConf")]
    pub order_topic_conf: Option<String>,
    #[serde(rename = "queueDatas")]
    pub queue_datas: Vec<QueueData>,
    #[serde(rename = "brokerDatas")]
    pub broker_datas: Vec<BrokerData>,
    #[serde(rename = "filterServerTable")]
    pub filter_server_table: HashMap<String, Vec<String>>,
    #[serde(rename = "TopicQueueMappingInfo")]
    pub topic_queue_mapping_by_broker: Option<HashMap<String, TopicQueueMappingInfo>>,
}

impl TopicRouteData {
    pub fn topic_route_data_changed(&self, old_data: Option<&TopicRouteData>) -> bool {
        if old_data.is_none() {
            return true;
        }
        /*let mut now = TopicRouteData::from_existing(self);
        let mut old = TopicRouteData::from_existing(old_data.unwrap());*/
        /*now.queue_datas.sort_by(|a, b| a.cmp(&b));
        now.broker_datas.sort_by(|a, b| a.cmp(&b));
        old.queue_datas.sort_by(|a, b| a.cmp(&b));
        old.broker_datas.sort_by(|a, b| a.cmp(&b));*/
        //now != old
        false
    }

    pub fn new() -> Self {
        TopicRouteData {
            order_topic_conf: None,
            queue_datas: Vec::new(),
            broker_datas: Vec::new(),
            filter_server_table: HashMap::new(),
            topic_queue_mapping_by_broker: None,
        }
    }

    fn from_existing(topic_route_data: &TopicRouteData) -> Self {
        TopicRouteData {
            order_topic_conf: topic_route_data.order_topic_conf.clone(),
            queue_datas: topic_route_data.queue_datas.clone(),
            broker_datas: topic_route_data.broker_datas.clone(),
            filter_server_table: topic_route_data.filter_server_table.clone(),
            topic_queue_mapping_by_broker: topic_route_data.topic_queue_mapping_by_broker.clone(),
        }
    }
}
