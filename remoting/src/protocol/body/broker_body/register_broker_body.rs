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

use bytes::Bytes;
use rocketmq_common::common::mq_version::RocketMqVersion;
use serde::{Deserialize, Serialize};

use crate::protocol::{
    body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper,
    RemotingSerializable,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[derive(Default)]
pub struct RegisterBrokerBody {
    #[serde(rename = "topicConfigSerializeWrapper")]
    topic_config_serialize_wrapper: TopicConfigAndMappingSerializeWrapper,
    #[serde(rename = "filterServerList")]
    filter_server_list: Vec<String>,
}

impl RegisterBrokerBody {
    pub fn new(
        topic_config_serialize_wrapper: TopicConfigAndMappingSerializeWrapper,
        filter_server_list: Vec<String>,
    ) -> Self {
        RegisterBrokerBody {
            topic_config_serialize_wrapper,
            filter_server_list,
        }
    }

    pub fn topic_config_serialize_wrapper(&self) -> &TopicConfigAndMappingSerializeWrapper {
        &self.topic_config_serialize_wrapper
    }
    pub fn filter_server_list(&self) -> &Vec<String> {
        &self.filter_server_list
    }
}


impl RegisterBrokerBody {
    pub fn decode(
        bytes: &Bytes,
        compressed: bool,
        _broker_version: RocketMqVersion,
    ) -> RegisterBrokerBody {
        if !compressed {
            return <RegisterBrokerBody as RemotingSerializable>::decode(bytes.iter().as_slice());
        }
        todo!()
    }
}

impl RemotingSerializable for RegisterBrokerBody {
    type Output = RegisterBrokerBody;

    fn decode(bytes: &[u8]) -> Self::Output {
        serde_json::from_slice::<Self::Output>(bytes).unwrap()
    }

    fn encode(&self, _compress: bool) -> Vec<u8> {
        todo!()
    }
}
