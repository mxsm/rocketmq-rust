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

use rocketmq_macros::RequestHeaderCodecV3;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, Deserialize, Default, RequestHeaderCodecV3)]
#[serde(rename_all = "camelCase")]
#[header(
    type_id = "rocketmq_protocol::protocol::header::get_all_topic_config_response_header::GetAllTopicConfigResponseHeader",
    java_class = "org.apache.rocketmq.remoting.protocol.header.GetAllTopicConfigResponseHeader"
)]
pub struct GetAllTopicConfigResponseHeader {
    pub total_topic_num: Option<i32>,
}
