// Copyright 2026 The RocketMQ Rust Authors
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

use cheetah_string::CheetahString;
use rocketmq_admin_core::client_adapter::services::topic::TopicTarget;
use rocketmq_admin_core::client_adapter::services::topic::UpdateTopicListRequest;
use rocketmq_model::common::config::TopicConfig as RocketMQTopicConfig;

#[test]
fn update_topic_list_request_accepts_broker_target_and_topic_configs() {
    let request = UpdateTopicListRequest::try_new(
        TopicTarget::Broker(CheetahString::from_static_str("127.0.0.1:10911")),
        vec![RocketMQTopicConfig::default()],
    )
    .unwrap()
    .with_optional_namesrv_addr(Some(" 127.0.0.1:9876 ".to_string()));

    assert_eq!(
        request.target(),
        &TopicTarget::Broker(CheetahString::from_static_str("127.0.0.1:10911"))
    );
    assert_eq!(request.topic_configs().len(), 1);
    assert_eq!(request.namesrv_addr(), Some("127.0.0.1:9876"));
}

#[test]
fn update_topic_list_request_rejects_empty_topic_config_list() {
    let error = UpdateTopicListRequest::try_new(
        TopicTarget::Cluster(CheetahString::from_static_str("DefaultCluster")),
        Vec::new(),
    )
    .unwrap_err();

    assert!(error.to_string().contains("topicConfigs must not be empty"));
}
