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
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::body::delete_subscription_group_list_request_body::DeleteSubscriptionGroupListRequestBody;
use rocketmq_protocol::protocol::body::delete_topic_list_request_body::DeleteTopicListRequestBody;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::RemotingSerializable;

#[test]
fn broker_batch_delete_wire_contract_uses_body_only_java_requests() {
    let topic_body = DeleteTopicListRequestBody {
        topic_list: vec![CheetahString::from_static_str("TopicA")],
    };
    let topic_request = RemotingCommand::create_remoting_command(RequestCode::DeleteTopicInBrokerList.to_i32())
        .set_body(topic_body.encode().expect("topic body should encode"));
    assert_eq!(topic_request.code(), 5002);
    assert!(topic_request.ext_fields().is_none());

    let group_body = DeleteSubscriptionGroupListRequestBody {
        group_name_list: vec![CheetahString::from_static_str("GroupA")],
        clean_offset: true,
    };
    let group_request = RemotingCommand::create_remoting_command(RequestCode::DeleteSubscriptionGroupList.to_i32())
        .set_body(group_body.encode().expect("group body should encode"));
    assert_eq!(group_request.code(), 5003);
    assert!(group_request.ext_fields().is_none());
}
