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

#[test]
fn java_batch_delete_request_codes_round_trip() {
    assert_eq!(5002, RequestCode::DeleteTopicInBrokerList.to_i32());
    assert_eq!(5003, RequestCode::DeleteSubscriptionGroupList.to_i32());
    assert_eq!(RequestCode::DeleteTopicInBrokerList, RequestCode::from(5002));
    assert_eq!(RequestCode::DeleteSubscriptionGroupList, RequestCode::from(5003));
}

#[test]
fn topic_list_body_uses_java_wire_field() {
    let body = DeleteTopicListRequestBody {
        topic_list: vec![
            CheetahString::from_static_str("TopicA"),
            CheetahString::from_static_str("TopicB"),
        ],
    };

    let encoded = serde_json::to_string(&body).expect("encode topic list");
    assert_eq!(r#"{"topicList":["TopicA","TopicB"]}"#, encoded);
    assert_eq!(body, serde_json::from_str(&encoded).expect("decode topic list"));
}

#[test]
fn subscription_group_body_defaults_clean_offset_to_false() {
    let body: DeleteSubscriptionGroupListRequestBody =
        serde_json::from_str(r#"{"groupNameList":["GroupA","GroupB"]}"#).expect("decode group list");

    assert_eq!(
        vec![
            CheetahString::from_static_str("GroupA"),
            CheetahString::from_static_str("GroupB")
        ],
        body.group_name_list
    );
    assert!(!body.clean_offset);
    assert_eq!(
        r#"{"groupNameList":["GroupA","GroupB"],"cleanOffset":false}"#,
        serde_json::to_string(&body).expect("encode group list")
    );
}
