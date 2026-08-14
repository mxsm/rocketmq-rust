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

use rocketmq_admin_core::core::consumer::DeleteSubscriptionGroupsRequest;
use rocketmq_admin_core::core::topic::DeleteTopicsInBrokerRequest;

#[test]
fn batch_delete_requests_are_typed_and_reject_empty_resources() {
    let topics =
        DeleteTopicsInBrokerRequest::try_new("127.0.0.1:10911", vec!["TopicA".to_string(), "TopicB".to_string()])
            .expect("valid topic batch should build");
    assert_eq!(topics.topics.len(), 2);

    let groups = DeleteSubscriptionGroupsRequest::try_new(
        "127.0.0.1:10911",
        vec!["GroupA".to_string(), "GroupB".to_string()],
        true,
    )
    .expect("valid group batch should build");
    assert!(groups.clean_offset);
    assert_eq!(groups.group_names.len(), 2);

    assert!(DeleteTopicsInBrokerRequest::try_new("127.0.0.1:10911", Vec::new()).is_err());
    assert!(DeleteSubscriptionGroupsRequest::try_new("127.0.0.1:10911", Vec::new(), false).is_err());
}
