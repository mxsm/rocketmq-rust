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

#![recursion_limit = "256"]

use std::collections::HashSet;

use cheetah_string::CheetahString;
use rocketmq_client_rust::ConsumerAdmin;
use rocketmq_client_rust::TopicAdmin;

fn topic_batch_api_is_typed<T: TopicAdmin>(admin: &T) {
    let future = admin.delete_topic_in_broker_list(
        HashSet::from([CheetahString::from_static_str("127.0.0.1:10911")]),
        vec![CheetahString::from_static_str("TopicA")],
    );
    drop(future);
}

fn group_batch_api_is_typed<T: ConsumerAdmin>(admin: &T) {
    let future = admin.delete_subscription_group_list(
        CheetahString::from_static_str("127.0.0.1:10911"),
        vec![CheetahString::from_static_str("GroupA")],
        true,
    );
    drop(future);
}

#[test]
fn batch_delete_capabilities_are_public_and_typed() {
    fn assert_contract<T: TopicAdmin + ConsumerAdmin>() {
        let _ = topic_batch_api_is_typed::<T>;
        let _ = group_batch_api_is_typed::<T>;
    }

    let _ = assert_contract::<rocketmq_client_rust::DefaultMQAdminExt>;
    let _ = assert_contract::<rocketmq_client_rust::DefaultMQAdminExtImpl>;
}
