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

mod support;

use rocketmq_client_rust::AssignmentControl;
use rocketmq_client_rust::ClientSessionProvider;
use rocketmq_client_rust::ConsumerLifecycle;
use rocketmq_client_rust::ConsumerOffsetControl;
use rocketmq_client_rust::DefaultLitePullConsumer;
use rocketmq_client_rust::MessagePoll;
use rocketmq_client_rust::SubscriptionControl;

fn assert_lite_pull_capabilities<T>()
where
    T: SubscriptionControl + AssignmentControl + MessagePoll + ConsumerOffsetControl + ConsumerLifecycle,
{
}

#[test]
fn lite_pull_handle_implements_scoped_capabilities() {
    assert_lite_pull_capabilities::<DefaultLitePullConsumer>();
}

#[test]
fn cloned_lite_pull_handles_share_one_session() {
    let consumer = DefaultLitePullConsumer::builder(support::client_runtime("lite-pull-capability-session"))
        .consumer_group("lite-pull-capability-session")
        .build()
        .expect("consumer should build");
    let clone = consumer.clone();

    assert!(consumer
        .client_session()
        .expect("public consumer should have a session")
        .shares_runtime_with(
            clone
                .client_session()
                .expect("cloned public consumer should have a session"),
        ));
}
