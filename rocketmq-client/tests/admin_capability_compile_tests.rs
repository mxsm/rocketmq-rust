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

mod support;

use rocketmq_client_rust::AuthAdmin;
use rocketmq_client_rust::BrokerAdmin;
use rocketmq_client_rust::ClientSessionProvider;
use rocketmq_client_rust::ConsumerAdmin;
use rocketmq_client_rust::DefaultLitePullConsumer;
use rocketmq_client_rust::DefaultMQAdminExt;
use rocketmq_client_rust::DefaultMQAdminExtImpl;
use rocketmq_client_rust::DefaultMQProducer;
use rocketmq_client_rust::OffsetAdmin;
use rocketmq_client_rust::RouteAdmin;
use rocketmq_client_rust::TopicAdmin;

fn assert_admin_capabilities<T>()
where
    T: RouteAdmin + TopicAdmin + ConsumerAdmin + BrokerAdmin + AuthAdmin + OffsetAdmin,
{
}

#[test]
fn concrete_admin_handles_implement_scoped_capabilities() {
    assert_admin_capabilities::<DefaultMQAdminExt>();
    assert_admin_capabilities::<DefaultMQAdminExtImpl>();
}

#[test]
fn admin_handle_borrows_the_application_session() {
    let runtime = support::client_runtime("admin-capability-session");
    let admin = DefaultMQAdminExt::new(runtime.clone());
    let consumer = DefaultLitePullConsumer::builder(runtime.clone())
        .consumer_group("admin-capability-session")
        .build()
        .expect("consumer should build");
    let producer = DefaultMQProducer::builder(runtime.clone())
        .producer_group("admin-capability-session")
        .build();

    assert!(std::sync::Arc::ptr_eq(&runtime, &admin.client_runtime()));
    let session = admin.client_session().expect("public admin should have a session");
    assert!(session.shares_runtime_with(
        consumer
            .client_session()
            .expect("public consumer should have a session"),
    ));
    assert!(session.shares_runtime_with(
        producer
            .client_session()
            .expect("public producer should have a session"),
    ));
}
