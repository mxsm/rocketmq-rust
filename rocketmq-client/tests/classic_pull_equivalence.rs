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

#![allow(deprecated)]
#![recursion_limit = "256"]

use std::time::Duration;

use rocketmq_client_rust::ClientRuntime;
use rocketmq_client_rust::ClientRuntimeConfig;
use rocketmq_client_rust::DefaultMQPullConsumer;
use rocketmq_client_rust::MQPullConsumerScheduleService;
use rocketmq_client_rust::MessageQueueListener;
use rocketmq_client_rust::MessageSelector;
use rocketmq_client_rust::PullOptions;
use rocketmq_client_rust::PullTaskCallback;
use rocketmq_client_rust::PullTaskContext;
use rocketmq_client_rust::PullTaskImpl;
use rocketmq_client_rust::RebalancePullImpl;
use rocketmq_client_rust::TelemetryHandle;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_runtime::RuntimeConfig;
use rocketmq_runtime::RuntimeOwner;

fn client_runtime(owner: &RuntimeOwner, scope: &'static str) -> std::sync::Arc<ClientRuntime> {
    ClientRuntime::try_new(
        owner.root_context().component(scope),
        ClientRuntimeConfig::default(),
        TelemetryHandle::noop(),
    )
    .expect("classic pull test runtime should be valid")
}

#[test]
fn typed_options_validate_java_pull_arguments() {
    let queue = MessageQueue::from_parts("TopicA", "broker-a", 1);
    let selector = MessageSelector::by_sql("region = 'east'");
    let options = PullOptions::new(queue.clone(), selector.clone(), 12, 32)
        .expect("valid Java pull arguments")
        .max_size_in_bytes(1024)
        .timeout(Duration::from_secs(4))
        .broker_suspend_timeout(Duration::from_secs(3))
        .block_if_not_found(true);

    assert_eq!(options.message_queue(), &queue);
    assert_eq!(options.selector(), &selector);
    assert_eq!(options.offset(), 12);
    assert_eq!(options.max_messages(), 32);
    assert_eq!(options.max_size_in_bytes_value(), 1024);
    assert_eq!(options.timeout_value(), Duration::from_secs(4));
    assert_eq!(options.broker_suspend_timeout_value(), Duration::from_secs(3));
    assert!(options.is_block_if_not_found());

    assert!(PullOptions::new(queue.clone(), MessageSelector::by_tag("*"), -1, 1).is_err());
    assert!(PullOptions::new(queue.clone(), MessageSelector::by_tag("*"), 0, 0).is_err());
    assert!(PullOptions::new(queue, MessageSelector::by_tag("*"), 0, 1)
        .expect("base options are valid")
        .max_size_in_bytes(0)
        .validate()
        .is_err());
}

#[test]
fn configured_facade_fails_closed_before_start_without_unsupported_error() {
    let owner = RuntimeOwner::new(RuntimeConfig::server_default("classic-pull-equivalence"))
        .expect("runtime owner should start");
    let runtime = client_runtime(&owner, "client");
    let consumer = DefaultMQPullConsumer::builder(runtime.clone())
        .consumer_group("ClassicPullGroup")
        .build()
        .expect("classic pull builder should produce a configured facade");
    assert!(
        !consumer
            .client_config()
            .expect("configured facade should expose its client config")
            .is_enable_stream_request_type(),
        "Classic Pull must not advertise the LitePull stream request type"
    );
    let queue = MessageQueue::from_parts("TopicA", "broker-a", 0);
    let options = PullOptions::new(queue, MessageSelector::by_tag("TagA || TagB"), 0, 16).expect("valid pull options");

    owner.block_on(async {
        let pull_error = match consumer.pull_with_options(options).await {
            Ok(_) => panic!("pull before start must fail closed"),
            Err(error) => error,
        };
        assert!(pull_error.to_string().contains("not started"));
        assert!(!pull_error.to_string().contains("not supported"));

        let queue_error = consumer
            .fetch_subscribe_message_queues("TopicA")
            .await
            .expect_err("queue lookup before start must fail closed");
        assert!(queue_error.to_string().contains("not started"));
        assert!(!queue_error.to_string().contains("not supported"));

        let report = runtime.shutdown().await;
        assert!(report.is_healthy(), "{}", report.to_json());
    });
    owner
        .shutdown_runtime_blocking()
        .expect("runtime owner should stop cleanly");
}

#[test]
fn detached_legacy_constructor_reports_missing_runtime_instead_of_unsupported() {
    let owner =
        RuntimeOwner::new(RuntimeConfig::server_default("classic-pull-detached")).expect("runtime owner should start");
    let consumer = DefaultMQPullConsumer::with_consumer_group("ClassicPullGroup");

    owner.block_on(async {
        let error = consumer.start().await.expect_err("detached facade has no runtime");
        assert!(error.to_string().contains("builder"));
        assert!(!error.to_string().contains("not supported"));
    });
    owner
        .shutdown_runtime_blocking()
        .expect("runtime owner should stop cleanly");
}

#[test]
fn schedule_compatibility_types_are_live_or_fail_closed() {
    struct Callback;
    impl PullTaskCallback for Callback {}

    let service = MQPullConsumerScheduleService::new("ClassicPullGroup");
    let register_error = service
        .register_pull_task_callback("TopicA", Callback)
        .expect_err("detached schedule service must require a runtime");
    assert!(register_error.to_string().contains("with_client_runtime"));
    assert!(!register_error.to_string().contains("not supported"));

    let mut context = PullTaskContext::new();
    assert_eq!(context.get_pull_next_delay_time_millis(), 200);
    context.set_pull_next_delay_time_millis(25);
    context.set_pull_consumer(DefaultMQPullConsumer::with_consumer_group("ClassicPullGroup"));
    assert_eq!(context.get_pull_next_delay_time_millis(), 25);
    assert_eq!(
        context
            .get_pull_consumer()
            .expect("context consumer should be retained")
            .consumer_group()
            .map(|group| group.as_str()),
        Some("ClassicPullGroup")
    );

    let task = PullTaskImpl::new(MessageQueue::from_parts("TopicA", "broker-a", 0));
    task.run().expect("configured compatibility task should be runnable");
    RebalancePullImpl::new().expect("rebalance compatibility marker should be constructible");
}

#[test]
fn configured_facade_owns_start_and_shutdown_lifecycle() {
    let owner =
        RuntimeOwner::new(RuntimeConfig::server_default("classic-pull-lifecycle")).expect("runtime owner should start");
    let runtime = client_runtime(&owner, "client");
    let consumer = DefaultMQPullConsumer::builder(runtime.clone())
        .consumer_group("ClassicPullLifecycleGroup")
        .queue_refresh_interval(Duration::from_millis(10))
        .build()
        .expect("classic pull facade should build");

    owner.block_on(async {
        consumer.start().await.expect("first start should succeed");
        assert!(consumer.is_running().await);
        let repeated_start = consumer
            .start()
            .await
            .expect_err("second start must fail deterministically");
        assert!(repeated_start.to_string().contains("already started"));
        consumer
            .shutdown()
            .await
            .expect("first shutdown should join owned tasks");
        assert!(!consumer.is_running().await);
        consumer
            .shutdown()
            .await
            .expect("repeated shutdown should be idempotent");

        let report = runtime.shutdown().await;
        assert!(report.is_healthy(), "{}", report.to_json());
    });
    owner
        .shutdown_runtime_blocking()
        .expect("runtime owner should stop cleanly");
}

#[test]
fn queue_listener_can_be_registered_before_start() {
    struct Listener;
    impl MessageQueueListener for Listener {
        fn message_queue_changed(
            &self,
            _topic: &str,
            _mq_all: &std::collections::HashSet<MessageQueue>,
            _mq_divided: &std::collections::HashSet<MessageQueue>,
        ) {
        }
    }

    let owner = RuntimeOwner::new(RuntimeConfig::server_default("classic-pull-listener-lifecycle"))
        .expect("runtime owner should start");
    let runtime = client_runtime(&owner, "client");
    let consumer = DefaultMQPullConsumer::builder(runtime.clone())
        .consumer_group("ClassicPullListenerGroup")
        .build()
        .expect("classic pull facade should build");

    owner.block_on(async {
        consumer
            .register_message_queue_listener("TopicA", Listener)
            .await
            .expect("listener should register before start");
        consumer
            .start()
            .await
            .expect("pre-start listener registration should be retained");
        consumer.shutdown().await.expect("consumer should stop cleanly");
        let report = runtime.shutdown().await;
        assert!(report.is_healthy(), "{}", report.to_json());
    });
    owner
        .shutdown_runtime_blocking()
        .expect("runtime owner should stop cleanly");
}

#[test]
fn configured_schedule_service_cancels_and_joins_its_coordinator() {
    struct Callback;
    impl PullTaskCallback for Callback {}

    let owner = RuntimeOwner::new(RuntimeConfig::server_default("classic-pull-schedule-lifecycle"))
        .expect("runtime owner should start");
    let runtime = client_runtime(&owner, "client");
    let service = MQPullConsumerScheduleService::with_client_runtime(runtime.clone(), "ClassicPullScheduleGroup")
        .expect("schedule service should build");
    service
        .set_refresh_interval(Duration::from_millis(10))
        .expect("test refresh interval should be valid");
    service
        .register_pull_task_callback("TopicA", Callback)
        .expect("callback should register before start");

    owner.block_on(async {
        service.start().await.expect("schedule service should start");
        let repeated_start = service
            .start()
            .await
            .expect_err("second start must fail deterministically");
        assert!(repeated_start.to_string().contains("already started"));
        service
            .shutdown()
            .await
            .expect("schedule coordinator should stop cleanly");
        service
            .shutdown()
            .await
            .expect("repeated shutdown should be idempotent");

        let report = runtime.shutdown().await;
        assert!(report.is_healthy(), "{}", report.to_json());
    });
    owner
        .shutdown_runtime_blocking()
        .expect("runtime owner should stop cleanly");
}
