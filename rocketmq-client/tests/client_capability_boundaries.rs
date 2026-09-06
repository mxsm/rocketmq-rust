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

#![recursion_limit = "256"]

const CLIENT_FACADE: &str = include_str!("../src/implementation/mq_client_api_impl.rs");
const CLIENT_ADMIN: &str = include_str!("../src/implementation/mq_client_api_impl/admin.rs");
const CLIENT_VERSIONED_CONFIG: &str =
    include_str!("../src/implementation/mq_client_api_impl/admin/versioned_config.rs");
const CLIENT_SUPERVISED_MUTATION: &str =
    include_str!("../src/implementation/mq_client_api_impl/admin/supervised_mutation.rs");
const CLIENT_SUPERVISED_MUTATION_DECODE: &str =
    include_str!("../src/implementation/mq_client_api_impl/admin/supervised_mutation_decode.rs");
const CLIENT_CONSUMER: &str = include_str!("../src/implementation/mq_client_api_impl/consumer.rs");
const CLIENT_PRODUCER: &str = include_str!("../src/implementation/mq_client_api_impl/producer.rs");
const CLIENT_REQUEST_BUILDER: &str = include_str!("../src/implementation/mq_client_api_impl/request_builder.rs");
const CLIENT_RESPONSE_DECODER: &str = include_str!("../src/implementation/mq_client_api_impl/response_decoder.rs");
const CLIENT_ROUTE: &str = include_str!("../src/implementation/mq_client_api_impl/route.rs");
const CLIENT_TRANSACTION: &str = include_str!("../src/implementation/mq_client_api_impl/transaction.rs");
const CLIENT_TRANSPORT: &str = include_str!("../src/implementation/mq_client_api_impl/transport.rs");

const ADMIN_FACADE: &str = include_str!("../src/admin/default_mq_admin_ext_impl.rs");
const ADMIN_CAPABILITIES: &str = include_str!("../src/admin/capability.rs");
const ADMIN_API: &str = include_str!("../src/admin/default_mq_admin_ext_impl/admin_api.rs");
const ADMIN_BROKER: &str = include_str!("../src/admin/default_mq_admin_ext_impl/broker.rs");
const ADMIN_GROUP: &str = include_str!("../src/admin/default_mq_admin_ext_impl/group.rs");
const ADMIN_LIFECYCLE: &str = include_str!("../src/admin/default_mq_admin_ext_impl/lifecycle.rs");
const ADMIN_SECURITY: &str = include_str!("../src/admin/default_mq_admin_ext_impl/security.rs");
const ADMIN_TOPIC: &str = include_str!("../src/admin/default_mq_admin_ext_impl/topic.rs");

const PRODUCER_FACADE: &str = include_str!("../src/producer/producer_impl/default_mq_producer_impl.rs");
const PRODUCER_CAPABILITIES: &str = include_str!("../src/producer/capability.rs");
const PRODUCER_BACKEND: &str = include_str!("../src/producer/producer_backend.rs");
const PRODUCER_HOOKS: &str = include_str!("../src/producer/producer_impl/default_mq_producer_impl/hooks.rs");
const PRODUCER_LIFECYCLE: &str = include_str!("../src/producer/producer_impl/default_mq_producer_impl/lifecycle.rs");
const PRODUCER_RETRY: &str = include_str!("../src/producer/producer_impl/default_mq_producer_impl/retry.rs");
const PRODUCER_SEND: &str = include_str!("../src/producer/producer_impl/default_mq_producer_impl/send.rs");
const PRODUCER_TRANSACTION: &str =
    include_str!("../src/producer/producer_impl/default_mq_producer_impl/transaction.rs");
const LITE_PULL_CAPABILITIES: &str = include_str!("../src/consumer/lite_pull_consumer.rs");

#[test]
fn client_facades_declare_explicit_capability_modules() {
    for module in [
        "admin",
        "consumer",
        "producer",
        "request_builder",
        "response_decoder",
        "route",
        "transaction",
        "transport",
    ] {
        assert!(CLIENT_FACADE.contains(&format!("mod {module};")));
    }
    for module in ["admin_api", "broker", "group", "lifecycle", "security", "topic"] {
        assert!(ADMIN_FACADE.contains(&format!("mod {module};")));
    }
    for module in ["hooks", "lifecycle", "retry", "send", "transaction"] {
        assert!(PRODUCER_FACADE.contains(&format!("mod {module};")));
    }

    assert!(CLIENT_FACADE.lines().count() <= 450);
    assert!(ADMIN_FACADE.lines().count() <= 300);
    assert!(PRODUCER_FACADE.lines().count() <= 450);
    assert!(CLIENT_ADMIN.contains("mod versioned_config;"));
    let admin_lines = CLIENT_ADMIN.lines().collect::<Vec<_>>();
    for module in ["supervised_mutation", "supervised_mutation_decode"] {
        let declaration = format!("mod {module};");
        let declaration_index = admin_lines
            .iter()
            .position(|line| line.trim() == declaration)
            .unwrap_or_else(|| panic!("missing {declaration}"));
        assert_eq!(
            admin_lines[declaration_index - 1].trim(),
            r#"#[cfg(feature = "admin-mutation")]"#,
            "{declaration} must be directly gated by admin-mutation"
        );
    }
    assert!(!CLIENT_FACADE.contains("pub async fn send_message"));
    assert!(!PRODUCER_FACADE.contains("pub async fn send_with_timeout"));
}

#[test]
fn client_god_traits_cannot_reappear() {
    for capability in [
        "RouteAdmin",
        "TopicAdmin",
        "ConsumerAdmin",
        "BrokerAdmin",
        "AuthAdmin",
        "OffsetAdmin",
    ] {
        assert!(ADMIN_CAPABILITIES.contains(&format!("pub trait {capability}")));
    }
    for capability in [
        "SubscriptionControl",
        "AssignmentControl",
        "MessagePoll",
        "ConsumerOffsetControl",
        "ConsumerLifecycle",
    ] {
        assert!(LITE_PULL_CAPABILITIES.contains(&format!("pub trait {capability}")));
    }
    for capability in [
        "MessageSend",
        "TransactionSend",
        "RequestReply",
        "MessageRecall",
        "MessageQuery",
        "ProducerLifecycle",
    ] {
        assert!(PRODUCER_CAPABILITIES.contains(&format!("pub trait {capability}")));
    }

    for retired in ["MQAdminExt", "LitePullConsumerLocal", "MQProducer"] {
        for source in [
            ADMIN_CAPABILITIES,
            LITE_PULL_CAPABILITIES,
            PRODUCER_CAPABILITIES,
            PRODUCER_BACKEND,
        ] {
            let contains_identifier = source
                .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
                .any(|identifier| identifier == retired);
            assert!(!contains_identifier, "retired client trait {retired} reappeared");
        }
    }
}

#[test]
fn mq_client_exposes_five_typed_capability_views() {
    for (source, capability, getter) in [
        (CLIENT_ROUTE, "RouteClient", "route_client"),
        (CLIENT_ADMIN, "AdminClient", "admin_client"),
        (CLIENT_PRODUCER, "ProducerClient", "producer_client"),
        (CLIENT_CONSUMER, "ConsumerClient", "consumer_client"),
        (CLIENT_TRANSACTION, "TransactionClient", "transaction_client"),
    ] {
        assert!(source.contains(&format!("pub struct {capability}<'a>")));
        assert!(source.contains(&format!("pub fn {getter}(&self)")));
    }

    assert!(CLIENT_ROUTE.contains("topic_route_info"));
    assert!(CLIENT_ADMIN.contains("broker_cluster_info"));
    assert!(CLIENT_PRODUCER.contains("send_heartbeat"));
    assert!(CLIENT_CONSUMER.contains("consumer_offset"));
    assert!(CLIENT_TRANSACTION.contains("end_transaction"));
    assert!(CLIENT_REQUEST_BUILDER.contains("heartbeat_request"));
    assert!(CLIENT_REQUEST_BUILDER.contains("notification_request"));
    assert!(CLIENT_RESPONSE_DECODER.contains("consumer_offset_json_from_response"));
    assert!(CLIENT_RESPONSE_DECODER.contains("reset_offset_table_from_response"));
}

#[test]
fn protocol_and_retry_responsibilities_remain_in_their_own_modules() {
    assert!(CLIENT_TRANSPORT.contains("pub async fn invoke("));
    assert!(CLIENT_TRANSPORT.contains("pub async fn invoke_oneway("));
    assert!(CLIENT_ROUTE.contains("RequestCode::GetRouteinfoByTopic"));
    assert!(CLIENT_PRODUCER.contains("RequestCode::SendMessage"));
    assert!(CLIENT_CONSUMER.contains("RequestCode::PullMessage"));
    assert!(CLIENT_TRANSACTION.contains("RequestCode::EndTransaction"));

    assert!(PRODUCER_SEND.contains("send_kernel_impl"));
    assert!(PRODUCER_RETRY.contains("send_with_retry"));
    assert!(PRODUCER_RETRY.contains("RetryPolicy::decide"));
    assert!(PRODUCER_TRANSACTION.contains("send_message_in_transaction"));
    assert!(PRODUCER_LIFECYCLE.contains("shutdown_with_factory"));
    for method in [
        "register_end_transaction_hook",
        "register_check_forbidden_hook",
        "register_send_message_hook",
        "set_rpc_hook",
    ] {
        assert!(PRODUCER_HOOKS.contains(&format!("pub fn {method}(")));
    }
}

#[test]
fn capability_files_stay_within_the_reviewed_split_limits() {
    for (name, source, limit) in [
        ("client/admin.rs", CLIENT_ADMIN, 3_650),
        ("client/admin/versioned_config.rs", CLIENT_VERSIONED_CONFIG, 700),
        ("client/admin/supervised_mutation.rs", CLIENT_SUPERVISED_MUTATION, 450),
        (
            "client/admin/supervised_mutation_decode.rs",
            CLIENT_SUPERVISED_MUTATION_DECODE,
            450,
        ),
        ("client/consumer.rs", CLIENT_CONSUMER, 1_650),
        ("client/producer.rs", CLIENT_PRODUCER, 950),
        ("client/request_builder.rs", CLIENT_REQUEST_BUILDER, 150),
        ("client/response_decoder.rs", CLIENT_RESPONSE_DECODER, 100),
        ("client/route.rs", CLIENT_ROUTE, 150),
        ("client/transaction.rs", CLIENT_TRANSACTION, 100),
        ("client/transport.rs", CLIENT_TRANSPORT, 200),
        ("admin/admin_api.rs", ADMIN_API, 2_750),
        ("admin/broker.rs", ADMIN_BROKER, 250),
        ("admin/group.rs", ADMIN_GROUP, 550),
        ("admin/lifecycle.rs", ADMIN_LIFECYCLE, 200),
        ("admin/security.rs", ADMIN_SECURITY, 200),
        ("admin/topic.rs", ADMIN_TOPIC, 450),
        ("producer/lifecycle.rs", PRODUCER_LIFECYCLE, 1_250),
        ("producer/hooks.rs", PRODUCER_HOOKS, 100),
        ("producer/retry.rs", PRODUCER_RETRY, 250),
        ("producer/send.rs", PRODUCER_SEND, 2_050),
        ("producer/transaction.rs", PRODUCER_TRANSACTION, 450),
    ] {
        assert!(
            source.lines().count() <= limit,
            "{name} exceeded its reviewed split limit of {limit} lines"
        );
    }
}

#[test]
fn capability_split_does_not_introduce_detached_runtime_work() {
    let production_sources = [
        CLIENT_ADMIN,
        CLIENT_VERSIONED_CONFIG,
        CLIENT_SUPERVISED_MUTATION,
        CLIENT_SUPERVISED_MUTATION_DECODE,
        CLIENT_CONSUMER,
        CLIENT_PRODUCER,
        CLIENT_REQUEST_BUILDER,
        CLIENT_RESPONSE_DECODER,
        CLIENT_ROUTE,
        CLIENT_TRANSACTION,
        CLIENT_TRANSPORT,
        ADMIN_API,
        ADMIN_BROKER,
        ADMIN_GROUP,
        ADMIN_LIFECYCLE,
        ADMIN_SECURITY,
        ADMIN_TOPIC,
        PRODUCER_LIFECYCLE,
        PRODUCER_HOOKS,
        PRODUCER_RETRY,
        PRODUCER_SEND,
        PRODUCER_TRANSACTION,
    ];

    for source in production_sources {
        assert!(!source.contains("tokio::spawn("));
        assert!(!source.contains("tokio::task::spawn_blocking("));
        assert!(!source.contains("std::thread::spawn("));
        assert!(!source.contains("Runtime::new("));
    }
}
