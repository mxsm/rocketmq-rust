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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::future;
use std::net::TcpListener;
use std::ops::RangeInclusive;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Condvar;
use std::time::Duration;

use crate::controller::replicas_manager::RegisterState;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;
use rocketmq_controller::BrokerHeartbeatManager;
use rocketmq_controller::ConsensusNode;
use rocketmq_controller::Controller;
use rocketmq_controller::ControllerConfig as TestControllerConfig;
use rocketmq_controller::ControllerManager as TestControllerManager;
use rocketmq_controller::MembershipChange;
use rocketmq_controller::MembershipChangeRequest;
use rocketmq_controller::Node;
use rocketmq_controller::RaftPeer;
use rocketmq_controller::StorageBackendType;
use rocketmq_model::common::attribute::subscription_group_attributes::LITE_BIND_TOPIC_ATTRIBUTE_NAME;
use rocketmq_model::common::attribute::topic_attributes;
use rocketmq_model::common::attribute::Attribute;
use rocketmq_model::common::boundary_type::BoundaryType;
use rocketmq_model::common::config::TopicConfig;
use rocketmq_model::common::constant::file_readahead_mode::READ_AHEAD_MODE;
use rocketmq_model::common::constant::PermName;
use rocketmq_model::common::entity::ClientGroup;
use rocketmq_model::common::lite::to_lmq_name;
use rocketmq_model::common::message::message_batch::MessageExtBatch;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_model::common::mix_all;
use rocketmq_model::common::mix_all::MASTER_ID;
use rocketmq_model::common::topic::TopicValidator;
use rocketmq_model::utils::crc32_utils::crc32;
use rocketmq_namesrv::bootstrap::Builder as NameServerBuilder;
use rocketmq_namesrv::NamesrvConfig;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_protocol::protocol::admin::consume_stats::ConsumeStats;
use rocketmq_protocol::protocol::admin::topic_stats_table::TopicStatsTable;
use rocketmq_protocol::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
use rocketmq_protocol::protocol::body::broker_body::broker_member_group::GetBrokerMemberGroupResponseBody;
use rocketmq_protocol::protocol::body::get_broker_lite_info_response_body::GetBrokerLiteInfoResponseBody;
use rocketmq_protocol::protocol::body::get_lite_client_info_response_body::GetLiteClientInfoResponseBody;
use rocketmq_protocol::protocol::body::get_lite_group_info_response_body::GetLiteGroupInfoResponseBody;
use rocketmq_protocol::protocol::body::get_lite_topic_info_response_body::GetLiteTopicInfoResponseBody;
use rocketmq_protocol::protocol::body::get_parent_topic_info_response_body::GetParentTopicInfoResponseBody;
use rocketmq_protocol::protocol::body::kv_table::KVTable;
use rocketmq_protocol::protocol::body::query_consume_queue_response_body::QueryConsumeQueueResponseBody;
use rocketmq_protocol::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use rocketmq_protocol::protocol::body::user_info::UserInfo;
use rocketmq_protocol::protocol::header::add_broker_request_header::AddBrokerRequestHeader;
use rocketmq_protocol::protocol::header::consumer_send_msg_back_request_header::ConsumerSendMsgBackRequestHeader;
use rocketmq_protocol::protocol::header::controller::apply_broker_id_request_header::ApplyBrokerIdRequestHeader;
use rocketmq_protocol::protocol::header::create_user_request_header::CreateUserRequestHeader;
use rocketmq_protocol::protocol::header::delete_subscription_group_request_header::DeleteSubscriptionGroupRequestHeader;
use rocketmq_protocol::protocol::header::delete_user_request_header::DeleteUserRequestHeader;
use rocketmq_protocol::protocol::header::empty_header::EmptyHeader;
use rocketmq_protocol::protocol::header::get_consume_stats_request_header::GetConsumeStatsRequestHeader;
use rocketmq_protocol::protocol::header::get_consumer_connection_list_request_header::GetConsumerConnectionListRequestHeader;
use rocketmq_protocol::protocol::header::get_earliest_msg_storetime_request_header::GetEarliestMsgStoretimeRequestHeader;
use rocketmq_protocol::protocol::header::get_earliest_msg_storetime_response_header::GetEarliestMsgStoretimeResponseHeader;
use rocketmq_protocol::protocol::header::get_lite_client_info_request_header::GetLiteClientInfoRequestHeader;
use rocketmq_protocol::protocol::header::get_lite_group_info_request_header::GetLiteGroupInfoRequestHeader;
use rocketmq_protocol::protocol::header::get_lite_topic_info_request_header::GetLiteTopicInfoRequestHeader;
use rocketmq_protocol::protocol::header::get_max_offset_request_header::GetMaxOffsetRequestHeader;
use rocketmq_protocol::protocol::header::get_max_offset_response_header::GetMaxOffsetResponseHeader;
use rocketmq_protocol::protocol::header::get_min_offset_request_header::GetMinOffsetRequestHeader;
use rocketmq_protocol::protocol::header::get_min_offset_response_header::GetMinOffsetResponseHeader;
use rocketmq_protocol::protocol::header::get_parent_topic_info_request_header::GetParentTopicInfoRequestHeader;
use rocketmq_protocol::protocol::header::get_producer_connection_list_request_header::GetProducerConnectionListRequestHeader;
use rocketmq_protocol::protocol::header::get_subscription_group_config_request_header::GetSubscriptionGroupConfigRequestHeader;
use rocketmq_protocol::protocol::header::get_topic_config_request_header::GetTopicConfigRequestHeader;
use rocketmq_protocol::protocol::header::get_topic_stats_request_header::GetTopicStatsRequestHeader;
use rocketmq_protocol::protocol::header::get_user_request_headers::GetUserRequestHeader;
use rocketmq_protocol::protocol::header::list_users_request_header::ListUsersRequestHeader;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_protocol::protocol::header::namesrv::broker_request::GetBrokerMemberGroupRequestHeader;
use rocketmq_protocol::protocol::header::pop_lite_message_request_header::PopLiteMessageRequestHeader;
use rocketmq_protocol::protocol::header::pop_lite_message_response_header::PopLiteMessageResponseHeader;
use rocketmq_protocol::protocol::header::query_consume_queue_request_header::QueryConsumeQueueRequestHeader;
use rocketmq_protocol::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
use rocketmq_protocol::protocol::header::query_consumer_offset_response_header::QueryConsumerOffsetResponseHeader;
use rocketmq_protocol::protocol::header::remove_broker_request_header::RemoveBrokerRequestHeader;
use rocketmq_protocol::protocol::header::search_offset_request_header::SearchOffsetRequestHeader;
use rocketmq_protocol::protocol::header::search_offset_response_header::SearchOffsetResponseHeader;
use rocketmq_protocol::protocol::header::trigger_lite_dispatch_request_header::TriggerLiteDispatchRequestHeader;
use rocketmq_protocol::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::static_topic::topic_config_and_queue_mapping::TopicConfigAndQueueMapping;
use rocketmq_protocol::protocol::subscription::subscription_group_config::SubscriptionGroupConfig;
use rocketmq_protocol::protocol::RemotingDeserializable;
use rocketmq_protocol::protocol::RemotingSerializable;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::RuntimeContext;
use rocketmq_security_api::MaintenanceAuthorizationContext;
use rocketmq_security_api::MaintenanceAuthorizationGrant;
use rocketmq_security_api::MaintenanceAuthorizer;
use rocketmq_security_api::MaintenanceCapability;
use rocketmq_security_api::MaintenancePolicy;
use rocketmq_security_api::MaintenancePrincipalBinding;
use rocketmq_security_api::MaintenanceRequestClass;
use rocketmq_security_api::MaintenanceResourceBudget;
use rocketmq_security_api::MaintenanceRole;
use rocketmq_security_api::MaintenanceRoleGrant;
use rocketmq_security_api::MAINTENANCE_POLICY_SCHEMA_VERSION;
use rocketmq_store::BrokerAdminStore;
use rocketmq_store::BrokerReadStore;
use rocketmq_store::BrokerReplicationStore;
use rocketmq_store::BrokerStorePort;
use rocketmq_store::CommitLogReadMode;
use rocketmq_store::ConsumeQueueStoreTrait;
use rocketmq_store::FlushDiskType;
use rocketmq_store::GetMessageStatus;
use rocketmq_store::HAService;
use rocketmq_store::MessageStoreConfig;
use rocketmq_store::TimerCheckpointSnapshot;
use rocketmq_store::TimerMessageStore;
#[cfg(feature = "extended_timeline")]
use rocketmq_store_api::TimerStoreMode;
use rocketmq_transport::api::AdmissionController;
use rocketmq_transport::api::AdmissionLimits;
use rocketmq_transport::api::DefaultRequestProcessor;
use rocketmq_transport::api::OutboundRequestOutcome;
use rocketmq_transport::api::ServerConfig;
use rocketmq_transport::api::TransportClient;
use rocketmq_transport::api::TransportClientConfig;
use rocketmq_transport::test_support::Connection;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::sleep;

use super::*;

mod pop_lite_core_causality_tests;
mod pop_lite_deferred_wire_tests;

impl BrokerRuntime {
    pub(crate) fn pull_message_context_for_test(&self) -> Arc<PullMessageProcessorContext<BrokerMessageStore>> {
        self.composition.state.build_pull_message_context()
    }

    pub(crate) fn pop_message_processor_for_test(&self) -> Arc<PopMessageProcessor<BrokerMessageStore>> {
        self.composition
            .state
            .build_pop_message_processor()
            .expect("test POP processor should initialize")
    }

    pub(crate) fn seed_pop_topic_and_group_for_test(&mut self, topic: &str, group: &str) {
        let _ = self
            .composition
            .state
            .topic_config_manager()
            .update_topic_config(TopicConfig::with_queues(topic, 1, 1), 0);
        let mut config = SubscriptionGroupConfig::new(CheetahString::from_slice(group));
        self.composition
            .state
            .subscription_group_manager_mut()
            .update_subscription_group_config(&mut config);
    }

    pub(crate) fn has_pop_consumer_filter_data_for_test(&self, topic: &str, group: &str) -> bool {
        self.composition
            .state
            .consumer_filter_manager()
            .get_consumer_filter_data(&topic.into(), &group.into())
            .is_some()
    }

    pub(crate) fn admin_runtime_for_test(&self) -> BrokerAdminRuntime<BrokerMessageStore> {
        self.admin_runtime()
    }

    pub(crate) async fn start_message_store_for_test(&mut self) -> Result<(), StoreError> {
        self.start_message_store().await
    }

    pub(crate) fn with_message_store_mut_for_test<R>(
        &mut self,
        operation: impl FnOnce(&mut BrokerMessageStore) -> R,
    ) -> R {
        self.detach_message_store_provider();
        let result = operation(
            self.composition
                .state
                .message_store_exclusive_mut()
                .expect("test message store should be initialized and exclusively owned"),
        );
        self.bind_message_store_provider();
        result
    }

    pub(crate) async fn load_message_store_for_test(&mut self) -> bool {
        self.load_message_store().await
    }

    pub(crate) async fn reput_message_store_once_for_test(&mut self) {
        self.detach_message_store_provider();
        if let Some(deferred) = self.composition.data_plane.deferred.as_ref() {
            deferred.unbind_producer_store();
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while self
                .composition
                .state
                .message_store
                .as_ref()
                .is_some_and(|store| Arc::strong_count(store) != 1 || Arc::weak_count(store) != 0)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("test message store leases should drain after provider detachment");
        self.composition
            .state
            .message_store_exclusive_mut()
            .expect("test message store should be initialized and exclusively owned")
            .reput_once()
            .await;
        self.bind_message_store_provider();
    }

    pub(crate) async fn shutdown_message_store_for_test(&mut self) {
        self.detach_message_store_provider();
        if let Some(deferred) = self.composition.data_plane.deferred.as_ref() {
            deferred.unbind_producer_store();
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while self
                .composition
                .state
                .message_store
                .as_ref()
                .is_some_and(|store| Arc::strong_count(store) != 1 || Arc::weak_count(store) != 0)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("test message store leases should drain after provider detachment");
        self.composition
            .state
            .message_store_exclusive_mut()
            .expect("test message store should be initialized and exclusively owned")
            .shutdown()
            .await;
    }
}
use crate::controller::replicas_manager::BrokerReplicaRole;

const CONTROLLER_TEST_MIN_BASE_PORT: u16 = 20_000;
const CONTROLLER_TEST_MAX_BASE_PORT: u16 = 60_000;
const CONTROLLER_TEST_PORT_BLOCK_SIZE: u16 = 128;
const CONTROLLER_TEST_FALLBACK_EPHEMERAL_PORT_RANGE: RangeInclusive<u16> = 32_768..=60_999;

fn shared_append_test_message(topic: &CheetahString, body: Bytes) -> MessageExtBrokerInner {
    let mut message = MessageExtBrokerInner::default();
    message.set_topic(topic.clone());
    message.message_ext_inner.set_queue_id(0);
    message.set_body(body);
    message
}

fn shared_append_test_batch(topic: &CheetahString, bodies: &[Bytes]) -> MessageExtBatch {
    let mut batch_body = BytesMut::new();
    for body in bodies {
        let record_size = 4 + 4 + 4 + 4 + 4 + body.len() + 2;
        batch_body.put_i32(record_size as i32);
        batch_body.put_i32(0);
        batch_body.put_i32(crc32(body.as_ref()) as i32);
        batch_body.put_i32(0);
        batch_body.put_i32(body.len() as i32);
        batch_body.put_slice(body.as_ref());
        batch_body.put_i16(0);
    }

    MessageExtBatch {
        message_ext_broker_inner: shared_append_test_message(topic, batch_body.freeze()),
        is_inner_batch: false,
        encoded_buff: None,
    }
}
static NEXT_CONTROLLER_TEST_PORT_BLOCK: AtomicU16 = AtomicU16::new(0);
static NEXT_CONTROLLER_TEST_TEMP_ID: AtomicU64 = AtomicU64::new(0);
static CONTROLLER_INTEGRATION_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[test]
fn background_tasks_capture_narrow_capabilities() {
    let production_source = [
        include_str!("../../src/broker_runtime.rs"),
        include_str!("../../src/broker_runtime/composition.rs"),
        include_str!("../../src/broker_runtime/control_plane.rs"),
        include_str!("../../src/broker_runtime/data_plane.rs"),
        include_str!("../../src/broker_runtime/request_pipeline.rs"),
        include_str!("../../src/broker_runtime/lifecycle.rs"),
        include_str!("../../src/broker_runtime/metadata.rs"),
    ]
    .join("\n");

    assert!(
        !production_source.contains("self.composition.state.clone()"),
        "background work must not retain the complete BrokerRuntimeState root"
    );
    assert!(
        !production_source.contains("inner: ArcMut<BrokerRuntimeState"),
        "BrokerRuntime must exclusively own its composition root"
    );
    assert!(
        production_source.contains("state: Box<BrokerRuntimeState"),
        "BrokerRuntime must keep an explicit exclusive composition root"
    );
}

fn next_controller_test_temp_id() -> u64 {
    NEXT_CONTROLLER_TEST_TEMP_ID.fetch_add(1, Ordering::Relaxed)
}

#[cfg(any(feature = "otel-metrics", feature = "otel-traces", feature = "otel-logs"))]
#[test]
fn build_broker_observability_config_applies_file_overrides() {
    let overrides = rocketmq_observability::ObservabilityOverrides {
        environment: Some("prod".to_string()),
        service_instance_id: Some("broker-a-0".to_string()),
        resource_attributes: Some(HashMap::from([
            ("zone".to_string(), "az-a".to_string()),
            ("rack".to_string(), "rack-1".to_string()),
        ])),
        metrics: rocketmq_observability::MetricsOverrides {
            exporter: Some(rocketmq_observability::MetricsExporter::OtlpGrpc),
            export_timeout_millis: Some(1_500),
            cardinality_limit: Some(64),
            sample_ratio: Some(0.25),
            topic_label_enabled: Some(false),
            consumer_group_label_enabled: Some(true),
            ..Default::default()
        },
        traces: rocketmq_observability::TracesOverrides {
            exporter: Some(rocketmq_observability::TraceExporter::OtlpGrpc),
            record_message_id: Some(true),
            record_message_keys: Some(true),
            record_body_size: Some(false),
            ..Default::default()
        },
        logs: rocketmq_observability::LogsOverrides {
            exporter: Some(rocketmq_observability::LogsExporter::OtlpGrpc),
        },
        otlp: rocketmq_observability::OtlpOverrides {
            endpoint: Some("http://collector:4317".to_string()),
            headers: Some(HashMap::from([
                ("authorization".to_string(), "Bearer token".to_string()),
                ("tenant".to_string(), "rocketmq".to_string()),
            ])),
            timeout_millis: Some(1_500),
            ..Default::default()
        },
        ..Default::default()
    };

    let config =
        build_broker_telemetry_bootstrap_config_with_overrides(&BrokerConfig::default(), &overrides).observability;

    assert!(config.enabled);
    assert!(config.metrics.enabled);
    assert!(config.traces.enabled);
    assert!(config.logs.enabled);
    assert_eq!(config.environment, "prod");
    assert_eq!(config.service_instance_id, "broker-a-0");
    assert_eq!(config.resource_attributes.get("zone").map(String::as_str), Some("az-a"));
    assert_eq!(
        config.resource_attributes.get("rack").map(String::as_str),
        Some("rack-1")
    );
    assert_eq!(config.otlp.endpoint, "http://collector:4317");
    assert_eq!(config.otlp.timeout_millis, 1_500);
    assert_eq!(config.metrics.export_timeout_millis, 1_500);
    assert_eq!(config.metrics.cardinality_limit, 64);
    assert!((config.metrics.sample_ratio - 0.25).abs() < f64::EPSILON);
    assert!(!config.metrics.topic_label_enabled);
    assert!(config.metrics.consumer_group_label_enabled);
    assert!(config.traces.record_message_id);
    assert!(config.traces.record_message_keys);
    assert!(!config.traces.record_body_size);
    assert_eq!(
        config.otlp.headers.get("authorization").map(String::as_str),
        Some("Bearer token")
    );
    assert_eq!(config.otlp.headers.get("tenant").map(String::as_str), Some("rocketmq"));
}

#[test]
fn build_broker_observability_config_maps_logging_bootstrap_defaults() {
    let broker_config = BrokerConfig {
        store_path_root_dir: "target/broker-telemetry-bootstrap".into(),
        ..Default::default()
    };

    let config = build_broker_telemetry_bootstrap_config(&broker_config);

    assert_eq!(
        config.observability.subscriber_install_policy,
        rocketmq_observability::SubscriberInstallPolicy::Required
    );
    assert!(!config.observability.enabled);
    assert!(!config.observability.logs.enabled);
    assert!(config.logging.enabled);
    assert_eq!(config.logging.filter, "info");
    assert!(config.logging.console.enabled);
    assert!(!config.logging.file.enabled);
    assert_eq!(config.logging.file.file_name_prefix, "rocketmq-broker");

    let expected_log_dir = std::path::Path::new("target/broker-telemetry-bootstrap").join("logs");
    assert_eq!(
        std::path::Path::new(config.logging.file.directory.as_str()),
        expected_log_dir.as_path()
    );
}

#[test]
fn broker_bootstrap_accepts_standard_otlp_environment_values() {
    let environment = rocketmq_observability::TelemetryEnvironmentValues {
        otlp_endpoint: Some("http://collector:4317".into()),
        otlp_protocol: Some("grpc".into()),
        ..Default::default()
    };
    let resolution = rocketmq_observability::resolve_telemetry_values(
        "rocketmq-broker",
        build_broker_telemetry_bootstrap_config(&BrokerConfig::default()),
        &rocketmq_observability::ObservabilityOverrides::default(),
        &environment,
        rocketmq_observability::TelemetryEnvironmentSpec {
            trace_sample_ratio_env: Some("ROCKETMQ_BROKER_TRACE_SAMPLE_RATIO"),
        },
    )
    .expect("valid standard OTLP environment should resolve");
    let config = resolution.bootstrap;

    assert!(resolution.environment_applied);
    assert_eq!(config.observability.service_name, "rocketmq-broker");
    assert_eq!(
        config.observability.metrics.exporter,
        rocketmq_observability::MetricsExporter::OtlpGrpc
    );
    assert_eq!(
        config.observability.traces.exporter,
        rocketmq_observability::TraceExporter::OtlpGrpc
    );
    assert_eq!(
        config.observability.logs.exporter,
        rocketmq_observability::LogsExporter::OtlpGrpc
    );
}

#[cfg(feature = "otel-metrics")]
#[test]
fn broker_runtime_observability_registration_uses_injected_handle_metrics_policy() {
    let mut telemetry_config = rocketmq_observability::ObservabilityConfig {
        enabled: true,
        ..rocketmq_observability::ObservabilityConfig::default()
    };
    telemetry_config.metrics.enabled = true;
    telemetry_config.metrics.exporter = rocketmq_observability::MetricsExporter::Log;
    let telemetry_guard =
        rocketmq_observability::init_observability(&telemetry_config).expect("log metrics telemetry should initialize");
    let telemetry_handle = telemetry_guard.handle();
    assert!(telemetry_handle.metrics_enabled());

    let mut runtime = BrokerRuntime::new_with_validated_config_and_telemetry(
        Arc::new(ValidatedBrokerConfig::default()),
        crate::test_service_context("broker-final-metrics-policy"),
        telemetry_handle,
    );
    runtime.initialize_observability();

    assert!(runtime.composition.state.observability_metrics_initialized);
    telemetry_guard
        .shutdown()
        .into_result()
        .expect("test telemetry should shut down");
}

#[cfg(feature = "otel-metrics")]
#[test]
fn broker_metrics_sampling_uses_resolved_runtime_policy() {
    let policy = rocketmq_observability::MetricsRuntimePolicy {
        enabled: true,
        sample_ratio: 0.125,
        export_interval_millis: 1_250,
        cardinality_limit: 7,
    };

    let sampling = super::composition::broker_metrics_sampling_config(policy);

    assert!((sampling.sample_ratio - 0.125).abs() < f64::EPSILON);
}

#[cfg(feature = "otel-metrics")]
#[test]
fn consumer_lag_refresh_uses_resolved_export_interval() {
    let policy = rocketmq_observability::MetricsRuntimePolicy {
        enabled: true,
        sample_ratio: 0.125,
        export_interval_millis: 1_250,
        cardinality_limit: 7,
    };

    let (refresh_interval, _) = super::control_plane::consumer_lag_runtime_settings(policy);

    assert_eq!(refresh_interval, Duration::from_millis(1_250));
}

#[cfg(feature = "otel-metrics")]
#[test]
fn consumer_lag_work_bound_uses_resolved_cardinality_limit() {
    let policy = rocketmq_observability::MetricsRuntimePolicy {
        enabled: true,
        sample_ratio: 0.125,
        export_interval_millis: 1_250,
        cardinality_limit: 7,
    };

    let (_, cardinality_limit) = super::control_plane::consumer_lag_runtime_settings(policy);

    assert_eq!(cardinality_limit, 7);
}

#[test]
fn build_auth_config_maps_signature_algorithm() {
    let broker_config = BrokerConfig {
        signature_algorithm: CheetahString::from_static_str("HmacSHA256"),
        request_timestamp_expired_millis: 300_000,
        ..BrokerConfig::default()
    };

    let auth_config = build_auth_config(&broker_config);

    assert_eq!(auth_config.signature_algorithm, SignatureAlgorithm::HmacSha256);
    assert_eq!(auth_config.request_timestamp_expired_millis, 300_000);
}

#[test]
fn transaction_capability_handles_share_runtime_generations() {
    let runtime = BrokerRuntime::new(
        Arc::new(BrokerConfig::default()),
        Arc::new(MessageStoreConfig::default()),
    );

    let offset_handle = runtime.composition.state.consumer_offset_manager_handle();
    let mapping_handle = runtime.composition.state.topic_queue_mapping_manager_handle();

    assert!(Arc::ptr_eq(
        &offset_handle,
        &runtime.composition.state.consumer_offset_manager
    ));
    assert!(Arc::ptr_eq(
        &mapping_handle,
        &runtime.composition.state.topic_queue_mapping_manager
    ));
}

#[test]
fn build_auth_config_maps_java_auth_integration_fields() {
    let broker_config = BrokerConfig {
        authentication_provider: CheetahString::from_static_str(
            "org.apache.rocketmq.auth.authentication.provider.DefaultAuthenticationProvider",
        ),
        authentication_metadata_provider: CheetahString::from_static_str(
            "org.apache.rocketmq.auth.authentication.provider.LocalAuthenticationMetadataProvider",
        ),
        authentication_strategy: CheetahString::from_static_str(
            "org.apache.rocketmq.auth.authentication.strategy.StatefulAuthenticationStrategy",
        ),
        authorization_provider: CheetahString::from_static_str(
            "org.apache.rocketmq.auth.authorization.provider.DefaultAuthorizationProvider",
        ),
        authorization_metadata_provider: CheetahString::from_static_str(
            "org.apache.rocketmq.auth.authorization.provider.LocalAuthorizationMetadataProvider",
        ),
        authorization_strategy: CheetahString::from_static_str(
            "org.apache.rocketmq.auth.authorization.strategy.StatefulAuthorizationStrategy",
        ),
        migrate_auth_from_v1_enabled: true,
        user_cache_max_num: 11,
        user_cache_expired_second: 12,
        user_cache_refresh_second: 13,
        acl_cache_max_num: 21,
        acl_cache_expired_second: 22,
        acl_cache_refresh_second: 23,
        stateful_authentication_cache_max_num: 31,
        stateful_authentication_cache_expired_second: 32,
        stateful_authorization_cache_max_num: 41,
        stateful_authorization_cache_expired_second: 42,
        stateful_authorization_cache_negative_enable: true,
        ..BrokerConfig::default()
    };

    let auth_config = build_auth_config(&broker_config);

    assert_eq!(
        auth_config.authentication_provider,
        broker_config.authentication_provider
    );
    assert_eq!(
        auth_config.authentication_metadata_provider,
        broker_config.authentication_metadata_provider
    );
    assert_eq!(
        auth_config.authentication_strategy,
        broker_config.authentication_strategy
    );
    assert_eq!(auth_config.authorization_provider, broker_config.authorization_provider);
    assert_eq!(
        auth_config.authorization_metadata_provider,
        broker_config.authorization_metadata_provider
    );
    assert_eq!(auth_config.authorization_strategy, broker_config.authorization_strategy);
    assert!(auth_config.migrate_auth_from_v1_enabled);
    assert_eq!(auth_config.user_cache_max_num, 11);
    assert_eq!(auth_config.user_cache_expired_second, 12);
    assert_eq!(auth_config.user_cache_refresh_second, 13);
    assert_eq!(auth_config.acl_cache_max_num, 21);
    assert_eq!(auth_config.acl_cache_expired_second, 22);
    assert_eq!(auth_config.acl_cache_refresh_second, 23);
    assert_eq!(auth_config.stateful_authentication_cache_max_num, 31);
    assert_eq!(auth_config.stateful_authentication_cache_expired_second, 32);
    assert_eq!(auth_config.stateful_authorization_cache_max_num, 41);
    assert_eq!(auth_config.stateful_authorization_cache_expired_second, 42);
    assert!(auth_config.stateful_authorization_cache_negative_enable);
}

#[tokio::test]
async fn initial_rpc_hooks_accepts_java_inner_client_credentials() {
    let broker_config = Arc::new(BrokerConfig {
        inner_client_authentication_credentials: CheetahString::from_static_str(
            r#"{"accessKey":"inner","secretKey":"inner-secret"}"#,
        ),
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig::default());
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);

    assert!(runtime.initial_rpc_hooks());
}

#[tokio::test]
async fn broker_runtime_service_context_parents_probe_task_groups() {
    let context = RuntimeContext::from_current("broker-context-runtime-test");
    let service = context.service_context("broker-service");
    let broker_config = Arc::new(BrokerConfig::default());
    let message_store_config = Arc::new(MessageStoreConfig::default());
    let mut runtime = BrokerRuntime::new_with_service_context(broker_config, message_store_config, service.clone());

    assert!(runtime.install_remoting_server_report_probe());
    assert!(runtime.install_request_processor_task_probe());

    let remoting_group = runtime
        .lifecycle
        .remoting_server_task_group
        .as_ref()
        .expect("remoting probe task group should be installed");
    let request_group = runtime
        .lifecycle
        .request_processor_task_group
        .as_ref()
        .expect("request processor probe task group should be installed");
    assert_eq!(remoting_group.parent_id(), Some(service.task_group().id()));
    assert_eq!(request_group.parent_id(), Some(service.task_group().id()));

    let report = runtime.shutdown_basic_service_with_report().await;
    assert!(report.is_healthy());
    assert!(report.remoting.is_some());
    assert!(report.request_processor.is_some());
    assert_eq!(report.unhealthy_component_count(), 0);
    assert!(report.timed_out_component_names().is_empty());
    assert!(report.component_names().contains(&"scheduled_tasks"));

    let scheduled_task_group_report = runtime
        .lifecycle
        .scheduled_task_manager
        .last_task_group_shutdown_report()
        .expect("scheduled task group shutdown report should be retained by its owner");
    assert!(
        scheduled_task_group_report.is_healthy(),
        "{}",
        scheduled_task_group_report.to_json()
    );
    assert_eq!(
        scheduled_task_group_report.name,
        rocketmq_runtime::schedule::simple_scheduler::LEGACY_SCHEDULED_TASK_MANAGER_BOUNDARY
    );

    let service_report = service.task_group().shutdown(Duration::from_secs(1)).await;
    assert!(
        !service_report
            .children
            .iter()
            .any(|child| child.name
                == rocketmq_runtime::schedule::simple_scheduler::LEGACY_SCHEDULED_TASK_MANAGER_BOUNDARY),
        "{}",
        service_report.to_json()
    );
}

#[tokio::test]
async fn broker_metadata_io_actor_durably_persists_topic_snapshot() {
    let temp = tempfile::tempdir().expect("metadata I/O test directory should be created");
    let root = temp.path().to_string_lossy().into_owned();
    let broker_config = Arc::new(BrokerConfig {
        store_path_root_dir: root.clone().into(),
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: root.into(),
        ..MessageStoreConfig::default()
    });
    let context = RuntimeContext::try_from_current("broker-metadata-io-test").unwrap();
    let runtime =
        BrokerRuntime::new_with_service_context(broker_config, message_store_config, context.service_context("broker"));
    let manager = runtime.composition.state.topic_config_manager_handle();
    manager.update_topic_config(TopicConfig::with_queues("MetadataIoTopic", 2, 3), 0);
    let coordinator = runtime.composition.state.topic_config_coordinator_handle();
    coordinator.persist_and_wait().await.unwrap();

    let actor = runtime
        .composition
        .state
        .metadata_io
        .as_ref()
        .and_then(|result| result.as_ref().ok())
        .expect("service-context broker should own metadata I/O actor")
        .clone();
    let snapshot = actor.snapshot();
    let topics = snapshot
        .resources
        .iter()
        .find(|resource| resource.resource.as_ref() == "broker.topic-config")
        .expect("topic resource should be tracked");
    assert!(topics.durable_generation.is_some());
    assert_eq!(snapshot.pending_operations, 0);
    assert!(std::path::Path::new(&manager.config_file_path()).is_file());

    let coordinator_report = coordinator
        .shutdown_until(ShutdownDeadline::after(Duration::from_secs(5)))
        .await;
    assert!(coordinator_report.can_unregister(), "{coordinator_report:?}");
    let metadata_report = actor
        .shutdown_until(MetadataDeadline::after(Duration::from_secs(5)))
        .await;
    assert!(!metadata_report.timed_out);
}

#[tokio::test]
async fn schedule_role_status_changes_only_after_final_persistence_succeeds() {
    let temp_dir = tempfile::tempdir().expect("schedule role transition temp dir should be created");
    let blocked_root = temp_dir.path().join("blocked-store-root");
    std::fs::write(&blocked_root, b"not a directory").expect("blocking file should be created");
    let root = blocked_root.to_string_lossy().into_owned();
    let broker_config = Arc::new(BrokerConfig {
        store_path_root_dir: root.clone().into(),
        auth_config_path: temp_dir.path().join("auth.json").to_string_lossy().into_owned().into(),
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: root.into(),
        ..MessageStoreConfig::default()
    });
    let context = RuntimeContext::from_current("broker-schedule-role-transition-test");
    let service_context = context.service_context("broker");
    let mut runtime = BrokerRuntime::new_with_service_context(broker_config, message_store_config, service_context);

    runtime
        .composition
        .state
        .change_schedule_service_status(true)
        .await
        .expect("schedule service should start");
    assert!(runtime
        .composition
        .state
        .is_schedule_service_start
        .load(Ordering::Acquire));

    let error = runtime
        .composition
        .state
        .change_schedule_service_status(false)
        .await
        .expect_err("invalid store root should reject the final persistence transition");
    assert!(error.to_string().contains("ScheduleMessageService"));
    assert!(runtime
        .composition
        .state
        .is_schedule_service_start
        .load(Ordering::Acquire));
    assert!(!runtime.composition.state.schedule_message_service().is_started());

    std::fs::remove_file(&blocked_root).expect("blocking file should be removed");
    std::fs::create_dir_all(&blocked_root).expect("store root should be repaired");
    runtime
        .composition
        .state
        .change_schedule_service_status(false)
        .await
        .expect("schedule transition should succeed after the store root is repaired");
    assert!(!runtime
        .composition
        .state
        .is_schedule_service_start
        .load(Ordering::Acquire));

    let report = context.shutdown_tasks(Duration::from_secs(1)).await;
    assert!(report.is_healthy(), "{report:?}");
}

#[test]
fn broker_basic_shutdown_report_exposes_required_component_names() {
    let report = BrokerBasicServiceShutdownReport::default();

    assert_eq!(
        report.component_names(),
        vec![
            "remoting",
            "request_processor",
            "topic_config",
            "broker_outer_api",
            "client_housekeeping",
            "auth",
            "service_tasks",
            "observability",
            "scheduled_tasks",
            "message_store",
            "deferred_services",
            "transaction_services",
            "fast_failure",
            "topic_route",
            "consumer_offset",
            "subscription_group",
            "metadata_io",
            "shutdown_deadline",
        ]
    );
}

#[test]
fn transaction_shutdown_precedes_topic_coordinator_and_message_store() {
    let source = include_str!("../../src/broker_runtime/lifecycle.rs");
    let transaction_shutdown = source
        .find("self.composition.state.transactional_message_check_service.take()")
        .expect("transaction check service should detach during shutdown");
    let topic_shutdown = source
        .find(
            "if let Some(topic_config_coordinator) = self.composition.state.topic_config_coordinator.as_ref().cloned()",
        )
        .expect("topic coordinator shutdown should exist");
    let store_shutdown = source
        .find("let message_store_outcome =")
        .expect("message store shutdown should exist");

    assert!(transaction_shutdown < topic_shutdown);
    assert!(topic_shutdown < store_shutdown);
    assert!(source.contains("self.composition.state.transactional_message_check_listener.take()"));
    assert!(source.contains("self.composition.state.transactional_message_service.take()"));
}

#[test]
fn scheduled_tasks_shutdown_precedes_exclusive_message_store_shutdown() {
    let source = include_str!("../../src/broker_runtime/lifecycle.rs");
    let scheduled_shutdown = source
        .find("let scheduled_report = self")
        .expect("scheduled task shutdown should exist");
    let store_detach = source
        .find("self.detach_message_store_provider();")
        .expect("message store provider detach should exist");
    let store_shutdown = source
        .find("let message_store_outcome =")
        .expect("message store shutdown should exist");

    assert!(scheduled_shutdown < store_detach);
    assert!(store_detach < store_shutdown);
}

#[test]
fn broker_basic_shutdown_report_aggregates_unhealthy_and_timed_out_components() {
    let report = BrokerBasicServiceShutdownReport {
        auth: BrokerShutdownComponentReport::completed("auth", Duration::from_millis(1)),
        message_store: BrokerShutdownComponentReport::timed_out("message_store", Duration::from_millis(2)),
        fast_failure: BrokerShutdownComponentReport::unhealthy(
            "fast_failure",
            Duration::from_millis(3),
            "forced unhealthy component",
        ),
        ..Default::default()
    };

    assert!(!report.is_healthy());
    assert_eq!(report.unhealthy_component_count(), 2);
    assert_eq!(
        report.unhealthy_component_names(),
        vec!["message_store", "fast_failure"]
    );
    assert_eq!(report.timed_out_component_names(), vec!["message_store"]);
}

#[tokio::test]
async fn expired_broker_shutdown_deadline_does_not_poll_forever() {
    let deadline = ShutdownDeadline::at(Instant::now());

    let result = await_shutdown_deadline(deadline, std::future::pending::<()>()).await;

    assert!(result.is_err());
}

#[test]
fn broker_shutdown_timeout_report_preserves_unfinished_components() {
    let progress = BrokerShutdownProgress::new();
    progress.complete("remoting");
    progress.complete("request_processor");
    let report = BrokerBasicServiceShutdownReport {
        deadline: BrokerShutdownComponentReport::timed_out("shutdown_deadline", Duration::from_millis(1)),
        unfinished_components: progress.unfinished(),
        ..Default::default()
    };

    assert!(!report.is_healthy());
    assert!(!report.unhealthy_component_names().contains(&"remoting"));
    assert!(!report.unhealthy_component_names().contains(&"request_processor"));
    assert!(report.unhealthy_component_names().contains(&"message_store"));
    assert!(report.unhealthy_component_names().contains(&"observability"));
    assert_eq!(report.timed_out_component_names(), vec!["shutdown_deadline"]);
}

#[test]
fn broker_store_shutdown_failure_preserves_typed_cause_and_remains_unfinished() {
    let progress = BrokerShutdownProgress::new();
    let mut report = BrokerBasicServiceShutdownReport::default();
    let error = rocketmq_store::StoreError::new(
        &rocketmq_error::STORAGE_WRITE_FAILED,
        rocketmq_store::StoreOperation::Shutdown,
    )
    .with_detail("injected final flush failure");

    record_message_store_shutdown_outcome(
        &mut report,
        &progress,
        MessageStoreShutdownOutcome::Failed(error),
        Duration::from_millis(3),
    );

    assert!(!report.message_store.healthy);
    assert_eq!(report.message_store.error_kind, Some("storage.write.failed"));
    assert_eq!(
        report.message_store.detail.as_deref(),
        Some("storage.write.failed: Storage write failed")
    );
    assert!(progress.unfinished().contains(&"message_store"));
    let recorded = progress
        .message_store_report()
        .expect("store failure should remain available if a later phase reaches the deadline");
    assert_eq!(recorded.error_kind, Some("storage.write.failed"));
    assert_eq!(
        recorded.detail.as_deref(),
        Some("storage.write.failed: Storage write failed")
    );
    assert!(!report.is_healthy());
}

#[tokio::test]
async fn timed_out_shutdown_blocking_operation_remains_owned_until_completion() {
    let context = RuntimeContext::from_current("broker-blocking-shutdown-test");
    let service = context.service_context("broker-blocking-shutdown-service");
    let release = Arc::new((StdMutex::new(false), Condvar::new()));
    let operation_release = Arc::clone(&release);

    let result = run_shutdown_blocking_operation(
        &service,
        ShutdownDeadline::after(Duration::from_millis(10)),
        "broker.test-blocking-shutdown",
        move || {
            let (released, signal) = &*operation_release;
            let mut released = released.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            while !*released {
                released = signal.wait(released).unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        },
    )
    .await;

    assert!(matches!(result, Err(BrokerBlockingShutdownError::TimedOut)));
    assert_eq!(service.metadata_io().blocking_still_running(), 1);
    assert!(service.task_group().task_count() > 0);

    let (released, signal) = &*release;
    *released.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = true;
    signal.notify_all();
    tokio::time::timeout(Duration::from_secs(1), async {
        while service.metadata_io().blocking_still_running() != 0 || service.task_group().task_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owned shutdown blocking task should finish after release");
}

#[tokio::test]
async fn blocking_shutdown_hook_cannot_extend_the_absolute_deadline() {
    struct BlockingHook {
        release: Arc<(StdMutex<bool>, Condvar)>,
    }

    impl crate::broker::broker_hook::ShutdownHook for BlockingHook {
        fn before_shutdown(&self) {
            let (released, signal) = &*self.release;
            let mut released = released.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            while !*released {
                released = signal.wait(released).unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        }
    }

    let context = RuntimeContext::from_current("broker-blocking-hook-test");
    let service = context.service_context("broker-blocking-hook-service");
    let mut runtime = BrokerRuntime::new_with_service_context(
        Arc::new(BrokerConfig::default()),
        Arc::new(MessageStoreConfig::default()),
        service,
    );
    let release = Arc::new((StdMutex::new(false), Condvar::new()));
    runtime.lifecycle.shutdown_hook = Some(Arc::new(BlockingHook {
        release: Arc::clone(&release),
    }));
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        runtime.shutdown_basic_service_until(ShutdownDeadline::after(Duration::from_millis(20))),
    )
    .await;
    let (released, signal) = &*release;
    *released.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = true;
    signal.notify_all();

    let report = result.expect("shutdown must return while the hook remains blocked");
    assert!(report.deadline.timed_out);
    let _ = context.shutdown_tasks(Duration::from_secs(1)).await;
}

#[test]
fn broker_shutdown_report_records_healthy_telemetry_detail() {
    let telemetry_report = rocketmq_observability::TelemetryShutdownReport {
        subscriber_installed: true,
        file_log_enabled: true,
        dropped_log_lines: 0,
        logs_shutdown_ok: true,
        traces_shutdown_ok: true,
        metrics_shutdown_ok: true,
        logs_shutdown_error: None,
        traces_shutdown_error: None,
        metrics_shutdown_error: None,
    };

    let component =
        BrokerShutdownComponentReport::from_telemetry_shutdown_report(&telemetry_report, Duration::from_millis(4));

    assert!(component.present);
    assert!(component.healthy);
    assert_eq!(component.name, "observability");
    let detail = component.detail.expect("telemetry detail should be recorded");
    assert!(detail.contains("\"subscriber_installed\": true"));
    assert!(detail.contains("\"file_log_enabled\": true"));
}

#[test]
fn broker_shutdown_report_marks_telemetry_provider_failure_unhealthy() {
    let telemetry_report = rocketmq_observability::TelemetryShutdownReport {
        subscriber_installed: true,
        file_log_enabled: false,
        dropped_log_lines: 0,
        logs_shutdown_ok: false,
        traces_shutdown_ok: true,
        metrics_shutdown_ok: true,
        logs_shutdown_error: Some("logger provider failed".to_string()),
        traces_shutdown_error: None,
        metrics_shutdown_error: None,
    };

    let component =
        BrokerShutdownComponentReport::from_telemetry_shutdown_report(&telemetry_report, Duration::from_millis(5));

    assert!(component.present);
    assert!(!component.healthy);
    assert_eq!(component.name, "observability");
    assert!(component
        .detail
        .expect("telemetry failure detail should be recorded")
        .contains("logger provider failed"));
}

#[tokio::test]
async fn initial_rpc_hooks_rejects_malformed_inner_client_credentials() {
    let broker_config = Arc::new(BrokerConfig {
        inner_client_authentication_credentials: CheetahString::from_static_str("{invalid-json"),
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig::default());
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);

    assert!(!runtime.initial_rpc_hooks());
}

#[tokio::test]
async fn shutdown_scheduled_tasks_waits_for_running_task_drop() {
    struct DropMarker(Arc<AtomicBool>);

    impl Drop for DropMarker {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    let broker_config = Arc::new(BrokerConfig::default());
    let message_store_config = Arc::new(MessageStoreConfig::default());
    let runtime = BrokerRuntime::new(broker_config, message_store_config);
    let started = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));
    runtime
        .lifecycle
        .scheduled_task_manager
        .add_fixed_delay_task(Duration::ZERO, Duration::from_secs(60), {
            let started = Arc::clone(&started);
            let dropped = Arc::clone(&dropped);
            move |_token| {
                let started = Arc::clone(&started);
                let dropped = Arc::clone(&dropped);
                async move {
                    let _marker = DropMarker(dropped);
                    started.store(true, Ordering::Release);
                    future::pending::<rocketmq_runtime::RuntimeResult<()>>().await
                }
            }
        })
        .expect("scheduled shutdown test task should start");
    tokio::time::timeout(Duration::from_secs(1), async {
        while !started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("scheduled task should start");

    runtime
        .shutdown_scheduled_tasks_with_timeout(Duration::from_millis(10))
        .await;

    assert_eq!(runtime.lifecycle.scheduled_task_manager.task_count(), 0);
    assert!(
        dropped.load(Ordering::Acquire),
        "broker scheduled shutdown should wait until aborted tasks release their running future"
    );
}

#[tokio::test]
async fn broker_shutdown_cancels_scheduled_store_lease_before_store_lock_release() {
    let temp_root = std::env::temp_dir().join(format!(
        "rocketmq-rust-broker-scheduled-store-lease-{}",
        current_millis()
    ));
    let ha_listen_port = allocate_broker_runtime_test_port() as usize;
    let broker_config = Arc::new(BrokerConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        ha_listen_port,
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    assert!(runtime.initialize_metadata().await.is_ok());
    assert!(runtime.initialize_message_store().await);
    assert!(runtime.load_message_store_for_test().await);
    runtime
        .start_message_store_for_test()
        .await
        .expect("message store should acquire its lock file");

    let store = runtime.composition.data_plane.escape_bridge_owner.store_capability();
    let lease_started = Arc::new(AtomicBool::new(false));
    runtime
        .lifecycle
        .scheduled_task_manager
        .add_fixed_delay_task(Duration::ZERO, Duration::from_secs(60), {
            let lease_started = Arc::clone(&lease_started);
            move |token| {
                let store = store.clone();
                let lease_started = Arc::clone(&lease_started);
                async move {
                    let _lease = store.read_lease().expect("scheduled task should acquire a Store lease");
                    lease_started.store(true, Ordering::Release);
                    token.cancelled().await;
                    Ok(())
                }
            }
        })
        .expect("scheduled Store lease task should start");
    tokio::time::timeout(Duration::from_secs(1), async {
        while !lease_started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("scheduled Store lease should be active");

    let report = runtime
        .shutdown_basic_service_until(ShutdownDeadline::after(Duration::from_secs(10)))
        .await;

    assert!(
        report.scheduled_tasks.present && report.scheduled_tasks.healthy,
        "{report:?}"
    );
    assert!(
        report.message_store.present && report.message_store.healthy,
        "{report:?}"
    );
    assert!(
        runtime.composition.state.message_store().is_none(),
        "a healthy shutdown should release the runtime's final message-store owner"
    );
    let mut restarted = BrokerRuntime::new(
        Arc::new(BrokerConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            ..BrokerConfig::default()
        }),
        Arc::new(MessageStoreConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            ha_listen_port,
            ..MessageStoreConfig::default()
        }),
    );
    assert!(restarted.initialize_metadata().await.is_ok());
    assert!(restarted.initialize_message_store().await);
    assert!(restarted.load_message_store_for_test().await);
    restarted
        .start_message_store_for_test()
        .await
        .expect("shutdown Broker should release the Store lock for a same-path restart");
    restarted.shutdown_message_store_for_test().await;
    let _ = std::fs::remove_dir_all(temp_root);
}

#[tokio::test]
async fn shutdown_basic_service_stops_auth_acl_file_watcher() {
    let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-broker-auth-shutdown-{}", current_millis()));
    std::fs::create_dir_all(&temp_root).expect("create broker auth shutdown test root");
    let acl_file = temp_root.join("plain_acl.yml");
    std::fs::write(
        &acl_file,
        r#"
accounts:
  - accessKey: alice
    secretKey: first
"#,
    )
    .expect("write initial acl file");

    let broker_config = Arc::new(BrokerConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        auth_config_path: temp_root.join("auth.json").to_string_lossy().into_owned().into(),
        acl_file: acl_file.to_string_lossy().into_owned().into(),
        acl_file_watch_enabled: true,
        acl_file_watch_interval_millis: 20,
        authentication_enabled: true,
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    assert!(runtime.initial_acl().await);
    let auth_runtime = runtime
        .composition
        .request_pipeline
        .auth_runtime
        .as_ref()
        .expect("auth runtime should be initialized")
        .clone();

    tokio::time::timeout(Duration::from_secs(5), runtime.shutdown_basic_service())
        .await
        .expect("broker basic service shutdown should be bounded");
    assert!(runtime.composition.request_pipeline.auth_runtime.is_none());

    let generation = auth_runtime.acl_generation();
    let reload_attempts = auth_runtime.metrics_snapshot().acl_reload_attempts;
    std::fs::write(
        &acl_file,
        r#"
accounts:
  - accessKey: alice
    secretKey: second
"#,
    )
    .expect("write changed acl file after shutdown");
    sleep(Duration::from_millis(120)).await;

    assert_eq!(auth_runtime.acl_generation(), generation);
    assert_eq!(auth_runtime.metrics_snapshot().acl_reload_attempts, reload_attempts);
    let _ = std::fs::remove_dir_all(temp_root);
}

#[tokio::test]
async fn broker_runtime_exposes_auth_metrics_snapshot() {
    let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-broker-auth-metrics-{}", current_millis()));
    std::fs::create_dir_all(&temp_root).expect("create broker auth metrics test root");
    let acl_file = temp_root.join("plain_acl.yml");
    std::fs::write(
        &acl_file,
        r#"
accounts:
  - accessKey: alice
    secretKey: secret
"#,
    )
    .expect("write initial acl file");

    let broker_config = Arc::new(BrokerConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        auth_config_path: temp_root.join("auth.json").to_string_lossy().into_owned().into(),
        acl_file: acl_file.to_string_lossy().into_owned().into(),
        authentication_enabled: true,
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    assert!(runtime.auth_metrics_snapshot().is_none());
    assert!(runtime.initial_acl().await);

    let auth_runtime = runtime
        .composition
        .request_pipeline
        .auth_runtime
        .as_ref()
        .expect("auth runtime should be initialized")
        .clone();
    assert_eq!(
        runtime
            .auth_metrics_snapshot()
            .expect("auth metrics should be exposed")
            .whitelist_misses,
        0
    );

    assert!(!auth_runtime
        .is_acl_white_remote_address(None, Some("203.0.113.10"))
        .expect("white list check should succeed"));

    assert_eq!(
        runtime
            .auth_metrics_snapshot()
            .expect("auth metrics should be exposed")
            .whitelist_misses,
        1
    );
    tokio::time::timeout(Duration::from_secs(5), runtime.shutdown_basic_service())
        .await
        .expect("broker basic service shutdown should be bounded");
    let _ = std::fs::remove_dir_all(temp_root);
}

struct TestNameServer {
    addr: CheetahString,
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl TestNameServer {
    fn addr(&self) -> CheetahString {
        self.addr.clone()
    }

    async fn shutdown(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            match tokio::time::timeout(Duration::from_secs(15), handle).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => panic!("namesrv task should not panic: {error}"),
                Err(_) => panic!("timed out waiting for namesrv shutdown"),
            }
        }
    }
}

fn controller_addr_list(peers: &[RaftPeer]) -> CheetahString {
    CheetahString::from_string(
        peers
            .iter()
            .map(|peer| peer.addr.to_string())
            .collect::<Vec<String>>()
            .join(";"),
    )
}

fn controller_cluster_root(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rocketmq-rust-{prefix}-{}-{}-{}",
        std::process::id(),
        current_millis(),
        next_controller_test_temp_id()
    ))
}

fn allocate_controller_test_base_port() -> u16 {
    let block_count = (u32::from(CONTROLLER_TEST_MAX_BASE_PORT - CONTROLLER_TEST_MIN_BASE_PORT)
        / u32::from(CONTROLLER_TEST_PORT_BLOCK_SIZE)) as u16;
    let seed = ((u64::from(std::process::id()) ^ current_millis()) % u64::from(block_count)) as u16;
    let ephemeral_port_range = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range")
        .ok()
        .and_then(|contents| {
            let mut ports = contents.split_whitespace();
            let start = ports.next()?.parse::<u16>().ok()?;
            let end = ports.next()?.parse::<u16>().ok()?;
            (start <= end).then_some(start..=end)
        })
        .unwrap_or(CONTROLLER_TEST_FALLBACK_EPHEMERAL_PORT_RANGE);

    for _ in 0..block_count {
        let block = NEXT_CONTROLLER_TEST_PORT_BLOCK
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(seed)
            % block_count;
        let base_port =
            u32::from(CONTROLLER_TEST_MIN_BASE_PORT) + u32::from(block) * u32::from(CONTROLLER_TEST_PORT_BLOCK_SIZE);
        let base_port = u16::try_from(base_port).expect("controller test base port should fit u16");
        let required_ports = controller_test_required_ports(base_port);
        if controller_test_ports_available(&required_ports, &ephemeral_port_range) {
            return base_port;
        }
    }

    panic!("failed to allocate a free controller-mode test port block after checking {block_count} candidate blocks");
}

fn controller_test_required_ports(base_port: u16) -> [u16; 16] {
    [
        base_port + 1,
        base_port + 2,
        base_port + 3,
        base_port + 11,
        base_port + 12,
        base_port + 13,
        base_port + 19,
        base_port + 21,
        base_port + 22,
        base_port + 29,
        base_port + 31,
        base_port + 32,
        base_port + 39,
        base_port + 41,
        base_port + 42,
        base_port + 90,
    ]
}

fn controller_test_ports_available(required_ports: &[u16], ephemeral_port_range: &RangeInclusive<u16>) -> bool {
    if required_ports.iter().any(|port| ephemeral_port_range.contains(port)) {
        return false;
    }

    let mut listeners = Vec::with_capacity(required_ports.len());
    for port in required_ports {
        match TcpListener::bind(("0.0.0.0", *port)) {
            Ok(listener) => listeners.push(listener),
            Err(_) => return false,
        }
    }
    true
}

#[test]
fn controller_test_required_ports_include_broker_fast_remoting_ports() {
    let base_port = 20_000;
    let required_ports = controller_test_required_ports(base_port);

    assert!(required_ports.contains(&(base_port + 19)));
    assert!(required_ports.contains(&(base_port + 29)));
    assert!(required_ports.contains(&(base_port + 39)));
}

#[test]
fn controller_test_ports_available_rejects_ephemeral_ports() {
    assert!(!controller_test_ports_available(
        &[32_768, 60_999],
        &CONTROLLER_TEST_FALLBACK_EPHEMERAL_PORT_RANGE
    ));
}

#[cfg(target_os = "linux")]
#[test]
fn controller_test_ports_available_rejects_non_primary_loopback_conflict() {
    let Ok(other_loopback_listener) = TcpListener::bind(("127.0.0.2", 0)) else {
        return;
    };
    let port = other_loopback_listener
        .local_addr()
        .expect("read non-primary loopback listener address")
        .port();
    let Ok(primary_loopback_listener) = TcpListener::bind(("127.0.0.1", port)) else {
        return;
    };
    drop(primary_loopback_listener);

    assert!(!controller_test_ports_available(&[port], &(0..=0)));
}

async fn process_broker_request(processor: &DefaultServerProcessor, request: &mut RemotingCommand) -> RemotingCommand {
    dispatch_broker_request(processor, request)
        .await
        .expect("canonical dispatch should succeed")
        .expect("canonical dispatch should return a response")
}

async fn dispatch_broker_request(
    processor: &DefaultServerProcessor,
    request: &mut RemotingCommand,
) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
    let mut processor = processor.clone();
    processor.set_auth_disabled_by_validated_config();
    let (mut client, server) = crate::processor::processor_test_support::start_processor_server(
        "broker-runtime-fixture",
        processor,
        Arc::new(AdmissionController::new(AdmissionLimits::default())),
    )
    .await;
    client
        .send_command(request.clone())
        .await
        .expect("send request through the canonical server");
    let response = server
        .receive_one_then_finish_and_collect(client)
        .await
        .into_iter()
        .next()
        .expect("canonical server should return a response");
    Ok(Some(response))
}

async fn send_message_through_broker_processor(
    processor: &DefaultServerProcessor,
    topic: CheetahString,
    body: Bytes,
) -> SendMessageResponseHeader {
    let send_header = SendMessageRequestHeader {
        producer_group: CheetahString::from_static_str("request-code-phase-producer"),
        topic,
        default_topic: CheetahString::from_static_str("TBW102"),
        default_topic_queue_nums: 1,
        queue_id: 0,
        sys_flag: 0,
        born_timestamp: current_millis() as i64,
        flag: 0,
        properties: None,
        reconsume_times: None,
        unit_mode: Some(false),
        batch: Some(false),
        max_reconsume_times: None,
        topic_request_header: None,
    };
    let mut request = RemotingCommand::create_request_command(RequestCode::SendMessage, send_header).set_body(body);
    request.make_custom_header_to_net();

    let response = process_broker_request(processor, &mut request).await;
    assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
    response
        .decode_command_custom_header::<SendMessageResponseHeader>()
        .expect("send response should include SendMessageResponseHeader")
}

fn lite_test_root(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rocketmq-rust-broker-lite-manager-{label}-{}",
        current_millis()
    ))
}

#[test]
fn registration_permission_mask_does_not_mutate_stored_topic_generation() {
    let temp_root = lite_test_root("registration-permission-mask");
    let broker_config = Arc::new(BrokerConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        broker_permission: PermName::PERM_READ,
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        ..MessageStoreConfig::default()
    });
    let runtime = BrokerRuntime::new(broker_config, message_store_config);
    let stored = Arc::new(TopicConfig::with_perm(
        "PermissionTopic",
        1,
        1,
        PermName::PERM_READ | PermName::PERM_WRITE,
    ));

    let masked = runtime
        .composition
        .state
        .build_registration_runtime()
        .topic_config_for_registration(stored.as_ref());

    assert_eq!(stored.perm, PermName::PERM_READ | PermName::PERM_WRITE);
    assert_eq!(masked.perm, PermName::PERM_READ);
    let _ = std::fs::remove_dir_all(temp_root);
}

async fn new_lite_test_runtime(label: &str) -> BrokerRuntime {
    let temp_root = lite_test_root(label);
    let broker_config = Arc::new(BrokerConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        auth_config_path: temp_root.join("auth.json").to_string_lossy().into_owned().into(),
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        enable_lmq: true,
        enable_multi_dispatch: true,
        max_lmq_consume_queue_num: 32,
        read_uncommitted: true,
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new_with_service_context(
        broker_config,
        message_store_config,
        crate::test_service_context(format!("broker-runtime.{label}")),
    );
    assert!(runtime.initialize().await.is_ok());
    runtime
}

async fn new_phase3_test_runtime(label: &str) -> BrokerRuntime {
    let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-{label}-{}", current_millis()));
    let broker_config = Arc::new(BrokerConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        auth_config_path: temp_root.join("auth.json").to_string_lossy().into_owned().into(),
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        flush_disk_type: FlushDiskType::AsyncFlush,
        ha_listen_port: allocate_broker_runtime_test_port() as usize,
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    assert!(runtime.initialize().await.is_ok());
    runtime
}

async fn new_phase3_lifecycle_test_runtime(label: &str) -> BrokerRuntime {
    let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-{label}-{}", current_millis()));
    let listen_port = allocate_broker_runtime_test_port();
    let broker_config = Arc::new(BrokerConfig {
        broker_server_config: ServerConfig {
            listen_port: listen_port as u32,
            bind_address: "127.0.0.1".to_owned(),
            ..ServerConfig::default()
        },
        broker_ip1: CheetahString::from_static_str("127.0.0.1"),
        listen_port: listen_port as u32,
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        auth_config_path: temp_root.join("auth.json").to_string_lossy().into_owned().into(),
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        flush_disk_type: FlushDiskType::AsyncFlush,
        ha_listen_port: allocate_broker_runtime_test_port() as usize,
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new_with_service_context(
        broker_config,
        message_store_config,
        crate::test_service_context(format!("broker-runtime.{label}")),
    );
    assert!(runtime.initialize().await.is_ok());
    runtime
}

async fn assert_deferred_lifecycle_shutdown(runtime: &mut BrokerRuntime) {
    let deferred = runtime
        .composition
        .data_plane
        .deferred
        .as_ref()
        .expect("request pipeline should retain the Broker deferred lifecycle");
    deferred.seal();
    let producer_report = deferred
        .producer_task_group()
        .expect("deferred producer task group should be retained")
        .shutdown(Duration::from_secs(1))
        .await;
    assert!(producer_report.is_healthy(), "{}", producer_report.to_json());

    let initial = deferred.shutdown();
    let terminal = deferred.shutdown();
    let registry_report = crate::broker_runtime::deferred::BrokerDeferredRegistryShutdownReport::new(initial, terminal);
    assert!(registry_report.is_healthy(), "{registry_report:?}");

    let terminal = deferred.resource_snapshot();
    assert_eq!(terminal.producer_task_count, 0, "{terminal:?}");
    assert_eq!(terminal.shared_admission_current_count, 0, "{terminal:?}");
    assert_eq!(terminal.shared_admission_current_bytes, 0, "{terminal:?}");
    assert!(terminal.is_zero(), "{terminal:?}");
    assert!(terminal.handoff_zero, "{terminal:?}");
}

#[tokio::test]
async fn registering_message_store_hooks_does_not_retain_runtime_root() {
    let mut runtime = new_phase3_test_runtime("schedule-hook-ownership").await;
    let store_strong_count_before = Arc::strong_count(
        runtime
            .composition
            .state
            .message_store
            .as_ref()
            .expect("message store should be initialized"),
    );

    runtime.register_message_store_hook();

    assert_eq!(
        Arc::strong_count(
            runtime
                .composition
                .state
                .message_store
                .as_ref()
                .expect("message store should remain initialized"),
        ),
        store_strong_count_before
    );
    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[test]
fn message_arriving_listener_does_not_retain_runtime_root() {
    let production_source = include_str!("../../src/long_polling/notify_message_arriving_listener.rs")
        .split("#[cfg(test)]")
        .next()
        .expect("message-arriving listener production source should precede its tests");

    assert!(!production_source.contains(concat!("Broker", "RuntimeInner")));
    assert!(!production_source.contains(concat!("Arc", "Mut")));
}

fn allocate_broker_runtime_test_port() -> u16 {
    loop {
        let Ok(listener) = TcpListener::bind(("127.0.0.1", 0)) else {
            continue;
        };
        let Ok(addr) = listener.local_addr() else {
            continue;
        };
        let port = addr.port();
        if port <= 1024 + 2 {
            continue;
        }
        if TcpListener::bind(("127.0.0.1", port - 2)).is_ok() {
            return port;
        }
    }
}

#[cfg(feature = "tieredstore")]
fn tieredstore_message_store_config(root: &Path) -> MessageStoreConfig {
    serde_json::from_value(serde_json::json!({
        "storePathRootDir": root.to_string_lossy(),
        "duplicationEnable": true,
        "flushDiskType": "ASYNC_FLUSH",
        "readUncommitted": true,
        "timerWheelEnable": false,
        "tieredStoreConfig": {
            "storageLevel": "force",
            "backendProvider": "memory",
            "metadataProvider": "json",
            "storePathRootDir": root.join("tieredstore").to_string_lossy(),
            "maxPendingTasks": 16
        }
    }))
    .expect("deserialize tieredstore-enabled message store config")
}

#[cfg(feature = "tieredstore")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broker_runtime_tieredstore_start_shutdown_e2e() {
    let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-broker-tiered-e2e-{}", current_millis()));
    let listen_port = allocate_broker_runtime_test_port();
    let broker_config = Arc::new(BrokerConfig {
        broker_server_config: ServerConfig {
            listen_port: listen_port as u32,
            bind_address: "127.0.0.1".to_owned(),
            ..ServerConfig::default()
        },
        broker_ip1: CheetahString::from_static_str("127.0.0.1"),
        listen_port: listen_port as u32,
        namesrv_addr: None,
        skip_pre_online: true,
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        auth_config_path: temp_root.join("auth.json").to_string_lossy().into_owned().into(),
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(tieredstore_message_store_config(&temp_root));
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);

    let initialized = tokio::time::timeout(Duration::from_secs(15), runtime.initialize())
        .await
        .expect("tieredstore broker initialize should not hang");
    initialized.expect("tieredstore broker initialize should succeed");
    assert!(
        runtime.runtime_state_mut().message_store().is_some(),
        "message store should be initialized before broker start"
    );

    tokio::time::timeout(Duration::from_secs(15), runtime.start())
        .await
        .expect("tieredstore broker start should not hang")
        .expect("tieredstore broker start should succeed");
    tokio::time::timeout(Duration::from_secs(15), runtime.shutdown())
        .await
        .expect("tieredstore broker shutdown should not hang");

    let _ = std::fs::remove_dir_all(temp_root);
}

fn seed_lite_query_state(runtime: &mut BrokerRuntime) {
    let inner = runtime.runtime_state_mut();
    let mut topic_config = TopicConfig::with_queues("parent-topic", 1, 1);
    topic_config.attributes.insert(
        CheetahString::from_string(format!(
            "+{}",
            topic_attributes::TopicAttributes::topic_message_type_attribute().name()
        )),
        CheetahString::from_static_str("LITE"),
    );
    inner.topic_config_manager().update_topic_config(topic_config, 0);

    for group in ["group-a", "group-b"] {
        let mut config = SubscriptionGroupConfig::new(CheetahString::from_static_str(group));
        config.set_attributes(HashMap::from([(
            CheetahString::from_string(format!("+{LITE_BIND_TOPIC_ATTRIBUTE_NAME}")),
            CheetahString::from_static_str("parent-topic"),
        )]));
        inner
            .subscription_group_manager_mut()
            .update_subscription_group_config(&mut config);
    }

    let registry = inner.lite_subscription_registry();
    let client_one = CheetahString::from_static_str("client-1");
    let client_two = CheetahString::from_static_str("client-2");
    let parent_topic = CheetahString::from_static_str("parent-topic");
    let group_a = CheetahString::from_static_str("group-a");
    let group_b = CheetahString::from_static_str("group-b");

    registry.add_complete_subscription(
        &client_one,
        &group_a,
        &parent_topic,
        &HashSet::from([
            CheetahString::from_string(to_lmq_name("parent-topic", "child-a").expect("child-a lmq")),
            CheetahString::from_string(to_lmq_name("parent-topic", "child-b").expect("child-b lmq")),
        ]),
        1,
        false,
    );
    registry.add_complete_subscription(
        &client_two,
        &group_b,
        &parent_topic,
        &HashSet::from([CheetahString::from_string(
            to_lmq_name("parent-topic", "child-b").expect("child-b lmq"),
        )]),
        1,
        false,
    );
}

fn seed_lmq_offsets(runtime: &mut BrokerRuntime, offsets: &[(&str, i64)]) {
    let mut topic_queue_table = HashMap::new();
    for (lite_topic, offset) in offsets {
        let lmq_name = to_lmq_name("parent-topic", lite_topic).expect("lmq name");
        topic_queue_table.insert(CheetahString::from_string(format!("{lmq_name}-0")), *offset);
    }
    runtime.with_message_store_mut_for_test(|message_store| {
        message_store
            .consume_queue_store_mut()
            .set_topic_queue_table(topic_queue_table);
    });
}

fn set_parent_topic_message_type(runtime: &mut BrokerRuntime, message_type: &str) {
    let topic_config = runtime
        .runtime_state_mut()
        .topic_config_manager()
        .select_topic_config(&CheetahString::from_static_str("parent-topic"))
        .expect("parent topic should exist");
    let mut replacement = topic_config.as_ref().clone();
    replacement.attributes = HashMap::from([(
        CheetahString::from_string(format!(
            "+{}",
            topic_attributes::TopicAttributes::topic_message_type_attribute().name()
        )),
        CheetahString::from_string(message_type.to_string()),
    )]);
    runtime
        .runtime_state_mut()
        .topic_config_manager()
        .update_topic_config(replacement, 0);
}

fn set_parent_topic_lite_expiration(runtime: &mut BrokerRuntime, expiration: i32) {
    let topic_config = runtime
        .runtime_state_mut()
        .topic_config_manager()
        .select_topic_config(&CheetahString::from_static_str("parent-topic"))
        .expect("parent topic should exist");
    let mut replacement = topic_config.as_ref().clone();
    replacement.attributes = HashMap::from([(
        CheetahString::from_string(format!(
            "+{}",
            topic_attributes::TopicAttributes::lite_topic_expiration_attribute().name()
        )),
        CheetahString::from_string(expiration.to_string()),
    )]);
    runtime
        .runtime_state_mut()
        .topic_config_manager()
        .update_topic_config(replacement, 0);
}

fn seed_lite_bound_group(runtime: &mut BrokerRuntime, group: &str) {
    let inner = runtime.runtime_state_mut();
    let mut config = SubscriptionGroupConfig::new(CheetahString::from(group));
    config.set_attributes(HashMap::from([(
        CheetahString::from_string(format!("+{LITE_BIND_TOPIC_ATTRIBUTE_NAME}")),
        CheetahString::from_static_str("parent-topic"),
    )]));
    inner
        .subscription_group_manager_mut()
        .update_subscription_group_config(&mut config);
}

fn seed_lite_topic_publish_route(runtime: &mut BrokerRuntime, broker_names: &[CheetahString]) {
    let publish_info = crate::topic::route::BrokerPublishRoute::from_queues(
        broker_names
            .iter()
            .enumerate()
            .map(|(queue_id, broker_name)| {
                MessageQueue::from_parts("parent-topic", broker_name.clone(), queue_id as i32)
            })
            .collect(),
    );
    runtime
        .runtime_state_mut()
        .topic_route_info_manager()
        .topic_publish_info_table
        .insert(CheetahString::from_static_str("parent-topic"), publish_info);
}

fn seed_lmq_consumer_offset(runtime: &mut BrokerRuntime, group: &str, lite_topic: &str, offset: i64) {
    let inner = runtime.runtime_state_mut();
    let lmq_name = CheetahString::from_string(to_lmq_name("parent-topic", lite_topic).expect("lmq name"));
    inner.consumer_offset_manager().commit_offset(
        CheetahString::from_static_str("127.0.0.1"),
        &CheetahString::from(group),
        &lmq_name,
        0,
        offset,
    );
}

async fn seed_lmq_message(runtime: &mut BrokerRuntime, lite_topic: &str, body: &'static [u8]) -> i64 {
    let lmq_name = CheetahString::from_string(to_lmq_name("parent-topic", lite_topic).expect("lmq name"));
    let mut message = MessageExtBrokerInner::default();
    message.set_topic(CheetahString::from_static_str("parent-topic"));
    message.message_ext_inner.set_queue_id(0);
    message.set_body(Bytes::from_static(body));
    message.put_property(
        CheetahString::from_static_str(MessageConst::PROPERTY_INNER_MULTI_DISPATCH),
        lmq_name,
    );

    let append = runtime
        .composition
        .data_plane
        .escape_bridge_owner
        .store_capability()
        .append_message(message)
        .await;
    let append = append.expect("seed lmq message should reach the Store");
    assert!(append.result().is_ok(), "seed lmq message should succeed");
    let wrote_offset = append
        .result()
        .append_message_result()
        .expect("seed message should expose append result")
        .wrote_offset;
    runtime.reput_message_store_once_for_test().await;
    wrote_offset
}

async fn start_namesrv(port: u16, root: &Path) -> TestNameServer {
    let namesrv_root = root.join(format!("namesrv-{port}"));
    std::fs::create_dir_all(&namesrv_root).expect("create namesrv test root");
    let namesrv_config = NamesrvConfig {
        rocketmq_home: root.to_string_lossy().into_owned(),
        kv_config_path: namesrv_root.join("kvConfig.json").to_string_lossy().into_owned(),
        config_store_path: namesrv_root.join("namesrv.properties").to_string_lossy().into_owned(),
        ..NamesrvConfig::default()
    };
    let server_config = ServerConfig {
        bind_address: "127.0.0.1".to_string(),
        listen_port: port as u32,
        ..ServerConfig::default()
    };
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let namesrv_context = RuntimeContext::from_current("broker-namesrv-test").service_context("namesrv");
    let handle = tokio::spawn(async move {
        NameServerBuilder::new(namesrv_context, rocketmq_observability::TelemetryHandle::noop())
            .set_name_server_config(namesrv_config)
            .set_server_config(server_config)
            .build()
            .boot_with_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("boot namesrv");
    });

    wait_until(
        Duration::from_secs(10),
        || std::net::TcpStream::connect(("127.0.0.1", port)).is_ok(),
        "namesrv to listen on its remoting port",
    )
    .await;

    TestNameServer {
        addr: CheetahString::from_string(format!("127.0.0.1:{port}")),
        shutdown_tx: Some(shutdown_tx),
        handle: Some(handle),
    }
}

async fn configure_namesrv(runtime: &mut BrokerRuntime, namesrv_addr: &CheetahString) {
    let mut broker_config = runtime.composition.state.broker_config().as_ref().clone();
    broker_config.namesrv_addr = Some(namesrv_addr.clone());
    runtime
        .composition
        .state
        .set_broker_config(broker_config)
        .expect("nameserver test configuration should remain valid");
    runtime
        .composition
        .state
        .broker_outer_api
        .update_name_server_address_list(namesrv_addr.clone())
        .await;
}

async fn sync_namesrv_member_group(
    namesrv_addr: &CheetahString,
    cluster_name: &CheetahString,
    broker_name: &CheetahString,
) -> BrokerMemberGroup {
    let client = Arc::new(
        TransportClient::builder(
            Arc::new(TransportClientConfig::default()),
            DefaultRequestProcessor,
            crate::test_service_context("namesrv-client"),
        )
        .build()
        .expect("valid transport client configuration"),
    );
    client.start().await.expect("start transport client");
    let request_header = GetBrokerMemberGroupRequestHeader::new(cluster_name.clone(), broker_name.clone());
    let request = RemotingCommand::create_request_command(RequestCode::GetBrokerMemberGroup, request_header);
    let mut response = match client.invoke_request(Some(namesrv_addr), request, 3000).await {
        Ok(OutboundRequestOutcome::Response(response)) => response,
        Ok(OutboundRequestOutcome::Rejected(rejection)) => {
            panic!("broker member-group request was rejected: {rejection:?}")
        }
        Ok(OutboundRequestOutcome::Contract(contract)) => {
            panic!("broker member-group request contract failed: {contract:?}")
        }
        Err(error) => panic!("broker member-group request failed: {error:?}"),
    };
    assert_eq!(
        ResponseCode::from(response.code()),
        ResponseCode::Success,
        "namesrv should accept GetBrokerMemberGroup requests"
    );
    let response_body = GetBrokerMemberGroupResponseBody::decode(
        response
            .take_body()
            .expect("GetBrokerMemberGroup response should contain a body")
            .as_ref(),
    )
    .expect("decode GetBrokerMemberGroup response body");
    client.shutdown();
    response_body
        .broker_member_group
        .expect("namesrv should return broker member group body")
}

async fn wait_for_namesrv_member_group<F>(
    namesrv_addr: &CheetahString,
    cluster_name: &CheetahString,
    broker_name: &CheetahString,
    timeout: Duration,
    context: &str,
    mut predicate: F,
) -> BrokerMemberGroup
where
    F: FnMut(&BrokerMemberGroup) -> bool,
{
    let start = std::time::Instant::now();
    loop {
        let member_group = sync_namesrv_member_group(namesrv_addr, cluster_name, broker_name).await;
        if predicate(&member_group) {
            return member_group;
        }
        if start.elapsed() >= timeout {
            panic!("Timed out waiting for {context}, last member group: {:?}", member_group);
        }
        sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_until<F>(timeout: Duration, mut predicate: F, context: &str)
where
    F: FnMut() -> bool,
{
    let start = std::time::Instant::now();
    loop {
        if predicate() {
            return;
        }
        if start.elapsed() >= timeout {
            panic!("Timed out waiting for {context}");
        }
        sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_controller_learner_to_reach_committed_frontier(
    manager: &TestControllerManager,
    node_id: u64,
    timeout: Duration,
) -> u64 {
    let start = std::time::Instant::now();
    loop {
        let membership = manager
            .raft()
            .consensus_membership()
            .await
            .expect("read controller membership while waiting for learner replication");
        if membership.caught_up().contains(&node_id) {
            return membership.version();
        }
        if start.elapsed() >= timeout {
            panic!(
                "Timed out waiting for controller learner {node_id} to reach the committed log frontier; \
                 membership_version={}, caught_up={:?}",
                membership.version(),
                membership.caught_up()
            );
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_tcp_listener(addr: std::net::SocketAddr, context: &str) {
    wait_until(
        Duration::from_secs(5),
        || std::net::TcpStream::connect(addr).is_ok(),
        context,
    )
    .await;
}

async fn new_test_controller_manager(
    controller_peer: RaftPeer,
    controller_peers: Vec<RaftPeer>,
    raft_peers: Vec<RaftPeer>,
    root: &Path,
) -> Arc<TestControllerManager> {
    let node_id = controller_peer.id;
    let config = TestControllerConfig::default()
        .with_node_info(node_id, controller_peer.addr)
        .with_controller_peers(controller_peers)
        .with_raft_peers(raft_peers)
        .with_storage_backend(StorageBackendType::Memory)
        .with_storage_path(
            root.join(format!("controller-{node_id}"))
                .to_string_lossy()
                .into_owned(),
        )
        .with_election_timeout_ms(800)
        .with_heartbeat_interval_ms(200);
    let config = config
        .with_enable_elect_unclean_master(true)
        .with_enable_elect_unclean_master_local(true);

    let manager = Arc::new(
        TestControllerManager::new(
            config,
            crate::test_service_context("controller-manager"),
            rocketmq_observability::TelemetryHandle::noop(),
        )
        .await
        .expect("create controller manager"),
    );
    assert!(
        manager.initialize().await.expect("initialize controller manager"),
        "controller manager should initialize exactly once"
    );
    manager.start().await.expect("start controller manager");
    wait_for_tcp_listener(controller_peer.addr, "controller remoting server to listen").await;
    manager
}

fn controller_membership_authorization() -> MaintenanceAuthorizationGrant {
    let policy = MaintenancePolicy {
        schema_version: MAINTENANCE_POLICY_SCHEMA_VERSION,
        policy_id: "broker-runtime.controller-membership".to_string(),
        policy_version: 1,
        require_authentication: true,
        require_authorization: true,
        require_fencing_token: true,
        max_request_lifetime_millis: 30_000,
        resource_budget: MaintenanceResourceBudget {
            max_checkpoint_bytes: 1_024,
            max_store_members: 3,
            max_concurrent_operations: 1,
        },
        principal_bindings: vec![MaintenancePrincipalBinding {
            principal: "broker-runtime-release-operator".to_string(),
            roles: BTreeSet::from([MaintenanceRole::ReleaseOperator]),
        }],
        role_grants: vec![MaintenanceRoleGrant {
            role: MaintenanceRole::ReleaseOperator,
            capabilities: BTreeSet::from([MaintenanceCapability::ReleaseCheckpoint]),
        }],
    };
    MaintenanceAuthorizer::new(policy.into_validated().expect("valid membership policy"))
        .authorize(
            Some(&MaintenanceAuthorizationContext {
                authentication_enabled: true,
                authorization_enabled: true,
                principal: Some("broker-runtime-release-operator".to_string()),
                request_class: MaintenanceRequestClass::PrivilegedMaintenance,
                capability: MaintenanceCapability::ReleaseCheckpoint,
                deadline_unix_millis: current_millis() + 30_000,
                fencing_token: Some(1),
            }),
            current_millis(),
        )
        .expect("controller membership authorization")
}

async fn start_controller_cluster(base_port: u16, root: &Path) -> (Vec<Arc<TestControllerManager>>, Vec<RaftPeer>) {
    let controller_peers = vec![
        RaftPeer {
            id: 1,
            addr: format!("127.0.0.1:{}", base_port + 1).parse().expect("controller addr"),
        },
        RaftPeer {
            id: 2,
            addr: format!("127.0.0.1:{}", base_port + 2).parse().expect("controller addr"),
        },
        RaftPeer {
            id: 3,
            addr: format!("127.0.0.1:{}", base_port + 3).parse().expect("controller addr"),
        },
    ];
    let raft_peers = vec![
        RaftPeer {
            id: 1,
            addr: format!("127.0.0.1:{}", base_port + 11).parse().expect("raft addr"),
        },
        RaftPeer {
            id: 2,
            addr: format!("127.0.0.1:{}", base_port + 12).parse().expect("raft addr"),
        },
        RaftPeer {
            id: 3,
            addr: format!("127.0.0.1:{}", base_port + 13).parse().expect("raft addr"),
        },
    ];

    let bootstrap_manager = new_test_controller_manager(
        controller_peers[0].clone(),
        controller_peers.clone(),
        raft_peers.clone(),
        root,
    )
    .await;
    let mut managers = vec![bootstrap_manager.clone()];

    let mut initial_cluster = BTreeMap::new();
    initial_cluster.insert(
        controller_peers[0].id,
        Node {
            node_id: controller_peers[0].id,
            rpc_addr: raft_peers[0].addr.to_string(),
        },
    );
    bootstrap_manager
        .raft()
        .initialize_cluster(initial_cluster)
        .await
        .expect("initialize single-node controller cluster");

    wait_until(
        Duration::from_secs(10),
        || bootstrap_manager.is_leader(),
        "controller node 1 to become leader",
    )
    .await;

    wait_until(
        Duration::from_secs(10),
        || bootstrap_manager.raft().has_committed_log().unwrap_or(false),
        "controller leader to commit its first log entry",
    )
    .await;

    bootstrap_manager
        .controller()
        .apply_broker_id(&ApplyBrokerIdRequestHeader {
            cluster_name: CheetahString::from_static_str("bootstrap-cluster"),
            broker_name: CheetahString::from_static_str("bootstrap-broker"),
            applied_broker_id: 0,
            register_check_code: CheetahString::from_static_str("127.0.0.1:0;bootstrap"),
        })
        .await
        .expect("commit bootstrap controller write")
        .expect("bootstrap controller write response");

    for (controller_peer, raft_peer) in controller_peers.iter().zip(raft_peers.iter()).skip(1) {
        managers.push(
            new_test_controller_manager(
                controller_peer.clone(),
                controller_peers.clone(),
                vec![raft_peer.clone()],
                root,
            )
            .await,
        );
        let learner_manager = managers.last().expect("new learner controller manager");
        learner_manager
            .set_raft_runtime_heartbeat_enabled(false)
            .expect("disable learner heartbeat during bootstrap");
        learner_manager
            .set_raft_runtime_elect_enabled(false)
            .expect("disable learner election during bootstrap");
        learner_manager
            .set_raft_runtime_tick_enabled(false)
            .expect("disable learner tick during bootstrap");
        let membership = bootstrap_manager
            .raft()
            .consensus_membership()
            .await
            .expect("read controller membership before adding learner");
        let request = MembershipChangeRequest::new(
            format!("broker-runtime-add-controller-learner-{}", controller_peer.id),
            membership.version(),
            MembershipChange::AddLearner {
                node: ConsensusNode::new(controller_peer.id, raft_peer.addr.to_string())
                    .expect("valid controller learner"),
            },
            "form broker runtime Controller test cluster",
        )
        .expect("valid add-learner request");
        bootstrap_manager
            .raft()
            .apply_membership_change(&controller_membership_authorization(), request)
            .await
            .expect("add controller learner");
    }

    for controller_peer in controller_peers.iter().skip(1) {
        let membership_version = wait_for_controller_learner_to_reach_committed_frontier(
            bootstrap_manager.as_ref(),
            controller_peer.id,
            Duration::from_secs(20),
        )
        .await;
        let request = MembershipChangeRequest::new(
            format!("broker-runtime-promote-controller-voter-{}", controller_peer.id),
            membership_version,
            MembershipChange::PromoteVoter {
                node_id: controller_peer.id,
            },
            "form broker runtime Controller voting set",
        )
        .expect("valid promote-voter request");
        match tokio::time::timeout(
            Duration::from_secs(20),
            bootstrap_manager
                .raft()
                .apply_membership_change(&controller_membership_authorization(), request),
        )
        .await
        {
            Ok(result) => {
                result.expect("promote controller learner to voter");
            }
            Err(_) => {
                panic!(
                    "Timed out promoting controller learner {}; states={:?}",
                    controller_peer.id,
                    managers
                        .iter()
                        .map(|manager| (manager.is_leader(), manager.raft().has_committed_log().unwrap_or(false)))
                        .collect::<Vec<_>>()
                );
            }
        }
    }

    wait_until(
        Duration::from_secs(15),
        || {
            managers.iter().filter(|manager| manager.is_leader()).count() == 1
                && managers
                    .iter()
                    .all(|manager| manager.raft().has_committed_log().unwrap_or(false))
        },
        "controller cluster to replicate voter membership and elect a single leader",
    )
    .await;

    for manager in managers.iter().skip(1) {
        manager
            .set_raft_runtime_tick_enabled(true)
            .expect("re-enable learner tick after bootstrap");
        manager
            .set_raft_runtime_heartbeat_enabled(true)
            .expect("re-enable learner heartbeat after bootstrap");
        manager
            .set_raft_runtime_elect_enabled(true)
            .expect("re-enable learner election after bootstrap");
    }

    wait_until(
        Duration::from_secs(15),
        || managers.iter().filter(|manager| manager.is_leader()).count() == 1,
        "controller cluster to elect a single leader",
    )
    .await;

    (managers, controller_peers)
}

fn new_controller_mode_runtime(
    root: &Path,
    broker_name: &str,
    listen_port: u16,
    ha_listen_port: u16,
    controller_addrs: CheetahString,
) -> BrokerRuntime {
    new_controller_mode_runtime_with_store_key(
        root,
        &format!("broker-{listen_port}"),
        broker_name,
        listen_port,
        ha_listen_port,
        controller_addrs,
    )
}

fn new_controller_mode_runtime_with_store_key(
    root: &Path,
    store_key: &str,
    broker_name: &str,
    listen_port: u16,
    ha_listen_port: u16,
    controller_addrs: CheetahString,
) -> BrokerRuntime {
    let store_root = root.join(store_key);
    std::fs::create_dir_all(&store_root).expect("create broker store root");

    let broker_config = Arc::new(BrokerConfig {
        broker_identity: rocketmq_model::common::broker::broker_identity::BrokerIdentity {
            broker_name: CheetahString::from_string(broker_name.to_owned()),
            broker_cluster_name: CheetahString::from_static_str("controller-test-cluster"),
            broker_id: mix_all::MASTER_ID,
            is_broker_container: false,
            is_in_broker_container: false,
        },
        broker_server_config: ServerConfig {
            listen_port: listen_port as u32,
            ..ServerConfig::default()
        },
        broker_ip1: CheetahString::from_static_str("127.0.0.1"),
        broker_ip2: Some(CheetahString::from_static_str("127.0.0.1")),
        listen_port: listen_port as u32,
        enable_controller_mode: true,
        controller_addr: controller_addrs,
        sync_broker_metadata_period: 500,
        sync_controller_metadata_period: 500,
        broker_heartbeat_interval: 500,
        send_heartbeat_timeout_millis: 1000,
        controller_heartbeat_timeout_mills: 2000,
        broker_election_priority: 1,
        namesrv_addr: None,
        store_path_root_dir: store_root.to_string_lossy().into_owned().into(),
        auth_config_path: store_root.join("auth.json").to_string_lossy().into_owned().into(),
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: store_root.to_string_lossy().into_owned().into(),
        ha_listen_port: ha_listen_port as usize,
        broker_role: BrokerRole::Slave,
        total_replicas: 2,
        in_sync_replicas: 2,
        min_in_sync_replicas: 1,
        all_ack_in_sync_state_set: true,
        enable_controller_mode: true,
        ..MessageStoreConfig::default()
    });

    BrokerRuntime::new(broker_config, message_store_config)
}

async fn bootstrap_broker_against_controller(
    runtime: &mut BrokerRuntime,
    controller_leader_manager: &Arc<TestControllerManager>,
) {
    let controller_runtime = runtime.composition.state.build_controller_runtime();
    let controller_leader = controller_runtime
        .discover_controller_leader()
        .await
        .expect("discover controller leader");
    let cluster_name = runtime
        .composition
        .state
        .broker_config()
        .broker_identity
        .broker_cluster_name
        .clone();
    let broker_name = runtime
        .composition
        .state
        .broker_config()
        .broker_identity
        .broker_name
        .clone();
    let broker_addr = runtime.composition.state.get_broker_addr().clone();
    let controller_broker_id = controller_runtime
        .ensure_controller_broker_id(&controller_leader)
        .await
        .expect("ensure controller broker id");

    let (register_header, sync_state_set) = runtime
        .composition
        .state
        .broker_outer_api
        .register_broker_to_controller(
            cluster_name.clone(),
            broker_name.clone(),
            controller_broker_id as i64,
            broker_addr,
            &controller_leader,
        )
        .await
        .expect("register broker to controller");
    runtime
        .composition
        .state
        .controller_state
        .with_replicas_mut(ReplicasManager::mark_registered);

    let sync_state_set = sync_state_set.unwrap_or_default();
    if register_header.master_broker_id.is_some() && register_header.master_epoch.is_some() {
        controller_runtime
            .apply_controller_role_change(
                Some(controller_leader),
                register_header.master_broker_id.and_then(|id| u64::try_from(id).ok()),
                register_header.master_address,
                register_header.master_epoch,
                register_header.sync_state_set_epoch,
                sync_state_set,
            )
            .await
            .expect("apply controller register result");
        return;
    }

    controller_runtime
        .send_heartbeat_to_controller_leader(&controller_leader)
        .await
        .expect("send bootstrap heartbeat to controller");
    sleep(Duration::from_millis(300)).await;

    let (pre_elect_header, pre_elect_body) = runtime
        .composition
        .state
        .broker_outer_api
        .get_replica_info(
            &controller_leader,
            runtime
                .composition
                .state
                .broker_config()
                .broker_identity
                .broker_name
                .clone(),
        )
        .await
        .expect("query replica info before elect");
    if pre_elect_header.master_broker_id.is_some_and(|master_broker_id| {
        controller_leader_manager.heartbeat_manager().is_broker_active(
            cluster_name.as_str(),
            broker_name.as_str(),
            master_broker_id,
        )
    }) {
        let applied = controller_runtime
            .apply_controller_replica_info(
                controller_leader,
                pre_elect_header.master_broker_id.and_then(|id| u64::try_from(id).ok()),
                pre_elect_header.master_address.map(CheetahString::from_string),
                pre_elect_header.master_epoch,
                Some(pre_elect_body.get_sync_state_set_epoch()),
                pre_elect_body.get_sync_state_set().cloned().unwrap_or_default(),
            )
            .await
            .expect("controller replica preflight should remain operational");
        assert!(applied, "apply controller replica info before elect should succeed");
        return;
    }

    let (elect_header, sync_state_set) = runtime
        .composition
        .state
        .broker_outer_api
        .broker_elect(
            &controller_leader,
            cluster_name.clone(),
            broker_name.clone(),
            controller_broker_id as i64,
        )
        .await
        .unwrap_or_else(|error| {
            let pre_elect_master_id = pre_elect_header.master_broker_id.unwrap_or_default();
            let pre_elect_master_active = pre_elect_header.master_broker_id.is_some_and(|master_broker_id| {
                controller_leader_manager.heartbeat_manager().is_broker_active(
                    cluster_name.as_str(),
                    broker_name.as_str(),
                    master_broker_id,
                )
            });
            panic!(
                "controller elect should succeed, got error={}, pre_elect_master={:?}, pre_elect_epoch={:?}, \
                 pre_elect_sync_state={:?}, register_master={:?}, register_epoch={:?}, pre_elect_master_active={}, \
                 queried_master_id={}",
                error,
                pre_elect_header.master_broker_id,
                pre_elect_header.master_epoch,
                pre_elect_body.get_sync_state_set(),
                register_header.master_broker_id,
                register_header.master_epoch,
                pre_elect_master_active,
                pre_elect_master_id
            )
        });
    controller_runtime
        .apply_controller_role_change(
            Some(controller_leader),
            elect_header.master_broker_id.and_then(|id| u64::try_from(id).ok()),
            elect_header.master_address,
            elect_header.master_epoch,
            elect_header.sync_state_set_epoch,
            sync_state_set,
        )
        .await
        .expect("apply controller elect result");
}

async fn shutdown_controller_cluster(controllers: &[Arc<TestControllerManager>]) {
    // The full llvm-cov workspace job runs hundreds of instrumented Broker tests
    // concurrently. Keep the production 30-second Controller default unchanged,
    // but give this synthetic three-node cluster enough time to drain under that
    // test-only scheduler pressure. LLVM instrumentation can make the concurrent
    // three-node drain exceed one minute even when every shutdown phase progresses.
    let results = futures::future::join_all(
        controllers
            .iter()
            .map(|controller| controller.shutdown_until(ShutdownDeadline::after(Duration::from_secs(120)))),
    )
    .await;
    for result in results {
        result.expect("shutdown controller manager");
    }
}

async fn initialize_controller_mode_broker(runtime: &mut BrokerRuntime, broker_label: &str) {
    assert!(
        runtime.initialize_metadata().await.is_ok(),
        "{broker_label} metadata init should succeed"
    );
    assert!(
        runtime.initialize_message_store().await,
        "{broker_label} message store init should succeed"
    );
    runtime.composition.state.initialize_controller_mode();
    runtime.register_message_store_hook();
    assert!(
        runtime.load_message_store_for_test().await,
        "{broker_label} message store load should succeed"
    );
    assert!(
        runtime
            .composition
            .state
            .schedule_message_service
            .as_mut()
            .expect("controller mode schedule service")
            .load(),
        "{broker_label} schedule service load should succeed"
    );
    runtime.initialize_resources();
    runtime.initialize_scheduled_tasks().await;
    assert!(
        runtime.initial_transaction().await,
        "{broker_label} transaction init should succeed"
    );
    assert!(runtime.initial_acl().await, "{broker_label} acl init should succeed");
    assert!(
        runtime.initial_rpc_hooks(),
        "{broker_label} rpc hooks should initialize"
    );
}

async fn shutdown_controller_mode_broker_for_rejoin(runtime: &mut BrokerRuntime, broker_label: &str) {
    let report = runtime
        .shutdown_basic_service_until(ShutdownDeadline::after(Duration::from_secs(120)))
        .await;
    assert!(
        report.message_store.present && report.message_store.healthy,
        "{broker_label} message store should shut down before same-path rejoin: {report:?}"
    );
    assert!(
        runtime.composition.state.message_store().is_none(),
        "{broker_label} should release its message-store owner before same-path rejoin"
    );
}

#[tokio::test]
async fn timer_message_store_returns_configured_store() {
    let broker_config = Arc::new(BrokerConfig::default());
    let message_store_config = Arc::new(MessageStoreConfig::default());
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    runtime
        .composition
        .state
        .set_timer_message_store(TimerMessageStore::new_empty(crate::test_service_context("timer-store")));

    let timer_store = runtime
        .composition
        .state
        .timer_message_store()
        .expect("timer store should be present")
        .as_ref();
    let configured_store = runtime
        .composition
        .state
        .timer_message_store()
        .expect("timer store should be present")
        .as_ref();

    assert!(std::ptr::eq(timer_store, configured_store));
}

#[tokio::test]
async fn initialize_message_store_reuses_store_owned_timer_message_store() {
    let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-broker-runtime-timer-{}", current_millis()));
    let broker_config = Arc::new(BrokerConfig::default());
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    assert!(runtime.initialize_metadata().await.is_ok());
    assert!(runtime.initialize_message_store().await);

    let store_timer = runtime
        .composition
        .state
        .message_store()
        .expect("message store should be initialized")
        .get_timer_message_store()
        .cloned()
        .expect("store should own timer store");
    let runtime_timer = runtime
        .composition
        .state
        .timer_message_store()
        .cloned()
        .expect("runtime should expose timer store");

    assert!(Arc::ptr_eq(&store_timer, &runtime_timer));

    let _ = std::fs::remove_dir_all(temp_root);
}

#[tokio::test]
async fn escape_bridge_provider_does_not_retain_message_store_root() {
    let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-broker-fast-failure-{}", current_millis()));
    let broker_config = Arc::new(BrokerConfig::default());
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    assert!(runtime.initialize_metadata().await.is_ok());
    assert!(runtime.initialize_message_store().await);

    let capability = runtime.composition.data_plane.escape_bridge_owner.store_capability();
    let message_store = runtime
        .composition
        .state
        .message_store
        .as_ref()
        .expect("message store should be initialized");
    assert_eq!(
        Arc::strong_count(message_store),
        1,
        "BrokerRuntime must be the only long-lived Store lifecycle owner"
    );
    assert!(capability.with_store(|_| ()).is_ok());

    let message_store = runtime
        .composition
        .state
        .message_store
        .take()
        .expect("message store should remain initialized");
    let mut message_store = match Arc::try_unwrap(message_store) {
        Ok(owner) => owner,
        Err(_) => panic!("released provider must leave one exclusive Store owner"),
    };
    message_store.shutdown().await;
    drop(message_store);
    assert!(
        capability.with_store(|_| ()).is_err(),
        "the weak provider must fail closed after the lifecycle owner is released"
    );
    assert!(capability
        .append_message(MessageExtBrokerInner::default())
        .await
        .is_err());
    assert!(capability.append_batch(MessageExtBatch::default()).await.is_err());
    assert!(capability.put_message(MessageExtBrokerInner::default()).await.is_err());
    let _ = std::fs::remove_dir_all(temp_root);
}

#[tokio::test]
async fn broker_shutdown_waits_for_admitted_store_reads_before_lifecycle_access() {
    let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-broker-store-exclusive-{}", current_millis()));
    let broker_config = Arc::new(BrokerConfig::default());
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    assert!(runtime.initialize_metadata().await.is_ok());
    assert!(runtime.initialize_message_store().await);

    let capability = runtime.composition.data_plane.escape_bridge_owner.store_capability();
    let admitted_read = capability.read_lease().expect("read admitted before detach");
    runtime
        .composition
        .data_plane
        .escape_bridge_owner
        .detach_message_store();

    assert!(
        capability.with_store(|_| ()).is_err(),
        "detached provider must reject new Store operations"
    );
    assert!(
        runtime.composition.state.message_store_mut().is_none(),
        "an admitted request lease must prevent exclusive lifecycle access"
    );

    let release_read = tokio::spawn(async move {
        tokio::task::yield_now().await;
        drop(admitted_read);
    });
    let report = runtime
        .shutdown_basic_service_until(ShutdownDeadline::after(Duration::from_secs(1)))
        .await;
    release_read.await.expect("release admitted Store read");
    assert!(
        !report.message_store.timed_out,
        "Store shutdown should continue after admitted reads drain"
    );
    let _ = std::fs::remove_dir_all(temp_root);
}

#[tokio::test]
async fn shared_append_port_preserves_single_and_batch_receipts_without_retaining_store() {
    let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-broker-shared-append-{}", current_millis()));
    let broker_config = Arc::new(BrokerConfig::default());
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    assert!(runtime.initialize_metadata().await.is_ok());
    assert!(runtime.initialize_message_store().await);

    let topic = CheetahString::from_static_str("shared-append-topic");
    let capability = runtime.composition.data_plane.escape_bridge_owner.store_capability();
    let single = capability
        .append_message(shared_append_test_message(
            &topic,
            Bytes::from_static(b"single-message"),
        ))
        .await
        .expect("single shared append should succeed");
    let batch = capability
        .append_batch(shared_append_test_batch(
            &topic,
            &[
                Bytes::from_static(b"batch-message-one"),
                Bytes::from_static(b"batch-message-two"),
            ],
        ))
        .await
        .expect("batch shared append should succeed");

    assert!(single.result().is_ok());
    assert!(batch.result().is_ok());
    assert_eq!(
        single
            .result()
            .append_message_result()
            .expect("single append result")
            .msg_num,
        1
    );
    assert_eq!(
        batch
            .result()
            .append_message_result()
            .expect("batch append result")
            .msg_num,
        2
    );
    let single_range = single
        .canonical()
        .expect("single canonical receipt")
        .appended_range()
        .expect("single appended range");
    let batch_range = batch
        .canonical()
        .expect("batch canonical receipt")
        .appended_range()
        .expect("batch appended range");
    assert!(single_range.end <= batch_range.start);
    assert!(single.appended_watermark() >= single_range.end);
    assert!(batch.appended_watermark() >= batch_range.end);
    assert!(single.durable_watermark() <= single.appended_watermark());
    assert!(batch.durable_watermark() <= batch.appended_watermark());

    let message_store = runtime
        .composition
        .state
        .message_store
        .as_ref()
        .expect("message store should remain initialized");
    assert_eq!(Arc::strong_count(message_store), 1);

    runtime.shutdown_message_store_for_test().await;
    let _ = std::fs::remove_dir_all(temp_root);
}

#[tokio::test]
async fn admin_runtime_does_not_retain_message_store_root() {
    let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-broker-admin-owner-{}", current_millis()));
    let broker_config = Arc::new(BrokerConfig::default());
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    assert!(runtime.initialize_metadata().await.is_ok());
    assert!(runtime.initialize_message_store().await);

    let store_owner = runtime
        .composition
        .state
        .message_store
        .as_ref()
        .expect("message store should be initialized");
    let store_owner_count = Arc::strong_count(store_owner);
    let admin = runtime.admin_runtime_for_test();
    let admin_clone = admin.clone();

    assert!(admin.message_store().is_some());
    assert!(admin_clone.message_store().is_some());
    assert_eq!(
        runtime
            .composition
            .state
            .message_store
            .as_ref()
            .map(Arc::strong_count)
            .expect("message store should remain initialized"),
        store_owner_count,
        "Admin runtime instances must retain only a weak Store provider"
    );
    assert!(admin.set_commitlog_read_mode(CommitLogReadMode::Normal).await.is_ok());
    assert_eq!(admin.delete_topics(Vec::new()).expect("empty topic deletion"), 0);
    assert_eq!(
        runtime
            .composition
            .state
            .message_store
            .as_ref()
            .map(Arc::strong_count)
            .expect("message store should remain initialized"),
        store_owner_count,
        "named Admin controls must release their temporary Store lease"
    );

    let message_store = runtime
        .composition
        .state
        .message_store
        .take()
        .expect("message store should remain initialized");
    let mut message_store = match Arc::try_unwrap(message_store) {
        Ok(owner) => owner,
        Err(_) => panic!("released provider must leave one exclusive Store owner"),
    };
    message_store.shutdown().await;
    drop(message_store);

    assert!(admin.message_store().is_none());
    assert!(admin.put_message(MessageExtBrokerInner::default()).await.is_err());
    assert!(matches!(
        admin.set_commitlog_read_mode(CommitLogReadMode::Normal).await,
        Err(crate::broker::broker_admin_runtime::CommitLogReadModeUpdateError::Store(error))
            if error.descriptor() == &rocketmq_error::STORAGE_LIFECYCLE_NOT_STARTED
    ));
    assert!(admin.delete_topics(Vec::new()).is_err());

    drop(admin_clone);
    drop(admin);
    let _ = std::fs::remove_dir_all(temp_root);
}

#[tokio::test]
async fn message_index_gap_rejects_reenable_and_remains_unsafe_after_store_restart() {
    let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-broker-index-gap-restart-{}", current_millis()));
    let broker_config = Arc::new(BrokerConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        message_index_enable: true,
        ha_listen_port: allocate_broker_runtime_test_port() as usize,
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(Arc::clone(&broker_config), Arc::clone(&message_store_config));
    assert!(runtime.initialize_metadata().await.is_ok());
    assert!(runtime.initialize_message_store().await);
    assert!(runtime.load_message_store_for_test().await);
    runtime
        .start_message_store_for_test()
        .await
        .expect("message store should start");

    let admin = runtime.admin_runtime_for_test();
    admin
        .commit_broker_config_patch(&HashMap::from([(
            CheetahString::from_static_str("messageIndexEnable"),
            CheetahString::from_static_str("false"),
        )]))
        .await
        .expect("disable should persist the gap marker before publication");
    let topic = CheetahString::from_static_str("IndexGapRestartTopic");
    let key = CheetahString::from_static_str("IndexGapRestartKey");
    let mut skipped = shared_append_test_message(&topic, Bytes::from_static(b"skipped"));
    skipped.set_keys(key.clone());
    assert!(admin
        .put_message(skipped)
        .await
        .expect("disabled append should reach Store")
        .is_ok());
    admin
        .commit_broker_config_patch(&HashMap::from([(
            CheetahString::from_static_str("messageIndexEnable"),
            CheetahString::from_static_str("false"),
        )]))
        .await
        .expect("an idempotent disable must preserve the original marker baseline");
    drop(admin);
    runtime.reput_message_store_once_for_test().await;

    let admin = runtime.admin_runtime_for_test();
    let error = admin
        .commit_broker_config_patch(&HashMap::from([(
            CheetahString::from_static_str("messageIndexEnable"),
            CheetahString::from_static_str("true"),
        )]))
        .await
        .expect_err("a disabled-period gap must reject enable before publication");
    assert!(matches!(error, BrokerConfigError::RuntimeCoordination { .. }));
    assert!(!admin.message_store_config().message_index_enable);
    drop(admin);
    assert!(runtime
        .admin_runtime_for_test()
        .message_store()
        .and_then(|store| store.message_index_runtime_snapshot())
        .is_some_and(|snapshot| !snapshot.enabled && snapshot.incomplete));

    runtime.shutdown_message_store_for_test().await;
    drop(runtime);

    let mut restarted = BrokerRuntime::new(broker_config, message_store_config);
    assert!(restarted.initialize_metadata().await.is_ok());
    assert!(restarted.initialize_message_store().await);
    assert!(restarted.load_message_store_for_test().await);
    restarted
        .start_message_store_for_test()
        .await
        .expect("restarted message store should start");
    let store = restarted
        .admin_runtime_for_test()
        .message_store()
        .expect("restarted Store should be available");
    let snapshot = store
        .message_index_runtime_snapshot()
        .expect("restarted Store should expose index state");
    assert!(snapshot.enabled);
    assert!(
        snapshot.incomplete,
        "the persisted disabled-period gap must survive restart"
    );
    let result = store
        .query_message(&topic, &key, 10, 0, i64::MAX)
        .await
        .expect("query should expose the persisted disabled-period degradation");
    assert!(!result.index_query_safe);
    drop(store);

    restarted.shutdown_message_store_for_test().await;
    let _ = std::fs::remove_dir_all(temp_root);
}

#[tokio::test]
async fn blocked_runtime_topic_registration_does_not_delay_controller_fencing() {
    let store_root = std::env::temp_dir().join(format!(
        "rocketmq-rust-runtime-topic-registration-controller-fence-{}",
        current_millis()
    ));
    let broker_config = Arc::new(BrokerConfig {
        enable_controller_mode: true,
        controller_addr: CheetahString::from_static_str("127.0.0.1:19876"),
        store_path_root_dir: store_root.to_string_lossy().into_owned().into(),
        auth_config_path: store_root.join("auth.json").to_string_lossy().into_owned().into(),
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        enable_controller_mode: true,
        broker_role: BrokerRole::SyncMaster,
        store_path_root_dir: store_root.to_string_lossy().into_owned().into(),
        timer_wheel_enable: false,
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    assert!(runtime.initialize_message_store().await, "initialize message store");
    runtime.composition.state.initialize_controller_mode();
    let admin = runtime.admin_runtime_for_test();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let registration: crate::topic::manager::topic_config_coordinator::TopicRegistrationAction = Box::new(move || {
        Box::pin(async move {
            let _ = entered_tx.send(());
            let _ = release_rx.await;
            Ok(())
        })
    });
    let admin_for_commit = admin.clone();
    let commit = tokio::spawn(async move {
        admin_for_commit
            .commit_broker_config_patch_with_registration_for_test(
                &HashMap::from([(
                    CheetahString::from_static_str("defaultTopicQueueNums"),
                    CheetahString::from_static_str("9"),
                )]),
                registration,
            )
            .await
    });
    entered_rx.await.expect("runtime topic registration should block");

    let controller = Arc::new(runtime.composition.state.build_controller_runtime());
    let controller_for_change = Arc::clone(&controller);
    let role_change = tokio::spawn(async move {
        controller_for_change
            .apply_controller_role_change(
                None,
                Some(MASTER_ID + 1),
                Some(CheetahString::from_static_str("127.0.0.1:10911")),
                Some(1),
                None,
                HashSet::new(),
            )
            .await
    });
    tokio::time::timeout(Duration::from_millis(250), async {
        loop {
            if !runtime
                .composition
                .state
                .message_store()
                .expect("message store")
                .put_message_preflight()
                .is_writeable()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the real Store write fence must not wait for NameServer registration");
    let _ = release_tx.send(());
    commit
        .await
        .expect("runtime patch task should join")
        .expect("runtime patch should stay committed");
    let role_applied = role_change
        .await
        .expect("controller role task should join")
        .expect("complete controller metadata should apply a real role transition");
    assert!(role_applied, "the valid demotion must reach the real Store role path");

    runtime.shutdown_message_store_for_test().await;
    let _ = std::fs::remove_dir_all(store_root);
}

#[tokio::test]
async fn blocked_store_role_apply_holds_fence_but_not_runtime_config_permit() {
    let store_root = std::env::temp_dir().join(format!(
        "rocketmq-rust-controller-store-await-config-permit-{}",
        current_millis()
    ));
    let broker_config = Arc::new(BrokerConfig {
        enable_controller_mode: true,
        controller_addr: CheetahString::from_static_str("127.0.0.1:19876"),
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        enable_controller_mode: true,
        broker_role: BrokerRole::Slave,
        store_path_root_dir: store_root.to_string_lossy().into_owned().into(),
        timer_wheel_enable: false,
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    assert!(runtime.initialize_message_store().await, "initialize message store");
    runtime.composition.state.initialize_controller_mode();
    let controller = Arc::new(runtime.composition.state.build_controller_runtime());
    let (entered_tx, entered_rx) = oneshot::channel();
    let entered_tx = Arc::new(parking_lot::Mutex::new(Some(entered_tx)));
    let (release_tx, release_rx) = oneshot::channel();
    let release_rx = Arc::new(parking_lot::Mutex::new(Some(release_rx)));
    controller.set_store_role_action_for_test(Arc::new(move || {
        let entered_tx = entered_tx.lock().take();
        let release_rx = release_rx.lock().take();
        Box::pin(async move {
            if let Some(entered_tx) = entered_tx {
                let _ = entered_tx.send(());
            }
            if let Some(release_rx) = release_rx {
                let _ = release_rx.await;
            }
            Ok(false)
        })
    }));
    let applying_controller = Arc::clone(&controller);
    let apply = tokio::spawn(async move {
        applying_controller
            .apply_controller_role_change(None, Some(MASTER_ID), None, Some(1), None, HashSet::new())
            .await
    });

    entered_rx
        .await
        .expect("Store role action should block outside config serialization");
    tokio::time::timeout(
        Duration::from_millis(250),
        controller.await_runtime_config_permit_for_test(),
    )
    .await
    .expect("Store await must not retain the runtime config permit");
    assert!(
        !runtime
            .composition
            .state
            .message_store()
            .expect("message store")
            .put_message_preflight()
            .is_writeable(),
        "controller writes must remain fenced while Store role application is blocked"
    );

    let _ = release_tx.send(());
    assert!(!apply
        .await
        .expect("role-change task should join")
        .expect("role change result"));
    runtime.shutdown_message_store_for_test().await;
    let _ = std::fs::remove_dir_all(store_root);
}

#[tokio::test]
async fn successive_runtime_policy_reconciliation_converges_on_latest_generation() {
    let mut runtime = new_phase3_test_runtime("runtime-topic-registration-latest-generation").await;
    let store_root = runtime.message_store_config().store_path_root_dir.clone();
    let admin = runtime.admin_runtime_for_test();
    let registration = runtime.composition.state.build_registration_runtime();
    let attempted_permissions = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let published_permission = Arc::new(AtomicU64::new(0));
    let attempt = Arc::new(AtomicU64::new(0));
    let (entered_tx, entered_rx) = oneshot::channel();
    let first_entered = Arc::new(parking_lot::Mutex::new(Some(entered_tx)));
    let release = Arc::new(tokio::sync::Notify::new());

    let first_registration = registration.clone();
    let first_attempted_permissions = Arc::clone(&attempted_permissions);
    let first_published_permission = Arc::clone(&published_permission);
    let first_attempt = Arc::clone(&attempt);
    let first_entered_signal = Arc::clone(&first_entered);
    let first_release = Arc::clone(&release);
    let first_action: crate::topic::manager::topic_config_coordinator::TopicRegistrationAction = Box::new(move || {
        Box::pin(async move {
            first_registration
                .register_runtime_config_snapshot_with_test_hook(move |broker_config| {
                    let attempted_permissions = Arc::clone(&first_attempted_permissions);
                    let published_permission = Arc::clone(&first_published_permission);
                    let attempt = Arc::clone(&first_attempt);
                    let entered = Arc::clone(&first_entered_signal);
                    let release = Arc::clone(&first_release);
                    async move {
                        let permission = u64::from(broker_config.broker_permission);
                        attempted_permissions.lock().push(permission);
                        if attempt.fetch_add(1, Ordering::AcqRel) == 0 {
                            if let Some(entered) = entered.lock().take() {
                                let _ = entered.send(());
                            }
                            release.notified().await;
                        }
                        published_permission.store(permission, Ordering::Release);
                        Ok(crate::broker::broker_registration_runtime::BrokerRegistrationStatus::Unchanged)
                    }
                })
                .await
                .map(|_| ())
                .or_else(|error| error.into_coordination_result("runtime registration generation test"))
        })
    });
    let first_admin = admin.clone();
    let first_patch = tokio::spawn(async move {
        first_admin
            .commit_broker_config_patch_with_registration_for_test(
                &HashMap::from([(
                    CheetahString::from_static_str("brokerPermission"),
                    CheetahString::from_static_str("4"),
                )]),
                first_action,
            )
            .await
    });
    entered_rx.await.expect("first registration attempt should block");

    let second_observed_permission = Arc::new(AtomicU64::new(0));
    let second_observed = Arc::clone(&second_observed_permission);
    let second_admin_observer = admin.clone();
    let second_action: crate::topic::manager::topic_config_coordinator::TopicRegistrationAction = Box::new(move || {
        Box::pin(async move {
            second_observed.store(
                u64::from(second_admin_observer.broker_config().broker_permission),
                Ordering::Release,
            );
            Ok(())
        })
    });
    let second_admin = admin.clone();
    let second_patch = tokio::spawn(async move {
        second_admin
            .commit_broker_config_patch_with_registration_for_test(
                &HashMap::from([(
                    CheetahString::from_static_str("brokerPermission"),
                    CheetahString::from_static_str("2"),
                )]),
                second_action,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while admin.broker_config().broker_permission != PermName::PERM_WRITE {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second policy patch should publish while the first registration is blocked");

    release.notify_one();
    first_patch
        .await
        .expect("first patch task should join")
        .expect("first patch should remain committed");
    second_patch
        .await
        .expect("second patch task should join")
        .expect("second patch should remain committed");

    assert_eq!(&*attempted_permissions.lock(), &[4, 2]);
    assert_eq!(published_permission.load(Ordering::Acquire), 2);
    assert_eq!(second_observed_permission.load(Ordering::Acquire), 2);
    assert_eq!(admin.broker_config().broker_permission, PermName::PERM_WRITE);

    runtime.shutdown_message_store_for_test().await;
    let _ = std::fs::remove_dir_all(store_root.as_str());
}

#[cfg(feature = "rocksdb_store")]
#[tokio::test]
async fn initialize_message_store_opens_rocksdb_owner_for_rocksdb_store_type() {
    let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-broker-runtime-rocksdb-{}", current_millis()));
    let broker_config = Arc::new(BrokerConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        store_type: StoreType::RocksDB,
        ha_listen_port: allocate_broker_runtime_test_port() as usize,
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    runtime
        .initialize_metadata()
        .await
        .expect("initialize RocksDB broker metadata");
    assert!(runtime.initialize_message_store().await);
    runtime
        .start_message_store_for_test()
        .await
        .expect("start RocksDB message store");
    {
        let message_store = runtime
            .composition
            .state
            .message_store()
            .expect("message store should be initialized");
        let BrokerMessageStore::RocksDBStore(rocksdb_owner) = message_store else {
            panic!("RocksDB store type should initialize the broker main store as BrokerMessageStore::RocksDBStore");
        };
        assert!(rocksdb_owner.rocksdb_config().path.ends_with("consumequeue_rocksdb"));
    }

    let topic = CheetahString::from_static_str("rocks-shared-append-topic");
    let capability = runtime.composition.data_plane.escape_bridge_owner.store_capability();
    assert!(capability
        .append_message(shared_append_test_message(&topic, Bytes::from_static(b"rocks-single"),))
        .await
        .expect("Rocks single shared append")
        .result()
        .is_ok());
    assert!(capability
        .append_batch(shared_append_test_batch(
            &topic,
            &[
                Bytes::from_static(b"rocks-batch-one"),
                Bytes::from_static(b"rocks-batch-two")
            ],
        ))
        .await
        .expect("Rocks batch shared append")
        .result()
        .is_ok());
    runtime.reput_message_store_once_for_test().await;
    wait_until(
        Duration::from_secs(5),
        || {
            runtime
                .composition
                .state
                .message_store()
                .expect("message store should be initialized")
                .get_max_offset_in_queue(&topic, 0)
                == 3
        },
        "RocksDB ConsumeQueue to publish all three dispatched messages",
    )
    .await;
    assert_eq!(
        runtime
            .composition
            .state
            .message_store()
            .expect("message store should be initialized")
            .get_max_offset_in_queue(&topic, 0),
        3,
        "one single message and a two-message batch must create three ConsumeQueue units"
    );

    runtime.shutdown_message_store_for_test().await;
    let _ = std::fs::remove_dir_all(temp_root);
}

#[tokio::test]
async fn phase3_broker_production_request_codes_dispatch_to_expected_processors() {
    let mut runtime = new_phase3_test_runtime("phase3-dispatch").await;
    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");

    for request_code in [
        RequestCode::SendMessage,
        RequestCode::SendMessageV2,
        RequestCode::SendBatchMessage,
        RequestCode::ConsumerSendMsgBack,
    ] {
        assert_eq!(
            processor.dispatch_processor_variant_for_test(request_code),
            Some("Send"),
            "{request_code:?} should dispatch to SendMessageProcessor"
        );
    }

    for request_code in [RequestCode::SendReplyMessage, RequestCode::SendReplyMessageV2] {
        assert_eq!(
            processor.dispatch_processor_variant_for_test(request_code),
            Some("Reply"),
            "{request_code:?} should dispatch to ReplyMessageProcessor"
        );
    }

    assert_eq!(
        processor.dispatch_processor_variant_for_test(RequestCode::EndTransaction),
        Some("EndTransaction")
    );
    assert_eq!(
        processor.dispatch_processor_variant_for_test(RequestCode::RecallMessage),
        Some("Recall")
    );
    assert_eq!(
        processor.dispatch_processor_variant_for_test(RequestCode::QueryMessage),
        Some("QueryMessage")
    );
    assert_eq!(
        processor.dispatch_processor_variant_for_test(RequestCode::ViewMessageById),
        Some("QueryMessage")
    );

    for request_code in [
        RequestCode::UpdateAndCreateTopic,
        RequestCode::UpdateAndCreateTopicList,
        RequestCode::GetAllTopicConfig,
        RequestCode::GetTopicConfig,
    ] {
        assert_eq!(
            processor.dispatch_processor_variant_for_test(request_code),
            Some("AdminBroker"),
            "{request_code:?} should fall back to AdminBrokerProcessor"
        );
    }

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn pull_processor_uses_the_live_consumer_session_registry_installed_by_pipeline() {
    let mut runtime = new_phase3_test_runtime("pull-session-client-lookup").await;
    let (mut processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    processor.set_auth_disabled_by_validated_config();
    let pull_processor = runtime
        .composition
        .request_pipeline
        .pull_message_processor_for_test
        .as_ref()
        .cloned()
        .expect("request pipeline should retain the test Pull processor handle");
    let registration = runtime.composition.state.consumer_manager().client_registration();
    let session_registry = runtime.session_registry_for_test();
    let mut session_events = session_registry.subscribe();
    let (mut old_client, server) =
        crate::processor::processor_test_support::start_processor_server_with_session_registry(
            "pull-session-client-lookup",
            processor,
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
            session_registry,
        )
        .await;
    old_client
        .send_command(RemotingCommand::new_request(RequestCode::GetBrokerConfig, Bytes::new()))
        .await
        .expect("activate initial session");
    let session_id = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let rocketmq_transport::api::SessionEvent::Connected(view) =
                session_events.recv().await.expect("receive initial session event")
            {
                break view.id();
            }
        }
    })
    .await
    .expect("initial session should connect");
    let mut replacement_client = server.connect().await;
    replacement_client
        .send_command(RemotingCommand::new_request(RequestCode::GetBrokerConfig, Bytes::new()))
        .await
        .expect("activate replacement session");
    let replacement_session_id = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let rocketmq_transport::api::SessionEvent::Connected(view) =
                session_events.recv().await.expect("receive replacement session event")
            {
                break view.id();
            }
        }
    })
    .await
    .expect("replacement session should connect");
    let group = CheetahString::from_static_str("pull-session-group");

    assert!(registration.register_consumer_session(
        &group,
        crate::client::client_session_info::ClientSessionInfo::new(
            session_id,
            "pull-session-client".into(),
            None,
            rocketmq_protocol::protocol::LanguageCode::RUST,
            1,
        ),
        rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType::ConsumePassively,
        rocketmq_protocol::protocol::heartbeat::message_model::MessageModel::Clustering,
        rocketmq_model::common::consumer::consume_from_where::ConsumeFromWhere::ConsumeFromLastOffset,
        HashSet::new(),
        false,
    ));
    assert_eq!(
        pull_processor.session_client_id_for_test(session_id, &group),
        Some(CheetahString::from_static_str("pull-session-client"))
    );

    assert!(!registration.register_consumer_session(
        &group,
        crate::client::client_session_info::ClientSessionInfo::new(
            session_id,
            "pull-session-client".into(),
            None,
            rocketmq_protocol::protocol::LanguageCode::RUST,
            2,
        ),
        rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType::ConsumePassively,
        rocketmq_protocol::protocol::heartbeat::message_model::MessageModel::Clustering,
        rocketmq_model::common::consumer::consume_from_where::ConsumeFromWhere::ConsumeFromLastOffset,
        HashSet::new(),
        false,
    ));
    assert_eq!(
        pull_processor.session_client_id_for_test(session_id, &group),
        Some(CheetahString::from_static_str("pull-session-client"))
    );

    assert!(registration.register_consumer_session(
        &group,
        crate::client::client_session_info::ClientSessionInfo::new(
            replacement_session_id,
            "pull-session-client".into(),
            None,
            rocketmq_protocol::protocol::LanguageCode::RUST,
            3,
        ),
        rocketmq_protocol::protocol::heartbeat::consume_type::ConsumeType::ConsumePassively,
        rocketmq_protocol::protocol::heartbeat::message_model::MessageModel::Clustering,
        rocketmq_model::common::consumer::consume_from_where::ConsumeFromWhere::ConsumeFromLastOffset,
        HashSet::new(),
        false,
    ));
    assert_eq!(pull_processor.session_client_id_for_test(session_id, &group), None);
    assert_eq!(
        pull_processor.session_client_id_for_test(replacement_session_id, &group),
        Some(CheetahString::from_static_str("pull-session-client"))
    );

    // A delayed close for the replaced transport session must not remove the live replacement.
    registration.unregister_consumer_session(group.as_str(), session_id, false);
    assert_eq!(
        pull_processor.session_client_id_for_test(replacement_session_id, &group),
        Some(CheetahString::from_static_str("pull-session-client"))
    );
    registration.unregister_consumer_session(group.as_str(), replacement_session_id, false);
    assert_eq!(
        pull_processor.session_client_id_for_test(replacement_session_id, &group),
        None
    );
    drop(old_client);
    drop(replacement_client);
    server.finish().await;
    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_composition_owns_and_installs_one_deferred_lifecycle() {
    let mut runtime = new_phase3_lifecycle_test_runtime("shared-deferred-lifecycle").await;
    let _ = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let initial_admission = runtime
        .composition
        .data_plane
        .deferred
        .as_ref()
        .expect("request pipeline should initialize the Broker deferred lifecycle")
        .admission_controller
        .clone();
    let reinitialized_admission = runtime
        .initialize_deferred_lifecycle()
        .expect("deferred lifecycle initialization should be idempotent");
    assert!(Arc::ptr_eq(&initial_admission, &reinitialized_admission));
    let deferred = runtime
        .composition
        .data_plane
        .deferred
        .as_ref()
        .expect("request pipeline should initialize the Broker deferred lifecycle");
    let pipeline_admission = runtime
        .composition
        .request_pipeline
        .admission_controller()
        .expect("request pipeline should retain the shared admission controller");

    assert!(Arc::ptr_eq(&pipeline_admission, &deferred.admission_controller));
    assert!(Arc::ptr_eq(
        &runtime
            .composition
            .deferred_generation_handoff()
            .expect("composition should expose the shared handoff"),
        &deferred.handoff,
    ));
    assert!(runtime
        .composition
        .state
        .pop_message_processor
        .as_ref()
        .expect("POP processor")
        .pop_deferred_service_is_installed_for_test());
    assert!(runtime
        .composition
        .request_pipeline
        .pull_message_processor_for_test
        .as_ref()
        .expect("Pull processor")
        .pull_deferred_service_is_installed_for_test());
    assert!(runtime
        .composition
        .state
        .notification_processor
        .as_ref()
        .expect("Notification processor")
        .notification_deferred_service_is_installed_for_test());
    assert!(runtime
        .composition
        .state
        .pop_lite_message_processor
        .as_ref()
        .expect("PopLite processor")
        .pop_lite_deferred_service_is_installed_for_test());
    assert!(deferred.producer_is_installed());
    let producer = deferred
        .producer
        .as_ref()
        .cloned()
        .expect("deferred producer should be retained by the lifecycle owner");
    let producer_task_group = deferred
        .producer_task_group()
        .expect("deferred producer task group should be retained by its lifecycle owner");
    assert_eq!(
        producer_task_group.parent_id(),
        Some(
            runtime
                .composition
                .state
                .service_context
                .as_ref()
                .expect("phase3 runtime should retain a service context")
                .task_group()
                .id()
        )
    );
    assert!(
        producer_task_group.task_count() >= 2,
        "producer periodic tasks should be owned by the dedicated child group"
    );

    let topic = CheetahString::from_static_str("shared-deferred-lifecycle-topic");
    producer.route_pull_arrival(&topic, 0, 1, None, 0, None, None);
    producer.route_pop_arrival(&topic, 0, None, 0, None, None);
    producer.route_notification_arrival(&topic, 0, None, 0, None, None);

    let lite_dispatcher = runtime.composition.state.lite_event_dispatcher().clone();
    let lite_client = CheetahString::from_static_str("shared-deferred-lifecycle-client");
    let lite_group = CheetahString::from_static_str("shared-deferred-lifecycle-group");
    let lite_events = HashSet::from([CheetahString::from_static_str("%LMQ%shared-deferred-lifecycle")]);
    assert_eq!(
        lite_dispatcher.do_full_dispatch(&lite_client, &lite_group, &lite_events),
        1
    );
    assert_eq!(
        lite_dispatcher.take_pending_events(&lite_client),
        vec![CheetahString::from_static_str("%LMQ%shared-deferred-lifecycle")],
        "the canonical event owner must receive the event exactly once"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while !deferred.resource_snapshot().handoff_zero {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("canonical arrival probes must release every transient route permit");
    drop(producer);

    assert_deferred_lifecycle_shutdown(&mut runtime).await;

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn broker_normal_fast_and_embedded_routes_share_the_canonical_dispatcher() {
    let mut runtime = new_phase3_lifecycle_test_runtime("canonical-route-identity").await;
    runtime
        .start_basic_service()
        .await
        .expect("start canonical Broker listeners");
    let identity = runtime
        .dispatcher_identity_snapshot_for_test()
        .expect("startup should record dispatcher ownership");

    assert!(identity.normal_is_canonical);
    assert!(identity.fast_is_canonical);
    assert!(identity.embedded_proxy_is_canonical);

    let report = runtime
        .shutdown_basic_service_until(ShutdownDeadline::after(Duration::from_secs(10)))
        .await;
    assert!(report.is_healthy(), "{report:?}");
    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initialized_broker_shutdown_unbinds_deferred_replay_store_before_exclusive_store_access() {
    let mut runtime = new_phase3_lifecycle_test_runtime("deferred-replay-store-shutdown").await;
    let _ = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let store_root = runtime
        .composition
        .state
        .message_store
        .as_ref()
        .expect("message store should be initialized");
    assert!(
        Arc::weak_count(store_root) > 0,
        "the running deferred producer should hold its replay Store capability"
    );
    assert!(
        runtime.composition.state.message_store_mut().is_none(),
        "a bound replay capability must prevent premature exclusive Store access"
    );

    let report = runtime
        .shutdown_basic_service_until(ShutdownDeadline::after(Duration::from_secs(10)))
        .await;

    assert!(
        report
            .deferred_producer_tasks
            .as_ref()
            .is_some_and(ShutdownReport::is_healthy),
        "deferred producers must join before their Store capability is unbound: {report:?}"
    );
    assert!(
        report.message_store.present && report.message_store.healthy && !report.message_store.timed_out,
        "exclusive Store shutdown should complete after deferred replay unbind: {report:?}"
    );
    assert!(
        runtime.composition.state.message_store().is_none(),
        "healthy shutdown should release the final Store lifecycle owner"
    );

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_deferred_producer_install_is_exactly_once_and_transactional() {
    use crate::broker_runtime::deferred_producer::BrokerDeferredProducer;
    use crate::broker_runtime::deferred_producer::BrokerDeferredProducerInstallError;

    let mut runtime = new_phase3_lifecycle_test_runtime("deferred-producer-install-transaction").await;
    let _ = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    {
        let deferred = runtime
            .composition
            .data_plane
            .deferred
            .as_ref()
            .expect("request pipeline should initialize the Broker deferred lifecycle");
        let handoff = Arc::clone(&deferred.handoff);
        let pop = Arc::clone(&deferred.pop);
        let pull = Arc::clone(&deferred.pull);
        let notification = Arc::clone(&deferred.notification);
        let pop_lite = Arc::clone(&deferred.pop_lite);
        let task_group = deferred
            .producer_task_group()
            .expect("deferred producer task group should be retained");
        let pull_processor = runtime
            .composition
            .request_pipeline
            .pull_message_processor_for_test
            .as_ref()
            .cloned()
            .expect("Pull processor");
        let pop_processor = runtime
            .composition
            .state
            .pop_message_processor
            .as_ref()
            .cloned()
            .expect("POP processor");
        let notification_processor = runtime
            .composition
            .state
            .notification_processor
            .as_ref()
            .cloned()
            .expect("Notification processor");
        let pop_lite_processor = runtime
            .composition
            .state
            .pop_lite_message_processor
            .as_ref()
            .cloned()
            .expect("PopLite processor");
        let installed_lite_dispatcher = runtime.composition.state.lite_event_dispatcher().clone();

        let duplicate = BrokerDeferredProducer::new(
            Arc::clone(&handoff),
            Arc::clone(&pop),
            Arc::clone(&pull),
            Arc::clone(&notification),
            Arc::clone(&pop_lite),
            installed_lite_dispatcher.clone(),
            &pull_processor,
            &pop_processor,
            &notification_processor,
            &pop_lite_processor,
            task_group.clone(),
            Duration::from_millis(1),
        );
        let Err(duplicate) = duplicate else {
            panic!("a second producer must not replace the lifecycle owner");
        };
        assert_eq!(duplicate, BrokerDeferredProducerInstallError::LiteEventObserver);
        assert!(installed_lite_dispatcher.has_deferred_event_observer());
        assert!(pop_processor.has_lag_refresh_producer());

        let rejected_lite_hits = Arc::new(AtomicU64::new(0));
        let rejected_lite_probe = {
            let hits = Arc::clone(&rejected_lite_hits);
            Arc::new(move |_client_id: &CheetahString| {
                hits.fetch_add(1, Ordering::Relaxed);
                true
            }) as Arc<crate::lite::lite_event_dispatcher::DeferredEventObserver>
        };
        assert!(installed_lite_dispatcher
            .install_deferred_event_observer(Arc::clone(&rejected_lite_probe))
            .is_err());
        let rejected_pop_hits = Arc::new(AtomicU64::new(0));
        let rejected_pop_probe: Arc<crate::processor::pop_message_processor::PopLagRefreshProducer> = {
            let hits = Arc::clone(&rejected_pop_hits);
            Arc::new(move |_topic: &CheetahString, _group: &CheetahString| {
                hits.fetch_add(1, Ordering::Relaxed);
                None
            })
        };
        assert!(pop_processor
            .install_lag_refresh_producer(Arc::clone(&rejected_pop_probe))
            .is_err());

        let client = CheetahString::from_static_str("producer-owner-client");
        let group = CheetahString::from_static_str("producer-owner-group");
        let events = HashSet::from([CheetahString::from_static_str("%LMQ%producer-owner")]);
        assert_eq!(installed_lite_dispatcher.do_full_dispatch(&client, &group, &events), 1);
        assert_eq!(installed_lite_dispatcher.take_pending_events(&client).len(), 1);
        let topic = CheetahString::from_static_str("producer-owner-topic");
        let _ = pop_processor.notify_message_arriving_before_lag(&topic, &group);
        assert_eq!(rejected_lite_hits.load(Ordering::Relaxed), 0);
        assert_eq!(rejected_pop_hits.load(Ordering::Relaxed), 0);

        let late_lite_dispatcher = crate::lite::lite_event_dispatcher::LiteEventDispatcher::default();
        let late_pop_processor = runtime.pop_message_processor_for_test();
        let late_pop_hits = Arc::new(AtomicU64::new(0));
        let late_pop_probe: Arc<crate::processor::pop_message_processor::PopLagRefreshProducer> = {
            let hits = Arc::clone(&late_pop_hits);
            Arc::new(move |_topic: &CheetahString, _group: &CheetahString| {
                hits.fetch_add(1, Ordering::Relaxed);
                None
            })
        };
        assert!(late_pop_processor
            .install_lag_refresh_producer(Arc::clone(&late_pop_probe))
            .is_ok());
        let late_error = BrokerDeferredProducer::new(
            handoff,
            pop,
            pull,
            notification,
            pop_lite,
            late_lite_dispatcher.clone(),
            &pull_processor,
            &late_pop_processor,
            &notification_processor,
            &pop_lite_processor,
            task_group,
            Duration::from_millis(1),
        );
        let Err(late_error) = late_error else {
            panic!("a late-stage conflict must roll back the Lite callback");
        };
        assert_eq!(late_error, BrokerDeferredProducerInstallError::PopLagRefresh);
        assert!(!late_lite_dispatcher.has_deferred_event_observer());
        assert!(late_pop_processor.has_lag_refresh_producer());

        let late_lite_hits = Arc::new(AtomicU64::new(0));
        let late_lite_probe = {
            let hits = Arc::clone(&late_lite_hits);
            Arc::new(move |_client_id: &CheetahString| {
                hits.fetch_add(1, Ordering::Relaxed);
                true
            }) as Arc<crate::lite::lite_event_dispatcher::DeferredEventObserver>
        };
        assert!(late_lite_dispatcher
            .install_deferred_event_observer(Arc::clone(&late_lite_probe))
            .is_ok());
        assert_eq!(late_lite_dispatcher.do_full_dispatch(&client, &group, &events), 1);
        let _ = late_pop_processor.notify_message_arriving_before_lag(&topic, &group);
        assert_eq!(late_lite_hits.load(Ordering::Relaxed), 1);
        assert_eq!(late_pop_hits.load(Ordering::Relaxed), 1);

        assert!(late_lite_dispatcher.uninstall_deferred_event_observer(&late_lite_probe));
        assert!(late_pop_processor.uninstall_lag_refresh_producer(&late_pop_probe));
    }

    assert_deferred_lifecycle_shutdown(&mut runtime).await;
    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn phase3_send_message_processor_writes_to_local_store() {
    let mut runtime = new_phase3_test_runtime("phase3-send").await;
    let topic = CheetahString::from_static_str("phase3-send-topic");
    runtime
        .runtime_state_mut()
        .topic_config_manager()
        .update_topic_config(TopicConfig::with_queues(topic.clone(), 1, 1), 0);

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let send_header = SendMessageRequestHeader {
        producer_group: CheetahString::from_static_str("phase3-producer"),
        topic: topic.clone(),
        default_topic: CheetahString::from_static_str("TBW102"),
        default_topic_queue_nums: 1,
        queue_id: 0,
        sys_flag: 0,
        born_timestamp: current_millis() as i64,
        flag: 0,
        properties: None,
        reconsume_times: None,
        unit_mode: Some(false),
        batch: Some(false),
        max_reconsume_times: None,
        topic_request_header: None,
    };
    let mut request = RemotingCommand::create_request_command(RequestCode::SendMessage, send_header)
        .set_body(Bytes::from_static(b"phase3-message-body"));
    request.make_custom_header_to_net();

    let response = process_broker_request(&processor, &mut request).await;

    assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
    let response_header = response
        .decode_command_custom_header::<SendMessageResponseHeader>()
        .expect("send response should include SendMessageResponseHeader");
    assert!(!response_header.msg_id().is_empty());
    assert_eq!(response_header.queue_id(), 0);
    assert_eq!(response_header.queue_offset(), 0);

    drop(processor);
    runtime.reput_message_store_once_for_test().await;
    assert_eq!(
        runtime
            .composition
            .state
            .message_store()
            .expect("message store should be initialized")
            .get_max_offset_in_queue(&topic, 0),
        1
    );

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn phase3_consumer_send_msg_back_writes_retry_delay_message() {
    let mut runtime = new_phase3_test_runtime("phase3-send-back").await;
    let topic = CheetahString::from_static_str("phase3-send-back-topic");
    let group = CheetahString::from_static_str("phase3-send-back-group");
    runtime
        .runtime_state_mut()
        .topic_config_manager()
        .update_topic_config(TopicConfig::with_queues(topic.clone(), 1, 1), 0);
    runtime
        .runtime_state_mut()
        .subscription_group_manager_mut()
        .update_subscription_group_config(&mut SubscriptionGroupConfig::new(group.clone()));
    runtime
        .start_message_store_for_test()
        .await
        .expect("message store should start");

    let mut original = MessageExtBrokerInner::default();
    original.set_topic(topic.clone());
    original.message_ext_inner.set_queue_id(0);
    original.set_body(Bytes::from_static(b"phase3-send-back-body"));
    original.put_property(
        CheetahString::from_static_str(MessageConst::PROPERTY_TAGS),
        CheetahString::from_static_str("RetryTag"),
    );

    let append = runtime
        .composition
        .data_plane
        .escape_bridge_owner
        .store_capability()
        .append_message(original)
        .await;
    let append = append.expect("seed message should reach the Store");
    let put_result = append.result();
    assert!(put_result.is_ok(), "seed message should be stored");
    let commit_log_offset = put_result
        .append_message_result()
        .expect("seed put should expose append result")
        .wrote_offset;

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let send_back_header = ConsumerSendMsgBackRequestHeader {
        offset: commit_log_offset,
        group: group.clone(),
        delay_level: 3,
        origin_msg_id: Some(CheetahString::from_static_str("origin-msg-id")),
        origin_topic: Some(topic),
        unit_mode: false,
        max_reconsume_times: Some(16),
        rpc_request_header: None,
    };
    let mut send_back_request =
        RemotingCommand::create_request_command(RequestCode::ConsumerSendMsgBack, send_back_header);
    send_back_request.make_custom_header_to_net();

    let send_back_response = process_broker_request(&processor, &mut send_back_request).await;
    assert_eq!(ResponseCode::from(send_back_response.code()), ResponseCode::Success);

    let schedule_queue_id = crate::schedule::schedule_message_service::delay_level_to_queue_id(3);
    let schedule_topic = CheetahString::from_static_str(TopicValidator::RMQ_SYS_SCHEDULE_TOPIC);
    let mut scheduled_offset = 0;
    for _ in 0..100 {
        scheduled_offset = runtime
            .composition
            .state
            .message_store()
            .expect("message store should be initialized")
            .get_max_offset_in_queue(&schedule_topic, schedule_queue_id);
        if scheduled_offset == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        scheduled_offset, 1,
        "send-back should write one delayed retry message into SCHEDULE_TOPIC_XXXX"
    );

    let retry_topic = CheetahString::from_string(mix_all::get_retry_topic(group.as_str()));
    assert!(
        runtime
            .runtime_state_mut()
            .topic_config_manager()
            .select_topic_config(&retry_topic)
            .is_some(),
        "send-back should create the retry topic for the consumer group"
    );

    drop(processor);
    runtime.shutdown_message_store_for_test().await;
    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn phase3_topic_config_admin_processor_returns_decodable_bodies() {
    let mut runtime = new_phase3_test_runtime("phase3-topic-config").await;
    let topic = CheetahString::from_static_str("phase3-topic-config");
    runtime
        .runtime_state_mut()
        .topic_config_manager()
        .update_topic_config(TopicConfig::with_queues(topic.clone(), 2, 3), 0);

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let mut all_config_request = RemotingCommand::create_remoting_command(RequestCode::GetAllTopicConfig);
    let all_config_response = process_broker_request(&processor, &mut all_config_request).await;
    assert_eq!(ResponseCode::from(all_config_response.code()), ResponseCode::Success);
    let all_config_body = all_config_response
        .body()
        .expect("GetAllTopicConfig response should include a body");
    let all_config =
        TopicConfigAndMappingSerializeWrapper::decode(all_config_body).expect("GetAllTopicConfig body should decode");
    assert!(all_config
        .topic_config_serialize_wrapper
        .topic_config_table
        .contains_key(&topic));

    let get_config_header = GetTopicConfigRequestHeader {
        topic: topic.clone(),
        topic_request_header: None,
    };
    let mut get_config_request =
        RemotingCommand::create_request_command(RequestCode::GetTopicConfig, get_config_header);
    get_config_request.make_custom_header_to_net();
    let get_config_response = process_broker_request(&processor, &mut get_config_request).await;
    assert_eq!(ResponseCode::from(get_config_response.code()), ResponseCode::Success);
    let get_config_body = get_config_response
        .body()
        .expect("GetTopicConfig response should include a body");
    let topic_config = TopicConfigAndQueueMapping::decode(get_config_body).expect("GetTopicConfig body should decode");
    assert_eq!(topic_config.topic_config.topic_name.as_ref(), Some(&topic));
    assert_eq!(topic_config.topic_config.read_queue_nums, 2);
    assert_eq!(topic_config.topic_config.write_queue_nums, 3);

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn phase4_broker_consumer_request_codes_dispatch_to_expected_processors() {
    let mut runtime = new_phase3_test_runtime("phase4-dispatch").await;
    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");

    for (request_code, expected_processor) in [
        (RequestCode::PullMessage, "Pull"),
        (RequestCode::LitePullMessage, "Pull"),
        (RequestCode::PeekMessage, "Peek"),
        (RequestCode::PopMessage, "Pop"),
        (RequestCode::PopLiteMessage, "PopLite"),
        (RequestCode::AckMessage, "Ack"),
        (RequestCode::BatchAckMessage, "Ack"),
        (RequestCode::ChangeMessageInvisibleTime, "ChangeInvisible"),
        (RequestCode::Notification, "Notification"),
        (RequestCode::PollingInfo, "PollingInfo"),
        (RequestCode::GetConsumerListByGroup, "ConsumerManage"),
        (RequestCode::UpdateConsumerOffset, "ConsumerManage"),
        (RequestCode::QueryConsumerOffset, "ConsumerManage"),
        (RequestCode::QueryAssignment, "QueryAssignment"),
        (RequestCode::SetMessageRequestMode, "QueryAssignment"),
    ] {
        assert_eq!(
            processor.dispatch_processor_variant_for_test(request_code),
            Some(expected_processor),
            "{request_code:?} should dispatch to {expected_processor}"
        );
    }

    for request_code in [
        RequestCode::LockBatchMq,
        RequestCode::UnlockBatchMq,
        RequestCode::PopRollback,
        RequestCode::ResetConsumerOffsetInBroker,
        RequestCode::InvokeBrokerToResetOffset,
        RequestCode::InvokeBrokerToGetConsumerStatus,
        RequestCode::QueryTopicConsumeByWho,
        RequestCode::QueryTopicsByConsumer,
        RequestCode::QuerySubscriptionByConsumer,
        RequestCode::QueryConsumeTimeSpan,
        RequestCode::QueryCorrectionOffset,
        RequestCode::ConsumeMessageDirectly,
        RequestCode::CloneGroupOffset,
        RequestCode::GetAllMessageRequestMode,
    ] {
        assert_eq!(
            processor.dispatch_processor_variant_for_test(request_code),
            Some("AdminBroker"),
            "{request_code:?} should fall back to AdminBrokerProcessor"
        );
    }

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn phase4_consumer_offset_processors_round_trip_committed_offset() {
    let mut runtime = new_phase3_test_runtime("phase4-offset").await;
    let topic = CheetahString::from_static_str("phase4-offset-topic");
    let group = CheetahString::from_static_str("phase4-consumer-group");
    runtime
        .runtime_state_mut()
        .topic_config_manager()
        .update_topic_config(TopicConfig::with_queues(topic.clone(), 1, 1), 0);

    let mut group_config = SubscriptionGroupConfig::new(group.clone());
    runtime
        .runtime_state_mut()
        .subscription_group_manager_mut()
        .update_subscription_group_config(&mut group_config);

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let update_header = UpdateConsumerOffsetRequestHeader {
        consumer_group: group.clone(),
        topic: topic.clone(),
        queue_id: 0,
        commit_offset: 42,
        topic_request_header: None,
    };
    let mut update_request = RemotingCommand::create_request_command(RequestCode::UpdateConsumerOffset, update_header);
    update_request.make_custom_header_to_net();

    let update_response = process_broker_request(&processor, &mut update_request).await;
    assert_eq!(ResponseCode::from(update_response.code()), ResponseCode::Success);

    let mut query_header = QueryConsumerOffsetRequestHeader::new(group.clone(), topic.clone(), 0);
    query_header.set_zero_if_not_found = Some(false);
    let mut query_request = RemotingCommand::create_request_command(RequestCode::QueryConsumerOffset, query_header);
    query_request.make_custom_header_to_net();

    let mut query_response = process_broker_request(&processor, &mut query_request).await;
    assert_eq!(ResponseCode::from(query_response.code()), ResponseCode::Success);
    query_response.make_custom_header_to_net();
    let response_header = query_response
        .decode_command_custom_header::<QueryConsumerOffsetResponseHeader>()
        .expect("QueryConsumerOffset response should include QueryConsumerOffsetResponseHeader");
    assert_eq!(response_header.offset, Some(42));

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn java_definition_only_g8_request_codes_return_explicit_unsupported() {
    let mut runtime = new_phase3_test_runtime("g8-definition-only-unsupported").await;
    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");

    for request_code in [
        RequestCode::QueryBrokerOffset,
        RequestCode::GetTopicConfigList,
        RequestCode::GetTopicNameList,
        RequestCode::TriggerDeleteFiles,
        RequestCode::GetClientConfig,
        RequestCode::AckLiteMessage,
        RequestCode::SuspendConsumer,
        RequestCode::ResumeConsumer,
        RequestCode::ResetConsumerOffsetInConsumer,
        RequestCode::ResetConsumerOffsetInBroker,
        RequestCode::AdjustConsumerThreadPool,
        RequestCode::WhoConsumeTheMessage,
        RequestCode::RegisterFilterServer,
        RequestCode::RegisterMessageFilterClass,
    ] {
        let mut request = RemotingCommand::create_remoting_command(request_code);
        let response = process_broker_request(&processor, &mut request).await;
        assert_eq!(
            ResponseCode::from(response.code()),
            ResponseCode::RequestCodeNotSupported,
            "{request_code:?} should keep an explicit unsupported contract"
        );
        assert_eq!(
            response.remark().map(|remark| remark.as_str()),
            Some("Protocol request is unsupported"),
            "{request_code:?} should use the canonical unsupported remark"
        );
    }

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn add_remove_broker_without_container_returns_request_code_not_supported() {
    let mut runtime = new_phase3_test_runtime("container-add-remove-unsupported").await;
    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");

    let mut add_request = RemotingCommand::create_request_command(
        RequestCode::AddBroker,
        AddBrokerRequestHeader {
            config_path: Some(CheetahString::from_static_str("broker.conf")),
        },
    );
    add_request.make_custom_header_to_net();
    let add_response = process_broker_request(&processor, &mut add_request).await;
    assert_eq!(
        ResponseCode::from(add_response.code()),
        ResponseCode::RequestCodeNotSupported
    );
    assert_eq!(
        add_response.remark().map(|remark| remark.as_str()),
        Some("Protocol request is unsupported")
    );

    let mut remove_request = RemotingCommand::create_request_command(
        RequestCode::RemoveBroker,
        RemoveBrokerRequestHeader {
            broker_name: CheetahString::from_static_str("broker-a"),
            broker_cluster_name: CheetahString::from_static_str("DefaultCluster"),
            broker_id: 1,
        },
    );
    remove_request.make_custom_header_to_net();
    let remove_response = process_broker_request(&processor, &mut remove_request).await;
    assert_eq!(
        ResponseCode::from(remove_response.code()),
        ResponseCode::RequestCodeNotSupported
    );
    assert_eq!(
        remove_response.remark().map(|remark| remark.as_str()),
        Some("Protocol request is unsupported")
    );

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn phase5_broker_admin_request_codes_dispatch_to_admin_processor() {
    let mut runtime = new_phase3_test_runtime("phase5-dispatch").await;
    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");

    for request_code in [
        RequestCode::UpdateAndCreateTopic,
        RequestCode::UpdateAndCreateTopicList,
        RequestCode::GetAllTopicConfig,
        RequestCode::UpdateBrokerConfig,
        RequestCode::GetBrokerConfig,
        RequestCode::GetBrokerRuntimeInfo,
        RequestCode::UpdateAndCreateSubscriptionGroup,
        RequestCode::UpdateAndCreateSubscriptionGroupList,
        RequestCode::GetAllSubscriptionGroupConfig,
        RequestCode::GetTopicStatsInfo,
        RequestCode::GetConsumerConnectionList,
        RequestCode::GetProducerConnectionList,
        RequestCode::DeleteSubscriptionGroup,
        RequestCode::GetConsumeStats,
        RequestCode::GetSystemTopicListFromBroker,
        RequestCode::GetConsumerRunningInfo,
        RequestCode::ViewBrokerStatsData,
        RequestCode::GetBrokerConsumeStats,
        RequestCode::GetAllProducerInfo,
        RequestCode::DeleteTopicInBroker,
        RequestCode::GetTopicConfig,
        RequestCode::GetSubscriptionGroupConfig,
        RequestCode::UpdateAndGetGroupForbidden,
        RequestCode::UpdateAndCreateStaticTopic,
        RequestCode::UpdateColdDataFlowCtrConfig,
        RequestCode::RemoveColdDataFlowCtrConfig,
        RequestCode::GetColdDataFlowCtrInfo,
    ] {
        assert_eq!(
            processor.dispatch_processor_variant_for_test(request_code),
            Some("AdminBroker"),
            "{request_code:?} should fall back to AdminBrokerProcessor"
        );
    }

    assert_eq!(
        processor.dispatch_processor_variant_for_test(RequestCode::CheckClientConfig),
        Some("ClientManage")
    );
    for request_code in [
        RequestCode::GetBrokerLiteInfo,
        RequestCode::GetParentTopicInfo,
        RequestCode::GetLiteTopicInfo,
        RequestCode::GetLiteClientInfo,
        RequestCode::GetLiteGroupInfo,
        RequestCode::TriggerLiteDispatch,
    ] {
        assert_eq!(
            processor.dispatch_processor_variant_for_test(request_code),
            Some("LiteManager"),
            "{request_code:?} should dispatch to LiteManagerProcessor"
        );
    }
    assert_eq!(
        processor.dispatch_processor_variant_for_test(RequestCode::LiteSubscriptionCtl),
        Some("LiteSubscriptionCtl")
    );

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn phase5_subscription_group_admin_lifecycle_returns_decodable_bodies() {
    let mut runtime = new_phase3_test_runtime("phase5-subscription-group").await;
    let group = CheetahString::from_static_str("phase5-admin-group");
    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");

    let group_config = SubscriptionGroupConfig::new(group.clone());
    let mut create_request = RemotingCommand::create_remoting_command(RequestCode::UpdateAndCreateSubscriptionGroup)
        .set_body(group_config.encode().expect("subscription group config should encode"));
    let create_response = process_broker_request(&processor, &mut create_request).await;
    assert_eq!(ResponseCode::from(create_response.code()), ResponseCode::Success);

    let get_header = GetSubscriptionGroupConfigRequestHeader {
        group: group.clone(),
        rpc_request_header: None,
    };
    let mut get_request = RemotingCommand::create_request_command(RequestCode::GetSubscriptionGroupConfig, get_header);
    get_request.make_custom_header_to_net();
    let get_response = process_broker_request(&processor, &mut get_request).await;
    assert_eq!(ResponseCode::from(get_response.code()), ResponseCode::Success);
    let decoded_group = SubscriptionGroupConfig::decode(
        get_response
            .body()
            .expect("GetSubscriptionGroupConfig should include a body")
            .as_ref(),
    )
    .expect("subscription group response body should decode");
    assert_eq!(decoded_group.group_name(), &group);

    let mut list_request = RemotingCommand::create_remoting_command(RequestCode::GetAllSubscriptionGroupConfig);
    let list_response = process_broker_request(&processor, &mut list_request).await;
    assert_eq!(ResponseCode::from(list_response.code()), ResponseCode::Success);
    let list_body = std::str::from_utf8(
        list_response
            .body()
            .expect("GetAllSubscriptionGroupConfig should include a body")
            .as_ref(),
    )
    .expect("subscription group list body should be utf8");
    assert!(list_body.contains(group.as_str()));

    let delete_header = DeleteSubscriptionGroupRequestHeader {
        group_name: group.clone(),
        clean_offset: true,
        rpc_request_header: None,
    };
    let mut delete_request =
        RemotingCommand::create_request_command(RequestCode::DeleteSubscriptionGroup, delete_header);
    delete_request.make_custom_header_to_net();
    let delete_response = process_broker_request(&processor, &mut delete_request).await;
    assert_eq!(ResponseCode::from(delete_response.code()), ResponseCode::Success);
    assert!(
        !runtime
            .runtime_state_mut()
            .subscription_group_manager()
            .subscription_group_table()
            .contains_key(&group),
        "DeleteSubscriptionGroup should remove the stored group"
    );

    let missing_header = GetSubscriptionGroupConfigRequestHeader {
        group: group.clone(),
        rpc_request_header: None,
    };
    let mut missing_request =
        RemotingCommand::create_request_command(RequestCode::GetSubscriptionGroupConfig, missing_header);
    missing_request.make_custom_header_to_net();
    let missing_response = process_broker_request(&processor, &mut missing_request).await;
    assert_eq!(
        ResponseCode::from(missing_response.code()),
        ResponseCode::SubscriptionGroupNotExist
    );
    assert!(
        missing_response.body().is_none(),
        "GetSubscriptionGroupConfig must not recreate a missing group"
    );

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn phase5_admin_config_runtime_stats_and_empty_connection_queries_are_compatible() {
    let mut runtime = new_phase3_test_runtime("phase5-admin-query").await;
    let topic = CheetahString::from_static_str("phase5-admin-topic");
    let group = CheetahString::from_static_str("phase5-admin-consumer-group");
    runtime
        .runtime_state_mut()
        .topic_config_manager()
        .update_topic_config(TopicConfig::with_queues(topic.clone(), 2, 2), 0);
    let mut group_config = SubscriptionGroupConfig::new(group.clone());
    runtime
        .runtime_state_mut()
        .subscription_group_manager_mut()
        .update_subscription_group_config(&mut group_config);

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");

    let mut config_request = RemotingCommand::create_remoting_command(RequestCode::GetBrokerConfig);
    let config_response = process_broker_request(&processor, &mut config_request).await;
    assert_eq!(ResponseCode::from(config_response.code()), ResponseCode::Success);
    let config_body = std::str::from_utf8(
        config_response
            .body()
            .expect("GetBrokerConfig should include a body")
            .as_ref(),
    )
    .expect("broker config body should be utf8");
    assert!(config_body.contains("brokerName"));

    let mut runtime_request = RemotingCommand::create_remoting_command(RequestCode::GetBrokerRuntimeInfo);
    let runtime_response = process_broker_request(&processor, &mut runtime_request).await;
    assert_eq!(ResponseCode::from(runtime_response.code()), ResponseCode::Success);
    let runtime_table: KVTable = serde_json::from_slice(
        runtime_response
            .body()
            .expect("GetBrokerRuntimeInfo should include a body")
            .as_ref(),
    )
    .expect("broker runtime body should decode as KVTable");
    assert!(runtime_table.table.contains_key("brokerActive"));
    assert!(runtime_table.table.contains_key("brokerVersionDesc"));

    let topic_stats_header = GetTopicStatsRequestHeader {
        topic: topic.clone(),
        topic_request_header: None,
    };
    let mut topic_stats_request =
        RemotingCommand::create_request_command(RequestCode::GetTopicStatsInfo, topic_stats_header);
    topic_stats_request.make_custom_header_to_net();
    let topic_stats_response = process_broker_request(&processor, &mut topic_stats_request).await;
    assert_eq!(ResponseCode::from(topic_stats_response.code()), ResponseCode::Success);
    let topic_stats: TopicStatsTable = serde_json::from_slice(
        topic_stats_response
            .body()
            .expect("GetTopicStatsInfo should include a body")
            .as_ref(),
    )
    .expect("topic stats body should decode");
    assert_eq!(topic_stats.get_offset_table().len(), 2);

    let consume_stats_header = GetConsumeStatsRequestHeader {
        consumer_group: group.clone(),
        topic: topic.clone(),
        topic_list: None,
        topic_request_header: None,
    };
    let mut consume_stats_request =
        RemotingCommand::create_request_command(RequestCode::GetConsumeStats, consume_stats_header);
    consume_stats_request.make_custom_header_to_net();
    let consume_stats_response = process_broker_request(&processor, &mut consume_stats_request).await;
    assert_eq!(ResponseCode::from(consume_stats_response.code()), ResponseCode::Success);
    let consume_stats = ConsumeStats::decode(
        consume_stats_response
            .body()
            .expect("GetConsumeStats should include a body")
            .as_ref(),
    )
    .expect("consume stats body should decode");
    assert_eq!(consume_stats.get_offset_table().len(), 2);

    let consumer_connection_header = GetConsumerConnectionListRequestHeader {
        consumer_group: group.clone(),
        rpc_request_header: None,
    };
    let mut consumer_connection_request =
        RemotingCommand::create_request_command(RequestCode::GetConsumerConnectionList, consumer_connection_header);
    consumer_connection_request.make_custom_header_to_net();
    let consumer_connection_response = process_broker_request(&processor, &mut consumer_connection_request).await;
    assert_eq!(
        ResponseCode::from(consumer_connection_response.code()),
        ResponseCode::ConsumerNotOnline
    );

    let producer_connection_header = GetProducerConnectionListRequestHeader {
        producer_group: CheetahString::from_static_str("phase5-producer-group"),
        rpc_request_header: None,
    };
    let mut producer_connection_request =
        RemotingCommand::create_request_command(RequestCode::GetProducerConnectionList, producer_connection_header);
    producer_connection_request.make_custom_header_to_net();
    let producer_connection_response = process_broker_request(&processor, &mut producer_connection_request).await;
    assert_eq!(
        ResponseCode::from(producer_connection_response.code()),
        ResponseCode::SystemError
    );

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn phase6_store_delay_timer_request_codes_dispatch_to_expected_processors() {
    let mut runtime = new_phase3_test_runtime("phase6-dispatch").await;
    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");

    for request_code in [RequestCode::QueryMessage, RequestCode::ViewMessageById] {
        assert_eq!(
            processor.dispatch_processor_variant_for_test(request_code),
            Some("QueryMessage"),
            "{request_code:?} should dispatch to QueryMessageProcessor"
        );
    }

    for request_code in [
        RequestCode::SearchOffsetByTimestamp,
        RequestCode::GetMaxOffset,
        RequestCode::GetMinOffset,
        RequestCode::GetEarliestMsgStoreTime,
        RequestCode::GetAllConsumerOffset,
        RequestCode::GetAllDelayOffset,
        RequestCode::GetTimerCheckPoint,
        RequestCode::GetTimerMetrics,
        RequestCode::CleanExpiredConsumequeue,
        RequestCode::CleanUnusedTopic,
        RequestCode::QueryConsumeQueue,
        RequestCode::DeleteExpiredCommitlog,
        RequestCode::CheckRocksdbCqWriteProgress,
        RequestCode::ExportRocksdbConfigToJson,
        RequestCode::SetCommitlogReadMode,
        RequestCode::SwitchTimerEngine,
    ] {
        assert_eq!(
            processor.dispatch_processor_variant_for_test(request_code),
            Some("AdminBroker"),
            "{request_code:?} should fall back to AdminBrokerProcessor"
        );
    }

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn phase6_store_offset_and_consume_queue_queries_return_decodable_models() {
    let mut runtime = new_phase3_test_runtime("phase6-store-query").await;
    let topic = CheetahString::from_static_str("phase6-store-topic");
    runtime
        .runtime_state_mut()
        .topic_config_manager()
        .update_topic_config(TopicConfig::with_queues(topic.clone(), 1, 1), 0);

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let send_response_header =
        send_message_through_broker_processor(&processor, topic.clone(), Bytes::from_static(b"phase6-message-body"))
            .await;
    assert_eq!(send_response_header.queue_id(), 0);
    assert_eq!(send_response_header.queue_offset(), 0);

    runtime.reput_message_store_once_for_test().await;

    let max_header = GetMaxOffsetRequestHeader {
        topic: topic.clone(),
        queue_id: 0,
        committed: false,
        topic_request_header: None,
    };
    let mut max_request = RemotingCommand::create_request_command(RequestCode::GetMaxOffset, max_header);
    max_request.make_custom_header_to_net();
    let mut max_response = process_broker_request(&processor, &mut max_request).await;
    assert_eq!(ResponseCode::from(max_response.code()), ResponseCode::Success);
    max_response.make_custom_header_to_net();
    let max_response_header = max_response
        .decode_command_custom_header::<GetMaxOffsetResponseHeader>()
        .expect("GetMaxOffset should include response header");
    assert_eq!(max_response_header.offset, 1);

    let min_header = GetMinOffsetRequestHeader {
        topic: topic.clone(),
        queue_id: 0,
        topic_request_header: None,
    };
    let mut min_request = RemotingCommand::create_request_command(RequestCode::GetMinOffset, min_header);
    min_request.make_custom_header_to_net();
    let mut min_response = process_broker_request(&processor, &mut min_request).await;
    assert_eq!(ResponseCode::from(min_response.code()), ResponseCode::Success);
    min_response.make_custom_header_to_net();
    let min_response_header = min_response
        .decode_command_custom_header::<GetMinOffsetResponseHeader>()
        .expect("GetMinOffset should include response header");
    assert_eq!(min_response_header.offset, 0);

    let search_header = SearchOffsetRequestHeader {
        topic: topic.clone(),
        lite_topic: None,
        queue_id: 0,
        timestamp: 0,
        boundary_type: BoundaryType::Lower,
        topic_request_header: None,
    };
    let mut search_request =
        RemotingCommand::create_request_command(RequestCode::SearchOffsetByTimestamp, search_header);
    search_request.make_custom_header_to_net();
    let mut search_response = process_broker_request(&processor, &mut search_request).await;
    assert_eq!(ResponseCode::from(search_response.code()), ResponseCode::Success);
    search_response.make_custom_header_to_net();
    let search_response_header = search_response
        .decode_command_custom_header::<SearchOffsetResponseHeader>()
        .expect("SearchOffsetByTimestamp should include response header");
    assert_eq!(search_response_header.offset, 0);

    let earliest_header = GetEarliestMsgStoretimeRequestHeader {
        topic: topic.clone(),
        queue_id: 0,
        topic_request_header: None,
    };
    let mut earliest_request =
        RemotingCommand::create_request_command(RequestCode::GetEarliestMsgStoreTime, earliest_header);
    earliest_request.make_custom_header_to_net();
    let mut earliest_response = process_broker_request(&processor, &mut earliest_request).await;
    assert_eq!(ResponseCode::from(earliest_response.code()), ResponseCode::Success);
    earliest_response.make_custom_header_to_net();
    let earliest_response_header = earliest_response
        .decode_command_custom_header::<GetEarliestMsgStoretimeResponseHeader>()
        .expect("GetEarliestMsgStoreTime should include response header");
    assert!(earliest_response_header.timestamp >= 0);

    let query_cq_header = QueryConsumeQueueRequestHeader {
        topic: topic.clone(),
        queue_id: 0,
        index: 0,
        count: 16,
        consumer_group: None,
        topic_request_header: None,
    };
    let mut query_cq_request = RemotingCommand::create_request_command(RequestCode::QueryConsumeQueue, query_cq_header);
    query_cq_request.make_custom_header_to_net();
    let query_cq_response = process_broker_request(&processor, &mut query_cq_request).await;
    assert_eq!(ResponseCode::from(query_cq_response.code()), ResponseCode::Success);
    let query_cq_body: QueryConsumeQueueResponseBody = serde_json::from_slice(
        query_cq_response
            .body()
            .expect("QueryConsumeQueue should include a body")
            .as_ref(),
    )
    .expect("QueryConsumeQueue response body should decode");
    assert_eq!(query_cq_body.min_queue_index, 0);
    assert_eq!(query_cq_body.max_queue_index, 1);
    assert_eq!(
        query_cq_body
            .queue_data
            .expect("QueryConsumeQueue should include queue data")
            .len(),
        1
    );

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn phase6_timer_delay_and_clean_admin_requests_return_expected_responses() {
    let mut runtime = new_phase3_test_runtime("phase6-timer-clean").await;
    let timer_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: runtime.message_store_config().store_path_root_dir.clone(),
        timer_wheel_enable: true,
        ..MessageStoreConfig::default()
    });
    let timer_message_store =
        TimerMessageStore::new_with_message_store_config(timer_config, crate::test_service_context("timer-store"));
    assert!(timer_message_store.load());
    runtime.runtime_state_mut().set_timer_message_store(timer_message_store);

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");

    let mut metrics_request = RemotingCommand::create_remoting_command(RequestCode::GetTimerMetrics);
    let metrics_response = process_broker_request(&processor, &mut metrics_request).await;
    assert_eq!(ResponseCode::from(metrics_response.code()), ResponseCode::Success);
    let metrics_json: serde_json::Value = serde_json::from_slice(
        metrics_response
            .body()
            .expect("GetTimerMetrics should include a body")
            .as_ref(),
    )
    .expect("GetTimerMetrics body should decode as json");
    assert!(metrics_json.get("timerDist").is_some());

    let mut checkpoint_request = RemotingCommand::create_remoting_command(RequestCode::GetTimerCheckPoint);
    let checkpoint_response = process_broker_request(&processor, &mut checkpoint_request).await;
    assert_eq!(ResponseCode::from(checkpoint_response.code()), ResponseCode::Success);
    let checkpoint_snapshot = TimerCheckpointSnapshot::decode(
        checkpoint_response
            .body()
            .expect("GetTimerCheckPoint should include a body")
            .as_ref(),
    )
    .expect("GetTimerCheckPoint body should decode");
    assert!(checkpoint_snapshot.last_read_time_ms() >= 0);

    let mut delay_offset_request = RemotingCommand::create_remoting_command(RequestCode::GetAllDelayOffset);
    let delay_offset_response = process_broker_request(&processor, &mut delay_offset_request).await;
    assert_eq!(ResponseCode::from(delay_offset_response.code()), ResponseCode::Success);
    assert!(!delay_offset_response
        .body()
        .expect("GetAllDelayOffset should include a body")
        .is_empty());

    let mut read_mode_request =
        RemotingCommand::create_remoting_command(RequestCode::SetCommitlogReadMode).set_ext_fields(HashMap::new());
    read_mode_request.add_ext_field(
        CheetahString::from_static_str(READ_AHEAD_MODE),
        CommitLogReadMode::Normal.wire_value().to_string(),
    );
    let read_mode_response = process_broker_request(&processor, &mut read_mode_request).await;
    assert_eq!(ResponseCode::from(read_mode_response.code()), ResponseCode::Success);

    for request_code in [
        RequestCode::CleanExpiredConsumequeue,
        RequestCode::CleanUnusedTopic,
        RequestCode::DeleteExpiredCommitlog,
    ] {
        let mut request = RemotingCommand::create_remoting_command(request_code);
        let response = process_broker_request(&processor, &mut request).await;
        assert_eq!(
            ResponseCode::from(response.code()),
            ResponseCode::Success,
            "{request_code:?} should return success for local file store"
        );
    }

    let mut switch_timer_request = RemotingCommand::create_remoting_command(RequestCode::SwitchTimerEngine);
    let switch_timer_response = process_broker_request(&processor, &mut switch_timer_request).await;
    assert_eq!(
        ResponseCode::from(switch_timer_response.code()),
        ResponseCode::InvalidParameter,
        "new_phase3_test_runtime keeps timerWheelEnable disabled"
    );

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn phase7_broker_auth_request_codes_dispatch_to_admin_processor() {
    let mut runtime = new_phase3_test_runtime("phase7-auth-dispatch").await;
    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");

    for request_code in [
        RequestCode::AuthCreateUser,
        RequestCode::AuthUpdateUser,
        RequestCode::AuthDeleteUser,
        RequestCode::AuthGetUser,
        RequestCode::AuthListUsers,
        RequestCode::AuthCreateAcl,
        RequestCode::AuthUpdateAcl,
        RequestCode::AuthDeleteAcl,
        RequestCode::AuthGetAcl,
        RequestCode::AuthListAcl,
    ] {
        assert_eq!(
            processor.dispatch_processor_variant_for_test(request_code),
            Some("AdminBroker"),
            "{request_code:?} should fall back to AdminBrokerProcessor"
        );
    }

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn phase7_auth_user_admin_lifecycle_returns_decodable_models() {
    let mut runtime = new_phase3_test_runtime("phase7-auth-user").await;
    let username = CheetahString::from_static_str("phase7-user");
    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");

    let user_info = UserInfo {
        username: None,
        password: Some(CheetahString::from_static_str("secret")),
        user_type: Some(CheetahString::from_static_str("Normal")),
        user_status: Some(CheetahString::from_static_str("enable")),
    };
    let mut create_request = RemotingCommand::create_request_command(
        RequestCode::AuthCreateUser,
        CreateUserRequestHeader {
            username: username.clone(),
        },
    )
    .set_body(user_info.encode().expect("user info should encode"));
    create_request.make_custom_header_to_net();
    let create_response = process_broker_request(&processor, &mut create_request).await;
    assert_eq!(ResponseCode::from(create_response.code()), ResponseCode::Success);

    let mut get_request = RemotingCommand::create_request_command(
        RequestCode::AuthGetUser,
        GetUserRequestHeader {
            username: username.clone(),
        },
    );
    get_request.make_custom_header_to_net();
    let get_response = process_broker_request(&processor, &mut get_request).await;
    assert_eq!(ResponseCode::from(get_response.code()), ResponseCode::Success);
    let get_body = get_response.body().expect("AuthGetUser should include a body");
    let fetched_user = UserInfo::decode(get_body.as_ref()).expect("AuthGetUser body should decode");
    assert_eq!(fetched_user.username, Some(username.clone()));
    assert_eq!(fetched_user.user_type.as_deref(), Some("Normal"));
    assert_eq!(fetched_user.user_status.as_deref(), Some("enable"));

    let mut list_request = RemotingCommand::create_request_command(
        RequestCode::AuthListUsers,
        ListUsersRequestHeader {
            filter: CheetahString::from_static_str("phase7"),
        },
    );
    list_request.make_custom_header_to_net();
    let list_response = process_broker_request(&processor, &mut list_request).await;
    assert_eq!(ResponseCode::from(list_response.code()), ResponseCode::Success);
    let listed_users: Vec<UserInfo> = Vec::decode(
        list_response
            .body()
            .expect("AuthListUsers should include a body")
            .as_ref(),
    )
    .expect("AuthListUsers body should decode");
    assert!(
        listed_users
            .iter()
            .any(|user| user.username.as_ref() == Some(&username)),
        "AuthListUsers should include the created user"
    );

    let mut delete_request = RemotingCommand::create_request_command(
        RequestCode::AuthDeleteUser,
        DeleteUserRequestHeader {
            username: username.clone(),
        },
    );
    delete_request.make_custom_header_to_net();
    let delete_response = process_broker_request(&processor, &mut delete_request).await;
    assert_eq!(ResponseCode::from(delete_response.code()), ResponseCode::Success);

    let mut get_deleted_request =
        RemotingCommand::create_request_command(RequestCode::AuthGetUser, GetUserRequestHeader { username });
    get_deleted_request.make_custom_header_to_net();
    let get_deleted_response = process_broker_request(&processor, &mut get_deleted_request).await;
    assert_eq!(ResponseCode::from(get_deleted_response.code()), ResponseCode::Success);
    assert!(
        get_deleted_response.body().is_none(),
        "AuthGetUser should return success without body after deletion"
    );

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn init_processor_routes_lite_subscription_ctl_requests_to_lite_processor() {
    let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-broker-runtime-lite-{}", current_millis()));
    let broker_config = Arc::new(BrokerConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        auth_config_path: temp_root.join("auth.json").to_string_lossy().into_owned().into(),
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    assert!(runtime.initialize().await.is_ok());

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let mut request = RemotingCommand::create_request_command(RequestCode::LiteSubscriptionCtl, EmptyHeader {})
        .set_body(Bytes::from_static(b""));
    let response = dispatch_broker_request(&processor, &mut request)
        .await
        .expect("processor dispatch should succeed")
        .expect("lite subscription control should return a response");

    assert_eq!(ResponseCode::from(response.code()), ResponseCode::IllegalOperation);

    let _ = std::fs::remove_dir_all(temp_root);
}

#[tokio::test]
async fn get_broker_lite_info_returns_registry_aggregates() {
    let mut runtime = new_lite_test_runtime("broker-lite-info").await;
    seed_lite_query_state(&mut runtime);
    set_parent_topic_lite_expiration(&mut runtime, 600);
    seed_lite_bound_group(&mut runtime, "group-c");

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let mut request = RemotingCommand::create_request_command(RequestCode::GetBrokerLiteInfo, EmptyHeader {});
    let mut response = dispatch_broker_request(&processor, &mut request)
        .await
        .expect("processor dispatch should succeed")
        .expect("lite manager should return a response");

    assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
    let body = GetBrokerLiteInfoResponseBody::decode(
        response
            .take_body()
            .expect("GetBrokerLiteInfo response should contain a body")
            .as_ref(),
    )
    .expect("decode broker lite info response body");
    assert_eq!(
        body.get_store_type(),
        Some(&CheetahString::from_static_str("LocalFile"))
    );
    assert_eq!(body.get_max_lmq_num(), 32);
    assert_eq!(body.get_current_lmq_num(), 2);
    assert_eq!(body.get_lite_subscription_count(), 3);
    assert_eq!(
        body.get_topic_meta()
            .get(&CheetahString::from_static_str("parent-topic")),
        Some(&600)
    );
    assert_eq!(
        body.get_group_meta()
            .get(&CheetahString::from_static_str("parent-topic")),
        Some(&HashSet::from([
            CheetahString::from_static_str("group-a"),
            CheetahString::from_static_str("group-b"),
            CheetahString::from_static_str("group-c"),
        ]))
    );

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn get_broker_lite_info_reports_pop_lite_order_info_count() {
    let mut runtime = new_lite_test_runtime("broker-lite-order-info-count").await;
    seed_lite_query_state(&mut runtime);
    seed_lmq_message(&mut runtime, "child-a", b"lite-body").await;
    let lmq_name = CheetahString::from_string(to_lmq_name("parent-topic", "child-a").expect("child-a lmq"));

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    runtime.composition.state.lite_event_dispatcher().do_full_dispatch(
        &CheetahString::from_static_str("client-1"),
        &CheetahString::from_static_str("group-a"),
        &HashSet::from([lmq_name]),
    );

    let pop_header = PopLiteMessageRequestHeader {
        client_id: CheetahString::from_static_str("client-1"),
        consumer_group: CheetahString::from_static_str("group-a"),
        topic: CheetahString::from_static_str("parent-topic"),
        max_msg_num: 1,
        invisible_time: 60_000,
        poll_time: 0,
        born_time: current_millis() as i64,
        attempt_id: Some(CheetahString::from_static_str("attempt-1")),
        rpc: None,
    };
    let mut pop_request = RemotingCommand::create_request_command(RequestCode::PopLiteMessage, pop_header);
    pop_request.make_custom_header_to_net();
    let pop_response = dispatch_broker_request(&processor, &mut pop_request)
        .await
        .expect("pop lite should succeed")
        .expect("pop lite should return a response");
    assert_eq!(ResponseCode::from(pop_response.code()), ResponseCode::Success);

    let mut request = RemotingCommand::create_request_command(RequestCode::GetBrokerLiteInfo, EmptyHeader {});
    let mut response = dispatch_broker_request(&processor, &mut request)
        .await
        .expect("processor dispatch should succeed")
        .expect("lite manager should return a response");

    assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
    let body = GetBrokerLiteInfoResponseBody::decode(
        response
            .take_body()
            .expect("GetBrokerLiteInfo response should contain a body")
            .as_ref(),
    )
    .expect("decode broker lite info response body");
    assert_eq!(body.get_order_info_count(), 1);

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn get_parent_topic_info_returns_group_and_lite_counts() {
    let mut runtime = new_lite_test_runtime("parent-topic-info").await;
    seed_lite_query_state(&mut runtime);
    set_parent_topic_lite_expiration(&mut runtime, 600);

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let header = GetParentTopicInfoRequestHeader {
        topic: CheetahString::from_static_str("parent-topic"),
        rpc: None,
    };
    let mut request = RemotingCommand::create_request_command(RequestCode::GetParentTopicInfo, header);
    request.make_custom_header_to_net();
    let mut response = dispatch_broker_request(&processor, &mut request)
        .await
        .expect("processor dispatch should succeed")
        .expect("lite manager should return a response");

    assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
    let body = GetParentTopicInfoResponseBody::decode(
        response
            .take_body()
            .expect("GetParentTopicInfo response should contain a body")
            .as_ref(),
    )
    .expect("decode parent topic info response body");
    assert_eq!(body.get_topic(), Some(&CheetahString::from_static_str("parent-topic")));
    assert_eq!(body.get_ttl(), 600);
    assert_eq!(body.get_lmq_num(), 2);
    assert_eq!(body.get_lite_topic_count(), 2);
    assert_eq!(
        body.get_groups(),
        &HashSet::from([
            CheetahString::from_static_str("group-a"),
            CheetahString::from_static_str("group-b"),
        ])
    );

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn get_parent_topic_info_rejects_non_lite_parent_topic() {
    let mut runtime = new_lite_test_runtime("parent-topic-info-non-lite").await;
    seed_lite_query_state(&mut runtime);
    set_parent_topic_message_type(&mut runtime, "NORMAL");

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let header = GetParentTopicInfoRequestHeader {
        topic: CheetahString::from_static_str("parent-topic"),
        rpc: None,
    };
    let mut request = RemotingCommand::create_request_command(RequestCode::GetParentTopicInfo, header);
    request.make_custom_header_to_net();
    let response = dispatch_broker_request(&processor, &mut request)
        .await
        .expect("processor dispatch should succeed")
        .expect("lite manager should return a response");

    assert_eq!(ResponseCode::from(response.code()), ResponseCode::InvalidParameter);

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn get_lite_topic_info_returns_subscribers_for_matching_lite_topic() {
    let mut runtime = new_lite_test_runtime("lite-topic-info").await;
    seed_lite_query_state(&mut runtime);
    seed_lite_topic_publish_route(&mut runtime, &[CheetahString::from_static_str("other-broker")]);

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let header = GetLiteTopicInfoRequestHeader {
        parent_topic: CheetahString::from_static_str("parent-topic"),
        lite_topic: CheetahString::from_static_str("child-b"),
    };
    let mut request = RemotingCommand::create_request_command(RequestCode::GetLiteTopicInfo, header);
    request.make_custom_header_to_net();
    let mut response = dispatch_broker_request(&processor, &mut request)
        .await
        .expect("processor dispatch should succeed")
        .expect("lite manager should return a response");

    assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
    let body = GetLiteTopicInfoResponseBody::decode(
        response
            .take_body()
            .expect("GetLiteTopicInfo response should contain a body")
            .as_ref(),
    )
    .expect("decode lite topic info response body");
    assert_eq!(body.parent_topic(), &CheetahString::from_static_str("parent-topic"));
    assert_eq!(body.lite_topic(), &CheetahString::from_static_str("child-b"));
    assert!(!body.sharding_to_broker());
    assert_eq!(body.subscriber().len(), 2);
    assert!(body.subscriber().contains(&ClientGroup::from_parts(
        CheetahString::from_static_str("client-1"),
        CheetahString::from_static_str("group-a"),
    )));
    assert!(body.subscriber().contains(&ClientGroup::from_parts(
        CheetahString::from_static_str("client-2"),
        CheetahString::from_static_str("group-b"),
    )));

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn get_lite_topic_info_marks_current_broker_when_sharding_route_points_local_broker() {
    let mut runtime = new_lite_test_runtime("lite-topic-info-local-shard").await;
    seed_lite_query_state(&mut runtime);
    let broker_name = runtime
        .composition
        .state
        .broker_config()
        .broker_identity
        .broker_name
        .clone();
    seed_lite_topic_publish_route(&mut runtime, &[broker_name]);

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let header = GetLiteTopicInfoRequestHeader {
        parent_topic: CheetahString::from_static_str("parent-topic"),
        lite_topic: CheetahString::from_static_str("child-b"),
    };
    let mut request = RemotingCommand::create_request_command(RequestCode::GetLiteTopicInfo, header);
    request.make_custom_header_to_net();
    let mut response = dispatch_broker_request(&processor, &mut request)
        .await
        .expect("processor dispatch should succeed")
        .expect("lite manager should return a response");

    assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
    let body = GetLiteTopicInfoResponseBody::decode(
        response
            .take_body()
            .expect("GetLiteTopicInfo response should contain a body")
            .as_ref(),
    )
    .expect("decode lite topic info response body");
    assert!(body.sharding_to_broker());

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn get_lite_topic_info_rejects_non_lite_parent_topic() {
    let mut runtime = new_lite_test_runtime("lite-topic-info-non-lite").await;
    seed_lite_query_state(&mut runtime);
    set_parent_topic_message_type(&mut runtime, "NORMAL");

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let header = GetLiteTopicInfoRequestHeader {
        parent_topic: CheetahString::from_static_str("parent-topic"),
        lite_topic: CheetahString::from_static_str("child-b"),
    };
    let mut request = RemotingCommand::create_request_command(RequestCode::GetLiteTopicInfo, header);
    request.make_custom_header_to_net();
    let response = dispatch_broker_request(&processor, &mut request)
        .await
        .expect("processor dispatch should succeed")
        .expect("lite manager should return a response");

    assert_eq!(ResponseCode::from(response.code()), ResponseCode::InvalidParameter);

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn get_lite_client_info_returns_topics_for_bound_client() {
    let mut runtime = new_lite_test_runtime("lite-client-info").await;
    seed_lite_query_state(&mut runtime);

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let header = GetLiteClientInfoRequestHeader {
        parent_topic: Some(CheetahString::from_static_str("parent-topic")),
        group: Some(CheetahString::from_static_str("group-a")),
        client_id: Some(CheetahString::from_static_str("client-1")),
        max_count: 32,
    };
    let mut request = RemotingCommand::create_request_command(RequestCode::GetLiteClientInfo, header);
    request.make_custom_header_to_net();
    let mut response = dispatch_broker_request(&processor, &mut request)
        .await
        .expect("processor dispatch should succeed")
        .expect("lite manager should return a response");

    assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
    let body = GetLiteClientInfoResponseBody::decode(
        response
            .take_body()
            .expect("GetLiteClientInfo response should contain a body")
            .as_ref(),
    )
    .expect("decode lite client info response body");
    assert_eq!(
        body.parent_topic(),
        Some(&CheetahString::from_static_str("parent-topic"))
    );
    assert_eq!(body.group(), &CheetahString::from_static_str("group-a"));
    assert_eq!(body.client_id(), &CheetahString::from_static_str("client-1"));
    assert!(body.last_access_time() > 0);
    assert_eq!(body.last_consume_time(), 0);
    assert_eq!(body.lite_topic_count(), 2);
    assert_eq!(
        body.lite_topic_set(),
        &HashSet::from([
            CheetahString::from_static_str("child-a"),
            CheetahString::from_static_str("child-b"),
        ])
    );

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn get_lite_client_info_rejects_non_lite_parent_topic() {
    let mut runtime = new_lite_test_runtime("lite-client-info-non-lite").await;
    seed_lite_query_state(&mut runtime);
    set_parent_topic_message_type(&mut runtime, "NORMAL");

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let header = GetLiteClientInfoRequestHeader {
        parent_topic: Some(CheetahString::from_static_str("parent-topic")),
        group: Some(CheetahString::from_static_str("group-a")),
        client_id: Some(CheetahString::from_static_str("client-1")),
        max_count: 32,
    };
    let mut request = RemotingCommand::create_request_command(RequestCode::GetLiteClientInfo, header);
    request.make_custom_header_to_net();
    let response = dispatch_broker_request(&processor, &mut request)
        .await
        .expect("processor dispatch should succeed")
        .expect("lite manager should return a response");

    assert_eq!(ResponseCode::from(response.code()), ResponseCode::InvalidParameter);

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn get_lite_group_info_returns_offset_wrapper_for_specific_lite_topic() {
    let mut runtime = new_lite_test_runtime("lite-group-info-topic").await;
    seed_lite_query_state(&mut runtime);
    seed_lmq_offsets(&mut runtime, &[("child-a", 8), ("child-b", 12)]);
    seed_lmq_consumer_offset(&mut runtime, "group-a", "child-b", 0);

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let header = GetLiteGroupInfoRequestHeader {
        group: CheetahString::from_static_str("group-a"),
        lite_topic: CheetahString::from_static_str("child-b"),
        top_k: 10,
        rpc: None,
    };
    let mut request = RemotingCommand::create_request_command(RequestCode::GetLiteGroupInfo, header);
    request.make_custom_header_to_net();
    let mut response = dispatch_broker_request(&processor, &mut request)
        .await
        .expect("processor dispatch should succeed")
        .expect("lite manager should return a response");

    assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
    let body = GetLiteGroupInfoResponseBody::decode(
        response
            .take_body()
            .expect("GetLiteGroupInfo response should contain a body")
            .as_ref(),
    )
    .expect("decode lite group info response body");
    assert_eq!(body.group(), &CheetahString::from_static_str("group-a"));
    assert_eq!(body.parent_topic(), &CheetahString::from_static_str("parent-topic"));
    assert_eq!(body.lite_topic(), &CheetahString::from_static_str("child-b"));
    assert_eq!(body.total_lag_count(), 12);
    assert_eq!(body.earliest_unconsumed_timestamp(), 0);
    let offset_wrapper = body
        .lite_topic_offset_wrapper()
        .expect("specific lite topic should include offset wrapper");
    assert_eq!(offset_wrapper.get_broker_offset(), 12);
    assert_eq!(offset_wrapper.get_consumer_offset(), 0);
    assert_eq!(offset_wrapper.get_last_timestamp(), 0);

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn get_lite_group_info_returns_topk_aggregates_for_group() {
    let mut runtime = new_lite_test_runtime("lite-group-info-topk").await;
    seed_lite_query_state(&mut runtime);
    seed_lmq_offsets(&mut runtime, &[("child-a", 8), ("child-b", 12)]);
    seed_lmq_consumer_offset(&mut runtime, "group-a", "child-a", 3);
    seed_lmq_consumer_offset(&mut runtime, "group-a", "child-b", 0);

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let header = GetLiteGroupInfoRequestHeader {
        group: CheetahString::from_static_str("group-a"),
        lite_topic: CheetahString::from_static_str(""),
        top_k: 1,
        rpc: None,
    };
    let mut request = RemotingCommand::create_request_command(RequestCode::GetLiteGroupInfo, header);
    request.make_custom_header_to_net();
    let mut response = dispatch_broker_request(&processor, &mut request)
        .await
        .expect("processor dispatch should succeed")
        .expect("lite manager should return a response");

    assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
    let body = GetLiteGroupInfoResponseBody::decode(
        response
            .take_body()
            .expect("GetLiteGroupInfo response should contain a body")
            .as_ref(),
    )
    .expect("decode lite group info response body");
    assert_eq!(body.group(), &CheetahString::from_static_str("group-a"));
    assert_eq!(body.parent_topic(), &CheetahString::from_static_str("parent-topic"));
    assert!(body.lite_topic().is_empty());
    assert_eq!(body.total_lag_count(), 17);
    assert_eq!(body.earliest_unconsumed_timestamp(), 0);
    assert_eq!(body.lag_count_top_k().len(), 1);
    assert_eq!(
        body.lag_count_top_k()[0].lite_topic(),
        &CheetahString::from_static_str("child-b")
    );
    assert_eq!(body.lag_count_top_k()[0].lag_count(), 12);
    assert_eq!(body.lag_timestamp_top_k().len(), 1);
    assert_eq!(body.lag_timestamp_top_k()[0].earliest_unconsumed_timestamp(), 0);

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn get_lite_group_info_uses_offset_table_entries_not_present_in_registry() {
    let mut runtime = new_lite_test_runtime("lite-group-info-offset-table").await;
    seed_lite_query_state(&mut runtime);
    seed_lmq_offsets(&mut runtime, &[("child-a", 8), ("child-b", 12), ("child-c", 20)]);
    seed_lmq_consumer_offset(&mut runtime, "group-a", "child-b", 0);
    seed_lmq_consumer_offset(&mut runtime, "group-a", "child-c", 5);

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let header = GetLiteGroupInfoRequestHeader {
        group: CheetahString::from_static_str("group-a"),
        lite_topic: CheetahString::from_static_str(""),
        top_k: 2,
        rpc: None,
    };
    let mut request = RemotingCommand::create_request_command(RequestCode::GetLiteGroupInfo, header);
    request.make_custom_header_to_net();
    let mut response = dispatch_broker_request(&processor, &mut request)
        .await
        .expect("processor dispatch should succeed")
        .expect("lite manager should return a response");

    assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
    let body = GetLiteGroupInfoResponseBody::decode(
        response
            .take_body()
            .expect("GetLiteGroupInfo response should contain a body")
            .as_ref(),
    )
    .expect("decode lite group info response body");
    assert_eq!(body.total_lag_count(), 27);
    assert_eq!(body.lag_count_top_k().len(), 2);
    let lite_topics = body
        .lag_count_top_k()
        .iter()
        .map(|lag_info| lag_info.lite_topic().clone())
        .collect::<HashSet<_>>();
    assert_eq!(
        lite_topics,
        HashSet::from([
            CheetahString::from_static_str("child-b"),
            CheetahString::from_static_str("child-c"),
        ])
    );
    assert!(!lite_topics.contains(&CheetahString::from_static_str("child-a")));

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn trigger_lite_dispatch_enqueues_events_for_target_client() {
    let mut runtime = new_lite_test_runtime("trigger-lite-dispatch").await;
    seed_lite_query_state(&mut runtime);
    seed_lmq_offsets(&mut runtime, &[("child-a", 8), ("child-b", 12)]);

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let header = TriggerLiteDispatchRequestHeader {
        group: CheetahString::from_static_str("group-a"),
        client_id: Some(CheetahString::from_static_str("client-1")),
    };
    let mut request = RemotingCommand::create_request_command(RequestCode::TriggerLiteDispatch, header);
    request.make_custom_header_to_net();
    let response = dispatch_broker_request(&processor, &mut request)
        .await
        .expect("processor dispatch should succeed")
        .expect("lite manager should return a response");

    assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);

    let mut request = RemotingCommand::create_request_command(RequestCode::GetBrokerLiteInfo, EmptyHeader {});
    let mut response = dispatch_broker_request(&processor, &mut request)
        .await
        .expect("processor dispatch should succeed")
        .expect("broker lite info should return a response");
    let body = GetBrokerLiteInfoResponseBody::decode(
        response
            .take_body()
            .expect("GetBrokerLiteInfo response should contain a body")
            .as_ref(),
    )
    .expect("decode broker lite info response body");
    assert_eq!(body.get_event_map_size(), 1);

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn trigger_lite_dispatch_without_client_id_enqueues_events_for_group_subscribers() {
    let mut runtime = new_lite_test_runtime("trigger-lite-dispatch-group").await;
    seed_lite_query_state(&mut runtime);
    seed_lmq_offsets(&mut runtime, &[("child-a", 8), ("child-b", 12)]);

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let header = TriggerLiteDispatchRequestHeader {
        group: CheetahString::from_static_str("group-a"),
        client_id: None,
    };
    let mut request = RemotingCommand::create_request_command(RequestCode::TriggerLiteDispatch, header);
    request.make_custom_header_to_net();
    let response = dispatch_broker_request(&processor, &mut request)
        .await
        .expect("processor dispatch should succeed")
        .expect("lite manager should return a response");

    assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);

    let dispatcher = runtime.composition.state.lite_event_dispatcher();
    assert_eq!(dispatcher.event_map_size(), 1);
    assert_eq!(
        dispatcher
            .pending_events(&CheetahString::from_static_str("client-1"))
            .into_iter()
            .collect::<HashSet<_>>(),
        HashSet::from([
            CheetahString::from_string(to_lmq_name("parent-topic", "child-a").expect("child-a lmq")),
            CheetahString::from_string(to_lmq_name("parent-topic", "child-b").expect("child-b lmq")),
        ])
    );
    assert!(dispatcher
        .pending_events(&CheetahString::from_static_str("client-2"))
        .is_empty());

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn trigger_lite_dispatch_respects_broker_max_client_event_count_fallback() {
    let mut runtime = new_lite_test_runtime("trigger-lite-dispatch-max-client-event-count").await;
    seed_lite_query_state(&mut runtime);
    seed_lmq_offsets(&mut runtime, &[("child-a", 8), ("child-b", 12)]);
    let mut broker_config = runtime.runtime_state_mut().broker_config().as_ref().clone();
    broker_config.max_client_event_count = 1;
    runtime
        .runtime_state_mut()
        .set_broker_config(broker_config)
        .expect("lite dispatch test configuration should remain valid");

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let header = TriggerLiteDispatchRequestHeader {
        group: CheetahString::from_static_str("group-a"),
        client_id: Some(CheetahString::from_static_str("client-1")),
    };
    let mut request = RemotingCommand::create_request_command(RequestCode::TriggerLiteDispatch, header);
    request.make_custom_header_to_net();
    let response = dispatch_broker_request(&processor, &mut request)
        .await
        .expect("processor dispatch should succeed")
        .expect("lite manager should return a response");

    assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);

    let pending_events = runtime
        .composition
        .state
        .lite_event_dispatcher()
        .pending_events(&CheetahString::from_static_str("client-1"));
    assert_eq!(pending_events.len(), 1);
    assert!(pending_events[0].ends_with("child-a") || pending_events[0].ends_with("child-b"));

    let drained_events = runtime
        .composition
        .state
        .lite_event_dispatcher()
        .take_pending_events(&CheetahString::from_static_str("client-1"));
    assert_eq!(drained_events.len(), 2);

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn pop_lite_message_without_events_returns_polling_timeout() {
    let mut runtime = new_lite_test_runtime("pop-lite-route").await;
    seed_lite_query_state(&mut runtime);

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let header = PopLiteMessageRequestHeader {
        client_id: CheetahString::from_static_str("client-1"),
        consumer_group: CheetahString::from_static_str("group-a"),
        topic: CheetahString::from_static_str("parent-topic"),
        max_msg_num: 1,
        invisible_time: 60_000,
        poll_time: 0,
        born_time: current_millis() as i64,
        attempt_id: None,
        rpc: None,
    };
    let mut request = RemotingCommand::create_request_command(RequestCode::PopLiteMessage, header);
    request.make_custom_header_to_net();
    let response = dispatch_broker_request(&processor, &mut request)
        .await
        .expect("processor dispatch should succeed")
        .expect("pop lite should return a response");

    assert_eq!(ResponseCode::from(response.code()), ResponseCode::PollingTimeout);

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn pop_lite_message_returns_dispatched_lmq_payload_and_advances_offset() {
    let mut runtime = new_lite_test_runtime("pop-lite-consume").await;
    seed_lite_query_state(&mut runtime);
    let commit_log_offset = seed_lmq_message(&mut runtime, "child-a", b"lite-body").await;
    let expected_lmq_name = to_lmq_name("parent-topic", "child-a").expect("child-a lmq");
    let seeded = runtime
        .composition
        .state
        .message_store()
        .expect("message store should be initialized")
        .look_message_by_offset(commit_log_offset)
        .expect("seeded parent message should be readable");
    assert_eq!(seeded.topic(), &CheetahString::from_static_str("parent-topic"));
    assert_eq!(
        seeded
            .property(&CheetahString::from_static_str(
                MessageConst::PROPERTY_INNER_MULTI_DISPATCH
            ))
            .as_deref(),
        Some(expected_lmq_name.as_str())
    );
    assert_eq!(
        seeded
            .property(&CheetahString::from_static_str(
                MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET
            ))
            .as_deref(),
        Some("0")
    );
    assert_eq!(
        runtime
            .composition
            .state
            .message_store()
            .expect("message store should be initialized")
            .get_max_offset_in_queue(&CheetahString::from_static_str("parent-topic"), 0),
        1
    );
    let lmq_name = CheetahString::from_string(expected_lmq_name);
    assert_eq!(
        runtime
            .composition
            .state
            .message_store()
            .expect("message store should be initialized")
            .get_max_offset_in_queue(&lmq_name, 0),
        1
    );
    let direct_read = runtime
        .composition
        .state
        .message_store()
        .expect("message store should be initialized")
        .get_message(&CheetahString::from_static_str("group-a"), &lmq_name, 0, 0, 1, None)
        .await
        .expect("direct lmq read should return a result");
    assert_eq!(direct_read.status(), Some(GetMessageStatus::Found));
    runtime.composition.state.lite_event_dispatcher().do_full_dispatch(
        &CheetahString::from_static_str("client-1"),
        &CheetahString::from_static_str("group-a"),
        &HashSet::from([lmq_name.clone()]),
    );

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    let pop_header = PopLiteMessageRequestHeader {
        client_id: CheetahString::from_static_str("client-1"),
        consumer_group: CheetahString::from_static_str("group-a"),
        topic: CheetahString::from_static_str("parent-topic"),
        max_msg_num: 1,
        invisible_time: 60_000,
        poll_time: 0,
        born_time: current_millis() as i64,
        attempt_id: None,
        rpc: None,
    };
    let mut pop_request = RemotingCommand::create_request_command(RequestCode::PopLiteMessage, pop_header);
    pop_request.make_custom_header_to_net();
    let mut pop_response = dispatch_broker_request(&processor, &mut pop_request)
        .await
        .expect("pop lite should succeed")
        .expect("pop lite should return a response");

    assert_eq!(ResponseCode::from(pop_response.code()), ResponseCode::Success);
    let body = pop_response
        .take_body()
        .expect("pop lite success response should contain a body");
    let mut bytes = body;
    let message =
        MessageDecoder::decode(&mut bytes, true, false, false, false, false).expect("decode pop lite response message");
    assert_eq!(message.topic(), &CheetahString::from_static_str("parent-topic"));
    assert_eq!(
        message
            .property(&CheetahString::from_static_str(
                MessageConst::PROPERTY_INNER_MULTI_DISPATCH
            ))
            .as_deref(),
        Some(lmq_name.as_str())
    );
    assert_eq!(message.body(), Some(Bytes::from_static(b"lite-body")));
    assert_eq!(
        runtime.composition.state.consumer_offset_manager().query_offset(
            &CheetahString::from_static_str("group-a"),
            &lmq_name,
            0,
        ),
        1
    );
    assert!(runtime
        .composition
        .state
        .lite_event_dispatcher()
        .pending_events(&CheetahString::from_static_str("client-1"))
        .is_empty());

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn pop_lite_message_blocks_fifo_for_different_attempt_id() {
    let mut runtime = new_lite_test_runtime("pop-lite-fifo-block").await;
    seed_lite_query_state(&mut runtime);
    seed_lmq_message(&mut runtime, "child-a", b"lite-body-1").await;
    seed_lmq_message(&mut runtime, "child-a", b"lite-body-2").await;
    let lmq_name = CheetahString::from_string(to_lmq_name("parent-topic", "child-a").expect("child-a lmq"));

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    runtime.composition.state.lite_event_dispatcher().do_full_dispatch(
        &CheetahString::from_static_str("client-1"),
        &CheetahString::from_static_str("group-a"),
        &HashSet::from([lmq_name.clone()]),
    );

    let first_header = PopLiteMessageRequestHeader {
        client_id: CheetahString::from_static_str("client-1"),
        consumer_group: CheetahString::from_static_str("group-a"),
        topic: CheetahString::from_static_str("parent-topic"),
        max_msg_num: 1,
        invisible_time: 5_000,
        poll_time: 0,
        born_time: current_millis() as i64,
        attempt_id: Some(CheetahString::from_static_str("attempt-1")),
        rpc: None,
    };
    let mut first_request = RemotingCommand::create_request_command(RequestCode::PopLiteMessage, first_header);
    first_request.make_custom_header_to_net();
    let first_response = dispatch_broker_request(&processor, &mut first_request)
        .await
        .expect("first pop lite should succeed")
        .expect("first pop lite should return a response");
    assert_eq!(ResponseCode::from(first_response.code()), ResponseCode::Success);
    assert_eq!(
        runtime.composition.state.consumer_offset_manager().query_offset(
            &CheetahString::from_static_str("group-a"),
            &lmq_name,
            0
        ),
        1
    );

    let second_header = PopLiteMessageRequestHeader {
        client_id: CheetahString::from_static_str("client-1"),
        consumer_group: CheetahString::from_static_str("group-a"),
        topic: CheetahString::from_static_str("parent-topic"),
        max_msg_num: 1,
        invisible_time: 5_000,
        poll_time: 0,
        born_time: current_millis() as i64,
        attempt_id: Some(CheetahString::from_static_str("attempt-2")),
        rpc: None,
    };
    let mut second_request = RemotingCommand::create_request_command(RequestCode::PopLiteMessage, second_header);
    second_request.make_custom_header_to_net();
    let second_response = dispatch_broker_request(&processor, &mut second_request)
        .await
        .expect("second pop lite should succeed")
        .expect("second pop lite should return a response");
    assert_eq!(ResponseCode::from(second_response.code()), ResponseCode::PollingTimeout);
    assert_eq!(
        runtime.composition.state.consumer_offset_manager().query_offset(
            &CheetahString::from_static_str("group-a"),
            &lmq_name,
            0
        ),
        1
    );
    assert_eq!(
        runtime
            .composition
            .state
            .lite_event_dispatcher()
            .pending_events(&CheetahString::from_static_str("client-1")),
        vec![lmq_name.clone()]
    );

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[tokio::test]
async fn pop_lite_message_allows_same_attempt_id_to_continue_fifo_consumption() {
    let mut runtime = new_lite_test_runtime("pop-lite-fifo-same-attempt").await;
    seed_lite_query_state(&mut runtime);
    seed_lmq_message(&mut runtime, "child-a", b"lite-body-1").await;
    seed_lmq_message(&mut runtime, "child-a", b"lite-body-2").await;
    let lmq_name = CheetahString::from_string(to_lmq_name("parent-topic", "child-a").expect("child-a lmq"));

    let (processor, _) = runtime
        .init_processor_checked()
        .expect("initialize canonical processors");
    runtime.composition.state.lite_event_dispatcher().do_full_dispatch(
        &CheetahString::from_static_str("client-1"),
        &CheetahString::from_static_str("group-a"),
        &HashSet::from([lmq_name.clone()]),
    );

    let first_header = PopLiteMessageRequestHeader {
        client_id: CheetahString::from_static_str("client-1"),
        consumer_group: CheetahString::from_static_str("group-a"),
        topic: CheetahString::from_static_str("parent-topic"),
        max_msg_num: 1,
        invisible_time: 5_000,
        poll_time: 0,
        born_time: current_millis() as i64,
        attempt_id: Some(CheetahString::from_static_str("attempt-1")),
        rpc: None,
    };
    let mut first_request = RemotingCommand::create_request_command(RequestCode::PopLiteMessage, first_header);
    first_request.make_custom_header_to_net();
    let _ = dispatch_broker_request(&processor, &mut first_request)
        .await
        .expect("first pop lite should succeed")
        .expect("first pop lite should return a response");

    let second_header = PopLiteMessageRequestHeader {
        client_id: CheetahString::from_static_str("client-1"),
        consumer_group: CheetahString::from_static_str("group-a"),
        topic: CheetahString::from_static_str("parent-topic"),
        max_msg_num: 1,
        invisible_time: 5_000,
        poll_time: 0,
        born_time: current_millis() as i64,
        attempt_id: Some(CheetahString::from_static_str("attempt-1")),
        rpc: None,
    };
    let mut second_request = RemotingCommand::create_request_command(RequestCode::PopLiteMessage, second_header);
    second_request.make_custom_header_to_net();
    let second_response = dispatch_broker_request(&processor, &mut second_request)
        .await
        .expect("second pop lite should succeed")
        .expect("second pop lite should return a response");
    assert_eq!(ResponseCode::from(second_response.code()), ResponseCode::Success);
    let second_header = second_response
        .decode_command_custom_header::<PopLiteMessageResponseHeader>()
        .expect("pop lite success response should decode its wire response header");
    assert!(second_header.order_count_info.is_some());
    assert_eq!(
        runtime.composition.state.consumer_offset_manager().query_offset(
            &CheetahString::from_static_str("group-a"),
            &lmq_name,
            0
        ),
        2
    );

    let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
}

#[cfg(feature = "rocksdb_store")]
#[tokio::test]
async fn broker_runtime_keeps_local_file_config_managers_by_default() {
    let temp_root = std::env::temp_dir().join(format!(
        "rocketmq-rust-broker-runtime-config-default-{}",
        current_millis()
    ));
    let broker_config = Arc::new(BrokerConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    let inner = runtime.runtime_state_mut();

    assert!(!inner.topic_config_manager().is_rocksdb_config_enabled());
    assert!(!inner.consumer_offset_manager().is_rocksdb_config_enabled());
    assert!(!inner.subscription_group_manager().is_rocksdb_config_enabled());

    let _ = std::fs::remove_dir_all(temp_root);
}

#[cfg(feature = "rocksdb_store")]
#[tokio::test]
async fn broker_runtime_wires_rocksdb_config_managers_for_rocksdb_store_type() {
    let temp_root = std::env::temp_dir().join(format!(
        "rocketmq-rust-broker-runtime-config-rocksdb-{}",
        current_millis()
    ));
    let broker_config = Arc::new(BrokerConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        store_type: StoreType::RocksDB,
        real_time_persist_rocksdb_config: true,
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    let inner = runtime.runtime_state_mut();

    assert!(inner.topic_config_manager().is_rocksdb_config_enabled());
    assert_eq!(
        inner.topic_config_manager().rocksdb_config_path(),
        Some(temp_root.join("config").join("topics").as_path())
    );
    assert!(inner.consumer_offset_manager().is_rocksdb_config_enabled());
    assert_eq!(
        inner.consumer_offset_manager().rocksdb_config_path(),
        Some(temp_root.join("config").join("consumerOffsets").as_path())
    );
    assert!(inner.subscription_group_manager().is_rocksdb_config_enabled());
    assert_eq!(
        inner.subscription_group_manager().rocksdb_config_path(),
        Some(temp_root.join("config").join("subscriptionGroups").as_path())
    );

    let _ = std::fs::remove_dir_all(temp_root);
}

#[cfg(feature = "rocksdb_store")]
#[tokio::test]
async fn broker_runtime_uses_single_rocksdb_config_path_when_enabled() {
    let temp_root = std::env::temp_dir().join(format!(
        "rocketmq-rust-broker-runtime-config-single-{}",
        current_millis()
    ));
    let broker_config = Arc::new(BrokerConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        use_single_rocksdb_for_all_configs: true,
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        store_type: StoreType::RocksDB,
        real_time_persist_rocksdb_config: true,
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    let inner = runtime.runtime_state_mut();
    let metadata_path = temp_root.join("config").join("metadata");

    assert_eq!(
        inner.topic_config_manager().rocksdb_config_path(),
        Some(metadata_path.as_path())
    );
    assert_eq!(
        inner.consumer_offset_manager().rocksdb_config_path(),
        Some(metadata_path.as_path())
    );
    assert_eq!(
        inner.subscription_group_manager().rocksdb_config_path(),
        Some(metadata_path.as_path())
    );

    let _ = std::fs::remove_dir_all(temp_root);
}

#[cfg(feature = "rocksdb_store")]
#[tokio::test]
async fn broker_runtime_migrates_separate_file_metadata_to_rocksdb_and_recovers_without_json_files() {
    run_metadata_migration_to_rocksdb_case(false).await;
}

#[cfg(feature = "rocksdb_store")]
#[tokio::test]
async fn broker_runtime_migrates_single_file_metadata_to_rocksdb_and_recovers_without_json_files() {
    run_metadata_migration_to_rocksdb_case(true).await;
}

#[cfg(feature = "rocksdb_store")]
async fn run_metadata_migration_to_rocksdb_case(use_single_rocksdb_for_all_configs: bool) {
    let suffix = if use_single_rocksdb_for_all_configs {
        "single"
    } else {
        "separate"
    };
    let temp_root = std::env::temp_dir().join(format!(
        "rocketmq-rust-broker-runtime-config-migrate-{suffix}-{}",
        current_millis()
    ));
    let topic = CheetahString::from_static_str("MigratedTopic");
    let group = CheetahString::from_static_str("MigratedGroup");

    let mut file_runtime =
        new_metadata_config_runtime(&temp_root, StoreType::LocalFile, use_single_rocksdb_for_all_configs);
    {
        let inner = file_runtime.runtime_state_mut();
        inner
            .topic_config_manager()
            .update_topic_config(TopicConfig::with_queues(topic.clone(), 3, 5), 0);
        inner.topic_config_manager().persist().unwrap();
        inner.consumer_offset_manager().commit_offset(
            CheetahString::from_static_str("127.0.0.1:10911"),
            &group,
            &topic,
            1,
            42,
        );
        inner.consumer_offset_manager().persist().unwrap();
        let mut group_config = SubscriptionGroupConfig::new(group.clone());
        group_config.set_consume_broadcast_enable(false);
        inner
            .subscription_group_manager_mut()
            .update_subscription_group_config(&mut group_config);
        inner
            .subscription_group_manager_mut()
            .update_forbidden_value(&group, &topic, 1 << 2);
    }

    let topic_config_file = file_runtime
        .runtime_state_mut()
        .topic_config_manager()
        .config_file_path();
    let consumer_offset_file = file_runtime
        .runtime_state_mut()
        .consumer_offset_manager()
        .config_file_path();
    let subscription_group_file = file_runtime
        .runtime_state_mut()
        .subscription_group_manager()
        .config_file_path();
    drop(file_runtime);

    let mut migrating_runtime =
        new_metadata_config_runtime(&temp_root, StoreType::RocksDB, use_single_rocksdb_for_all_configs);
    {
        let inner = migrating_runtime.runtime_state_mut();
        assert!(inner.topic_config_manager().load());
        assert!(inner.consumer_offset_manager().load());
        assert!(inner.subscription_group_manager().load());
        assert_eq!(
            inner
                .topic_config_manager()
                .select_topic_config(&topic)
                .map(|config| (config.read_queue_nums, config.write_queue_nums)),
            Some((3, 5))
        );
        assert_eq!(inner.consumer_offset_manager().query_offset(&group, &topic, 1), 42);
        assert!(inner
            .subscription_group_manager()
            .find_subscription_group_config(&group)
            .is_some());
        assert!(inner
            .subscription_group_manager()
            .is_forbidden(group.as_str(), topic.as_str(), 2));
    }
    tokio::time::timeout(Duration::from_secs(5), migrating_runtime.shutdown_basic_service())
        .await
        .expect("migrating runtime shutdown should finish");
    drop(migrating_runtime);

    for file in [topic_config_file, consumer_offset_file, subscription_group_file] {
        let _ = std::fs::remove_file(&file);
        let _ = std::fs::remove_file(format!("{file}.bak"));
    }

    let mut recovered_runtime =
        new_metadata_config_runtime(&temp_root, StoreType::RocksDB, use_single_rocksdb_for_all_configs);
    {
        let inner = recovered_runtime.runtime_state_mut();
        assert!(inner.topic_config_manager().load());
        assert!(inner.consumer_offset_manager().load());
        assert!(inner.subscription_group_manager().load());
        assert_eq!(
            inner
                .topic_config_manager()
                .select_topic_config(&topic)
                .map(|config| (config.read_queue_nums, config.write_queue_nums)),
            Some((3, 5))
        );
        assert_eq!(inner.consumer_offset_manager().query_offset(&group, &topic, 1), 42);
        let recovered_group = inner
            .subscription_group_manager()
            .find_subscription_group_config(&group)
            .expect("subscription group should recover from rocksdb");
        assert!(!recovered_group.consume_broadcast_enable());
        assert!(inner
            .subscription_group_manager()
            .is_forbidden(group.as_str(), topic.as_str(), 2));
    }
    tokio::time::timeout(Duration::from_secs(5), recovered_runtime.shutdown_basic_service())
        .await
        .expect("recovered runtime shutdown should finish");

    let _ = std::fs::remove_dir_all(temp_root);
}

#[cfg(feature = "rocksdb_store")]
fn new_metadata_config_runtime(
    root: &Path,
    store_type: StoreType,
    use_single_rocksdb_for_all_configs: bool,
) -> BrokerRuntime {
    let broker_config = Arc::new(BrokerConfig {
        store_path_root_dir: root.to_string_lossy().into_owned().into(),
        use_single_rocksdb_for_all_configs,
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        store_path_root_dir: root.to_string_lossy().into_owned().into(),
        store_type,
        real_time_persist_rocksdb_config: matches!(store_type, StoreType::RocksDB),
        ..MessageStoreConfig::default()
    });
    BrokerRuntime::new(broker_config, message_store_config)
}

#[tokio::test]
async fn apply_message_store_role_change_promotes_store_to_master() {
    let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-broker-runtime-master-{}", current_millis()));
    let broker_config = Arc::new(BrokerConfig {
        enable_controller_mode: true,
        controller_addr: CheetahString::from_static_str("127.0.0.1:19876"),
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        enable_controller_mode: true,
        broker_role: BrokerRole::Slave,
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        timer_wheel_enable: false,
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    assert!(runtime.initialize_message_store().await, "initialize message store");

    let applied = runtime
        .composition
        .data_plane
        .escape_bridge_owner
        .store_capability()
        .apply_controller_role(BrokerRole::Slave, BrokerReplicaRole::Master, 0, None, 2)
        .await
        .expect("promote store to master");
    assert!(applied, "store promotion should pass every fail-closed preflight");

    let store = runtime
        .composition
        .state
        .message_store()
        .expect("message store should exist");
    let ha_service = store.get_ha_service().expect("ha service should exist");
    let runtime_info = ha_service.get_runtime_info(0);
    assert!(runtime_info.master);
    assert_eq!(runtime_info.ha_client_runtime_info.master_addr, "");
    assert_eq!(store.current_broker_role(), BrokerRole::SyncMaster);

    let _ = std::fs::remove_dir_all(temp_root);
}

#[tokio::test]
async fn apply_message_store_role_change_demotes_store_to_slave() {
    let temp_root = std::env::temp_dir().join(format!("rocketmq-rust-broker-runtime-slave-{}", current_millis()));
    let broker_config = Arc::new(BrokerConfig {
        enable_controller_mode: true,
        controller_addr: CheetahString::from_static_str("127.0.0.1:19876"),
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        enable_controller_mode: true,
        broker_role: BrokerRole::SyncMaster,
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        timer_wheel_enable: false,
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    assert!(runtime.initialize_message_store().await, "initialize message store");

    let applied = runtime
        .composition
        .data_plane
        .escape_bridge_owner
        .store_capability()
        .apply_controller_role(
            BrokerRole::SyncMaster,
            BrokerReplicaRole::Slave,
            7,
            Some(&CheetahString::from_static_str("127.0.0.1:10911")),
            3,
        )
        .await
        .expect("demote store to slave");
    assert!(applied, "store demotion should pass every fail-closed preflight");

    let store = runtime
        .composition
        .state
        .message_store()
        .expect("message store should exist");
    let ha_service = store.get_ha_service().expect("ha service should exist");
    let runtime_info = ha_service.get_runtime_info(0);
    assert!(!runtime_info.master);
    assert_eq!(runtime_info.ha_client_runtime_info.master_addr, "127.0.0.1:10911");
    assert_eq!(store.current_broker_role(), BrokerRole::Slave);

    let _ = std::fs::remove_dir_all(temp_root);
}

#[cfg(feature = "extended_timeline")]
#[tokio::test]
async fn rejected_controller_promotion_does_not_publish_role_or_start_special_services() {
    let temp_root = std::env::temp_dir().join(format!(
        "rocketmq-rust-broker-runtime-rejected-promotion-{}",
        current_millis()
    ));
    let broker_config = Arc::new(BrokerConfig {
        enable_controller_mode: true,
        controller_addr: CheetahString::from_static_str("127.0.0.1:19876"),
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        enable_controller_mode: true,
        broker_role: BrokerRole::Slave,
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        timer_wheel_enable: false,
        timer_store_mode: TimerStoreMode::ExtendedTimeline,
        timer_extended_admission_enable: true,
        timer_extended_activation_epoch: 7,
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    assert!(runtime.initialize_message_store().await, "initialize message store");
    runtime.composition.state.initialize_controller_mode();
    let controller = runtime.composition.state.build_controller_runtime();

    let applied = controller
        .apply_controller_role_change(None, Some(MASTER_ID), None, Some(1), None, HashSet::new())
        .await
        .expect("promotion rejection is not an operational failure");

    assert!(
        !applied,
        "promotion without an installed Extended snapshot must fail closed"
    );
    assert_eq!(
        runtime.composition.state.message_store_config().broker_role,
        BrokerRole::Slave
    );
    assert!(!runtime
        .composition
        .state
        .is_schedule_service_start
        .load(Ordering::Acquire));
    assert_eq!(
        runtime
            .composition
            .state
            .message_store()
            .expect("message store")
            .current_broker_role(),
        BrokerRole::Slave
    );

    drop(runtime);
    let _ = std::fs::remove_dir_all(temp_root);
}

#[cfg(feature = "extended_timeline")]
#[tokio::test]
async fn rejected_controller_demotion_publishes_fenced_policy_without_starting_services() {
    let temp_root = std::env::temp_dir().join(format!(
        "rocketmq-rust-broker-runtime-rejected-demotion-{}",
        current_millis()
    ));
    let broker_config = Arc::new(BrokerConfig {
        enable_controller_mode: true,
        controller_addr: CheetahString::from_static_str("127.0.0.1:19876"),
        ..BrokerConfig::default()
    });
    let message_store_config = Arc::new(MessageStoreConfig {
        enable_controller_mode: true,
        broker_role: BrokerRole::SyncMaster,
        store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
        timer_wheel_enable: false,
        timer_store_mode: TimerStoreMode::ExtendedTimeline,
        timer_extended_admission_enable: true,
        timer_extended_activation_epoch: 7,
        ..MessageStoreConfig::default()
    });
    let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
    assert!(runtime.initialize_message_store().await, "initialize message store");
    runtime.composition.state.initialize_controller_mode();
    let controller = runtime.composition.state.build_controller_runtime();

    let applied = controller
        .apply_controller_role_change(
            None,
            Some(MASTER_ID + 1),
            Some(CheetahString::new()),
            Some(1),
            None,
            HashSet::new(),
        )
        .await
        .expect("demotion rejection is not an operational failure");

    assert!(!applied, "demotion with an empty master endpoint must fail closed");
    assert_eq!(
        runtime.composition.state.message_store_config().broker_role,
        BrokerRole::Slave,
        "demotion policy must publish before the fallible Store transition"
    );
    assert!(!runtime
        .composition
        .state
        .is_schedule_service_start
        .load(Ordering::Acquire));
    assert_eq!(
        runtime
            .composition
            .state
            .message_store()
            .expect("message store")
            .current_broker_role(),
        BrokerRole::SyncMaster,
        "a rejected Store transition remains physically master but write-fenced"
    );
    assert!(!runtime
        .composition
        .state
        .message_store()
        .expect("message store")
        .put_message_preflight()
        .is_writeable());

    drop(runtime);
    let _ = std::fs::remove_dir_all(temp_root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_controller_two_broker_controller_mode_bootstrap() {
    let _controller_test_guard = CONTROLLER_INTEGRATION_TEST_LOCK.lock().await;
    let base_port = allocate_controller_test_base_port();
    let root = controller_cluster_root("controller-mode-integration");

    let (controllers, controller_peers) = start_controller_cluster(base_port, &root).await;
    let controller_addrs = controller_addr_list(&controller_peers);
    let controller_leader_manager = controllers
        .iter()
        .find(|manager| manager.is_leader())
        .expect("controller cluster should elect a leader")
        .clone();

    let mut broker_a = new_controller_mode_runtime(
        &root,
        "controller-mode-broker",
        base_port + 21,
        base_port + 22,
        controller_addrs.clone(),
    );
    let mut broker_b = new_controller_mode_runtime(
        &root,
        "controller-mode-broker",
        base_port + 31,
        base_port + 32,
        controller_addrs,
    );

    initialize_controller_mode_broker(&mut broker_a, "broker A").await;
    initialize_controller_mode_broker(&mut broker_b, "broker B").await;
    broker_a.start().await.expect("broker A should start");
    broker_b.start().await.expect("broker B should start");
    bootstrap_broker_against_controller(&mut broker_a, &controller_leader_manager).await;
    broker_a
        .composition
        .state
        .build_controller_runtime()
        .send_heartbeat()
        .await;
    let broker_a_controller_id = broker_a
        .composition
        .state
        .replicas_manager()
        .expect("broker A replicas manager should exist after bootstrap")
        .broker_controller_id();
    let broker_cluster_name = broker_a
        .composition
        .state
        .broker_config()
        .broker_identity
        .broker_cluster_name
        .clone();
    let broker_name = broker_a
        .composition
        .state
        .broker_config()
        .broker_identity
        .broker_name
        .clone();
    wait_until(
        Duration::from_secs(5),
        || {
            controller_leader_manager.heartbeat_manager().is_broker_active(
                broker_cluster_name.as_str(),
                broker_name.as_str(),
                broker_a_controller_id as i64,
            )
        },
        "controller leader to mark broker A active",
    )
    .await;
    bootstrap_broker_against_controller(&mut broker_b, &controller_leader_manager).await;

    wait_until(
        Duration::from_secs(10),
        || {
            let manager_a = broker_a.composition.state.replicas_manager();
            let manager_b = broker_b.composition.state.replicas_manager();
            match (manager_a, manager_b) {
                (Some(manager_a), Some(manager_b)) => {
                    manager_a.register_state() == RegisterState::Registered
                        && manager_b.register_state() == RegisterState::Registered
                        && manager_a.master_broker_id().is_some()
                        && manager_b.master_broker_id().is_some()
                        && manager_a.master_epoch() > 0
                        && manager_b.master_epoch() > 0
                }
                _ => false,
            }
        },
        "brokers to finish controller bootstrap",
    )
    .await;

    let manager_a = broker_a
        .composition
        .state
        .replicas_manager()
        .expect("broker A replicas manager should exist");
    let manager_b = broker_b
        .composition
        .state
        .replicas_manager()
        .expect("broker B replicas manager should exist");

    assert_ne!(
        manager_a.broker_controller_id(),
        manager_b.broker_controller_id(),
        "controller should allocate distinct broker ids"
    );
    assert_eq!(
        manager_a.master_broker_id(),
        manager_b.master_broker_id(),
        "both brokers should converge on the same controller master"
    );

    let master_broker_id = manager_a.master_broker_id().expect("master broker id");
    let broker_a_is_master = manager_a.broker_controller_id() == master_broker_id;
    let broker_b_is_master = manager_b.broker_controller_id() == master_broker_id;
    assert_ne!(
        broker_a_is_master, broker_b_is_master,
        "exactly one broker should become master"
    );
    assert!(
        manager_a.controller_leader_address().is_some() && manager_b.controller_leader_address().is_some(),
        "controller leader discovery should complete for both brokers"
    );

    let broker_a_sync_state = manager_a.sync_state_set().clone();
    let broker_b_sync_state = manager_b.sync_state_set().clone();
    assert_eq!(
        broker_a_sync_state, broker_b_sync_state,
        "brokers should observe the same sync state set"
    );
    assert!(
        broker_a_sync_state.contains(&(master_broker_id as i64)),
        "sync state set should contain the elected master"
    );

    let controller_metadata_target = broker_a
        .composition
        .state
        .replicas_manager()
        .expect("broker A replicas manager should exist for metadata lookup")
        .heartbeat_targets()
        .into_iter()
        .next()
        .expect("controller metadata lookup should have at least one target");
    let controller_leader = broker_a
        .composition
        .state
        .broker_outer_api
        .get_controller_metadata(&controller_metadata_target)
        .await
        .expect("query controller metadata")
        .controller_leader_address
        .expect("controller metadata should include leader address");
    let (response_header, response_body) = broker_a
        .composition
        .state
        .broker_outer_api
        .get_replica_info(
            &controller_leader,
            CheetahString::from_static_str("controller-mode-broker"),
        )
        .await
        .expect("query controller replica info");

    assert_eq!(
        response_header.master_broker_id,
        Some(master_broker_id as i64),
        "controller leader should expose the same elected master"
    );
    assert_eq!(
        response_body.get_sync_state_set().cloned().unwrap_or_default(),
        broker_a_sync_state,
        "controller leader should expose the same sync state set as brokers"
    );

    if broker_a_is_master {
        assert_eq!(
            broker_a.composition.state.message_store_config().broker_role,
            BrokerRole::SyncMaster
        );
        assert_eq!(
            broker_b.composition.state.message_store_config().broker_role,
            BrokerRole::Slave
        );
    } else {
        assert_eq!(
            broker_a.composition.state.message_store_config().broker_role,
            BrokerRole::Slave
        );
        assert_eq!(
            broker_b.composition.state.message_store_config().broker_role,
            BrokerRole::SyncMaster
        );
    }

    let ((), ()) = tokio::join!(broker_a.shutdown(), broker_b.shutdown());
    shutdown_controller_cluster(&controllers).await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_controller_two_broker_controller_mode_failover_and_rejoin() {
    let _controller_test_guard = CONTROLLER_INTEGRATION_TEST_LOCK.lock().await;
    let base_port = allocate_controller_test_base_port();
    let root = controller_cluster_root("controller-mode-failover");

    let (controllers, controller_peers) = start_controller_cluster(base_port, &root).await;
    let controller_addrs = controller_addr_list(&controller_peers);
    let controller_leader_manager = controllers
        .iter()
        .find(|manager| manager.is_leader())
        .expect("controller cluster should elect a leader")
        .clone();

    let mut broker_a = new_controller_mode_runtime(
        &root,
        "controller-mode-broker",
        base_port + 21,
        base_port + 22,
        controller_addrs.clone(),
    );
    let mut broker_b = new_controller_mode_runtime(
        &root,
        "controller-mode-broker",
        base_port + 31,
        base_port + 32,
        controller_addrs.clone(),
    );

    initialize_controller_mode_broker(&mut broker_a, "broker A").await;
    initialize_controller_mode_broker(&mut broker_b, "broker B").await;
    broker_a.start().await.expect("broker A should start");
    broker_b.start().await.expect("broker B should start");
    bootstrap_broker_against_controller(&mut broker_a, &controller_leader_manager).await;
    broker_a
        .composition
        .state
        .build_controller_runtime()
        .send_heartbeat()
        .await;
    let broker_a_controller_id = broker_a
        .composition
        .state
        .replicas_manager()
        .expect("broker A replicas manager should exist after bootstrap")
        .broker_controller_id();
    wait_until(
        Duration::from_secs(5),
        || {
            controller_leader_manager.heartbeat_manager().is_broker_active(
                "controller-test-cluster",
                "controller-mode-broker",
                broker_a_controller_id as i64,
            )
        },
        "controller leader to mark broker A active before broker B bootstrap",
    )
    .await;
    bootstrap_broker_against_controller(&mut broker_b, &controller_leader_manager).await;

    wait_until(
        Duration::from_secs(10),
        || {
            let manager_a = broker_a.composition.state.replicas_manager();
            let manager_b = broker_b.composition.state.replicas_manager();
            match (manager_a, manager_b) {
                (Some(manager_a), Some(manager_b)) => {
                    manager_a.register_state() == RegisterState::Registered
                        && manager_b.register_state() == RegisterState::Registered
                        && manager_a.master_broker_id().is_some()
                        && manager_b.master_broker_id().is_some()
                }
                _ => false,
            }
        },
        "brokers to finish controller bootstrap",
    )
    .await;

    let initial_master_id = broker_a
        .composition
        .state
        .replicas_manager()
        .expect("broker A replicas manager should exist")
        .master_broker_id()
        .expect("controller should elect an initial master");
    let broker_a_is_master = broker_a
        .composition
        .state
        .replicas_manager()
        .expect("broker A replicas manager should exist")
        .broker_controller_id()
        == initial_master_id;
    let old_master_controller_id = initial_master_id;
    let surviving_controller_id = if broker_a_is_master {
        broker_b
            .composition
            .state
            .replicas_manager()
            .expect("broker B replicas manager should exist")
            .broker_controller_id()
    } else {
        broker_a
            .composition
            .state
            .replicas_manager()
            .expect("broker A replicas manager should exist")
            .broker_controller_id()
    };

    if broker_a_is_master {
        shutdown_controller_mode_broker_for_rejoin(&mut broker_a, "broker A").await;
        broker_b
            .composition
            .state
            .build_controller_runtime()
            .send_heartbeat()
            .await;
    } else {
        shutdown_controller_mode_broker_for_rejoin(&mut broker_b, "broker B").await;
        broker_a
            .composition
            .state
            .build_controller_runtime()
            .send_heartbeat()
            .await;
    }

    wait_until(
        Duration::from_secs(15),
        || {
            let surviving_broker = if broker_a_is_master { &broker_b } else { &broker_a };
            surviving_broker
                .composition
                .state
                .replicas_manager()
                .is_some_and(|manager| {
                    manager.master_broker_id() == Some(surviving_controller_id)
                        && manager.master_epoch() > 0
                        && manager.sync_state_set().contains(&(surviving_controller_id as i64))
                })
                && surviving_broker.composition.state.message_store_config().broker_role == BrokerRole::SyncMaster
        },
        "surviving broker to be promoted to master",
    )
    .await;

    let current_controller_leader = if broker_a_is_master {
        broker_b
            .composition
            .state
            .build_controller_runtime()
            .discover_controller_leader()
            .await
            .expect("discover controller leader from surviving broker")
    } else {
        broker_a
            .composition
            .state
            .build_controller_runtime()
            .discover_controller_leader()
            .await
            .expect("discover controller leader from surviving broker")
    };
    let (replica_header_after_failover, replica_body_after_failover) = if broker_a_is_master {
        broker_b
            .composition
            .state
            .broker_outer_api
            .get_replica_info(
                &current_controller_leader,
                CheetahString::from_static_str("controller-mode-broker"),
            )
            .await
            .expect("query replica info after failover")
    } else {
        broker_a
            .composition
            .state
            .broker_outer_api
            .get_replica_info(
                &current_controller_leader,
                CheetahString::from_static_str("controller-mode-broker"),
            )
            .await
            .expect("query replica info after failover")
    };
    assert_eq!(
        replica_header_after_failover.master_broker_id,
        Some(surviving_controller_id as i64),
        "controller should expose the promoted broker as new master"
    );
    assert!(
        replica_body_after_failover
            .get_sync_state_set()
            .cloned()
            .unwrap_or_default()
            .contains(&(surviving_controller_id as i64)),
        "controller sync state set should contain the promoted master"
    );

    let rejoining_store_key = if broker_a_is_master {
        format!("broker-{}", base_port + 21)
    } else {
        format!("broker-{}", base_port + 31)
    };
    let mut rejoining_broker = new_controller_mode_runtime_with_store_key(
        &root,
        &rejoining_store_key,
        "controller-mode-broker",
        base_port + 41,
        base_port + 42,
        controller_addrs,
    );
    initialize_controller_mode_broker(&mut rejoining_broker, "rejoining broker").await;
    rejoining_broker.start().await.expect("rejoining broker should start");
    let current_leader_manager = controllers
        .iter()
        .find(|manager| manager.is_leader())
        .expect("controller cluster should keep a leader")
        .clone();
    bootstrap_broker_against_controller(&mut rejoining_broker, &current_leader_manager).await;
    rejoining_broker
        .composition
        .state
        .build_controller_runtime()
        .send_heartbeat()
        .await;

    wait_until(
        Duration::from_secs(30),
        || {
            rejoining_broker
                .composition
                .state
                .replicas_manager()
                .is_some_and(|manager| {
                    manager.register_state() == RegisterState::Registered
                        && manager.broker_controller_id() == old_master_controller_id
                        && manager.master_broker_id() == Some(surviving_controller_id)
                        && manager.sync_state_set().contains(&(surviving_controller_id as i64))
                })
                && rejoining_broker.composition.state.message_store_config().broker_role == BrokerRole::Slave
        },
        "old master to rejoin as slave",
    )
    .await;

    let surviving_broker = if broker_a_is_master { &broker_b } else { &broker_a };
    let surviving_manager = surviving_broker
        .composition
        .state
        .replicas_manager()
        .expect("surviving broker replicas manager should exist");
    let rejoining_manager = rejoining_broker
        .composition
        .state
        .replicas_manager()
        .expect("rejoining broker replicas manager should exist");
    assert_eq!(
        surviving_manager.master_broker_id(),
        Some(surviving_controller_id),
        "surviving broker should keep master view after rejoin"
    );
    assert_eq!(
        rejoining_manager.master_broker_id(),
        Some(surviving_controller_id),
        "rejoining broker should converge on surviving master"
    );
    assert_eq!(
        rejoining_manager.broker_controller_id(),
        old_master_controller_id,
        "rejoining broker should reuse persisted controller broker id"
    );
    assert_eq!(
        surviving_manager.sync_state_set(),
        rejoining_manager.sync_state_set(),
        "brokers should converge on the same controller sync state set after rejoin"
    );

    rejoining_broker.shutdown().await;
    if broker_a_is_master {
        broker_b.shutdown().await;
    } else {
        broker_a.shutdown().await;
    }
    for controller in &controllers {
        controller.shutdown().await.expect("shutdown controller manager");
    }
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_controller_two_broker_controller_mode_failover_reregisters_namesrv_and_updates_store_ha() {
    let _controller_test_guard = CONTROLLER_INTEGRATION_TEST_LOCK.lock().await;
    let base_port = allocate_controller_test_base_port();
    let root = controller_cluster_root("controller-mode-namesrv-ha");
    let mut namesrv = start_namesrv(base_port + 90, &root).await;
    let namesrv_addr = namesrv.addr();

    let (controllers, controller_peers) = start_controller_cluster(base_port, &root).await;
    let controller_addrs = controller_addr_list(&controller_peers);
    let controller_leader_manager = controllers
        .iter()
        .find(|manager| manager.is_leader())
        .expect("controller cluster should elect a leader")
        .clone();

    let mut broker_a = new_controller_mode_runtime(
        &root,
        "controller-mode-broker",
        base_port + 21,
        base_port + 22,
        controller_addrs.clone(),
    );
    let mut broker_b = new_controller_mode_runtime(
        &root,
        "controller-mode-broker",
        base_port + 31,
        base_port + 32,
        controller_addrs.clone(),
    );
    configure_namesrv(&mut broker_a, &namesrv_addr).await;
    configure_namesrv(&mut broker_b, &namesrv_addr).await;

    initialize_controller_mode_broker(&mut broker_a, "broker A").await;
    initialize_controller_mode_broker(&mut broker_b, "broker B").await;
    broker_a.start().await.expect("broker A should start");
    broker_b.start().await.expect("broker B should start");
    bootstrap_broker_against_controller(&mut broker_a, &controller_leader_manager).await;
    broker_a
        .composition
        .state
        .build_controller_runtime()
        .send_heartbeat()
        .await;
    let broker_a_controller_id = broker_a
        .composition
        .state
        .replicas_manager()
        .expect("broker A replicas manager should exist after bootstrap")
        .broker_controller_id();
    wait_until(
        Duration::from_secs(5),
        || {
            controller_leader_manager.heartbeat_manager().is_broker_active(
                "controller-test-cluster",
                "controller-mode-broker",
                broker_a_controller_id as i64,
            )
        },
        "controller leader to mark broker A active before broker B bootstrap",
    )
    .await;
    bootstrap_broker_against_controller(&mut broker_b, &controller_leader_manager).await;

    wait_until(
        Duration::from_secs(10),
        || {
            let manager_a = broker_a.composition.state.replicas_manager();
            let manager_b = broker_b.composition.state.replicas_manager();
            match (manager_a, manager_b) {
                (Some(manager_a), Some(manager_b)) => {
                    manager_a.register_state() == RegisterState::Registered
                        && manager_b.register_state() == RegisterState::Registered
                        && manager_a.master_broker_id().is_some()
                        && manager_b.master_broker_id().is_some()
                }
                _ => false,
            }
        },
        "brokers to finish controller bootstrap with namesrv enabled",
    )
    .await;

    let manager_a = broker_a
        .composition
        .state
        .replicas_manager()
        .expect("broker A replicas manager should exist");
    let manager_b = broker_b
        .composition
        .state
        .replicas_manager()
        .expect("broker B replicas manager should exist");
    let initial_master_id = manager_a
        .master_broker_id()
        .expect("controller should elect an initial master");
    let broker_a_controller_id = manager_a.broker_controller_id();
    let broker_b_controller_id = manager_b.broker_controller_id();
    let broker_a_is_master = broker_a_controller_id == initial_master_id;
    let initial_master_addr = if broker_a_is_master {
        broker_a.composition.state.get_broker_addr().clone()
    } else {
        broker_b.composition.state.get_broker_addr().clone()
    };
    let initial_slave_addr = if broker_a_is_master {
        broker_b.composition.state.get_broker_addr().clone()
    } else {
        broker_a.composition.state.get_broker_addr().clone()
    };
    let initial_slave_controller_id = if broker_a_is_master {
        broker_b_controller_id
    } else {
        broker_a_controller_id
    };
    let broker_cluster_name = broker_a
        .composition
        .state
        .broker_config()
        .broker_identity
        .broker_cluster_name
        .clone();
    let broker_name = broker_a
        .composition
        .state
        .broker_config()
        .broker_identity
        .broker_name
        .clone();

    let initial_member_group = wait_for_namesrv_member_group(
        &namesrv_addr,
        &broker_cluster_name,
        &broker_name,
        Duration::from_secs(15),
        "namesrv to reflect initial master/slave registration",
        |member_group| {
            member_group.broker_addrs.len() == 2
                && member_group.broker_addrs.get(&MASTER_ID) == Some(&initial_master_addr)
                && member_group.broker_addrs.get(&initial_slave_controller_id) == Some(&initial_slave_addr)
        },
    )
    .await;
    assert_eq!(initial_member_group.broker_addrs.len(), 2);

    let old_master_controller_id = initial_master_id;
    let surviving_controller_id = if broker_a_is_master {
        broker_b_controller_id
    } else {
        broker_a_controller_id
    };
    let surviving_broker_addr = if broker_a_is_master {
        broker_b.composition.state.get_broker_addr().clone()
    } else {
        broker_a.composition.state.get_broker_addr().clone()
    };

    if broker_a_is_master {
        shutdown_controller_mode_broker_for_rejoin(&mut broker_a, "broker A").await;
        broker_b
            .composition
            .state
            .build_controller_runtime()
            .send_heartbeat()
            .await;
    } else {
        shutdown_controller_mode_broker_for_rejoin(&mut broker_b, "broker B").await;
        broker_a
            .composition
            .state
            .build_controller_runtime()
            .send_heartbeat()
            .await;
    }

    let surviving_broker = if broker_a_is_master { &broker_b } else { &broker_a };
    let surviving_ha_addr = surviving_broker.composition.state.get_ha_server_addr();
    let surviving_store = surviving_broker
        .composition
        .state
        .message_store()
        .expect("surviving broker message store should exist");

    wait_until(
        Duration::from_secs(15),
        || {
            let Some(manager) = surviving_broker.composition.state.replicas_manager() else {
                return false;
            };
            let Some(ha_service) = surviving_store.get_ha_service() else {
                return false;
            };
            let ha_runtime_info = ha_service.get_runtime_info(0);
            manager.master_broker_id() == Some(surviving_controller_id)
                && manager.master_epoch() > 0
                && manager.sync_state_set() == &HashSet::from([surviving_controller_id as i64])
                && surviving_store.get_alive_replica_num_in_group() == 1
                && ha_runtime_info.master
                && ha_runtime_info.in_sync_slave_nums == 0
                && ha_runtime_info.ha_client_runtime_info.master_addr.is_empty()
                && surviving_broker.composition.state.message_store_config().broker_role == BrokerRole::SyncMaster
        },
        "surviving broker store/HA view to converge after controller failover",
    )
    .await;

    let member_group_after_failover = wait_for_namesrv_member_group(
        &namesrv_addr,
        &broker_cluster_name,
        &broker_name,
        Duration::from_secs(15),
        "namesrv to re-register the promoted master without stale slave entry",
        |member_group| {
            member_group.broker_addrs.len() == 1
                && member_group.broker_addrs.get(&MASTER_ID) == Some(&surviving_broker_addr)
                && !member_group.broker_addrs.contains_key(&surviving_controller_id)
        },
    )
    .await;
    assert_eq!(
        member_group_after_failover.broker_addrs,
        std::collections::HashMap::from([(MASTER_ID, surviving_broker_addr.clone())]),
        "namesrv should only retain the promoted master after old master shutdown",
    );

    let rejoining_store_key = if broker_a_is_master {
        format!("broker-{}", base_port + 21)
    } else {
        format!("broker-{}", base_port + 31)
    };
    let mut rejoining_broker = new_controller_mode_runtime_with_store_key(
        &root,
        &rejoining_store_key,
        "controller-mode-broker",
        base_port + 41,
        base_port + 42,
        controller_addrs,
    );
    configure_namesrv(&mut rejoining_broker, &namesrv_addr).await;
    initialize_controller_mode_broker(&mut rejoining_broker, "rejoining broker").await;
    rejoining_broker.start().await.expect("rejoining broker should start");
    assert!(
        rejoining_broker.composition.state.broker_pre_online_service.is_none(),
        "controller mode must not start the slave-acting-master pre-online service",
    );
    let current_leader_manager = controllers
        .iter()
        .find(|manager| manager.is_leader())
        .expect("controller cluster should keep a leader")
        .clone();
    bootstrap_broker_against_controller(&mut rejoining_broker, &current_leader_manager).await;
    rejoining_broker
        .composition
        .state
        .build_controller_runtime()
        .send_heartbeat()
        .await;

    let rejoining_store = rejoining_broker
        .composition
        .state
        .message_store()
        .expect("rejoining broker message store should exist");
    let rejoining_addr = rejoining_broker.composition.state.get_broker_addr().clone();
    wait_until(
        Duration::from_secs(30),
        || {
            let Some(rejoining_manager) = rejoining_broker.composition.state.replicas_manager() else {
                return false;
            };
            let Some(surviving_manager) = surviving_broker.composition.state.replicas_manager() else {
                return false;
            };
            let Some(surviving_ha_service) = surviving_store.get_ha_service() else {
                return false;
            };
            let Some(rejoining_ha_service) = rejoining_store.get_ha_service() else {
                return false;
            };
            let surviving_runtime_info = surviving_ha_service.get_runtime_info(0);
            let rejoining_runtime_info = rejoining_ha_service.get_runtime_info(0);
            rejoining_manager.register_state() == RegisterState::Registered
                && rejoining_manager.broker_controller_id() == old_master_controller_id
                && rejoining_manager.master_broker_id() == Some(surviving_controller_id)
                && surviving_manager.sync_state_set() == rejoining_manager.sync_state_set()
                && surviving_store.get_alive_replica_num_in_group()
                    == surviving_manager.sync_state_set().len().max(1) as i32
                && rejoining_store.get_alive_replica_num_in_group()
                    == rejoining_manager.sync_state_set().len().max(1) as i32
                && surviving_runtime_info.master
                && surviving_runtime_info.in_sync_slave_nums
                    == (surviving_manager.sync_state_set().len().max(1) as i32 - 1).max(0)
                && !rejoining_runtime_info.master
                && rejoining_runtime_info.ha_client_runtime_info.master_addr == surviving_ha_addr.as_str()
                && rejoining_broker.composition.state.message_store_config().broker_role == BrokerRole::Slave
        },
        "rejoining broker namesrv/store/HA view to converge as slave",
    )
    .await;

    let member_group_after_rejoin = wait_for_namesrv_member_group(
        &namesrv_addr,
        &broker_cluster_name,
        &broker_name,
        Duration::from_secs(15),
        "namesrv to re-register the returning slave under its controller broker id",
        |member_group| {
            member_group.broker_addrs.len() == 2
                && member_group.broker_addrs.get(&MASTER_ID) == Some(&surviving_broker_addr)
                && member_group.broker_addrs.get(&old_master_controller_id) == Some(&rejoining_addr)
                && !member_group.broker_addrs.contains_key(&surviving_controller_id)
        },
    )
    .await;
    assert_eq!(
        member_group_after_rejoin.broker_addrs.get(&MASTER_ID),
        Some(&surviving_broker_addr),
        "namesrv should advertise the promoted broker as master after rejoin",
    );
    assert_eq!(
        member_group_after_rejoin.broker_addrs.get(&old_master_controller_id),
        Some(&rejoining_addr),
        "namesrv should advertise the returning broker under its controller-assigned slave id",
    );

    rejoining_broker.shutdown().await;
    if broker_a_is_master {
        broker_b.shutdown().await;
    } else {
        broker_a.shutdown().await;
    }
    for controller in &controllers {
        let _ = controller.shutdown().await;
    }
    namesrv.shutdown().await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_controller_two_broker_controller_leader_failover_keeps_broker_view_consistent() {
    let _controller_test_guard = CONTROLLER_INTEGRATION_TEST_LOCK.lock().await;
    let base_port = allocate_controller_test_base_port();
    let root = controller_cluster_root("controller-leader-failover");

    let (controllers, controller_peers) = start_controller_cluster(base_port, &root).await;
    let controller_addrs = controller_addr_list(&controller_peers);
    let controller_leader_manager = controllers
        .iter()
        .find(|manager| manager.is_leader())
        .expect("controller cluster should elect a leader")
        .clone();

    let mut broker_a = new_controller_mode_runtime(
        &root,
        "controller-mode-broker",
        base_port + 21,
        base_port + 22,
        controller_addrs.clone(),
    );
    let mut broker_b = new_controller_mode_runtime(
        &root,
        "controller-mode-broker",
        base_port + 31,
        base_port + 32,
        controller_addrs,
    );

    initialize_controller_mode_broker(&mut broker_a, "broker A").await;
    initialize_controller_mode_broker(&mut broker_b, "broker B").await;
    broker_a.start().await.expect("broker A should start");
    broker_b.start().await.expect("broker B should start");
    bootstrap_broker_against_controller(&mut broker_a, &controller_leader_manager).await;
    broker_a
        .composition
        .state
        .build_controller_runtime()
        .send_heartbeat()
        .await;
    bootstrap_broker_against_controller(&mut broker_b, &controller_leader_manager).await;

    wait_until(
        Duration::from_secs(10),
        || {
            let manager_a = broker_a.composition.state.replicas_manager();
            let manager_b = broker_b.composition.state.replicas_manager();
            match (manager_a, manager_b) {
                (Some(manager_a), Some(manager_b)) => {
                    manager_a.register_state() == RegisterState::Registered
                        && manager_b.register_state() == RegisterState::Registered
                        && manager_a.master_broker_id().is_some()
                        && manager_b.master_broker_id().is_some()
                        && manager_a.controller_leader_address().is_some()
                        && manager_b.controller_leader_address().is_some()
                }
                _ => false,
            }
        },
        "brokers to finish controller bootstrap before controller leader failover",
    )
    .await;

    let _initial_master_broker_id = broker_a
        .composition
        .state
        .replicas_manager()
        .expect("broker A replicas manager should exist")
        .master_broker_id()
        .expect("controller should elect a broker master");
    let broker_a_controller_leader = broker_a
        .composition
        .state
        .replicas_manager()
        .and_then(|manager| manager.controller_leader_address().cloned())
        .expect("broker A should know controller leader");
    let broker_b_controller_leader = broker_b
        .composition
        .state
        .replicas_manager()
        .and_then(|manager| manager.controller_leader_address().cloned())
        .expect("broker B should know controller leader");
    assert_eq!(
        broker_a_controller_leader, broker_b_controller_leader,
        "brokers should agree on controller leader before failover"
    );

    let old_controller_leader = broker_a_controller_leader;
    let old_controller_leader_manager = controllers
        .iter()
        .find(|manager| manager.controller_config().listen_addr.to_string() == old_controller_leader.as_str())
        .expect("find old controller leader manager")
        .clone();
    tokio::time::timeout(Duration::from_secs(25), old_controller_leader_manager.shutdown())
        .await
        .expect("old controller leader shutdown should be bounded")
        .expect("shutdown old controller leader");

    wait_until(
        Duration::from_secs(15),
        || {
            controllers
                .iter()
                .filter(|manager| manager.is_running() && manager.is_leader())
                .count()
                == 1
                && controllers.iter().any(|manager| {
                    manager.is_running()
                        && manager.is_leader()
                        && manager.controller_config().listen_addr.to_string() != old_controller_leader.as_str()
                })
        },
        "remaining controller nodes to elect a new leader",
    )
    .await;

    broker_a
        .composition
        .state
        .build_controller_runtime()
        .send_heartbeat()
        .await;
    broker_b
        .composition
        .state
        .build_controller_runtime()
        .send_heartbeat()
        .await;

    let refreshed_controller_leader = controllers
        .iter()
        .find(|manager| {
            manager.is_running()
                && manager.is_leader()
                && manager.controller_config().listen_addr.to_string() != old_controller_leader.as_str()
        })
        .map(|manager| CheetahString::from_string(manager.controller_config().listen_addr.to_string()))
        .expect("remaining controller nodes should have a new leader");
    let replica_info_wait_start = std::time::Instant::now();
    let (replica_header, replica_body) = loop {
        let replica_info_result = tokio::time::timeout(
            Duration::from_secs(3),
            broker_a.composition.state.broker_outer_api.get_replica_info(
                &refreshed_controller_leader,
                CheetahString::from_static_str("controller-mode-broker"),
            ),
        )
        .await;
        let last_replica_info_state = match replica_info_result {
            Ok(Ok((header, body))) => {
                let sync_state_set = body.get_sync_state_set().cloned().unwrap_or_default();
                if let Some(master_broker_id) = header.master_broker_id {
                    if sync_state_set.contains(&master_broker_id) {
                        break (header, body);
                    }
                }
                format!(
                    "master_broker_id={:?}, sync_state_set={:?}",
                    header.master_broker_id, sync_state_set
                )
            }
            Ok(Err(error)) => error.to_string(),
            Err(_) => "get_replica_info timed out".to_owned(),
        };

        assert!(
            replica_info_wait_start.elapsed() < Duration::from_secs(10),
            "Timed out waiting for new controller leader replica info to expose broker master; last state: {}",
            last_replica_info_state
        );
        sleep(Duration::from_millis(200)).await;
    };
    let converged_master_broker_id = replica_header
        .master_broker_id
        .and_then(|id| u64::try_from(id).ok())
        .expect("new controller leader should expose broker master id");
    let expected_sync_state_set = replica_body.get_sync_state_set().cloned().unwrap_or_default();
    assert_eq!(
        replica_header.master_broker_id,
        Some(converged_master_broker_id as i64),
        "new controller leader should expose the expected broker master"
    );
    assert!(
        expected_sync_state_set.contains(&(converged_master_broker_id as i64)),
        "new controller leader should expose a sync state set containing the broker master"
    );

    wait_until(
        Duration::from_secs(25),
        || {
            let manager_a = broker_a.composition.state.replicas_manager();
            let manager_b = broker_b.composition.state.replicas_manager();
            match (manager_a, manager_b) {
                (Some(manager_a), Some(manager_b)) => {
                    manager_a.controller_leader_address() == Some(&refreshed_controller_leader)
                        && manager_b.controller_leader_address() == Some(&refreshed_controller_leader)
                        && manager_a.master_broker_id() == Some(converged_master_broker_id)
                        && manager_b.master_broker_id() == Some(converged_master_broker_id)
                        && manager_a.sync_state_set() == &expected_sync_state_set
                        && manager_b.sync_state_set() == &expected_sync_state_set
                }
                _ => false,
            }
        },
        "brokers to refresh controller leader and converge broker master view",
    )
    .await;

    let broker_a_manager = broker_a
        .composition
        .state
        .replicas_manager()
        .expect("broker A replicas manager should exist");
    let broker_b_manager = broker_b
        .composition
        .state
        .replicas_manager()
        .expect("broker B replicas manager should exist");
    let broker_a_is_master = broker_a_manager.broker_controller_id() == converged_master_broker_id;
    let broker_b_is_master = broker_b_manager.broker_controller_id() == converged_master_broker_id;
    assert_ne!(
        broker_a_is_master, broker_b_is_master,
        "controller leader failover should keep exactly one broker master"
    );
    if broker_a_is_master {
        assert_eq!(
            broker_a.composition.state.message_store_config().broker_role,
            BrokerRole::SyncMaster
        );
        assert_eq!(
            broker_b.composition.state.message_store_config().broker_role,
            BrokerRole::Slave
        );
    } else {
        assert_eq!(
            broker_a.composition.state.message_store_config().broker_role,
            BrokerRole::Slave
        );
        assert_eq!(
            broker_b.composition.state.message_store_config().broker_role,
            BrokerRole::SyncMaster
        );
    }

    broker_a.shutdown().await;
    broker_b.shutdown().await;
    for controller in &controllers {
        let _ = controller.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(root);
}
