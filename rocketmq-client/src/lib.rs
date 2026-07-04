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

#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::result_large_err)]
#![recursion_limit = "256"]

extern crate core;

// Define macros at crate root so they're available throughout the crate
/// Create a client error with optional response code
#[macro_export]
macro_rules! mq_client_err {
    // Handle errors with a custom ResponseCode and formatted string
    ($response_code:expr, $fmt:expr, $($arg:expr),*) => {{
        let formatted_msg = format!($fmt, $($arg),*);
        let error_message = format!("CODE: {}  DESC: {}", $response_code as i32, formatted_msg);
        let faq_msg = rocketmq_common::common::FAQUrl::attach_default_url(Some(error_message.as_str()));
        rocketmq_error::RocketMQError::illegal_argument(faq_msg)
    }};

    ($response_code:expr, $error_message:expr) => {{
        let error_message = format!("CODE: {}  DESC: {}", $response_code as i32, $error_message);
        let faq_msg = rocketmq_common::common::FAQUrl::attach_default_url(Some(error_message.as_str()));
        rocketmq_error::RocketMQError::illegal_argument(faq_msg)
    }};

    // Handle errors without a ResponseCode, using only the error message (accepts both &str and String)
    ($error_message:expr) => {{
        let error_msg = format!("{}", $error_message);
        let faq_msg = rocketmq_common::common::FAQUrl::attach_default_url(Some(error_msg.as_str()));
        rocketmq_error::RocketMQError::illegal_argument(faq_msg)
    }};
}

/// Create a broker operation error
#[macro_export]
macro_rules! client_broker_err {
    // Handle errors with a custom ResponseCode and formatted string
    ($response_code:expr, $error_message:expr, $broker_addr:expr) => {{
        rocketmq_error::RocketMQError::broker_operation_failed(
            "BROKER_OPERATION",
            $response_code as i32,
            $error_message,
        )
        .with_broker_addr($broker_addr)
    }};
    // Handle errors without a ResponseCode, using only the error message
    ($response_code:expr, $error_message:expr) => {{
        rocketmq_error::RocketMQError::broker_operation_failed(
            "BROKER_OPERATION",
            $response_code as i32,
            $error_message,
        )
    }};
}

// Define client_error module
pub mod client_error;

pub mod admin;
pub mod base;
pub mod common;
pub mod consumer;
pub mod exception;
pub mod factory;
mod hook;
pub mod implementation;
pub mod latency;
pub mod legacy;
pub mod lock;
pub mod producer;
mod runtime;
pub mod stat;
mod trace;
mod types;
pub mod utils;

pub use crate::admin::DefaultMQAdminExt;
pub use crate::admin::DefaultMQAdminExtImpl;
pub use crate::admin::MQAdminExt;
pub use crate::admin::MQAdminExtInner;
pub use crate::admin::MQAdminExtInnerImpl;
pub use crate::base::MQAdmin;
pub use crate::base::MqClientAdmin;
pub use crate::base::MqClientAdminInner;
pub use crate::common::acl::AclConstants;
pub use crate::common::acl::AclException;
pub use crate::common::acl::AclSigner;
pub use crate::common::acl::AclUtils;
pub use crate::common::acl::Permission;
pub use crate::common::acl::SigningAlgorithm;
pub use crate::common::acl_client_rpc_hook::AclClientRPCHook;
pub use crate::common::admin_tool_result::AdminToolResult;
pub use crate::common::admin_tools_result_code_enum::AdminToolsResultCodeEnum;
pub use crate::common::nameserver_access_config::NameserverAccessConfig;
pub use crate::common::session_credentials::SessionCredentials;
#[doc(hidden)]
pub use crate::consumer::consumer_impl::consume_message_concurrently_service::run_concurrent_clean_expire_lifecycle_probe;
#[doc(hidden)]
pub use crate::consumer::consumer_impl::consume_message_concurrently_service::ConcurrentCleanExpireLifecycleProbe;
#[doc(hidden)]
pub use crate::consumer::consumer_impl::consume_message_orderly_service::run_orderly_lock_periodic_lifecycle_probe;
#[doc(hidden)]
pub use crate::consumer::consumer_impl::consume_message_orderly_service::OrderlyLockPeriodicLifecycleProbe;
#[doc(hidden)]
pub use crate::consumer::consumer_impl::consume_message_pop_orderly_service::run_pop_orderly_lock_refresh_lifecycle_probe;
#[doc(hidden)]
pub use crate::consumer::consumer_impl::consume_message_pop_orderly_service::PopOrderlyLockRefreshLifecycleProbe;
#[doc(hidden)]
pub use crate::consumer::consumer_impl::default_lite_pull_consumer_impl::run_lite_pull_task_lifecycle_probe;
#[doc(hidden)]
pub use crate::consumer::consumer_impl::default_lite_pull_consumer_impl::LitePullTaskLifecycleProbe;
#[doc(hidden)]
pub use crate::consumer::consumer_impl::pull_message_service::run_pull_message_service_lifecycle_probe;
#[doc(hidden)]
pub use crate::consumer::consumer_impl::pull_message_service::PullMessageServiceLifecycleProbe;
pub use crate::consumer::consumer_impl::pull_request_ext::PullResultExt;
#[doc(hidden)]
pub use crate::consumer::consumer_impl::re_balance::rebalance_service::run_rebalance_service_lifecycle_probe;
#[doc(hidden)]
pub use crate::consumer::consumer_impl::re_balance::rebalance_service::RebalanceServiceLifecycleProbe;
pub use crate::consumer::notify_result::NotifyResult;
#[doc(hidden)]
pub use crate::consumer::store::local_file_offset_store::run_local_file_offset_store_lifecycle_probe;
#[doc(hidden)]
pub use crate::consumer::store::local_file_offset_store::LocalFileOffsetStoreLifecycleProbe;
pub use crate::consumer::AbstractAllocateMessageQueueStrategy;
pub use crate::consumer::AckCallback;
pub use crate::consumer::AckCallbackFn;
pub use crate::consumer::AckResult;
pub use crate::consumer::AckStatus;
pub use crate::consumer::AllocateMachineRoomNearby;
pub use crate::consumer::AllocateMessageQueueAveragely;
pub use crate::consumer::AllocateMessageQueueAveragelyByCircle;
pub use crate::consumer::AllocateMessageQueueByConfig;
pub use crate::consumer::AllocateMessageQueueByMachineRoom;
pub use crate::consumer::AllocateMessageQueueByMachineRoomNearby;
pub use crate::consumer::AllocateMessageQueueConsistentHash;
pub use crate::consumer::AllocateMessageQueueStrategy;
pub use crate::consumer::ArcMessageQueueListener;
pub use crate::consumer::ConsumeConcurrentlyContext;
pub use crate::consumer::ConsumeConcurrentlyStatus;
pub use crate::consumer::ConsumeOrderlyContext;
pub use crate::consumer::ConsumeOrderlyStatus;
pub use crate::consumer::ConsumerTuningProfile;
pub use crate::consumer::ControllableOffset;
pub use crate::consumer::DefaultLitePullConsumer;
pub use crate::consumer::DefaultLitePullConsumerBuilder;
pub use crate::consumer::DefaultMQPushConsumer;
pub use crate::consumer::DefaultMQPushConsumerBuilder;
pub use crate::consumer::HashFunction;
pub use crate::consumer::LitePullConsumer;
pub use crate::consumer::LocalFileOffsetStore;
pub use crate::consumer::MQConsumer;
pub use crate::consumer::MQConsumerInner;
pub use crate::consumer::MQPushConsumer;
pub use crate::consumer::MachineRoomResolver;
pub use crate::consumer::MessageListener;
pub use crate::consumer::MessageListenerConcurrently;
pub use crate::consumer::MessageListenerOrderly;
pub use crate::consumer::MessageQueueListener;
pub use crate::consumer::MessageSelector;
pub use crate::consumer::OffsetSerialize;
pub use crate::consumer::OffsetSerializeWrapper;
pub use crate::consumer::OffsetStore;
pub use crate::consumer::PopCallback;
pub use crate::consumer::PopCallbackFn;
pub use crate::consumer::PopResult;
pub use crate::consumer::PopStatus;
pub use crate::consumer::PullCallback;
pub use crate::consumer::PullCallbackFn;
pub use crate::consumer::PullResult;
pub use crate::consumer::PullStatus;
pub use crate::consumer::ReadOffsetType;
pub use crate::consumer::RemoteBrokerOffsetStore;
pub use crate::consumer::TopicMessageQueueChangeListener;
pub use crate::exception::MQBrokerException;
pub use crate::exception::MQClientException;
pub use crate::exception::OffsetNotFoundException;
pub use crate::exception::RequestTimeoutException;
#[doc(hidden)]
pub use crate::factory::mq_client_instance::run_connection_event_listener_lifecycle_probe;
#[doc(hidden)]
pub use crate::factory::mq_client_instance::run_heartbeat_route_index_probe;
#[doc(hidden)]
pub use crate::factory::mq_client_instance::run_route_refresh_concurrent_stale_guard_probe;
#[doc(hidden)]
pub use crate::factory::mq_client_instance::run_route_refresh_shard_probe;
#[doc(hidden)]
pub use crate::factory::mq_client_instance::ConnectionEventListenerLifecycleProbe;
#[doc(hidden)]
pub use crate::factory::mq_client_instance::HeartbeatRouteIndexProbe;
#[doc(hidden)]
pub use crate::factory::mq_client_instance::RouteRefreshConcurrentProbe;
#[doc(hidden)]
pub use crate::factory::mq_client_instance::RouteRefreshShardProbe;
pub use crate::hook::consume_message_context::ConsumeMessageContext;
pub use crate::hook::consume_message_hook::ConsumeMessageHook;
pub use crate::hook::consume_message_hook::ConsumeMessageHookArc;
#[doc(hidden)]
pub use crate::implementation::mq_client_api_factory::run_namesrv_refresh_lifecycle_probe;
pub use crate::implementation::mq_client_api_factory::MQClientAPIFactory;
#[doc(hidden)]
pub use crate::implementation::mq_client_api_factory::NamesrvRefreshLifecycleProbe;
#[doc(hidden)]
pub use crate::runtime::client_runtime_fallback_snapshot;
#[doc(hidden)]
pub use crate::runtime::reset_client_runtime_fallback_for_diagnostics;
#[doc(hidden)]
pub use crate::runtime::spawn_client_runtime_probe_task;
#[doc(hidden)]
pub use crate::runtime::ClientRuntimeTaskHandle;
#[doc(hidden)]
pub use crate::runtime::ClientSharedFallbackLifecycleState;
#[doc(hidden)]
pub use crate::runtime::ClientSharedFallbackSnapshot;
#[doc(hidden)]
pub use crate::stat::consumer_stats_manager::run_consumer_stats_manager_lifecycle_probe;
#[doc(hidden)]
pub use crate::stat::consumer_stats_manager::ConsumerStatsManagerLifecycleProbe;
pub type MQClientAPIExt = crate::implementation::mq_client_api_impl::MQClientAPIImpl;
pub type MqClientAdminImpl = crate::implementation::mq_client_api_impl::MQClientAPIImpl;
#[doc(hidden)]
pub use crate::latency::latency_fault_tolerance_impl::run_latency_fault_detector_lifecycle_probe;
#[doc(hidden)]
pub use crate::latency::latency_fault_tolerance_impl::LatencyFaultDetectorLifecycleProbe;
pub use crate::latency::BrokerFilter;
pub use crate::latency::MQFaultStrategy;
pub use crate::latency::Resolver;
pub use crate::latency::ServiceDetector;
#[allow(deprecated)]
pub use crate::legacy::ConsumeMessageOpenTracingHookImpl;
pub use crate::legacy::ConsumeRequest;
#[allow(deprecated)]
pub use crate::legacy::DefaultMQPullConsumer;
#[allow(deprecated)]
pub use crate::legacy::DefaultMQPullConsumerImpl;
pub use crate::legacy::DoNothingClientRemotingProcessor;
#[allow(deprecated)]
pub use crate::legacy::EndTransactionOpenTracingHookImpl;
#[allow(deprecated)]
pub use crate::legacy::MQHelper;
#[allow(deprecated)]
pub use crate::legacy::MQPullConsumer;
#[allow(deprecated)]
pub use crate::legacy::MQPullConsumerScheduleService;
#[allow(deprecated)]
pub use crate::legacy::PullTaskCallback;
#[allow(deprecated)]
pub use crate::legacy::PullTaskContext;
#[allow(deprecated)]
pub use crate::legacy::PullTaskImpl;
pub use crate::legacy::RebalanceImpl;
#[allow(deprecated)]
pub use crate::legacy::RebalancePullImpl;
#[allow(deprecated)]
pub use crate::legacy::SendMessageOpenTracingHookImpl;
#[allow(deprecated)]
pub use crate::legacy::TransactionCheckListener;
pub use crate::lock::ReadWriteCASLock;
#[doc(hidden)]
pub use crate::producer::produce_accumulator::run_produce_accumulator_guard_lifecycle_probe;
#[doc(hidden)]
pub use crate::producer::produce_accumulator::ProduceAccumulatorGuardLifecycleProbe;
#[doc(hidden)]
pub use crate::producer::request_future_holder::run_request_future_holder_lifecycle_probe;
#[doc(hidden)]
pub use crate::producer::request_future_holder::run_request_future_holder_scan_probe;
#[doc(hidden)]
pub use crate::producer::request_future_holder::RequestFutureHolderLifecycleProbe;
#[doc(hidden)]
pub use crate::producer::request_future_holder::RequestFutureHolderScanProbe;
pub use crate::producer::DefaultMQProducer;
pub use crate::producer::JavaHashCode;
pub use crate::producer::LocalTransactionState;
pub use crate::producer::MQProducer;
pub use crate::producer::MessageQueueSelector;
pub use crate::producer::MessageQueueSelectorFn;
pub use crate::producer::RequestCallback;
pub use crate::producer::SelectMessageQueueByHash;
pub use crate::producer::SelectMessageQueueByMachineRoom;
pub use crate::producer::SelectMessageQueueByRandom;
pub use crate::producer::SendCallback;
pub use crate::producer::SendResult;
pub use crate::producer::SendStatus;
pub use crate::producer::TransactionListener;
pub use crate::producer::TransactionMQProducer;
pub use crate::producer::TransactionMQProducerBuilder;
pub use crate::producer::TransactionSendResult;
#[doc(hidden)]
pub use crate::trace::async_trace_dispatcher::run_trace_queue_depth_accounting_probe;
#[doc(hidden)]
pub use crate::trace::async_trace_dispatcher::run_trace_worker_lifecycle_probe;
pub use crate::trace::async_trace_dispatcher::AsyncTraceDispatcher;
#[doc(hidden)]
pub use crate::trace::async_trace_dispatcher::TraceWorkerLifecycleProbe;
pub use crate::trace::trace_data_encoder::TraceDataEncoder;
pub use crate::trace::trace_dispatcher::ArcTraceDispatcher;
pub use crate::trace::trace_dispatcher::TraceDispatcher;
pub use crate::trace::trace_dispatcher::Type as TraceDispatcherOperation;
pub use crate::trace::trace_dispatcher_type::TraceDispatcherType;
pub use crate::trace::trace_type::TraceType;

#[cfg(test)]
mod tests {
    #[test]
    fn mq_client_err_without_response_code_preserves_message() {
        let err = crate::mq_client_err!("simple client error");

        match err {
            rocketmq_error::RocketMQError::IllegalArgument(message) => {
                assert!(message.contains("simple client error"));
                assert!(!message.contains("Body is empty"));
            }
            other => panic!("expected illegal argument error, got {other:?}"),
        }
    }
}
