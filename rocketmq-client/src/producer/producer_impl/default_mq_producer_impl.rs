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

use std::any::Any;
use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::Weak;
use std::time::Duration;
use std::time::Instant;

use arc_swap::ArcSwap;
use parking_lot::Mutex as ParkingLotMutex;
use parking_lot::RwLock as ParkingLotRwLock;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rand::random;
use rocketmq_error::RocketMQError;
use rocketmq_model::common::base::service_state::ServiceState;
use rocketmq_model::common::compression::compression_type::CompressionType;
use rocketmq_model::common::message::message_accessor::MessageAccessor;
use rocketmq_model::common::message::message_batch::MessageBatch;
use rocketmq_model::common::message::message_client_id_setter::MessageClientIDSetter;
use rocketmq_model::common::message::message_enum::MessageType;
use rocketmq_model::common::message::message_ext::MessageExt;
use rocketmq_model::common::message::message_queue::MessageQueue;
use rocketmq_model::common::message::message_single::Message;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_model::common::mix_all;
use rocketmq_model::common::mix_all::CLIENT_INNER_PRODUCER_GROUP;
use rocketmq_model::common::mix_all::DEFAULT_PRODUCER_GROUP;
use rocketmq_model::common::producer::RecallMessageHandle;
use rocketmq_model::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_model::common::FAQUrl;
use rocketmq_model::utils::correlation_id_util::CorrelationIdUtil;
use rocketmq_protocol::common::compression::compressor::Compressor;
use rocketmq_protocol::common::message::message_decoder as MessageDecoder;
use rocketmq_protocol::protocol::header::check_transaction_state_request_header::CheckTransactionStateRequestHeader;
use rocketmq_protocol::protocol::header::end_transaction_request_header::EndTransactionRequestHeader;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_protocol::protocol::header::recall_message_request_header::RecallMessageRequestHeader;
use rocketmq_protocol::protocol::namespace_util::NamespaceUtil;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_runtime::ChildServiceContext;
use rocketmq_transport::RPCHook;
use rocketmq_transport::RpcRequestHeader;
use rocketmq_transport::TopicRequestHeader;
use tokio::sync::watch;
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::base::query_result::QueryResult;
use crate::base::validators::Validators;
use crate::common::client_error_code::ClientErrorCode;
use crate::common::retry_decision::producer_send_fault_decision;
use crate::common::retry_decision::producer_send_retry_decision;
use crate::common::retry_decision::ClientRetryDecision;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::hook::check_forbidden_context::CheckForbiddenContext;
use crate::hook::check_forbidden_hook::CheckForbiddenHook;
use crate::hook::end_transaction_context::EndTransactionContext;
use crate::hook::end_transaction_hook::EndTransactionHook;
use crate::hook::send_message_context::SendMessageContext;
use crate::hook::send_message_context::SendMessageTraceSnapshot;
use crate::hook::send_message_hook::SendMessageHook;
use crate::implementation::communication_mode::CommunicationMode;
use crate::implementation::mq_client_manager::ClientPool;
use crate::implementation::mq_client_manager::ClientPoolToken;
use crate::latency::mq_fault_strategy::MQFaultStrategy;
use crate::latency::resolver::Resolver;
use crate::latency::service_detector::ServiceDetector;
use crate::producer::default_mq_producer::ProducerConfig;
use crate::producer::default_mq_producer::MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM;
use crate::producer::default_mq_producer::MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE;
use crate::producer::local_transaction_state::LocalTransactionState;
use crate::producer::producer_impl::egress::BoundedEgress;
use crate::producer::producer_impl::egress::OnewayEgressSnapshot;
use crate::producer::producer_impl::egress::OnewayEnvelope;
use crate::producer::producer_impl::mq_producer_inner::MQProducerInner;
use crate::producer::producer_impl::mq_producer_inner::MQProducerInnerImpl;
use crate::producer::producer_impl::topic_publish_info::TopicPublishInfo;
use crate::producer::request_callback::RequestCallbackFn;
use crate::producer::request_future_holder::RequestFutureHolder;
use crate::producer::request_response_future::RequestResponseFuture;
use crate::producer::send_callback::ArcSendCallback;
use crate::producer::send_result::SendResult;
use crate::producer::send_status::SendStatus;
use crate::producer::transaction_listener::ArcTransactionListener;
use crate::producer::transaction_send_result::TransactionSendResult;
use crate::runtime::spawn_client_blocking_io_with_context;
use crate::runtime::spawn_client_task_with_context;
use crate::runtime::ClientRuntime;

type Topic = CheetahString;
type TopicPublishInfoSnapshot = Arc<TopicPublishInfo>;

#[derive(Clone)]
struct ProducerSendConfigSnapshot {
    producer_group: CheetahString,
    create_topic_key: CheetahString,
    default_topic_queue_nums: i32,
    compress_msg_body_over_howmuch: usize,
    compress_level: i32,
    compress_type: CompressionType,
    compressor: Option<&'static (dyn Compressor + Send + Sync)>,
    unit_mode: bool,
}

impl ProducerSendConfigSnapshot {
    fn new(client_config: &ClientConfig, producer_config: &ProducerConfig) -> Self {
        Self {
            producer_group: producer_config.producer_group().clone(),
            create_topic_key: producer_config.create_topic_key().clone(),
            default_topic_queue_nums: producer_config.default_topic_queue_nums() as i32,
            compress_msg_body_over_howmuch: producer_config.compress_msg_body_over_howmuch() as usize,
            compress_level: producer_config.compress_level(),
            compress_type: producer_config.compress_type(),
            compressor: producer_config.compressor(),
            unit_mode: client_config.unit_mode,
        }
    }
}

const QUERY_UNIQ_KEY_LOOKBACK_MILLIS: u64 = 3 * 24 * 60 * 60 * 1000;
const PRODUCER_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const PRODUCER_TASK_FORCE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

/// Producer state machine (atomic)
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProducerState {
    Created = 0,
    Starting = 1,
    Running = 2,
    Stopping = 3,
    Stopped = 4,
    StartFailed = 5,
}

impl ProducerState {
    #[inline]
    fn from_u8(val: u8) -> Self {
        match val {
            0 => Self::Created,
            1 => Self::Starting,
            2 => Self::Running,
            3 => Self::Stopping,
            4 => Self::Stopped,
            5 => Self::StartFailed,
            _ => Self::Stopped,
        }
    }
}

#[derive(Clone)]
struct TransactionCheckEnv {
    request_slots: Arc<Semaphore>,
    worker_slots: Arc<Semaphore>,
}

#[derive(Clone)]
struct ProducerRuntimeSnapshot {
    client_config: ClientConfig,
    producer_config: Arc<ProducerConfig>,
    send_config: ProducerSendConfigSnapshot,
}

impl ProducerRuntimeSnapshot {
    fn new(client_config: ClientConfig, producer_config: ProducerConfig) -> Self {
        let send_config = ProducerSendConfigSnapshot::new(&client_config, &producer_config);
        Self {
            client_config,
            producer_config: Arc::new(producer_config),
            send_config,
        }
    }
}

#[derive(Clone, Default)]
struct TransactionRuntime {
    listener: Option<ArcTransactionListener>,
    check_env: Option<TransactionCheckEnv>,
}

fn spawn_producer_task<F>(
    service_context: &ChildServiceContext,
    thread_name: &'static str,
    tracker: &TaskTracker,
    shutdown_token: &CancellationToken,
    task: F,
) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    if shutdown_token.is_cancelled() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "producer is shutting down",
        ));
    }

    let shutdown_token = shutdown_token.clone();
    let tracked_task = tracker.track_future(async move {
        let mut task = Box::pin(task);
        tokio::select! {
            biased;
            _ = shutdown_token.cancelled() => {}
            _ = &mut task => {}
        }
    });

    drop(spawn_client_task_with_context(
        service_context,
        thread_name,
        tracked_task,
    )?);
    Ok(())
}

/// Send context - encapsulates mutable state during message sending
struct SendContext {
    invoke_id: u64,
    start_time: Instant,
    timeout_ms: u64,
    communication_mode: CommunicationMode,
}

impl SendContext {
    fn new(timeout_ms: u64, communication_mode: CommunicationMode) -> Self {
        Self {
            invoke_id: random::<u64>(),
            start_time: Instant::now(),
            timeout_ms,
            communication_mode,
        }
    }

    #[inline]
    fn elapsed(&self) -> u64 {
        self.start_time.elapsed().as_millis() as u64
    }

    #[inline]
    fn remaining_timeout(&self) -> u64 {
        self.timeout_ms.saturating_sub(self.elapsed())
    }

    fn check_timeout(&self) -> rocketmq_error::RocketMQResult<()> {
        if self.elapsed() >= self.timeout_ms {
            return Err(rocketmq_error::RocketMQError::Timeout {
                operation: "send_with_retry",
                timeout_ms: self.timeout_ms,
            });
        }
        Ok(())
    }
}

/// Retry state tracker
struct RetryState {
    times_total: u32,
    brokers_sent: Vec<String>,
    last_error: Option<rocketmq_error::RocketMQError>,
}

impl RetryState {
    fn new(times_total: u32) -> Self {
        Self {
            times_total,
            brokers_sent: vec![String::new(); times_total as usize],
            last_error: None,
        }
    }

    fn record_broker(&mut self, attempt: usize, broker_name: &str) {
        if attempt < self.brokers_sent.len() {
            self.brokers_sent[attempt] = broker_name.to_string();
        }
    }

    fn set_error(&mut self, error: rocketmq_error::RocketMQError) {
        self.last_error = Some(error);
    }

    fn build_failure_error(&self, topic: &CheetahString, elapsed_ms: u128) -> rocketmq_error::RocketMQError {
        let info = format!(
            "Send [{}] times, still failed, cost [{}]ms, Topic:{}, BrokersSent: {} {}",
            self.times_total,
            elapsed_ms,
            topic,
            self.brokers_sent.join(","),
            FAQUrl::suggest_todo(FAQUrl::SEND_MSG_FAILED)
        );

        if let Some(ref err) = self.last_error {
            match err {
                rocketmq_error::RocketMQError::IllegalArgument(_)
                | rocketmq_error::RocketMQError::Timeout { .. }
                | rocketmq_error::RocketMQError::BrokerOperationFailed { .. }
                | rocketmq_error::RocketMQError::Network(_) => {
                    mq_client_err!(ClientErrorCode::BROKER_NOT_EXIST_EXCEPTION, info)
                }
                _ => {
                    // For other error types, create a new error with info
                    mq_client_err!(
                        ClientErrorCode::BROKER_NOT_EXIST_EXCEPTION,
                        format!("{}: {}", info, err)
                    )
                }
            }
        } else {
            mq_client_err!(info)
        }
    }
}

pub struct DefaultMQProducerImpl {
    client_runtime: Option<Arc<ClientRuntime>>,
    service_context: ChildServiceContext,
    client_pool: Option<ClientPool>,
    client_pool_token: ParkingLotMutex<Option<ClientPoolToken>>,
    request_future_holder: Arc<RequestFutureHolder>,
    // ===== Immutable configuration =====
    runtime: ArcSwap<ProducerRuntimeSnapshot>,
    config_update: ParkingLotMutex<()>,

    // ===== Atomic state machine =====
    state: AtomicU8, // ProducerState
    state_changes: watch::Sender<ProducerState>,
    lifecycle_transition: TokioMutex<()>,
    service_state: ParkingLotRwLock<ServiceState>, // Keep for compatibility

    // ===== Read-only hot data (immutable after init, zero-cost sharing) =====
    send_message_hook_list: ParkingLotRwLock<Arc<[Arc<dyn SendMessageHook>]>>,
    end_transaction_hook_list: ParkingLotRwLock<Arc<[Arc<dyn EndTransactionHook>]>>,
    check_forbidden_hook_list: ParkingLotRwLock<Arc<[Arc<dyn CheckForbiddenHook>]>>,

    // Temporary hook storage during initialization
    pending_send_hooks: parking_lot::Mutex<Option<Vec<Arc<dyn SendMessageHook>>>>,
    pending_end_transaction_hooks: parking_lot::Mutex<Option<Vec<Arc<dyn EndTransactionHook>>>>,
    pending_forbidden_hooks: parking_lot::Mutex<Option<Vec<Arc<dyn CheckForbiddenHook>>>>,

    topic_publish_info_table: Arc<DashMap<Topic, TopicPublishInfoSnapshot>>,

    rpc_hook: ParkingLotRwLock<Option<Arc<dyn RPCHook>>>,
    client_instance: OnceLock<Weak<MQClientInstance>>,
    mq_fault_strategy: ParkingLotRwLock<MQFaultStrategy>,

    // ===== Backpressure control =====
    semaphore_async_send_num: Arc<Semaphore>,
    semaphore_async_send_size: Arc<Semaphore>,
    default_mqproducer_impl_inner: OnceLock<Weak<DefaultMQProducerImpl>>,
    transaction_runtime: ParkingLotRwLock<TransactionRuntime>,
    producer_task_tracker: TaskTracker,
    producer_task_shutdown: CancellationToken,
    task_admission: ParkingLotMutex<()>,
    oneway_egress: OnceLock<BoundedEgress>,
    compressor_missing_logged: AtomicBool,
}

mod lifecycle;
mod retry;
mod send;
mod transaction;

pub(crate) use send::DefaultResolver;
pub(crate) use send::DefaultServiceDetector;

#[cfg(test)]
#[path = "../../../tests/producer/default_mq_producer_impl/unit.rs"]
mod tests;
