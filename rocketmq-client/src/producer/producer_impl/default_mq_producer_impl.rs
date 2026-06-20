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
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use parking_lot::Mutex as ParkingLotMutex;

use cheetah_string::CheetahString;
use dashmap::DashMap;
use rand::random;
use rocketmq_common::common::base::service_state::ServiceState;
use rocketmq_common::common::message::message_batch::MessageBatch;
use rocketmq_common::common::message::message_client_id_setter::MessageClientIDSetter;
use rocketmq_common::common::message::message_enum::MessageType;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::CLIENT_INNER_PRODUCER_GROUP;
use rocketmq_common::common::mix_all::DEFAULT_PRODUCER_GROUP;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_common::common::FAQUrl;
use rocketmq_common::utils::correlation_id_util::CorrelationIdUtil;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::MessageDecoder;
use rocketmq_common::RecallMessageHandle;
use rocketmq_common::TimeUtils::current_millis;
use rocketmq_error::ClientErr;
use rocketmq_error::RocketmqError::RemotingTooMuchRequestError;
use rocketmq_remoting::protocol::header::check_transaction_state_request_header::CheckTransactionStateRequestHeader;
use rocketmq_remoting::protocol::header::end_transaction_request_header::EndTransactionRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_remoting::protocol::header::recall_message_request_header::RecallMessageRequestHeader;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::rpc::rpc_request_header::RpcRequestHeader;
use rocketmq_remoting::rpc::topic_request_header::TopicRequestHeader;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_rust::ArcMut;
use rocketmq_rust::WeakArcMut;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::base::query_result::QueryResult;
use crate::base::validators::Validators;
use crate::common::client_error_code::ClientErrorCode;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::hook::check_forbidden_context::CheckForbiddenContext;
use crate::hook::check_forbidden_hook::CheckForbiddenHook;
use crate::hook::end_transaction_context::EndTransactionContext;
use crate::hook::end_transaction_hook::EndTransactionHook;
use crate::hook::send_message_context::SendMessageContext;
use crate::hook::send_message_context::SendMessageTraceSnapshot;
use crate::hook::send_message_hook::SendMessageHook;
use crate::implementation::communication_mode::CommunicationMode;
use crate::implementation::mq_client_manager::MQClientManager;
use crate::latency::mq_fault_strategy::MQFaultStrategy;
use crate::latency::resolver::Resolver;
use crate::latency::service_detector::ServiceDetector;
use crate::producer::default_mq_producer::ProducerConfig;
use crate::producer::default_mq_producer::MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM;
use crate::producer::default_mq_producer::MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE;
use crate::producer::local_transaction_state::LocalTransactionState;
use crate::producer::producer_impl::mq_producer_inner::MQProducerInner;
use crate::producer::producer_impl::mq_producer_inner::MQProducerInnerImpl;
use crate::producer::producer_impl::topic_publish_info::TopicPublishInfo;
use crate::producer::request_callback::RequestCallbackFn;
use crate::producer::request_future_holder::REQUEST_FUTURE_HOLDER;
use crate::producer::request_response_future::RequestResponseFuture;
use crate::producer::send_callback::ArcSendCallback;
use crate::producer::send_result::SendResult;
use crate::producer::send_status::SendStatus;
use crate::producer::transaction_listener::ArcTransactionListener;
use crate::producer::transaction_send_result::TransactionSendResult;
use crate::runtime::spawn_client_blocking_io;
use crate::runtime::spawn_client_task_on;

type Topic = CheetahString;
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

fn spawn_producer_task<F>(
    executor: Option<&tokio::runtime::Handle>,
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

    drop(spawn_client_task_on(thread_name, executor, tracked_task)?);
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
    // ===== Immutable configuration =====
    client_config: ClientConfig,
    producer_config: Arc<ProducerConfig>,

    // ===== Atomic state machine =====
    state: AtomicU8,             // ProducerState
    service_state: ServiceState, // Keep for compatibility

    // ===== Read-only hot data (immutable after init, zero-cost sharing) =====
    send_message_hook_list: Arc<[Arc<dyn SendMessageHook>]>,
    end_transaction_hook_list: Arc<[Arc<dyn EndTransactionHook>]>,
    check_forbidden_hook_list: Arc<[Arc<dyn CheckForbiddenHook>]>,

    // Temporary hook storage during initialization
    pending_send_hooks: parking_lot::Mutex<Option<Vec<Arc<dyn SendMessageHook>>>>,
    pending_end_transaction_hooks: parking_lot::Mutex<Option<Vec<Arc<dyn EndTransactionHook>>>>,
    pending_forbidden_hooks: parking_lot::Mutex<Option<Vec<Arc<dyn CheckForbiddenHook>>>>,

    topic_publish_info_table: Arc<DashMap<Topic, TopicPublishInfo>>,

    rpc_hook: Option<Arc<dyn RPCHook>>,
    client_instance: Option<ArcMut<MQClientInstance>>,
    pub(crate) mq_fault_strategy: ArcMut<MQFaultStrategy>,

    // ===== Backpressure control =====
    semaphore_async_send_num: Arc<Semaphore>,
    semaphore_async_send_size: Arc<Semaphore>,
    default_mqproducer_impl_inner: Option<WeakArcMut<DefaultMQProducerImpl>>,
    transaction_listener: Option<ArcTransactionListener>,
    transaction_executor_service: Option<tokio::runtime::Handle>,
    transaction_check_env: Option<TransactionCheckEnv>,
    producer_task_tracker: TaskTracker,
    producer_task_shutdown: CancellationToken,
}

#[allow(unused_must_use)]
#[allow(unused_assignments)]
impl DefaultMQProducerImpl {
    pub fn new(
        client_config: ClientConfig,
        producer_config: ProducerConfig,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> Self {
        let semaphore_async_send_num = Semaphore::new(
            producer_config
                .back_pressure_for_async_send_num()
                .max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM) as usize,
        );
        let semaphore_async_send_size = Semaphore::new(
            producer_config
                .back_pressure_for_async_send_size()
                .max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE) as usize,
        );
        let topic_publish_info_table = Arc::new(DashMap::new());
        DefaultMQProducerImpl {
            client_config: client_config.clone(),
            producer_config: Arc::new(producer_config),
            state: AtomicU8::new(ProducerState::Created as u8),
            service_state: ServiceState::CreateJust,
            topic_publish_info_table,
            send_message_hook_list: Arc::new([]),
            end_transaction_hook_list: Arc::new([]),
            check_forbidden_hook_list: Arc::new([]),
            pending_send_hooks: ParkingLotMutex::new(Some(Vec::new())),
            pending_end_transaction_hooks: ParkingLotMutex::new(Some(Vec::new())),
            pending_forbidden_hooks: ParkingLotMutex::new(Some(Vec::new())),
            rpc_hook,
            client_instance: None,
            mq_fault_strategy: ArcMut::new(MQFaultStrategy::new(&client_config)),
            semaphore_async_send_num: Arc::new(semaphore_async_send_num),
            semaphore_async_send_size: Arc::new(semaphore_async_send_size),
            default_mqproducer_impl_inner: None,
            transaction_listener: None,
            transaction_executor_service: None,
            transaction_check_env: None,
            producer_task_tracker: TaskTracker::new(),
            producer_task_shutdown: CancellationToken::new(),
        }
    }

    #[inline]
    pub(crate) fn is_use_tls(&self) -> bool {
        self.client_config.is_use_tls()
    }

    #[inline]
    pub(crate) fn set_use_tls(&mut self, use_tls: bool) {
        self.client_config.set_use_tls(use_tls);
    }

    #[inline]
    fn message_body_len_for_backpressure<T: MessageTrait>(msg: &T) -> usize {
        msg.get_body().map_or(1, |body| body.len())
    }

    fn select_message_queue_with_user_message<M, S, T>(
        client_config: &mut ClientConfig,
        message_queue_list: &[MessageQueue],
        msg: &mut M,
        selector: &S,
        arg: &T,
    ) -> Option<MessageQueue>
    where
        M: MessageTrait,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue>,
    {
        let original_topic = msg.topic().clone();
        let user_topic = NamespaceUtil::without_namespace_with_namespace(
            original_topic.as_str(),
            client_config.get_namespace().unwrap_or_default().as_str(),
        );
        msg.set_topic(CheetahString::from_string(user_topic));
        let selected = selector(message_queue_list, msg, arg).map(|mq| client_config.queue_with_namespace(mq));
        msg.set_topic(original_topic);
        selected
    }

    #[inline]
    fn notify_callback_exception(send_callback: &Option<ArcSendCallback>, error: &dyn std::error::Error) {
        if let Some(send_callback) = send_callback.as_ref() {
            send_callback.on_exception(error);
        } else {
            tracing::error!("Async send failed without callback: {}", error);
        }
    }

    #[inline]
    fn remaining_async_timeout(timeout: u64, elapsed: u64) -> Option<u64> {
        timeout.checked_sub(elapsed).filter(|remaining| *remaining > 0)
    }

    #[inline]
    fn remaining_request_timeout(timeout: u64, elapsed: u64) -> rocketmq_error::RocketMQResult<u64> {
        Self::remaining_async_timeout(timeout, elapsed).ok_or(rocketmq_error::RocketMQError::Timeout {
            operation: "send request message",
            timeout_ms: timeout,
        })
    }

    #[inline]
    fn client_instance(&self) -> rocketmq_error::RocketMQResult<ArcMut<MQClientInstance>> {
        self.client_instance
            .as_ref()
            .cloned()
            .ok_or_else(|| mq_client_err!("MQClientInstance is not available; producer has not been started"))
    }

    pub fn get_mq_client_factory(&self) -> rocketmq_error::RocketMQResult<ArcMut<MQClientInstance>> {
        self.client_instance()
    }

    #[inline]
    pub(crate) fn client_id(&self) -> Option<CheetahString> {
        self.client_instance
            .as_ref()
            .map(|client_instance| client_instance.client_id.clone())
    }

    #[inline]
    pub(crate) fn producer_config(&self) -> &ProducerConfig {
        self.producer_config.as_ref()
    }

    #[inline]
    pub(crate) fn enable_backpressure_for_async_mode(&self) -> bool {
        self.producer_config.enable_backpressure_for_async_mode()
    }

    #[inline]
    pub(crate) fn back_pressure_for_async_send_num(&self) -> u32 {
        self.producer_config.back_pressure_for_async_send_num()
    }

    #[inline]
    pub(crate) fn back_pressure_for_async_send_size(&self) -> u32 {
        self.producer_config.back_pressure_for_async_send_size()
    }

    pub fn set_enable_backpressure_for_async_mode(&mut self, enable_backpressure_for_async_mode: bool) {
        Arc::make_mut(&mut self.producer_config)
            .set_enable_backpressure_for_async_mode(enable_backpressure_for_async_mode);
    }

    pub fn replace_producer_config(&mut self, producer_config: ProducerConfig) {
        let old_num_total = self
            .producer_config
            .back_pressure_for_async_send_num()
            .max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM) as usize;
        let old_size_total = self
            .producer_config
            .back_pressure_for_async_send_size()
            .max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE) as usize;
        let new_num_total = producer_config
            .back_pressure_for_async_send_num()
            .max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM) as usize;
        let new_size_total = producer_config
            .back_pressure_for_async_send_size()
            .max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE) as usize;

        self.producer_config = Arc::new(producer_config);
        Self::resize_available_permits(&self.semaphore_async_send_num, old_num_total, new_num_total);
        Self::resize_available_permits(&self.semaphore_async_send_size, old_size_total, new_size_total);
    }

    pub fn set_back_pressure_for_async_send_num(&mut self, back_pressure_for_async_send_num: u32) {
        let old_total = self
            .producer_config
            .back_pressure_for_async_send_num()
            .max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM) as usize;
        let new_total = back_pressure_for_async_send_num.max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM) as usize;
        Arc::make_mut(&mut self.producer_config).set_back_pressure_for_async_send_num(back_pressure_for_async_send_num);
        Self::resize_available_permits(&self.semaphore_async_send_num, old_total, new_total);
    }

    pub fn set_back_pressure_for_async_send_size(&mut self, back_pressure_for_async_send_size: u32) {
        let old_total = self
            .producer_config
            .back_pressure_for_async_send_size()
            .max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE) as usize;
        let new_total = back_pressure_for_async_send_size.max(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE) as usize;
        Arc::make_mut(&mut self.producer_config)
            .set_back_pressure_for_async_send_size(back_pressure_for_async_send_size);
        Self::resize_available_permits(&self.semaphore_async_send_size, old_total, new_total);
    }

    pub fn semaphore_processor(&self) {}

    pub fn semaphore_async_adjust(
        &mut self,
        semaphore_async_num: i32,
        semaphore_async_size: i32,
    ) -> rocketmq_error::RocketMQResult<()> {
        let current_num = self.producer_config.back_pressure_for_async_send_num() as i64;
        let current_size = self.producer_config.back_pressure_for_async_send_size() as i64;
        let new_num = current_num + semaphore_async_num as i64;
        let new_size = current_size + semaphore_async_size as i64;

        if new_num <= 0 || new_num > u32::MAX as i64 {
            return Err(rocketmq_error::RocketMQError::IllegalArgument(format!(
                "semaphoreAsyncNum adjustment out of range: current={}, delta={}",
                current_num, semaphore_async_num
            )));
        }
        if new_size <= 0 || new_size > u32::MAX as i64 {
            return Err(rocketmq_error::RocketMQError::IllegalArgument(format!(
                "semaphoreAsyncSize adjustment out of range: current={}, delta={}",
                current_size, semaphore_async_size
            )));
        }

        self.set_back_pressure_for_async_send_num(new_num as u32);
        self.set_back_pressure_for_async_send_size(new_size as u32);
        Ok(())
    }

    fn resize_available_permits(semaphore: &Semaphore, old_total: usize, new_total: usize) {
        let available = semaphore.available_permits();
        let in_flight = old_total.saturating_sub(available);
        let target_available = new_total.saturating_sub(in_flight);

        match target_available.cmp(&available) {
            std::cmp::Ordering::Greater => semaphore.add_permits(target_available - available),
            std::cmp::Ordering::Less => {
                let _ = semaphore.forget_permits(available - target_available);
            }
            std::cmp::Ordering::Equal => {}
        }
    }

    #[inline]
    fn request_correlation_id<M: MessageTrait>(msg: &M) -> rocketmq_error::RocketMQResult<CheetahString> {
        msg.property(&CheetahString::from_static_str(MessageConst::PROPERTY_CORRELATION_ID))
            .ok_or_else(|| mq_client_err!("Request correlation id was not set before sending request message"))
    }

    #[inline]
    pub async fn send_with_timeout<T>(
        &mut self,
        msg: &mut T,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        T: MessageTrait + Send + Sync,
    {
        self.send_default_impl(msg, CommunicationMode::Sync, None, timeout)
            .await
    }

    #[inline]
    pub async fn send<T>(&mut self, msg: &mut T) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        T: MessageTrait + Send + Sync,
    {
        self.send_with_timeout(msg, self.producer_config.send_msg_timeout() as u64)
            .await
    }

    #[inline]
    pub async fn async_send_with_callback<T>(
        &mut self,
        msg: T,
        send_callback: Option<ArcSendCallback>,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        T: MessageTrait + Send + Sync,
    {
        self.async_send_with_callback_timeout(msg, send_callback, self.producer_config.send_msg_timeout() as u64)
            .await
    }

    #[inline]
    pub async fn sync_send_with_message_queue<T>(
        &mut self,
        msg: T,
        mq: MessageQueue,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        T: MessageTrait + Send + Sync,
    {
        self.sync_send_with_message_queue_timeout(msg, mq, self.producer_config.send_msg_timeout() as u64)
            .await
    }

    #[inline]
    pub async fn send_oneway<T>(&mut self, mut msg: T) -> rocketmq_error::RocketMQResult<()>
    where
        T: MessageTrait + Send + Sync,
    {
        self.send_default_impl(
            &mut msg,
            CommunicationMode::Oneway,
            None,
            self.producer_config.send_msg_timeout() as u64,
        )
        .await?;
        Ok(())
    }

    pub async fn send_oneway_with_message_queue<T>(
        &mut self,
        mut msg: T,
        mq: MessageQueue,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        T: MessageTrait + Send + Sync,
    {
        self.make_sure_state_ok()?;
        Validators::check_message(Some(&msg), self.producer_config.as_ref())?;

        let timeout = self.producer_config.send_msg_timeout() as u64;
        self.send_kernel_impl(&mut msg, &mq, CommunicationMode::Oneway, None, None, timeout)
            .await?;
        Ok(())
    }

    /// **High-Performance** batch send messages in oneway mode (fire-and-forget).
    ///
    /// This API provides **extreme throughput** for scenarios where performance is more important
    /// than reliability, such as log collection, metrics reporting, and telemetry.
    ///
    /// # Arguments
    /// * `msgs` - Iterator of messages to send
    ///
    /// # Semantics
    /// - All messages are sent in parallel (background tasks)
    /// - Returns immediately after spawning all send tasks
    /// - No retry, no error propagation
    /// - Errors are silently logged
    /// - Uses `send_oneway_unbounded` for maximum throughput
    ///
    /// # Performance Characteristics
    /// - **Throughput**: 100K+ messages/second per producer
    /// - **Latency**: < 10μs per message (spawn overhead only)
    /// - **Memory**: ~1KB per message (task structure)
    /// - **Parallel**: All messages sent concurrently
    ///
    /// # Example
    /// ```rust,ignore
    /// let messages = vec![msg1, msg2, msg3];
    /// producer.send_oneway_batch(messages).await?;
    /// ```
    pub async fn send_oneway_batch<T>(
        &mut self,
        msgs: impl IntoIterator<Item = T>,
    ) -> rocketmq_error::RocketMQResult<usize>
    where
        T: MessageTrait + Send + Sync + 'static,
    {
        self.make_sure_state_ok()?;

        let timeout = self.producer_config.send_msg_timeout() as u64;
        let mut sent_count = 0;

        for msg in msgs {
            // Validate each message
            if let Err(e) = Validators::check_message(Some(&msg), self.producer_config.as_ref()) {
                tracing::debug!("Message validation failed in batch oneway: {:?}", e);
                continue;
            }

            let topic = msg.topic().clone();
            let topic_publish_info = self.try_to_find_topic_publish_info(&topic).await;

            if let Some(info) = topic_publish_info {
                if info.ok() {
                    if let Some(mq) = self.select_one_message_queue(&info, None, false) {
                        // Spawn background task for each message
                        self.spawn_oneway_send(msg, mq, info, timeout);
                        sent_count += 1;
                    }
                }
            }
        }

        Ok(sent_count)
    }

    /// Spawn a background task for oneway message sending.
    ///
    /// This is a helper method for send_oneway_batch to avoid code duplication.
    fn spawn_oneway_send<T>(&self, msg: T, mq: MessageQueue, topic_publish_info: TopicPublishInfo, timeout: u64)
    where
        T: MessageTrait + Send + Sync + 'static,
    {
        let Ok(client_instance) = self.client_instance() else {
            tracing::debug!("Oneway batch send skipped: MQClientInstance is not available");
            return;
        };
        let producer_config = self.producer_config.clone();
        let client_config = self.client_config.clone();
        let topic_publish_info = Arc::new(topic_publish_info);

        let task = async move {
            // Prepare message in background task
            let mut msg = msg;

            // Get broker address
            let broker_name = client_instance.get_broker_name_from_message_queue(&mq).await;
            let broker_addr = client_instance.find_broker_address_in_publish(broker_name.as_ref());

            let Some(broker_addr) = broker_addr else {
                return;
            };
            let broker_addr = mix_all::broker_vip_channel(client_config.vip_channel_enabled, broker_addr.as_str());

            // Build request (simplified for oneway)
            let request = match build_oneway_request_internal(
                &mut msg,
                &mq,
                &broker_name,
                &producer_config,
                client_config.namespace.as_deref(),
            ) {
                Ok(req) => req,
                Err(e) => {
                    tracing::debug!("Failed to build oneway request: {:?}", e);
                    return;
                }
            };

            // Fire and forget (use unbounded method for maximum batch throughput)
            match client_instance.get_mq_client_api_impl() {
                Ok(mut mq_client_api) => {
                    let send_start = Instant::now();
                    if let Err(e) = mq_client_api.send_oneway_unbounded(&broker_addr, request).await {
                        tracing::debug!("Oneway batch send failed: {:?}", e);
                    }
                    rocketmq_observability::metrics::client::record_send(send_start.elapsed());
                }
                Err(e) => tracing::debug!("Oneway batch send skipped: {:?}", e),
            }
        };
        if let Err(error) = spawn_producer_task(
            self.producer_config.async_sender_executor(),
            "rocketmq-client-producer-oneway-batch",
            &self.producer_task_tracker,
            &self.producer_task_shutdown,
            task,
        ) {
            warn!("Failed to spawn batch oneway send task: {}", error);
        }
    }
    #[inline]
    pub async fn sync_send_with_message_queue_timeout<T>(
        &mut self,
        mut msg: T,
        mq: MessageQueue,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        T: MessageTrait + Send + Sync,
    {
        let begin_start_time = Instant::now();
        self.make_sure_state_ok()?;
        Validators::check_message(Some(&msg), self.producer_config.as_ref())?;

        if msg.topic() != mq.topic_str() {
            return Err(mq_client_err!("message's topic not equal mq's topic"));
        }
        let cost_time = begin_start_time.elapsed().as_millis() as u64;
        if timeout < cost_time {
            return Err(rocketmq_error::RocketMQError::Timeout {
                operation: "send_with_timeout",
                timeout_ms: timeout,
            });
        }
        // Java send(msg, mq, timeout) uses cost time only as a pre-check here.
        self.send_kernel_impl(&mut msg, &mq, CommunicationMode::Sync, None, None, timeout)
            .await
    }

    #[inline]
    pub async fn async_send_with_message_queue_callback<T>(
        &mut self,
        msg: T,
        mq: MessageQueue,
        send_callback: Option<ArcSendCallback>,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        T: MessageTrait + Send + Sync,
    {
        self.async_send_batch_to_queue_with_callback_timeout(
            msg,
            mq,
            send_callback,
            self.producer_config.send_msg_timeout() as u64,
        )
        .await
    }

    pub async fn send_with_selector_callback_timeout<M, S, T>(
        &mut self,
        msg: M,
        selector: S,
        arg: T,
        send_callback: Option<ArcSendCallback>,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync + 'static,
    {
        let begin_start_time = Instant::now();
        let mut producer_impl = self
            .default_mqproducer_impl_inner
            .as_ref()
            .and_then(|weak| weak.upgrade())
            .ok_or_else(|| {
                mq_client_err!(
                    "Failed to upgrade default_mqproducer_impl_inner: producer implementation is not available"
                )
            })?;
        let msg_len = Self::message_body_len_for_backpressure(&msg);
        let send_callback_clone = send_callback.clone();
        let future = async move {
            let cost_time = begin_start_time.elapsed().as_millis() as u64;
            let Some(remaining_timeout) = Self::remaining_async_timeout(timeout, cost_time) else {
                Self::notify_callback_exception(
                    &send_callback_clone,
                    &RemotingTooMuchRequestError("call timeout".to_string()),
                );
                return Ok(None);
            };

            producer_impl
                .send_select_impl(
                    msg,
                    selector,
                    arg,
                    CommunicationMode::Async,
                    send_callback_clone,
                    remaining_timeout,
                )
                .await
        };
        self.execute_async_message_send(future, send_callback, timeout, begin_start_time, msg_len)
            .await
    }

    pub async fn send_oneway_with_selector<M, S, T>(
        &mut self,
        msg: M,
        selector: S,
        arg: T,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync,
    {
        self.send_select_impl(
            msg,
            selector,
            arg,
            CommunicationMode::Oneway,
            None,
            self.producer_config.send_msg_timeout() as u64,
        )
        .await?;
        Ok(())
    }

    pub async fn send_select_impl<M, S, T>(
        &mut self,
        mut msg: M,
        selector: S,
        arg: T,
        communication_mode: CommunicationMode,
        send_message_callback: Option<ArcSendCallback>,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync,
        T: Send + Sync,
    {
        let begin_start_time = Instant::now();
        self.make_sure_state_ok()?;
        Validators::check_message(Some(&msg), self.producer_config.as_ref())?;
        let topic_publish_info = self.try_to_find_topic_publish_info(msg.topic()).await;
        if let Some(topic_publish_info) = topic_publish_info {
            if topic_publish_info.ok() {
                let client_instance = self.client_instance()?;
                let message_queue_list = client_instance
                    .mut_from_ref()
                    .mq_admin_impl
                    .parse_publish_message_queues(&topic_publish_info.message_queue_list, &mut self.client_config);
                let message_queue = Self::select_message_queue_with_user_message(
                    &mut self.client_config,
                    &message_queue_list,
                    &mut msg,
                    &selector,
                    &arg,
                );
                let cost_time = begin_start_time.elapsed().as_millis() as u64;
                if timeout < cost_time {
                    return Err(rocketmq_error::RocketMQError::Timeout {
                        operation: "sendSelectImpl",
                        timeout_ms: timeout,
                    });
                }
                if let Some(message_queue) = message_queue {
                    return self
                        .send_kernel_impl(
                            &mut msg,
                            &message_queue,
                            communication_mode,
                            send_message_callback,
                            None,
                            timeout - cost_time,
                        )
                        .await;
                }
                return Err(mq_client_err!("select message queue return null."));
            }
        }
        self.validate_name_server_setting()?;
        Err(mq_client_err!(format!("No route info for this topic, {}", msg.topic())))
    }

    #[inline]
    pub async fn async_send_batch_to_queue_with_callback_timeout<T>(
        &mut self,
        mut msg: T,
        mq: MessageQueue,
        send_callback: Option<ArcSendCallback>,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        T: MessageTrait + Send + Sync,
    {
        let mut producer_impl = self
            .default_mqproducer_impl_inner
            .as_ref()
            .and_then(|weak| weak.upgrade())
            .ok_or_else(|| {
                mq_client_err!(
                    "Failed to upgrade default_mqproducer_impl_inner: producer implementation is not available"
                )
            })?;
        let begin_start_time = Instant::now();
        let send_callback_inner = send_callback.clone();
        let msg_len = Self::message_body_len_for_backpressure(&msg);
        let future = async move {
            if let Err(err) = producer_impl.make_sure_state_ok() {
                Self::notify_callback_exception(&send_callback_inner, &err);
                return;
            }
            if let Err(err) = Validators::check_message(Some(&msg), producer_impl.producer_config.as_ref()) {
                Self::notify_callback_exception(&send_callback_inner, &err);
                return;
            }
            if msg.topic() != mq.topic_str() {
                let err = mq_client_err!("Topic of the message does not match its target message queue");
                Self::notify_callback_exception(&send_callback_inner, &err);
                return;
            }

            let cost_time = (Instant::now() - begin_start_time).as_millis() as u64;
            let Some(remaining_timeout) = Self::remaining_async_timeout(timeout, cost_time) else {
                Self::notify_callback_exception(
                    &send_callback_inner,
                    &RemotingTooMuchRequestError("call timeout".to_string()),
                );
                return;
            };
            let result = producer_impl
                .send_kernel_impl(
                    &mut msg,
                    &mq,
                    CommunicationMode::Async,
                    send_callback_inner.clone(),
                    None,
                    remaining_timeout,
                )
                .await;
            match result {
                Ok(_) => {}
                Err(err) => {
                    Self::notify_callback_exception(&send_callback_inner, &err);
                }
            }
        };

        self.execute_async_message_send(future, send_callback, timeout, begin_start_time, msg_len)
            .await
    }

    pub async fn async_send_with_callback_timeout<T>(
        &mut self,
        mut msg: T,
        send_callback: Option<ArcSendCallback>,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        T: MessageTrait + Send + Sync,
    {
        let mut producer_impl = self
            .default_mqproducer_impl_inner
            .as_ref()
            .and_then(|weak| weak.upgrade())
            .ok_or_else(|| {
                mq_client_err!(
                    "Failed to upgrade default_mqproducer_impl_inner: producer implementation is not available"
                )
            })?;
        let begin_start_time = Instant::now();
        let send_callback_inner = send_callback.clone();
        let msg_len = Self::message_body_len_for_backpressure(&msg);
        let future = async move {
            let cost_time = (Instant::now() - begin_start_time).as_millis() as u64;
            let Some(remaining_timeout) = Self::remaining_async_timeout(timeout, cost_time) else {
                Self::notify_callback_exception(
                    &send_callback_inner,
                    &RemotingTooMuchRequestError("asyncSend call timeout".to_string()),
                );
                return;
            };

            let result = producer_impl
                .send_default_impl(
                    &mut msg,
                    CommunicationMode::Async,
                    send_callback_inner.clone(),
                    remaining_timeout,
                )
                .await;
            match result {
                Ok(_) => {}
                Err(err) => {
                    Self::notify_callback_exception(&send_callback_inner, &err);
                }
            }
        };

        self.execute_async_message_send(future, send_callback, timeout, begin_start_time, msg_len)
            .await
    }

    async fn execute_async_message_send<F>(
        &mut self,
        f: F,
        send_callback: Option<ArcSendCallback>,
        timeout: u64,
        begin_start_time: Instant,
        msg_len: usize,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let is_enable_backpressure_for_async_mode = self.producer_config.enable_backpressure_for_async_mode();

        let (acquire_value_num, acquire_value_size) = if is_enable_backpressure_for_async_mode {
            //back pressure
            let cost_time = (Instant::now() - begin_start_time).as_millis() as u64;
            let Some(remaining_timeout) = timeout.checked_sub(cost_time).filter(|remaining| *remaining > 0) else {
                Self::notify_callback_exception(
                    &send_callback,
                    &RemotingTooMuchRequestError("send message tryAcquire semaphoreAsyncNum timeout".to_string()),
                );
                return Ok(());
            };
            let result = tokio::time::timeout(
                Duration::from_millis(remaining_timeout),
                self.semaphore_async_send_num.clone().acquire_owned(),
            )
            .await;
            let acquire_value_num = match result {
                Ok(acquire_value) => match acquire_value {
                    Ok(value) => Some(value),
                    Err(_) => {
                        Self::notify_callback_exception(
                            &send_callback,
                            &RemotingTooMuchRequestError(
                                "send message tryAcquire semaphoreAsyncNum timeout".to_string(),
                            ),
                        );
                        return Ok(());
                    }
                },
                Err(_) => {
                    Self::notify_callback_exception(
                        &send_callback,
                        &RemotingTooMuchRequestError("send message tryAcquire semaphoreAsyncNum timeout".to_string()),
                    );
                    return Ok(());
                }
            };

            //message size
            let cost_time = (Instant::now() - begin_start_time).as_millis() as u64;
            let Some(remaining_timeout) = timeout.checked_sub(cost_time).filter(|remaining| *remaining > 0) else {
                Self::notify_callback_exception(
                    &send_callback,
                    &RemotingTooMuchRequestError("send message tryAcquire semaphoreAsyncSize timeout".to_string()),
                );
                return Ok(());
            };
            let result = tokio::time::timeout(
                Duration::from_millis(remaining_timeout),
                self.semaphore_async_send_size
                    .clone()
                    .acquire_many_owned(msg_len as u32),
            )
            .await;
            let acquire_value_size = match result {
                Ok(acquire_value) => match acquire_value {
                    Ok(value) => Some(value),
                    Err(_) => {
                        Self::notify_callback_exception(
                            &send_callback,
                            &RemotingTooMuchRequestError(
                                "send message tryAcquire semaphoreAsyncSize timeout".to_string(),
                            ),
                        );
                        return Ok(());
                    }
                },
                Err(_) => {
                    Self::notify_callback_exception(
                        &send_callback,
                        &RemotingTooMuchRequestError("send message tryAcquire semaphoreAsyncSize timeout".to_string()),
                    );
                    return Ok(());
                }
            };
            (acquire_value_num, acquire_value_size)
        } else {
            (None, None)
        };
        let task = async move {
            let _acquire_value_num = acquire_value_num;
            let _acquire_value_size = acquire_value_size;
            f.await;
        };
        if let Err(error) = spawn_producer_task(
            self.producer_config.async_sender_executor(),
            "rocketmq-client-producer-async-send",
            &self.producer_task_tracker,
            &self.producer_task_shutdown,
            task,
        ) {
            Self::notify_callback_exception(
                &send_callback,
                &mq_client_err!(format!("failed to spawn async send task: {error}")),
            );
            return Err(mq_client_err!(format!("failed to spawn async send task: {error}")));
        }
        Ok(())
    }

    async fn send_default_impl<T>(
        &mut self,
        msg: &mut T,
        communication_mode: CommunicationMode,
        send_callback: Option<ArcSendCallback>,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        T: MessageTrait + Send + Sync,
    {
        self.make_sure_state_ok()?;
        Validators::check_message(Some(&*msg), self.producer_config.as_ref())?;

        let topic = msg.topic().clone();
        let topic_publish_info = self.try_to_find_topic_publish_info(&topic).await;

        if let Some(topic_publish_info) = topic_publish_info {
            if topic_publish_info.ok() {
                let ctx = SendContext::new(timeout, communication_mode);
                return self
                    .send_with_retry(msg, &topic, &topic_publish_info, send_callback, ctx)
                    .await;
            }
        }

        self.validate_name_server_setting()?;
        Err(mq_client_err!(
            ClientErrorCode::NOT_FOUND_TOPIC_EXCEPTION,
            format!(
                "No route info of this topic:{},{}",
                topic,
                FAQUrl::suggest_todo(FAQUrl::NO_TOPIC_ROUTE_INFO)
            )
        ))
    }

    /// Core: send with retry logic
    async fn send_with_retry<T>(
        &mut self,
        msg: &mut T,
        topic: &CheetahString,
        topic_publish_info: &TopicPublishInfo,
        send_callback: Option<ArcSendCallback>,
        ctx: SendContext,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        T: MessageTrait + Send + Sync,
    {
        let retry_times = self.get_retry_times(ctx.communication_mode);
        let mut retry_state = RetryState::new(retry_times);
        let mut last_broker_name: Option<CheetahString> = None;
        let send_result: Option<SendResult> = None;

        for attempt in 0..retry_times {
            let reset_index = attempt > 0;

            // Select message queue
            let mq = match self.select_one_message_queue(topic_publish_info, last_broker_name.as_ref(), reset_index) {
                Some(mq) => mq,
                None => break,
            };

            retry_state.record_broker(attempt as usize, mq.broker_name());
            last_broker_name = Some(mq.broker_name().clone());

            // Prepare message for retry
            if attempt > 0 {
                self.prepare_message_for_retry(msg, topic);
            }

            // Check timeout
            ctx.check_timeout()?;

            // Send to broker
            let remaining_timeout = ctx.remaining_timeout();
            let request_timeout = self.send_timeout_for_attempt(remaining_timeout, attempt, retry_times);
            let send_start = Instant::now();
            let result = self
                .send_kernel_impl(
                    msg,
                    &mq,
                    ctx.communication_mode,
                    send_callback.clone(),
                    Some(topic_publish_info),
                    request_timeout,
                )
                .await;

            let elapsed = send_start.elapsed().as_millis() as u64;

            match result {
                Ok(result) => {
                    // Update fault item - success
                    self.update_fault_item(mq.broker_name(), elapsed, false, true).await;

                    // Check if need to retry based on send status
                    if self.should_retry_on_result(&result, ctx.communication_mode) {
                        retry_state.set_error(mq_client_err!("Send status not OK"));
                        continue;
                    }

                    return Ok(result);
                }
                Err(e) => {
                    // Handle send error
                    self.handle_send_error(&mq, &e, elapsed, ctx.invoke_id).await;

                    if !self.should_retry_on_error(&e) {
                        return Err(e);
                    }

                    retry_state.set_error(e);
                }
            }
        }

        // All retries exhausted
        if send_result.is_some() {
            return Ok(send_result);
        }

        Err(retry_state.build_failure_error(topic, ctx.elapsed() as u128))
    }

    /// Get retry times based on communication mode
    #[inline]
    fn get_retry_times(&self, mode: CommunicationMode) -> u32 {
        match mode {
            CommunicationMode::Sync => self.producer_config.retry_times_when_send_failed() + 1,
            CommunicationMode::Async | CommunicationMode::Oneway => 1,
        }
    }

    #[inline]
    fn send_timeout_for_attempt(&self, remaining_timeout: u64, attempt: u32, retry_times: u32) -> u64 {
        let can_retry_again = attempt < retry_times.saturating_sub(1);
        if can_retry_again {
            if let Some(max_timeout_per_request) = self.producer_config.send_msg_max_timeout_per_request() {
                return remaining_timeout.min(max_timeout_per_request as u64);
            }
        }
        remaining_timeout
    }

    /// Prepare message for retry (reset topic with namespace)
    fn prepare_message_for_retry<T: MessageTrait>(&self, msg: &mut T, topic: &CheetahString) {
        let namespace = self.client_config.namespace.clone().unwrap_or(CheetahString::empty());
        msg.set_topic(NamespaceUtil::wrap_namespace(namespace, topic));
    }

    /// Handle send error - update fault item and log
    async fn handle_send_error(
        &self,
        mq: &MessageQueue,
        error: &rocketmq_error::RocketMQError,
        elapsed: u64,
        invoke_id: u64,
    ) {
        let broker_name = mq.broker_name();

        match error {
            rocketmq_error::RocketMQError::IllegalArgument(_) => {
                self.update_fault_item(broker_name, elapsed, false, true).await;
                warn!(
                    "sendKernelImpl exception, resend at once, InvokeID: {}, RT: {}ms, Broker: {:?}, {}",
                    invoke_id, elapsed, mq, error
                );
            }
            rocketmq_error::RocketMQError::BrokerOperationFailed { .. } => {
                self.update_fault_item(broker_name, elapsed, true, false).await;
            }
            rocketmq_error::RocketMQError::Network(_) => {
                let reachable = !self.mq_fault_strategy.is_start_detector_enable();
                self.update_fault_item(broker_name, elapsed, true, reachable).await;
            }
            _ => {}
        }
    }

    /// Check if should retry based on error type
    #[inline]
    fn should_retry_on_error(&self, error: &rocketmq_error::RocketMQError) -> bool {
        match error {
            rocketmq_error::RocketMQError::IllegalArgument(_) => true,
            rocketmq_error::RocketMQError::Network(_) => true,
            rocketmq_error::RocketMQError::BrokerOperationFailed { code, .. } => {
                self.producer_config.retry_response_codes().contains(code)
            }
            _ => false,
        }
    }

    /// Check if should retry based on send result
    #[inline]
    fn should_retry_on_result(&self, result: &Option<SendResult>, mode: CommunicationMode) -> bool {
        if mode != CommunicationMode::Sync {
            return false;
        }

        result.as_ref().is_some_and(|r| {
            r.send_status != SendStatus::SendOk && self.producer_config.retry_another_broker_when_not_store_ok()
        })
    }

    #[inline]
    pub async fn update_fault_item(
        &self,
        broker_name: &CheetahString,
        current_latency: u64,
        isolation: bool,
        reachable: bool,
    ) {
        self.mq_fault_strategy
            .update_fault_item(broker_name.clone(), current_latency, isolation, reachable)
            .await;
    }

    #[cfg_attr(
        feature = "observability",
        tracing::instrument(
            name = "RocketMQ PRODUCER SEND",
            skip_all,
            fields(
                messaging.system = "rocketmq",
                messaging.message.id = tracing::field::Empty,
                messaging.message.body.size = tracing::field::Empty,
                messaging.rocketmq.message.keys = tracing::field::Empty,
            )
        )
    )]
    async fn send_kernel_impl<T>(
        &mut self,
        msg: &mut T,
        mq: &MessageQueue,
        communication_mode: CommunicationMode,
        send_callback: Option<ArcSendCallback>,
        topic_publish_info: Option<&TopicPublishInfo>,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        T: MessageTrait + Send + Sync,
    {
        let begin_start_time = Instant::now();

        let client_instance = self.client_instance()?;

        // Get broker info with a single lookup path
        let mut broker_name = client_instance.get_broker_name_from_message_queue(mq).await;
        let mut broker_addr = client_instance.find_broker_address_in_publish(broker_name.as_ref());

        if broker_addr.is_none() {
            self.try_to_find_topic_publish_info(mq.topic()).await;
            broker_name = client_instance.get_broker_name_from_message_queue(mq).await;
            broker_addr = client_instance.find_broker_address_in_publish(broker_name.as_ref());
        }

        let Some(mut broker_addr) = broker_addr else {
            return Err(mq_client_err!(format!("The broker[{}] not exist", broker_name,)));
        };
        broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, broker_addr.as_str());

        let batch = msg.as_any().downcast_ref::<MessageBatch>().is_some();
        if !batch {
            MessageClientIDSetter::set_uniq_id(msg);
        }
        #[cfg(feature = "observability")]
        rocketmq_observability::trace::record_current_message_attributes(msg);

        let namespace = self.client_config.get_namespace();
        let mut topic_with_namespace = false;
        if let Some(ref ns) = namespace {
            msg.set_instance_id(ns.clone());
            topic_with_namespace = true;
        }

        let mut sys_flag = 0i32;
        let mut msg_body_compressed = false;
        if self.try_to_compress_message(msg) {
            sys_flag |= MessageSysFlag::COMPRESSED_FLAG;
            sys_flag |= self.producer_config.compress_type().get_compression_flag();
            msg_body_compressed = true;
        }

        let tran_msg_property = msg.property_ref(&CheetahString::from_static_str(
            MessageConst::PROPERTY_TRANSACTION_PREPARED,
        ));
        let is_transaction_prepared = tran_msg_property.and_then(|v| v.parse().ok()).unwrap_or(false);

        if is_transaction_prepared {
            sys_flag |= MessageSysFlag::TRANSACTION_PREPARED_TYPE;
        }

        if self.has_check_forbidden_hook() {
            let check_forbidden_context = CheckForbiddenContext {
                name_srv_addr: self.client_config.get_namesrv_addr(),
                group: Some(self.producer_config.producer_group().clone()),
                communication_mode: Some(communication_mode),
                broker_addr: Some(broker_addr.clone()),
                message: Some(msg),
                mq: Some(mq),
                unit_mode: self.is_unit_mode(),
                ..Default::default()
            };
            self.execute_check_forbidden_hook(&check_forbidden_context)?;
        }

        // Build send message request header
        #[cfg(feature = "observability")]
        rocketmq_observability::propagation::inject_current_context_into_message(msg);

        let producer_group = self.producer_config.producer_group();
        let topic = msg.topic();
        let create_topic_key = self.producer_config.create_topic_key();

        let mut request_header = SendMessageRequestHeader {
            producer_group: producer_group.clone(),
            topic: topic.clone(),
            default_topic: create_topic_key.clone(),
            default_topic_queue_nums: self.producer_config.default_topic_queue_nums() as i32,
            queue_id: mq.queue_id(),
            sys_flag,
            born_timestamp: current_millis() as i64,
            flag: msg.get_flag(),
            properties: Some(MessageDecoder::message_properties_to_string(msg.get_properties())),
            reconsume_times: Some(0),
            unit_mode: Some(self.is_unit_mode()),
            batch: Some(batch),
            topic_request_header: Some(TopicRequestHeader {
                rpc_request_header: Some(RpcRequestHeader {
                    broker_name: Some(broker_name.clone()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        if request_header.topic.starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX) {
            let reconsume_times = MessageAccessor::get_reconsume_time(msg);
            if let Some(value) = reconsume_times {
                request_header.reconsume_times = value.parse::<i32>().map_or(Some(0), Some);
                MessageAccessor::clear_property(msg, MessageConst::PROPERTY_RECONSUME_TIME);
            }

            let max_reconsume_times = MessageAccessor::get_max_reconsume_times(msg);
            if let Some(value) = max_reconsume_times {
                request_header.max_reconsume_times = value.parse::<i32>().map_or(Some(0), Some);
                MessageAccessor::clear_property(msg, MessageConst::PROPERTY_MAX_RECONSUME_TIMES);
            }
        }

        // Helper macro to create send_message_context for a message
        macro_rules! create_send_context {
            ($msg_ref:expr) => {
                if self.has_send_message_hook() {
                    let born_host = self.client_config.client_ip.clone();

                    // Check all delay message properties (aligned with Java implementation)
                    let has_delay_property = $msg_ref
                        .property_ref(&CheetahString::from_static_str(
                            MessageConst::PROPERTY_STARTDE_LIVER_TIME,
                        ))
                        .is_some()
                        || $msg_ref
                            .property_ref(&CheetahString::from_static_str(
                                MessageConst::PROPERTY_DELAY_TIME_LEVEL,
                            ))
                            .is_some()
                        || $msg_ref
                            .property_ref(&CheetahString::from_static_str(
                                MessageConst::PROPERTY_TIMER_DELIVER_MS,
                            ))
                            .is_some()
                        || $msg_ref
                            .property_ref(&CheetahString::from_static_str(
                                MessageConst::PROPERTY_TIMER_DELAY_SEC,
                            ))
                            .is_some()
                        || $msg_ref
                            .property_ref(&CheetahString::from_static_str(
                                MessageConst::PROPERTY_TIMER_DELAY_MS,
                            ))
                            .is_some();

                    let mut send_message_context = SendMessageContext {
                        producer: self
                            .default_mqproducer_impl_inner
                            .as_ref()
                            .and_then(|weak| weak.upgrade()),
                        producer_group: Some(producer_group.clone()),
                        communication_mode: Some(communication_mode),
                        born_host,
                        broker_addr: Some(broker_addr.clone()),
                        message: None, // Don't store message reference to avoid borrow conflicts
                        message_trace_snapshot: Some(SendMessageTraceSnapshot::from_message($msg_ref)),
                        mq: Some(mq),
                        namespace: namespace.clone(),
                        trace_start_time: Some(current_millis()),
                        ..Default::default()
                    };

                    if is_transaction_prepared {
                        send_message_context.msg_type = Some(MessageType::TransMsgHalf);
                    } else if has_delay_property {
                        send_message_context.msg_type = Some(MessageType::DelayMsg);
                    }

                    let send_message_context = Some(send_message_context);
                    self.execute_send_message_hook_before(&send_message_context);
                    send_message_context
                } else {
                    None
                }
            };
        }

        let mut send_message_context = create_send_context!(msg);
        if topic_with_namespace {
            // Restore original topic without namespace
            let origin_topic = NamespaceUtil::without_namespace_with_namespace(
                msg.topic(),
                self.client_config.get_namespace().unwrap_or_default().as_str(),
            );
            msg.set_topic(origin_topic.into());
        }

        let send_result = match communication_mode {
            CommunicationMode::Async => {
                let cost_time_async = (Instant::now() - begin_start_time).as_millis() as u64;
                if timeout < cost_time_async {
                    return Err(rocketmq_error::RocketMQError::Timeout {
                        operation: "sendKernelImpl",
                        timeout_ms: timeout,
                    });
                }
                client_instance
                    .get_mq_client_api_impl()?
                    .send_message(
                        &broker_addr,
                        &broker_name,
                        msg,
                        request_header,
                        timeout - cost_time_async,
                        communication_mode,
                        send_callback,
                        topic_publish_info,
                        self.client_instance.clone(),
                        self.producer_config.retry_times_when_send_async_failed(),
                        &mut send_message_context,
                        self,
                    )
                    .await
            }
            CommunicationMode::Oneway | CommunicationMode::Sync => {
                let cost_time_sync = (Instant::now() - begin_start_time).as_millis() as u64;
                if timeout < cost_time_sync {
                    return Err(rocketmq_error::RocketMQError::Timeout {
                        operation: "sendKernelImpl",
                        timeout_ms: timeout,
                    });
                }
                client_instance
                    .get_mq_client_api_impl()?
                    .send_message_simple(
                        &broker_addr,
                        &broker_name,
                        msg,
                        request_header,
                        timeout - cost_time_sync,
                        communication_mode,
                        &mut send_message_context,
                        self,
                    )
                    .await
            }
        };

        rocketmq_observability::metrics::client::record_send(begin_start_time.elapsed());

        match send_result {
            Ok(result) => {
                if self.has_send_message_hook() {
                    if let Some(smc) = send_message_context.as_mut() {
                        smc.send_result = result.as_ref();
                    }
                    self.execute_send_message_hook_after(&send_message_context);
                }
                Ok(result)
            }
            Err(err) => {
                if self.has_send_message_hook() {
                    if let Some(smc) = send_message_context.as_mut() {
                        smc.exception = Some(Self::boxed_context_error(err.to_string()));
                    }
                    self.execute_send_message_hook_after(&send_message_context);
                }
                Err(err)
            }
        }
        // Message state is guaranteed to be restored before function returns
    }

    pub fn execute_send_message_hook_before(&self, context: &Option<SendMessageContext<'_>>) {
        if !self.send_message_hook_list.is_empty() {
            for hook in self.send_message_hook_list.iter() {
                hook.send_message_before(context);
            }
        }
    }

    pub fn execute_send_message_hook_after(&self, context: &Option<SendMessageContext<'_>>) {
        if self.has_send_message_hook() {
            for hook in self.send_message_hook_list.iter() {
                hook.send_message_after(context);
            }
        }
    }

    #[inline]
    pub fn has_send_message_hook(&self) -> bool {
        !self.send_message_hook_list.is_empty()
    }

    fn boxed_context_error(message: String) -> Arc<Box<dyn std::error::Error + Send + Sync>> {
        Arc::new(Box::new(std::io::Error::other(message)))
    }

    #[inline]
    pub fn has_check_forbidden_hook(&self) -> bool {
        !self.check_forbidden_hook_list.is_empty()
    }

    #[inline]
    pub fn has_end_transaction_hook(&self) -> bool {
        !self.end_transaction_hook_list.is_empty()
    }

    pub fn execute_check_forbidden_hook(&self, context: &CheckForbiddenContext) -> rocketmq_error::RocketMQResult<()> {
        if self.has_check_forbidden_hook() {
            for hook in self.check_forbidden_hook_list.iter() {
                hook.check_forbidden(context)?;
            }
        }
        Ok(())
    }

    fn try_to_compress_message<T: MessageTrait>(&self, msg: &mut T) -> bool {
        if msg.as_any().downcast_ref::<MessageBatch>().is_some() {
            return false;
        }

        if let Some(message) = msg.as_any_mut().downcast_mut::<Message>() {
            let body_len = message.body_slice().len();
            if body_len >= self.producer_config.compress_msg_body_over_howmuch() as usize {
                let Some(compressor) = self.producer_config.compressor() else {
                    tracing::error!("tryToCompressMessage exception: compressor is not configured");
                    return false;
                };
                match compressor.compress(message.body_slice(), self.producer_config.compress_level()) {
                    Ok(data) => {
                        // Store the compressed data to compressed_body field
                        // (Rust design: preserve original body + store compressed separately)
                        msg.set_compressed_body_mut(data);
                        return true;
                    }
                    Err(e) => {
                        tracing::error!("tryToCompressMessage exception: {:?}", e);
                        if tracing::enabled!(tracing::Level::DEBUG) {
                            tracing::debug!("Message: {:?}", msg);
                        }
                    }
                }
            }
        }

        false
    }

    #[inline]
    pub fn select_one_message_queue(
        &self,
        tp_info: &TopicPublishInfo,
        last_broker_name: Option<&CheetahString>,
        reset_index: bool,
    ) -> Option<MessageQueue> {
        self.mq_fault_strategy
            .select_one_message_queue(tp_info, last_broker_name, reset_index)
    }

    fn validate_name_server_setting(&self) -> rocketmq_error::RocketMQResult<()> {
        let binding = self.client_instance()?.get_mq_client_api_impl()?;
        let ns_list = binding.get_name_server_address_list();
        if ns_list.is_empty() {
            return Err(mq_client_err!(
                ClientErrorCode::NO_NAME_SERVER_EXCEPTION,
                format!(
                    "No name remoting_server address, please set it. {}",
                    FAQUrl::suggest_todo(FAQUrl::NAME_SERVER_ADDR_NOT_EXIST_URL)
                )
            ));
        }
        Ok(())
    }

    async fn try_to_find_topic_publish_info(&self, topic: &Topic) -> Option<TopicPublishInfo> {
        let mut topic_publish_info = self.topic_publish_info_table.get(topic).map(|v| v.clone());
        if !topic_publish_info.as_ref().is_some_and(TopicPublishInfo::ok) {
            self.topic_publish_info_table
                .insert(topic.clone(), TopicPublishInfo::new());
            let Ok(client_instance) = self.client_instance() else {
                tracing::debug!(
                    "Skip topic route refresh for {} because MQClientInstance is not available",
                    topic
                );
                return self.topic_publish_info_table.get(topic).map(|v| v.clone());
            };
            client_instance
                .mut_from_ref()
                .update_topic_route_info_from_name_server_topic(topic)
                .await;
            topic_publish_info = self.topic_publish_info_table.get(topic).map(|v| v.clone());
        }

        let topic_publish_info_ref = topic_publish_info.as_ref()?;
        if topic_publish_info_ref.have_topic_router_info || topic_publish_info_ref.ok() {
            return topic_publish_info;
        }

        let Ok(client_instance) = self.client_instance() else {
            tracing::debug!(
                "Skip default topic route refresh for {} because MQClientInstance is not available",
                topic
            );
            return topic_publish_info;
        };
        client_instance
            .mut_from_ref()
            .update_topic_route_info_from_name_server_default(topic, true, Some(&self.producer_config))
            .await;
        self.topic_publish_info_table.get(topic).map(|v| v.clone())
    }

    fn make_sure_state_ok(&self) -> rocketmq_error::RocketMQResult<()> {
        let current_state = ProducerState::from_u8(self.state.load(Ordering::Acquire));
        if current_state != ProducerState::Running {
            return Err(mq_client_err!(format!(
                "The producer service state not OK, {:?} {}",
                current_state,
                FAQUrl::suggest_todo(FAQUrl::CLIENT_SERVICE_NOT_OK)
            )));
        }
        Ok(())
    }

    /// Ensure producer is in running state (atomic check)
    /// Freeze hook lists from mutable to immutable (called once during start)
    fn freeze_hook_lists(&mut self) {
        // Take ownership of pending hooks and convert to Arc<[_]>
        if let Some(send_hooks) = self.pending_send_hooks.lock().take() {
            if !send_hooks.is_empty() {
                self.send_message_hook_list = send_hooks.into();
                tracing::info!("Frozen {} send message hooks", self.send_message_hook_list.len());
            }
        }

        if let Some(end_hooks) = self.pending_end_transaction_hooks.lock().take() {
            if !end_hooks.is_empty() {
                self.end_transaction_hook_list = end_hooks.into();
                tracing::info!("Frozen {} end transaction hooks", self.end_transaction_hook_list.len());
            }
        }

        if let Some(forbidden_hooks) = self.pending_forbidden_hooks.lock().take() {
            if !forbidden_hooks.is_empty() {
                self.check_forbidden_hook_list = forbidden_hooks.into();
                tracing::info!("Frozen {} check forbidden hooks", self.check_forbidden_hook_list.len());
            }
        }
    }

    #[inline]
    fn ensure_running(&self) -> rocketmq_error::RocketMQResult<()> {
        if self.state.load(Ordering::Acquire) != ProducerState::Running as u8 {
            return Err(mq_client_err!(format!(
                "Producer is not running, current state: {:?}",
                ProducerState::from_u8(self.state.load(Ordering::Relaxed))
            )));
        }
        Ok(())
    }

    pub async fn invoke_message_queue_selector<M, S, T>(
        &mut self,
        msg: &mut M,
        selector: S,
        arg: &T,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<MessageQueue>
    where
        M: MessageTrait,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync,
        T: Send,
    {
        let begin_start_time = Instant::now();
        self.make_sure_state_ok()?;
        Validators::check_message(Some(msg), self.producer_config.as_ref())?;
        let topic_publish_info = self.try_to_find_topic_publish_info(msg.topic()).await;
        if let Some(topic_publish_info) = topic_publish_info {
            if topic_publish_info.ok() {
                let client_instance = self.client_instance()?;
                let message_queue_list = client_instance
                    .mut_from_ref()
                    .mq_admin_impl
                    .parse_publish_message_queues(&topic_publish_info.message_queue_list, &mut self.client_config);
                let message_queue = Self::select_message_queue_with_user_message(
                    &mut self.client_config,
                    &message_queue_list,
                    msg,
                    &selector,
                    arg,
                );
                let cost_time = begin_start_time.elapsed().as_millis() as u64;
                if timeout < cost_time {
                    return Err(rocketmq_error::RocketMQError::Timeout {
                        operation: "sendSelectImpl",
                        timeout_ms: timeout,
                    });
                }
                if let Some(message_queue) = message_queue {
                    return Ok(self.client_config.queue_with_namespace(message_queue));
                }
                return Err(mq_client_err!("select message queue return None."));
            }
        }
        self.validate_name_server_setting()?;
        Err(mq_client_err!("select message queue return null."))
    }

    pub async fn send_with_selector_timeout<M, S, T>(
        &mut self,
        msg: M,
        selector: S,
        arg: T,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync,
        T: Send + Sync,
    {
        self.send_select_impl(msg, selector, arg, CommunicationMode::Sync, None, timeout)
            .await
    }

    pub async fn fetch_publish_message_queues(
        &mut self,
        topic: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<Vec<MessageQueue>> {
        self.make_sure_state_ok()?;
        let client_instance = self.client_instance()?;
        let mq_client_api_impl = client_instance
            .mq_client_api_impl
            .clone()
            .ok_or_else(|| mq_client_err!("MQClientAPIImpl is not available; producer has not been started"))?;
        client_instance
            .mut_from_ref()
            .mq_admin_impl
            .fetch_publish_message_queues(topic, mq_client_api_impl, &mut self.client_config)
            .await
    }

    pub async fn create_topic(
        &mut self,
        key: &str,
        new_topic: &str,
        queue_num: i32,
        topic_sys_flag: i32,
        attributes: HashMap<String, String>,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.make_sure_state_ok()?;
        self.client_instance()?
            .mut_from_ref()
            .mq_admin_impl
            .create_topic(key, new_topic, queue_num, topic_sys_flag, attributes)
            .await
    }

    pub async fn search_offset(&mut self, mq: &MessageQueue, timestamp: u64) -> rocketmq_error::RocketMQResult<i64> {
        self.make_sure_state_ok()?;
        let mq = self.client_config.queue_with_namespace(mq.clone());
        self.client_instance()?
            .mut_from_ref()
            .mq_admin_impl
            .search_offset(&mq, timestamp)
            .await
    }

    pub async fn max_offset(&mut self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        self.make_sure_state_ok()?;
        let mq = self.client_config.queue_with_namespace(mq.clone());
        self.client_instance()?
            .mut_from_ref()
            .mq_admin_impl
            .max_offset(&mq)
            .await
    }

    pub async fn min_offset(&mut self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        self.make_sure_state_ok()?;
        let mq = self.client_config.queue_with_namespace(mq.clone());
        self.client_instance()?
            .mut_from_ref()
            .mq_admin_impl
            .min_offset(&mq)
            .await
    }

    pub async fn earliest_msg_store_time(&mut self, mq: &MessageQueue) -> rocketmq_error::RocketMQResult<i64> {
        self.make_sure_state_ok()?;
        let mq = self.client_config.queue_with_namespace(mq.clone());
        self.client_instance()?
            .mut_from_ref()
            .mq_admin_impl
            .earliest_msg_store_time(&mq)
            .await
    }

    pub async fn query_message(
        &mut self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: u64,
        end: u64,
    ) -> rocketmq_error::RocketMQResult<QueryResult> {
        self.make_sure_state_ok()?;
        self.client_instance()?
            .mut_from_ref()
            .mq_admin_impl
            .query_message(topic, key, max_num, begin, end)
            .await
    }

    pub async fn query_message_by_uniq_key(
        &mut self,
        topic: &str,
        uniq_key: &str,
    ) -> rocketmq_error::RocketMQResult<MessageExt> {
        self.make_sure_state_ok()?;
        let begin = current_millis().saturating_sub(QUERY_UNIQ_KEY_LOOKBACK_MILLIS);
        let result = self
            .client_instance()?
            .mut_from_ref()
            .mq_admin_impl
            .query_message_with_unique_flag(topic, uniq_key, 32, begin, i64::MAX as u64, true)
            .await?;
        result
            .message_list()
            .first()
            .cloned()
            .ok_or_else(|| mq_client_err!("query message by uniq key finished, but no message."))
    }

    pub async fn view_message(&mut self, topic: &str, msg_id: &str) -> rocketmq_error::RocketMQResult<MessageExt> {
        self.make_sure_state_ok()?;
        self.client_instance()?
            .mut_from_ref()
            .mq_admin_impl
            .view_message(topic, msg_id)
            .await
    }

    pub async fn request_with_selector<M, S, T>(
        &mut self,
        mut msg: M,
        selector: S,
        arg: T,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Box<dyn MessageTrait + Send>>
    where
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync,
        M: MessageTrait + Send + Sync,
    {
        let begin_timestamp = Instant::now();
        self.prepare_send_request(&mut msg, timeout).await?;
        let correlation_id = Self::request_correlation_id(&msg)?;
        let cost = begin_timestamp.elapsed().as_millis() as u64;
        let remaining_timeout = Self::remaining_request_timeout(timeout, cost)?;
        let request_response_future = Arc::new(RequestResponseFuture::new(correlation_id.clone(), timeout, None));
        REQUEST_FUTURE_HOLDER
            .put_request(correlation_id.to_string(), request_response_future.clone())
            .await;
        let request_response_future_inner = request_response_future.clone();
        let send_callback = move |result: Option<&SendResult>, err: Option<&dyn std::error::Error>| {
            if result.is_some() {
                request_response_future_inner.set_send_request_ok(true);
                return;
            }
            if let Some(error) = err {
                request_response_future_inner.set_send_request_ok(false);
                request_response_future_inner.put_response_message(None);
                request_response_future_inner.set_cause(Box::new(rocketmq_error::RocketmqError::MQClientErr(
                    ClientErr::new(error.to_string()),
                )) as Box<dyn std::error::Error + Send + Sync>);
            }
        };
        let topic = msg.topic().clone();
        let send_result = self
            .send_select_impl(
                msg,
                selector,
                arg,
                CommunicationMode::Async,
                Some(Arc::new(send_callback)),
                remaining_timeout,
            )
            .await;
        if let Err(error) = send_result {
            REQUEST_FUTURE_HOLDER.remove_request(correlation_id.as_str()).await;
            return Err(error);
        }
        let result = self
            .wait_response(&topic, timeout, request_response_future, remaining_timeout)
            .await;

        REQUEST_FUTURE_HOLDER.remove_request(correlation_id.as_str()).await;
        result
    }

    pub async fn request_with_selector_callback<M, S, T>(
        &mut self,
        mut msg: M,
        selector: S,
        arg: T,
        request_callback: RequestCallbackFn,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        S: Fn(&[MessageQueue], &M, &T) -> Option<MessageQueue> + Send + Sync + 'static,
        T: Send + Sync,
        M: MessageTrait + Send + Sync,
    {
        let begin_timestamp = Instant::now();
        self.prepare_send_request(&mut msg, timeout).await?;
        let correlation_id = Self::request_correlation_id(&msg)?;
        let cost = begin_timestamp.elapsed().as_millis() as u64;
        let remaining_timeout = Self::remaining_request_timeout(timeout, cost)?;
        let request_response_future = Arc::new(RequestResponseFuture::new(
            correlation_id.clone(),
            timeout,
            Some(request_callback.clone()),
        ));
        REQUEST_FUTURE_HOLDER
            .put_request(correlation_id.to_string(), request_response_future.clone())
            .await;
        let send_callback = move |result: Option<&SendResult>, err: Option<&dyn std::error::Error>| {
            if result.is_some() {
                request_response_future.set_send_request_ok(true);
                return;
            }
            if let Some(error) = err {
                request_response_future.set_cause(Box::new(rocketmq_error::RocketmqError::MQClientErr(ClientErr::new(
                    error.to_string(),
                ))) as Box<dyn std::error::Error + Send + Sync>);
                Self::request_fail(correlation_id.as_str());
            }
        };
        let _ = self
            .send_select_impl(
                msg,
                selector,
                arg,
                CommunicationMode::Async,
                Some(Arc::new(send_callback)),
                remaining_timeout,
            )
            .await?;
        Ok(())
    }

    pub async fn request_to_queue<M>(
        &mut self,
        mut msg: M,
        mq: MessageQueue,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Box<dyn MessageTrait + Send>>
    where
        M: MessageTrait + Send + Sync,
    {
        let begin_timestamp = Instant::now();
        self.prepare_send_request(&mut msg, timeout).await?;
        let correlation_id = Self::request_correlation_id(&msg)?;
        let cost = begin_timestamp.elapsed().as_millis() as u64;
        let remaining_timeout = Self::remaining_request_timeout(timeout, cost)?;
        let request_response_future = Arc::new(RequestResponseFuture::new(correlation_id.clone(), timeout, None));
        REQUEST_FUTURE_HOLDER
            .put_request(correlation_id.to_string(), request_response_future.clone())
            .await;
        let request_response_future_inner = request_response_future.clone();
        let send_callback = move |result: Option<&SendResult>, err: Option<&dyn std::error::Error>| {
            if result.is_some() {
                request_response_future_inner.set_send_request_ok(true);
                return;
            }
            if let Some(error) = err {
                request_response_future_inner.set_send_request_ok(false);
                request_response_future_inner.put_response_message(None);
                request_response_future_inner.set_cause(Box::new(rocketmq_error::RocketmqError::MQClientErr(
                    ClientErr::new(error.to_string()),
                )) as Box<dyn std::error::Error + Send + Sync>);
            }
        };
        let topic = msg.topic().clone();
        let send_result = self
            .send_kernel_impl(
                &mut msg,
                &mq,
                CommunicationMode::Async,
                Some(Arc::new(send_callback)),
                None,
                remaining_timeout,
            )
            .await;
        if let Err(error) = send_result {
            REQUEST_FUTURE_HOLDER.remove_request(correlation_id.as_str()).await;
            return Err(error);
        }
        let result = self
            .wait_response(&topic, timeout, request_response_future, remaining_timeout)
            .await;

        REQUEST_FUTURE_HOLDER.remove_request(correlation_id.as_str()).await;
        result
    }

    pub async fn request_to_queue_with_callback<M>(
        &mut self,
        mut msg: M,
        mq: MessageQueue,
        request_callback: RequestCallbackFn,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
    {
        let begin_timestamp = Instant::now();
        self.prepare_send_request(&mut msg, timeout).await?;
        let correlation_id = Self::request_correlation_id(&msg)?;
        let cost = begin_timestamp.elapsed().as_millis() as u64;
        let remaining_timeout = Self::remaining_request_timeout(timeout, cost)?;
        let request_response_future = Arc::new(RequestResponseFuture::new(
            correlation_id.clone(),
            timeout,
            Some(request_callback.clone()),
        ));
        REQUEST_FUTURE_HOLDER
            .put_request(correlation_id.to_string(), request_response_future.clone())
            .await;
        let send_callback = move |result: Option<&SendResult>, err: Option<&dyn std::error::Error>| {
            if result.is_some() {
                request_response_future.set_send_request_ok(true);
                return;
            }
            if let Some(error) = err {
                request_response_future.set_cause(Box::new(rocketmq_error::RocketmqError::MQClientErr(ClientErr::new(
                    error.to_string(),
                ))) as Box<dyn std::error::Error + Send + Sync>);
                Self::request_fail(correlation_id.as_str());
            }
        };
        let _ = self
            .send_kernel_impl(
                &mut msg,
                &mq,
                CommunicationMode::Async,
                Some(Arc::new(send_callback)),
                None,
                remaining_timeout,
            )
            .await?;
        Ok(())
    }

    pub async fn request_with_callback<M>(
        &mut self,
        mut msg: M,
        request_callback: RequestCallbackFn,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
    {
        let begin_timestamp = Instant::now();
        self.prepare_send_request(&mut msg, timeout).await?;
        let correlation_id = Self::request_correlation_id(&msg)?;
        let cost = begin_timestamp.elapsed().as_millis() as u64;
        let remaining_timeout = Self::remaining_request_timeout(timeout, cost)?;
        let request_response_future = Arc::new(RequestResponseFuture::new(
            correlation_id.clone(),
            timeout,
            Some(request_callback.clone()),
        ));
        REQUEST_FUTURE_HOLDER
            .put_request(correlation_id.to_string(), request_response_future.clone())
            .await;
        let send_callback = move |result: Option<&SendResult>, err: Option<&dyn std::error::Error>| {
            if result.is_some() {
                request_response_future.set_send_request_ok(true);
                request_response_future.execute_request_callback();
                return;
            }
            if let Some(error) = err {
                request_response_future.set_cause(Box::new(rocketmq_error::RocketmqError::MQClientErr(ClientErr::new(
                    error.to_string(),
                ))) as Box<dyn std::error::Error + Send + Sync>);
                Self::request_fail(correlation_id.as_str());
            }
        };
        self.send_default_impl(
            &mut msg,
            CommunicationMode::Async,
            Some(Arc::new(send_callback)),
            remaining_timeout,
        )
        .await?;
        Ok(())
    }

    async fn request_fail(correlation_id: &str) {
        let request_response_future = REQUEST_FUTURE_HOLDER.remove_request_and_get(correlation_id).await;
        if let Some(request_response_future) = request_response_future {
            request_response_future.set_send_request_ok(false);
            request_response_future.put_response_message(None);
            request_response_future.execute_request_callback();
        }
    }

    pub async fn request<M>(
        &mut self,
        mut msg: M,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Box<dyn MessageTrait + Send>>
    where
        M: MessageTrait + Send + Sync,
    {
        let begin_timestamp = Instant::now();
        self.prepare_send_request(&mut msg, timeout).await?;
        let correlation_id = Self::request_correlation_id(&msg)?;
        let topic = msg.topic().clone();
        let cost = begin_timestamp.elapsed().as_millis() as u64;
        let remaining_timeout = Self::remaining_request_timeout(timeout, cost)?;
        let request_response_future = Arc::new(RequestResponseFuture::new(correlation_id.clone(), timeout, None));
        REQUEST_FUTURE_HOLDER
            .put_request(correlation_id.to_string(), request_response_future.clone())
            .await;
        let request_response_future_inner = request_response_future.clone();
        let send_callback = move |result: Option<&SendResult>, err: Option<&dyn std::error::Error>| {
            if result.is_some() {
                request_response_future_inner.set_send_request_ok(true);
                return;
            }
            if let Some(error) = err {
                //request_response_future_inner.set_send_request_ok(false);
                request_response_future_inner.put_response_message(None);
                request_response_future_inner.set_cause(Box::new(rocketmq_error::RocketmqError::MQClientErr(
                    ClientErr::new(error.to_string()),
                )) as Box<dyn std::error::Error + Send + Sync>);
            }
        };
        let send_result = self
            .send_default_impl(
                &mut msg,
                CommunicationMode::Async,
                Some(Arc::new(send_callback)),
                remaining_timeout,
            )
            .await;
        if let Err(error) = send_result {
            REQUEST_FUTURE_HOLDER.remove_request(correlation_id.as_str()).await;
            return Err(error);
        }

        let result = self
            .wait_response(&topic, timeout, request_response_future, remaining_timeout)
            .await;

        REQUEST_FUTURE_HOLDER.remove_request(correlation_id.as_str()).await;
        result
    }

    async fn wait_response(
        &mut self,
        topic: &CheetahString,
        timeout: u64,
        request_response_future: Arc<RequestResponseFuture>,
        remaining_timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Box<dyn MessageTrait + Send>> {
        let response_message = request_response_future
            .wait_response_message(Duration::from_millis(remaining_timeout))
            .await;

        if let Some(response_message) = response_message {
            Ok(response_message)
        } else if request_response_future.is_send_request_ok().await {
            Err(rocketmq_error::RocketMQError::Timeout {
                operation: "send request message",
                timeout_ms: timeout,
            })
        } else {
            Err(mq_client_err!(format!(
                "send request message to <{}> fail, {}",
                topic,
                request_response_future
                    .get_cause()
                    .map_or("".to_string(), |cause| { cause.to_string() })
            )))
        }
    }

    async fn prepare_send_request<M>(&mut self, msg: &mut M, timeout: u64) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait,
    {
        let correlation_id = CorrelationIdUtil::create_correlation_id();
        let client_instance = self.client_instance()?;
        let request_client_id = client_instance.client_id.clone();
        MessageAccessor::put_property(
            msg,
            CheetahString::from_static_str(MessageConst::PROPERTY_CORRELATION_ID),
            CheetahString::from_string(correlation_id),
        );
        MessageAccessor::put_property(
            msg,
            CheetahString::from_static_str(MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT),
            request_client_id,
        );
        MessageAccessor::put_property(
            msg,
            CheetahString::from_static_str(MessageConst::PROPERTY_MESSAGE_TTL),
            CheetahString::from_string(timeout.to_string()),
        );
        let has_route_data = client_instance.topic_route_table.contains_key(msg.topic().as_str());
        if !has_route_data {
            let begin_timestamp = Instant::now();
            self.try_to_find_topic_publish_info(msg.topic()).await;
            client_instance
                .mut_from_ref()
                .send_heartbeat_to_all_broker_with_lock()
                .await;
            let cost = begin_timestamp.elapsed().as_millis() as u64;
            if cost > 500 {
                warn!("prepare send request for <{}> cost {} ms", msg.topic(), cost);
            }
        }
        Ok(())
    }

    pub async fn send_message_in_transaction<M>(
        &mut self,
        mut msg: M,
        arg: Option<Box<dyn Any + Send + Sync>>,
    ) -> rocketmq_error::RocketMQResult<TransactionSendResult>
    where
        M: MessageTrait + Send + Sync,
    {
        let transaction_listener = self
            .transaction_listener
            .clone()
            .ok_or_else(|| mq_client_err!("tranExecutor is null"))?;

        // Ensure transactional messages do not support delayed delivery
        self.ensure_not_delayed_for_transactional(&msg)?;

        // ignore DelayTimeLevel parameter
        if msg.delay_time_level() != 0 {
            MessageAccessor::clear_property(&mut msg, MessageConst::PROPERTY_DELAY_TIME_LEVEL);
        }
        Validators::check_message(Some(&msg), self.producer_config.as_ref())?;
        MessageAccessor::put_property(
            &mut msg,
            CheetahString::from_static_str(MessageConst::PROPERTY_TRANSACTION_PREPARED),
            CheetahString::from_string("true".to_owned()),
        );
        MessageAccessor::put_property(
            &mut msg,
            CheetahString::from_static_str(MessageConst::PROPERTY_PRODUCER_GROUP),
            self.producer_config.producer_group().to_owned(),
        );
        let send_result = self
            .send(&mut msg)
            .await
            .map_err(|e| mq_client_err!(format!("send message in transaction error, {}", e)))?
            .ok_or_else(|| mq_client_err!("send result is none"))?;
        let (local_transaction_state, local_exception) = match send_result.send_status {
            SendStatus::SendOk => {
                if let Some(ref transaction_id) = send_result.transaction_id {
                    msg.put_user_property(
                        CheetahString::from_static_str(MessageConst::PROPERTY_TRANSACTION_ID),
                        CheetahString::from_string(transaction_id.to_owned()),
                    )
                    .map_err(|e| mq_client_err!(e.to_string()))?;
                }
                let transaction_id = msg.property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
                ));
                if let Some(transaction_id) = transaction_id {
                    msg.set_transaction_id(transaction_id);
                }
                Self::execute_local_transaction_branch(&transaction_listener, &msg, arg.as_deref())
            }
            SendStatus::FlushDiskTimeout | SendStatus::FlushSlaveTimeout | SendStatus::SlaveNotAvailable => {
                (LocalTransactionState::RollbackMessage, None)
            }
        };
        if let Err(e) = self
            .end_transaction(&msg, &send_result, local_transaction_state, local_exception)
            .await
        {
            warn!(
                "local transaction execute {}, but end broker transaction failed,{}",
                local_transaction_state,
                e.to_string()
            );
        }
        let transaction_send_result = TransactionSendResult {
            local_transaction_state: Some(local_transaction_state),
            send_result: Some(send_result),
        };
        Ok(transaction_send_result)
    }

    fn execute_local_transaction_branch(
        transaction_listener: &ArcTransactionListener,
        msg: &dyn MessageTrait,
        arg: Option<&(dyn Any + Send + Sync)>,
    ) -> (LocalTransactionState, Option<CheetahString>) {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            transaction_listener.execute_local_transaction(msg, arg)
        })) {
            Ok(state) => (state, None),
            Err(error) => {
                let error_message = Self::panic_payload_to_string(error.as_ref());
                tracing::error!(
                    "executeLocalTransactionBranch panic, messageTopic: {} transactionId: {:?}: {}",
                    msg.topic(),
                    msg.transaction_id(),
                    error_message
                );
                (
                    LocalTransactionState::Unknown,
                    Some(CheetahString::from_string(format!(
                        "executeLocalTransactionBranch exception: {}",
                        error_message
                    ))),
                )
            }
        }
    }

    fn panic_payload_to_string(error: &(dyn Any + Send)) -> String {
        if let Some(message) = error.downcast_ref::<&'static str>() {
            (*message).to_string()
        } else if let Some(message) = error.downcast_ref::<String>() {
            message.clone()
        } else {
            "non-string panic payload".to_string()
        }
    }

    fn u64_to_java_long_field(
        operation: &'static str,
        field: &'static str,
        value: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        i64::try_from(value).map_err(|_| {
            rocketmq_error::RocketMQError::IllegalArgument(format!("{operation} {field} exceeds Java long range"))
        })
    }

    pub async fn end_transaction(
        &mut self,
        msg: &dyn MessageTrait,
        send_result: &SendResult,
        local_transaction_state: LocalTransactionState,
        local_exception: Option<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<()> {
        let id = if let Some(ref offset_msg_id) = send_result.offset_msg_id {
            MessageDecoder::decode_message_id(offset_msg_id).map_err(|e| {
                rocketmq_error::RocketMQError::IllegalArgument(format!("Failed to decode message ID: {}", e))
            })?
        } else {
            let msg_id = send_result
                .msg_id
                .as_ref()
                .ok_or_else(|| mq_client_err!("send result missing msg_id for end transaction"))?;
            MessageDecoder::decode_message_id(msg_id).map_err(|e| {
                rocketmq_error::RocketMQError::IllegalArgument(format!("Failed to decode message ID: {}", e))
            })?
        };
        let transaction_id = send_result.transaction_id.clone();
        let message_queue = send_result
            .message_queue
            .clone()
            .ok_or_else(|| mq_client_err!("send result missing message_queue for end transaction"))?;
        let queue = self.client_config.queue_with_namespace(message_queue);
        let client_instance = self
            .client_instance
            .as_ref()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("MQClientInstance"))?;
        let dest_broker_name = client_instance.get_broker_name_from_message_queue(&queue).await;
        let broker_addr = client_instance
            .find_broker_address_in_publish(dest_broker_name.as_ref())
            .ok_or_else(|| mq_client_err!(format!("broker address not found for {}", dest_broker_name)))?;
        let request_header = EndTransactionRequestHeader {
            topic: CheetahString::from_string(msg.topic().to_string()),
            producer_group: CheetahString::from_string(self.producer_config.producer_group().to_string()),
            tran_state_table_offset: Self::u64_to_java_long_field(
                "endTransaction",
                "tranStateTableOffset",
                send_result.queue_offset,
            )?,
            commit_log_offset: id.offset,
            commit_or_rollback: match local_transaction_state {
                LocalTransactionState::CommitMessage => MessageSysFlag::TRANSACTION_COMMIT_TYPE,
                LocalTransactionState::RollbackMessage => MessageSysFlag::TRANSACTION_ROLLBACK_TYPE,
                LocalTransactionState::Unknown => MessageSysFlag::TRANSACTION_NOT_TYPE,
            },
            from_transaction_check: false,
            msg_id: send_result.msg_id.clone().unwrap_or_default(),
            transaction_id: transaction_id.map(CheetahString::from_string),
            rpc_request_header: RpcRequestHeader {
                broker_name: Some(dest_broker_name),
                ..Default::default()
            },
        };
        if let Some(message) = msg.as_any().downcast_ref::<Message>() {
            self.do_execute_end_transaction_hook(
                message,
                &request_header.msg_id,
                &broker_addr,
                local_transaction_state,
                false,
            );
        }
        self.client_instance
            .as_mut()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("MQClientInstance"))?
            .mq_client_api_impl
            .as_mut()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("MQClientAPIImpl"))?
            .end_transaction_oneway(
                &broker_addr,
                request_header,
                local_exception.unwrap_or_default(),
                self.producer_config.send_msg_timeout() as u64,
            )
            .await;
        Ok(())
    }

    pub fn do_execute_end_transaction_hook(
        &self,
        msg: &Message,
        msg_id: &CheetahString,
        broker_addr: &CheetahString,
        local_transaction_state: LocalTransactionState,
        from_transaction_check: bool,
    ) {
        if !self.has_end_transaction_hook() {
            return;
        }
        let end_transaction_context = EndTransactionContext {
            producer_group: self.producer_config.producer_group().clone(),
            message: msg,
            msg_id: msg_id.clone(),
            transaction_id: msg.get_transaction_id().cloned().unwrap_or_default(),
            broker_addr: broker_addr.clone(),
            from_transaction_check,
            transaction_state: local_transaction_state,
        };
        self.execute_end_transaction_hook(&end_transaction_context);
    }

    pub fn execute_end_transaction_hook<'a>(&self, context: &'a EndTransactionContext<'a>) {
        if self.has_end_transaction_hook() {
            for hook in self.end_transaction_hook_list.iter() {
                hook.end_transaction(context);
            }
        }
    }

    fn build_end_transaction_header_for_check(
        producer_group: CheetahString,
        check_request_header: &CheckTransactionStateRequestHeader,
        msg_id: CheetahString,
        transaction_state: LocalTransactionState,
    ) -> EndTransactionRequestHeader {
        EndTransactionRequestHeader {
            topic: check_request_header.topic.clone().unwrap_or_default(),
            producer_group,
            tran_state_table_offset: check_request_header.tran_state_table_offset,
            commit_log_offset: check_request_header.commit_log_offset,
            commit_or_rollback: match transaction_state {
                LocalTransactionState::CommitMessage => MessageSysFlag::TRANSACTION_COMMIT_TYPE,
                LocalTransactionState::RollbackMessage => MessageSysFlag::TRANSACTION_ROLLBACK_TYPE,
                LocalTransactionState::Unknown => MessageSysFlag::TRANSACTION_NOT_TYPE,
            },
            from_transaction_check: true,
            msg_id,
            transaction_id: check_request_header.transaction_id.clone(),
            rpc_request_header: RpcRequestHeader {
                broker_name: check_request_header
                    .rpc_request_header
                    .clone()
                    .unwrap_or_default()
                    .broker_name,
                ..Default::default()
            },
        }
    }

    pub fn set_default_mqproducer_impl_inner(
        &mut self,
        default_mqproducer_impl_inner: WeakArcMut<DefaultMQProducerImpl>,
    ) {
        self.default_mqproducer_impl_inner = Some(default_mqproducer_impl_inner);
    }

    pub fn set_transaction_listener(&mut self, transaction_listener: ArcTransactionListener) {
        self.transaction_listener = Some(transaction_listener);
    }

    pub fn check_listener(&self) -> Option<ArcTransactionListener> {
        self.transaction_listener.clone()
    }
}

impl MQProducerInner for DefaultMQProducerImpl {
    fn get_publish_topic_list(&self) -> HashSet<CheetahString> {
        self.topic_publish_info_table
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    fn is_publish_topic_need_update(&self, topic: &CheetahString) -> bool {
        if let Some(topic_publish_info) = self.topic_publish_info_table.get(topic) {
            return !topic_publish_info.ok();
        }
        true
    }

    fn get_check_listener(&self) -> Option<ArcTransactionListener> {
        self.transaction_listener.clone()
    }

    fn check_transaction_state(
        &self,
        broker_addr: &CheetahString,
        msg: MessageExt,
        check_request_header: CheckTransactionStateRequestHeader,
    ) {
        let Some(transaction_listener) = self.transaction_listener.clone() else {
            warn!("TransactionListener is null, cannot check transaction state");
            return;
        };
        let Some(mut producer_impl_inner) = self
            .default_mqproducer_impl_inner
            .as_ref()
            .and_then(|weak| weak.upgrade())
        else {
            warn!("Failed to upgrade default_mqproducer_impl_inner: producer implementation is not available");
            return;
        };
        let broker_addr = broker_addr.clone();
        let group = self.producer_config.producer_group().clone();

        let Some(transaction_check_env) = self.transaction_check_env.clone() else {
            warn!("Transaction check env is not initialized, cannot check transaction state");
            return;
        };
        let Ok(request_slot) = transaction_check_env.request_slots.clone().try_acquire_owned() else {
            warn!(
                "Transaction check request rejected: hold queue is full for producer group {}",
                group
            );
            return;
        };
        let worker_slots = transaction_check_env.worker_slots.clone();
        let executor_service = self.transaction_executor_service.clone();
        let task_executor = executor_service.clone();
        let group_for_spawn_error = group.clone();

        let task = async move {
            let _request_slot = request_slot;
            let Ok(_worker_slot) = worker_slots.acquire_owned().await else {
                tracing::warn!(
                    "Transaction check worker limiter was closed for producer group {}",
                    group
                );
                return;
            };

            // Use spawn_blocking to avoid blocking Tokio worker threads (matches Java's
            // ExecutorService behavior)
            let check_group = group.clone();
            let check_task = move || {
                let unique_key = msg
                    .property(&CheetahString::from_static_str(
                        MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
                    ))
                    .unwrap_or_else(|| msg.msg_id.clone());

                // Check local transaction state with exception handling (synchronous execution)
                let transaction_state = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    transaction_listener.check_local_transaction(&msg)
                })) {
                    Ok(state) => state,
                    Err(e) => {
                        tracing::error!(
                            "Broker call checkTransactionState, but checkLocalTransaction panic: {:?}, group: {}",
                            e,
                            check_group
                        );
                        LocalTransactionState::Unknown
                    }
                };

                (msg, unique_key, transaction_state)
            };
            let check_result = if let Some(executor) = task_executor {
                executor
                    .spawn_blocking(check_task)
                    .await
                    .map_err(|error| error.to_string())
            } else {
                spawn_client_blocking_io("client.transaction.check", check_task)
                    .await
                    .map_err(|error| error.to_string())
            };

            let Ok((msg, unique_key, transaction_state)) = check_result else {
                tracing::error!("Transaction check task join failed for producer group {}", group);
                return;
            };

            let request_header = Self::build_end_transaction_header_for_check(
                producer_impl_inner.producer_config.producer_group().clone(),
                &check_request_header,
                unique_key.clone(),
                transaction_state,
            );
            // Execute end transaction hook
            producer_impl_inner.do_execute_end_transaction_hook(
                &msg.message,
                &unique_key,
                &broker_addr,
                transaction_state,
                true,
            );

            // Send end transaction request with error handling
            let Some(client_instance) = producer_impl_inner.client_instance.as_mut() else {
                tracing::warn!("endTransactionOneway skipped: client instance is not available");
                return;
            };
            let Some(mq_client_api_impl) = client_instance.mq_client_api_impl.as_mut() else {
                tracing::warn!("endTransactionOneway skipped: MQClientAPIImpl is not available");
                return;
            };
            if let Err(e) = mq_client_api_impl
                .end_transaction_oneway(&broker_addr, request_header, CheetahString::from_static_str(""), 3000)
                .await
            {
                tracing::error!("endTransactionOneway exception: {:?}", e);
            }
        };
        if let Err(error) = spawn_producer_task(
            executor_service.as_ref(),
            "rocketmq-client-producer-transaction-check",
            &self.producer_task_tracker,
            &self.producer_task_shutdown,
            task,
        ) {
            warn!(
                "Failed to spawn transaction check task for producer group {}: {}",
                group_for_spawn_error, error
            );
        }
    }

    fn update_topic_publish_info(&mut self, topic: impl Into<CheetahString>, info: Option<TopicPublishInfo>) {
        let topic = topic.into();
        if topic.is_empty() {
            return;
        }
        let Some(info) = info else {
            return;
        };
        self.topic_publish_info_table.insert(topic, info);
    }

    fn is_unit_mode(&self) -> bool {
        self.client_config.unit_mode
    }
}

impl DefaultMQProducerImpl {
    pub async fn start(&mut self) -> rocketmq_error::RocketMQResult<()> {
        self.start_with_factory(true).await
    }

    #[inline]
    pub async fn start_with_factory(&mut self, start_factory: bool) -> rocketmq_error::RocketMQResult<()> {
        // Atomic CAS state transition
        match self.state.compare_exchange(
            ProducerState::Created as u8,
            ProducerState::Starting as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => {
                // First-time startup
                self.service_state = ServiceState::StartFailed;

                // Freeze hook lists (convert from mutable Vec to immutable Arc<[_]>)
                self.freeze_hook_lists();

                if let Err(error) = self.check_config() {
                    self.state.store(ProducerState::StartFailed as u8, Ordering::SeqCst);
                    return Err(error);
                }

                if self.producer_config.producer_group() != CLIENT_INNER_PRODUCER_GROUP {
                    self.client_config.change_instance_name_to_pid();
                }

                let client_instance = MQClientManager::get_instance()
                    .get_or_create_mq_client_instance(self.client_config.clone(), self.rpc_hook.clone());

                let service_detector = DefaultServiceDetector {
                    client_instance: client_instance.clone(),
                    topic_publish_info_table: self.topic_publish_info_table.clone(),
                };
                let resolver = DefaultResolver {
                    client_instance: client_instance.clone(),
                };
                self.mq_fault_strategy.set_resolve(resolver);
                self.mq_fault_strategy.set_service_detector(service_detector);
                self.client_instance = Some(client_instance.clone());
                let self_clone = self
                    .default_mqproducer_impl_inner
                    .clone()
                    .and_then(|weak| weak.upgrade())
                    .ok_or_else(|| {
                        mq_client_err!(
                            "Failed to upgrade default_mqproducer_impl_inner: producer implementation is not available"
                        )
                    });
                let self_clone = match self_clone {
                    Ok(self_clone) => self_clone,
                    Err(error) => {
                        self.state.store(ProducerState::StartFailed as u8, Ordering::SeqCst);
                        return Err(error);
                    }
                };
                let register_ok = client_instance
                    .mut_from_ref()
                    .register_producer(
                        self.producer_config.producer_group(),
                        MQProducerInnerImpl {
                            default_mqproducer_impl_inner: Some(self_clone),
                        },
                    )
                    .await;
                if !register_ok {
                    self.service_state = ServiceState::CreateJust;
                    self.state.store(ProducerState::Created as u8, Ordering::SeqCst);
                    return Err(mq_client_err!(format!(
                        "The producer group[{}] has been created before, specify another name please. {}",
                        self.producer_config.producer_group(),
                        FAQUrl::suggest_todo(FAQUrl::GROUP_NAME_DUPLICATE_URL)
                    )));
                }
                if start_factory {
                    if let Err(error) = Box::pin(client_instance.mut_from_ref().start(client_instance.clone())).await {
                        self.state.store(ProducerState::StartFailed as u8, Ordering::SeqCst);
                        return Err(error);
                    }
                }

                self.init_topic_route().await;
                self.mq_fault_strategy.start_detector();

                // Update both states
                self.service_state = ServiceState::Running;
                self.state.store(ProducerState::Running as u8, Ordering::SeqCst);
                REQUEST_FUTURE_HOLDER
                    .start_scheduled_task(self.producer_config.producer_group().to_string())
                    .await;

                tracing::info!(
                    "Producer [{}] started successfully",
                    self.producer_config.producer_group()
                );
                Ok(())
            }
            Err(current) => {
                let state = ProducerState::from_u8(current);
                match state {
                    ProducerState::Running => {
                        // Already running, idempotent
                        Ok(())
                    }
                    ProducerState::Starting => {
                        // Another thread is starting, wait for completion
                        while self.state.load(Ordering::SeqCst) == ProducerState::Starting as u8 {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                        self.ensure_running()
                    }
                    ProducerState::Stopped => Err(mq_client_err!("The producer service state is ShutdownAlready")),
                    ProducerState::StartFailed => Err(mq_client_err!(format!(
                        "The producer service state not OK, maybe started once, {:?} {}",
                        state,
                        FAQUrl::suggest_todo(FAQUrl::CLIENT_SERVICE_NOT_OK)
                    ))),
                    _ => Err(mq_client_err!(format!("Cannot start producer in state {:?}", state))),
                }
            }
        }
    }

    /// Shutdown the producer gracefully
    pub async fn shutdown(&mut self) -> rocketmq_error::RocketMQResult<()> {
        self.shutdown_with_factory(true).await
    }

    async fn shutdown_producer_tasks(&self) {
        self.producer_task_tracker.close();
        if tokio::time::timeout(PRODUCER_TASK_SHUTDOWN_TIMEOUT, self.producer_task_tracker.wait())
            .await
            .is_ok()
        {
            return;
        }

        tracing::warn!(
            timeout_ms = PRODUCER_TASK_SHUTDOWN_TIMEOUT.as_millis(),
            "producer background send tasks did not stop before graceful timeout; cancelling"
        );
        self.producer_task_shutdown.cancel();

        if tokio::time::timeout(PRODUCER_TASK_FORCE_SHUTDOWN_TIMEOUT, self.producer_task_tracker.wait())
            .await
            .is_err()
        {
            tracing::warn!(
                timeout_ms = PRODUCER_TASK_FORCE_SHUTDOWN_TIMEOUT.as_millis(),
                "producer background send tasks did not stop after cancellation"
            );
        }
    }

    /// Shutdown the producer with option to shutdown factory
    pub async fn shutdown_with_factory(&mut self, shutdown_factory: bool) -> rocketmq_error::RocketMQResult<()> {
        // Atomic CAS state transition
        match self.state.compare_exchange(
            ProducerState::Running as u8,
            ProducerState::Stopping as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => {
                self.shutdown_producer_tasks().await;

                // Perform shutdown
                self.do_shutdown_internal(shutdown_factory).await?;

                // Update states
                self.service_state = ServiceState::ShutdownAlready;
                self.state.store(ProducerState::Stopped as u8, Ordering::SeqCst);

                tracing::info!("Producer [{}] shutdown OK", self.producer_config.producer_group());
                Ok(())
            }
            Err(current) => {
                let state = ProducerState::from_u8(current);
                match state {
                    ProducerState::Stopped => {
                        // Already stopped, idempotent
                        Ok(())
                    }
                    ProducerState::Created => {
                        // Not started, nothing to do
                        Ok(())
                    }
                    ProducerState::StartFailed => {
                        // Java shutdown() falls through for START_FAILED.
                        Ok(())
                    }
                    ProducerState::Stopping => {
                        // Another thread is stopping, wait for completion
                        while self.state.load(Ordering::SeqCst) == ProducerState::Stopping as u8 {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                        Ok(())
                    }
                    _ => Err(mq_client_err!(format!("Cannot shutdown producer in state {:?}", state))),
                }
            }
        }
    }

    /// Internal shutdown logic
    async fn do_shutdown_internal(&mut self, shutdown_factory: bool) -> rocketmq_error::RocketMQResult<()> {
        let producer_group = self.producer_config.producer_group().to_string();

        // 1. Unregister producer from client instance
        if let Some(client_instance) = self.client_instance.as_mut() {
            client_instance
                .unregister_producer(self.producer_config.producer_group())
                .await;
        }

        REQUEST_FUTURE_HOLDER.shutdown(producer_group.as_str()).await;

        // 2. Stop fault strategy detector
        if !self.mq_fault_strategy.shutdown_async().await {
            tracing::warn!(
                "producer [{}] fault detector task did not stop before timeout; aborted",
                producer_group
            );
        }

        // 3. Shutdown client factory if requested
        if shutdown_factory {
            if let Some(client_instance) = self.client_instance.as_mut() {
                client_instance.shutdown().await;
            }
        }

        Ok(())
    }

    pub fn register_end_transaction_hook(&mut self, hook: Arc<dyn EndTransactionHook>) {
        // Only allow registration before start
        let current_state = ProducerState::from_u8(self.state.load(Ordering::Relaxed));
        if current_state != ProducerState::Created {
            tracing::warn!(
                "Cannot register hook after producer started (state: {:?})",
                current_state
            );
            return;
        }

        if let Some(pending) = self.pending_end_transaction_hooks.lock().as_mut() {
            pending.push(hook);
            tracing::info!("Registered endTransaction Hook, pending hooks: {}", pending.len());
        }
    }

    pub fn register_check_forbidden_hook(&mut self, hook: Arc<dyn CheckForbiddenHook>) {
        // Only allow registration before start
        let current_state = ProducerState::from_u8(self.state.load(Ordering::Relaxed));
        if current_state != ProducerState::Created {
            tracing::warn!(
                "Cannot register hook after producer started (state: {:?})",
                current_state
            );
            return;
        }

        if let Some(pending) = self.pending_forbidden_hooks.lock().as_mut() {
            pending.push(hook);
            tracing::info!("Registered checkForbidden Hook, pending hooks: {}", pending.len());
        }
    }

    pub fn register_send_message_hook(&mut self, hook: Arc<dyn SendMessageHook>) {
        // Only allow registration before start
        let current_state = ProducerState::from_u8(self.state.load(Ordering::Relaxed));
        if current_state != ProducerState::Created {
            tracing::warn!(
                "Cannot register hook after producer started (state: {:?})",
                current_state
            );
            return;
        }

        if let Some(pending) = self.pending_send_hooks.lock().as_mut() {
            pending.push(hook);
            tracing::info!("Registered sendMessage Hook, pending hooks: {}", pending.len());
        }
    }

    pub fn set_rpc_hook(&mut self, rpc_hook: Arc<dyn RPCHook>) {
        let current_state = ProducerState::from_u8(self.state.load(Ordering::Relaxed));
        if current_state != ProducerState::Created {
            tracing::warn!(
                "Cannot update RPC hook after producer started (state: {:?})",
                current_state
            );
            return;
        }

        self.rpc_hook = Some(rpc_hook);
    }

    #[inline]
    fn check_config(&self) -> rocketmq_error::RocketMQResult<()> {
        Validators::check_group(self.producer_config.producer_group())?;
        if self.producer_config.producer_group() == DEFAULT_PRODUCER_GROUP {
            return Err(mq_client_err!(format!(
                "The specified group name[{}] is equal to default group, please specify another one.",
                DEFAULT_PRODUCER_GROUP
            )));
        }
        Ok(())
    }

    async fn init_topic_route(&mut self) {
        for topic in self.producer_config.topics() {
            let new_topic =
                NamespaceUtil::wrap_namespace(self.client_config.get_namespace().unwrap_or_default().as_str(), topic);
            let topic_publish_info = self.try_to_find_topic_publish_info(&new_topic).await;
            if !topic_publish_info.as_ref().is_some_and(TopicPublishInfo::ok) {
                warn!(
                    "No route info of this topic: {} {}",
                    new_topic,
                    FAQUrl::suggest_todo(FAQUrl::NO_TOPIC_ROUTE_INFO)
                );
            }
        }
    }

    #[inline]
    pub fn set_send_latency_fault_enable(&mut self, send_latency_fault_enable: bool) {
        self.client_config.set_send_latency_enable(send_latency_fault_enable);
        self.mq_fault_strategy
            .set_send_latency_fault_enable(send_latency_fault_enable);
    }

    #[inline]
    pub fn is_send_latency_fault_enable(&self) -> bool {
        self.mq_fault_strategy.is_send_latency_fault_enable()
    }

    #[inline]
    pub fn set_start_detector_enable(&mut self, start_detector_enable: bool) {
        self.client_config.set_start_detector_enable(start_detector_enable);
        self.mq_fault_strategy.set_start_detector_enable(start_detector_enable);
    }

    #[inline]
    pub fn is_start_detector_enable(&self) -> bool {
        self.mq_fault_strategy.is_start_detector_enable()
    }

    #[inline]
    pub fn is_send_message_with_vip_channel(&self) -> bool {
        self.client_config.is_vip_channel_enabled()
    }

    #[inline]
    pub fn set_send_message_with_vip_channel(&mut self, send_message_with_vip_channel: bool) {
        self.client_config
            .set_vip_channel_enabled(send_message_with_vip_channel);
    }

    #[inline]
    pub fn latency_max(&self) -> &[u64] {
        self.mq_fault_strategy.get_latency_max()
    }

    #[inline]
    pub fn set_latency_max(&mut self, latency_max: impl Into<Vec<u64>>) {
        self.mq_fault_strategy.set_latency_max(latency_max);
    }

    #[inline]
    pub fn not_available_duration(&self) -> &[u64] {
        self.mq_fault_strategy.get_not_available_duration()
    }

    #[inline]
    pub fn set_not_available_duration(&mut self, not_available_duration: impl Into<Vec<u64>>) {
        self.mq_fault_strategy
            .set_not_available_duration(not_available_duration);
    }

    #[inline]
    pub fn set_callback_executor(&mut self, callback_executor: tokio::runtime::Handle) {
        Arc::make_mut(&mut self.producer_config).set_callback_executor(callback_executor);
    }

    #[inline]
    pub fn set_async_sender_executor(&mut self, async_sender_executor: tokio::runtime::Handle) {
        Arc::make_mut(&mut self.producer_config).set_async_sender_executor(async_sender_executor);
    }

    #[inline]
    pub fn set_transaction_executor_service(&mut self, executor_service: Option<tokio::runtime::Handle>) {
        self.transaction_executor_service = executor_service;
    }

    /// Ensure transactional messages do not support delayed delivery
    fn ensure_not_delayed_for_transactional<M>(&self, msg: &M) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait,
    {
        if msg
            .property(&CheetahString::from_static_str(MessageConst::PROPERTY_DELAY_TIME_LEVEL))
            .is_some()
            || msg
                .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELAY_MS))
                .is_some()
            || msg
                .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELAY_SEC))
                .is_some()
            || msg
                .property(&CheetahString::from_static_str(MessageConst::PROPERTY_TIMER_DELIVER_MS))
                .is_some()
        {
            return Err(mq_client_err!("Transactional messages do not support delayed delivery"));
        }
        Ok(())
    }

    pub fn init_transaction_env(
        &mut self,
        check_thread_pool_min_size: u32,
        check_thread_pool_max_size: u32,
        check_request_hold_max: u32,
    ) -> rocketmq_error::RocketMQResult<()> {
        if check_thread_pool_min_size == 0 || check_thread_pool_max_size == 0 {
            return Err(mq_client_err!(
                "transaction check thread pool min and max size must be greater than 0"
            ));
        }
        if check_thread_pool_min_size > check_thread_pool_max_size {
            return Err(mq_client_err!(
                "transaction check thread pool min size cannot exceed max size"
            ));
        }
        if check_request_hold_max == 0 {
            return Err(mq_client_err!(
                "transaction check request hold max must be greater than 0"
            ));
        }

        self.transaction_check_env = Some(TransactionCheckEnv {
            request_slots: Arc::new(Semaphore::new(check_request_hold_max as usize)),
            worker_slots: Arc::new(Semaphore::new(check_thread_pool_max_size as usize)),
        });
        Ok(())
    }

    pub async fn destroy_transaction_env(&mut self) {
        self.transaction_check_env = None;
    }

    #[cfg(test)]
    fn is_transaction_env_initialized(&self) -> bool {
        self.transaction_check_env.is_some()
    }

    pub async fn recall_message(
        &mut self,
        topic: impl Into<CheetahString>,
        recall_handle: impl Into<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<String> {
        let topic = topic.into();
        let recall_handle = recall_handle.into();

        self.make_sure_state_ok()?;
        Validators::check_topic(&topic)?;

        if recall_handle.is_empty() {
            return Err(mq_client_err!("Recall handle cannot be empty"));
        }

        if NamespaceUtil::is_retry_topic(&topic) || NamespaceUtil::is_dlq_topic(&topic) {
            return Err(mq_client_err!("topic is not supported"));
        }

        let handle_entity = RecallMessageHandle::decode_handle(&recall_handle)
            .map_err(|e| mq_client_err!(format!("Failed to decode recall handle: {}", e)))?;

        self.try_to_find_topic_publish_info(&topic).await;

        let broker_name_cs = CheetahString::from_string(handle_entity.broker_name().to_string());
        let client_instance = self
            .client_instance
            .as_ref()
            .ok_or_else(|| rocketmq_error::RocketMQError::not_initialized("MQClientInstance"))?;
        let mut broker_addr = client_instance.find_broker_address_in_publish(&broker_name_cs);

        if broker_addr.is_none() {
            broker_addr = client_instance.find_broker_addr_by_topic(&topic).await;
        }

        let broker_addr = broker_addr.ok_or_else(|| {
            warn!(
                "Can't find broker service address for broker: {}",
                handle_entity.broker_name()
            );
            mq_client_err!("The broker service address not found")
        })?;

        let mut request_header = RecallMessageRequestHeader::new(
            topic,
            recall_handle,
            Some(self.producer_config.producer_group().clone()),
        );

        request_header.topic_request_header = Some(TopicRequestHeader {
            rpc_request_header: Some(RpcRequestHeader {
                broker_name: Some(broker_name_cs.clone()),
                namespace: None,
                namespaced: None,
                oneway: None,
            }),
            lo: None,
        });

        client_instance
            .get_mq_client_api_impl()?
            .recall_message(
                &broker_addr,
                request_header,
                self.producer_config.send_msg_timeout() as u64,
            )
            .await
    }
}

pub(crate) struct DefaultServiceDetector {
    client_instance: ArcMut<MQClientInstance>,
    topic_publish_info_table: Arc<DashMap<CheetahString /* topic */, TopicPublishInfo>>,
}

impl ServiceDetector for DefaultServiceDetector {
    async fn detect(&self, endpoint: &str, timeout_millis: u64) -> bool {
        let topic = match self
            .topic_publish_info_table
            .iter()
            .next()
            .map(|entry| entry.key().clone())
        {
            Some(t) => t,
            None => return false,
        };

        let mq = MessageQueue::from_parts(topic.as_str(), endpoint, 0);
        let mut client_instance = self.client_instance.clone();

        let result = tokio::time::timeout(Duration::from_millis(timeout_millis), async move {
            match client_instance.mq_client_api_impl.as_mut() {
                Some(api) => api.get_max_offset(endpoint, &mq, timeout_millis).await.is_ok(),
                None => false,
            }
        })
        .await;

        matches!(result, Ok(true))
    }
}

/// Helper function to build oneway request (simplified version for performance).
///
/// This is used internally by batch oneway to avoid code duplication.
fn build_oneway_request_internal<T>(
    msg: &mut T,
    mq: &MessageQueue,
    broker_name: &CheetahString,
    producer_config: &ProducerConfig,
    namespace: Option<&str>,
) -> rocketmq_error::RocketMQResult<RemotingCommand>
where
    T: MessageTrait,
{
    use rocketmq_remoting::code::request_code::RequestCode;
    use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
    use rocketmq_remoting::protocol::remoting_command::RemotingCommand;

    // Set message ID
    MessageClientIDSetter::set_uniq_id(msg);

    // Build request header (simplified for oneway)
    let request_header = SendMessageRequestHeader {
        producer_group: CheetahString::from_string(producer_config.producer_group().to_string()),
        topic: CheetahString::from_string(msg.topic().to_string()),
        default_topic: CheetahString::from_string(producer_config.create_topic_key().to_string()),
        default_topic_queue_nums: producer_config.default_topic_queue_nums() as i32,
        queue_id: mq.queue_id(),
        sys_flag: 0,
        born_timestamp: current_millis() as i64,
        flag: msg.get_flag(),
        properties: Some(MessageDecoder::message_properties_to_string(msg.get_properties())),
        reconsume_times: Some(0),
        unit_mode: Some(false),
        batch: Some(false),
        topic_request_header: Some(TopicRequestHeader {
            rpc_request_header: Some(RpcRequestHeader {
                broker_name: Some(broker_name.clone()),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };

    // Build command
    let mut request = RemotingCommand::create_request_command(RequestCode::SendMessage, request_header);

    // Set body (zero-copy: Bytes is reference-counted)
    if let Some(body) = msg.get_body() {
        request.set_body_mut_ref(body.clone());
    } else {
        return Err(mq_client_err!(-1, "Message body is None"));
    }

    Ok(request)
}

pub(crate) struct DefaultResolver {
    client_instance: ArcMut<MQClientInstance>,
}

impl Resolver for DefaultResolver {
    async fn resolve(&self, name: &CheetahString) -> Option<CheetahString> {
        self.client_instance.find_broker_address_in_publish(name)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use crate::producer::default_mq_producer::DefaultMQProducer;
    use crate::producer::transaction_listener::TransactionListener;

    fn running_producer_without_client() -> DefaultMQProducerImpl {
        let mut producer = DefaultMQProducerImpl::new(ClientConfig::default(), ProducerConfig::default(), None);
        producer.state.store(ProducerState::Running as u8, Ordering::SeqCst);
        producer.service_state = ServiceState::Running;
        producer
    }

    fn running_producer_arc_with_self_inner() -> ArcMut<DefaultMQProducerImpl> {
        let producer = ArcMut::new(running_producer_without_client());
        producer
            .mut_from_ref()
            .set_default_mqproducer_impl_inner(ArcMut::downgrade(&producer));
        producer
    }

    #[test]
    fn spawn_producer_task_without_tokio_runtime_runs_on_fallback_thread() {
        let (tx, rx) = std::sync::mpsc::channel();
        let tracker = TaskTracker::new();
        let shutdown_token = CancellationToken::new();

        spawn_producer_task(
            None,
            "rocketmq-client-producer-test",
            &tracker,
            &shutdown_token,
            async move {
                let current_thread = std::thread::current();
                let thread_name = current_thread.name().unwrap_or_default().to_string();
                tx.send(thread_name).expect("test receiver should still be open");
            },
        )
        .expect("producer task should spawn without an ambient Tokio runtime");

        let thread_name = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("fallback producer task should complete");
        assert_eq!(thread_name, "rocketmq-client-fallback");
    }

    #[tokio::test]
    async fn tracked_producer_task_cancellation_stops_pending_task() {
        struct DropFlag(Arc<AtomicBool>);

        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let tracker = TaskTracker::new();
        let shutdown_token = CancellationToken::new();
        let started = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let started_in_task = started.clone();
        let dropped_in_task = dropped.clone();

        spawn_producer_task(
            None,
            "rocketmq-client-producer-tracked-test",
            &tracker,
            &shutdown_token,
            async move {
                let _drop_flag = DropFlag(dropped_in_task);
                started_in_task.store(true, Ordering::Release);
                std::future::pending::<()>().await;
            },
        )
        .expect("producer task should spawn on current Tokio runtime");

        tokio::time::timeout(Duration::from_secs(1), async {
            while !started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("producer task should start before cancellation");

        tracker.close();
        assert!(tokio::time::timeout(Duration::from_millis(20), tracker.wait())
            .await
            .is_err());

        shutdown_token.cancel();
        tokio::time::timeout(Duration::from_secs(1), tracker.wait())
            .await
            .expect("producer task should stop after cancellation");
        assert!(dropped.load(Ordering::Acquire));
    }

    struct PanicTransactionListener;

    impl TransactionListener for PanicTransactionListener {
        fn execute_local_transaction(
            &self,
            _msg: &dyn MessageTrait,
            _arg: Option<&(dyn Any + Send + Sync)>,
        ) -> LocalTransactionState {
            std::panic::panic_any(String::from("local transaction boom"));
        }

        fn check_local_transaction(&self, _msg: &MessageExt) -> LocalTransactionState {
            LocalTransactionState::Unknown
        }
    }

    struct NoopTransactionListener;

    impl TransactionListener for NoopTransactionListener {
        fn execute_local_transaction(
            &self,
            _msg: &dyn MessageTrait,
            _arg: Option<&(dyn Any + Send + Sync)>,
        ) -> LocalTransactionState {
            LocalTransactionState::Unknown
        }

        fn check_local_transaction(&self, _msg: &MessageExt) -> LocalTransactionState {
            LocalTransactionState::Unknown
        }
    }

    struct CapturingSendHook {
        exception_message: Arc<std::sync::Mutex<Option<String>>>,
    }

    impl SendMessageHook for CapturingSendHook {
        fn hook_name(&self) -> &'static str {
            "CapturingSendHook"
        }

        fn send_message_before(&self, _context: &Option<SendMessageContext<'_>>) {}

        fn send_message_after(&self, context: &Option<SendMessageContext<'_>>) {
            let Some(exception) = context.as_ref().and_then(|context| context.exception.as_ref()) else {
                return;
            };
            *self
                .exception_message
                .lock()
                .expect("exception message lock should not be poisoned") = Some(exception.to_string());
        }
    }

    #[test]
    fn send_message_after_hook_observes_exception_like_java() {
        let mut producer = running_producer_without_client();
        let exception_message = Arc::new(std::sync::Mutex::new(None));
        let hook: Arc<dyn SendMessageHook> = Arc::new(CapturingSendHook {
            exception_message: exception_message.clone(),
        });
        producer.send_message_hook_list = vec![hook].into();
        let context = Some(SendMessageContext {
            exception: Some(DefaultMQProducerImpl::boxed_context_error(
                "sendKernelImpl exception".to_string(),
            )),
            ..Default::default()
        });

        producer.execute_send_message_hook_after(&context);

        assert_eq!(
            exception_message
                .lock()
                .expect("exception message lock should not be poisoned")
                .as_deref(),
            Some("sendKernelImpl exception")
        );
    }

    #[tokio::test]
    async fn start_failure_enters_start_failed_and_shutdown_noops_like_java() {
        let mut producer = DefaultMQProducerImpl::new(ClientConfig::default(), ProducerConfig::default(), None);

        let start_result = producer.start().await;
        assert!(start_result.is_err());
        assert_eq!(
            ProducerState::from_u8(producer.state.load(Ordering::SeqCst)),
            ProducerState::StartFailed
        );
        assert_eq!(producer.service_state, ServiceState::StartFailed);

        tokio::time::timeout(Duration::from_millis(100), producer.shutdown())
            .await
            .expect("shutdown after start failure should not wait forever like Starting")
            .expect("shutdown after Java START_FAILED should be a no-op");
        assert_eq!(
            ProducerState::from_u8(producer.state.load(Ordering::SeqCst)),
            ProducerState::StartFailed
        );
        assert_eq!(producer.service_state, ServiceState::StartFailed);
    }

    #[tokio::test]
    async fn producer_selector_paths_without_client_return_error_instead_of_panicking() {
        let mut producer = running_producer_without_client();
        let mut msg = Message::builder().topic("TopicTest").empty_body().build_unchecked();

        let selector_result = producer
            .invoke_message_queue_selector(&mut msg, |_queues, _msg, _arg| None, &(), 3000)
            .await;
        assert!(selector_result.is_err());

        let fetch_result = producer.fetch_publish_message_queues(&"TopicTest".into()).await;
        assert!(fetch_result.is_err());
        assert!(fetch_result
            .err()
            .is_some_and(|error| error.to_string().contains("MQClientInstance is not available")));
    }

    #[tokio::test]
    async fn request_fail_removes_future_and_executes_callback_once_like_java() {
        let correlation_id = format!("request-fail-{}", current_millis());
        REQUEST_FUTURE_HOLDER.remove_request(correlation_id.as_str()).await;

        let calls = Arc::new(AtomicUsize::new(0));
        let calls_inner = Arc::clone(&calls);
        let callback: RequestCallbackFn = Arc::new(move |response, error| {
            assert!(response.is_none());
            assert!(error.is_none());
            calls_inner.fetch_add(1, Ordering::SeqCst);
        });
        let future = Arc::new(RequestResponseFuture::new(
            correlation_id.as_str().into(),
            3_000,
            Some(callback),
        ));
        REQUEST_FUTURE_HOLDER.put_request(correlation_id.clone(), future).await;

        DefaultMQProducerImpl::request_fail(correlation_id.as_str()).await;
        DefaultMQProducerImpl::request_fail(correlation_id.as_str()).await;

        assert!(REQUEST_FUTURE_HOLDER
            .get_request(correlation_id.as_str())
            .await
            .is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn selector_helper_uses_user_topic_and_restores_namespace_like_java() {
        let mut client_config = ClientConfig::default();
        client_config.set_namespace(CheetahString::from_static_str("ns-a"));
        let queues = vec![MessageQueue::from_parts("TopicTest", "broker-a", 0)];
        let mut msg = Message::builder()
            .topic("ns-a%TopicTest")
            .empty_body()
            .build_unchecked();
        let seen_topic = std::sync::Mutex::new(None);

        let selected = DefaultMQProducerImpl::select_message_queue_with_user_message(
            &mut client_config,
            &queues,
            &mut msg,
            &|queues, msg, _arg| {
                *seen_topic.lock().expect("seen topic lock should not be poisoned") = Some(msg.topic().clone());
                queues.first().cloned()
            },
            &(),
        )
        .expect("selector should return a queue");

        assert_eq!(
            seen_topic
                .into_inner()
                .expect("seen topic lock should not be poisoned")
                .as_deref(),
            Some("TopicTest")
        );
        assert_eq!(msg.topic(), "ns-a%TopicTest");
        assert_eq!(selected.topic(), "ns-a%TopicTest");
    }

    #[test]
    fn default_send_retry_attempts_match_java_communication_mode() {
        let producer = running_producer_without_client();

        assert_eq!(
            producer.get_retry_times(CommunicationMode::Sync),
            producer.producer_config.retry_times_when_send_failed() + 1
        );
        assert_eq!(producer.get_retry_times(CommunicationMode::Async), 1);
        assert_eq!(producer.get_retry_times(CommunicationMode::Oneway), 1);
    }

    #[test]
    fn send_timeout_for_attempt_caps_only_retryable_attempts_like_java() {
        let mut producer = running_producer_without_client();
        let configured = DefaultMQProducer::builder()
            .producer_group("retry-timeout-group")
            .send_msg_max_timeout_per_request(500)
            .build();
        producer.replace_producer_config(configured.producer_config().clone());

        assert_eq!(producer.send_timeout_for_attempt(3_000, 0, 3), 500);
        assert_eq!(producer.send_timeout_for_attempt(3_000, 1, 3), 500);
        assert_eq!(producer.send_timeout_for_attempt(3_000, 2, 3), 3_000);
        assert_eq!(producer.send_timeout_for_attempt(3_000, 0, 1), 3_000);

        let producer_without_cap = running_producer_without_client();
        assert_eq!(producer_without_cap.send_timeout_for_attempt(3_000, 0, 3), 3_000);
    }

    #[test]
    fn async_remaining_timeout_requires_positive_budget_like_java() {
        assert_eq!(DefaultMQProducerImpl::remaining_async_timeout(3_000, 2_999), Some(1));
        assert_eq!(DefaultMQProducerImpl::remaining_async_timeout(3_000, 3_000), None);
        assert_eq!(DefaultMQProducerImpl::remaining_async_timeout(3_000, 3_001), None);
    }

    #[test]
    fn request_remaining_timeout_returns_typed_error_instead_of_underflow() {
        assert_eq!(
            DefaultMQProducerImpl::remaining_request_timeout(3_000, 2_999).expect("one ms remains"),
            1
        );

        for elapsed in [3_000, 3_001] {
            let error = DefaultMQProducerImpl::remaining_request_timeout(3_000, elapsed)
                .expect_err("exhausted request budget should be a typed timeout");
            assert!(matches!(
                error,
                rocketmq_error::RocketMQError::Timeout {
                    operation: "send request message",
                    timeout_ms: 3_000
                }
            ));
        }
    }

    #[tokio::test]
    async fn send_oneway_with_message_queue_does_not_reject_topic_mismatch_before_kernel_like_java() {
        let mut producer = running_producer_without_client();
        let msg = Message::builder().topic("TopicA").body_slice(b"body").build_unchecked();
        let mq = MessageQueue::from_parts("TopicB", "broker-a", 0);

        let result = producer.send_oneway_with_message_queue(msg, mq).await;

        let error = result.expect_err("kernel path should fail without a client instance");
        assert!(error.to_string().contains("MQClientInstance is not available"));
        assert!(!error.to_string().contains("is not equal with message queue topic"));
    }

    #[tokio::test]
    async fn sync_send_to_queue_topic_mismatch_uses_java_error_message() {
        let mut producer = running_producer_without_client();
        let msg = Message::builder().topic("TopicA").body_slice(b"body").build_unchecked();
        let mq = MessageQueue::from_parts("TopicB", "broker-a", 0);

        let error = producer
            .sync_send_with_message_queue_timeout(msg, mq, 3_000)
            .await
            .expect_err("topic mismatch should fail before broker lookup");

        assert!(error.to_string().contains("message's topic not equal mq's topic"));
    }

    #[tokio::test]
    async fn async_send_to_queue_validates_message_before_kernel_like_java() {
        let producer = running_producer_arc_with_self_inner();
        let notify = Arc::new(tokio::sync::Notify::new());
        let seen_error = Arc::new(std::sync::Mutex::new(None::<String>));
        let notify_for_callback = notify.clone();
        let seen_for_callback = seen_error.clone();
        let callback: ArcSendCallback = Arc::new(
            move |_result: Option<&SendResult>, error: Option<&dyn std::error::Error>| {
                if let Some(error) = error {
                    *seen_for_callback
                        .lock()
                        .expect("seen error lock should not be poisoned") = Some(error.to_string());
                    notify_for_callback.notify_one();
                }
            },
        );
        let msg = Message::builder().topic("TopicTest").empty_body().build_unchecked();
        let mq = MessageQueue::from_parts("TopicTest", "broker-a", 0);

        producer
            .mut_from_ref()
            .async_send_batch_to_queue_with_callback_timeout(msg, mq, Some(callback), 3_000)
            .await
            .expect("async send should schedule validation");
        tokio::time::timeout(Duration::from_secs(1), notify.notified())
            .await
            .expect("validation error should reach callback");

        let error = seen_error
            .lock()
            .expect("seen error lock should not be poisoned")
            .clone()
            .expect("callback should record validation error");
        assert!(error.contains("message body is null") || error.contains("message body length is zero"));
        assert!(!error.contains("MQClientInstance is not available"));
    }

    #[tokio::test]
    async fn async_send_to_queue_topic_mismatch_uses_java_callback_error_message() {
        let producer = running_producer_arc_with_self_inner();
        let notify = Arc::new(tokio::sync::Notify::new());
        let seen_error = Arc::new(std::sync::Mutex::new(None::<String>));
        let notify_for_callback = notify.clone();
        let seen_for_callback = seen_error.clone();
        let callback: ArcSendCallback = Arc::new(
            move |_result: Option<&SendResult>, error: Option<&dyn std::error::Error>| {
                if let Some(error) = error {
                    *seen_for_callback
                        .lock()
                        .expect("seen error lock should not be poisoned") = Some(error.to_string());
                    notify_for_callback.notify_one();
                }
            },
        );
        let msg = Message::builder().topic("TopicA").body_slice(b"body").build_unchecked();
        let mq = MessageQueue::from_parts("TopicB", "broker-a", 0);

        producer
            .mut_from_ref()
            .async_send_batch_to_queue_with_callback_timeout(msg, mq, Some(callback), 3_000)
            .await
            .expect("async send should schedule mismatch validation");
        tokio::time::timeout(Duration::from_secs(1), notify.notified())
            .await
            .expect("topic mismatch should reach callback");

        let error = seen_error
            .lock()
            .expect("seen error lock should not be poisoned")
            .clone()
            .expect("callback should record topic mismatch");
        assert!(
            error.contains("Topic of the message does not match its target message queue"),
            "unexpected callback error: {error}"
        );
        assert!(!error.contains("MQClientInstance is not available"));
    }

    #[tokio::test]
    async fn invoke_selector_uses_user_message_and_returns_namespaced_queue_like_java() {
        let mut client_config = ClientConfig::default();
        client_config.set_namespace(CheetahString::from_static_str("ns-a"));
        let client_instance = MQClientInstance::new_arc(client_config.clone(), 0, "selector-namespace-client", None);
        let mut producer = DefaultMQProducerImpl::new(client_config.clone(), ProducerConfig::default(), None);
        producer.state.store(ProducerState::Running as u8, Ordering::SeqCst);
        producer.service_state = ServiceState::Running;
        producer.client_instance = Some(client_instance);
        producer.topic_publish_info_table.insert(
            CheetahString::from_static_str("ns-a%TopicTest"),
            TopicPublishInfo {
                have_topic_router_info: true,
                message_queue_list: vec![MessageQueue::from_parts("ns-a%TopicTest", "broker-a", 0)],
                ..Default::default()
            },
        );
        let mut msg = Message::builder()
            .topic("ns-a%TopicTest")
            .body_slice(b"selector")
            .build_unchecked();
        let seen = std::sync::Mutex::new((None::<CheetahString>, None::<CheetahString>));

        let selected = producer
            .invoke_message_queue_selector(
                &mut msg,
                |queues, msg, seen| {
                    let mut seen = seen.lock().expect("seen lock should not be poisoned");
                    seen.0 = Some(msg.topic().clone());
                    seen.1 = queues.first().map(|queue| queue.topic().clone());
                    queues.first().cloned()
                },
                &seen,
                3000,
            )
            .await
            .expect("selector should resolve from cached route info");

        let seen = seen.into_inner().expect("seen lock should not be poisoned");
        assert_eq!(seen.0.as_deref(), Some("TopicTest"));
        assert_eq!(seen.1.as_deref(), Some("TopicTest"));
        assert_eq!(msg.topic(), "ns-a%TopicTest");
        assert_eq!(selected.topic(), "ns-a%TopicTest");
    }

    #[tokio::test]
    async fn producer_request_prepare_without_client_returns_error_instead_of_panicking() {
        let mut producer = running_producer_without_client();
        let mut msg = Message::builder().topic("TopicTest").empty_body().build_unchecked();

        let result = producer.prepare_send_request(&mut msg, 3000).await;

        assert!(result.is_err());
        assert!(result
            .err()
            .is_some_and(|error| error.to_string().contains("MQClientInstance is not available")));
    }

    #[test]
    fn backpressure_setters_clamp_and_resize_permits_like_java() {
        let mut producer = running_producer_without_client();

        producer.set_enable_backpressure_for_async_mode(true);
        producer.set_back_pressure_for_async_send_num(1);
        producer.set_back_pressure_for_async_send_size(128);

        assert!(producer.enable_backpressure_for_async_mode());
        assert_eq!(
            producer.back_pressure_for_async_send_num(),
            MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM
        );
        assert_eq!(
            producer.back_pressure_for_async_send_size(),
            MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE
        );
        assert_eq!(
            producer.semaphore_async_send_num.available_permits(),
            MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM as usize
        );
        assert_eq!(
            producer.semaphore_async_send_size.available_permits(),
            MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE as usize
        );
    }

    #[test]
    fn semaphore_async_adjust_updates_backpressure_limits_like_java_callback() {
        let mut producer = running_producer_without_client();
        producer.set_back_pressure_for_async_send_num(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM);
        producer.set_back_pressure_for_async_send_size(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE);

        producer
            .semaphore_async_adjust(2, 16)
            .expect("positive semaphore adjustment should be accepted");

        assert_eq!(
            producer.back_pressure_for_async_send_num(),
            MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM + 2
        );
        assert_eq!(
            producer.back_pressure_for_async_send_size(),
            MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE + 16
        );
        assert_eq!(
            producer.semaphore_async_send_num.available_permits(),
            (MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM + 2) as usize
        );
        assert_eq!(
            producer.semaphore_async_send_size.available_permits(),
            (MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE + 16) as usize
        );

        producer.semaphore_processor();
    }

    #[test]
    fn check_listener_alias_returns_transaction_listener() {
        let mut producer = running_producer_without_client();

        assert!(producer.check_listener().is_none());
        producer.set_transaction_listener(Arc::new(NoopTransactionListener));

        assert!(producer.check_listener().is_some());
    }

    #[tokio::test]
    async fn async_backpressure_permits_are_held_until_task_finishes_like_java() {
        let mut producer = running_producer_without_client();
        producer.set_enable_backpressure_for_async_mode(true);
        producer.set_back_pressure_for_async_send_num(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM);
        producer.set_back_pressure_for_async_send_size(MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE);

        let msg_len = 8;
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();

        producer
            .execute_async_message_send(
                async move {
                    let _ = started_tx.send(());
                    let _ = release_rx.await;
                },
                None,
                1000,
                Instant::now(),
                msg_len,
            )
            .await
            .expect("backpressure execution should spawn task");
        started_rx.await.expect("spawned task should start");

        assert_eq!(
            producer.semaphore_async_send_num.available_permits(),
            (MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM - 1) as usize
        );
        assert_eq!(
            producer.semaphore_async_send_size.available_permits(),
            MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE as usize - msg_len
        );

        release_tx.send(()).expect("spawned task should still be waiting");
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if producer.semaphore_async_send_num.available_permits()
                    == MIN_BACK_PRESSURE_FOR_ASYNC_SEND_NUM as usize
                    && producer.semaphore_async_send_size.available_permits()
                        == MIN_BACK_PRESSURE_FOR_ASYNC_SEND_SIZE as usize
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("permits should be released after async task finishes");
    }

    #[tokio::test]
    async fn transaction_env_initializes_and_destroys_java_lifecycle_state() {
        let mut producer = running_producer_without_client();

        producer.init_transaction_env(1, 2, 3).unwrap();
        assert!(producer.is_transaction_env_initialized());

        producer.destroy_transaction_env().await;
        assert!(!producer.is_transaction_env_initialized());
    }

    #[test]
    fn transaction_env_rejects_invalid_java_executor_config() {
        let mut producer = running_producer_without_client();

        let min_over_max = producer.init_transaction_env(2, 1, 3);
        assert!(min_over_max
            .err()
            .is_some_and(|error| error.to_string().contains("min size cannot exceed max size")));

        let zero_pool = producer.init_transaction_env(0, 1, 3);
        assert!(zero_pool
            .err()
            .is_some_and(|error| error.to_string().contains("must be greater than 0")));

        let zero_hold = producer.init_transaction_env(1, 1, 0);
        assert!(zero_hold
            .err()
            .is_some_and(|error| error.to_string().contains("hold max must be greater than 0")));
    }

    #[tokio::test]
    async fn transaction_send_without_impl_listener_fails_before_send_like_java() {
        let mut producer = running_producer_without_client();
        let msg = Message::builder().topic("TopicTest").empty_body().build_unchecked();

        let result = producer.send_message_in_transaction(msg, None).await;

        assert!(result
            .err()
            .is_some_and(|error| error.to_string().contains("tranExecutor is null")));
    }

    #[tokio::test]
    async fn transaction_send_delay_millis_fails_before_send_like_java() {
        let mut producer = running_producer_without_client();
        producer.set_transaction_listener(Arc::new(NoopTransactionListener));
        let msg = Message::builder()
            .topic("TopicTest")
            .body_slice(b"transaction")
            .delay_millis(3000)
            .build_unchecked();

        let result = producer.send_message_in_transaction(msg, None).await;

        assert!(result.err().is_some_and(|error| {
            error
                .to_string()
                .contains("Transactional messages do not support delayed delivery")
        }));
    }

    #[test]
    fn local_transaction_listener_panic_becomes_unknown_like_java() {
        let listener: ArcTransactionListener = Arc::new(PanicTransactionListener);
        let msg = Message::builder()
            .topic("TopicTest")
            .body_slice(b"transaction")
            .build_unchecked();

        let (state, remark) = DefaultMQProducerImpl::execute_local_transaction_branch(&listener, &msg, None);

        assert_eq!(state, LocalTransactionState::Unknown);
        assert!(remark
            .as_ref()
            .is_some_and(|remark| remark.as_str().contains("local transaction boom")));
    }

    #[test]
    fn transaction_check_end_header_preserves_java_offsets() {
        let check_header = CheckTransactionStateRequestHeader {
            topic: Some(CheetahString::from_static_str("TopicA")),
            tran_state_table_offset: 123,
            commit_log_offset: 456,
            msg_id: Some(CheetahString::from_static_str("msg-id")),
            transaction_id: Some(CheetahString::from_static_str("tx-id")),
            offset_msg_id: Some(CheetahString::from_static_str("offset-msg-id")),
            rpc_request_header: Some(RpcRequestHeader {
                broker_name: Some(CheetahString::from_static_str("broker-a")),
                ..Default::default()
            }),
        };

        let end_header = DefaultMQProducerImpl::build_end_transaction_header_for_check(
            CheetahString::from_static_str("ProducerA"),
            &check_header,
            CheetahString::from_static_str("unique-msg-id"),
            LocalTransactionState::CommitMessage,
        );

        assert_eq!(end_header.topic, "TopicA");
        assert_eq!(end_header.producer_group, "ProducerA");
        assert_eq!(end_header.tran_state_table_offset, 123);
        assert_eq!(end_header.commit_log_offset, 456);
        assert_eq!(end_header.commit_or_rollback, MessageSysFlag::TRANSACTION_COMMIT_TYPE);
        assert!(end_header.from_transaction_check);
        assert_eq!(end_header.msg_id, "unique-msg-id");
        assert_eq!(end_header.transaction_id.as_deref(), Some("tx-id"));
        assert_eq!(end_header.rpc_request_header.broker_name.as_deref(), Some("broker-a"));
    }

    #[test]
    fn transaction_check_end_header_preserves_negative_java_offsets_without_wrapping() {
        let check_header = CheckTransactionStateRequestHeader {
            topic: Some(CheetahString::from_static_str("TopicA")),
            tran_state_table_offset: -123,
            commit_log_offset: -456,
            ..Default::default()
        };

        let end_header = DefaultMQProducerImpl::build_end_transaction_header_for_check(
            CheetahString::from_static_str("ProducerA"),
            &check_header,
            CheetahString::from_static_str("msg-id"),
            LocalTransactionState::Unknown,
        );

        assert_eq!(end_header.tran_state_table_offset, -123);
        assert_eq!(end_header.commit_log_offset, -456);
    }

    #[test]
    fn end_transaction_send_result_queue_offset_must_fit_java_long() {
        assert_eq!(
            DefaultMQProducerImpl::u64_to_java_long_field("endTransaction", "tranStateTableOffset", i64::MAX as u64)
                .expect("i64 max should fit Java long"),
            i64::MAX
        );

        let error = DefaultMQProducerImpl::u64_to_java_long_field(
            "endTransaction",
            "tranStateTableOffset",
            i64::MAX as u64 + 1,
        )
        .expect_err("queue offsets larger than Java long must not wrap");

        assert!(error
            .to_string()
            .contains("endTransaction tranStateTableOffset exceeds Java long range"));
    }

    #[test]
    fn transaction_check_end_header_maps_local_transaction_state() {
        let check_header = CheckTransactionStateRequestHeader {
            tran_state_table_offset: 1,
            commit_log_offset: 2,
            ..Default::default()
        };

        let cases = [
            (
                LocalTransactionState::CommitMessage,
                MessageSysFlag::TRANSACTION_COMMIT_TYPE,
            ),
            (
                LocalTransactionState::RollbackMessage,
                MessageSysFlag::TRANSACTION_ROLLBACK_TYPE,
            ),
            (LocalTransactionState::Unknown, MessageSysFlag::TRANSACTION_NOT_TYPE),
        ];

        for (state, expected_flag) in cases {
            let end_header = DefaultMQProducerImpl::build_end_transaction_header_for_check(
                CheetahString::from_static_str("ProducerA"),
                &check_header,
                CheetahString::from_static_str("msg-id"),
                state,
            );

            assert_eq!(end_header.commit_or_rollback, expected_flag);
        }
    }
}
