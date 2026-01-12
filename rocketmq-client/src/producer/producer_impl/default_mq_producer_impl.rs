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
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::ClientErr;
use rocketmq_error::RocketmqError::RemotingTooMuchRequestError;
use rocketmq_remoting::protocol::header::check_transaction_state_request_header::CheckTransactionStateRequestHeader;
use rocketmq_remoting::protocol::header::end_transaction_request_header::EndTransactionRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::rpc::rpc_request_header::RpcRequestHeader;
use rocketmq_remoting::rpc::topic_request_header::TopicRequestHeader;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_runtime::RocketMQRuntime;
use rocketmq_rust::ArcMut;
use tokio::sync::Semaphore;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::base::validators::Validators;
use crate::common::client_error_code::ClientErrorCode;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::hook::check_forbidden_context::CheckForbiddenContext;
use crate::hook::check_forbidden_hook::CheckForbiddenHook;
use crate::hook::end_transaction_context::EndTransactionContext;
use crate::hook::end_transaction_hook::EndTransactionHook;
use crate::hook::send_message_context::SendMessageContext;
use crate::hook::send_message_hook::SendMessageHook;
use crate::implementation::communication_mode::CommunicationMode;
use crate::implementation::mq_client_manager::MQClientManager;
use crate::latency::mq_fault_strategy::MQFaultStrategy;
use crate::latency::resolver::Resolver;
use crate::latency::service_detector::ServiceDetector;
use crate::producer::default_mq_producer::ProducerConfig;
use crate::producer::local_transaction_state::LocalTransactionState;
use crate::producer::message_queue_selector::MessageQueueSelectorFn;
use crate::producer::producer_impl::mq_producer_inner::MQProducerInner;
use crate::producer::producer_impl::mq_producer_inner::MQProducerInnerImpl;
use crate::producer::producer_impl::topic_publish_info::TopicPublishInfo;
use crate::producer::request_callback::RequestCallbackFn;
use crate::producer::request_future_holder::REQUEST_FUTURE_HOLDER;
use crate::producer::request_response_future::RequestResponseFuture;
use crate::producer::send_callback::SendMessageCallback;
use crate::producer::send_result::SendResult;
use crate::producer::send_status::SendStatus;
use crate::producer::transaction_listener::TransactionListener;
use crate::producer::transaction_send_result::TransactionSendResult;
use tokio::task::JoinHandle;

/// Producer state machine (atomic)
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProducerState {
    Created = 0,
    Starting = 1,
    Running = 2,
    Stopping = 3,
    Stopped = 4,
}

impl ProducerState {
    #[inline]
    fn from_u8(val: u8) -> Self {
        match val {
            0 => Self::Created,
            1 => Self::Starting,
            2 => Self::Running,
            3 => Self::Stopping,
            _ => Self::Stopped,
        }
    }
}

/// Producer runtime state (only exists when Running)
struct ProducerRuntime {
    /// Background task handles
    background_tasks: Vec<JoinHandle<()>>,
    /// Shutdown signal sender
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
}

impl ProducerRuntime {
    async fn shutdown(mut self) {
        // Send shutdown signal
        let _ = self.shutdown_tx.send(());

        // Wait for all background tasks to complete
        for handle in self.background_tasks.drain(..) {
            let _ = handle.await;
        }
    }
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

    // ===== Runtime (created/destroyed on demand) =====
    runtime: tokio::sync::RwLock<Option<ProducerRuntime>>,

    // ===== Read-only hot data (immutable after init, zero-cost sharing) =====
    send_message_hook_list: Arc<[Arc<dyn SendMessageHook>]>,
    end_transaction_hook_list: Arc<[Arc<dyn EndTransactionHook>]>,
    check_forbidden_hook_list: Arc<[Arc<dyn CheckForbiddenHook>]>,

    // Temporary hook storage during initialization
    pending_send_hooks: parking_lot::Mutex<Option<Vec<Arc<dyn SendMessageHook>>>>,
    pending_end_transaction_hooks: parking_lot::Mutex<Option<Vec<Arc<dyn EndTransactionHook>>>>,
    pending_forbidden_hooks: parking_lot::Mutex<Option<Vec<Arc<dyn CheckForbiddenHook>>>>,

    topic_publish_info_table: Arc<DashMap<CheetahString /* topic */, TopicPublishInfo>>,

    rpc_hook: Option<Arc<dyn RPCHook>>,
    client_instance: Option<ArcMut<MQClientInstance>>,
    mq_fault_strategy: ArcMut<MQFaultStrategy>,

    // ===== Backpressure control =====
    semaphore_async_send_num: Arc<Semaphore>,
    semaphore_async_send_size: Arc<Semaphore>,
    default_mqproducer_impl_inner: Option<ArcMut<DefaultMQProducerImpl>>,
    transaction_listener: Option<Arc<Box<dyn TransactionListener>>>,
    check_runtime: Option<Arc<RocketMQRuntime>>,
}

#[allow(unused_must_use)]
#[allow(unused_assignments)]
impl DefaultMQProducerImpl {
    pub fn new(
        client_config: ClientConfig,
        producer_config: ProducerConfig,
        rpc_hook: Option<Arc<dyn RPCHook>>,
    ) -> Self {
        let semaphore_async_send_num =
            Semaphore::new(producer_config.back_pressure_for_async_send_num().max(10) as usize);
        let semaphore_async_send_size =
            Semaphore::new(producer_config.back_pressure_for_async_send_size().max(1024 * 1024) as usize);
        let topic_publish_info_table = Arc::new(DashMap::new());
        DefaultMQProducerImpl {
            client_config: client_config.clone(),
            producer_config: Arc::new(producer_config),
            state: AtomicU8::new(ProducerState::Created as u8),
            service_state: ServiceState::CreateJust,
            runtime: tokio::sync::RwLock::new(None),
            topic_publish_info_table,
            send_message_hook_list: Arc::new([]),
            end_transaction_hook_list: Arc::new([]),
            check_forbidden_hook_list: Arc::new([]),
            pending_send_hooks: ParkingLotMutex::new(Some(Vec::new())),
            pending_end_transaction_hooks: ParkingLotMutex::new(Some(Vec::new())),
            pending_forbidden_hooks: ParkingLotMutex::new(Some(Vec::new())),
            rpc_hook: None,
            client_instance: None,
            mq_fault_strategy: ArcMut::new(MQFaultStrategy::new(&client_config)),
            semaphore_async_send_num: Arc::new(semaphore_async_send_num),
            semaphore_async_send_size: Arc::new(semaphore_async_send_size),
            default_mqproducer_impl_inner: None,
            transaction_listener: None,
            check_runtime: None,
        }
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
        send_callback: Option<SendMessageCallback>,
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
        self.send_default_impl(
            &mut msg,
            CommunicationMode::Oneway,
            None,
            self.producer_config.send_msg_timeout() as u64,
        )
        .await?;
        Ok(())
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

        if msg.get_topic() != mq.get_topic() {
            return Err(mq_client_err!(format!(
                "message topic [{}] is not equal with message queue topic [{}]",
                msg.get_topic(),
                mq.get_topic()
            )));
        }
        let cost_time = begin_start_time.elapsed().as_millis() as u64;
        if timeout < cost_time {
            return Err(rocketmq_error::RocketMQError::Timeout {
                operation: "send_with_timeout",
                timeout_ms: timeout,
            });
        }
        self.send_kernel_impl(&mut msg, &mq, CommunicationMode::Sync, None, None, timeout - cost_time)
            .await
    }

    #[inline]
    pub async fn async_send_with_message_queue_callback<T>(
        &mut self,
        msg: T,
        mq: MessageQueue,
        send_callback: Option<SendMessageCallback>,
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

    pub async fn send_with_selector_callback_timeout<M, T>(
        &mut self,
        msg: M,
        selector: MessageQueueSelectorFn,
        arg: T,
        send_callback: Option<SendMessageCallback>,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        T: std::any::Any + Sync + Send,
    {
        let begin_start_time = Instant::now();
        let mut producer_impl = self.default_mqproducer_impl_inner.clone().unwrap();
        let msg_len = if msg.get_body().is_some() {
            msg.get_body().unwrap().len()
        } else {
            1
        };
        let send_callback_clone = send_callback.clone();
        let future = async move {
            let cost_time = begin_start_time.elapsed().as_millis() as u64;
            if timeout <= cost_time {
                send_callback_clone.as_ref().unwrap()(
                    None,
                    Some(&RemotingTooMuchRequestError("call timeout".to_string())),
                );
            }

            producer_impl
                .send_select_impl(
                    msg,
                    selector,
                    arg,
                    CommunicationMode::Async,
                    send_callback_clone,
                    timeout - cost_time,
                )
                .await
        };
        self.execute_async_message_send(future, send_callback, timeout, begin_start_time, msg_len)
            .await
    }

    pub async fn send_oneway_with_selector<M, T>(
        &mut self,
        msg: M,
        selector: MessageQueueSelectorFn,
        arg: T,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait + Send + Sync,
        T: std::any::Any + Sync + Send,
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

    pub async fn send_select_impl<M, T>(
        &mut self,
        mut msg: M,
        selector: MessageQueueSelectorFn,
        arg: T,
        communication_mode: CommunicationMode,
        send_message_callback: Option<SendMessageCallback>,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
        T: std::any::Any + Sync + Send,
    {
        let begin_start_time = Instant::now();
        self.make_sure_state_ok()?;
        Validators::check_message(Some(&msg), self.producer_config.as_ref())?;
        let topic_publish_info = self.try_to_find_topic_publish_info(msg.get_topic()).await;
        if let Some(topic_publish_info) = topic_publish_info {
            if topic_publish_info.ok() {
                let message_queue_list = self
                    .client_instance
                    .as_mut()
                    .unwrap()
                    .mq_admin_impl
                    .parse_publish_message_queues(&topic_publish_info.message_queue_list, &mut self.client_config);
                let mut user_message = MessageAccessor::clone_message(&msg);
                let user_topic = NamespaceUtil::without_namespace_with_namespace(
                    user_message.get_topic(),
                    self.client_config.get_namespace().unwrap_or_default().as_str(),
                );
                user_message.set_topic(CheetahString::from_string(user_topic));
                let message_queue = selector(&message_queue_list, &msg, &arg);
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
        Err(mq_client_err!(format!(
            "No route info for this topic, {}",
            msg.get_topic()
        )))
    }

    #[inline]
    pub async fn async_send_batch_to_queue_with_callback_timeout<T>(
        &mut self,
        mut msg: T,
        mq: MessageQueue,
        send_callback: Option<SendMessageCallback>,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        T: MessageTrait + Send + Sync,
    {
        let mut producer_impl = self.default_mqproducer_impl_inner.clone().unwrap();
        let begin_start_time = Instant::now();
        let send_callback_inner = send_callback.clone();
        let msg_len = if msg.get_body().is_some() {
            msg.get_body().unwrap().len()
        } else {
            1
        };
        let future = async move {
            if let Err(err) = producer_impl.make_sure_state_ok() {
                send_callback_inner.as_ref().unwrap()(None, Some(&err));
                return;
            }
            if msg.get_topic() != mq.get_topic() {
                send_callback_inner.as_ref().unwrap()(
                    None,
                    Some(&rocketmq_error::RocketmqError::MQClientErr(ClientErr::new(format!(
                        "message topic [{}] is not equal with message queue topic [{}]",
                        msg.get_topic(),
                        mq.get_topic()
                    )))),
                );
                return;
            }

            let cost_time = (Instant::now() - begin_start_time).as_millis() as u64;
            if timeout <= cost_time {
                send_callback_inner.as_ref().unwrap()(
                    None,
                    Some(&RemotingTooMuchRequestError("call timeout".to_string())),
                );
            }
            let result = producer_impl
                .send_kernel_impl(
                    &mut msg,
                    &mq,
                    CommunicationMode::Async,
                    send_callback_inner.clone(),
                    None,
                    timeout,
                )
                .await;
            match result {
                Ok(_) => {}
                Err(err) => {
                    send_callback_inner.as_ref().unwrap()(None, Some(&err));
                }
            }
        };

        self.execute_async_message_send(future, send_callback, timeout, begin_start_time, msg_len)
            .await
    }

    pub async fn async_send_with_callback_timeout<T>(
        &mut self,
        mut msg: T,
        send_callback: Option<SendMessageCallback>,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        T: MessageTrait + Send + Sync,
    {
        let producer_impl = self.default_mqproducer_impl_inner.clone().unwrap();
        let begin_start_time = Instant::now();
        let send_callback_inner = send_callback.clone();
        let msg_len = if msg.get_body().is_some() {
            msg.get_body().unwrap().len()
        } else {
            1
        };
        let future = async move {
            let cost_time = (Instant::now() - begin_start_time).as_millis() as u64;
            if timeout <= cost_time {
                send_callback_inner.as_ref().unwrap()(
                    None,
                    Some(&RemotingTooMuchRequestError("asyncSend call timeout".to_string())),
                );
            }

            let result = producer_impl
                .clone()
                .send_default_impl(&mut msg, CommunicationMode::Async, send_callback_inner.clone(), timeout)
                .await;
            match result {
                Ok(_) => {}
                Err(err) => {
                    send_callback_inner.as_ref().unwrap()(None, Some(&err));
                }
            }
        };

        self.execute_async_message_send(future, send_callback, timeout, begin_start_time, msg_len)
            .await
    }

    async fn execute_async_message_send<F>(
        &mut self,
        f: F,
        send_callback: Option<SendMessageCallback>,
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
            let is_semaphore_async_numb_acquired = (timeout - cost_time) > 0;
            if !is_semaphore_async_numb_acquired {
                send_callback.as_ref().unwrap()(
                    None,
                    Some(&RemotingTooMuchRequestError(
                        "send message tryAcquire semaphoreAsyncNum timeout".to_string(),
                    )),
                );
                return Ok(());
            }
            let result = tokio::time::timeout(
                Duration::from_millis(timeout - cost_time),
                self.semaphore_async_send_num.acquire(),
            )
            .await;
            let acquire_value_num = match result {
                Ok(acquire_value) => match acquire_value {
                    Ok(value) => Some(value),
                    Err(_) => {
                        send_callback.as_ref().unwrap()(
                            None,
                            Some(&RemotingTooMuchRequestError(
                                "send message tryAcquire semaphoreAsyncNum timeout".to_string(),
                            )),
                        );
                        return Ok(());
                    }
                },
                Err(_) => {
                    send_callback.as_ref().unwrap()(
                        None,
                        Some(&RemotingTooMuchRequestError(
                            "send message tryAcquire semaphoreAsyncNum timeout".to_string(),
                        )),
                    );
                    return Ok(());
                }
            };

            //message size
            let cost_time = (Instant::now() - begin_start_time).as_millis() as u64;
            let is_semaphore_async_size_acquired = (timeout - cost_time) > 0;
            if !is_semaphore_async_size_acquired {
                send_callback.as_ref().unwrap()(
                    None,
                    Some(&RemotingTooMuchRequestError(
                        "send message tryAcquire semaphoreAsyncSize timeout".to_string(),
                    )),
                );
                return Ok(());
            }
            let result = tokio::time::timeout(
                Duration::from_millis(timeout - cost_time),
                self.semaphore_async_send_size.acquire_many(msg_len as u32),
            )
            .await;
            let acquire_value_size = match result {
                Ok(acquire_value) => match acquire_value {
                    Ok(value) => Some(value),
                    Err(_) => {
                        send_callback.as_ref().unwrap()(
                            None,
                            Some(&RemotingTooMuchRequestError(
                                "send message tryAcquire semaphoreAsyncSize timeout".to_string(),
                            )),
                        );
                        return Ok(());
                    }
                },
                Err(_) => {
                    send_callback.as_ref().unwrap()(
                        None,
                        Some(&RemotingTooMuchRequestError(
                            "send message tryAcquire semaphoreAsyncSize timeout".to_string(),
                        )),
                    );
                    return Ok(());
                }
            };
            (acquire_value_num, acquire_value_size)
        } else {
            (None, None)
        };
        tokio::spawn(f);
        drop((acquire_value_num, acquire_value_size));
        Ok(())
    }

    async fn send_default_impl<T>(
        &mut self,
        msg: &mut T,
        communication_mode: CommunicationMode,
        send_callback: Option<SendMessageCallback>,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        T: MessageTrait + Send + Sync,
    {
        self.make_sure_state_ok()?;

        let topic = msg.get_topic().clone();
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
        send_callback: Option<SendMessageCallback>,
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

            retry_state.record_broker(attempt as usize, mq.get_broker_name());
            last_broker_name = Some(mq.get_broker_name().clone());

            // Prepare message for retry
            if attempt > 0 {
                self.prepare_message_for_retry(msg, topic);
            }

            // Check timeout
            ctx.check_timeout()?;

            // Send to broker
            let send_start = Instant::now();
            let result = self
                .send_kernel_impl(
                    msg,
                    &mq,
                    ctx.communication_mode,
                    send_callback.clone(),
                    Some(topic_publish_info),
                    ctx.remaining_timeout(),
                )
                .await;

            let elapsed = send_start.elapsed().as_millis() as u64;

            match result {
                Ok(result) => {
                    // Update fault item - success
                    self.update_fault_item(mq.get_broker_name(), elapsed, false, true).await;

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
        if mode == CommunicationMode::Sync {
            self.producer_config.retry_times_when_send_failed() + 1
        } else {
            1
        }
    }

    /// Prepare message for retry (reset topic with namespace)
    fn prepare_message_for_retry<T: MessageTrait>(&self, msg: &mut T, topic: &CheetahString) {
        let namespace = self.client_config.namespace.as_ref().map(|s| s.as_str()).unwrap_or("");
        msg.set_topic(CheetahString::from_string(NamespaceUtil::wrap_namespace(
            namespace,
            topic.as_str(),
        )));
    }

    /// Handle send error - update fault item and log
    async fn handle_send_error(
        &self,
        mq: &MessageQueue,
        error: &rocketmq_error::RocketMQError,
        elapsed: u64,
        invoke_id: u64,
    ) {
        let broker_name = mq.get_broker_name();

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
            .mut_from_ref()
            .update_fault_item(broker_name.clone(), current_latency, isolation, reachable)
            .await;
    }

    async fn send_kernel_impl<T>(
        &mut self,
        msg: &mut T,
        mq: &MessageQueue,
        communication_mode: CommunicationMode,
        send_callback: Option<SendMessageCallback>,
        topic_publish_info: Option<&TopicPublishInfo>,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        T: MessageTrait + Send + Sync,
    {
        let begin_start_time = Instant::now();
        let mut broker_name = self
            .client_instance
            .as_ref()
            .unwrap()
            .get_broker_name_from_message_queue(mq)
            .await;
        let mut broker_addr = self
            .client_instance
            .as_ref()
            .unwrap()
            .find_broker_address_in_publish(broker_name.as_ref())
            .await;
        if broker_addr.is_none() {
            self.try_to_find_topic_publish_info(mq.get_topic_cs()).await;
            broker_name = self
                .client_instance
                .as_ref()
                .unwrap()
                .get_broker_name_from_message_queue(mq)
                .await;
            broker_addr = self
                .client_instance
                .as_ref()
                .unwrap()
                .find_broker_address_in_publish(broker_name.as_ref())
                .await;
        }

        if broker_addr.is_none() {
            return Err(mq_client_err!(format!("The broker[{}] not exist", broker_name,)));
        }
        let mut broker_addr = broker_addr.unwrap();
        broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, broker_addr.as_str());
        //let prev_body = msg.body.clone();
        let batch = msg.as_any().downcast_ref::<MessageBatch>().is_some();
        if !batch {
            MessageClientIDSetter::set_uniq_id(msg);
        }
        let mut topic_with_namespace = false;
        if self.client_config.get_namespace().is_some() {
            msg.set_instance_id(self.client_config.get_namespace().unwrap_or_default());
            topic_with_namespace = true;
        }
        let mut sys_flag = 0i32;
        let mut msg_body_compressed = false;
        if self.try_to_compress_message(msg) {
            sys_flag |= MessageSysFlag::COMPRESSED_FLAG;
            sys_flag |= self.producer_config.compress_type().get_compression_flag();
            msg_body_compressed = true;
        }
        let tran_msg = msg.get_property(&CheetahString::from_static_str(
            MessageConst::PROPERTY_TRANSACTION_PREPARED,
        ));
        if let Some(value) = tran_msg {
            let value_ = value.parse().unwrap_or(false);
            if value_ {
                sys_flag |= MessageSysFlag::TRANSACTION_PREPARED_TYPE;
            }
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

        //build send message request header
        let mut request_header = SendMessageRequestHeader {
            producer_group: CheetahString::from_string(self.producer_config.producer_group().to_string()),
            topic: CheetahString::from_string(msg.get_topic().to_string()),
            default_topic: CheetahString::from_string(self.producer_config.create_topic_key().to_string()),
            default_topic_queue_nums: self.producer_config.default_topic_queue_nums() as i32,
            queue_id: mq.get_queue_id(),
            sys_flag,
            born_timestamp: get_current_millis() as i64,
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

        // Handle namespace before creating send_message_context
        if topic_with_namespace && communication_mode == CommunicationMode::Async {
            msg.set_topic(CheetahString::from_string(
                NamespaceUtil::without_namespace_with_namespace(
                    msg.get_topic(),
                    self.client_config.get_namespace().unwrap_or_default().as_str(),
                ),
            ));
        }

        let mut send_message_context = if self.has_send_message_hook() {
            let namespace = self.client_config.get_namespace();
            let producer_group = self.producer_config.producer_group().clone();
            let born_host = self.client_config.client_ip.clone();
            let is_trans = msg.get_property(&CheetahString::from_static_str(
                MessageConst::PROPERTY_TRANSACTION_PREPARED,
            ));
            let msg_type_flag = msg
                .get_property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_STARTDE_LIVER_TIME,
                ))
                .is_some()
                || msg
                    .get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_DELAY_TIME_LEVEL))
                    .is_some();
            let mut send_message_context = SendMessageContext {
                producer: self.default_mqproducer_impl_inner.clone(),
                producer_group: Some(producer_group),
                communication_mode: Some(communication_mode),
                born_host,
                broker_addr: Some(broker_addr.clone()),
                message: None, // Don't store message reference to avoid borrow conflicts
                mq: Some(mq),
                namespace,
                ..Default::default()
            };

            if let Some(value) = is_trans {
                let value_ = value.parse().unwrap_or(false);
                if value_ {
                    send_message_context.msg_type = Some(MessageType::TransMsgHalf);
                }
            }
            if msg_type_flag {
                send_message_context.msg_type = Some(MessageType::DelayMsg);
            }
            let send_message_context = Some(send_message_context);
            self.execute_send_message_hook_before(&send_message_context);
            send_message_context
        } else {
            None
        };

        let send_result = match communication_mode {
            CommunicationMode::Async => {
                let cost_time_sync = (Instant::now() - begin_start_time).as_millis() as u64;
                self.client_instance
                    .as_ref()
                    .unwrap()
                    .get_mq_client_api_impl()
                    .send_message(
                        &broker_addr,
                        &broker_name,
                        msg,
                        request_header,
                        timeout - cost_time_sync,
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
                self.client_instance
                    .as_ref()
                    .unwrap()
                    .get_mq_client_api_impl()
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

        match send_result {
            Ok(result) => {
                if self.has_send_message_hook() {
                    let smc = send_message_context.as_mut().unwrap();
                    smc.send_result = result.clone();
                    self.execute_send_message_hook_after(&send_message_context);
                }
                Ok(result)
            }
            Err(err) => {
                if self.has_send_message_hook() {
                    //send_message_context.as_mut().unwrap().exception =
                    // Some(Arc::new(err.clone()));
                    self.execute_send_message_hook_after(&send_message_context);
                }
                Err(err)
            }
        }
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
        if let Some(message) = msg.as_any_mut().downcast_mut::<Message>() {
            if let Some(body) = message.compressed_body.as_mut() {
                if body.len() >= self.producer_config.compress_msg_body_over_howmuch() as usize {
                    let data = self
                        .producer_config
                        .compressor()
                        .unwrap()
                        .compress(body, self.producer_config.compress_level());
                    if let Ok(data) = data {
                        //store the compressed data
                        msg.set_compressed_body_mut(data);
                        return true;
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
        let binding = self.client_instance.as_ref().unwrap().get_mq_client_api_impl();
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

    async fn try_to_find_topic_publish_info(&self, topic: &CheetahString) -> Option<TopicPublishInfo> {
        let mut topic_publish_info = self.topic_publish_info_table.get(topic).map(|v| v.clone());
        if topic_publish_info.is_none() || !topic_publish_info.as_ref().unwrap().ok() {
            self.topic_publish_info_table
                .insert(topic.clone(), TopicPublishInfo::new());
            self.client_instance
                .as_ref()
                .unwrap()
                .mut_from_ref()
                .update_topic_route_info_from_name_server_topic(topic)
                .await;
            topic_publish_info = self.topic_publish_info_table.get(topic).map(|v| v.clone());
        }

        let topic_publish_info_ref = topic_publish_info.as_ref().unwrap();
        if topic_publish_info_ref.have_topic_router_info || topic_publish_info_ref.ok() {
            return topic_publish_info;
        }

        self.client_instance
            .as_ref()
            .unwrap()
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

    pub async fn invoke_message_queue_selector<M, T>(
        &mut self,
        msg: &M,
        selector: MessageQueueSelectorFn,
        arg: &T,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<MessageQueue>
    where
        M: MessageTrait,
        T: std::any::Any + Send,
    {
        let begin_start_time = Instant::now();
        self.make_sure_state_ok()?;
        Validators::check_message(Some(msg), self.producer_config.as_ref())?;
        let topic_publish_info = self.try_to_find_topic_publish_info(msg.get_topic()).await;
        if let Some(topic_publish_info) = topic_publish_info {
            if topic_publish_info.ok() {
                let message_queue_list = self
                    .client_instance
                    .as_mut()
                    .unwrap()
                    .mq_admin_impl
                    .parse_publish_message_queues(&topic_publish_info.message_queue_list, &mut self.client_config);
                let mut user_message = MessageAccessor::clone_message(msg);
                let user_topic = NamespaceUtil::without_namespace_with_namespace(
                    user_message.get_topic(),
                    self.client_config.get_namespace().unwrap_or_default().as_str(),
                );
                user_message.set_topic(CheetahString::from_string(user_topic));
                let message_queue = selector(&message_queue_list, msg, arg);
                let cost_time = begin_start_time.elapsed().as_millis() as u64;
                if timeout < cost_time {
                    return Err(rocketmq_error::RocketMQError::Timeout {
                        operation: "sendSelectImpl",
                        timeout_ms: timeout,
                    });
                }
                if let Some(message_queue) = message_queue {
                    return Ok(message_queue);
                }
                return Err(mq_client_err!("select message queue return None."));
            }
        }
        self.validate_name_server_setting();
        Err(mq_client_err!("select message queue return null."))
    }

    pub async fn send_with_selector_timeout<M, T>(
        &mut self,
        msg: M,
        selector: MessageQueueSelectorFn,
        arg: T,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        M: MessageTrait + Send + Sync,
        T: std::any::Any + Sync + Send,
    {
        self.send_select_impl(msg, selector, arg, CommunicationMode::Sync, None, timeout)
            .await
    }

    pub async fn fetch_publish_message_queues(
        &mut self,
        topic: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<Vec<MessageQueue>> {
        self.make_sure_state_ok()?;
        let client_instance = self.client_instance.as_mut().unwrap().mq_client_api_impl.clone();
        self.client_instance
            .as_mut()
            .unwrap()
            .mq_admin_impl
            .fetch_publish_message_queues(topic, client_instance.unwrap(), &mut self.client_config)
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
        S: Fn(&[MessageQueue], &dyn MessageTrait, &dyn std::any::Any) -> Option<MessageQueue> + Send + Sync + 'static,
        T: std::any::Any + Sync + Send,
        M: MessageTrait + Send + Sync,
    {
        let begin_timestamp = Instant::now();
        self.prepare_send_request(&mut msg, timeout).await;
        let correlation_id = msg
            .get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_CORRELATION_ID))
            .unwrap();
        let request_response_future = Arc::new(RequestResponseFuture::new(correlation_id.clone(), timeout, None));
        REQUEST_FUTURE_HOLDER
            .put_request(correlation_id.to_string(), request_response_future.clone())
            .await;
        let cost = begin_timestamp.elapsed().as_millis() as u64;
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
                )));
            }
        };
        let topic = msg.get_topic().clone();
        let _ = self
            .send_select_impl(
                msg,
                Arc::new(selector),
                arg,
                CommunicationMode::Async,
                Some(Arc::new(send_callback)),
                timeout - cost,
            )
            .await?;
        let result = self.wait_response(&topic, timeout, request_response_future, cost).await;

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
        S: Fn(&[MessageQueue], &dyn MessageTrait, &dyn std::any::Any) -> Option<MessageQueue> + Send + Sync + 'static,
        T: std::any::Any + Sync + Send,
        M: MessageTrait + Send + Sync,
    {
        let begin_timestamp = Instant::now();
        self.prepare_send_request(&mut msg, timeout).await;
        let correlation_id = msg
            .get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_CORRELATION_ID))
            .unwrap();
        let request_response_future = Arc::new(RequestResponseFuture::new(
            correlation_id.clone(),
            timeout,
            Some(request_callback.clone()),
        ));
        REQUEST_FUTURE_HOLDER
            .put_request(correlation_id.to_string(), request_response_future.clone())
            .await;
        let cost = begin_timestamp.elapsed().as_millis() as u64;
        let send_callback = move |result: Option<&SendResult>, err: Option<&dyn std::error::Error>| {
            if result.is_some() {
                request_response_future.set_send_request_ok(true);
                request_response_future.execute_request_callback();
                return;
            }
            if let Some(error) = err {
                request_response_future.set_cause(Box::new(rocketmq_error::RocketmqError::MQClientErr(
                    ClientErr::new(error.to_string()),
                )));
                Self::request_fail(correlation_id.as_str());
            }
        };
        let _ = self
            .send_select_impl(
                msg,
                Arc::new(selector),
                arg,
                CommunicationMode::Async,
                Some(Arc::new(send_callback)),
                timeout - cost,
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
        self.prepare_send_request(&mut msg, timeout).await;
        let correlation_id = msg
            .get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_CORRELATION_ID))
            .unwrap();
        let request_response_future = Arc::new(RequestResponseFuture::new(correlation_id.clone(), timeout, None));
        REQUEST_FUTURE_HOLDER
            .put_request(correlation_id.to_string(), request_response_future.clone())
            .await;
        let cost = begin_timestamp.elapsed().as_millis() as u64;
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
                )));
            }
        };
        let topic = msg.get_topic().clone();
        let _ = self
            .send_kernel_impl(
                &mut msg,
                &mq,
                CommunicationMode::Async,
                Some(Arc::new(send_callback)),
                None,
                timeout - cost,
            )
            .await?;
        let result = self.wait_response(&topic, timeout, request_response_future, cost).await;

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
        self.prepare_send_request(&mut msg, timeout).await;
        let correlation_id = msg
            .get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_CORRELATION_ID))
            .unwrap();
        let request_response_future = Arc::new(RequestResponseFuture::new(
            correlation_id.clone(),
            timeout,
            Some(request_callback.clone()),
        ));
        REQUEST_FUTURE_HOLDER
            .put_request(correlation_id.to_string(), request_response_future.clone())
            .await;
        let cost = begin_timestamp.elapsed().as_millis() as u64;
        let send_callback = move |result: Option<&SendResult>, err: Option<&dyn std::error::Error>| {
            if result.is_some() {
                request_response_future.set_send_request_ok(true);
                return;
            }
            if let Some(error) = err {
                request_response_future.set_cause(Box::new(rocketmq_error::RocketmqError::MQClientErr(
                    ClientErr::new(error.to_string()),
                )));
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
                timeout - cost,
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
        self.prepare_send_request(&mut msg, timeout).await;
        let correlation_id = msg
            .get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_CORRELATION_ID))
            .unwrap();
        let request_response_future = Arc::new(RequestResponseFuture::new(
            correlation_id.clone(),
            timeout,
            Some(request_callback.clone()),
        ));
        REQUEST_FUTURE_HOLDER
            .put_request(correlation_id.to_string(), request_response_future.clone())
            .await;
        let cost = begin_timestamp.elapsed().as_millis() as u64;
        let send_callback = move |result: Option<&SendResult>, err: Option<&dyn std::error::Error>| {
            if result.is_some() {
                request_response_future.set_send_request_ok(true);
                request_response_future.execute_request_callback();
                return;
            }
            if let Some(error) = err {
                request_response_future.set_cause(Box::new(rocketmq_error::RocketmqError::MQClientErr(
                    ClientErr::new(error.to_string()),
                )));
                Self::request_fail(correlation_id.as_str());
            }
        };
        self.send_default_impl(
            &mut msg,
            CommunicationMode::Async,
            Some(Arc::new(send_callback)),
            timeout - cost,
        )
        .await?;
        Ok(())
    }

    async fn request_fail(correlation_id: &str) {
        let request_response_future = REQUEST_FUTURE_HOLDER.get_request(correlation_id).await;
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
        self.prepare_send_request(&mut msg, timeout).await;
        let correlation_id = msg
            .get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_CORRELATION_ID))
            .unwrap();
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
                )));
            }
        };
        let topic = msg.get_topic().clone();
        let cost = begin_timestamp.elapsed().as_millis() as u64;
        self.send_default_impl(
            &mut msg,
            CommunicationMode::Async,
            Some(Arc::new(send_callback)),
            timeout - cost,
        )
        .await?;

        let result = self.wait_response(&topic, timeout, request_response_future, cost).await;

        REQUEST_FUTURE_HOLDER.remove_request(correlation_id.as_str()).await;
        result
    }

    async fn wait_response(
        &mut self,
        topic: &CheetahString,
        timeout: u64,
        request_response_future: Arc<RequestResponseFuture>,
        cost: u64,
    ) -> rocketmq_error::RocketMQResult<Box<dyn MessageTrait + Send>> {
        let response_message = request_response_future
            .wait_response_message(Duration::from_millis(timeout - cost))
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

    async fn prepare_send_request<M>(&mut self, msg: &mut M, timeout: u64)
    where
        M: MessageTrait,
    {
        let correlation_id = CorrelationIdUtil::create_correlation_id();
        let request_client_id = self.client_instance.as_mut().unwrap().client_id.clone();
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
        let guard = self.client_instance.as_mut().unwrap().topic_route_table.read().await;
        let has_route_data = guard.contains_key(msg.get_topic().as_str());
        drop(guard);
        if !has_route_data {
            let begin_timestamp = Instant::now();
            self.try_to_find_topic_publish_info(msg.get_topic()).await;
            self.client_instance
                .as_mut()
                .unwrap()
                .send_heartbeat_to_all_broker_with_lock()
                .await;
            let cost = begin_timestamp.elapsed().as_millis() as u64;
            if cost > 500 {
                warn!("prepare send request for <{}> cost {} ms", msg.get_topic(), cost);
            }
        }
    }

    pub async fn send_message_in_transaction<M>(
        &mut self,
        mut msg: M,
        arg: Option<Box<dyn Any + Send + Sync>>,
    ) -> rocketmq_error::RocketMQResult<TransactionSendResult>
    where
        M: MessageTrait + Send + Sync,
    {
        // Ensure transactional messages do not support delayed delivery
        self.ensure_not_delayed_for_transactional(&msg)?;

        // ignore DelayTimeLevel parameter
        if msg.get_delay_time_level() != 0 {
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
        let result = self.send(&mut msg).await;
        if let Err(e) = result {
            return Err(mq_client_err!(format!("send message in transaction error, {}", e)));
        }
        let send_result = match result.unwrap() {
            Some(res) => res,
            None => return Err(mq_client_err!("send result is none")),
        };
        let local_transaction_state = match send_result.send_status {
            SendStatus::SendOk => {
                if let Some(ref transaction_id) = send_result.transaction_id {
                    msg.put_user_property(
                        CheetahString::from_static_str(MessageConst::PROPERTY_TRANSACTION_ID),
                        CheetahString::from_string(transaction_id.to_owned()),
                    );
                }
                let transaction_id = msg.get_property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
                ));
                if let Some(transaction_id) = transaction_id {
                    msg.set_transaction_id(transaction_id);
                }
                self.transaction_listener
                    .as_ref()
                    .unwrap()
                    .execute_local_transaction(&msg, arg.as_deref())
            }
            SendStatus::FlushDiskTimeout | SendStatus::FlushSlaveTimeout | SendStatus::SlaveNotAvailable => {
                LocalTransactionState::RollbackMessage
            }
        };
        if let Err(e) = self.end_transaction(&msg, &send_result, local_transaction_state).await {
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

    pub fn init_transaction_env(&mut self, check_runtime: Option<Arc<RocketMQRuntime>>) {
        if check_runtime.is_some() {
            self.check_runtime = check_runtime;
        } else {
            self.check_runtime = Some(Arc::new(RocketMQRuntime::new_multi(
                num_cpus::get(),
                "thread-transaction-check-",
            )));
        }
    }

    pub async fn end_transaction(
        &mut self,
        msg: &dyn MessageTrait,
        send_result: &SendResult,
        local_transaction_state: LocalTransactionState,
    ) -> rocketmq_error::RocketMQResult<()> {
        let id = if let Some(ref offset_msg_id) = send_result.offset_msg_id {
            MessageDecoder::decode_message_id(offset_msg_id)
        } else {
            MessageDecoder::decode_message_id(send_result.msg_id.as_ref().unwrap())
        };
        let transaction_id = send_result.transaction_id.clone();
        let queue = self
            .client_config
            .queue_with_namespace(send_result.message_queue.clone().unwrap());
        let dest_broker_name = self
            .client_instance
            .as_mut()
            .unwrap()
            .get_broker_name_from_message_queue(&queue)
            .await;
        let broker_addr = self
            .client_instance
            .as_mut()
            .unwrap()
            .find_broker_address_in_publish(dest_broker_name.as_ref())
            .await;
        let request_header = EndTransactionRequestHeader {
            topic: CheetahString::from_string(msg.get_topic().to_string()),
            producer_group: CheetahString::from_string(self.producer_config.producer_group().to_string()),
            tran_state_table_offset: send_result.queue_offset,
            commit_log_offset: id.offset as u64,
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
        self.client_instance
            .as_mut()
            .unwrap()
            .mq_client_api_impl
            .as_mut()
            .unwrap()
            .end_transaction_oneway(
                broker_addr.as_ref().unwrap(),
                request_header,
                CheetahString::from_static_str(""),
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

    pub fn set_default_mqproducer_impl_inner(&mut self, default_mqproducer_impl_inner: ArcMut<DefaultMQProducerImpl>) {
        self.default_mqproducer_impl_inner = Some(default_mqproducer_impl_inner);
    }

    pub fn set_transaction_listener(&mut self, transaction_listener: Arc<Box<dyn TransactionListener>>) {
        self.transaction_listener = Some(transaction_listener);
    }

    pub fn set_check_runtime(&mut self, check_runtime: Arc<RocketMQRuntime>) {
        self.check_runtime = Some(check_runtime);
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

    fn get_check_listener(&self) -> Arc<Box<dyn TransactionListener>> {
        todo!()
    }

    fn check_transaction_state(
        &self,
        broker_addr: &CheetahString,
        msg: MessageExt,
        check_request_header: CheckTransactionStateRequestHeader,
    ) {
        let transaction_listener = self.transaction_listener.clone().unwrap();
        let mut producer_impl_inner = self.default_mqproducer_impl_inner.clone().unwrap();
        let broker_addr = broker_addr.clone();
        self.check_runtime.as_ref().unwrap().get_handle().spawn(async move {
            let mut unique_key = msg.get_property(&CheetahString::from_static_str(
                MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
            ));
            if unique_key.is_none() {
                unique_key = Some(msg.msg_id.clone());
            }
            let transaction_state = transaction_listener.check_local_transaction(&msg);
            let request_header = EndTransactionRequestHeader {
                topic: check_request_header.topic.clone().unwrap_or_default(),
                producer_group: CheetahString::from_string(
                    producer_impl_inner.producer_config.producer_group().to_string(),
                ),
                tran_state_table_offset: check_request_header.commit_log_offset as u64,
                commit_log_offset: check_request_header.commit_log_offset as u64,
                commit_or_rollback: match transaction_state {
                    LocalTransactionState::CommitMessage => MessageSysFlag::TRANSACTION_COMMIT_TYPE,
                    LocalTransactionState::RollbackMessage => MessageSysFlag::TRANSACTION_ROLLBACK_TYPE,
                    LocalTransactionState::Unknown => MessageSysFlag::TRANSACTION_NOT_TYPE,
                },
                from_transaction_check: true,
                msg_id: unique_key.clone().unwrap_or_default(),
                transaction_id: check_request_header.transaction_id.clone(),
                rpc_request_header: RpcRequestHeader {
                    broker_name: check_request_header.rpc_request_header.unwrap_or_default().broker_name,
                    ..Default::default()
                },
            };
            producer_impl_inner.do_execute_end_transaction_hook(
                &msg.message,
                unique_key.as_ref().unwrap(),
                &broker_addr,
                transaction_state,
                true,
            );
            let _ = producer_impl_inner
                .client_instance
                .as_mut()
                .unwrap()
                .mq_client_api_impl
                .as_mut()
                .unwrap()
                .end_transaction_oneway(&broker_addr, request_header, CheetahString::from_static_str(""), 3000)
                .await;
        });
    }

    fn update_topic_publish_info(&mut self, topic: impl Into<CheetahString>, info: Option<TopicPublishInfo>) {
        let topic = topic.into();
        if topic.is_empty() || info.is_none() {
            return;
        }
        self.topic_publish_info_table.insert(topic, info.unwrap());
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

                self.check_config()?;

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
                self.client_instance = Some(client_instance);
                let self_clone = self.default_mqproducer_impl_inner.clone();
                let register_ok = self
                    .client_instance
                    .as_mut()
                    .unwrap()
                    .register_producer(
                        self.producer_config.producer_group(),
                        MQProducerInnerImpl {
                            default_mqproducer_impl_inner: self_clone,
                        },
                    )
                    .await;
                if !register_ok {
                    self.service_state = ServiceState::CreateJust;
                    return Err(mq_client_err!(format!(
                        "The producer group[{}] has been created before, specify another name please. {}",
                        self.producer_config.producer_group(),
                        FAQUrl::suggest_todo(FAQUrl::GROUP_NAME_DUPLICATE_URL)
                    )));
                }
                if start_factory {
                    let cloned = self.client_instance.as_mut().cloned().unwrap();
                    Box::pin(self.client_instance.as_mut().unwrap().start(cloned)).await?;
                    //self.client_instance.as_mut().unwrap().start().await;
                }

                self.init_topic_route().await;
                self.mq_fault_strategy.start_detector();

                // Update both states
                self.service_state = ServiceState::Running;
                self.state.store(ProducerState::Running as u8, Ordering::SeqCst);

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
                    _ => Err(mq_client_err!(format!("Cannot start producer in state {:?}", state))),
                }
            }
        }
    }

    /// Shutdown the producer gracefully
    pub async fn shutdown(&mut self) -> rocketmq_error::RocketMQResult<()> {
        self.shutdown_with_factory(true).await
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
                // Perform shutdown
                self.do_shutdown_internal(shutdown_factory).await?;

                // Shutdown runtime if exists
                if let Some(runtime) = self.runtime.write().await.take() {
                    runtime.shutdown().await;
                }

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
        // 1. Unregister producer from client instance
        if let Some(client_instance) = self.client_instance.as_mut() {
            client_instance
                .unregister_producer(self.producer_config.producer_group())
                .await;
        }

        // 2. Stop fault strategy detector
        self.mq_fault_strategy.shutdown();

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
            let new_topic = CheetahString::from_string(NamespaceUtil::wrap_namespace(
                self.client_config.get_namespace().unwrap_or_default().as_str(),
                topic,
            ));
            let topic_publish_info = self.try_to_find_topic_publish_info(&new_topic).await;
            if topic_publish_info.is_none() || !topic_publish_info.unwrap().ok() {
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
        self.mq_fault_strategy
            .set_send_latency_fault_enable(send_latency_fault_enable);
    }

    /// Ensure transactional messages do not support delayed delivery
    fn ensure_not_delayed_for_transactional<M>(&self, msg: &M) -> rocketmq_error::RocketMQResult<()>
    where
        M: MessageTrait,
    {
        if msg
            .get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_DELAY_TIME_LEVEL))
            .is_some()
            || msg
                .get_property(&CheetahString::from_static_str("TIMER_DELAY_MS"))
                .is_some()
            || msg
                .get_property(&CheetahString::from_static_str("TIMER_DELAY_SEC"))
                .is_some()
            || msg
                .get_property(&CheetahString::from_static_str("TIMER_DELIVER_MS"))
                .is_some()
        {
            return Err(mq_client_err!("Transactional messages do not support delayed delivery"));
        }
        Ok(())
    }
}

pub(crate) struct DefaultServiceDetector {
    client_instance: ArcMut<MQClientInstance>,
    topic_publish_info_table: Arc<DashMap<CheetahString /* topic */, TopicPublishInfo>>,
}

impl ServiceDetector for DefaultServiceDetector {
    type Fut<'a>
        = impl std::future::Future<Output = bool> + Send + 'a
    where
        Self: 'a;

    fn detect<'a>(&'a self, endpoint: &'a str, timeout_millis: u64) -> Self::Fut<'a> {
        async move {
            // Pick a topic to use for detection
            let topic = match self
                .topic_publish_info_table
                .iter()
                .next()
                .map(|entry| entry.key().clone())
            {
                Some(t) => t,
                None => return false,
            };

            // Create a message queue for the detection request
            let mq = MessageQueue::from_parts(topic.as_str(), endpoint, 0);

            // Clone the client instance to get mutable access
            let mut client_instance = self.client_instance.clone();

            // Try to get max offset from the broker with timeout
            // This is a lightweight operation that verifies broker connectivity
            let result = tokio::time::timeout(Duration::from_millis(timeout_millis), async move {
                match client_instance.mq_client_api_impl.as_mut() {
                    Some(api) => {
                        // Attempt to get max offset - if this succeeds, broker is healthy
                        api.get_max_offset(endpoint, &mq, timeout_millis).await.is_ok()
                    }
                    None => false,
                }
            })
            .await;

            matches!(result, Ok(true))
        }
    }
}

pub(crate) struct DefaultResolver {
    client_instance: ArcMut<MQClientInstance>,
}

impl Resolver for DefaultResolver {
    async fn resolve(&self, name: &CheetahString) -> Option<CheetahString> {
        self.client_instance.find_broker_address_in_publish(name).await
    }
}
