/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use rand::random;
use rocketmq_common::common::base::service_state::ServiceState;
use rocketmq_common::common::message::message_batch::MessageBatch;
use rocketmq_common::common::message::message_client_id_setter::MessageClientIDSetter;
use rocketmq_common::common::message::message_enum::MessageType;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::message_single::MessageExt;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::CLIENT_INNER_PRODUCER_GROUP;
use rocketmq_common::common::mix_all::DEFAULT_PRODUCER_GROUP;
use rocketmq_common::common::sys_flag::message_sys_flag::MessageSysFlag;
use rocketmq_common::common::FAQUrl;
use rocketmq_common::utils::correlation_id_util::CorrelationIdUtil;
use rocketmq_common::ArcRefCellWrapper;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::MessageDecoder;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_remoting::protocol::header::check_transaction_state_request_header::CheckTransactionStateRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::rpc::rpc_request_header::RpcRequestHeader;
use rocketmq_remoting::rpc::topic_request_header::TopicRequestHeader;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_runtime::RocketMQRuntime;
use tokio::runtime::Handle;
use tokio::sync::RwLock;
use tokio::sync::Semaphore;
use tokio_util::bytes::Bytes;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::base::validators::Validators;
use crate::common::client_error_code::ClientErrorCode;
use crate::error::MQClientError;
use crate::error::MQClientError::MQClientException;
use crate::error::MQClientError::RemotingTooMuchRequestException;
use crate::error::MQClientError::RequestTimeoutException;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::hook::check_forbidden_context::CheckForbiddenContext;
use crate::hook::check_forbidden_hook::CheckForbiddenHook;
use crate::hook::end_transaction_hook::EndTransactionHook;
use crate::hook::send_message_context::SendMessageContext;
use crate::hook::send_message_hook::SendMessageHook;
use crate::implementation::communication_mode::CommunicationMode;
use crate::implementation::mq_client_manager::MQClientManager;
use crate::latency::mq_fault_strategy::MQFaultStrategy;
use crate::latency::resolver::Resolver;
use crate::latency::service_detector::ServiceDetector;
use crate::producer::default_mq_producer::ProducerConfig;
use crate::producer::message_queue_selector::MessageQueueSelectorFn;
use crate::producer::producer_impl::mq_producer_inner::MQProducerInner;
use crate::producer::producer_impl::topic_publish_info::TopicPublishInfo;
use crate::producer::request_future_holder::REQUEST_FUTURE_HOLDER;
use crate::producer::request_response_future::RequestResponseFuture;
use crate::producer::send_callback::SendMessageCallback;
use crate::producer::send_result::SendResult;
use crate::producer::send_status::SendStatus;
use crate::producer::transaction_listener::TransactionListener;
use crate::Result;

#[derive(Clone)]
pub struct DefaultMQProducerImpl {
    client_config: ClientConfig,
    producer_config: Arc<ProducerConfig>,
    topic_publish_info_table: Arc<RwLock<HashMap<String /* topic */, TopicPublishInfo>>>,
    send_message_hook_list: ArcRefCellWrapper<Vec<Box<dyn SendMessageHook>>>,
    end_transaction_hook_list: Vec<Arc<Box<dyn EndTransactionHook>>>,
    check_forbidden_hook_list: Vec<Arc<Box<dyn CheckForbiddenHook>>>,
    rpc_hook: Option<Arc<Box<dyn RPCHook>>>,
    service_state: ServiceState,
    client_instance: Option<ArcRefCellWrapper<MQClientInstance>>,
    mq_fault_strategy: ArcRefCellWrapper<MQFaultStrategy>,
    semaphore_async_send_num: Arc<Semaphore>,
    semaphore_async_send_size: Arc<Semaphore>,
    async_sender_runtime: Option<Arc<RocketMQRuntime>>,
    default_async_sender_runtime: Option<Arc<RocketMQRuntime>>,
}

#[allow(unused_must_use)]
#[allow(unused_assignments)]
impl DefaultMQProducerImpl {
    pub fn new(
        client_config: ClientConfig,
        producer_config: ProducerConfig,
        rpc_hook: Option<Arc<Box<dyn RPCHook>>>,
    ) -> Self {
        let semaphore_async_send_num =
            Semaphore::new(producer_config.back_pressure_for_async_send_num().max(10) as usize);
        let semaphore_async_send_size = Semaphore::new(
            producer_config
                .back_pressure_for_async_send_size()
                .max(1024 * 1024) as usize,
        );
        let topic_publish_info_table = Arc::new(RwLock::new(HashMap::new()));
        DefaultMQProducerImpl {
            client_config: client_config.clone(),
            producer_config: Arc::new(producer_config),
            topic_publish_info_table,
            send_message_hook_list: ArcRefCellWrapper::new(vec![]),
            end_transaction_hook_list: vec![],
            check_forbidden_hook_list: vec![],
            rpc_hook: None,
            service_state: ServiceState::CreateJust,
            client_instance: None,
            mq_fault_strategy: ArcRefCellWrapper::new(MQFaultStrategy::new(&client_config)),
            semaphore_async_send_num: Arc::new(semaphore_async_send_num),
            semaphore_async_send_size: Arc::new(semaphore_async_send_size),
            async_sender_runtime: None,
            default_async_sender_runtime: Some(Arc::new(RocketMQRuntime::new_multi(
                num_cpus::get(),
                "async-sender",
            ))),
        }
    }

    #[inline]
    pub async fn send_with_timeout<T>(&mut self, msg: T, timeout: u64) -> Result<Option<SendResult>>
    where
        T: MessageTrait + Clone + Send + Sync,
    {
        self.send_default_impl(msg, CommunicationMode::Sync, None, timeout)
            .await
    }

    #[inline]
    pub async fn send<T>(&mut self, msg: T) -> Result<Option<SendResult>>
    where
        T: MessageTrait + Clone + Send + Sync,
    {
        self.send_with_timeout(msg, self.producer_config.send_msg_timeout() as u64)
            .await
    }

    #[inline]
    pub async fn async_send_with_callback<T>(
        &mut self,
        msg: T,
        send_callback: Option<SendMessageCallback>,
    ) -> Result<()>
    where
        T: MessageTrait + Clone + Send + Sync,
    {
        self.async_send_with_callback_timeout(
            msg,
            send_callback,
            self.producer_config.send_msg_timeout() as u64,
        )
        .await
    }

    #[inline]
    pub async fn sync_send_with_message_queue<T>(
        &mut self,
        msg: T,
        mq: MessageQueue,
    ) -> Result<Option<SendResult>>
    where
        T: MessageTrait + Clone + Send + Sync,
    {
        self.sync_send_with_message_queue_timeout(
            msg,
            mq,
            self.producer_config.send_msg_timeout() as u64,
        )
        .await
    }

    #[inline]
    pub async fn send_oneway<T>(&mut self, msg: T) -> Result<()>
    where
        T: MessageTrait + Clone + Send + Sync,
    {
        self.send_default_impl(
            msg,
            CommunicationMode::Oneway,
            None,
            self.producer_config.send_msg_timeout() as u64,
        )
        .await?;
        Ok(())
    }

    pub async fn send_oneway_with_message_queue<T>(
        &mut self,
        msg: T,
        mq: MessageQueue,
    ) -> Result<()>
    where
        T: MessageTrait + Clone + Send + Sync,
    {
        self.make_sure_state_ok()?;
        Validators::check_message(Some(&msg), self.producer_config.as_ref())?;
        self.send_default_impl(
            msg,
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
    ) -> Result<Option<SendResult>>
    where
        T: MessageTrait + Clone + Send + Sync,
    {
        let begin_start_time = Instant::now();
        self.make_sure_state_ok()?;
        Validators::check_message(Some(&msg), self.producer_config.as_ref())?;

        if msg.get_topic() != mq.get_topic() {
            return Err(MQClientError::MQClientException(
                -1,
                format!(
                    "message topic [{}] is not equal with message queue topic [{}]",
                    msg.get_topic(),
                    mq.get_topic()
                ),
            ));
        }
        let cost_time = begin_start_time.elapsed().as_millis() as u64;
        if timeout < cost_time {
            return Err(MQClientError::RequestTimeoutException(
                -1,
                format!(
                    "send message timeout {}ms is required, but {}ms is given",
                    timeout, cost_time
                ),
            ));
        }
        self.send_kernel_impl(
            &mut msg,
            &mq,
            CommunicationMode::Sync,
            None,
            None,
            timeout - cost_time,
        )
        .await
    }

    #[inline]
    pub async fn async_send_with_message_queue_callback<T>(
        &mut self,
        msg: T,
        mq: MessageQueue,
        send_callback: Option<SendMessageCallback>,
    ) -> Result<()>
    where
        T: MessageTrait + Clone + Send + Sync,
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
    ) -> Result<()>
    where
        M: MessageTrait + Clone + Send + Sync,
        T: std::any::Any + Sync + Send,
    {
        let begin_start_time = Instant::now();
        let mut clone_self = self.clone();
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
                    Some(&RemotingTooMuchRequestException("call timeout".to_string())),
                );
            }
            clone_self
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
    ) -> Result<()>
    where
        M: MessageTrait + Clone + Send + Sync,
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
    ) -> Result<Option<SendResult>>
    where
        M: MessageTrait + Clone + Send + Sync,
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
                    .parse_publish_message_queues(
                        &topic_publish_info.message_queue_list,
                        &mut self.client_config,
                    );
                let mut user_message = msg.clone();
                let user_topic = NamespaceUtil::without_namespace_with_namespace(
                    user_message.get_topic(),
                    self.client_config
                        .get_namespace()
                        .unwrap_or("".to_string())
                        .as_str(),
                );
                user_message.set_topic(user_topic.as_str());
                let message_queue = selector(&message_queue_list, &msg, &arg);
                let cost_time = begin_start_time.elapsed().as_millis() as u64;
                if timeout < cost_time {
                    return Err(MQClientError::RemotingTooMuchRequestException(
                        "sendSelectImpl call timeout".to_string(),
                    ));
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
                return Err(MQClientError::MQClientException(
                    -1,
                    "select message queue return null.".to_string(),
                ));
            }
        }
        self.validate_name_server_setting()?;
        Err(MQClientError::MQClientException(
            -1,
            format!("No route info for this topic, {}", msg.get_topic()),
        ))
    }

    #[inline]
    pub async fn async_send_batch_to_queue_with_callback_timeout<T>(
        &mut self,
        mut msg: T,
        mq: MessageQueue,
        send_callback: Option<SendMessageCallback>,
        timeout: u64,
    ) -> Result<()>
    where
        T: MessageTrait + Clone + Send + Sync,
    {
        let mut producer_impl = self.clone();
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
                    Some(&MQClientError::MQClientException(
                        -1,
                        format!(
                            "message topic [{}] is not equal with message queue topic [{}]",
                            msg.get_topic(),
                            mq.get_topic()
                        ),
                    )),
                );
                return;
            }

            let cost_time = (Instant::now() - begin_start_time).as_millis() as u64;
            if timeout <= cost_time {
                send_callback_inner.as_ref().unwrap()(
                    None,
                    Some(&RemotingTooMuchRequestException("call timeout".to_string())),
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
        msg: T,
        send_callback: Option<SendMessageCallback>,
        timeout: u64,
    ) -> Result<()>
    where
        T: MessageTrait + Clone + Send + Sync,
    {
        let mut producer_impl = self.clone();
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
                    Some(&RemotingTooMuchRequestException(
                        "asyncSend call timeout".to_string(),
                    )),
                );
            }
            let result = producer_impl
                .send_default_impl(
                    msg,
                    CommunicationMode::Async,
                    send_callback_inner.clone(),
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

    async fn execute_async_message_send<F>(
        &mut self,
        f: F,
        send_callback: Option<SendMessageCallback>,
        timeout: u64,
        begin_start_time: Instant,
        msg_len: usize,
    ) -> Result<()>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let is_enable_backpressure_for_async_mode =
            self.producer_config.enable_backpressure_for_async_mode();

        let (acquire_value_num, acquire_value_size) = if is_enable_backpressure_for_async_mode {
            //back pressure
            let cost_time = (Instant::now() - begin_start_time).as_millis() as u64;
            let is_semaphore_async_numb_acquired = (timeout - cost_time) > 0;
            if !is_semaphore_async_numb_acquired {
                send_callback.as_ref().unwrap()(
                    None,
                    Some(&RemotingTooMuchRequestException(
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
                            Some(&RemotingTooMuchRequestException(
                                "send message tryAcquire semaphoreAsyncNum timeout".to_string(),
                            )),
                        );
                        return Ok(());
                    }
                },
                Err(_) => {
                    send_callback.as_ref().unwrap()(
                        None,
                        Some(&RemotingTooMuchRequestException(
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
                    Some(&RemotingTooMuchRequestException(
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
                            Some(&RemotingTooMuchRequestException(
                                "send message tryAcquire semaphoreAsyncSize timeout".to_string(),
                            )),
                        );
                        return Ok(());
                    }
                },
                Err(_) => {
                    send_callback.as_ref().unwrap()(
                        None,
                        Some(&RemotingTooMuchRequestException(
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

        self.get_async_sender_executor().get_handle().spawn(f);
        drop((acquire_value_num, acquire_value_size));
        Ok(())
    }

    #[inline]
    pub fn get_async_sender_executor(&self) -> &Arc<RocketMQRuntime> {
        if let Some(ref async_sender_runtime) = self.async_sender_runtime {
            async_sender_runtime
        } else {
            self.default_async_sender_runtime.as_ref().unwrap()
        }
    }

    async fn send_default_impl<T>(
        &mut self,
        mut msg: T,
        communication_mode: CommunicationMode,
        send_callback: Option<SendMessageCallback>,
        timeout: u64,
    ) -> Result<Option<SendResult>>
    where
        T: MessageTrait + Clone + Send + Sync,
    {
        self.make_sure_state_ok()?;
        let invoke_id = random::<u64>();
        let begin_timestamp_first = Instant::now();
        let mut begin_timestamp_prev = begin_timestamp_first;
        let mut end_timestamp = begin_timestamp_first;
        let topic = msg.get_topic().to_string();
        let topic_publish_info = self.try_to_find_topic_publish_info(topic.as_str()).await;
        if let Some(topic_publish_info) = topic_publish_info {
            if topic_publish_info.ok() {
                let mut call_timeout = false;
                let mut mq: Option<MessageQueue> = None;
                let mut exception: Option<MQClientError> = None;
                let mut send_result: Option<SendResult> = None;
                let times_total = if communication_mode == CommunicationMode::Sync {
                    self.producer_config.retry_times_when_send_failed() + 1
                } else {
                    1
                };
                let mut brokers_sent = vec![String::new(); times_total as usize];
                let mut reset_index = false;
                //handle send message
                for times in 0..times_total {
                    let last_broker_name = mq.as_ref().map(|mq_inner| mq_inner.get_broker_name());
                    if times > 0 {
                        reset_index = true;
                    }

                    //select one message queue to send message
                    let mq_selected = self.select_one_message_queue(
                        &topic_publish_info,
                        last_broker_name,
                        reset_index,
                    );
                    if mq_selected.is_some() {
                        mq = mq_selected;
                        brokers_sent[times as usize] =
                            mq.as_ref().unwrap().get_broker_name().to_string();
                        begin_timestamp_prev = Instant::now();
                        if times > 0 {
                            //Reset topic with namespace during resend.
                            let namespace =
                                self.client_config.get_namespace().unwrap_or("".to_string());
                            msg.set_topic(
                                NamespaceUtil::wrap_namespace(namespace.as_str(), topic.as_str())
                                    .as_str(),
                            );
                        }
                        let cost_time =
                            (begin_timestamp_prev - begin_timestamp_first).as_millis() as u64;
                        if timeout < cost_time {
                            call_timeout = true;
                            break;
                        }

                        //send message to broker
                        let result_inner = self
                            .send_kernel_impl(
                                &mut msg,
                                mq.as_ref().unwrap(),
                                communication_mode,
                                send_callback.clone(),
                                Some(&topic_publish_info),
                                timeout - cost_time,
                            )
                            .await;

                        match result_inner {
                            Ok(result) => {
                                send_result = result;
                                end_timestamp = Instant::now();
                                self.update_fault_item(
                                    mq.as_ref().unwrap().get_broker_name(),
                                    (end_timestamp - begin_timestamp_prev).as_millis() as u64,
                                    false,
                                    true,
                                );
                                return match communication_mode {
                                    CommunicationMode::Sync => {
                                        if let Some(ref result) = send_result {
                                            if result.send_status != SendStatus::SendOk
                                                && self
                                                    .producer_config
                                                    .retry_another_broker_when_not_store_ok()
                                            {
                                                continue;
                                            }
                                        }
                                        Ok(send_result)
                                    }
                                    CommunicationMode::Async | CommunicationMode::Oneway => {
                                        Ok(None)
                                    }
                                };
                            }
                            Err(err) => match err {
                                MQClientError::MQClientException(_, _) => {
                                    end_timestamp = Instant::now();
                                    let elapsed =
                                        (end_timestamp - begin_timestamp_prev).as_millis() as u64;
                                    self.update_fault_item(
                                        mq.as_ref().unwrap().get_broker_name(),
                                        elapsed,
                                        false,
                                        true,
                                    );
                                    warn!(
                                        "sendKernelImpl exception, resend at once, InvokeID: {}, \
                                         RT: {}ms, Broker: {:?},{}",
                                        invoke_id,
                                        elapsed,
                                        mq,
                                        err.to_string()
                                    );
                                    // warn!("{:?}", msg);
                                    exception = Some(err);
                                    continue;
                                }
                                MQClientError::MQBrokerException(code, _, _) => {
                                    end_timestamp = Instant::now();
                                    let elapsed =
                                        (end_timestamp - begin_timestamp_prev).as_millis() as u64;
                                    self.update_fault_item(
                                        mq.as_ref().unwrap().get_broker_name(),
                                        elapsed,
                                        true,
                                        false,
                                    );
                                    if self.producer_config.retry_response_codes().contains(&code) {
                                        exception = Some(err);
                                        continue;
                                    } else {
                                        if send_result.is_some() {
                                            return Ok(send_result);
                                        }
                                        return Err(err);
                                    }
                                }
                                MQClientError::RemotingException(_) => {
                                    end_timestamp = Instant::now();
                                    let elapsed =
                                        (end_timestamp - begin_timestamp_prev).as_millis() as u64;
                                    if self.mq_fault_strategy.is_start_detector_enable() {
                                        self.update_fault_item(
                                            mq.as_ref().unwrap().get_broker_name(),
                                            elapsed,
                                            true,
                                            false,
                                        );
                                    } else {
                                        self.update_fault_item(
                                            mq.as_ref().unwrap().get_broker_name(),
                                            elapsed,
                                            true,
                                            true,
                                        );
                                    }
                                    exception = Some(err);
                                    continue;
                                }

                                _ => {
                                    return Err(err);
                                }
                            },
                        }
                    } else {
                        break;
                    }
                }
                if send_result.is_some() {
                    return Ok(send_result);
                }

                if call_timeout {
                    return Err(MQClientError::RemotingTooMuchRequestException(
                        "sendDefaultImpl call timeout".to_string(),
                    ));
                }

                let info = format!(
                    "Send [{}] times, still failed, cost [{}]ms, Topic:{}, BrokersSent: {} {}",
                    times_total,
                    (Instant::now() - begin_timestamp_first).as_millis(),
                    topic,
                    brokers_sent.join(","),
                    FAQUrl::suggest_todo(FAQUrl::SEND_MSG_FAILED)
                );

                return if let Some(err) = exception {
                    match err {
                        MQClientError::MQClientException(_, _) => Err(MQClientException(
                            ClientErrorCode::BROKER_NOT_EXIST_EXCEPTION,
                            info,
                        )),
                        RemotingTooMuchRequestException(_) => Err(MQClientException(
                            ClientErrorCode::BROKER_NOT_EXIST_EXCEPTION,
                            info,
                        )),
                        MQClientError::MQBrokerException(_, _, _) => Err(MQClientException(
                            ClientErrorCode::BROKER_NOT_EXIST_EXCEPTION,
                            info,
                        )),
                        MQClientError::RequestTimeoutException(_, _) => Err(MQClientException(
                            ClientErrorCode::BROKER_NOT_EXIST_EXCEPTION,
                            info,
                        )),
                        MQClientError::OffsetNotFoundException(_, _, _) => Err(MQClientException(
                            ClientErrorCode::BROKER_NOT_EXIST_EXCEPTION,
                            info,
                        )),
                        MQClientError::RemotingException(_) => Err(MQClientException(
                            ClientErrorCode::BROKER_NOT_EXIST_EXCEPTION,
                            info,
                        )),
                    }
                } else {
                    Err(MQClientException(-1, info))
                };
            }
        }
        self.validate_name_server_setting()?;
        Err(MQClientException(
            ClientErrorCode::NOT_FOUND_TOPIC_EXCEPTION,
            format!(
                "No route info of this topic:{},{}",
                topic,
                FAQUrl::suggest_todo(FAQUrl::NO_TOPIC_ROUTE_INFO)
            ),
        ))
    }

    #[inline]
    pub fn update_fault_item(
        &self,
        broker_name: &str,
        current_latency: u64,
        isolation: bool,
        reachable: bool,
    ) {
        self.mq_fault_strategy.mut_from_ref().update_fault_item(
            broker_name,
            current_latency,
            isolation,
            reachable,
        );
    }

    async fn send_kernel_impl<T>(
        &mut self,
        msg: &mut T,
        mq: &MessageQueue,
        communication_mode: CommunicationMode,
        send_callback: Option<SendMessageCallback>,
        topic_publish_info: Option<&TopicPublishInfo>,
        timeout: u64,
    ) -> Result<Option<SendResult>>
    where
        T: MessageTrait + Clone + Send + Sync,
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
            .find_broker_address_in_publish(broker_name.as_str())
            .await;
        if broker_addr.is_none() {
            self.try_to_find_topic_publish_info(mq.get_topic()).await;
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
                .find_broker_address_in_publish(broker_name.as_str())
                .await;
        }

        if broker_addr.is_none() {
            return Err(MQClientError::MQClientException(
                -1,
                format!("The broker[{}] not exist", broker_name,),
            ));
        }
        let mut broker_addr = broker_addr.unwrap();
        broker_addr = mix_all::broker_vip_channel(
            self.client_config.vip_channel_enabled,
            broker_addr.as_str(),
        );
        //let prev_body = msg.body.clone();
        let batch = msg.as_any().downcast_ref::<MessageBatch>().is_some();
        if !batch {
            MessageClientIDSetter::set_uniq_id(msg);
        }
        let mut topic_with_namespace = false;
        if self.client_config.get_namespace().is_some() {
            msg.set_instance_id(self.client_config.get_namespace().unwrap().as_str());
            topic_with_namespace = true;
        }
        let mut sys_flag = 0i32;
        let mut msg_body_compressed = false;
        if self.try_to_compress_message(msg) {
            sys_flag |= MessageSysFlag::COMPRESSED_FLAG;
            sys_flag |= self.producer_config.compress_type().get_compression_flag();
            msg_body_compressed = true;
        }
        let tran_msg = msg.get_property(MessageConst::PROPERTY_TRANSACTION_PREPARED);
        if let Some(value) = tran_msg {
            let value_ = value.parse().unwrap_or(false);
            if value_ {
                sys_flag |= MessageSysFlag::TRANSACTION_PREPARED_TYPE;
            }
        }

        if self.has_check_forbidden_hook() {
            let check_forbidden_context = CheckForbiddenContext {
                name_srv_addr: self.client_config.get_namesrv_addr(),
                group: Some(self.producer_config.producer_group().to_string()),
                communication_mode: Some(communication_mode),
                broker_addr: Some(broker_addr.clone()),
                message: Some(msg),
                mq: Some(mq),
                unit_mode: self.is_unit_mode(),
                ..Default::default()
            };
            self.execute_check_forbidden_hook(&check_forbidden_context)?;
        }

        let mut send_message_context = if self.has_send_message_hook() {
            let namespace = self.client_config.get_namespace();
            let producer_group = self.producer_config.producer_group().to_string();
            let born_host = self.client_config.client_ip.clone();
            let is_trans = msg.get_property(MessageConst::PROPERTY_TRANSACTION_PREPARED);
            let msg_type_flag = msg
                .get_property(MessageConst::PROPERTY_STARTDE_LIVER_TIME)
                .is_some()
                || msg
                    .get_property(MessageConst::PROPERTY_DELAY_TIME_LEVEL)
                    .is_some();
            let mut send_message_context = SendMessageContext {
                producer: Some(self.clone()),
                producer_group: Some(producer_group),
                communication_mode: Some(communication_mode),
                born_host,
                broker_addr: Some(broker_addr.clone()),
                message: Some(Box::new(msg.clone())),
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

        //build send message request header
        let mut request_header = SendMessageRequestHeader {
            producer_group: self.producer_config.producer_group().to_string(),
            topic: msg.get_topic().to_string(),
            default_topic: self.producer_config.create_topic_key().to_string(),
            default_topic_queue_nums: self.producer_config.default_topic_queue_nums() as i32,
            queue_id: Some(mq.get_queue_id()),
            sys_flag,
            born_timestamp: get_current_millis() as i64,
            flag: msg.get_flag(),
            properties: Some(MessageDecoder::message_properties_to_string(
                msg.get_properties(),
            )),
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

        if request_header
            .topic
            .starts_with(mix_all::RETRY_GROUP_TOPIC_PREFIX)
        {
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

        let send_result = match communication_mode {
            CommunicationMode::Async => {
                if topic_with_namespace {
                    msg.set_topic(
                        NamespaceUtil::without_namespace_with_namespace(
                            msg.get_topic(),
                            self.client_config
                                .get_namespace()
                                .unwrap_or(String::from(""))
                                .as_str(),
                        )
                        .as_str(),
                    );
                }
                let cost_time_sync = (Instant::now() - begin_start_time).as_millis() as u64;
                self.client_instance
                    .as_ref()
                    .unwrap()
                    .get_mq_client_api_impl()
                    .send_message(
                        broker_addr.as_str(),
                        broker_name.as_str(),
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
                    return Err(RemotingTooMuchRequestException(
                        "sendKernelImpl call timeout".to_string(),
                    ));
                }
                self.client_instance
                    .as_ref()
                    .unwrap()
                    .get_mq_client_api_impl()
                    .send_message_simple(
                        broker_addr.as_str(),
                        broker_name.as_str(),
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
                    send_message_context.as_mut().unwrap().send_result = result.clone();
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

    pub fn execute_send_message_hook_before(&mut self, context: &Option<SendMessageContext<'_>>) {
        if self.has_send_message_hook() {
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

    pub fn has_send_message_hook(&self) -> bool {
        !self.send_message_hook_list.is_empty()
    }

    #[inline]
    pub fn has_check_forbidden_hook(&self) -> bool {
        !self.check_forbidden_hook_list.is_empty()
    }

    pub fn execute_check_forbidden_hook(&self, context: &CheckForbiddenContext) -> Result<()> {
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
                        .as_ref()
                        .unwrap()
                        .compress(body, self.producer_config.compress_level());
                    if let Ok(data) = data {
                        //store the compressed data
                        msg.set_compressed_body_mut(Bytes::from(data));
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
        last_broker_name: Option<&str>,
        reset_index: bool,
    ) -> Option<MessageQueue> {
        self.mq_fault_strategy
            .select_one_message_queue(tp_info, last_broker_name, reset_index)
    }

    fn validate_name_server_setting(&self) -> Result<()> {
        let binding = self
            .client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl();
        let ns_list = binding.get_name_server_address_list();
        if ns_list.is_empty() {
            return Err(MQClientError::MQClientException(
                ClientErrorCode::NO_NAME_SERVER_EXCEPTION,
                format!(
                    "No name server address, please set it. {}",
                    FAQUrl::suggest_todo(FAQUrl::NAME_SERVER_ADDR_NOT_EXIST_URL)
                ),
            ));
        }
        Ok(())
    }

    async fn try_to_find_topic_publish_info(&self, topic: &str) -> Option<TopicPublishInfo> {
        let mut write_guard = self.topic_publish_info_table.write().await;
        let mut topic_publish_info = write_guard.get(topic).cloned();
        if topic_publish_info.is_none() || !topic_publish_info.as_ref().unwrap().ok() {
            write_guard.insert(topic.to_string(), TopicPublishInfo::new());
            drop(write_guard);
            self.client_instance
                .as_ref()
                .unwrap()
                .mut_from_ref()
                .update_topic_route_info_from_name_server_topic(topic)
                .await;
            let write_guard = self.topic_publish_info_table.write().await;
            topic_publish_info = write_guard.get(topic).cloned();
        }

        let topic_publish_info_ref = topic_publish_info.as_ref().unwrap();
        if topic_publish_info_ref.have_topic_router_info || topic_publish_info_ref.ok() {
            return topic_publish_info;
        }

        self.client_instance
            .as_ref()
            .unwrap()
            .mut_from_ref()
            .update_topic_route_info_from_name_server_default(
                topic,
                true,
                Some(&self.producer_config),
            )
            .await;
        self.topic_publish_info_table
            .write()
            .await
            .get(topic)
            .cloned()
    }

    fn make_sure_state_ok(&self) -> Result<()> {
        if self.service_state != ServiceState::Running {
            return Err(MQClientError::MQClientException(
                -1,
                format!(
                    "The producer service state not OK, {:?} {}",
                    self.service_state,
                    FAQUrl::suggest_todo(FAQUrl::CLIENT_SERVICE_NOT_OK)
                ),
            ));
        }
        Ok(())
    }

    pub async fn invoke_message_queue_selector<M, T>(
        &mut self,
        msg: &M,
        selector: MessageQueueSelectorFn,
        arg: &T,
        timeout: u64,
    ) -> Result<MessageQueue>
    where
        M: MessageTrait + Clone,
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
                    .parse_publish_message_queues(
                        &topic_publish_info.message_queue_list,
                        &mut self.client_config,
                    );
                let mut user_message = msg.clone();
                let user_topic = NamespaceUtil::without_namespace_with_namespace(
                    user_message.get_topic(),
                    self.client_config
                        .get_namespace()
                        .unwrap_or("".to_string())
                        .as_str(),
                );
                user_message.set_topic(user_topic.as_str());
                let message_queue = selector(&message_queue_list, msg, arg);
                let cost_time = begin_start_time.elapsed().as_millis() as u64;
                if timeout < cost_time {
                    return Err(MQClientError::RemotingTooMuchRequestException(
                        "sendSelectImpl call timeout".to_string(),
                    ));
                }
                if let Some(message_queue) = message_queue {
                    return Ok(message_queue);
                }
                return Err(MQClientError::MQClientException(
                    -1,
                    "select message queue return None.".to_string(),
                ));
            }
        }
        self.validate_name_server_setting();
        Err(MQClientException(
            -1,
            "select message queue return null.".to_string(),
        ))
    }

    pub async fn send_with_selector_timeout<M, T>(
        &mut self,
        msg: M,
        selector: MessageQueueSelectorFn,
        arg: T,
        timeout: u64,
    ) -> Result<SendResult>
    where
        M: MessageTrait + Clone + Send + Sync,
        T: std::any::Any + Sync + Send,
    {
        let result = self
            .send_select_impl(msg, selector, arg, CommunicationMode::Sync, None, timeout)
            .await?;
        Ok(result.expect("send result is none"))
    }

    pub async fn fetch_publish_message_queues(&mut self, topic: &str) -> Result<Vec<MessageQueue>> {
        self.make_sure_state_ok()?;
        let client_instance = self
            .client_instance
            .as_mut()
            .unwrap()
            .mq_client_api_impl
            .clone();
        self.client_instance
            .as_mut()
            .unwrap()
            .mq_admin_impl
            .fetch_publish_message_queues(topic, client_instance, &mut self.client_config)
            .await
    }

    pub async fn request(&mut self, mut msg: Message, timeout: u64) -> Result<Message> {
        let begin_timestamp = Instant::now();
        self.prepare_send_request(&mut msg, timeout).await;
        let correlation_id = msg
            .get_property(MessageConst::PROPERTY_CORRELATION_ID)
            .unwrap();
        let request_response_future = Arc::new(RequestResponseFuture::new(
            correlation_id.clone(),
            timeout,
            None,
        ));
        REQUEST_FUTURE_HOLDER
            .put_request(correlation_id.clone(), request_response_future.clone())
            .await;
        let request_response_future_inner = request_response_future.clone();
        let send_callback = move |result: Option<&SendResult>,
                                  err: Option<&dyn std::error::Error>| {
            if result.is_some() {
                request_response_future_inner.set_send_request_ok(true);
            }
            if let Some(error) = err {
                request_response_future_inner.set_send_request_ok(false);
                request_response_future_inner.put_response_message(None);
                request_response_future_inner.set_cause(Box::new(
                    MQClientError::MQClientException(-1, error.to_string()),
                ));
            }
        };
        let topic = msg.topic.clone();
        self.send_default_impl(
            msg,
            CommunicationMode::Async,
            Some(Arc::new(send_callback)),
            timeout,
        )
        .await?;

        let result = self
            .wait_response(
                topic.as_str(),
                timeout,
                request_response_future,
                begin_timestamp.elapsed().as_millis() as u64,
            )
            .await;

        REQUEST_FUTURE_HOLDER
            .remove_request(correlation_id.as_str())
            .await;
        result
    }

    async fn wait_response(
        &mut self,
        topic: &str,
        timeout: u64,
        request_response_future: Arc<RequestResponseFuture>,
        cost: u64,
    ) -> Result<Message> {
        let response_message = request_response_future
            .wait_response_message(Duration::from_millis(timeout - cost))
            .await;

        if let Some(response_message) = response_message {
            Ok(response_message)
        } else if request_response_future.is_send_request_ok().await {
            Err(RequestTimeoutException(
                ClientErrorCode::REQUEST_TIMEOUT_EXCEPTION,
                format!(
                    "send request message to <{}> OK, but wait reply message timeout, {} ms.",
                    topic, timeout
                ),
            ))
        } else {
            Err(MQClientException(
                -1,
                format!(
                    "send request message to <{}> fail, {}",
                    topic,
                    request_response_future.get_cause().await.unwrap()
                ),
            ))
        }
    }

    async fn prepare_send_request(&mut self, msg: &mut Message, timeout: u64) {
        let correlation_id = CorrelationIdUtil::create_correlation_id();
        let request_client_id = self.client_instance.as_mut().unwrap().client_id.clone();
        MessageAccessor::put_property(
            msg,
            MessageConst::PROPERTY_CORRELATION_ID,
            correlation_id.as_str(),
        );
        MessageAccessor::put_property(
            msg,
            MessageConst::PROPERTY_MESSAGE_REPLY_TO_CLIENT,
            request_client_id.as_str(),
        );
        MessageAccessor::put_property(
            msg,
            MessageConst::PROPERTY_MESSAGE_TTL,
            timeout.to_string().as_str(),
        );
        let guard = self
            .client_instance
            .as_mut()
            .unwrap()
            .topic_route_table
            .read()
            .await;
        let has_route_data = guard.contains_key(msg.get_topic());
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
                warn!(
                    "prepare send request for <{}> cost {} ms",
                    msg.get_topic(),
                    cost
                );
            }
        }
    }

    /*    async fn send_with_selector_timeout_impl<M, T>(
        &mut self,
        mut msg: M,
        selector: MessageQueueSelectorFn,
        arg: T,
        timeout: u64,
        send_callback: Option<SendMessageCallback>,
    ) -> Result<SendResult>
    where
        M: MessageTrait + Clone + Send + Sync,
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
                    .parse_publish_message_queues(
                        &topic_publish_info.message_queue_list,
                        &mut self.client_config,
                    );
                let mut user_message = msg.clone();
                let user_topic = NamespaceUtil::without_namespace_with_namespace(
                    user_message.get_topic(),
                    self.client_config
                        .get_namespace()
                        .unwrap_or("".to_string())
                        .as_str(),
                );
                user_message.set_topic(user_topic.as_str());
                let message_queue = selector(&message_queue_list, &msg, &arg);
                let cost_time = begin_start_time.elapsed().as_millis() as u64;
                if timeout < cost_time {
                    return Err(MQClientError::RemotingTooMuchRequestException(
                        "sendSelectImpl call timeout".to_string(),
                    ));
                }
                if message_queue.is_some() {
                    let result = self
                        .send_kernel_impl(
                            &mut msg,
                            message_queue.as_ref().unwrap(),
                            CommunicationMode::Sync,
                            send_callback,
                            None,
                            timeout - cost_time,
                        )
                        .await?;
                    return Ok(result.unwrap());
                }
                return Err(MQClientError::MQClientException(
                    -1,
                    "select message queue return None.".to_string(),
                ));
            }
        }
        self.validate_name_server_setting()?;
        Err(MQClientError::MQClientException(
            -1,
            "No route info for this topic, ".to_string(),
        ))
    }*/
}

impl MQProducerInner for DefaultMQProducerImpl {
    fn get_publish_topic_list(&self) -> HashSet<String> {
        todo!()
    }

    fn is_publish_topic_need_update(&self, topic: &str) -> bool {
        let handle = Handle::current();
        let topic = topic.to_string();
        let topic_publish_info_table = self.topic_publish_info_table.clone();
        thread::spawn(move || {
            handle.block_on(async move {
                let guard = topic_publish_info_table.read().await;
                let topic_publish_info = guard.get(topic.as_str());
                if topic_publish_info.is_none() {
                    return true;
                }
                !topic_publish_info.unwrap().ok()
            })
        })
        .join()
        .unwrap_or(false)
    }

    fn get_check_listener(&self) -> Arc<Box<dyn TransactionListener>> {
        todo!()
    }

    fn check_transaction_state(
        &self,
        addr: &str,
        msg: &MessageExt,
        check_request_header: &CheckTransactionStateRequestHeader,
    ) {
        todo!()
    }

    fn update_topic_publish_info(&mut self, topic: String, info: Option<TopicPublishInfo>) {
        if topic.is_empty() || info.is_none() {
            return;
        }
        let handle = Handle::current();
        let topic_publish_info_table = self.topic_publish_info_table.clone();
        let _ = thread::spawn(move || {
            handle.block_on(async move {
                let mut write_guard = topic_publish_info_table.write().await;
                write_guard.insert(topic, info.unwrap());
            })
        })
        .join();
    }

    fn is_unit_mode(&self) -> bool {
        self.client_config.unit_mode
    }
}

impl DefaultMQProducerImpl {
    pub async fn start(&mut self) -> Result<()> {
        self.start_with_factory(true).await
    }

    pub async fn start_with_factory(&mut self, start_factory: bool) -> Result<()> {
        match self.service_state {
            ServiceState::CreateJust => {
                self.service_state = ServiceState::StartFailed;
                self.check_config()?;

                if self.producer_config.producer_group() != CLIENT_INNER_PRODUCER_GROUP {
                    self.client_config.change_instance_name_to_pid();
                }

                let client_instance = MQClientManager::get_instance()
                    .get_or_create_mq_client_instance(
                        self.client_config.clone(),
                        self.rpc_hook.clone(),
                    )
                    .await;

                let service_detector = DefaultServiceDetector {
                    client_instance: client_instance.clone(),
                    topic_publish_info_table: self.topic_publish_info_table.clone(),
                };
                let resolver = DefaultResolver {
                    client_instance: client_instance.clone(),
                };
                self.mq_fault_strategy.set_resolver(Box::new(resolver));
                self.mq_fault_strategy
                    .set_service_detector(Box::new(service_detector));
                self.client_instance = Some(client_instance);
                let self_clone = self.clone();
                let register_ok = self
                    .client_instance
                    .as_mut()
                    .unwrap()
                    .register_producer(self.producer_config.producer_group(), self_clone)
                    .await;
                if !register_ok {
                    self.service_state = ServiceState::CreateJust;
                    return Err(MQClientError::MQClientException(
                        -1,
                        format!(
                            "The producer group[{}] has been created before, specify another name \
                             please. {}",
                            self.producer_config.producer_group(),
                            FAQUrl::suggest_todo(FAQUrl::GROUP_NAME_DUPLICATE_URL)
                        ),
                    ));
                }
                if start_factory {
                    Box::pin(self.client_instance.as_mut().unwrap().start()).await?;
                    //self.client_instance.as_mut().unwrap().start().await;
                }

                self.init_topic_route();
                self.mq_fault_strategy.start_detector();
                self.service_state = ServiceState::Running;
            }
            ServiceState::Running => {
                return Err(MQClientError::MQClientException(
                    -1,
                    "The producer service state is Running".to_string(),
                ));
            }
            ServiceState::ShutdownAlready => {
                return Err(MQClientError::MQClientException(
                    -1,
                    "The producer service state is ShutdownAlready".to_string(),
                ));
            }
            ServiceState::StartFailed => {
                return Err(MQClientError::MQClientException(
                    -1,
                    format!(
                        "The producer service state not OK, maybe started once,{:?},{}",
                        self.service_state,
                        FAQUrl::suggest_todo(FAQUrl::CLIENT_SERVICE_NOT_OK)
                    ),
                ));
            }
        }
        Ok(())
    }

    pub fn register_end_transaction_hook(&mut self, hook: impl EndTransactionHook) {
        todo!()
    }

    pub fn register_send_message_hook(&mut self, hook: impl SendMessageHook) {
        todo!()
    }

    #[inline]
    fn check_config(&self) -> Result<()> {
        Validators::check_group(self.producer_config.producer_group())?;
        if self.producer_config.producer_group() == DEFAULT_PRODUCER_GROUP {
            return Err(MQClientError::MQClientException(
                -1,
                format!(
                    "The specified group name[{}] is equal to default group, please specify \
                     another one.",
                    DEFAULT_PRODUCER_GROUP
                ),
            ));
        }
        Ok(())
    }

    fn init_topic_route(&mut self) {}

    #[inline]
    pub fn set_send_latency_fault_enable(&mut self, send_latency_fault_enable: bool) {
        self.mq_fault_strategy
            .set_send_latency_fault_enable(send_latency_fault_enable);
    }
}

struct DefaultServiceDetector {
    client_instance: ArcRefCellWrapper<MQClientInstance>,
    topic_publish_info_table: Arc<RwLock<HashMap<String /* topic */, TopicPublishInfo>>>,
}

impl ServiceDetector for DefaultServiceDetector {
    fn detect(&self, endpoint: &str, timeout_millis: u64) -> bool {
        todo!()
    }
}

struct DefaultResolver {
    client_instance: ArcRefCellWrapper<MQClientInstance>,
}

impl Resolver for DefaultResolver {
    fn resolve(&self, name: &str) -> String {
        todo!()
    }
}
