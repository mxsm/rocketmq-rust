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
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use rand::thread_rng;
use rand::Rng;
use rocketmq_common::common::base::service_state::ServiceState;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::common::message::message_single::MessageExt;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::CLIENT_INNER_PRODUCER_GROUP;
use rocketmq_common::common::mix_all::DEFAULT_PRODUCER_GROUP;
use rocketmq_common::common::FAQUrl;
use rocketmq_common::ArcRefCellWrapper;
use rocketmq_remoting::protocol::header::check_transaction_state_request_header::CheckTransactionStateRequestHeader;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::runtime::RPCHook;
use tokio::runtime::Handle;
use tokio::sync::RwLock;
use tokio::sync::Semaphore;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::base::validators::Validators;
use crate::common::client_error_code::ClientErrorCode;
use crate::error::MQClientError;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::hook::check_forbidden_hook::CheckForbiddenHook;
use crate::hook::end_transaction_hook::EndTransactionHook;
use crate::hook::send_message_hook::SendMessageHook;
use crate::implementation::communication_mode::CommunicationMode;
use crate::implementation::mq_client_manager::MQClientManager;
use crate::latency::mq_fault_strategy::MQFaultStrategy;
use crate::latency::resolver::Resolver;
use crate::latency::service_detector::ServiceDetector;
use crate::producer::default_mq_producer::ProducerConfig;
use crate::producer::producer_impl::mq_producer_inner::MQProducerInner;
use crate::producer::producer_impl::topic_publish_info::TopicPublishInfo;
use crate::producer::send_callback::SendCallback;
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
    end_transaction_hook_list: ArcRefCellWrapper<Vec<Box<dyn EndTransactionHook>>>,
    check_forbidden_hook_list: ArcRefCellWrapper<Vec<Box<dyn CheckForbiddenHook>>>,
    rpc_hook: Option<Arc<Box<dyn RPCHook>>>,
    service_state: ServiceState,
    client_instance: Option<ArcRefCellWrapper<MQClientInstance>>,
    mq_fault_strategy: ArcRefCellWrapper<MQFaultStrategy>,
    semaphore_async_send_num: Arc<Semaphore>,
    semaphore_async_send_size: Arc<Semaphore>,
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
            end_transaction_hook_list: ArcRefCellWrapper::new(vec![]),
            check_forbidden_hook_list: ArcRefCellWrapper::new(vec![]),
            rpc_hook: None,
            service_state: ServiceState::CreateJust,
            client_instance: None,
            mq_fault_strategy: ArcRefCellWrapper::new(MQFaultStrategy::new(client_config)),
            semaphore_async_send_num: Arc::new(semaphore_async_send_num),
            semaphore_async_send_size: Arc::new(semaphore_async_send_size),
        }
    }

    pub async fn send(&mut self, msg: Message, timeout: u64) -> Result<Option<SendResult>> {
        self.send_default_impl(msg, CommunicationMode::Sync, None, timeout)
            .await
    }

    async fn send_default_impl(
        &mut self,
        mut msg: Message,
        communication_mode: CommunicationMode,
        send_callback: Option<Arc<Box<dyn SendCallback>>>,
        timeout: u64,
    ) -> Result<Option<SendResult>> {
        self.make_sure_state_ok()?;
        let invoke_id = thread_rng().gen::<u64>();
        let begin_timestamp_first = Instant::now();
        let mut begin_timestamp_prev = begin_timestamp_first;
        let mut end_timestamp = begin_timestamp_first;
        let topic = msg.topic().to_string();
        let topic_publish_info = self.try_to_find_topic_publish_info(topic.as_str()).await;
        if let Some(topic_publish_info) = topic_publish_info {
            if topic_publish_info.ok() {
                let mut call_timeout = false;
                let mut mq: Option<MessageQueue> = None;
                let mut exception: Option<MQClientError> = None;
                let mut send_result: Option<SendResult> = None;
                let times_total = if communication_mode == CommunicationMode::Async {
                    self.producer_config.retry_times_when_send_failed() + 1
                } else {
                    1
                };
                //let mut times = 0u32;
                let mut brokers_sent = Vec::<String>::with_capacity(times_total as usize);
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
                            msg.topic =
                                NamespaceUtil::wrap_namespace(namespace.as_str(), topic.as_str());
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
                                &msg,
                                mq.as_ref().unwrap(),
                                communication_mode,
                                send_callback.clone(),
                                &topic_publish_info,
                                timeout - cost_time,
                            )
                            .await;

                        match result_inner {
                            Ok(result) => {
                                send_result = Some(result);
                                match communication_mode {
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
                                        return Ok(send_result);
                                    }
                                    CommunicationMode::Async => {
                                        return Ok(None);
                                    }
                                    CommunicationMode::Oneway => {
                                        return Ok(None);
                                    }
                                }
                            }
                            Err(err) => match err {
                                MQClientError::MQClientException(_, _) => {
                                    end_timestamp = Instant::now();
                                    let elapsed =
                                        (end_timestamp - begin_timestamp_prev).as_millis() as i64;
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
                                    warn!("{:?}", msg);
                                    exception = Some(err);
                                    continue;
                                }
                                MQClientError::MQBrokerException(code, _, _) => {
                                    end_timestamp = Instant::now();
                                    let elapsed =
                                        (end_timestamp - begin_timestamp_prev).as_millis() as i64;
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
                                MQClientError::RequestTimeoutException(_, _) => {
                                    return Err(err);
                                }
                                MQClientError::OffsetNotFoundException(_, _, _) => {
                                    return Err(err);
                                }
                                MQClientError::RemotingException(_) => {
                                    end_timestamp = Instant::now();
                                    let elapsed =
                                        (end_timestamp - begin_timestamp_prev).as_millis() as i64;
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
                            },
                        }
                    } else {
                        break;
                    }
                }
            }
        }
        self.validate_name_server_setting()?;
        Err(MQClientError::MQClientException(
            ClientErrorCode::NOT_FOUND_TOPIC_EXCEPTION,
            format!(
                "No route info of this topic:{},{}",
                topic,
                FAQUrl::suggest_todo(FAQUrl::NO_TOPIC_ROUTE_INFO)
            ),
        ))
    }

    pub fn update_fault_item(
        &self,
        broker_name: &str,
        current_latency: i64,
        isolation: bool,
        reachable: bool,
    ) {
        /*self.mq_fault_strategy.mut_from_ref().update_fault_item(
            broker_name,
            current_latency,
            isolation,
        );*/
    }

    async fn send_kernel_impl(
        &mut self,
        msg: &Message,
        mq: &MessageQueue,
        communication_mode: CommunicationMode,
        send_callback: Option<Arc<Box<dyn SendCallback>>>,
        topic_publish_info: &TopicPublishInfo,
        timeout: u64,
    ) -> Result<SendResult> {
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
        let prev_body = msg.body.clone();

        unimplemented!()
    }

    pub fn select_one_message_queue(
        &self,
        tp_info: &TopicPublishInfo,
        last_broker_name: Option<&str>,
        reset_index: bool,
    ) -> Option<MessageQueue> {
        None
    }

    fn validate_name_server_setting(&self) -> Result<()> {
        let ns_list = self
            .client_instance
            .as_ref()
            .unwrap()
            .get_mq_client_api_impl()
            .get_name_server_address_list();
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
                self.mq_fault_strategy.set_resolver(resolver);
                self.mq_fault_strategy
                    .set_service_detector(service_detector);
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
