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
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Instant;

use lazy_static::lazy_static;
use rocketmq_common::common::message::message_batch::MessageBatch;
use rocketmq_common::common::message::message_client_id_setter::MessageClientIDSetter;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::namesrv::default_top_addressing::DefaultTopAddressing;
use rocketmq_common::common::namesrv::name_server_update_callback::NameServerUpdateCallback;
use rocketmq_common::common::namesrv::top_addressing::TopAddressing;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::ArcRefCellWrapper;
use rocketmq_remoting::clients::rocketmq_default_impl::RocketmqDefaultClient;
use rocketmq_remoting::clients::RemotingClient;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::remoting::RemotingService;
use rocketmq_remoting::request_processor::default_request_processor::DefaultRemotingRequestProcessor;
use rocketmq_remoting::runtime::config::client_config::TokioClientConfig;
use rocketmq_remoting::runtime::RPCHook;
use tracing::error;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::error::MQClientError;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::hook::send_message_context::SendMessageContext;
use crate::implementation::client_remoting_processor::ClientRemotingProcessor;
use crate::implementation::communication_mode::CommunicationMode;
use crate::producer::producer_impl::default_mq_producer_impl::DefaultMQProducerImpl;
use crate::producer::producer_impl::topic_publish_info::TopicPublishInfo;
use crate::producer::send_callback::SendMessageCallback;
use crate::producer::send_result::SendResult;
use crate::producer::send_status::SendStatus;
use crate::Result;

lazy_static! {
    static ref sendSmartMsg: bool = std::env::var("org.apache.rocketmq.client.sendSmartMsg")
        .unwrap_or("false".to_string())
        .parse()
        .unwrap_or(false);
}

pub struct MQClientAPIImpl {
    remoting_client: RocketmqDefaultClient,
    top_addressing: Box<dyn TopAddressing>,
    client_remoting_processor: ClientRemotingProcessor,
    name_srv_addr: Option<String>,
    client_config: ClientConfig,
}

impl NameServerUpdateCallback for MQClientAPIImpl {
    fn on_name_server_address_changed(&self, namesrv_address: Option<String>) -> String {
        unimplemented!("on_name_server_address_changed")
    }
}

impl MQClientAPIImpl {
    pub fn new(
        tokio_client_config: Arc<TokioClientConfig>,
        client_remoting_processor: ClientRemotingProcessor,
        rpc_hook: Option<Arc<Box<dyn RPCHook>>>,
        client_config: ClientConfig,
    ) -> Self {
        let mut default_client =
            RocketmqDefaultClient::new(tokio_client_config, DefaultRemotingRequestProcessor);
        if let Some(hook) = rpc_hook {
            default_client.register_rpc_hook(hook);
        }

        MQClientAPIImpl {
            remoting_client: default_client,
            top_addressing: Box::new(DefaultTopAddressing::new(
                mix_all::get_ws_addr(),
                client_config.unit_name.clone(),
            )),
            client_remoting_processor,
            name_srv_addr: None,
            client_config,
        }
    }

    pub async fn start(&self) {
        self.remoting_client.start().await;
    }

    pub async fn fetch_name_server_addr(&mut self) -> Option<String> {
        let addrs = self.top_addressing.fetch_ns_addr();
        if addrs.is_some() && !addrs.as_ref().unwrap().is_empty() {
            let mut notify = false;
            if let Some(addr) = self.name_srv_addr.as_mut() {
                let addrs = addrs.unwrap();
                if addr != addrs.as_str() {
                    *addr = addrs.clone();
                    notify = true;
                }
            }
            if notify {
                let name_srv = self.name_srv_addr.as_ref().unwrap().as_str();
                self.update_name_server_address_list(name_srv).await;
                return Some(name_srv.to_string());
            }
        }

        self.name_srv_addr.clone()
    }

    pub async fn update_name_server_address_list(&self, addrs: &str) {
        let addr_vec = addrs
            .split(";")
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        self.remoting_client
            .update_name_server_address_list(addr_vec)
            .await;
    }

    #[inline]
    pub async fn get_default_topic_route_info_from_name_server(
        &self,
        timeout_millis: u64,
    ) -> Result<Option<TopicRouteData>> {
        self.get_topic_route_info_from_name_server_detail(
            TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC,
            timeout_millis,
            false,
        )
        .await
    }

    #[inline]
    pub async fn get_topic_route_info_from_name_server(
        &self,
        topic: &str,
        timeout_millis: u64,
    ) -> Result<Option<TopicRouteData>> {
        self.get_topic_route_info_from_name_server_detail(topic, timeout_millis, true)
            .await
    }

    #[inline]
    pub async fn get_topic_route_info_from_name_server_detail(
        &self,
        topic: &str,
        timeout_millis: u64,
        allow_topic_not_exist: bool,
    ) -> Result<Option<TopicRouteData>> {
        let request_header = GetRouteInfoRequestHeader {
            topic: topic.to_string(),
            accept_standard_json_only: None,
            topic_request_header: None,
        };
        let request = RemotingCommand::create_request_command(
            RequestCode::GetRouteinfoByTopic,
            request_header,
        );
        let response = self
            .remoting_client
            .invoke_async(None, request, timeout_millis)
            .await;
        match response {
            Ok(result) => {
                let code = result.code();
                let response_code = ResponseCode::from(code);
                match response_code {
                    ResponseCode::Success => {
                        let body = result.body();
                        if body.is_some() && !body.as_ref().unwrap().is_empty() {
                            let route_data =
                                TopicRouteData::decode(body.as_ref().unwrap().as_ref());
                            if let Ok(data) = route_data {
                                return Ok(Some(data));
                            }
                        }
                    }
                    ResponseCode::TopicNotExist => {
                        if allow_topic_not_exist {
                            warn!(
                                "get Topic [{}] RouteInfoFromNameServer is not exist value",
                                topic
                            );
                        }
                    }
                    _ => {
                        return Err(MQClientError::MQClientException(
                            code,
                            result.remark().cloned().unwrap_or_default(),
                        ))
                    }
                }
                return Err(MQClientError::MQClientException(
                    code,
                    result.remark().cloned().unwrap_or_default(),
                ));
            }
            Err(err) => Err(MQClientError::RemotingException(err)),
        }
    }

    pub fn get_name_server_address_list(&self) -> &[String] {
        self.remoting_client.get_name_server_address_list()
    }

    pub async fn send_message<T>(
        &mut self,
        addr: &str,
        broker_name: &str,
        msg: &mut T,
        request_header: SendMessageRequestHeader,
        timeout_millis: u64,
        communication_mode: CommunicationMode,
        send_callback: Option<SendMessageCallback>,
        topic_publish_info: Option<&TopicPublishInfo>,
        instance: Option<ArcRefCellWrapper<MQClientInstance>>,
        retry_times_when_send_failed: u32,
        context: &mut Option<SendMessageContext<'_>>,
        producer: &DefaultMQProducerImpl,
    ) -> Result<Option<SendResult>>
    where
        T: MessageTrait,
    {
        let begin_start_time = Instant::now();
        let msg_type = msg.get_property(MessageConst::PROPERTY_MESSAGE_TYPE);
        let is_reply = msg_type.is_some() && msg_type.unwrap() == mix_all::REPLY_MESSAGE_FLAG;
        let mut request = if is_reply {
            if *sendSmartMsg {
                let request_header_v2 =
                    SendMessageRequestHeaderV2::create_send_message_request_header_v2(
                        &request_header,
                    );
                RemotingCommand::create_request_command(
                    RequestCode::SendReplyMessageV2,
                    request_header_v2,
                )
            } else {
                RemotingCommand::create_request_command(
                    RequestCode::SendReplyMessage,
                    request_header,
                )
            }
        } else {
            let is_batch_message = msg.as_any().downcast_ref::<MessageBatch>().is_some();
            if *sendSmartMsg || is_batch_message {
                let request_header_v2 =
                    SendMessageRequestHeaderV2::create_send_message_request_header_v2(
                        &request_header,
                    );
                let request_code = if is_batch_message {
                    RequestCode::SendBatchMessage
                } else {
                    RequestCode::SendMessageV2
                };
                RemotingCommand::create_request_command(request_code, request_header_v2)
            } else {
                RemotingCommand::create_request_command(RequestCode::SendMessage, request_header)
            }
        };

        // if compressed_body is not None, set request body to compressed_body
        if msg.get_compressed_body_mut().is_some() {
            let compressed_body = std::mem::take(msg.get_compressed_body_mut());
            request.set_body_mut_ref(compressed_body);
        } else {
            request.set_body_mut_ref(msg.get_body().cloned());
        }
        match communication_mode {
            CommunicationMode::Sync => {
                let cost_time_sync = (Instant::now() - begin_start_time).as_millis() as u64;
                if cost_time_sync > timeout_millis {
                    return Err(MQClientError::RemotingTooMuchRequestException(
                        "sendMessage call timeout".to_string(),
                    ));
                }
                let result = self
                    .send_message_sync(
                        addr,
                        broker_name,
                        msg,
                        timeout_millis - cost_time_sync,
                        request,
                    )
                    .await?;
                Ok(Some(result))
            }
            CommunicationMode::Async => {
                let times = AtomicU32::new(0);
                let cost_time_sync = (Instant::now() - begin_start_time).as_millis() as u64;
                if cost_time_sync > timeout_millis {
                    return Err(MQClientError::RemotingTooMuchRequestException(
                        "sendMessage call timeout".to_string(),
                    ));
                }
                Box::pin(self.send_message_async(
                    addr,
                    broker_name,
                    msg,
                    timeout_millis,
                    request,
                    send_callback,
                    topic_publish_info,
                    instance,
                    retry_times_when_send_failed,
                    &times,
                    context,
                    producer,
                ))
                .await;
                Ok(None)
            }
            CommunicationMode::Oneway => {
                self.remoting_client
                    .invoke_oneway(addr.to_string(), request, timeout_millis)
                    .await;
                Ok(None)
            }
        }
    }

    pub async fn send_message_simple<T>(
        &mut self,
        addr: &str,
        broker_name: &str,
        msg: &mut T,
        request_header: SendMessageRequestHeader,
        timeout_millis: u64,
        communication_mode: CommunicationMode,
        context: &mut Option<SendMessageContext<'_>>,
        producer: &DefaultMQProducerImpl,
    ) -> Result<Option<SendResult>>
    where
        T: MessageTrait,
    {
        self.send_message(
            addr,
            broker_name,
            msg,
            request_header,
            timeout_millis,
            communication_mode,
            None,
            None,
            None,
            0,
            context,
            producer,
        )
        .await
    }

    async fn send_message_sync<T>(
        &mut self,
        addr: &str,
        broker_name: &str,
        msg: &T,
        timeout_millis: u64,
        request: RemotingCommand,
    ) -> Result<SendResult>
    where
        T: MessageTrait,
    {
        let response = self
            .remoting_client
            .invoke_async(Some(addr.to_string()), request, timeout_millis)
            .await?;
        self.process_send_response(broker_name, msg, &response, addr)
    }

    async fn send_message_async<T: MessageTrait>(
        &mut self,
        addr: &str,
        broker_name: &str,
        msg: &T,
        timeout_millis: u64,
        request: RemotingCommand,
        send_callback: Option<SendMessageCallback>,
        topic_publish_info: Option<&TopicPublishInfo>,
        instance: Option<ArcRefCellWrapper<MQClientInstance>>,
        retry_times_when_send_failed: u32,
        times: &AtomicU32,
        context: &mut Option<SendMessageContext<'_>>,
        producer: &DefaultMQProducerImpl,
    ) {
        let begin_start_time = Instant::now();
        let result = self
            .remoting_client
            .invoke_async(Some(addr.to_string()), request.clone(), timeout_millis)
            .await;
        match result {
            Ok(response) => {
                let cost_time = (Instant::now() - begin_start_time).as_millis() as u64;
                if send_callback.is_none() {
                    let send_result = self.process_send_response(broker_name, msg, &response, addr);
                    if let Ok(result) = send_result {
                        if context.is_some() {
                            context.as_mut().unwrap().send_result = Some(result.clone());
                            producer.execute_send_message_hook_after(context);
                        }
                    }
                    let duration = (Instant::now() - begin_start_time).as_millis() as u64;
                    producer.update_fault_item(broker_name, duration, false, true);
                    return;
                }
                let send_result = self.process_send_response(broker_name, msg, &response, addr);
                match send_result {
                    Ok(result) => {
                        if context.is_some() {
                            context.as_mut().unwrap().send_result = Some(result.clone());
                            producer.execute_send_message_hook_after(context);
                        }
                        let duration = (Instant::now() - begin_start_time).as_millis() as u64;
                        send_callback.as_ref().unwrap()(Some(&result), None);
                        producer.update_fault_item(broker_name, duration, false, true);
                    }
                    Err(err) => {
                        let duration = (Instant::now() - begin_start_time).as_millis() as u64;
                        producer.update_fault_item(broker_name, duration, true, true);
                        Box::pin(self.on_exception_impl(
                            broker_name,
                            msg,
                            duration,
                            request,
                            send_callback,
                            topic_publish_info,
                            instance,
                            retry_times_when_send_failed,
                            times,
                            err,
                            context,
                            false,
                            producer,
                        ))
                        .await;
                    }
                }
            }
            Err(err) => {
                error!("send message async error: {:?}", err);
            }
        }
    }

    fn process_send_response<T>(
        &mut self,
        broker_name: &str,
        msg: &T,
        response: &RemotingCommand,
        addr: &str,
    ) -> Result<SendResult>
    where
        T: MessageTrait,
    {
        let response_code = ResponseCode::from(response.code());
        let send_status = match response_code {
            ResponseCode::FlushDiskTimeout => SendStatus::FlushDiskTimeout,
            ResponseCode::FlushSlaveTimeout => SendStatus::FlushSlaveTimeout,
            ResponseCode::SlaveNotAvailable => SendStatus::SlaveNotAvailable,
            ResponseCode::Success => SendStatus::SendOk,
            _ => {
                return Err(MQClientError::MQBrokerException(
                    response.code(),
                    response.remark().map_or("".to_string(), |s| s.to_string()),
                    addr.to_string(),
                ))
            }
        };
        let response_header = response
            .decode_command_custom_header_fast::<SendMessageResponseHeader>()
            .unwrap();
        let mut topic = msg.get_topic().to_string();
        let namespace = self.client_config.get_namespace();
        if namespace.is_some() && !namespace.as_ref().unwrap().is_empty() {
            topic = NamespaceUtil::without_namespace_with_namespace(
                topic.as_str(),
                namespace.as_ref().unwrap().as_str(),
            );
        }
        let message_queue =
            MessageQueue::from_parts(topic.as_str(), broker_name, response_header.queue_id());
        let mut uniq_msg_id = MessageClientIDSetter::get_uniq_id(msg);
        let msgs = msg.as_any().downcast_ref::<MessageBatch>();
        if msgs.is_some() && response_header.batch_uniq_id().is_none() {
            let mut sb = String::new();
            for msg in msgs.unwrap().messages.as_ref().unwrap().iter() {
                sb.push_str(if sb.is_empty() { "" } else { "," });
                sb.push_str(MessageClientIDSetter::get_uniq_id(msg).unwrap().as_str());
            }
            uniq_msg_id = Some(sb);
        }

        let region_id = response
            .ext_fields()
            .unwrap()
            .get(MessageConst::PROPERTY_MSG_REGION)
            .map_or(mix_all::DEFAULT_TRACE_REGION_ID.to_string(), |s| {
                s.to_string()
            });
        let trace_on = response
            .ext_fields()
            .unwrap()
            .get(MessageConst::PROPERTY_TRACE_SWITCH)
            .map_or(false, |s| s.parse().unwrap_or(false));
        let send_result = SendResult {
            send_status,
            msg_id: uniq_msg_id,
            offset_msg_id: Some(response_header.msg_id().to_string()),
            message_queue: Some(message_queue),
            queue_offset: response_header.queue_offset() as u64,
            transaction_id: response_header.transaction_id().map(|s| s.to_string()),
            region_id: Some(region_id),
            trace_on,
            ..Default::default()
        };

        Ok(send_result)
    }

    async fn on_exception_impl<T: MessageTrait>(
        &mut self,
        broker_name: &str,
        msg: &T,
        timeout_millis: u64,
        mut request: RemotingCommand,
        send_callback: Option<SendMessageCallback>,
        topic_publish_info: Option<&TopicPublishInfo>,
        instance: Option<ArcRefCellWrapper<MQClientInstance>>,
        times_total: u32,
        cur_times: &AtomicU32,
        e: MQClientError,
        context: &mut Option<SendMessageContext<'_>>,
        need_retry: bool,
        producer: &DefaultMQProducerImpl,
    ) {
        let tmp = cur_times.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        if need_retry && tmp < times_total {
            let mut retry_broker_name = broker_name.to_string();
            if let Some(topic_publish_info) = topic_publish_info {
                let mq_chosen =
                    producer.select_one_message_queue(topic_publish_info, Some(broker_name), false);
                retry_broker_name = instance
                    .as_ref()
                    .unwrap()
                    .get_broker_name_from_message_queue(mq_chosen.as_ref().unwrap())
                    .await;
            }
            let addr = instance
                .as_ref()
                .unwrap()
                .find_broker_address_in_publish(retry_broker_name.as_str())
                .await
                .unwrap();
            warn!(
                "async send msg by retry {} times. topic={}, brokerAddr={}, brokerName={}",
                tmp,
                msg.get_topic(),
                addr,
                retry_broker_name
            );
            request.set_opaque_mut(RemotingCommand::create_new_request_id());
            Box::pin(self.send_message_async(
                addr.as_str(),
                retry_broker_name.as_str(),
                msg,
                timeout_millis,
                request,
                send_callback,
                topic_publish_info,
                instance,
                times_total,
                cur_times,
                context,
                producer,
            ))
            .await;
        } else if context.is_some() {
            context.as_mut().unwrap().exception = Some(Arc::new(Box::new(e)));
            producer.execute_send_message_hook_after(context);
        }
    }
}
