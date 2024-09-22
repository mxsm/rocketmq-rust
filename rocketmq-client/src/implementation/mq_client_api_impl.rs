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

use bytes::Bytes;
use lazy_static::lazy_static;
use rocketmq_common::common::message::message_batch::MessageBatch;
use rocketmq_common::common::message::message_client_id_setter::MessageClientIDSetter;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::namesrv::default_top_addressing::DefaultTopAddressing;
use rocketmq_common::common::namesrv::name_server_update_callback::NameServerUpdateCallback;
use rocketmq_common::common::namesrv::top_addressing::TopAddressing;
use rocketmq_common::common::sys_flag::pull_sys_flag::PullSysFlag;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::ArcRefCellWrapper;
use rocketmq_remoting::base::connection_net_event::ConnectionNetEvent;
use rocketmq_remoting::clients::rocketmq_default_impl::RocketmqDefaultClient;
use rocketmq_remoting::clients::RemotingClient;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::body::check_client_request_body::CheckClientRequestBody;
use rocketmq_remoting::protocol::body::get_consumer_listby_group_response_body::GetConsumerListByGroupResponseBody;
use rocketmq_remoting::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_remoting::protocol::header::consumer_send_msg_back_request_header::ConsumerSendMsgBackRequestHeader;
use rocketmq_remoting::protocol::header::get_consumer_listby_group_request_header::GetConsumerListByGroupRequestHeader;
use rocketmq_remoting::protocol::header::heartbeat_request_header::HeartbeatRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_remoting::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_remoting::protocol::header::pull_message_response_header::PullMessageResponseHeader;
use rocketmq_remoting::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
use rocketmq_remoting::protocol::header::query_consumer_offset_response_header::QueryConsumerOffsetResponseHeader;
use rocketmq_remoting::protocol::header::unregister_client_request_header::UnregisterClientRequestHeader;
use rocketmq_remoting::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
use rocketmq_remoting::protocol::heartbeat::heartbeat_data::HeartbeatData;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::remoting::RemotingService;
use rocketmq_remoting::rpc::rpc_request_header::RpcRequestHeader;
use rocketmq_remoting::runtime::config::client_config::TokioClientConfig;
use rocketmq_remoting::runtime::RPCHook;
use tracing::error;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::consumer::consumer_impl::pull_request_ext::PullResultExt;
use crate::consumer::pull_callback::PullCallback;
use crate::consumer::pull_result::PullResult;
use crate::consumer::pull_status::PullStatus;
use crate::error::MQClientError;
use crate::error::MQClientError::MQBrokerError;
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
    remoting_client: RocketmqDefaultClient<ClientRemotingProcessor>,
    top_addressing: Box<dyn TopAddressing>,
    // client_remoting_processor: ClientRemotingProcessor,
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
        tx: Option<tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
    ) -> Self {
        let mut default_client =
            RocketmqDefaultClient::new_with_cl(tokio_client_config, client_remoting_processor, tx);
        if let Some(hook) = rpc_hook {
            default_client.register_rpc_hook(hook);
        }

        MQClientAPIImpl {
            remoting_client: default_client,
            top_addressing: Box::new(DefaultTopAddressing::new(
                mix_all::get_ws_addr(),
                client_config.unit_name.clone(),
            )),
            //client_remoting_processor,
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
                        return Err(MQClientError::MQClientErr(
                            code,
                            result.remark().cloned().unwrap_or_default(),
                        ))
                    }
                }
                return Err(MQClientError::MQClientErr(
                    code,
                    result.remark().cloned().unwrap_or_default(),
                ));
            }
            Err(err) => Err(MQClientError::RemotingError(err)),
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
                    return Err(MQClientError::RemotingTooMuchRequestError(
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
                    return Err(MQClientError::RemotingTooMuchRequestError(
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
                return Err(MQClientError::MQBrokerError(
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

    pub async fn send_heartbeat(
        &mut self,
        addr: &str,
        heartbeat_data: &HeartbeatData,
        timeout_millis: u64,
    ) -> Result<i32> {
        let request = RemotingCommand::create_request_command(
            RequestCode::HeartBeat,
            HeartbeatRequestHeader::default(),
        )
        .set_language(self.client_config.language)
        .set_body(Some(Bytes::from(heartbeat_data.encode())));
        let response = self
            .remoting_client
            .invoke_async(Some(addr.to_string()), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(response.version());
        }
        Err(MQClientError::MQBrokerError(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string(),
        ))
    }

    pub async fn check_client_in_broker(
        &mut self,
        broker_addr: &str,
        consumer_group: &str,
        client_id: &str,
        subscription_data: &SubscriptionData,
        timeout_millis: u64,
    ) -> Result<()> {
        let mut request = RemotingCommand::create_remoting_command(RequestCode::CheckClientConfig);
        let body = CheckClientRequestBody::new(
            client_id.to_string(),
            consumer_group.to_string(),
            subscription_data.clone(),
        );
        request.set_body_mut_ref(Some(body.encode()));
        let response = self
            .remoting_client
            .invoke_async(
                Some(mix_all::broker_vip_channel(
                    self.client_config.vip_channel_enabled,
                    broker_addr,
                )),
                request,
                timeout_millis,
            )
            .await?;
        if ResponseCode::from(response.code()) != ResponseCode::Success {
            return Err(MQClientError::MQClientErr(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string()),
            ));
        }
        Ok(())
    }

    pub async fn get_consumer_id_list_by_group(
        &mut self,
        addr: &str,
        consumer_group: &str,
        timeout_millis: u64,
    ) -> Result<Vec<String>> {
        let request_header = GetConsumerListByGroupRequestHeader {
            consumer_group: consumer_group.to_string(),
            rpc: None,
        };
        let request = RemotingCommand::create_request_command(
            RequestCode::GetConsumerListByGroup,
            request_header,
        );
        let response = self
            .remoting_client
            .invoke_async(
                Some(mix_all::broker_vip_channel(
                    self.client_config.vip_channel_enabled,
                    addr,
                )),
                request,
                timeout_millis,
            )
            .await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let body = response.body();
                if let Some(body) = response.body() {
                    return match GetConsumerListByGroupResponseBody::decode(body) {
                        Ok(value) => Ok(value.consumer_id_list),
                        Err(e) => Err(MQClientError::MQClientErr(
                            -1,
                            response.remark().map_or("".to_string(), |s| s.to_string()),
                        )),
                    };
                }
            }
            _ => {
                return Err(MQClientError::MQBrokerError(
                    response.code(),
                    response.remark().map_or("".to_string(), |s| s.to_string()),
                    addr.to_string(),
                ));
            }
        }
        Err(MQClientError::MQBrokerError(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string(),
        ))
    }

    pub async fn update_consumer_offset_oneway(
        &mut self,
        addr: &str,
        request_header: UpdateConsumerOffsetRequestHeader,
        timeout_millis: u64,
    ) -> Result<()> {
        let request = RemotingCommand::create_request_command(
            RequestCode::UpdateConsumerOffset,
            request_header,
        );
        self.remoting_client
            .invoke_oneway(
                mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr),
                request,
                timeout_millis,
            )
            .await;
        Ok(())
    }

    pub async fn update_consumer_offset(
        &mut self,
        addr: &str,
        request_header: UpdateConsumerOffsetRequestHeader,
        timeout_millis: u64,
    ) -> Result<()> {
        let request = RemotingCommand::create_request_command(
            RequestCode::UpdateConsumerOffset,
            request_header,
        );
        let response = self
            .remoting_client
            .invoke_async(Some(addr.to_string()), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) != ResponseCode::Success {
            Err(MQClientError::MQBrokerError(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string()),
                addr.to_string(),
            ))
        } else {
            Ok(())
        }
    }

    pub async fn query_consumer_offset(
        &mut self,
        addr: &str,
        request_header: QueryConsumerOffsetRequestHeader,
        timeout_millis: u64,
    ) -> Result<i64> {
        let request = RemotingCommand::create_request_command(
            RequestCode::QueryConsumerOffset,
            request_header,
        );
        let response = self
            .remoting_client
            .invoke_async(
                Some(mix_all::broker_vip_channel(
                    self.client_config.vip_channel_enabled,
                    addr,
                )),
                request,
                timeout_millis,
            )
            .await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let response_header = response
                    .decode_command_custom_header::<QueryConsumerOffsetResponseHeader>()
                    .unwrap();
                return Ok(response_header.offset.unwrap());
            }
            ResponseCode::QueryNotFound => {
                return Err(MQClientError::OffsetNotFoundError(
                    response.code(),
                    response.remark().map_or("".to_string(), |s| s.to_string()),
                    addr.to_string(),
                ))
            }
            _ => {}
        }
        Err(MQClientError::MQBrokerError(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string(),
        ))
    }

    pub async fn pull_message<PCB>(
        mut this: ArcRefCellWrapper<Self>,
        addr: String,
        request_header: PullMessageRequestHeader,
        timeout_millis: u64,
        communication_mode: CommunicationMode,
        pull_callback: PCB,
    ) -> Result<Option<PullResultExt>>
    where
        PCB: PullCallback + 'static,
    {
        let request = if PullSysFlag::has_lite_pull_flag(request_header.sys_flag as u32) {
            RemotingCommand::create_request_command(RequestCode::LitePullMessage, request_header)
        } else {
            RemotingCommand::create_request_command(RequestCode::PullMessage, request_header)
        };
        match communication_mode {
            CommunicationMode::Sync => {
                let result_ext = this
                    .pull_message_sync(addr.as_str(), request, timeout_millis)
                    .await?;
                Ok(Some(result_ext))
            }
            CommunicationMode::Async => {
                tokio::spawn(async move {
                    let instant = Instant::now();
                    let _ = this
                        .pull_message_async(addr.as_str(), request, timeout_millis, pull_callback)
                        .await;
                    println!(
                        ">>>>>>>>>>>>>>>>>>pull_message_async cost: {:?}",
                        instant.elapsed()
                    );
                });
                Ok(None)
            }
            CommunicationMode::Oneway => Ok(None),
        }
    }

    async fn pull_message_sync(
        &mut self,
        addr: &str,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> Result<PullResultExt> {
        let response = self
            .remoting_client
            .invoke_async(Some(addr.to_string()), request, timeout_millis)
            .await?;
        self.process_pull_response(response, addr).await
    }

    async fn pull_message_async<PCB>(
        &mut self,
        addr: &str,
        request: RemotingCommand,
        timeout_millis: u64,
        mut pull_callback: PCB,
    ) -> Result<()>
    where
        PCB: PullCallback,
    {
        match self
            .remoting_client
            .invoke_async(Some(addr.to_string()), request, timeout_millis)
            .await
        {
            Ok(response) => {
                println!(
                    "++++++++++++++++++++++++pull_message_async response: {}",
                    response
                );
                let result = self.process_pull_response(response, addr).await;

                match result {
                    Ok(pull_result) => {
                        pull_callback.on_success(pull_result).await;
                    }
                    Err(error) => {
                        pull_callback.on_exception(Box::new(error));
                    }
                }
            }
            Err(err) => {
                pull_callback.on_exception(Box::new(err));
            }
        }
        Ok(())
    }

    async fn process_pull_response(
        &mut self,
        mut response: RemotingCommand,
        addr: &str,
    ) -> Result<PullResultExt> {
        let pull_status = match ResponseCode::from(response.code()) {
            ResponseCode::Success => PullStatus::Found,
            ResponseCode::PullNotFound => PullStatus::NoNewMsg,
            ResponseCode::PullRetryImmediately => PullStatus::NoMatchedMsg,
            ResponseCode::PullOffsetMoved => PullStatus::OffsetIllegal,
            _ => {
                return Err(MQBrokerError(
                    response.code(),
                    response.remark().map_or("".to_string(), |s| s.to_string()),
                    addr.to_string(),
                ))
            }
        };
        let response_header = response
            .decode_command_custom_header::<PullMessageResponseHeader>()
            .unwrap();
        let pull_result = PullResultExt {
            pull_result: PullResult {
                pull_status,
                next_begin_offset: response_header.next_begin_offset.unwrap_or(0) as u64,
                min_offset: response_header.min_offset.unwrap_or(0) as u64,
                max_offset: response_header.max_offset.unwrap_or(0) as u64,
                msg_found_list: vec![],
            },
            suggest_which_broker_id: response_header.suggest_which_broker_id.unwrap_or(0),
            message_binary: response.take_body(),
            offset_delta: response_header.offset_delta,
        };
        Ok(pull_result)
    }

    pub async fn consumer_send_message_back(
        &mut self,
        addr: &str,
        broker_name: &str,
        msg: &MessageExt,
        consumer_group: &str,
        delay_level: i32,
        timeout_millis: u64,
        max_consume_retry_times: i32,
    ) -> Result<()> {
        let header = ConsumerSendMsgBackRequestHeader {
            offset: msg.commit_log_offset,
            group: consumer_group.to_string(),
            delay_level,
            origin_msg_id: Some(msg.msg_id.clone()),
            origin_topic: Some(msg.get_topic().to_string()),
            unit_mode: false,
            max_reconsume_times: Some(max_consume_retry_times),
            rpc_request_header: Some(RpcRequestHeader {
                namespace: None,
                namespaced: None,
                broker_name: Some(broker_name.to_string()),
                oneway: None,
            }),
        };

        let request_command =
            RemotingCommand::create_request_command(RequestCode::ConsumerSendMsgBack, header);
        let response = self
            .remoting_client
            .invoke_async(
                Some(mix_all::broker_vip_channel(
                    self.client_config.vip_channel_enabled,
                    addr,
                )),
                request_command,
                timeout_millis,
            )
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            Ok(())
        } else {
            Err(MQBrokerError(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string()),
                addr.to_string(),
            ))
        }
    }

    pub async fn unregister_client(
        &mut self,
        addr: &str,
        client_id: &str,
        producer_group: Option<String>,
        consumer_group: Option<String>,
        timeout_millis: u64,
    ) -> Result<()> {
        let request_header = UnregisterClientRequestHeader {
            client_id: client_id.to_string(),
            producer_group,
            consumer_group,
            rpc_request_header: None,
        };
        let request =
            RemotingCommand::create_request_command(RequestCode::UnregisterClient, request_header);
        let response = self
            .remoting_client
            .invoke_async(Some(addr.to_string()), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            Ok(())
        } else {
            Err(MQBrokerError(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string()),
                addr.to_string(),
            ))
        }
    }
}
