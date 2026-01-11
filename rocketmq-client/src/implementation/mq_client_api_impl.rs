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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_batch::MessageBatch;
use rocketmq_common::common::message::message_client_id_setter::MessageClientIDSetter;
use rocketmq_common::common::message::message_enum::MessageRequestMode;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::message_queue_assignment::MessageQueueAssignment;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mq_version::CURRENT_VERSION;
use rocketmq_common::common::namesrv::default_top_addressing::DefaultTopAddressing;
use rocketmq_common::common::namesrv::name_server_update_callback::NameServerUpdateCallback;
use rocketmq_common::common::namesrv::top_addressing::TopAddressing;
use rocketmq_common::common::sys_flag::pull_sys_flag::PullSysFlag;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_common::EnvUtils::EnvUtils;
use rocketmq_common::MessageDecoder;
use rocketmq_error::RocketMQResult;
use rocketmq_remoting::base::connection_net_event::ConnectionNetEvent;
use rocketmq_remoting::clients::rocketmq_tokio_client::RocketmqDefaultClient;
use rocketmq_remoting::clients::RemotingClient;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::body::batch_ack_message_request_body::BatchAckMessageRequestBody;
use rocketmq_remoting::protocol::body::broker_body::cluster_info::ClusterInfo;
use rocketmq_remoting::protocol::body::check_client_request_body::CheckClientRequestBody;
use rocketmq_remoting::protocol::body::get_consumer_listby_group_response_body::GetConsumerListByGroupResponseBody;
use rocketmq_remoting::protocol::body::query_assignment_request_body::QueryAssignmentRequestBody;
use rocketmq_remoting::protocol::body::query_assignment_response_body::QueryAssignmentResponseBody;
use rocketmq_remoting::protocol::body::request::lock_batch_request_body::LockBatchRequestBody;
use rocketmq_remoting::protocol::body::response::lock_batch_response_body::LockBatchResponseBody;
use rocketmq_remoting::protocol::body::set_message_request_mode_request_body::SetMessageRequestModeRequestBody;
use rocketmq_remoting::protocol::body::unlock_batch_request_body::UnlockBatchRequestBody;
use rocketmq_remoting::protocol::header::ack_message_request_header::AckMessageRequestHeader;
use rocketmq_remoting::protocol::header::change_invisible_time_request_header::ChangeInvisibleTimeRequestHeader;
use rocketmq_remoting::protocol::header::change_invisible_time_response_header::ChangeInvisibleTimeResponseHeader;
use rocketmq_remoting::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_remoting::protocol::header::consumer_send_msg_back_request_header::ConsumerSendMsgBackRequestHeader;
use rocketmq_remoting::protocol::header::empty_header::EmptyHeader;
use rocketmq_remoting::protocol::header::end_transaction_request_header::EndTransactionRequestHeader;
use rocketmq_remoting::protocol::header::extra_info_util::ExtraInfoUtil;
use rocketmq_remoting::protocol::header::get_consumer_listby_group_request_header::GetConsumerListByGroupRequestHeader;
use rocketmq_remoting::protocol::header::get_max_offset_request_header::GetMaxOffsetRequestHeader;
use rocketmq_remoting::protocol::header::get_max_offset_response_header::GetMaxOffsetResponseHeader;
use rocketmq_remoting::protocol::header::get_meta_data_response_header::GetMetaDataResponseHeader;
use rocketmq_remoting::protocol::header::heartbeat_request_header::HeartbeatRequestHeader;
use rocketmq_remoting::protocol::header::lock_batch_mq_request_header::LockBatchMqRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_remoting::protocol::header::namesrv::kv_config_header::DeleteKVConfigRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::kv_config_header::PutKVConfigRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::perm_broker_header::AddWritePermOfBrokerRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::perm_broker_header::AddWritePermOfBrokerResponseHeader;
use rocketmq_remoting::protocol::header::namesrv::perm_broker_header::WipeWritePermOfBrokerRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::perm_broker_header::WipeWritePermOfBrokerResponseHeader;
use rocketmq_remoting::protocol::header::pop_message_request_header::PopMessageRequestHeader;
use rocketmq_remoting::protocol::header::pop_message_response_header::PopMessageResponseHeader;
use rocketmq_remoting::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_remoting::protocol::header::pull_message_response_header::PullMessageResponseHeader;
use rocketmq_remoting::protocol::header::query_consumer_offset_request_header::QueryConsumerOffsetRequestHeader;
use rocketmq_remoting::protocol::header::query_consumer_offset_response_header::QueryConsumerOffsetResponseHeader;
use rocketmq_remoting::protocol::header::unlock_batch_mq_request_header::UnlockBatchMqRequestHeader;
use rocketmq_remoting::protocol::header::unregister_client_request_header::UnregisterClientRequestHeader;
use rocketmq_remoting::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetRequestHeader;
use rocketmq_remoting::protocol::heartbeat::heartbeat_data::HeartbeatData;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::namespace_util::NamespaceUtil;
use rocketmq_remoting::protocol::remoting_command;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::remoting::RemotingService;
use rocketmq_remoting::rpc::rpc_request_header::RpcRequestHeader;
use rocketmq_remoting::rpc::topic_request_header::TopicRequestHeader;
use rocketmq_remoting::runtime::config::client_config::TokioClientConfig;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_rust::ArcMut;
use tracing::error;
use tracing::warn;

use crate::base::client_config::ClientConfig;
use crate::consumer::ack_callback::AckCallback;
use crate::consumer::ack_result::AckResult;
use crate::consumer::ack_status::AckStatus;
use crate::consumer::consumer_impl::pull_request_ext::PullResultExt;
use crate::consumer::pop_callback::PopCallback;
use crate::consumer::pop_result::PopResult;
use crate::consumer::pop_status::PopStatus;
use crate::consumer::pull_callback::PullCallback;
use crate::consumer::pull_result::PullResult;
use crate::consumer::pull_status::PullStatus;
use crate::factory::mq_client_instance::MQClientInstance;
use crate::hook::send_message_context::SendMessageContext;
use crate::implementation::client_remoting_processor::ClientRemotingProcessor;
use crate::implementation::communication_mode::CommunicationMode;
use crate::producer::producer_impl::default_mq_producer_impl::DefaultMQProducerImpl;
use crate::producer::producer_impl::topic_publish_info::TopicPublishInfo;
use crate::producer::send_callback::SendMessageCallback;
use crate::producer::send_result::SendResult;
use crate::producer::send_status::SendStatus;

static INIT_REMOTING_VERSION: OnceLock<()> = OnceLock::new();

static SEND_SMART_MSG: LazyLock<bool> = LazyLock::new(|| {
    std::env::var("org.apache.rocketmq.client.sendSmartMsg")
        .unwrap_or("false".to_string())
        .parse()
        .unwrap_or(false)
});

pub struct MQClientAPIImpl {
    remoting_client: ArcMut<RocketmqDefaultClient<ClientRemotingProcessor>>,
    top_addressing: Arc<Box<dyn TopAddressing>>,
    // client_remoting_processor: ClientRemotingProcessor,
    name_srv_addr: Option<String>,
    client_config: ClientConfig,
}

impl MQClientAPIImpl {
    pub(crate) async fn delete_kvconfig_value(
        &self,
        namespace: CheetahString,
        key: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request_header = DeleteKVConfigRequestHeader::new(namespace, key);
        let request = RemotingCommand::create_request_command(RequestCode::DeleteKvConfig, request_header);

        let name_server_address_list = self.remoting_client.get_name_server_address_list();
        let mut err_response = None;
        for name_srv_addr in name_server_address_list {
            let response = self
                .remoting_client
                .invoke_request(Some(name_srv_addr), request.clone(), timeout_millis)
                .await?;
            match ResponseCode::from(response.code()) {
                ResponseCode::Success => {}
                _ => err_response = Some(response),
            }
        }

        if let Some(err_response) = err_response {
            return Err(mq_client_err!(
                err_response.code(),
                err_response.remark().map_or("".to_string(), |s| s.to_string())
            ));
        }
        Ok(())
    }

    pub(crate) async fn put_kvconfig_value(
        &self,
        namespace: CheetahString,
        key: CheetahString,
        value: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let request_header = PutKVConfigRequestHeader::new(namespace, key, value);
        let request = RemotingCommand::create_request_command(RequestCode::PutKvConfig, request_header);

        let name_server_address_list = self.remoting_client.get_name_server_address_list();
        let mut err_response = None;
        for name_srv_addr in name_server_address_list {
            let response = self
                .remoting_client
                .invoke_request(Some(name_srv_addr), request.clone(), timeout_millis)
                .await?;
            match ResponseCode::from(response.code()) {
                ResponseCode::Success => {}
                _ => err_response = Some(response),
            }
        }

        if let Some(err_response) = err_response {
            return Err(mq_client_err!(
                err_response.code(),
                err_response.remark().map_or("".to_string(), |s| s.to_string())
            ));
        }
        Ok(())
    }

    pub(crate) async fn update_name_server_config(
        &self,
        properties: HashMap<CheetahString, CheetahString>,
        special_name_servers: Option<Vec<CheetahString>>,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let body = mix_all::properties_to_string(&properties);
        if body.is_empty() {
            return Ok(());
        }
        let invoke_name_servers = if let Some(name_servers) = special_name_servers {
            if !name_servers.is_empty() {
                name_servers
            } else {
                Vec::from(self.get_name_server_address_list())
            }
        } else {
            Vec::from(self.get_name_server_address_list())
        };
        if invoke_name_servers.is_empty() {
            return Ok(());
        }
        let empty_header = EmptyHeader {};
        let mut request = RemotingCommand::create_request_command(RequestCode::UpdateNamesrvConfig, empty_header);

        request = request.set_body(body.to_string());
        let mut err_response = None;
        for name_srv_addr in invoke_name_servers {
            let response = self
                .remoting_client
                .invoke_request(Some(&name_srv_addr), request.clone(), timeout_millis)
                .await?;
            match ResponseCode::from(response.code()) {
                ResponseCode::Success => {}
                _ => err_response = Some(response),
            }
        }

        if let Some(err_response) = err_response {
            return Err(mq_client_err!(
                err_response.code(),
                err_response.remark().map_or("".to_string(), |s| s.to_string())
            ));
        }
        Ok(())
    }

    pub(crate) async fn add_write_perm_of_broker(
        &self,
        namesrv_addr: CheetahString,
        broker_name: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<i32> {
        let request_header = AddWritePermOfBrokerRequestHeader::new(broker_name);
        let request = RemotingCommand::create_request_command(RequestCode::AddWritePermOfBroker, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(&namesrv_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            let request_header = response.decode_command_custom_header_fast::<AddWritePermOfBrokerResponseHeader>()?;
            return Ok(request_header.get_add_topic_count());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn wipe_write_perm_of_broker(
        &self,
        namesrv_addr: CheetahString,
        broker_name: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<i32> {
        let request_header = WipeWritePermOfBrokerRequestHeader::new(broker_name);
        let request = RemotingCommand::create_request_command(RequestCode::WipeWritePermOfBroker, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(&namesrv_addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            let request_header = response.decode_command_custom_header_fast::<WipeWritePermOfBrokerResponseHeader>()?;
            return Ok(request_header.get_wipe_topic_count());
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }

    pub(crate) async fn get_broker_cluster_info(&self, timeout_millis: u64) -> RocketMQResult<ClusterInfo> {
        let request = RemotingCommand::create_request_command(RequestCode::GetBrokerClusterInfo, EmptyHeader {});
        let response = self
            .remoting_client
            .invoke_request(None, request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.get_body() {
                return ClusterInfo::decode(body.as_ref());
            }
        }
        Err(mq_client_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string())
        ))
    }
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
        rpc_hook: Option<Arc<dyn RPCHook>>,
        client_config: ClientConfig,
        tx: Option<tokio::sync::broadcast::Sender<ConnectionNetEvent>>,
    ) -> Self {
        init_remoting_version();

        let mut default_client = RocketmqDefaultClient::new_with_cl(tokio_client_config, client_remoting_processor, tx);
        if let Some(hook) = rpc_hook {
            default_client.register_rpc_hook(hook);
        }

        MQClientAPIImpl {
            remoting_client: ArcMut::new(default_client),
            top_addressing: Arc::new(Box::new(DefaultTopAddressing::new(
                mix_all::get_ws_addr().into(),
                client_config.unit_name.clone(),
            ))),
            //client_remoting_processor,
            name_srv_addr: None,
            client_config,
        }
    }

    pub async fn start(&self) {
        let client = ArcMut::downgrade(&self.remoting_client);
        self.remoting_client.start(client).await;
    }

    pub async fn fetch_name_server_addr(&mut self) -> Option<String> {
        let top_addressing = self.top_addressing.clone();
        let addrs = tokio::task::spawn_blocking(move || top_addressing.fetch_ns_addr())
            .await
            .unwrap_or_default();

        if let Some(addrs) = addrs.as_ref() {
            if !addrs.is_empty() {
                let mut notify = false;
                if let Some(addr) = self.name_srv_addr.as_mut() {
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
        }
        self.name_srv_addr.clone()
    }

    pub async fn update_name_server_address_list(&self, addrs: &str) {
        let addr_vec = addrs
            .split(";")
            .map(CheetahString::from_slice)
            .collect::<Vec<CheetahString>>();
        self.remoting_client.update_name_server_address_list(addr_vec).await;
    }

    #[inline]
    pub async fn get_default_topic_route_info_from_name_server(
        &self,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>> {
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
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>> {
        self.get_topic_route_info_from_name_server_detail(topic, timeout_millis, true)
            .await
    }

    #[inline]
    pub async fn get_topic_route_info_from_name_server_detail(
        &self,
        topic: &str,
        timeout_millis: u64,
        allow_topic_not_exist: bool,
    ) -> rocketmq_error::RocketMQResult<Option<TopicRouteData>> {
        let request_header = GetRouteInfoRequestHeader {
            topic: CheetahString::from_slice(topic),
            accept_standard_json_only: None,
            topic_request_header: None,
        };
        let request = RemotingCommand::create_request_command(RequestCode::GetRouteinfoByTopic, request_header);
        let response = self.remoting_client.invoke_request(None, request, timeout_millis).await;
        match response {
            Ok(mut result) => {
                let code = result.code();
                let response_code = ResponseCode::from(code);
                match response_code {
                    ResponseCode::Success => {
                        let body = result.take_body();
                        if let Some(body_inner) = body {
                            let route_data = TopicRouteData::decode(body_inner.as_ref())?;
                            return Ok(Some(route_data));
                        }
                    }
                    ResponseCode::TopicNotExist => {
                        if allow_topic_not_exist {
                            warn!("get Topic [{}] RouteInfoFromNameServer is not exist value", topic);
                        }
                    }
                    _ => {
                        return Err(mq_client_err!(
                            code,
                            result.remark().cloned().unwrap_or_default().to_string()
                        ))
                    }
                }
                Err(mq_client_err!(
                    code,
                    result.remark().cloned().unwrap_or_default().to_string()
                ))
            }
            Err(err) => Err(err),
        }
    }

    pub fn get_name_server_address_list(&self) -> &[CheetahString] {
        self.remoting_client.get_name_server_address_list()
    }

    pub async fn send_message<T>(
        &mut self,
        addr: &CheetahString,
        broker_name: &CheetahString,
        msg: &mut T,
        request_header: SendMessageRequestHeader,
        timeout_millis: u64,
        communication_mode: CommunicationMode,
        send_callback: Option<SendMessageCallback>,
        topic_publish_info: Option<&TopicPublishInfo>,
        instance: Option<ArcMut<MQClientInstance>>,
        retry_times_when_send_failed: u32,
        context: &mut Option<SendMessageContext<'_>>,
        producer: &DefaultMQProducerImpl,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
    where
        T: MessageTrait,
    {
        let begin_start_time = Instant::now();
        let msg_type = msg.get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_MESSAGE_TYPE));
        let is_reply = msg_type.is_some() && msg_type.unwrap() == mix_all::REPLY_MESSAGE_FLAG;
        let mut request = if is_reply {
            if *SEND_SMART_MSG {
                let request_header_v2 =
                    SendMessageRequestHeaderV2::create_send_message_request_header_v2(&request_header);
                RemotingCommand::create_request_command(RequestCode::SendReplyMessageV2, request_header_v2)
            } else {
                RemotingCommand::create_request_command(RequestCode::SendReplyMessage, request_header)
            }
        } else {
            let is_batch_message = msg.as_any().downcast_ref::<MessageBatch>().is_some();
            if *SEND_SMART_MSG || is_batch_message {
                let request_header_v2 =
                    SendMessageRequestHeaderV2::create_send_message_request_header_v2(&request_header);
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
            request.set_body_mut_ref(compressed_body.unwrap());
        } else {
            request.set_body_mut_ref(msg.get_body().cloned().unwrap());
        }
        match communication_mode {
            CommunicationMode::Sync => {
                let cost_time_sync = (Instant::now() - begin_start_time).as_millis() as u64;
                if cost_time_sync > timeout_millis {
                    return Err(rocketmq_error::RocketMQError::Timeout {
                        operation: "sendMessage",
                        timeout_ms: timeout_millis,
                    });
                }
                let result = self
                    .send_message_sync(addr, broker_name, msg, timeout_millis - cost_time_sync, request)
                    .await?;
                Ok(Some(result))
            }
            CommunicationMode::Async => {
                let times = AtomicU32::new(0);
                let cost_time_sync = (Instant::now() - begin_start_time).as_millis() as u64;
                if cost_time_sync > timeout_millis {
                    return Err(rocketmq_error::RocketMQError::Timeout {
                        operation: "sendMessage",
                        timeout_ms: timeout_millis,
                    });
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
                    .invoke_request_oneway(addr, request, timeout_millis)
                    .await;
                Ok(None)
            }
        }
    }

    pub async fn send_message_simple<T>(
        &mut self,
        addr: &CheetahString,
        broker_name: &CheetahString,
        msg: &mut T,
        request_header: SendMessageRequestHeader,
        timeout_millis: u64,
        communication_mode: CommunicationMode,
        context: &mut Option<SendMessageContext<'_>>,
        producer: &DefaultMQProducerImpl,
    ) -> rocketmq_error::RocketMQResult<Option<SendResult>>
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
        addr: &CheetahString,
        broker_name: &CheetahString,
        msg: &T,
        timeout_millis: u64,
        request: RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<SendResult>
    where
        T: MessageTrait,
    {
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        self.process_send_response(broker_name, msg, &response, addr)
    }

    /*    async fn send_message_async<T: MessageTrait>(
        &mut self,
        addr: &CheetahString,
        broker_name: &CheetahString,
        msg: &T,
        timeout_millis: u64,
        request: RemotingCommand,
        send_callback: Option<SendMessageCallback>,
        topic_publish_info: Option<&TopicPublishInfo>,
        instance: Option<ArcMut<MQClientInstance>>,
        retry_times_when_send_failed: u32,
        times: &AtomicU32,
        context: &mut Option<SendMessageContext<'_>>,
        producer: &DefaultMQProducerImpl,
    ) {
        let begin_start_time = Instant::now();
        let result = self
            .remoting_client
            .invoke_async(Some(addr), request.clone(), timeout_millis)
            .await;
        match result {
            Ok(response) => {
                let cost_time = (Instant::now() - begin_start_time).as_millis() as u64;
                if send_callback.is_none() {
                    let send_result = self.process_send_response(broker_name, msg, &response, addr);
                    if let Ok(result) = send_result {
                        if context.is_some() {
                            let inner = context.as_mut().unwrap();
                            inner.send_result = Some(result.clone());
                            producer.execute_send_message_hook_after(context);
                        }
                    }
                    let duration = (Instant::now() - begin_start_time).as_millis() as u64;
                    producer
                        .update_fault_item(&broker_name, duration, false, true)
                        .await;
                    return;
                }
                let send_result = self.process_send_response(broker_name, msg, &response, addr);
                match send_result {
                    Ok(result) => {
                        if context.is_some() {
                            let inner = context.as_mut().unwrap();
                            inner.send_result = Some(result.clone());
                            producer.execute_send_message_hook_after(context);
                        }
                        let duration = (Instant::now() - begin_start_time).as_millis() as u64;
                        send_callback.as_ref().unwrap()(Some(&result), None);
                        producer
                            .update_fault_item(&broker_name, duration, false, true)
                            .await;
                    }
                    Err(err) => {
                        let duration = (Instant::now() - begin_start_time).as_millis() as u64;
                        producer
                            .update_fault_item(&broker_name, duration, true, true)
                            .await;
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
    }*/

    async fn send_message_async<T: MessageTrait>(
        &mut self,
        addr: &CheetahString,
        broker_name: &CheetahString,
        msg: &T,
        timeout_millis: u64,
        request: RemotingCommand,
        send_callback: Option<SendMessageCallback>,
        topic_publish_info: Option<&TopicPublishInfo>,
        instance: Option<ArcMut<MQClientInstance>>,
        retry_times_when_send_failed: u32,
        times: &AtomicU32,
        context: &mut Option<SendMessageContext<'_>>,
        producer: &DefaultMQProducerImpl,
    ) {
        let mut current_addr = addr.clone();
        let mut current_broker_name = broker_name.clone();
        let mut current_request = request;

        loop {
            let begin_start_time = Instant::now();
            let result = self
                .remoting_client
                .invoke_request(Some(&current_addr), current_request.clone(), timeout_millis)
                .await;

            match result {
                Ok(response) => {
                    let cost_time = (Instant::now() - begin_start_time).as_millis() as u64;
                    if send_callback.is_none() {
                        let send_result =
                            self.process_send_response(&current_broker_name, msg, &response, &current_addr);
                        if let Ok(result) = send_result {
                            if context.is_some() {
                                let inner = context.as_mut().unwrap();
                                inner.send_result = Some(result);
                                producer.execute_send_message_hook_after(context);
                            }
                        }
                        let duration = (Instant::now() - begin_start_time).as_millis() as u64;
                        producer
                            .update_fault_item(&current_broker_name, duration, false, true)
                            .await;
                        return;
                    }

                    let send_result = self.process_send_response(&current_broker_name, msg, &response, &current_addr);
                    match send_result {
                        Ok(result) => {
                            if context.is_some() {
                                let inner = context.as_mut().unwrap();
                                inner.send_result = Some(result.clone());
                                producer.execute_send_message_hook_after(context);
                            }
                            let duration = (Instant::now() - begin_start_time).as_millis() as u64;
                            send_callback.as_ref().unwrap()(Some(&result), None);
                            producer
                                .update_fault_item(&current_broker_name, duration, false, true)
                                .await;
                            return; // success, return loop
                        }
                        Err(err) => {
                            let duration = (Instant::now() - begin_start_time).as_millis() as u64;
                            producer
                                .update_fault_item(&current_broker_name, duration, true, true)
                                .await;

                            // Check if a retry is needed
                            let current_times = times.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                            if current_times < retry_times_when_send_failed {
                                // Prepare to retry: Select a new broker
                                match self
                                    .prepare_retry(
                                        &current_broker_name,
                                        msg,
                                        &mut current_request,
                                        topic_publish_info,
                                        instance.as_ref(),
                                        producer,
                                    )
                                    .await
                                {
                                    Some((new_addr, new_broker_name)) => {
                                        current_addr = new_addr;
                                        current_broker_name = new_broker_name;
                                        warn!(
                                            "async send msg by retry {} times. topic={}, brokerAddr={}, brokerName={}",
                                            current_times + 1,
                                            msg.get_topic(),
                                            current_addr,
                                            current_broker_name
                                        );
                                        current_request.set_opaque_mut(RemotingCommand::create_new_request_id());
                                        continue; // continue to retry
                                    }
                                    None => {
                                        // can not choose new broker, fail
                                        if let Some(callback) = send_callback {
                                            callback(None, Some(&err));
                                        }
                                        return;
                                    }
                                }
                            } else {
                                // The maximum number of retries has been reached
                                if let Some(callback) = send_callback {
                                    callback(None, Some(&err));
                                }
                                return;
                            }
                        }
                    }
                }
                Err(err) => {
                    error!("send message async error: {:?}", err);
                    let current_times = times.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                    if current_times < retry_times_when_send_failed {
                        // Try to retry even if there is a network error.
                        match self
                            .prepare_retry(
                                &current_broker_name,
                                msg,
                                &mut current_request,
                                topic_publish_info,
                                instance.as_ref(),
                                producer,
                            )
                            .await
                        {
                            Some((new_addr, new_broker_name)) => {
                                current_addr = new_addr;
                                current_broker_name = new_broker_name;
                                current_request.set_opaque_mut(RemotingCommand::create_new_request_id());
                                continue;
                            }
                            None => {
                                if let Some(callback) = send_callback {
                                    callback(None, Some(&err));
                                }
                                return;
                            }
                        }
                    } else {
                        if let Some(callback) = send_callback {
                            callback(None, Some(&err));
                        }
                        return;
                    }
                }
            }
        }
    }

    fn process_send_response<T>(
        &mut self,
        broker_name: &CheetahString,
        msg: &T,
        response: &RemotingCommand,
        addr: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<SendResult>
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
                return Err(client_broker_err!(
                    response.code(),
                    response.remark().map_or("".to_string(), |s| s.to_string()),
                    addr.to_string()
                ))
            }
        };
        let response_header = response
            .decode_command_custom_header_fast::<SendMessageResponseHeader>()
            .unwrap();
        let mut topic = msg.get_topic().to_string();
        let namespace = self.client_config.get_namespace();
        if let Some(ns) = namespace.as_ref() {
            if !ns.is_empty() {
                topic = NamespaceUtil::without_namespace_with_namespace(topic.as_str(), ns.as_str());
            }
        }
        let message_queue = MessageQueue::from_parts(topic.as_str(), broker_name, response_header.queue_id());
        let mut uniq_msg_id = MessageClientIDSetter::get_uniq_id(msg);
        let msgs = msg.as_any().downcast_ref::<MessageBatch>();

        if let (Some(msgs), true) = (msgs, response_header.batch_uniq_id().is_none()) {
            let mut sb = String::new();
            for msg in &msgs.messages {
                sb.push_str(if sb.is_empty() { "" } else { "," });
                sb.push_str(MessageClientIDSetter::get_uniq_id(msg).unwrap().as_str());
            }
            uniq_msg_id = Some(CheetahString::from_string(sb));
        }

        let region_id = response
            .ext_fields()
            .unwrap()
            .get(MessageConst::PROPERTY_MSG_REGION)
            .map_or(mix_all::DEFAULT_TRACE_REGION_ID.to_string(), |s| s.to_string());
        let trace_on = response
            .ext_fields()
            .unwrap()
            .get(MessageConst::PROPERTY_TRACE_SWITCH)
            .is_some_and(|s| s.parse().unwrap_or(false));
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

    /*async fn on_exception_impl<T: MessageTrait>(
        &mut self,
        broker_name: &CheetahString,
        msg: &T,
        timeout_millis: u64,
        mut request: RemotingCommand,
        send_callback: Option<SendMessageCallback>,
        topic_publish_info: Option<&TopicPublishInfo>,
        instance: Option<ArcMut<MQClientInstance>>,
        times_total: u32,
        cur_times: &AtomicU32,
        e: rocketmq_error::RocketmqError,
        context: &mut Option<SendMessageContext<'_>>,
        need_retry: bool,
        producer: &DefaultMQProducerImpl,
    ) {
        let tmp = cur_times.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        if need_retry && tmp < times_total {
            let mut retry_broker_name = broker_name.clone();
            if let Some(topic_publish_info) = topic_publish_info {
                let mq_chosen = producer.select_one_message_queue(
                    topic_publish_info,
                    Some(&retry_broker_name),
                    false,
                );
                retry_broker_name = instance
                    .as_ref()
                    .unwrap()
                    .get_broker_name_from_message_queue(mq_chosen.as_ref().unwrap())
                    .await;
            }
            let addr = instance
                .as_ref()
                .unwrap()
                .find_broker_address_in_publish(retry_broker_name.as_ref())
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
                &addr,
                &retry_broker_name,
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
            let inner = context.as_mut().unwrap();
            inner.exception = Some(Arc::new(Box::new(e)));
            producer.execute_send_message_hook_after(context);
        }
    }*/

    async fn prepare_retry<T: MessageTrait>(
        &self,
        broker_name: &CheetahString,
        msg: &T,
        request: &mut RemotingCommand,
        topic_publish_info: Option<&TopicPublishInfo>,
        instance: Option<&ArcMut<MQClientInstance>>,
        producer: &DefaultMQProducerImpl,
    ) -> Option<(CheetahString, CheetahString)> {
        let mut retry_broker_name = broker_name.clone();

        if let Some(topic_publish_info) = topic_publish_info {
            let mq_chosen = producer.select_one_message_queue(topic_publish_info, Some(&retry_broker_name), false);
            if let Some(instance) = instance {
                retry_broker_name = instance
                    .get_broker_name_from_message_queue(mq_chosen.as_ref().unwrap())
                    .await;
            }
        }

        if let Some(instance) = instance {
            if let Some(addr) = instance
                .find_broker_address_in_publish(retry_broker_name.as_ref())
                .await
            {
                return Some((addr, retry_broker_name));
            }
        }

        None
    }

    pub async fn send_heartbeat(
        &mut self,
        addr: &CheetahString,
        heartbeat_data: &HeartbeatData,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i32> {
        let request =
            RemotingCommand::create_request_command(RequestCode::HeartBeat, HeartbeatRequestHeader::default())
                .set_language(self.client_config.language)
                .set_body(heartbeat_data.encode().expect("encode HeartbeatData failed"));
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            return Ok(response.version());
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn check_client_in_broker(
        &mut self,
        broker_addr: &str,
        consumer_group: &str,
        client_id: &str,
        subscription_data: &SubscriptionData,
        timeout_millis: u64,
    ) -> RocketMQResult<()> {
        let mut request = RemotingCommand::create_remoting_command(RequestCode::CheckClientConfig);
        let body = CheckClientRequestBody::new(
            client_id.to_string(),
            consumer_group.to_string(),
            subscription_data.clone(),
        );
        request.set_body_mut_ref(body.encode().expect("encode CheckClientRequestBody failed"));
        let response = self
            .remoting_client
            .invoke_request(
                Some(mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, broker_addr).as_ref()),
                request,
                timeout_millis,
            )
            .await?;
        if ResponseCode::from(response.code()) != ResponseCode::Success {
            return Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            ));
        }
        Ok(())
    }

    pub async fn get_consumer_id_list_by_group(
        &mut self,
        addr: &str,
        consumer_group: &str,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<Vec<CheetahString>> {
        let request_header = GetConsumerListByGroupRequestHeader {
            consumer_group: CheetahString::from_slice(consumer_group),
            rpc: None,
        };
        let request = RemotingCommand::create_request_command(RequestCode::GetConsumerListByGroup, request_header);
        let response = self
            .remoting_client
            .invoke_request(
                Some(mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr).as_ref()),
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
                        Err(e) => Err(mq_client_err!(response
                            .remark()
                            .map_or("".to_string(), |s| s.to_string()))),
                    };
                }
            }
            _ => {
                return Err(client_broker_err!(
                    response.code(),
                    response.remark().map_or("".to_string(), |s| s.to_string()),
                    addr.to_string()
                ));
            }
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn update_consumer_offset_oneway(
        &mut self,
        addr: &str,
        request_header: UpdateConsumerOffsetRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(RequestCode::UpdateConsumerOffset, request_header);
        self.remoting_client
            .invoke_request_oneway(
                mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr).as_ref(),
                request,
                timeout_millis,
            )
            .await;
        Ok(())
    }

    pub async fn update_consumer_offset(
        &mut self,
        addr: &CheetahString,
        request_header: UpdateConsumerOffsetRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request = RemotingCommand::create_request_command(RequestCode::UpdateConsumerOffset, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) != ResponseCode::Success {
            Err(client_broker_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string()),
                addr.to_string()
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
    ) -> rocketmq_error::RocketMQResult<i64> {
        let request = RemotingCommand::create_request_command(RequestCode::QueryConsumerOffset, request_header);
        let response = self
            .remoting_client
            .invoke_request(
                Some(mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr).as_ref()),
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
                return Err(client_broker_err!(
                    response.code(),
                    response.remark().map_or("".to_string(), |s| s.to_string()),
                    addr.to_string()
                ))
            }
            _ => {}
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn pull_message<PCB>(
        mut this: ArcMut<Self>,
        addr: CheetahString,
        request_header: PullMessageRequestHeader,
        timeout_millis: u64,
        communication_mode: CommunicationMode,
        pull_callback: PCB,
    ) -> rocketmq_error::RocketMQResult<Option<PullResultExt>>
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
                let result_ext = this.pull_message_sync(&addr, request, timeout_millis).await?;
                Ok(Some(result_ext))
            }
            CommunicationMode::Async => {
                tokio::spawn(async move {
                    let instant = Instant::now();
                    let _ = this
                        .pull_message_async(&addr, request, timeout_millis, pull_callback)
                        .await;
                });
                Ok(None)
            }
            CommunicationMode::Oneway => Ok(None),
        }
    }

    async fn pull_message_sync(
        &mut self,
        addr: &CheetahString,
        request: RemotingCommand,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<PullResultExt> {
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        self.process_pull_response(response, addr).await
    }

    async fn pull_message_async<PCB>(
        &mut self,
        addr: &CheetahString,
        request: RemotingCommand,
        timeout_millis: u64,
        mut pull_callback: PCB,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        PCB: PullCallback,
    {
        match self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await
        {
            Ok(response) => {
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
        addr: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<PullResultExt> {
        let pull_status = match ResponseCode::from(response.code()) {
            ResponseCode::Success => PullStatus::Found,
            ResponseCode::PullNotFound => PullStatus::NoNewMsg,
            ResponseCode::PullRetryImmediately => PullStatus::NoMatchedMsg,
            ResponseCode::PullOffsetMoved => PullStatus::OffsetIllegal,
            _ => {
                return Err(client_broker_err!(
                    response.code(),
                    response.remark().map_or("".to_string(), |s| s.to_string()),
                    addr.to_string()
                ))
            }
        };
        let response_header = response
            .decode_command_custom_header::<PullMessageResponseHeader>()
            .unwrap();
        let pull_result = PullResultExt {
            pull_result: PullResult {
                pull_status,
                next_begin_offset: response_header.next_begin_offset as u64,
                min_offset: response_header.min_offset as u64,
                max_offset: response_header.max_offset as u64,
                msg_found_list: Some(vec![]),
            },
            suggest_which_broker_id: response_header.suggest_which_broker_id,
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
    ) -> rocketmq_error::RocketMQResult<()> {
        let header = ConsumerSendMsgBackRequestHeader {
            offset: msg.commit_log_offset,
            group: CheetahString::from_slice(consumer_group),
            delay_level,
            origin_msg_id: Some(CheetahString::from_slice(msg.msg_id.as_str())),
            origin_topic: Some(CheetahString::from_slice(msg.get_topic())),
            unit_mode: false,
            max_reconsume_times: Some(max_consume_retry_times),
            rpc_request_header: Some(RpcRequestHeader {
                namespace: None,
                namespaced: None,
                broker_name: Some(CheetahString::from_slice(broker_name)),
                oneway: None,
            }),
        };

        let request_command = RemotingCommand::create_request_command(RequestCode::ConsumerSendMsgBack, header);
        let response = self
            .remoting_client
            .invoke_request(
                Some(&mix_all::broker_vip_channel(
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
            Err(client_broker_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string()),
                addr.to_string()
            ))
        }
    }

    pub async fn unregister_client(
        &mut self,
        addr: &CheetahString,
        client_id: CheetahString,
        producer_group: Option<CheetahString>,
        consumer_group: Option<CheetahString>,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request_header = UnregisterClientRequestHeader {
            client_id,
            producer_group,
            consumer_group,
            rpc_request_header: None,
        };
        let request = RemotingCommand::create_request_command(RequestCode::UnregisterClient, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            Ok(())
        } else {
            Err(client_broker_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string()),
                addr.to_string()
            ))
        }
    }

    pub async fn unlock_batch_mq(
        &mut self,
        addr: &CheetahString,
        request_body: UnlockBatchRequestBody,
        timeout_millis: u64,
        oneway: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        let mut request =
            RemotingCommand::create_request_command(RequestCode::UnlockBatchMq, UnlockBatchMqRequestHeader::default());
        request.set_body_mut_ref(request_body.encode().expect("encode UnlockBatchRequestBody failed"));
        if oneway {
            self.remoting_client
                .invoke_request_oneway(addr, request, timeout_millis)
                .await;
            Ok(())
        } else {
            let response = self
                .remoting_client
                .invoke_request(
                    Some(&mix_all::broker_vip_channel(
                        self.client_config.vip_channel_enabled,
                        addr,
                    )),
                    request,
                    timeout_millis,
                )
                .await?;
            if ResponseCode::from(response.code()) == ResponseCode::Success {
                Ok(())
            } else {
                Err(client_broker_err!(
                    response.code(),
                    response.remark().map_or("".to_string(), |s| s.to_string()),
                    addr.to_string()
                ))
            }
        }
    }

    pub async fn lock_batch_mq(
        &mut self,
        addr: &str,
        request_body: LockBatchRequestBody,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<HashSet<MessageQueue>> {
        let mut request =
            RemotingCommand::create_request_command(RequestCode::LockBatchMq, LockBatchMqRequestHeader::default());
        request.set_body_mut_ref(request_body.encode().expect("encode LockBatchRequestBody failed"));
        let response = self
            .remoting_client
            .invoke_request(
                Some(&mix_all::broker_vip_channel(
                    self.client_config.vip_channel_enabled,
                    addr,
                )),
                request,
                timeout_millis,
            )
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.body() {
                LockBatchResponseBody::decode(body.as_ref())
                    .map(|body| body.lock_ok_mq_set)
                    .map_err(|e| client_broker_err!(response.code(), e.to_string(), addr.to_string()))
            } else {
                Err(client_broker_err!(
                    response.code(),
                    "Response body is empty".to_string(),
                    addr.to_string()
                ))
            }
        } else {
            Err(client_broker_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string()),
                addr.to_string()
            ))
        }
    }

    pub async fn end_transaction_oneway(
        &mut self,
        addr: &CheetahString,
        request_header: EndTransactionRequestHeader,
        remark: CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request =
            RemotingCommand::create_request_command(RequestCode::EndTransaction, request_header).set_remark(remark);

        self.remoting_client
            .invoke_request_oneway(addr, request, timeout_millis)
            .await;
        Ok(())
    }

    pub async fn get_max_offset(
        &mut self,
        addr: &str,
        message_queue: &MessageQueue,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        let request_header = GetMaxOffsetRequestHeader {
            topic: CheetahString::from_slice(message_queue.get_topic()),
            queue_id: message_queue.get_queue_id(),
            committed: false,
            topic_request_header: Some(TopicRequestHeader {
                rpc_request_header: Some(RpcRequestHeader {
                    broker_name: Some(CheetahString::from_slice(message_queue.get_broker_name())),
                    ..Default::default()
                }),
                lo: None,
            }),
        };

        let request = RemotingCommand::create_request_command(RequestCode::GetMaxOffset, request_header);

        let response = self
            .remoting_client
            .invoke_request(
                Some(&mix_all::broker_vip_channel(
                    self.client_config.vip_channel_enabled,
                    addr,
                )),
                request,
                timeout_millis,
            )
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            let response_header = response
                .decode_command_custom_header::<GetMaxOffsetResponseHeader>()
                .expect("decode error");
            return Ok(response_header.offset);
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn set_message_request_mode(
        &mut self,
        broker_addr: &CheetahString,
        topic: &CheetahString,
        consumer_group: &CheetahString,
        mode: MessageRequestMode,
        pop_share_queue_num: i32,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let body = SetMessageRequestModeRequestBody {
            topic: topic.clone(),
            consumer_group: consumer_group.clone(),
            mode,
            pop_share_queue_num,
        };
        let request = RemotingCommand::create_remoting_command(RequestCode::SetMessageRequestMode)
            .set_body(body.encode().expect("encode SetMessageRequestModeRequestBody failed"));
        let response = self
            .remoting_client
            .invoke_request(
                Some(&mix_all::broker_vip_channel(
                    self.client_config.vip_channel_enabled,
                    broker_addr,
                )),
                request,
                timeout_millis,
            )
            .await?;
        if ResponseCode::from(response.code()) != ResponseCode::Success {
            return Err(mq_client_err!(
                response.code(),
                response.remark().cloned().unwrap_or_default().to_string()
            ));
        }
        Ok(())
    }

    pub async fn query_assignment(
        &mut self,
        addr: &CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        client_id: CheetahString,
        strategy_name: CheetahString,
        message_model: MessageModel,
        timeout: u64,
    ) -> rocketmq_error::RocketMQResult<Option<HashSet<MessageQueueAssignment>>> {
        let request_body = QueryAssignmentRequestBody {
            topic,
            consumer_group,
            client_id,
            strategy_name,
            message_model,
        };
        let request = RemotingCommand::new_request(
            RequestCode::QueryAssignment,
            request_body.encode().expect("encode QueryAssignmentRequestBody failed"),
        );
        let response = self
            .remoting_client
            .invoke_request(
                Some(&mix_all::broker_vip_channel(
                    self.client_config.vip_channel_enabled,
                    addr,
                )),
                request,
                timeout,
            )
            .await?;

        if ResponseCode::from(response.code()) == ResponseCode::Success {
            let body = response.body();
            if let Some(body) = body {
                let assignment = QueryAssignmentResponseBody::decode(body.as_ref());
                if let Ok(assignment) = assignment {
                    return Ok(Some(assignment.message_queue_assignments));
                }
            }
            return Ok(None);
        }

        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn change_invisible_time_async(
        &self,
        broker_name: &CheetahString,
        addr: &CheetahString,
        request_header: ChangeInvisibleTimeRequestHeader,
        timeout_millis: u64,
        ack_callback: impl AckCallback,
    ) -> rocketmq_error::RocketMQResult<()> {
        let offset = request_header.offset;
        let topic = request_header.topic.clone();
        let queue_id = request_header.queue_id;
        let request = RemotingCommand::create_request_command(RequestCode::ChangeMessageInvisibleTime, request_header);
        match self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await
        {
            Ok(response) => {
                let response_header = response.decode_command_custom_header::<ChangeInvisibleTimeResponseHeader>()?;
                let ack_result = if ResponseCode::from(response.code()) == ResponseCode::Success {
                    AckResult {
                        status: AckStatus::Ok,
                        pop_time: response_header.pop_time as i64,
                        extra_info: CheetahString::from_string(format!(
                            "{}{}{}",
                            ExtraInfoUtil::build_extra_info(
                                offset,
                                response_header.pop_time as i64,
                                response_header.invisible_time,
                                response_header.revive_qid,
                                &topic,
                                broker_name,
                                queue_id,
                            ),
                            MessageConst::KEY_SEPARATOR,
                            offset
                        )),
                    }
                } else {
                    AckResult {
                        status: AckStatus::NotExist,
                        ..Default::default()
                    }
                };
                ack_callback.on_success(ack_result);
            }
            Err(e) => {
                ack_callback.on_exception(Box::new(e));
            }
        };
        Ok(())
    }

    pub async fn pop_message_async<PC>(
        &self,
        broker_name: &CheetahString,
        addr: &CheetahString,
        request_header: PopMessageRequestHeader,
        timeout_millis: u64,
        mut pop_callback: PC,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        PC: PopCallback + 'static,
    {
        let topic = request_header.topic.clone();
        let order = request_header.order.unwrap_or_default();
        let request = RemotingCommand::create_request_command(RequestCode::PopMessage, request_header);
        match self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await
        {
            Ok(response) => {
                let result = self.process_pop_response(broker_name, response, &topic, order);
                match result {
                    Ok(pop_result) => {
                        pop_callback.on_success(pop_result).await;
                    }
                    Err(e) => {
                        pop_callback.on_error(Box::new(e));
                    }
                }
            }
            Err(e) => {
                pop_callback.on_error(Box::new(e));
            }
        }
        Ok(())
    }

    fn process_pop_response(
        &self,
        broker_name: &CheetahString,
        mut response: RemotingCommand,
        topic: &CheetahString,
        is_order: bool,
    ) -> rocketmq_error::RocketMQResult<PopResult> {
        let response_code = ResponseCode::from(response.code());
        let (pop_status, msg_found_list) = match response_code {
            ResponseCode::Success => {
                let messages = MessageDecoder::decodes_batch(
                    response.get_body_mut().unwrap(),
                    self.client_config.decode_read_body,
                    self.client_config.decode_decompress_body,
                );
                (PopStatus::Found, messages)
            }
            ResponseCode::PollingFull => (PopStatus::PollingFull, vec![]),
            ResponseCode::PollingTimeout | ResponseCode::PullNotFound => (PopStatus::PollingNotFound, vec![]),
            _ => {
                return Err(client_broker_err!(
                    response.code(),
                    response.remark().cloned().unwrap_or_default()
                ))
            }
        };
        let mut pop_result = PopResult {
            pop_status,
            msg_found_list: Some(msg_found_list),
            ..Default::default()
        };
        let response_header = response.decode_command_custom_header::<PopMessageResponseHeader>()?;
        pop_result.rest_num = response_header.rest_num;
        if pop_result.pop_status != PopStatus::Found {
            return Ok(pop_result);
        }
        // it is a pop command if pop time greater than 0, we should set the check point info to
        // extraInfo field
        pop_result.invisible_time = response_header.invisible_time;
        pop_result.pop_time = response_header.pop_time;
        let start_offset_info = ExtraInfoUtil::parse_start_offset_info(
            response_header
                .start_offset_info
                .as_ref()
                .unwrap_or(&CheetahString::from_slice("")),
        )?;
        let msg_offset_info = ExtraInfoUtil::parse_msg_offset_info(
            response_header
                .msg_offset_info
                .as_ref()
                .unwrap_or(&CheetahString::from_slice("")),
        )?;
        let order_count_info = ExtraInfoUtil::parse_order_count_info(
            response_header
                .order_count_info
                .as_ref()
                .unwrap_or(&CheetahString::from_slice("")),
        )?;
        let sort_map =
            build_queue_offset_sorted_map(topic.as_str(), pop_result.msg_found_list.as_ref().map_or(&[], |v| v))?;
        let mut map = HashMap::with_capacity(5);
        for message in pop_result.msg_found_list.as_mut().map_or(&mut vec![], |v| v) {
            if start_offset_info.is_empty() {
                let key = CheetahString::from_string(format!("{}{}", message.get_topic(), message.queue_id() as i64));
                if !map.contains_key(&key) {
                    let extra_info = ExtraInfoUtil::build_extra_info(
                        message.queue_offset(),
                        response_header.pop_time as i64,
                        response_header.invisible_time as i64,
                        response_header.revive_qid as i32,
                        message.get_topic(),
                        broker_name,
                        message.queue_id(),
                    );
                    map.insert(key.clone(), CheetahString::from_string(extra_info));
                }
                message.put_property(
                    CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK),
                    CheetahString::from_string(format!(
                        "{}{}{}",
                        map.get(&key).cloned().unwrap_or_default(),
                        MessageConst::KEY_SEPARATOR,
                        message.queue_offset()
                    )),
                );
            } else {
                let ck = message.get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK));
                if ck.is_none() {
                    let dispatch = message
                        .get_property(&CheetahString::from_static_str(
                            MessageConst::PROPERTY_INNER_MULTI_DISPATCH,
                        ))
                        .unwrap_or_default();
                    let (queue_offset_key, queue_id_key) = if mix_all::is_lmq(Some(topic.as_str()))
                        && !dispatch.is_empty()
                    {
                        let queues: Vec<&str> = dispatch.split(mix_all::MULTI_DISPATCH_QUEUE_SPLITTER).collect();
                        let data = message
                            .get_property(&CheetahString::from_static_str(
                                MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET,
                            ))
                            .unwrap_or_default();
                        let queue_offsets: Vec<&str> = data.split(mix_all::MULTI_DISPATCH_QUEUE_SPLITTER).collect();
                        let offset = queue_offsets[queues.iter().position(|&q| q == topic).unwrap()]
                            .parse::<i64>()
                            .unwrap();
                        let queue_id_key =
                            ExtraInfoUtil::get_start_offset_info_map_key(topic.as_str(), mix_all::LMQ_QUEUE_ID as i64);
                        let queue_offset_key = ExtraInfoUtil::get_queue_offset_map_key(
                            topic.as_str(),
                            mix_all::LMQ_QUEUE_ID as i64,
                            offset,
                        );
                        let index = sort_map
                            .get(&queue_id_key)
                            .unwrap()
                            .iter()
                            .position(|&q| q == offset as u64)
                            .unwrap_or_default();

                        let msg_queue_offset = sort_map
                            .get(&queue_offset_key)
                            .unwrap()
                            .get(index)
                            .cloned()
                            .unwrap_or_default();
                        if msg_queue_offset as i64 != offset {
                            warn!(
                                "Queue offset[{}] of msg is strange, not equal to the stored in msg, {:?}",
                                msg_queue_offset, message
                            );
                        }
                        let extra_info = ExtraInfoUtil::build_extra_info(
                            message.queue_offset(),
                            response_header.pop_time as i64,
                            response_header.invisible_time as i64,
                            response_header.revive_qid as i32,
                            message.get_topic(),
                            broker_name,
                            msg_queue_offset as i32,
                        );
                        message.put_property(
                            CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK),
                            CheetahString::from_string(extra_info),
                        );
                        (queue_offset_key, queue_id_key)
                    } else {
                        let queue_id_key = ExtraInfoUtil::get_start_offset_info_map_key(
                            message.get_topic(),
                            message.queue_id() as i64,
                        );
                        let queue_offset_key = ExtraInfoUtil::get_queue_offset_map_key(
                            message.get_topic(),
                            message.queue_id() as i64,
                            message.queue_offset(),
                        );
                        let queue_offset = message.queue_offset();
                        let index = sort_map
                            .get(&queue_id_key)
                            .unwrap()
                            .iter()
                            .position(|&q| q == queue_offset as u64)
                            .unwrap_or_default();

                        let msg_queue_offset = sort_map
                            .get(&queue_offset_key)
                            .unwrap()
                            .get(index)
                            .cloned()
                            .unwrap_or_default();
                        if msg_queue_offset as i64 != queue_offset {
                            warn!(
                                "Queue offset[{}] of msg is strange, not equal to the stored in msg, {:?}",
                                msg_queue_offset, message
                            );
                        }
                        let extra_info = ExtraInfoUtil::build_extra_info(
                            message.queue_offset(),
                            response_header.pop_time as i64,
                            response_header.invisible_time as i64,
                            response_header.revive_qid as i32,
                            message.get_topic(),
                            broker_name,
                            msg_queue_offset as i32,
                        );
                        message.put_property(
                            CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK),
                            CheetahString::from_string(extra_info),
                        );
                        (queue_offset_key, queue_id_key)
                    };
                    if is_order && !order_count_info.is_empty() {
                        let mut count = order_count_info.get(&queue_offset_key);
                        if count.is_none() {
                            count = order_count_info.get(&queue_id_key);
                        }
                        if let Some(ct) = count {
                            message.set_reconsume_times(*ct);
                        }
                    }
                }
            }
            message.put_property(
                CheetahString::from_static_str(MessageConst::PROPERTY_FIRST_POP_TIME),
                CheetahString::from(response_header.pop_time.to_string()),
            );
            message.broker_name = broker_name.clone();
            message.set_topic(
                NamespaceUtil::without_namespace_with_namespace(
                    topic.as_str(),
                    self.client_config.namespace.clone().unwrap_or_default().as_str(),
                )
                .into(),
            )
        }
        Ok(pop_result)
    }

    pub async fn ack_message_async(
        &self,
        addr: &CheetahString,
        request_header: AckMessageRequestHeader,
        timeout_millis: u64,
        ack_callback: impl AckCallback,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.ack_message_async_inner(addr, Some(request_header), None, timeout_millis, ack_callback)
            .await
    }

    pub(self) async fn ack_message_async_inner(
        &self,
        addr: &CheetahString,
        request_header: Option<AckMessageRequestHeader>,
        request_body: Option<BatchAckMessageRequestBody>,
        timeout_millis: u64,
        ack_callback: impl AckCallback,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request = if let Some(header) = request_header {
            RemotingCommand::create_request_command(RequestCode::AckMessage, header)
        } else {
            let body = request_body.unwrap();
            RemotingCommand::new_request(RequestCode::BatchAckMessage, body.encode()?)
        };
        match self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await
        {
            Ok(response) => {
                let response_code = ResponseCode::from(response.code());
                let ack_result = if response_code == ResponseCode::Success {
                    AckResult {
                        status: AckStatus::Ok,
                        ..Default::default()
                    }
                } else {
                    AckResult {
                        status: AckStatus::NotExist,
                        ..Default::default()
                    }
                };
                ack_callback.on_success(ack_result);
            }
            Err(e) => {
                ack_callback.on_exception(Box::new(e));
            }
        }
        Ok(())
    }

    pub async fn get_name_server_config(
        &self,
        name_servers: Option<Vec<CheetahString>>,
        timeout_millis: Duration,
    ) -> RocketMQResult<Option<HashMap<CheetahString, HashMap<CheetahString, CheetahString>>>> {
        // Determine which name servers to invoke
        let invoke_name_servers = match name_servers {
            Some(servers) if !servers.is_empty() => servers,
            _ => self.remoting_client.get_name_server_address_list().to_vec(),
        };

        if invoke_name_servers.is_empty() {
            return Ok(None);
        }

        // Create request command
        let request = RemotingCommand::create_remoting_command(RequestCode::GetNamesrvConfig);
        let mut config_map = HashMap::with_capacity(4);
        // Iterate through each name server
        for name_server in invoke_name_servers {
            // Make synchronous call with timeout
            let response = self
                .remoting_client
                .invoke_request(Some(&name_server), request.clone(), timeout_millis.as_millis() as u64)
                .await?;
            // Check response code
            match ResponseCode::from(response.code()) {
                ResponseCode::Success => {
                    // Parse response body as properties
                    match response.get_body() {
                        Some(body) => {
                            let body_str = String::from_utf8_lossy(body.as_ref()).to_string();
                            // if body_str contains =, return from Java version
                            let properties = if body_str.contains('=') {
                                mix_all::string_to_properties(&body_str).unwrap_or_default()
                            } else {
                                SerdeJsonUtils::from_json_str::<HashMap<CheetahString, CheetahString>>(&body_str)
                                    .expect("failed to parse JSON")
                            };

                            config_map.insert(name_server.clone(), properties);
                        }
                        None => return Err(mq_client_err!("Body is empty".to_string())),
                    }
                }
                code => {
                    return Err(mq_client_err!(
                        response.code(),
                        response.remark().map_or("".to_string(), |s| s.to_string())
                    ));
                }
            }
        }
        Ok(Some(config_map))
    }

    pub async fn get_controller_metadata(
        &self,
        controller_address: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<GetMetaDataResponseHeader> {
        let request = RemotingCommand::create_remoting_command(RequestCode::ControllerGetMetadataInfo);
        let response = self
            .remoting_client
            .invoke_request(Some(&controller_address), request, timeout_millis)
            .await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => match response.decode_command_custom_header_fast::<GetMetaDataResponseHeader>() {
                Ok(header) => Ok(header),
                Err(_) => Err(mq_client_err!("Could not decode GetMetaDataResponseHeader".to_string())),
            },
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string())
            )),
        }
    }
}

fn build_queue_offset_sorted_map(
    topic: &str,
    msg_found_list: &[MessageExt],
) -> RocketMQResult<HashMap<String, Vec<u64>>> {
    let mut sort_map: HashMap<String, Vec<u64>> = HashMap::with_capacity(16);
    for message_ext in msg_found_list {
        let key: String;
        let dispatch = message_ext
            .get_property(&CheetahString::from_static_str(
                MessageConst::PROPERTY_INNER_MULTI_DISPATCH,
            ))
            .unwrap_or_default();
        if mix_all::is_lmq(Some(topic)) && message_ext.reconsume_times() == 0 && !dispatch.is_empty() {
            // process LMQ
            let queues: Vec<&str> = dispatch.split(mix_all::MULTI_DISPATCH_QUEUE_SPLITTER).collect();
            let data = message_ext
                .get_property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET,
                ))
                .unwrap_or_default();
            let queue_offsets: Vec<&str> = data.split(mix_all::MULTI_DISPATCH_QUEUE_SPLITTER).collect();
            // LMQ topic has only 1 queue, which queue id is 0
            key = ExtraInfoUtil::get_start_offset_info_map_key(topic, mix_all::LMQ_QUEUE_ID as i64);
            sort_map.entry(key).or_insert_with(|| Vec::with_capacity(4)).push(
                queue_offsets[queues.iter().position(|&q| q == topic).unwrap()]
                    .parse()
                    .unwrap(),
            );
            continue;
        }
        // Value of POP_CK is used to determine whether it is a pop retry,
        // cause topic could be rewritten by broker.
        key = ExtraInfoUtil::get_start_offset_info_map_key_with_pop_ck(
            message_ext.get_topic(),
            message_ext
                .get_property(&CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK))
                .clone()
                .as_ref()
                .map(|item| item.as_str()),
            message_ext.queue_id() as i64,
        )?;
        sort_map
            .entry(key)
            .or_insert_with(|| Vec::with_capacity(4))
            .push(message_ext.queue_offset() as u64);
    }
    Ok(sort_map)
}

pub fn init_remoting_version() {
    INIT_REMOTING_VERSION.get_or_init(|| {
        EnvUtils::put_property(
            remoting_command::REMOTING_VERSION_KEY,
            (CURRENT_VERSION as u32).to_string(),
        );
    });
}
