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

use std::collections::HashSet;
use std::sync::Arc;

use crate::broker_runtime::BrokerRuntimeInner;
use cheetah_string::CheetahString;
use dns_lookup::lookup_host;
use rocketmq_client_rust::consumer::pull_result::PullResult;
use rocketmq_client_rust::consumer::pull_status::PullStatus;
use rocketmq_client_rust::producer::send_result::SendResult;
use rocketmq_client_rust::producer::send_status::SendStatus;
use rocketmq_client_rust::PullResultExt;
use rocketmq_common::common::broker::broker_config::BrokerIdentity;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::common::filter::expression_type::ExpressionType;
use rocketmq_common::common::message::message_client_id_setter::MessageClientIDSetter;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::sys_flag::pull_sys_flag::PullSysFlag;
use rocketmq_common::common::topic::TopicValidator;
use rocketmq_common::utils::crc32_utils;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::MessageDecoder;
use rocketmq_common::TimeUtils::get_current_millis;
use rocketmq_error::RocketMQError;
use rocketmq_remoting::clients::rocketmq_tokio_client::RocketmqDefaultClient;
use rocketmq_remoting::clients::RemotingClient;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::body::broker_body::broker_member_group::BrokerMemberGroup;
use rocketmq_remoting::protocol::body::broker_body::register_broker_body::RegisterBrokerBody;
use rocketmq_remoting::protocol::body::consumer_offset_serialize_wrapper::ConsumerOffsetSerializeWrapper;
use rocketmq_remoting::protocol::body::elect_master_response_body::ElectMasterResponseBody;
use rocketmq_remoting::protocol::body::kv_table::KVTable;
use rocketmq_remoting::protocol::body::message_request_mode_serialize_wrapper::MessageRequestModeSerializeWrapper;
use rocketmq_remoting::protocol::body::response::lock_batch_response_body::LockBatchResponseBody;
use rocketmq_remoting::protocol::body::subscription_group_wrapper::SubscriptionGroupWrapper;
use rocketmq_remoting::protocol::body::sync_state_set_body::SyncStateSet;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use rocketmq_remoting::protocol::broker_sync_info::BrokerSyncInfo;
use rocketmq_remoting::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_remoting::protocol::header::controller::alter_sync_state_set_request_header::AlterSyncStateSetRequestHeader;
use rocketmq_remoting::protocol::header::controller::apply_broker_id_request_header::ApplyBrokerIdRequestHeader;
use rocketmq_remoting::protocol::header::controller::apply_broker_id_response_header::ApplyBrokerIdResponseHeader;
use rocketmq_remoting::protocol::header::controller::elect_master_request_header::ElectMasterRequestHeader;
use rocketmq_remoting::protocol::header::controller::get_next_broker_id_request_header::GetNextBrokerIdRequestHeader;
use rocketmq_remoting::protocol::header::controller::get_next_broker_id_response_header::GetNextBrokerIdResponseHeader;
use rocketmq_remoting::protocol::header::controller::get_replica_info_request_header::GetReplicaInfoRequestHeader;
use rocketmq_remoting::protocol::header::controller::get_replica_info_response_header::GetReplicaInfoResponseHeader;
use rocketmq_remoting::protocol::header::controller::register_broker_to_controller_request_header::RegisterBrokerToControllerRequestHeader;
use rocketmq_remoting::protocol::header::controller::register_broker_to_controller_response_header::RegisterBrokerToControllerResponseHeader;
use rocketmq_remoting::protocol::header::elect_master_response_header::ElectMasterResponseHeader;
use rocketmq_remoting::protocol::header::exchange_ha_info_request_header::ExchangeHAInfoRequestHeader;
use rocketmq_remoting::protocol::header::exchange_ha_info_response_header::ExchangeHaInfoResponseHeader;
use rocketmq_remoting::protocol::header::get_max_offset_request_header::GetMaxOffsetRequestHeader;
use rocketmq_remoting::protocol::header::get_max_offset_response_header::GetMaxOffsetResponseHeader;
use rocketmq_remoting::protocol::header::get_meta_data_response_header::GetMetaDataResponseHeader;
use rocketmq_remoting::protocol::header::get_min_offset_request_header::GetMinOffsetRequestHeader;
use rocketmq_remoting::protocol::header::get_min_offset_response_header::GetMinOffsetResponseHeader;
use rocketmq_remoting::protocol::header::lock_batch_mq_request_header::LockBatchMqRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header::SendMessageRequestHeader;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_request_header_v2::SendMessageRequestHeaderV2;
use rocketmq_remoting::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_remoting::protocol::header::namesrv::broker_request::BrokerHeartbeatRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::broker_request::UnRegisterBrokerRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::query_data_version_header::QueryDataVersionRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::query_data_version_header::QueryDataVersionResponseHeader;
use rocketmq_remoting::protocol::header::namesrv::register_broker_header::RegisterBrokerRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::register_broker_header::RegisterBrokerResponseHeader;
use rocketmq_remoting::protocol::header::namesrv::topic_operation_header::RegisterTopicRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::topic_operation_header::TopicRequestHeader;
use rocketmq_remoting::protocol::header::pull_message_request_header::PullMessageRequestHeader;
use rocketmq_remoting::protocol::header::pull_message_response_header::PullMessageResponseHeader;
use rocketmq_remoting::protocol::header::unlock_batch_mq_request_header::UnlockBatchMqRequestHeader;
use rocketmq_remoting::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_remoting::protocol::namesrv::RegisterBrokerResult;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::route::route_data_view::QueueData;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::protocol::DataVersion;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::remoting::RemotingService;
use rocketmq_remoting::request_processor::default_request_processor::DefaultRemotingRequestProcessor;
use rocketmq_remoting::rpc::client_metadata::ClientMetadata;
use rocketmq_remoting::rpc::rpc_client_impl::RpcClientImpl;
use rocketmq_remoting::rpc::rpc_request_header::RpcRequestHeader;
use rocketmq_remoting::runtime::config::client_config::TokioClientConfig;
use rocketmq_remoting::runtime::RPCHook;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

pub struct BrokerOuterAPI {
    remoting_client: ArcMut<RocketmqDefaultClient<DefaultRemotingRequestProcessor>>,
    name_server_address: Option<String>,
    rpc_client: RpcClientImpl,
    client_metadata: Arc<ClientMetadata>,
}

impl BrokerOuterAPI {
    pub fn new(tokio_client_config: Arc<TokioClientConfig>) -> Self {
        let client = ArcMut::new(RocketmqDefaultClient::new(
            tokio_client_config,
            DefaultRemotingRequestProcessor,
        ));
        let client_metadata = Arc::new(ClientMetadata::new());
        Self {
            remoting_client: client.clone(),
            name_server_address: None,
            rpc_client: RpcClientImpl::new(Arc::clone(&client_metadata), client),
            client_metadata,
        }
    }

    pub fn new_with_hook(tokio_client_config: Arc<TokioClientConfig>, rpc_hook: Option<Arc<dyn RPCHook>>) -> Self {
        let mut client = ArcMut::new(RocketmqDefaultClient::new(
            tokio_client_config,
            DefaultRemotingRequestProcessor,
        ));
        let client_metadata = Arc::new(ClientMetadata::new());
        if let Some(rpc_hook) = rpc_hook {
            client.register_rpc_hook(rpc_hook);
        }
        Self {
            remoting_client: client.clone(),
            name_server_address: None,
            rpc_client: RpcClientImpl::new(Arc::clone(&client_metadata), client),
            client_metadata,
        }
    }

    fn create_request(broker_name: CheetahString, topic_config: ArcMut<TopicConfig>) -> RemotingCommand {
        let request_header = RegisterTopicRequestHeader::new(topic_config.topic_name.as_ref().cloned().unwrap());
        let queue_data = QueueData::new(
            broker_name,
            topic_config.read_queue_nums,
            topic_config.write_queue_nums,
            topic_config.perm,
            topic_config.topic_sys_flag,
        );
        let topic_route_data = TopicRouteData {
            queue_datas: vec![queue_data],
            ..Default::default()
        };
        let topic_route_body = topic_route_data.encode().expect("encode topic route data failed");

        RemotingCommand::create_request_command(RequestCode::RegisterTopicInNamesrv, request_header)
            .set_body(topic_route_body)
    }
}

impl BrokerOuterAPI {
    pub async fn start(&self) {
        let wrapper = ArcMut::downgrade(&self.remoting_client);
        self.remoting_client.start(wrapper).await;
    }

    pub async fn update_name_server_address_list(&self, addrs: CheetahString) {
        let addr_vec = addrs
            .split(";")
            .map(CheetahString::from_slice)
            .collect::<Vec<CheetahString>>();
        self.remoting_client.update_name_server_address_list(addr_vec).await
    }

    pub async fn update_name_server_address_list_by_dns_lookup(&self, domain: CheetahString) {
        let address_list = dns_lookup_address_by_domain(domain.as_str());
        self.remoting_client.update_name_server_address_list(address_list).await;
    }

    pub async fn register_broker_all<MS: MessageStore>(
        &self,
        cluster_name: CheetahString,
        broker_addr: CheetahString,
        broker_name: CheetahString,
        broker_id: u64,
        ha_server_addr: CheetahString,
        topic_config_wrapper: TopicConfigAndMappingSerializeWrapper,
        filter_server_list: Vec<CheetahString>,
        oneway: bool,
        timeout_mills: u64,
        enable_acting_master: bool,
        compressed: bool,
        heartbeat_timeout_millis: Option<i64>,
        _broker_identity: BrokerIdentity,
        broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
    ) -> Vec<RegisterBrokerResult> {
        let name_server_address_list = self.remoting_client.get_available_name_srv_list();
        let mut register_broker_result_list = Vec::new();
        if !name_server_address_list.is_empty() {
            let mut request_header = RegisterBrokerRequestHeader {
                broker_addr,
                broker_id,
                broker_name,
                cluster_name,
                ha_server_addr,
                enable_acting_master: Some(enable_acting_master),
                compressed: false,
                heartbeat_timeout_millis,
                body_crc32: 0,
            };

            //build request body
            let request_body = RegisterBrokerBody {
                topic_config_serialize_wrapper: topic_config_wrapper,
                filter_server_list,
            };
            let body = request_body.encode(compressed);
            let body_crc32 = crc32_utils::crc32(body.as_ref());
            request_header.body_crc32 = body_crc32;

            let mut handle_vec = Vec::with_capacity(name_server_address_list.len());
            for namesrv_addr in name_server_address_list.iter() {
                let cloned_body = body.clone();
                let cloned_header = request_header.clone();
                let addr = namesrv_addr.clone();
                let broker_runtime_inner_ = broker_runtime_inner.clone();
                let join_handle = tokio::spawn(async move {
                    broker_runtime_inner_
                        .broker_outer_api()
                        .register_broker(&addr, oneway, timeout_mills, cloned_header, cloned_body)
                        .await
                });
                /*let handle =
                self.register_broker(addr, oneway, timeout_mills, cloned_header, cloned_body);*/
                handle_vec.push(join_handle);
            }
            while let Some(handle) = handle_vec.pop() {
                let result = tokio::join!(handle);
                match result.0 {
                    Ok(value) => {
                        if let Some(v) = value {
                            register_broker_result_list.push(v);
                        } else {
                            error!("Register broker to name remoting_server error");
                        }
                    }
                    Err(e) => {
                        error!("Register broker to name remoting_server error, error={}", e);
                    }
                }
            }
        }

        register_broker_result_list
    }

    async fn register_broker(
        &self,
        namesrv_addr: &CheetahString,
        oneway: bool,
        timeout_mills: u64,
        request_header: RegisterBrokerRequestHeader,
        body: Vec<u8>,
    ) -> Option<RegisterBrokerResult> {
        debug!(
            "Register broker to name remoting_server, namesrv_addr={},request_code={:?}, request_header={:?}, \
             body={:?}",
            namesrv_addr,
            RequestCode::RegisterBroker,
            request_header,
            body
        );
        let request =
            RemotingCommand::create_request_command(RequestCode::RegisterBroker, request_header).set_body(body.clone());
        if oneway {
            self.remoting_client
                .invoke_request_oneway(namesrv_addr, request, timeout_mills)
                .await;
            return None;
        }
        match self
            .remoting_client
            .invoke_request(Some(namesrv_addr), request, timeout_mills)
            .await
        {
            Ok(response) => match From::from(response.code()) {
                ResponseCode::Success => {
                    info!(
                        "Register broker to name remoting_server success, namesrv_addr={} response body={:?}",
                        namesrv_addr,
                        response.body()
                    );
                    let register_broker_result =
                        response.decode_command_custom_header::<RegisterBrokerResponseHeader>();
                    let mut result = RegisterBrokerResult::default();
                    if let Ok(header) = register_broker_result {
                        result.ha_server_addr = header.ha_server_addr.clone().unwrap_or(CheetahString::empty());
                        result.master_addr = header.master_addr.unwrap_or(CheetahString::empty());
                    }
                    if let Some(body) = response.body() {
                        result.kv_table = SerdeJsonUtils::from_json_bytes::<KVTable>(body.as_ref()).unwrap();
                    }
                    Some(result)
                }
                _ => None,
            },
            Err(err) => {
                error!(
                    "Register broker to name remoting_server error, namesrv_addr={}, error={}",
                    namesrv_addr, err
                );
                None
            }
        }
    }

    /// Register the topic route info of single topic to all name remoting_server nodes.
    /// This method is used to replace incremental broker registration feature.
    pub async fn register_single_topic_all(
        &self,
        broker_name: CheetahString,
        topic_config: ArcMut<TopicConfig>,
        timeout_mills: u64,
    ) {
        let request = Self::create_request(broker_name, topic_config);
        let name_server_address_list = self.remoting_client.get_available_name_srv_list();
        let mut handle_vec = Vec::with_capacity(name_server_address_list.len());
        for namesrv_addr in name_server_address_list.iter() {
            let cloned_request = request.clone();
            let addr = namesrv_addr.clone();
            let client = self.remoting_client.clone();
            let join_handle =
                tokio::spawn(async move { client.invoke_request(Some(&addr), cloned_request, timeout_mills).await });
            handle_vec.push(join_handle);
        }
        while let Some(handle) = handle_vec.pop() {
            let _result = tokio::join!(handle);
        }
    }

    pub fn shutdown(&mut self) {
        self.remoting_client.shutdown();
    }

    pub fn refresh_metadata(&self) {}

    /// Send heartbeat to name servers
    ///
    /// # Arguments
    /// * `cluster_name` - Cluster name
    /// * `broker_addr` - Broker address
    /// * `broker_name` - Broker name
    /// * `broker_id` - Broker ID
    /// * `timeout_millis` - Request timeout in milliseconds
    /// * `is_in_broker_container` - Whether broker is in container
    pub async fn send_heartbeat(
        &self,
        cluster_name: CheetahString,
        broker_addr: CheetahString,
        broker_name: CheetahString,
        broker_id: i64,
        timeout_millis: u64,
        _is_in_broker_container: bool,
    ) {
        let name_server_address_list = self.remoting_client.get_available_name_srv_list();
        if name_server_address_list.is_empty() {
            warn!("No available name server for sending heartbeat");
            return;
        }

        let request_header = BrokerHeartbeatRequestHeader {
            cluster_name,
            broker_addr,
            broker_name,
            broker_id: Some(broker_id),
            epoch: None,
            max_offset: None,
            confirm_offset: None,
            heartbeat_timeout_mills: None,
            election_priority: None,
        };

        // Parallel heartbeat to all NameServers
        let handles: Vec<_> = name_server_address_list
            .iter()
            .map(|namesrv_addr| {
                let addr = namesrv_addr.clone();
                let header = request_header.clone();
                let client = self.remoting_client.clone();

                tokio::spawn(async move {
                    let request = RemotingCommand::create_request_command(RequestCode::BrokerHeartbeat, header);

                    client.invoke_request_oneway(&addr, request, timeout_millis).await;
                    debug!("Send heartbeat to name server {} success", addr);
                })
            })
            .collect();

        let _ = futures::future::join_all(handles).await;
    }

    /// Send heartbeat with data version to name servers
    ///
    /// This method sends data version along with heartbeat to allow
    /// name server to determine if broker metadata has changed.
    pub async fn send_heartbeat_via_data_version(
        &self,
        cluster_name: CheetahString,
        broker_addr: CheetahString,
        broker_name: CheetahString,
        broker_id: u64,
        timeout_millis: u64,
        data_version: &DataVersion,
        _is_in_broker_container: bool,
    ) {
        let name_server_address_list = self.remoting_client.get_available_name_srv_list();
        if name_server_address_list.is_empty() {
            return;
        }

        let request_header = QueryDataVersionRequestHeader {
            broker_addr,
            broker_name,
            broker_id,
            cluster_name,
        };

        let body = match data_version.encode() {
            Ok(b) => b,
            Err(e) => {
                error!("Failed to encode data version: {}", e);
                return;
            }
        };

        let handles: Vec<_> = name_server_address_list
            .iter()
            .map(|namesrv_addr| {
                let addr = namesrv_addr.clone();
                let header = request_header.clone();
                let body_clone = body.clone();
                let client = self.remoting_client.clone();

                tokio::spawn(async move {
                    let mut request = RemotingCommand::create_request_command(RequestCode::QueryDataVersion, header);
                    request.set_body_mut_ref(bytes::Bytes::from(body_clone));

                    client.invoke_request_oneway(&addr, request, timeout_millis).await;
                    debug!("Send heartbeat via data version to {} success", addr);
                })
            })
            .collect();

        let _ = futures::future::join_all(handles).await;
    }

    /// Check if broker needs to register to name servers
    ///
    /// This method queries all name servers to determine if broker
    /// metadata has changed and needs re-registration.
    ///
    /// # Returns
    /// Vector of booleans indicating if each name server needs registration
    pub async fn need_register(
        &self,
        cluster_name: CheetahString,
        broker_addr: CheetahString,
        broker_name: CheetahString,
        broker_id: u64,
        topic_config_wrapper: &TopicConfigAndMappingSerializeWrapper,
        timeout_millis: u64,
        _is_in_broker_container: bool,
    ) -> Vec<bool> {
        let name_server_address_list = self.remoting_client.get_name_server_address_list();
        if name_server_address_list.is_empty() {
            return vec![];
        }

        let data_version_body = match topic_config_wrapper
            .topic_config_serialize_wrapper
            .data_version()
            .encode()
        {
            Ok(body) => body,
            Err(e) => {
                error!("Failed to encode data version: {}", e);
                return vec![true; name_server_address_list.len()];
            }
        };

        let local_data_version = topic_config_wrapper
            .topic_config_serialize_wrapper
            .data_version()
            .clone();

        let handles: Vec<_> = name_server_address_list
            .iter()
            .map(|namesrv_addr| {
                let addr = namesrv_addr.clone();
                let header = QueryDataVersionRequestHeader {
                    cluster_name: cluster_name.clone(),
                    broker_addr: broker_addr.clone(),
                    broker_name: broker_name.clone(),
                    broker_id,
                };
                let body = data_version_body.clone();
                let client = self.remoting_client.clone();
                let local_version = local_data_version.clone();

                tokio::spawn(async move {
                    let mut request = RemotingCommand::create_request_command(RequestCode::QueryDataVersion, header);
                    request.set_body_mut_ref(bytes::Bytes::from(body));

                    match client.invoke_request(Some(&addr), request, timeout_millis).await {
                        Ok(response) => match ResponseCode::from(response.code()) {
                            ResponseCode::Success => {
                                if let Ok(response_header) =
                                    response.decode_command_custom_header::<QueryDataVersionResponseHeader>()
                                {
                                    // If name server explicitly indicates changed
                                    if response_header.changed() {
                                        return Some(true);
                                    }

                                    // Compare data versions
                                    if let Some(body) = response.body() {
                                        if let Ok(ns_data_version) = DataVersion::decode(body.as_ref()) {
                                            return Some(local_version != ns_data_version);
                                        }
                                    }

                                    Some(false)
                                } else {
                                    Some(true)
                                }
                            }
                            _ => {
                                warn!("Query data version from {} failed: {:?}", addr, response.code());
                                Some(true)
                            }
                        },
                        Err(e) => {
                            error!("Query data version from {} error: {}", addr, e);
                            Some(true)
                        }
                    }
                })
            })
            .collect();

        let results = futures::future::join_all(handles).await;
        results.into_iter().filter_map(|r| r.ok().flatten()).collect()
    }

    /// Get maximum offset of a message queue
    ///
    /// # Arguments
    /// * `addr` - Broker address
    /// * `topic` - Topic name
    /// * `queue_id` - Queue ID
    /// * `committed` - Whether to get committed offset
    ///
    /// # Returns
    /// Maximum offset or error
    pub async fn get_max_offset(
        &self,
        addr: &CheetahString,
        topic: CheetahString,
        queue_id: i32,
        committed: bool,
    ) -> rocketmq_error::RocketMQResult<i64> {
        let request_header = GetMaxOffsetRequestHeader {
            topic,
            queue_id,
            committed,
            topic_request_header: None,
        };

        let request = RemotingCommand::create_request_command(RequestCode::GetMaxOffset, request_header);

        let response = self.remoting_client.invoke_request(Some(addr), request, 3000).await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let response_header = response.decode_command_custom_header::<GetMaxOffsetResponseHeader>()?;
                Ok(response_header.offset)
            }
            _ => Err(RocketMQError::BrokerOperationFailed {
                operation: "get_max_offset",
                code: response.code(),
                message: response.remark().map_or("".to_string(), |s| s.to_string()),
                broker_addr: Some(addr.to_string()),
            }),
        }
    }

    /// Get minimum offset of a message queue
    ///
    /// # Arguments
    /// * `addr` - Broker address
    /// * `topic` - Topic name
    /// * `queue_id` - Queue ID
    ///
    /// # Returns
    /// Minimum offset or error
    pub async fn get_min_offset(
        &self,
        addr: &CheetahString,
        topic: CheetahString,
        queue_id: i32,
    ) -> rocketmq_error::RocketMQResult<i64> {
        let request_header = GetMinOffsetRequestHeader {
            topic,
            queue_id,
            topic_request_header: None,
        };

        let request = RemotingCommand::create_request_command(RequestCode::GetMinOffset, request_header);

        let response = self.remoting_client.invoke_request(Some(addr), request, 3000).await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let response_header = response.decode_command_custom_header::<GetMinOffsetResponseHeader>()?;
                Ok(response_header.offset)
            }
            _ => Err(RocketMQError::BrokerOperationFailed {
                operation: "get_min_offset",
                code: response.code(),
                message: response.remark().map_or("".to_string(), |s| s.to_string()),
                broker_addr: Some(addr.to_string()),
            }),
        }
    }

    pub fn rpc_client(&self) -> &RpcClientImpl {
        &self.rpc_client
    }

    pub async fn lock_batch_mq_async(
        &self,
        addr: &CheetahString,
        request_body: bytes::Bytes,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<HashSet<MessageQueue>> {
        let mut request =
            RemotingCommand::create_request_command(RequestCode::LockBatchMq, LockBatchMqRequestHeader::default());
        request.set_body_mut_ref(request_body);
        let result = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await;
        match result {
            Ok(response) => {
                if ResponseCode::from(response.code()) == ResponseCode::Success {
                    let lock_batch_response_body = LockBatchResponseBody::decode(response.get_body().unwrap()).unwrap();
                    Ok(lock_batch_response_body.lock_ok_mq_set)
                } else {
                    Err(RocketMQError::BrokerOperationFailed {
                        operation: "lock_batch_mq",
                        code: response.code(),
                        message: response
                            .remark()
                            .cloned()
                            .unwrap_or(CheetahString::empty())
                            .serialize_json()
                            .expect("to json failed"),
                        broker_addr: Some("".to_string()),
                    })
                }
            }
            Err(e) => Err(e),
        }
    }

    pub async fn unlock_batch_mq_async(
        &self,
        addr: &CheetahString,
        request_body: bytes::Bytes,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let mut request =
            RemotingCommand::create_request_command(RequestCode::UnlockBatchMq, UnlockBatchMqRequestHeader::default());
        request.set_body_mut_ref(request_body);
        let result = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await;
        match result {
            Ok(response) => {
                if ResponseCode::from(response.code()) == ResponseCode::Success {
                    Ok(())
                } else {
                    Err(RocketMQError::BrokerOperationFailed {
                        operation: "unlock_batch_mq",
                        code: response.code(),
                        message: response.remark().cloned().unwrap_or(CheetahString::empty()).to_string(),
                        broker_addr: Some("".to_string()),
                    })
                }
            }
            Err(e) => Err(e),
        }
    }

    pub async fn get_topic_route_info_from_name_server(
        &self,
        topic: &CheetahString,
        timeout_millis: u64,
        allow_topic_not_exist: bool,
    ) -> rocketmq_error::RocketMQResult<TopicRouteData> {
        let header = GetRouteInfoRequestHeader {
            topic: topic.clone(),
            ..Default::default()
        };
        let request = RemotingCommand::create_request_command(RequestCode::GetRouteinfoByTopic, header);
        let response = self
            .remoting_client
            .invoke_request(None, request, timeout_millis)
            .await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::TopicNotExist => {
                if allow_topic_not_exist {
                    warn!("get Topic [{}] RouteInfoFromNameServer is not exist value", topic);
                }
            }
            ResponseCode::Success => {
                if let Some(body) = response.body() {
                    let topic_route_data = TopicRouteData::decode(body).unwrap();
                    return Ok(topic_route_data);
                }
            }
            _ => {}
        }
        Err(RocketMQError::BrokerOperationFailed {
            operation: "notify_min_broker_id_changed",
            code: response.code(),
            message: response.remark().cloned().unwrap_or(CheetahString::empty()).to_string(),
            broker_addr: Some("".to_string()),
        })
    }

    pub async fn send_message_to_specific_broker(
        &self,
        broker_addr: &CheetahString,
        broker_name: &CheetahString,
        msg: MessageExt,
        group: CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<SendResult> {
        let uniq_msg_id = MessageClientIDSetter::get_uniq_id(&msg);
        let queue_id = msg.queue_id;
        let topic = msg.get_topic().clone();
        let request = build_send_message_request(msg, group);
        let response = self
            .remoting_client
            .invoke_request(Some(broker_addr), request, timeout_millis)
            .await?;

        process_send_response(broker_name, uniq_msg_id.unwrap_or_default(), queue_id, topic, &response)
    }

    pub async fn pull_message_from_specific_broker_async(
        &self,
        broker_name: &CheetahString,
        broker_addr: &CheetahString,
        consumer_group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_nums: i32,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<(Option<PullResult>, String, bool)> {
        let request_header = PullMessageRequestHeader {
            consumer_group: consumer_group.clone(),
            topic: topic.clone(),
            queue_id,
            queue_offset: offset,
            max_msg_nums: max_nums,
            sys_flag: PullSysFlag::build_sys_flag(false, false, true, false) as i32,
            commit_offset: 0,
            suspend_timeout_millis: 0,
            subscription: Some(CheetahString::from_static_str(SubscriptionData::SUB_ALL)),
            sub_version: get_current_millis() as i64,
            expression_type: Some(CheetahString::from_static_str(ExpressionType::TAG)),
            max_msg_bytes: Some(i32::MAX),
            topic_request: Some(TopicRequestHeader {
                lo: None,
                rpc: Some(RpcRequestHeader {
                    broker_name: Some(broker_name.clone()),
                    ..Default::default()
                }),
            }),
            ..Default::default()
        };
        let request_command = RemotingCommand::create_request_command(RequestCode::PullMessage, request_header);
        match self
            .remoting_client
            .invoke_request(Some(broker_addr), request_command, timeout_millis)
            .await
        {
            Ok(response) => {
                let code = response.code();
                let mut pull_result_ext = match process_pull_response(response, broker_addr) {
                    Ok(value) => value,
                    Err(_) => return Ok((None, format!("Response Code:{code}"), true)),
                };
                let name = pull_result_ext.pull_result.pull_status().to_string();
                process_pull_result(&mut pull_result_ext, broker_name, queue_id);
                Ok((Some(pull_result_ext.pull_result), name, false))
            }
            Err(e) => Ok((None, e.to_string(), true)),
        }
    }

    pub async fn unregister_broker_all(
        &self,
        cluster_name: &CheetahString,
        broker_name: &CheetahString,
        broker_addr: &CheetahString,
        broker_id: u64,
    ) {
        let name_server_address_list = self.remoting_client.get_name_server_address_list();
        for namesrv_addr in name_server_address_list.iter() {
            match self
                .unregister_broker(namesrv_addr, cluster_name, broker_addr, broker_name, broker_id)
                .await
            {
                Ok(_) => {
                    info!(
                        "Unregister broker from name remoting_server success, namesrv_addr={}",
                        namesrv_addr
                    );
                }
                Err(e) => {
                    error!(
                        "Unregister broker from name remoting_server error, namesrv_addr={}, error={}",
                        namesrv_addr, e
                    );
                }
            }
        }
    }
    pub async fn unregister_broker(
        &self,
        namesrv_addr: &CheetahString,
        cluster_name: &CheetahString,
        broker_addr: &CheetahString,
        broker_name: &CheetahString,
        broker_id: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request_header = UnRegisterBrokerRequestHeader {
            broker_name: broker_name.clone(),
            broker_addr: broker_addr.clone(),
            cluster_name: cluster_name.clone(),
            broker_id,
        };
        let request = RemotingCommand::create_request_command(RequestCode::UnregisterBroker, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(namesrv_addr), request, 3000)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            Ok(())
        } else {
            Err(RocketMQError::BrokerOperationFailed {
                operation: "sync_consumer_offset_all",
                code: response.code(),
                message: response.remark().map_or("".to_string(), |s| s.to_string()),
                broker_addr: Some(broker_addr.to_string()),
            })
        }
    }

    pub async fn get_all_topic_config(
        &self,
        addr: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<TopicConfigAndMappingSerializeWrapper>> {
        let request = RemotingCommand::create_remoting_command(RequestCode::GetAllTopicConfig);
        let response = self
            .remoting_client
            .invoke_request(Some(mix_all::broker_vip_channel(true, addr).as_ref()), request, 3000)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.body() {
                let topic_configs = TopicConfigAndMappingSerializeWrapper::decode(body)?;
                return Ok(Some(topic_configs));
            }
            Ok(None)
        } else {
            Err(RocketMQError::BrokerOperationFailed {
                operation: "get_all_topic_config",
                code: response.code(),
                message: response.remark().map_or("".to_string(), |s| s.to_string()),
                broker_addr: Some(addr.to_string()),
            })
        }
    }

    pub async fn get_all_consumer_offset(
        &self,
        addr: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<ConsumerOffsetSerializeWrapper>> {
        let request = RemotingCommand::create_remoting_command(RequestCode::GetAllConsumerOffset);
        // let addr_ = mix_all::broker_vip_channel(true, addr);
        let response = self.remoting_client.invoke_request(Some(addr), request, 3000).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.body() {
                let topic_configs = ConsumerOffsetSerializeWrapper::decode(body)?;
                return Ok(Some(topic_configs));
            }
            Ok(None)
        } else {
            Err(RocketMQError::BrokerOperationFailed {
                operation: "get_all_consumer_offset",
                code: response.code(),
                message: response.remark().map_or("".to_string(), |s| s.to_string()),
                broker_addr: Some(addr.to_string()),
            })
        }
    }

    pub async fn get_delay_offset(&self, addr: &CheetahString) -> rocketmq_error::RocketMQResult<Option<String>> {
        let request = RemotingCommand::create_remoting_command(RequestCode::GetAllDelayOffset);
        let mut response = self.remoting_client.invoke_request(Some(addr), request, 3000).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.take_body() {
                return Ok(Some(String::from_utf8_lossy(body.as_ref()).to_string()));
            }
            Ok(None)
        } else {
            Err(RocketMQError::BrokerOperationFailed {
                operation: "get_delay_offset",
                code: response.code(),
                message: response.remark().map_or("".to_string(), |s| s.to_string()),
                broker_addr: Some(addr.to_string()),
            })
        }
    }

    pub async fn get_all_subscription_group_config(
        &self,
        addr: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<SubscriptionGroupWrapper>> {
        let request = RemotingCommand::create_remoting_command(RequestCode::GetAllSubscriptionGroupConfig);
        let mut response = self.remoting_client.invoke_request(Some(addr), request, 3000).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.take_body() {
                return Ok(Some(SubscriptionGroupWrapper::decode(body.as_ref())?));
            }
            Ok(None)
        } else {
            Err(RocketMQError::BrokerOperationFailed {
                operation: "get_all_subscription_group_config",
                code: response.code(),
                message: response.remark().map_or("".to_string(), |s| s.to_string()),
                broker_addr: Some(addr.to_string()),
            })
        }
    }

    pub async fn get_message_request_mode(
        &self,
        addr: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<Option<MessageRequestModeSerializeWrapper>> {
        let request = RemotingCommand::create_remoting_command(RequestCode::GetAllMessageRequestMode);
        let mut response = self.remoting_client.invoke_request(Some(addr), request, 3000).await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            if let Some(body) = response.take_body() {
                return Ok(Some(MessageRequestModeSerializeWrapper::decode(body.as_ref())?));
            }
            Ok(None)
        } else {
            Err(RocketMQError::BrokerOperationFailed {
                operation: "get_message_request_mode",
                code: response.code(),
                message: response.remark().map_or("".to_string(), |s| s.to_string()),
                broker_addr: Some(addr.to_string()),
            })
        }
    }

    pub async fn sync_broker_member_group(
        &self,
        _cluster_name: &CheetahString,
        _broker_name: &CheetahString,
        _is_compatible_with_old_name_srv: bool,
    ) -> rocketmq_error::RocketMQResult<Option<BrokerMemberGroup>> {
        unimplemented!()
    }

    /// Get controller metadata information
    pub async fn get_controller_metadata(
        &self,
        controller_address: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<GetMetaDataResponseHeader> {
        let request = RemotingCommand::create_remoting_command(RequestCode::ControllerGetMetadataInfo);
        let response = self
            .remoting_client
            .invoke_request(Some(controller_address), request, 3000)
            .await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => Ok(response.decode_command_custom_header::<GetMetaDataResponseHeader>()?),
            _ => Err(RocketMQError::BrokerOperationFailed {
                operation: "get_controller_metadata",
                code: response.code(),
                message: response.remark().map_or("".to_string(), |s| s.to_string()),
                broker_addr: Some(controller_address.to_string()),
            }),
        }
    }

    /// Send heartbeat to controller
    pub async fn send_heartbeat_to_controller(
        &self,
        controller_address: CheetahString,
        cluster_name: CheetahString,
        broker_addr: CheetahString,
        broker_name: CheetahString,
        broker_id: i64,
        timeout_millis: u64,
        epoch: Option<i32>,
        max_offset: Option<i64>,
        confirm_offset: Option<i64>,
        heartbeat_timeout_millis: Option<i64>,
        election_priority: Option<i32>,
    ) {
        if controller_address.is_empty() {
            return;
        }
        let request_header = BrokerHeartbeatRequestHeader {
            cluster_name,
            broker_addr,
            broker_name,
            broker_id: Some(broker_id),
            epoch,
            max_offset,
            confirm_offset,
            heartbeat_timeout_mills: heartbeat_timeout_millis,
            election_priority,
        };
        let client = self.remoting_client.clone();
        tokio::spawn(async move {
            let request = RemotingCommand::create_request_command(RequestCode::BrokerHeartbeat, request_header);
            client
                .invoke_request_oneway(&controller_address, request, timeout_millis)
                .await;
        });
    }

    /// Alter sync state set in controller
    pub async fn alter_sync_state_set(
        &self,
        controller_address: &CheetahString,
        broker_name: CheetahString,
        master_broker_id: i64,
        master_epoch: i32,
        new_sync_state_set: HashSet<i64>,
        sync_state_set_epoch: i32,
    ) -> rocketmq_error::RocketMQResult<SyncStateSet> {
        let request_header = AlterSyncStateSetRequestHeader {
            broker_name,
            master_broker_id,
            master_epoch,
        };
        let mut request =
            RemotingCommand::create_request_command(RequestCode::ControllerAlterSyncStateSet, request_header);
        request.set_body_mut_ref(SyncStateSet::with_values(new_sync_state_set, sync_state_set_epoch).encode()?);

        let response = self
            .remoting_client
            .invoke_request(Some(controller_address), request, 3000)
            .await?;

        if ResponseCode::from(response.code()) != ResponseCode::Success {
            return Err(RocketMQError::BrokerOperationFailed {
                operation: "alter_sync_state_set",
                code: response.code(),
                message: response
                    .remark()
                    .map_or("alter_sync_state_set failed".to_string(), |s| s.to_string()),
                broker_addr: Some(controller_address.to_string()),
            });
        }

        if response.body().is_none() {
            return Err(RocketMQError::BrokerOperationFailed {
                operation: "alter_sync_state_set",
                code: -1,
                message: "No body in alter_sync_state_set response".to_string(),
                broker_addr: Some(controller_address.to_string()),
            });
        }
        let body = response.body().unwrap();
        let sync_state_set: SyncStateSet = SyncStateSet::decode(body.as_ref())?;
        Ok(sync_state_set)
    }

    /// Broker elect itself as master
    pub async fn broker_elect(
        &self,
        controller_address: &CheetahString,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_id: i64,
    ) -> rocketmq_error::RocketMQResult<(ElectMasterResponseHeader, HashSet<i64>)> {
        let request_header = ElectMasterRequestHeader {
            cluster_name,
            broker_name,
            broker_id,
            ..Default::default()
        };
        let request = RemotingCommand::create_request_command(RequestCode::ControllerElectMaster, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(controller_address), request, 3000)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success | ResponseCode::ControllerMasterStillExist => {
                let response_header = response
                    .decode_command_custom_header::<ElectMasterResponseHeader>()
                    .map_err(|e| RocketMQError::BrokerOperationFailed {
                        operation: "broker_elect",
                        code: -1,
                        message: format!("Failed to decode elect response: {:?}", e),
                        broker_addr: Some(controller_address.to_string()),
                    })?;
                if response.body().is_none() {
                    return Err(RocketMQError::BrokerOperationFailed {
                        operation: "broker_elect",
                        code: -1,
                        message: "No body in broker_elect response".to_string(),
                        broker_addr: Some(controller_address.to_string()),
                    });
                }
                let body = response.body().unwrap();
                let elect_master_response_body = ElectMasterResponseBody::decode(body.as_ref())?;
                Ok((response_header, elect_master_response_body.sync_state_set))
            }
            _ => Err(RocketMQError::BrokerOperationFailed {
                operation: "broker_elect",
                code: response.code(),
                message: response
                    .remark()
                    .map_or("broker_elect failed".to_string(), |s| s.to_string()),
                broker_addr: Some(controller_address.to_string()),
            }),
        }
    }

    /// Get next broker ID from controller
    pub async fn get_next_broker_id(
        &self,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        controller_address: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<GetNextBrokerIdResponseHeader> {
        let request_header = GetNextBrokerIdRequestHeader {
            cluster_name,
            broker_name,
        };
        let request = RemotingCommand::create_request_command(RequestCode::ControllerGetNextBrokerId, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(controller_address), request, 3000)
            .await?;

        if ResponseCode::from(response.code()) != ResponseCode::Success {
            return Err(RocketMQError::BrokerOperationFailed {
                operation: "get_next_broker_id",
                code: response.code(),
                message: response
                    .remark()
                    .map_or("get_next_broker_id failed".to_string(), |s| s.to_string()),
                broker_addr: Some(controller_address.to_string()),
            });
        }

        if let Some(body) = response.body() {
            let next_broker_id = GetNextBrokerIdResponseHeader::decode(body.as_ref())?;
            Ok(next_broker_id)
        } else {
            Err(RocketMQError::BrokerOperationFailed {
                operation: "get_next_broker_id",
                code: -1,
                message: "No next broker id in response".to_string(),
                broker_addr: Some(controller_address.to_string()),
            })
        }
    }

    /// Apply broker ID from controller
    pub async fn apply_broker_id(
        &self,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_id: i64,
        register_check_code: CheetahString,
        controller_address: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<ApplyBrokerIdResponseHeader> {
        let request_header = ApplyBrokerIdRequestHeader {
            cluster_name,
            broker_name,
            applied_broker_id: broker_id,
            register_check_code,
        };
        let request = RemotingCommand::create_request_command(RequestCode::ControllerApplyBrokerId, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(controller_address), request, 3000)
            .await?;

        if ResponseCode::from(response.code()) != ResponseCode::Success {
            return Err(RocketMQError::BrokerOperationFailed {
                operation: "apply_broker_id",
                code: response.code(),
                message: response
                    .remark()
                    .map_or("apply_broker_id failed".to_string(), |s| s.to_string()),
                broker_addr: Some(controller_address.to_string()),
            });
        }

        let response_header = response
            .decode_command_custom_header::<ApplyBrokerIdResponseHeader>()
            .map_err(|e| RocketMQError::BrokerOperationFailed {
                operation: "apply_broker_id",
                code: -1,
                message: format!("Failed to decode apply broker id response: {:?}", e),
                broker_addr: Some(controller_address.to_string()),
            })?;
        Ok(response_header)
    }

    /// Register broker to controller
    pub async fn register_broker_to_controller(
        &self,
        cluster_name: CheetahString,
        broker_name: CheetahString,
        broker_id: i64,
        broker_address: CheetahString,
        controller_address: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<(RegisterBrokerToControllerResponseHeader, Option<HashSet<i64>>)> {
        let request_header = RegisterBrokerToControllerRequestHeader {
            cluster_name: Some(cluster_name),
            broker_name: Some(broker_name),
            broker_id: Some(broker_id),
            broker_address: Some(broker_address),
            ..Default::default()
        };
        let request = RemotingCommand::create_request_command(RequestCode::ControllerRegisterBroker, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(controller_address), request, 3000)
            .await?;

        if ResponseCode::from(response.code()) != ResponseCode::Success {
            return Err(RocketMQError::BrokerOperationFailed {
                operation: "register_broker_to_controller",
                code: response.code(),
                message: response
                    .remark()
                    .map_or("register_broker_to_controller failed".to_string(), |s| s.to_string()),
                broker_addr: Some(controller_address.to_string()),
            });
        }

        let response_header = response
            .decode_command_custom_header::<RegisterBrokerToControllerResponseHeader>()
            .map_err(|e| RocketMQError::BrokerOperationFailed {
                operation: "register_broker_to_controller",
                code: -1,
                message: format!("Failed to decode register response: {:?}", e),
                broker_addr: Some(controller_address.to_string()),
            })?;

        if response.body().is_none() {
            return Err(RocketMQError::BrokerOperationFailed {
                operation: "register_broker_to_controller",
                code: -1,
                message: "No body in register_broker_to_controller response".to_string(),
                broker_addr: Some(controller_address.to_string()),
            });
        }
        let body = response.body().unwrap();
        let mut sync_state_set = SyncStateSet::decode(body.as_ref())?;

        Ok((response_header, sync_state_set.take_sync_state_set()))
    }

    /// Get replica info from controller
    /// Returns: (header, SyncStateSet)
    pub async fn get_replica_info(
        &self,
        controller_address: &CheetahString,
        broker_name: CheetahString,
    ) -> rocketmq_error::RocketMQResult<(GetReplicaInfoResponseHeader, SyncStateSet)> {
        let request_header = GetReplicaInfoRequestHeader { broker_name };
        let request = RemotingCommand::create_request_command(RequestCode::ControllerGetReplicaInfo, request_header);

        let response = self
            .remoting_client
            .invoke_request(Some(controller_address), request, 3000)
            .await?;

        if ResponseCode::from(response.code()) != ResponseCode::Success {
            return Err(RocketMQError::BrokerOperationFailed {
                operation: "get_replica_info",
                code: response.code(),
                message: response
                    .remark()
                    .map_or("get_replica_info failed".to_string(), |s| s.to_string()),
                broker_addr: Some(controller_address.to_string()),
            });
        }

        let response_header = response
            .decode_command_custom_header::<GetReplicaInfoResponseHeader>()
            .map_err(|e| RocketMQError::BrokerOperationFailed {
                operation: "get_replica_info",
                code: -1,
                message: format!("Failed to decode replica info response: {:?}", e),
                broker_addr: Some(controller_address.to_string()),
            })?;

        if response.body().is_none() {
            return Err(RocketMQError::BrokerOperationFailed {
                operation: "get_replica_info",
                code: -1,
                message: "No body in get_replica_info response".to_string(),
                broker_addr: Some(controller_address.to_string()),
            });
        }
        let body = response.body().unwrap();
        let sync_state_set = SyncStateSet::decode(body.as_ref())?;

        Ok((response_header, sync_state_set))
    }

    pub async fn send_broker_ha_info(
        &self,
        broker_addr: &CheetahString,
        master_ha_addr: &CheetahString,
        broker_init_max_offset: i64,
        master_addr: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<()> {
        let request_header = ExchangeHAInfoRequestHeader {
            master_ha_address: Some(master_ha_addr.clone()),
            master_flush_offset: Some(broker_init_max_offset),
            master_address: Some(master_addr.clone()),
        };
        let request = RemotingCommand::create_request_command(RequestCode::ExchangeBrokerHaInfo, request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(broker_addr), request, 3000)
            .await?;
        if ResponseCode::from(response.code()) == ResponseCode::Success {
            Ok(())
        } else {
            Err(RocketMQError::BrokerOperationFailed {
                operation: "sync_broker_member_group",
                code: response.code(),
                message: response.remark().map_or("".to_string(), |s| s.to_string()),
                broker_addr: Some(broker_addr.to_string()),
            })
        }
    }

    pub async fn retrieve_broker_ha_info(
        &self,
        master_broker_addr: Option<&CheetahString>,
    ) -> rocketmq_error::RocketMQResult<BrokerSyncInfo> {
        let request_header = ExchangeHAInfoRequestHeader::default();
        let request = RemotingCommand::create_request_command(RequestCode::ExchangeBrokerHaInfo, request_header);
        let response = self
            .remoting_client
            .invoke_request(master_broker_addr, request, 3000)
            .await?;

        if ResponseCode::from(response.code()) == ResponseCode::Success {
            let header = response.decode_command_custom_header::<ExchangeHaInfoResponseHeader>()?;
            return Ok(BrokerSyncInfo {
                master_address: header.master_address,
                master_ha_address: header.master_ha_address,
                master_flush_offset: header.master_flush_offset.unwrap_or(0),
            });
        }

        Err(RocketMQError::BrokerOperationFailed {
            operation: "retrieve_broker_ha_info",
            code: response.code(),
            message: response.remark().map_or("".to_string(), |s| s.to_string()),
            broker_addr: master_broker_addr.map(|s| s.to_string()),
        })
    }

    pub fn close_channel(&self, addr_list: Vec<String>) {
        self.remoting_client.mut_from_ref().close_clients(addr_list);
    }
}

fn process_pull_result(pull_result: &mut PullResultExt, broker_name: &CheetahString, queue_id: i32) {
    if *pull_result.pull_result.pull_status() == PullStatus::Found {
        let mut bytes = pull_result.message_binary.take().unwrap_or_default();
        let mut message_list = MessageDecoder::decodes_batch(&mut bytes, true, true);
        for message in message_list.iter_mut() {
            let tra_flag = message.get_property(&CheetahString::from_static_str(
                MessageConst::PROPERTY_TRANSACTION_PREPARED,
            ));
            if tra_flag.is_some() && tra_flag.unwrap() == "true" {
                if let Some(id) = message.get_property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
                )) {
                    message.set_transaction_id(id);
                }
            }
            MessageAccessor::put_property(
                message,
                CheetahString::from_static_str(MessageConst::PROPERTY_MIN_OFFSET),
                pull_result.pull_result.min_offset().to_string().into(),
            );
            MessageAccessor::put_property(
                message,
                CheetahString::from_static_str(MessageConst::PROPERTY_MAX_OFFSET),
                pull_result.pull_result.max_offset().to_string().into(),
            );
            message.set_broker_name(broker_name.clone());
            message.set_queue_id(queue_id);
            if let Some(offset_delta) = pull_result.offset_delta {
                message.set_queue_offset(message.queue_offset + offset_delta);
            }
        }
    }
}

fn process_pull_response(
    mut response: RemotingCommand,
    addr: &CheetahString,
) -> rocketmq_error::RocketMQResult<PullResultExt> {
    let pull_status = match ResponseCode::from(response.code()) {
        ResponseCode::Success => PullStatus::Found,
        ResponseCode::PullNotFound => PullStatus::NoNewMsg,
        ResponseCode::PullRetryImmediately => PullStatus::NoMatchedMsg,
        ResponseCode::PullOffsetMoved => PullStatus::OffsetIllegal,
        _ => {
            return Err(RocketMQError::BrokerOperationFailed {
                operation: "pull_message",
                code: response.code(),
                message: response.remark().map_or("".to_string(), |s| s.to_string()),
                broker_addr: Some(addr.to_string()),
            })
        }
    };
    let response_header = response.decode_command_custom_header::<PullMessageResponseHeader>()?;
    let pull_result = PullResultExt {
        pull_result: PullResult::new(
            pull_status,
            response_header.next_begin_offset as u64,
            response_header.min_offset as u64,
            response_header.max_offset as u64,
            Some(vec![]),
        ),
        suggest_which_broker_id: response_header.suggest_which_broker_id,
        message_binary: response.take_body(),
        offset_delta: response_header.offset_delta,
    };
    Ok(pull_result)
}

fn dns_lookup_address_by_domain(domain: &str) -> Vec<CheetahString> {
    let mut address_list = Vec::new();
    // Ensure logging is initialized

    match domain.find(':') {
        Some(index) => {
            let (domain_str, port_str) = domain.split_at(index);
            match lookup_host(domain_str) {
                Ok(addresses) => {
                    for address in addresses {
                        address_list.push(format!("{address}{port_str}").into());
                    }
                    info!(
                        "DNS lookup address by domain success, domain={}, result={:?}",
                        domain, address_list
                    );
                }
                Err(e) => {
                    error!("DNS lookup address by domain error, domain={}, error={}", domain, e);
                }
            }
        }
        None => {
            error!("Invalid domain format, missing port: {}", domain);
        }
    }

    address_list
}

fn build_send_message_request(msg: MessageExt, group: CheetahString) -> RemotingCommand {
    let header = build_send_message_request_header_v2(msg, group);
    RemotingCommand::create_request_command(RequestCode::SendMessage, header)
}

fn build_send_message_request_header_v2(msg: MessageExt, group: CheetahString) -> SendMessageRequestHeaderV2 {
    let header = SendMessageRequestHeader {
        producer_group: group,
        topic: msg.get_topic().clone(),
        default_topic: CheetahString::from_static_str(TopicValidator::AUTO_CREATE_TOPIC_KEY_TOPIC),
        default_topic_queue_nums: 8,
        queue_id: msg.queue_id,
        sys_flag: msg.sys_flag,
        born_timestamp: msg.born_timestamp,
        flag: msg.get_flag(),
        properties: Some(MessageDecoder::message_properties_to_string(msg.get_properties())),
        reconsume_times: Some(msg.reconsume_times),
        batch: Some(false),
        ..Default::default()
    };
    SendMessageRequestHeaderV2::create_send_message_request_header_v2_with_move(header)
}

pub fn process_send_response(
    broker_name: &CheetahString,
    uniq_msg_id: CheetahString,
    queue_id: i32,
    topic: CheetahString,
    response: &RemotingCommand,
) -> rocketmq_error::RocketMQResult<SendResult> {
    let mut send_status: Option<SendStatus> = None;

    // Match the response code to the corresponding SendStatus
    match ResponseCode::from(response.code()) {
        ResponseCode::FlushDiskTimeout => send_status = Some(SendStatus::FlushDiskTimeout),
        ResponseCode::FlushSlaveTimeout => send_status = Some(SendStatus::FlushSlaveTimeout),
        ResponseCode::SlaveNotAvailable => send_status = Some(SendStatus::SlaveNotAvailable),
        ResponseCode::Success => send_status = Some(SendStatus::SendOk),
        _ => (),
    };

    // If send_status is not None, process the response
    if let Some(status) = send_status {
        let response_header = response.decode_command_custom_header::<SendMessageResponseHeader>()?;

        let message_queue = MessageQueue::from_parts(topic, broker_name, queue_id);

        let mut send_result = SendResult::new(
            status,
            Some(uniq_msg_id),
            Some(response_header.msg_id().to_string()),
            Some(message_queue),
            response_header.queue_id() as u64,
        );

        send_result.set_transaction_id(
            response_header
                .transaction_id()
                .map_or("".to_string(), |s| s.to_string()),
        );
        if let Some(region_id) = response
            .get_ext_fields()
            .unwrap()
            .get(&CheetahString::from_static_str(MessageConst::PROPERTY_MSG_REGION))
        {
            send_result.set_region_id(region_id.to_string());
        } else {
            send_result.set_region_id(mix_all::DEFAULT_TRACE_REGION_ID.to_string());
        }

        if let Some(trace_on) = response
            .get_ext_fields()
            .unwrap()
            .get(&CheetahString::from_static_str(MessageConst::PROPERTY_MSG_REGION))
        {
            send_result.set_trace_on(trace_on == "true");
        } else {
            send_result.set_trace_on(false);
        }
        return Ok(send_result);
    }

    // If send_status is None, we throw an error
    Err(RocketMQError::BrokerOperationFailed {
        operation: "send_message",
        code: response.code(),
        message: response.remark().map_or("".to_string(), |s| s.to_string()),
        broker_addr: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dns_lookup_address_by_domain_returns_correct_addresses() {
        let domain = "localhost:8080";
        let addresses = dns_lookup_address_by_domain(domain);
        assert!(addresses.contains(&"127.0.0.1:8080".into()));
    }

    #[test]
    fn dns_lookup_address_by_domain_handles_invalid_domain() {
        let domain = "invalid_domain";
        let addresses = dns_lookup_address_by_domain(domain);
        assert!(addresses.is_empty());
    }

    #[test]
    fn dns_lookup_address_by_domain_handles_domain_without_port() {
        let domain = "localhost";
        let addresses = dns_lookup_address_by_domain(domain);
        assert!(addresses.is_empty());
    }
}
