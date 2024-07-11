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
use std::sync::Arc;

use dns_lookup::lookup_host;
use rocketmq_common::common::broker::broker_config::BrokerIdentity;
use rocketmq_common::common::config::TopicConfig;
use rocketmq_common::utils::crc32_utils;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_remoting::clients::rocketmq_default_impl::RocketmqDefaultClient;
use rocketmq_remoting::clients::RemotingClient;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::protocol::body::broker_body::register_broker_body::RegisterBrokerBody;
use rocketmq_remoting::protocol::body::kv_table::KVTable;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use rocketmq_remoting::protocol::header::namesrv::register_broker_header::RegisterBrokerRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::register_broker_header::RegisterBrokerResponseHeader;
use rocketmq_remoting::protocol::header::namesrv::topic_operation_header::RegisterTopicRequestHeader;
use rocketmq_remoting::protocol::namesrv::RegisterBrokerResult;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::route::route_data_view::QueueData;
use rocketmq_remoting::protocol::route::route_data_view::TopicRouteData;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::remoting::RemotingService;
use rocketmq_remoting::rpc::client_metadata::ClientMetadata;
use rocketmq_remoting::rpc::rpc_client_impl::RpcClientImpl;
use rocketmq_remoting::runtime::config::client_config::TokioClientConfig;
use rocketmq_remoting::runtime::RPCHook;
use tracing::error;
use tracing::info;

#[derive(Clone)]
pub struct BrokerOuterAPI {
    remoting_client: RocketmqDefaultClient,
    name_server_address: Option<String>,
    rpc_client: RpcClientImpl,
    client_metadata: ClientMetadata,
}

impl BrokerOuterAPI {
    pub fn new(tokio_client_config: Arc<TokioClientConfig>) -> Self {
        let client = RocketmqDefaultClient::new(tokio_client_config);
        let client_metadata = ClientMetadata::new();
        Self {
            remoting_client: client.clone(),
            name_server_address: None,
            rpc_client: RpcClientImpl::new(client_metadata.clone(), client),
            client_metadata,
        }
    }

    pub fn new_with_hook(
        tokio_client_config: Arc<TokioClientConfig>,
        rpc_hook: Option<impl RPCHook>,
    ) -> Self {
        let mut client = RocketmqDefaultClient::new(tokio_client_config);
        let client_metadata = ClientMetadata::new();
        if let Some(rpc_hook) = rpc_hook {
            client.register_rpc_hook(rpc_hook);
        }
        Self {
            remoting_client: client.clone(),
            name_server_address: None,
            rpc_client: RpcClientImpl::new(client_metadata.clone(), client),
            client_metadata,
        }
    }

    fn create_request(broker_name: String, topic_config: TopicConfig) -> RemotingCommand {
        let request_header =
            RegisterTopicRequestHeader::new(topic_config.topic_name.as_ref().unwrap());
        let queue_data = QueueData::new(
            broker_name.clone(),
            topic_config.read_queue_nums,
            topic_config.write_queue_nums,
            topic_config.perm,
            topic_config.topic_sys_flag,
        );
        let topic_route_data = TopicRouteData {
            queue_datas: vec![queue_data],
            ..Default::default()
        };
        let topic_route_body = topic_route_data.encode();

        RemotingCommand::create_request_command(RequestCode::RegisterTopicInNamesrv, request_header)
            .set_body(Some(topic_route_body))
    }
}

impl BrokerOuterAPI {
    pub async fn start(&self) {
        self.remoting_client.start().await;
    }

    pub fn update_name_server_address_list(&self, addrs: String) {
        let addr_vec = addrs
            .split("';'")
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        self.remoting_client
            .update_name_server_address_list(addr_vec)
    }

    pub fn update_name_server_address_list_by_dns_lookup(&self, domain: String) {
        let address_list = dns_lookup_address_by_domain(domain.as_str());
        self.remoting_client
            .update_name_server_address_list(address_list);
    }

    pub async fn register_broker_all(
        &self,
        cluster_name: String,
        broker_addr: String,
        broker_name: String,
        broker_id: u64,
        ha_server_addr: String,
        topic_config_wrapper: TopicConfigAndMappingSerializeWrapper,
        filter_server_list: Vec<String>,
        oneway: bool,
        timeout_mills: u64,
        enable_acting_master: bool,
        compressed: bool,
        heartbeat_timeout_millis: Option<i64>,
        _broker_identity: BrokerIdentity,
    ) -> Vec<RegisterBrokerResult> {
        let name_server_address_list = self.remoting_client.get_available_name_srv_list();
        let mut register_broker_result_list = Vec::new();
        if !name_server_address_list.is_empty() {
            let mut request_header = RegisterBrokerRequestHeader {
                broker_addr,
                broker_id: broker_id as i64,
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
                let outer_api = self.clone();
                let join_handle = tokio::spawn(async move {
                    outer_api
                        .register_broker(addr, oneway, timeout_mills, cloned_header, cloned_body)
                        .await
                });
                /*let handle =
                self.register_broker(addr, oneway, timeout_mills, cloned_header, cloned_body);*/
                handle_vec.push(join_handle);
            }
            while let Some(handle) = handle_vec.pop() {
                let result = tokio::join!(handle);
                register_broker_result_list.push(result.0.unwrap().unwrap());
            }
        }

        register_broker_result_list
    }

    async fn register_broker(
        &self,
        namesrv_addr: String,
        oneway: bool,
        timeout_mills: u64,
        request_header: RegisterBrokerRequestHeader,
        body: Vec<u8>,
    ) -> Option<RegisterBrokerResult> {
        let request =
            RemotingCommand::create_request_command(RequestCode::RegisterBroker, request_header)
                .set_body(Some(body.clone()));

        if oneway {
            self.remoting_client
                .invoke_oneway(namesrv_addr, request, timeout_mills)
                .await;
            return None;
        }
        match self
            .remoting_client
            .invoke_async(namesrv_addr.clone(), request, timeout_mills)
            .await
        {
            Ok(response) => match From::from(response.code()) {
                ResponseCode::Success => {
                    let register_broker_result =
                        response.decode_command_custom_header::<RegisterBrokerResponseHeader>();
                    let mut result = RegisterBrokerResult::default();
                    if let Some(header) = register_broker_result {
                        result.ha_server_addr =
                            header.ha_server_addr.clone().unwrap_or("".to_string());
                        result.master_addr = header.master_addr.clone().unwrap_or("".to_string());
                    }
                    if let Some(body) = response.body() {
                        result.kv_table = SerdeJsonUtils::decode::<KVTable>(body.as_ref()).unwrap();
                    }
                    Some(result)
                }
                _ => None,
            },
            Err(err) => {
                error!(
                    "Register broker to name server error, namesrv_addr={}, error={}",
                    namesrv_addr, err
                );
                None
            }
        }
    }

    /// Register the topic route info of single topic to all name server nodes.
    /// This method is used to replace incremental broker registration feature.
    pub async fn register_single_topic_all(
        &self,
        broker_name: String,
        topic_config: TopicConfig,
        timeout_mills: u64,
    ) {
        let request = Self::create_request(broker_name, topic_config);
        let name_server_address_list = self.remoting_client.get_available_name_srv_list();
        let mut handle_vec = Vec::with_capacity(name_server_address_list.len());
        for namesrv_addr in name_server_address_list.iter() {
            let cloned_request = request.clone();
            let addr = namesrv_addr.clone();
            let client = self.remoting_client.clone();
            let join_handle = tokio::spawn(async move {
                client
                    .invoke_async(addr, cloned_request, timeout_mills)
                    .await
            });
            handle_vec.push(join_handle);
        }
        while let Some(handle) = handle_vec.pop() {
            let _result = tokio::join!(handle);
        }
    }

    pub fn shutdown(&self) {}

    pub fn refresh_metadata(&self) {}

    pub fn rpc_client(&self) -> &RpcClientImpl {
        &self.rpc_client
    }
}

fn dns_lookup_address_by_domain(domain: &str) -> Vec<String> {
    let mut address_list = Vec::new();
    // Ensure logging is initialized

    match domain.find(':') {
        Some(index) => {
            let (domain_str, port_str) = domain.split_at(index);
            match lookup_host(domain_str) {
                Ok(addresses) => {
                    for address in addresses {
                        address_list.push(format!("{}{}", address, port_str));
                    }
                    info!(
                        "DNS lookup address by domain success, domain={}, result={:?}",
                        domain, address_list
                    );
                }
                Err(e) => {
                    error!(
                        "DNS lookup address by domain error, domain={}, error={}",
                        domain, e
                    );
                }
            }
        }
        None => {
            error!("Invalid domain format, missing port: {}", domain);
        }
    }

    address_list
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dns_lookup_address_by_domain_returns_correct_addresses() {
        let domain = "localhost:8080";
        let addresses = dns_lookup_address_by_domain(domain);
        assert!(addresses.contains(&"127.0.0.1:8080".to_string()));
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
