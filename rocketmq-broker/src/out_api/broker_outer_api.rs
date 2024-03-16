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

use rocketmq_common::{
    common::broker::broker_config::BrokerIdentity, utils::crc32_utils, TokioExecutorService,
};
use rocketmq_remoting::{
    clients::{rocketmq_default_impl::RocketmqDefaultClient, RemotingClient},
    code::{request_code::RequestCode, response_code::RemotingSysResponseCode},
    error::RemotingError,
    protocol::{
        body::{
            broker_body::register_broker_body::RegisterBrokerBody, kv_table::KVTable,
            topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper,
        },
        header::namesrv::register_broker_header::{
            RegisterBrokerRequestHeader, RegisterBrokerResponseHeader,
        },
        namesrv::RegisterBrokerResult,
        remoting_command::RemotingCommand,
        RemotingSerializable,
    },
    remoting::RemotingService,
    runtime::{config::client_config::TokioClientConfig, RPCHook},
};

pub struct BrokerOuterAPI {
    remoting_client: Arc<std::sync::Mutex<RocketmqDefaultClient>>,
    name_server_address: Option<String>,
    broker_outer_executor: TokioExecutorService,
}

impl BrokerOuterAPI {
    pub fn new(tokio_client_config: TokioClientConfig) -> Self {
        let client = RocketmqDefaultClient::new(tokio_client_config);
        Self {
            remoting_client: Arc::new(std::sync::Mutex::new(client)),
            name_server_address: None,
            broker_outer_executor: Default::default(),
        }
    }

    pub fn new_with_hook(
        tokio_client_config: TokioClientConfig,
        rpc_hook: Option<impl RPCHook>,
    ) -> Self {
        let mut client = RocketmqDefaultClient::new(tokio_client_config);
        if let Some(rpc_hook) = rpc_hook {
            client.register_rpc_hook(rpc_hook);
        }
        Self {
            remoting_client: Arc::new(std::sync::Mutex::new(client)),
            name_server_address: None,
            broker_outer_executor: Default::default(),
        }
    }
}

impl BrokerOuterAPI {
    pub fn update_name_server_address_list(&mut self, addrs: String) {
        let addr_vec = addrs
            .split("';'")
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        self.remoting_client
            .lock()
            .unwrap()
            .update_name_server_address_list(addr_vec)
    }

    pub async fn register_broker_all(
        &mut self,
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
        let mut register_broker_result_list = Vec::new();
        let name_server_address_list = self
            .remoting_client
            .lock()
            .unwrap()
            .get_available_name_srv_list();

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
                let remoting_client_arc = self.remoting_client.clone();
                let string = namesrv_addr.clone();
                let handle = self.broker_outer_executor.get_handle().spawn(async move {
                    Self::register_broker(
                        remoting_client_arc,
                        string,
                        oneway,
                        timeout_mills,
                        cloned_header,
                        cloned_body,
                    )
                });
                handle_vec.push(handle);
            }

            for handle in handle_vec {
                register_broker_result_list.push(handle.await.unwrap().unwrap().unwrap());
            }
        }

        register_broker_result_list
    }

    fn register_broker(
        remoting_client: Arc<std::sync::Mutex<RocketmqDefaultClient>>,
        namesrv_addr: String,
        oneway: bool,
        timeout_mills: u64,
        request_header: RegisterBrokerRequestHeader,
        body: Vec<u8>,
    ) -> Result<Option<RegisterBrokerResult>, RemotingError> {
        let request =
            RemotingCommand::create_request_command(RequestCode::RegisterBroker, request_header)
                .set_body(Some(body.clone()));

        if oneway {
            match remoting_client.lock().unwrap().invoke_oneway(
                namesrv_addr,
                request,
                timeout_mills,
            ) {
                Ok(_) => return Ok(None),
                Err(_) => {
                    // Ignore
                    return Ok(None);
                }
            }
        }

        match remoting_client
            .lock()
            .unwrap()
            .invoke_sync(namesrv_addr, request, timeout_mills)
        {
            Ok(response) => match From::from(response.code()) {
                RemotingSysResponseCode::Success => {
                    let response_header = response
                        .decode_command_custom_header::<RegisterBrokerResponseHeader>()
                        .unwrap();
                    let mut result = RegisterBrokerResult {
                        master_addr: response_header.master_addr.clone().unwrap(),
                        ha_server_addr: response_header.ha_server_addr.clone().unwrap(),
                        ..Default::default()
                    };
                    if let Some(body) = response.body() {
                        result.kv_table = KVTable::decode(body);
                    }
                    Ok(Some(result))
                }
                _ => Ok(None),
            },
            Err(_err) => Err(RemotingError::RemotingCommandException("1111".to_string())),
        }
    }
}
