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

use bytes::Bytes;
use rocketmq_common::{
    common::{mix_all, mq_version::RocketMqVersion, namesrv::namesrv_config::NamesrvConfig},
    CRC32Utils,
};
use rocketmq_remoting::{
    code::{request_code::RequestCode, response_code::RemotingSysResponseCode},
    protocol::{
        body::{
            broker_body::register_broker_body::RegisterBrokerBody,
            topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper,
        },
        header::namesrv::{
            kv_config_request_header::PutKVConfigRequestHeader,
            register_broker_header::{RegisterBrokerRequestHeader, RegisterBrokerResponseHeader},
        },
        remoting_command::RemotingCommand,
        RemotingSerializable,
    },
    runtime::processor::RequestProcessor,
};
use tracing::warn;

use crate::{route::route_info_manager::RouteInfoManager, KVConfigManager};

#[derive(Debug, Clone)]
pub struct DefaultRequestProcessor {
    route_info_manager: Arc<parking_lot::RwLock<RouteInfoManager>>,
    kvconfig_manager: Arc<parking_lot::RwLock<KVConfigManager>>,
}

impl RequestProcessor for DefaultRequestProcessor {
    fn process_request(&mut self, request: RemotingCommand) -> RemotingCommand {
        let code = request.code();
        let broker_request_code = RequestCode::value_of(code);
        match broker_request_code {
            //handle register broker
            Some(RequestCode::RegisterBroker) => self.process_register_broker(request),
            Some(RequestCode::BrokerHeartbeat) => self.process_broker_heartbeat(request),
            //handle get broker cluster info
            Some(RequestCode::GetBrokerClusterInfo) => {
                self.process_get_broker_cluster_info(request)
            }
            Some(RequestCode::PutKvConfig) => self.put_kv_config(request),
            _ => RemotingCommand::create_response_command_with_code(
                RemotingSysResponseCode::SystemError,
            ),
        }
    }
}

///implementation put KV config
impl DefaultRequestProcessor {
    fn put_kv_config(&mut self, request: RemotingCommand) -> RemotingCommand {
        let request_header = request
            .decode_command_custom_header::<PutKVConfigRequestHeader>()
            .unwrap();
        //check namespace and key, need?
        if request_header.namespace.is_empty() || request_header.key.is_empty() {
            return RemotingCommand::create_response_command_with_code(
                RemotingSysResponseCode::SystemError,
            )
            .set_remark(Some(String::from("namespace or key is empty")));
        }
        self.kvconfig_manager.write().put_kv_config(
            request_header.namespace.as_str(),
            request_header.key.as_str(),
            request_header.value.as_str(),
        );
        RemotingCommand::create_response_command()
    }
}

#[allow(clippy::new_without_default)]
impl DefaultRequestProcessor {
    pub fn new(namesrv_config: NamesrvConfig) -> Self {
        Self {
            route_info_manager: Arc::new(parking_lot::RwLock::new(RouteInfoManager::new())),
            kvconfig_manager: Arc::new(parking_lot::RwLock::new(KVConfigManager::new(
                namesrv_config,
            ))),
        }
    }

    pub(crate) fn new_with_route_info_manager(
        route_info_manager: Arc<parking_lot::RwLock<RouteInfoManager>>,
        kvconfig_manager: Arc<parking_lot::RwLock<KVConfigManager>>,
    ) -> Self {
        Self {
            route_info_manager,
            kvconfig_manager,
        }
    }
}
impl DefaultRequestProcessor {
    fn process_register_broker(&mut self, request: RemotingCommand) -> RemotingCommand {
        let request_header = request
            .decode_command_custom_header::<RegisterBrokerRequestHeader>()
            .unwrap();
        if !check_sum_crc32(&request, &request_header) {
            return RemotingCommand::create_response_command_with_code(
                RemotingSysResponseCode::SystemError,
            )
            .set_remark(Some(String::from("crc32 not match")));
        }

        let mut response_command = RemotingCommand::create_response_command();
        let broker_version = RocketMqVersion::try_from(request.version()).unwrap();
        let topic_config_wrapper;
        let mut filter_server_list = Vec::<String>::new();
        if broker_version as usize >= RocketMqVersion::V3011 as usize {
            let register_broker_body =
                extract_register_broker_body_from_request(&request, &request_header);
            topic_config_wrapper = register_broker_body
                .topic_config_serialize_wrapper()
                .clone();
            register_broker_body
                .filter_server_list()
                .iter()
                .for_each(|s| {
                    filter_server_list.push(s.clone());
                });
        } else {
            topic_config_wrapper = extract_register_topic_config_from_request(&request);
        }
        let result = self.route_info_manager.write().register_broker(
            request_header.cluster_name.clone(),
            request_header.broker_addr.clone(),
            request_header.broker_name.clone(),
            request_header.broker_id,
            request_header.ha_server_addr.clone(),
            match request.ext_fields() {
                Some(map) => map.get(mix_all::ZONE_NAME).map(|value| value.to_string()),
                None => None,
            },
            request_header.heartbeat_timeout_millis,
            request_header.enable_acting_master,
            topic_config_wrapper,
            filter_server_list,
        );
        if result.is_none() {
            return response_command
                .set_code(RemotingSysResponseCode::SystemError)
                .set_remark(Some(String::from("register broker failed")));
        }
        if self
            .kvconfig_manager
            .read()
            .namesrv_config
            .return_order_topic_config_to_broker
        {
            if let Some(value) = self
                .kvconfig_manager
                .write()
                .get_kv_list_by_namespace("ORDER_TOPIC_CONFIG")
            {
                response_command = response_command.set_body(Some(Bytes::from(value)));
            }
        }
        let register_broker_result = result.unwrap();
        response_command
            .set_code(RemotingSysResponseCode::Success)
            .set_command_custom_header(Some(Box::new(RegisterBrokerResponseHeader::new(
                Some(register_broker_result.ha_server_addr),
                Some(register_broker_result.master_addr),
            ))))
    }
}

impl DefaultRequestProcessor {
    fn process_broker_heartbeat(&mut self, _request: RemotingCommand) -> RemotingCommand {
        RemotingCommand::create_response_command_with_code(RemotingSysResponseCode::Success)
    }
}

impl DefaultRequestProcessor {
    fn process_get_broker_cluster_info(&mut self, _request: RemotingCommand) -> RemotingCommand {
        let vec = self
            .route_info_manager
            .write()
            .get_all_cluster_info()
            .encode();
        RemotingCommand::create_response_command_with_code(RemotingSysResponseCode::Success)
            .set_body(Some(Bytes::from(vec)))
    }
}

fn extract_register_topic_config_from_request(
    request: &RemotingCommand,
) -> TopicConfigAndMappingSerializeWrapper {
    if let Some(body_inner) = request.body() {
        return TopicConfigAndMappingSerializeWrapper::decode(body_inner.iter().as_slice());
    }
    TopicConfigAndMappingSerializeWrapper::default()
}

fn extract_register_broker_body_from_request(
    request: &RemotingCommand,
    request_header: &RegisterBrokerRequestHeader,
) -> RegisterBrokerBody {
    if let Some(_body_inner) = request.body() {
        let version = RocketMqVersion::try_from(request.version()).unwrap();
        return RegisterBrokerBody::decode(
            request.body().as_ref().unwrap(),
            request_header.compressed,
            version,
        );
    }
    RegisterBrokerBody::default()
}

fn check_sum_crc32(
    request: &RemotingCommand,
    request_header: &RegisterBrokerRequestHeader,
) -> bool {
    if request_header.body_crc32 == 0 {
        return true;
    }
    if let Some(bytes) = &request.get_body() {
        let crc_32 = CRC32Utils::crc32(bytes.iter().as_ref());
        if crc_32 != request_header.body_crc32 {
            warn!(
                "receive registerBroker request,crc32 not match,origin:{}, cal:{}",
                request_header.body_crc32, crc_32,
            );
            return false;
        }
    }
    true
}
