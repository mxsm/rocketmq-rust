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

use core::str;
use std::collections::HashMap;
use std::net::SocketAddr;

use cheetah_string::CheetahString;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::string_to_properties;
use rocketmq_common::common::mq_version::RocketMqVersion;
use rocketmq_common::utils::serde_json_utils::SerdeJsonUtils;
use rocketmq_common::CRC32Utils;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::RemotingSysResponseCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::broker_body::broker_member_group::GetBrokerMemberGroupResponseBody;
use rocketmq_remoting::protocol::body::broker_body::register_broker_body::RegisterBrokerBody;
use rocketmq_remoting::protocol::body::topic_info_wrapper::topic_config_wrapper::TopicConfigAndMappingSerializeWrapper;
use rocketmq_remoting::protocol::header::namesrv::broker_request::BrokerHeartbeatRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::broker_request::GetBrokerMemberGroupRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::broker_request::UnRegisterBrokerRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::kv_config_header::DeleteKVConfigRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::kv_config_header::GetKVConfigRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::kv_config_header::GetKVConfigResponseHeader;
use rocketmq_remoting::protocol::header::namesrv::kv_config_header::GetKVListByNamespaceRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::kv_config_header::PutKVConfigRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::perm_broker_header::AddWritePermOfBrokerRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::perm_broker_header::AddWritePermOfBrokerResponseHeader;
use rocketmq_remoting::protocol::header::namesrv::perm_broker_header::WipeWritePermOfBrokerRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::perm_broker_header::WipeWritePermOfBrokerResponseHeader;
use rocketmq_remoting::protocol::header::namesrv::query_data_version_header::QueryDataVersionRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::query_data_version_header::QueryDataVersionResponseHeader;
use rocketmq_remoting::protocol::header::namesrv::register_broker_header::RegisterBrokerRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::register_broker_header::RegisterBrokerResponseHeader;
use rocketmq_remoting::protocol::header::namesrv::topic_operation_header::DeleteTopicFromNamesrvRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::topic_operation_header::GetTopicsByClusterRequestHeader;
use rocketmq_remoting::protocol::header::namesrv::topic_operation_header::RegisterTopicRequestHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_remoting::protocol::DataVersion;
use rocketmq_remoting::protocol::RemotingDeserializable;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use tracing::warn;

use crate::processor::NAMESPACE_ORDER_TOPIC_CONFIG;
use crate::route::route_info_manager::RouteInfoManager;
use crate::KVConfigManager;

pub struct DefaultRequestProcessor {
    route_info_manager: RouteInfoManager,
    kvconfig_manager: KVConfigManager,
}

impl DefaultRequestProcessor {
    pub fn process_request(
        &mut self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        request_code: RequestCode,
        request: RemotingCommand,
    ) -> Option<RemotingCommand> {
        let response = match request_code {
            RequestCode::PutKvConfig => self.put_kv_config(request),
            RequestCode::GetKvConfig => self.get_kv_config(request),
            RequestCode::DeleteKvConfig => self.delete_kv_config(request),
            RequestCode::QueryDataVersion => self.query_broker_topic_config(request),
            //handle register broker
            RequestCode::RegisterBroker => {
                self.process_register_broker(channel.remote_address(), request)
            }
            RequestCode::UnregisterBroker => self.process_unregister_broker(request),
            RequestCode::BrokerHeartbeat => self.process_broker_heartbeat(request),
            RequestCode::GetBrokerMemberGroup => self.get_broker_member_group(request),
            //handle get broker cluster info
            RequestCode::GetBrokerClusterInfo => self.get_broker_cluster_info(request),
            RequestCode::WipeWritePermOfBroker => self.wipe_write_perm_of_broker(request),
            RequestCode::AddWritePermOfBroker => self.add_write_perm_of_broker(request),
            RequestCode::GetAllTopicListFromNameserver => {
                self.get_all_topic_list_from_nameserver(request)
            }
            RequestCode::DeleteTopicInNamesrv => self.delete_topic_in_name_srv(request),
            RequestCode::RegisterTopicInNamesrv => self.register_topic_to_name_srv(request),
            RequestCode::GetKvlistByNamespace => self.get_kv_list_by_namespace(request),
            RequestCode::GetTopicsByCluster => self.get_topics_by_cluster(request),
            RequestCode::GetSystemTopicListFromNs => self.get_system_topic_list_from_ns(request),
            RequestCode::GetUnitTopicList => self.get_unit_topic_list(request),
            RequestCode::GetHasUnitSubTopicList => self.get_has_unit_sub_topic_list(request),
            RequestCode::GetHasUnitSubUnunitTopicList => {
                self.get_has_unit_sub_un_unit_topic_list(request)
            }
            RequestCode::UpdateNamesrvConfig => self.update_config(request),
            _ => RemotingCommand::create_response_command_with_code(
                RemotingSysResponseCode::SystemError,
            ),
        };
        Some(response)
    }
}

///implementation put KV config
impl DefaultRequestProcessor {
    fn put_kv_config(&mut self, request: RemotingCommand) -> RemotingCommand {
        let request_header = request
            .decode_command_custom_header::<PutKVConfigRequestHeader>()
            .expect("decode PutKVConfigRequestHeader failed");
        //check namespace and key, need?
        if request_header.namespace.is_empty() || request_header.key.is_empty() {
            return RemotingCommand::create_response_command_with_code(
                RemotingSysResponseCode::SystemError,
            )
            .set_remark(CheetahString::from_static_str("namespace or key is empty"));
        }

        self.kvconfig_manager.put_kv_config(
            request_header.namespace.clone(),
            request_header.key.clone(),
            request_header.value.clone(),
        );
        RemotingCommand::create_response_command()
    }

    fn get_kv_config(&self, request: RemotingCommand) -> RemotingCommand {
        let request_header = request
            .decode_command_custom_header::<GetKVConfigRequestHeader>()
            .expect("decode GetKVConfigRequestHeader failed");

        let value = self
            .kvconfig_manager
            .get_kvconfig(&request_header.namespace, &request_header.key);

        if value.is_some() {
            return RemotingCommand::create_response_command()
                .set_command_custom_header(GetKVConfigResponseHeader::new(value));
        }
        RemotingCommand::create_response_command_with_code(RemotingSysResponseCode::SystemError)
            .set_remark(format!(
                "No config item, Namespace: {} Key: {}",
                request_header.namespace, request_header.key
            ))
    }

    fn delete_kv_config(&mut self, request: RemotingCommand) -> RemotingCommand {
        let request_header = request
            .decode_command_custom_header::<DeleteKVConfigRequestHeader>()
            .expect("decode DeleteKVConfigRequestHeader failed");

        self.kvconfig_manager
            .delete_kv_config(&request_header.namespace, &request_header.key);
        RemotingCommand::create_response_command()
    }

    fn query_broker_topic_config(&mut self, request: RemotingCommand) -> RemotingCommand {
        let request_header = request
            .decode_command_custom_header::<QueryDataVersionRequestHeader>()
            .expect("decode QueryDataVersionRequestHeader failed");
        let data_version = DataVersion::decode(request.get_body().expect("body is empty"))
            .expect("decode DataVersion failed");
        let changed = self.route_info_manager.is_broker_topic_config_changed(
            &request_header.cluster_name,
            &request_header.broker_addr,
            &data_version,
        );

        self.route_info_manager.update_broker_info_update_timestamp(
            request_header.cluster_name.clone(),
            request_header.broker_addr.clone(),
        );
        let mut command = RemotingCommand::create_response_command()
            .set_command_custom_header(QueryDataVersionResponseHeader::new(changed));
        if let Some(value) = self
            .route_info_manager
            .query_broker_topic_config(request_header.cluster_name, request_header.broker_addr)
        {
            command = command.set_body(value.encode());
        }
        command
    }
}

#[allow(clippy::new_without_default)]
impl DefaultRequestProcessor {
    pub fn new(route_info_manager: RouteInfoManager, kvconfig_manager: KVConfigManager) -> Self {
        Self {
            route_info_manager,
            kvconfig_manager,
        }
    }
}
impl DefaultRequestProcessor {
    fn process_register_broker(
        &mut self,
        remote_addr: SocketAddr,
        request: RemotingCommand,
    ) -> RemotingCommand {
        let request_header = request
            .decode_command_custom_header::<RegisterBrokerRequestHeader>()
            .expect("decode RegisterBrokerRequestHeader failed");
        if !check_sum_crc32(&request, &request_header) {
            return RemotingCommand::create_response_command_with_code(
                RemotingSysResponseCode::SystemError,
            )
            .set_remark(CheetahString::from_static_str("crc32 not match"));
        }

        let mut response_command = RemotingCommand::create_response_command();
        let broker_version = RocketMqVersion::try_from(request.version()).expect("invalid version");
        let topic_config_wrapper;
        let mut filter_server_list = Vec::<String>::new();
        if broker_version as usize >= RocketMqVersion::V3011 as usize {
            let register_broker_body =
                extract_register_broker_body_from_request(&request, &request_header);
            topic_config_wrapper = register_broker_body
                .topic_config_serialize_wrapper()
                .clone();
            filter_server_list = register_broker_body.filter_server_list().clone();
        } else {
            topic_config_wrapper = extract_register_topic_config_from_request(&request);
        }
        let result = self.route_info_manager.register_broker(
            request_header.cluster_name,
            request_header.broker_addr,
            request_header.broker_name,
            request_header.broker_id,
            request_header.ha_server_addr,
            request
                .ext_fields()
                .and_then(|map| map.get(mix_all::ZONE_NAME).cloned()),
            request_header.heartbeat_timeout_millis,
            request_header.enable_acting_master,
            topic_config_wrapper,
            filter_server_list,
            remote_addr,
        );
        if result.is_none() {
            return response_command
                .set_code(RemotingSysResponseCode::SystemError)
                .set_remark(CheetahString::from_static_str("register broker failed"));
        }
        if self
            .kvconfig_manager
            .namesrv_config
            .return_order_topic_config_to_broker
        {
            if let Some(value) =
                self.kvconfig_manager
                    .get_kv_list_by_namespace(&CheetahString::from_static_str(
                        NAMESPACE_ORDER_TOPIC_CONFIG,
                    ))
            {
                response_command = response_command.set_body(value);
            }
        }
        let register_broker_result = result.unwrap();
        response_command
            .set_code(RemotingSysResponseCode::Success)
            .set_command_custom_header(RegisterBrokerResponseHeader::new(
                Some(register_broker_result.ha_server_addr),
                Some(register_broker_result.master_addr),
            ))
    }

    fn process_unregister_broker(&mut self, request: RemotingCommand) -> RemotingCommand {
        let request_header = request
            .decode_command_custom_header::<UnRegisterBrokerRequestHeader>()
            .expect("decode UnRegisterBrokerRequestHeader failed");
        self.route_info_manager
            .un_register_broker(vec![request_header]);
        RemotingCommand::create_response_command()
    }
}

impl DefaultRequestProcessor {
    fn process_broker_heartbeat(&mut self, request: RemotingCommand) -> RemotingCommand {
        let request_header = request
            .decode_command_custom_header::<BrokerHeartbeatRequestHeader>()
            .expect("decode BrokerHeartbeatRequestHeader failed");
        self.route_info_manager.update_broker_info_update_timestamp(
            request_header.cluster_name,
            request_header.broker_addr,
        );
        RemotingCommand::create_response_command()
    }

    fn get_broker_member_group(&mut self, request: RemotingCommand) -> RemotingCommand {
        let request_header = request
            .decode_command_custom_header::<GetBrokerMemberGroupRequestHeader>()
            .expect("decode GetBrokerMemberGroupRequestHeader failed");

        let broker_member_group = self
            .route_info_manager
            .get_broker_member_group(&request_header.cluster_name, &request_header.broker_name);
        let response_body = GetBrokerMemberGroupResponseBody {
            broker_member_group,
        };
        RemotingCommand::create_response_command().set_body(response_body.encode())
    }

    fn get_broker_cluster_info(&self, _request: RemotingCommand) -> RemotingCommand {
        let vec = self.route_info_manager.get_all_cluster_info().encode();
        RemotingCommand::create_response_command_with_code(RemotingSysResponseCode::Success)
            .set_body(vec)
    }

    fn wipe_write_perm_of_broker(&mut self, request: RemotingCommand) -> RemotingCommand {
        let request_header = request
            .decode_command_custom_header::<WipeWritePermOfBrokerRequestHeader>()
            .expect("decode WipeWritePermOfBrokerRequestHeader failed");
        let wipe_topic_cnt = self
            .route_info_manager
            .wipe_write_perm_of_broker_by_lock(&request_header.broker_name);
        RemotingCommand::create_response_command()
            .set_command_custom_header(WipeWritePermOfBrokerResponseHeader::new(wipe_topic_cnt))
    }

    fn add_write_perm_of_broker(&mut self, request: RemotingCommand) -> RemotingCommand {
        let request_header = request
            .decode_command_custom_header::<AddWritePermOfBrokerRequestHeader>()
            .expect("decode AddWritePermOfBrokerRequestHeader failed");
        let add_topic_cnt = self
            .route_info_manager
            .add_write_perm_of_broker_by_lock(&request_header.broker_name);
        RemotingCommand::create_response_command()
            .set_command_custom_header(AddWritePermOfBrokerResponseHeader::new(add_topic_cnt))
    }

    fn get_all_topic_list_from_nameserver(&self, _request: RemotingCommand) -> RemotingCommand {
        if self.route_info_manager.namesrv_config.enable_all_topic_list {
            let topics = self.route_info_manager.get_all_topic_list();
            return RemotingCommand::create_response_command().set_body(topics.encode());
        }
        RemotingCommand::create_response_command_with_code(RemotingSysResponseCode::SystemError)
            .set_remark(CheetahString::from_static_str("disable"))
    }

    fn delete_topic_in_name_srv(&mut self, request: RemotingCommand) -> RemotingCommand {
        let request_header = request
            .decode_command_custom_header::<DeleteTopicFromNamesrvRequestHeader>()
            .expect("decode DeleteTopicFromNamesrvRequestHeader failed");
        self.route_info_manager
            .delete_topic(request_header.topic, request_header.cluster_name);
        RemotingCommand::create_response_command()
    }

    fn register_topic_to_name_srv(&mut self, request: RemotingCommand) -> RemotingCommand {
        let request_header = request
            .decode_command_custom_header::<RegisterTopicRequestHeader>()
            .expect("decode RegisterTopicRequestHeader failed");
        if let Some(ref body) = request.body() {
            let topic_route_data = TopicRouteData::decode(body).unwrap_or_default();
            if !topic_route_data.queue_datas.is_empty() {
                self.route_info_manager
                    .register_topic(request_header.topic, topic_route_data.queue_datas)
            }
        }
        RemotingCommand::create_response_command()
    }

    fn get_kv_list_by_namespace(&self, request: RemotingCommand) -> RemotingCommand {
        let request_header = request
            .decode_command_custom_header::<GetKVListByNamespaceRequestHeader>()
            .expect("decode GetKVListByNamespaceRequestHeader failed");
        let value = self
            .kvconfig_manager
            .get_kv_list_by_namespace(&request_header.namespace);
        if let Some(value) = value {
            return RemotingCommand::create_response_command().set_body(value);
        }
        RemotingCommand::create_response_command_with_code(ResponseCode::QueryNotFound).set_remark(
            format!(
                "No config item, Namespace: {}",
                request_header.namespace.as_str()
            ),
        )
    }

    fn get_topics_by_cluster(&self, request: RemotingCommand) -> RemotingCommand {
        if !self.route_info_manager.namesrv_config.enable_topic_list {
            return RemotingCommand::create_response_command_with_code(
                RemotingSysResponseCode::SystemError,
            )
            .set_remark(CheetahString::from_static_str("disable"));
        }

        let request_header = request
            .decode_command_custom_header::<GetTopicsByClusterRequestHeader>()
            .expect("decode GetTopicsByClusterRequestHeader failed");
        let topics_by_cluster = self
            .route_info_manager
            .get_topics_by_cluster(&request_header.cluster);
        RemotingCommand::create_response_command().set_body(topics_by_cluster.encode())
    }

    fn get_system_topic_list_from_ns(&self, _request: RemotingCommand) -> RemotingCommand {
        let topic_list = self.route_info_manager.get_system_topic_list();
        RemotingCommand::create_response_command().set_body(topic_list.encode())
    }

    fn get_unit_topic_list(&self, _request: RemotingCommand) -> RemotingCommand {
        if self.route_info_manager.namesrv_config.enable_topic_list {
            let topic_list = self.route_info_manager.get_unit_topics();
            return RemotingCommand::create_response_command().set_body(topic_list.encode());
        }
        RemotingCommand::create_response_command_with_code(RemotingSysResponseCode::SystemError)
            .set_remark("disable")
    }

    fn get_has_unit_sub_topic_list(&self, _request: RemotingCommand) -> RemotingCommand {
        if self.route_info_manager.namesrv_config.enable_topic_list {
            let topic_list = self.route_info_manager.get_has_unit_sub_topic_list();
            return RemotingCommand::create_response_command().set_body(topic_list.encode());
        }
        RemotingCommand::create_response_command_with_code(RemotingSysResponseCode::SystemError)
            .set_remark("disable")
    }

    fn get_has_unit_sub_un_unit_topic_list(&self, _request: RemotingCommand) -> RemotingCommand {
        if self.route_info_manager.namesrv_config.enable_topic_list {
            let topic_list = self
                .route_info_manager
                .get_has_unit_sub_un_unit_topic_list();
            return RemotingCommand::create_response_command().set_body(topic_list.encode());
        }
        RemotingCommand::create_response_command_with_code(RemotingSysResponseCode::SystemError)
            .set_remark("disable")
    }

    fn update_config(&mut self, request: RemotingCommand) -> RemotingCommand {
        if let Some(body) = request.body() {
            let body_str = match str::from_utf8(&body) {
                Ok(s) => s,
                Err(e) => {
                    return RemotingCommand::create_response_command_with_code(
                        RemotingSysResponseCode::SystemError,
                    )
                    .set_remark(format!("UnsupportedEncodingException {:?}", e));
                }
            };

            let properties = match string_to_properties(body_str) {
                Some(props) => props,
                None => {
                    return RemotingCommand::create_response_command_with_code(
                        RemotingSysResponseCode::SystemError,
                    )
                    .set_remark("string_to_properties error".to_string());
                }
            };
            if validate_blacklist_config_exist(
                &properties,
                &self
                    .kvconfig_manager
                    .get_namesrv_config()
                    .get_config_blacklist(),
            ) {
                return RemotingCommand::create_response_command_with_code(
                    RemotingSysResponseCode::NoPermission,
                )
                .set_remark("Cannot update config in blacklist.".to_string());
            }

            let result = self.kvconfig_manager.update_namesrv_config(properties);
            if let Err(e) = result {
                return RemotingCommand::create_response_command_with_code(
                    RemotingSysResponseCode::SystemError,
                )
                .set_remark(format!("Update error {:?}", e));
            }
        }

        RemotingCommand::create_response_command_with_code(RemotingSysResponseCode::Success)
            .set_remark(CheetahString::empty())
    }
}

fn extract_register_topic_config_from_request(
    request: &RemotingCommand,
) -> TopicConfigAndMappingSerializeWrapper {
    if let Some(body_inner) = request.body() {
        if body_inner.is_empty() {
            return TopicConfigAndMappingSerializeWrapper::default();
        }
        return SerdeJsonUtils::decode::<TopicConfigAndMappingSerializeWrapper>(
            body_inner.iter().as_slice(),
        )
        .expect("decode TopicConfigAndMappingSerializeWrapper failed");
    }
    TopicConfigAndMappingSerializeWrapper::default()
}

fn extract_register_broker_body_from_request(
    request: &RemotingCommand,
    request_header: &RegisterBrokerRequestHeader,
) -> RegisterBrokerBody {
    if let Some(body_inner) = request.body() {
        if body_inner.is_empty() {
            return RegisterBrokerBody::default();
        }
        let version = RocketMqVersion::try_from(request.version()).unwrap();
        return RegisterBrokerBody::decode(body_inner, request_header.compressed, version);
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

fn validate_blacklist_config_exist(
    properties: &HashMap<CheetahString, CheetahString>,
    config_blacklist: &[CheetahString],
) -> bool {
    for black_config in config_blacklist {
        if properties.contains_key(black_config.as_str()) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use rocketmq_remoting::protocol::header::namesrv::register_broker_header::RegisterBrokerRequestHeader;

    use super::*;

    #[test]
    fn extract_register_topic_config_from_request_with_body() {
        let body = vec![/* some valid encoded data */];
        let request = RemotingCommand::create_remoting_command(RequestCode::RegisterTopicInNamesrv)
            .set_body(body);

        let _result = extract_register_topic_config_from_request(&request);
    }

    #[test]
    fn extract_register_topic_config_from_request_without_body() {
        let request = RemotingCommand::new_request(0, vec![]);
        let _result = extract_register_topic_config_from_request(&request);
    }

    #[test]
    fn extract_register_broker_body_from_request_with_body() {
        let body: Vec<u8> = vec![/* some valid encoded data */];
        let request = RemotingCommand::new_request(0, body);
        let request_header = RegisterBrokerRequestHeader::default();
        let _result = extract_register_broker_body_from_request(&request, &request_header);
    }

    #[test]
    fn extract_register_broker_body_from_request_without_body() {
        let request = RemotingCommand::new_request(0, vec![]);
        let request_header = RegisterBrokerRequestHeader::default();
        let _result = extract_register_broker_body_from_request(&request, &request_header);
    }

    #[test]
    fn check_sum_crc32_valid_crc() {
        let body = vec![/* some valid data */];
        let crc32 = CRC32Utils::crc32(&body);
        let request = RemotingCommand::new_request(0, body);
        let mut request_header = RegisterBrokerRequestHeader::default();
        request_header.body_crc32 = crc32;
        let result = check_sum_crc32(&request, &request_header);
        assert!(result);
    }

    #[test]
    fn check_sum_crc32_invalid_crc() {
        let body = vec![/* some valid data */];
        let request = RemotingCommand::new_request(0, body);
        let mut request_header = RegisterBrokerRequestHeader::default();
        request_header.body_crc32 = 12345; // some invalid crc32
        let result = check_sum_crc32(&request, &request_header);
        assert!(!result);
    }

    #[test]
    fn check_sum_crc32_zero_crc() {
        let body = vec![/* some valid data */];
        let request = RemotingCommand::new_request(0, body);
        let mut request_header = RegisterBrokerRequestHeader::default();
        request_header.body_crc32 = 0;
        let result = check_sum_crc32(&request, &request_header);
        assert!(result);
    }
}
