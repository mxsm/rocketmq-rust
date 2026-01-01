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

use core::str;
use std::collections::HashMap;

use cheetah_string::CheetahString;
use rocketmq_common::common::mix_all;
use rocketmq_common::common::mix_all::string_to_properties;
use rocketmq_common::common::mq_version::RocketMqVersion;
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
use rocketmq_remoting::runtime::processor::RequestProcessor;
use rocketmq_rust::ArcMut;
use tracing::info;
use tracing::warn;

use crate::bootstrap::NameServerRuntimeInner;
use crate::processor::NAMESPACE_ORDER_TOPIC_CONFIG;

pub struct DefaultRequestProcessor {
    name_server_runtime_inner: ArcMut<NameServerRuntimeInner>,
}

impl RequestProcessor for DefaultRequestProcessor {
    async fn process_request(
        &mut self,
        channel: Channel,
        ctx: ConnectionHandlerContext,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_code = RequestCode::from(request.code());
        info!(
            "Name server DefaultRequestProcessor Received request code: {:?}",
            request_code
        );
        self.process_request_inner(channel, ctx, request_code, request)
    }
}

impl DefaultRequestProcessor {
    pub fn process_request_inner(
        &mut self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response = match request_code {
            RequestCode::PutKvConfig => self.put_kv_config(request),
            RequestCode::GetKvConfig => self.get_kv_config(request),
            RequestCode::DeleteKvConfig => self.delete_kv_config(request),
            RequestCode::QueryDataVersion => self.query_broker_topic_config(request),
            //handle register broker
            RequestCode::RegisterBroker => self.process_register_broker(channel, request),
            RequestCode::UnregisterBroker => self.process_unregister_broker(request),
            RequestCode::BrokerHeartbeat => self.process_broker_heartbeat(request),
            RequestCode::GetBrokerMemberGroup => self.get_broker_member_group(request),
            //handle get broker cluster info
            RequestCode::GetBrokerClusterInfo => self.get_broker_cluster_info(request),
            RequestCode::WipeWritePermOfBroker => self.wipe_write_perm_of_broker(request),
            RequestCode::AddWritePermOfBroker => self.add_write_perm_of_broker(request),
            RequestCode::GetAllTopicListFromNameserver => self.get_all_topic_list_from_nameserver(request),
            RequestCode::DeleteTopicInNamesrv => self.delete_topic_in_name_srv(request),
            RequestCode::RegisterTopicInNamesrv => self.register_topic_to_name_srv(request),
            RequestCode::GetKvlistByNamespace => self.get_kv_list_by_namespace(request),
            RequestCode::GetTopicsByCluster => self.get_topics_by_cluster(request),
            RequestCode::GetSystemTopicListFromNs => self.get_system_topic_list_from_ns(request),
            RequestCode::GetUnitTopicList => self.get_unit_topic_list(request),
            RequestCode::GetHasUnitSubTopicList => self.get_has_unit_sub_topic_list(request),
            RequestCode::GetHasUnitSubUnunitTopicList => self.get_has_unit_sub_un_unit_topic_list(request),
            RequestCode::UpdateNamesrvConfig => self.update_config(request),
            RequestCode::GetNamesrvConfig => self.get_config(request),
            _ => Ok(RemotingCommand::create_response_command_with_code(
                RemotingSysResponseCode::SystemError,
            )),
        }?;
        Ok(Some(response))
    }
}

///implementation put KV config
impl DefaultRequestProcessor {
    fn put_kv_config(&mut self, request: &mut RemotingCommand) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let request_header = request.decode_command_custom_header::<PutKVConfigRequestHeader>()?;
        //check namespace and key, need?
        if request_header.namespace.is_empty() || request_header.key.is_empty() {
            return Ok(
                RemotingCommand::create_response_command_with_code(RemotingSysResponseCode::SystemError)
                    .set_remark(CheetahString::from_static_str("namespace or key is empty")),
            );
        }

        self.name_server_runtime_inner.kvconfig_manager_mut().put_kv_config(
            request_header.namespace,
            request_header.key,
            request_header.value,
        )?;
        Ok(RemotingCommand::create_response_command())
    }

    fn get_kv_config(&self, request: &mut RemotingCommand) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let request_header = request.decode_command_custom_header::<GetKVConfigRequestHeader>()?;

        let value = self
            .name_server_runtime_inner
            .kvconfig_manager()
            .get_kvconfig(&request_header.namespace, &request_header.key);

        if value.is_some() {
            return Ok(RemotingCommand::create_response_command()
                .set_command_custom_header(GetKVConfigResponseHeader::new(value)));
        }
        Ok(
            RemotingCommand::create_response_command_with_code(ResponseCode::QueryNotFound).set_remark(format!(
                "No config item, Namespace: {} Key: {}",
                request_header.namespace, request_header.key
            )),
        )
    }

    fn delete_kv_config(&mut self, request: &mut RemotingCommand) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let request_header = request.decode_command_custom_header::<DeleteKVConfigRequestHeader>()?;

        let _ = self
            .name_server_runtime_inner
            .kvconfig_manager_mut()
            .delete_kv_config(&request_header.namespace, &request_header.key);
        Ok(RemotingCommand::create_response_command())
    }

    fn query_broker_topic_config(
        &mut self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let request_header = request.decode_command_custom_header::<QueryDataVersionRequestHeader>()?;

        let body = request
            .get_body()
            .ok_or_else(|| rocketmq_error::RocketMQError::request_body_invalid("decode", "request body is empty"))?;

        let data_version = DataVersion::decode(body).map_err(|e| {
            rocketmq_error::RocketMQError::request_body_invalid("decode", format!("DataVersion decode failed: {}", e))
        })?;

        // Check if broker topic config has changed
        let changed = self
            .name_server_runtime_inner
            .route_info_manager()
            .is_broker_topic_config_changed(&request_header.cluster_name, &request_header.broker_addr, &data_version);

        // Update broker timestamp
        self.name_server_runtime_inner
            .route_info_manager_mut()
            .update_broker_info_update_timestamp(
                request_header.cluster_name.clone(),
                request_header.broker_addr.clone(),
            );

        // Build response with changed flag
        let mut command = RemotingCommand::create_response_command()
            .set_command_custom_header(QueryDataVersionResponseHeader::new(changed));

        // Query and attach broker topic config if available
        if let Some(topic_config) = self
            .name_server_runtime_inner
            .route_info_manager()
            .query_broker_topic_config(request_header.cluster_name, request_header.broker_addr)
        {
            let body = topic_config.encode().map_err(|e| {
                rocketmq_error::RocketMQError::response_process_failed(
                    "encode",
                    format!("DataVersion encode failed: {}", e),
                )
            })?;
            command = command.set_body(body);
        }

        Ok(command)
    }
}

#[allow(clippy::new_without_default)]
impl DefaultRequestProcessor {
    pub(crate) fn new(name_server_runtime_inner: ArcMut<NameServerRuntimeInner>) -> Self {
        Self {
            name_server_runtime_inner,
        }
    }
}
impl DefaultRequestProcessor {
    fn process_register_broker(
        &mut self,
        channel: Channel,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let request_header = request.decode_command_custom_header::<RegisterBrokerRequestHeader>()?;
        if !check_sum_crc32(request, &request_header) {
            return Ok(
                RemotingCommand::create_response_command_with_code(RemotingSysResponseCode::SystemError)
                    .set_remark(CheetahString::from_static_str("crc32 not match")),
            );
        }

        let mut response_command = RemotingCommand::create_response_command();
        let broker_version = RocketMqVersion::try_from(request.version() as u32)?;
        let topic_config_wrapper;
        let mut filter_server_list = Vec::new();
        if broker_version >= RocketMqVersion::V3_0_11 {
            let register_broker_body = extract_register_broker_body_from_request(request, &request_header)?;
            topic_config_wrapper = register_broker_body.topic_config_serialize_wrapper;
            filter_server_list = register_broker_body.filter_server_list;
        } else {
            topic_config_wrapper = extract_register_topic_config_from_request(request)?;
        }
        let result = self.name_server_runtime_inner.route_info_manager().register_broker(
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
            channel,
        );
        if result.is_none() {
            return Ok(response_command
                .set_code(RemotingSysResponseCode::SystemError)
                .set_remark(CheetahString::from_static_str("register broker failed")));
        }
        if self
            .name_server_runtime_inner
            .name_server_config()
            .return_order_topic_config_to_broker
        {
            if let Some(value) = self
                .name_server_runtime_inner
                .kvconfig_manager()
                .get_kv_list_by_namespace(&CheetahString::from_static_str(NAMESPACE_ORDER_TOPIC_CONFIG))
            {
                response_command = response_command.set_body(value);
            }
        }
        let register_broker_result = result.unwrap();
        Ok(response_command
            .set_code(RemotingSysResponseCode::Success)
            .set_command_custom_header(RegisterBrokerResponseHeader::new(
                Some(register_broker_result.ha_server_addr),
                Some(register_broker_result.master_addr),
            )))
    }

    fn process_unregister_broker(
        &mut self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let request_header = request.decode_command_custom_header::<UnRegisterBrokerRequestHeader>()?;
        if !self
            .name_server_runtime_inner
            .route_info_manager()
            .submit_unregister_broker_request(request_header)
        {
            warn!("Couldn't submit the unregister broker request to handler");
            return Ok(RemotingCommand::create_response_command_with_code(
                ResponseCode::SystemError,
            ));
        }
        Ok(RemotingCommand::create_response_command())
    }
}

impl DefaultRequestProcessor {
    fn process_broker_heartbeat(
        &mut self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let request_header = request.decode_command_custom_header::<BrokerHeartbeatRequestHeader>()?;
        self.name_server_runtime_inner
            .route_info_manager_mut()
            .update_broker_info_update_timestamp(request_header.cluster_name, request_header.broker_addr);
        Ok(RemotingCommand::create_response_command())
    }

    fn get_broker_member_group(
        &mut self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let request_header = request.decode_command_custom_header::<GetBrokerMemberGroupRequestHeader>()?;

        let broker_member_group = self
            .name_server_runtime_inner
            .route_info_manager_mut()
            .get_broker_member_group(request_header.cluster_name, request_header.broker_name);
        let response_body = GetBrokerMemberGroupResponseBody { broker_member_group };
        let body = response_body.encode()?;
        Ok(RemotingCommand::create_response_command().set_body(body))
    }

    fn get_broker_cluster_info(
        &self,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let vec = self
            .name_server_runtime_inner
            .route_info_manager()
            .get_all_cluster_info()
            .encode()?;
        Ok(RemotingCommand::create_response_command_with_code(RemotingSysResponseCode::Success).set_body(vec))
    }

    fn wipe_write_perm_of_broker(
        &mut self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let request_header = request.decode_command_custom_header::<WipeWritePermOfBrokerRequestHeader>()?;
        let wipe_topic_cnt = self
            .name_server_runtime_inner
            .route_info_manager_mut()
            .wipe_write_perm_of_broker_by_lock(&request_header.broker_name);
        Ok(RemotingCommand::create_response_command()
            .set_command_custom_header(WipeWritePermOfBrokerResponseHeader::new(wipe_topic_cnt)))
    }

    fn add_write_perm_of_broker(
        &mut self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let request_header = request.decode_command_custom_header::<AddWritePermOfBrokerRequestHeader>()?;
        let add_topic_cnt = self
            .name_server_runtime_inner
            .route_info_manager_mut()
            .add_write_perm_of_broker_by_lock(&request_header.broker_name);
        Ok(RemotingCommand::create_response_command()
            .set_command_custom_header(AddWritePermOfBrokerResponseHeader::new(add_topic_cnt)))
    }

    fn get_all_topic_list_from_nameserver(
        &self,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        if self
            .name_server_runtime_inner
            .name_server_config()
            .enable_all_topic_list
        {
            let topics = self.name_server_runtime_inner.route_info_manager().get_all_topic_list();
            let body = topics.encode()?;
            return Ok(RemotingCommand::create_response_command().set_body(body));
        }
        Ok(
            RemotingCommand::create_response_command_with_code(RemotingSysResponseCode::SystemError)
                .set_remark(CheetahString::from_static_str("disable")),
        )
    }

    fn delete_topic_in_name_srv(
        &mut self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let request_header = request.decode_command_custom_header::<DeleteTopicFromNamesrvRequestHeader>()?;
        self.name_server_runtime_inner
            .route_info_manager_mut()
            .delete_topic(request_header.topic, request_header.cluster_name);
        Ok(RemotingCommand::create_response_command())
    }

    fn register_topic_to_name_srv(
        &mut self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let request_header = request.decode_command_custom_header::<RegisterTopicRequestHeader>()?;
        if let Some(body) = request.body() {
            let topic_route_data = TopicRouteData::decode(body).map_err(|e| {
                rocketmq_error::RocketMQError::request_body_invalid(
                    "decode",
                    format!("TopicRouteData decode failed: {}", e),
                )
            })?;
            if !topic_route_data.queue_datas.is_empty() {
                self.name_server_runtime_inner
                    .route_info_manager_mut()
                    .register_topic(request_header.topic, topic_route_data.queue_datas)
            }
        }
        Ok(RemotingCommand::create_response_command())
    }

    fn get_kv_list_by_namespace(
        &self,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let request_header = request.decode_command_custom_header::<GetKVListByNamespaceRequestHeader>()?;
        let value = self
            .name_server_runtime_inner
            .kvconfig_manager()
            .get_kv_list_by_namespace(&request_header.namespace);
        if let Some(value) = value {
            return Ok(RemotingCommand::create_response_command().set_body(value));
        }
        Ok(
            RemotingCommand::create_response_command_with_code(ResponseCode::QueryNotFound).set_remark(format!(
                "No config item, Namespace: {}",
                request_header.namespace.as_str()
            )),
        )
    }

    fn get_topics_by_cluster(&self, request: &mut RemotingCommand) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        if !self.name_server_runtime_inner.name_server_config().enable_topic_list {
            return Ok(
                RemotingCommand::create_response_command_with_code(RemotingSysResponseCode::SystemError)
                    .set_remark(CheetahString::from_static_str("disable")),
            );
        }

        let request_header = request.decode_command_custom_header::<GetTopicsByClusterRequestHeader>()?;
        let topics_by_cluster = self
            .name_server_runtime_inner
            .route_info_manager()
            .get_topics_by_cluster(&request_header.cluster);
        let body = topics_by_cluster.encode()?;
        Ok(RemotingCommand::create_response_command().set_body(body))
    }

    fn get_system_topic_list_from_ns(
        &self,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let topic_list = self
            .name_server_runtime_inner
            .route_info_manager()
            .get_system_topic_list();
        let body = topic_list.encode()?;
        Ok(RemotingCommand::create_response_command().set_body(body))
    }

    fn get_unit_topic_list(&self, _request: &mut RemotingCommand) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        if self.name_server_runtime_inner.name_server_config().enable_topic_list {
            let topic_list = self.name_server_runtime_inner.route_info_manager().get_unit_topics();
            let body = topic_list.encode()?;
            return Ok(RemotingCommand::create_response_command().set_body(body));
        }
        Ok(
            RemotingCommand::create_response_command_with_code(RemotingSysResponseCode::SystemError)
                .set_remark("disable"),
        )
    }

    fn get_has_unit_sub_topic_list(
        &self,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        if self.name_server_runtime_inner.name_server_config().enable_topic_list {
            let topic_list = self
                .name_server_runtime_inner
                .route_info_manager()
                .get_has_unit_sub_topic_list();
            let body = topic_list.encode()?;
            return Ok(RemotingCommand::create_response_command().set_body(body));
        }
        Ok(
            RemotingCommand::create_response_command_with_code(RemotingSysResponseCode::SystemError)
                .set_remark("disable"),
        )
    }

    fn get_has_unit_sub_un_unit_topic_list(
        &self,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        if self.name_server_runtime_inner.name_server_config().enable_topic_list {
            let topic_list = self
                .name_server_runtime_inner
                .route_info_manager()
                .get_has_unit_sub_un_unit_topic_list();
            return Ok(RemotingCommand::create_response_command()
                .set_body(topic_list.encode().expect("encode TopicList failed")));
        }
        Ok(
            RemotingCommand::create_response_command_with_code(RemotingSysResponseCode::SystemError)
                .set_remark("disable"),
        )
    }

    fn update_config(&mut self, request: &mut RemotingCommand) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        if let Some(body) = request.body() {
            let body_str = match str::from_utf8(body) {
                Ok(s) => s,
                Err(e) => {
                    return Ok(RemotingCommand::create_response_command_with_code(
                        RemotingSysResponseCode::SystemError,
                    )
                    .set_remark(format!("UnsupportedEncodingException {e:?}")));
                }
            };

            let properties = match string_to_properties(body_str) {
                Some(props) => props,
                None => {
                    return Ok(RemotingCommand::create_response_command_with_code(
                        RemotingSysResponseCode::SystemError,
                    )
                    .set_remark("string_to_properties error".to_string()));
                }
            };
            if validate_blacklist_config_exist(
                &properties,
                &self
                    .name_server_runtime_inner
                    .name_server_config()
                    .get_config_blacklist(),
            ) {
                return Ok(
                    RemotingCommand::create_response_command_with_code(RemotingSysResponseCode::NoPermission)
                        .set_remark("Cannot update config in blacklist.".to_string()),
                );
            }

            let result = self
                .name_server_runtime_inner
                .kvconfig_manager_mut()
                .update_namesrv_config(properties);
            if let Err(e) = result {
                return Ok(
                    RemotingCommand::create_response_command_with_code(RemotingSysResponseCode::SystemError)
                        .set_remark(format!("Update error {e:?}")),
                );
            }
        }

        Ok(
            RemotingCommand::create_response_command_with_code(RemotingSysResponseCode::Success)
                .set_remark(CheetahString::empty()),
        )
    }

    fn get_config(&mut self, _request: &mut RemotingCommand) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let config = self.name_server_runtime_inner.name_server_config();
        let result = match config.get_all_configs_format_string() {
            Ok(content) => {
                let response = RemotingCommand::create_response_command_with_code_remark(
                    RemotingSysResponseCode::Success,
                    CheetahString::empty(),
                );
                response.set_body(content.into_bytes())
            }
            Err(e) => RemotingCommand::create_response_command_with_code_remark(
                ResponseCode::SystemError,
                format!("UnsupportedEncodingException {e}"),
            ),
        };
        Ok(result)
    }
}

fn extract_register_topic_config_from_request(
    request: &RemotingCommand,
) -> rocketmq_error::RocketMQResult<TopicConfigAndMappingSerializeWrapper> {
    if let Some(body_inner) = request.body() {
        if !body_inner.is_empty() {
            return TopicConfigAndMappingSerializeWrapper::decode(body_inner.as_ref()).inspect_err(|e| {
                warn!("Failed to decode TopicConfigAndMappingSerializeWrapper: {:?}", e);
            });
        }
    }

    // When body is empty or None, create default wrapper with explicitly initialized DataVersion
    let mut topic_config_wrapper = TopicConfigAndMappingSerializeWrapper::default();
    topic_config_wrapper
        .topic_config_serialize_wrapper
        .data_version
        .set_counter(0);
    topic_config_wrapper
        .topic_config_serialize_wrapper
        .data_version
        .set_timestamp(0);
    topic_config_wrapper
        .topic_config_serialize_wrapper
        .data_version
        .set_state_version(0);

    Ok(topic_config_wrapper)
}

fn extract_register_broker_body_from_request(
    request: &RemotingCommand,
    request_header: &RegisterBrokerRequestHeader,
) -> rocketmq_error::RocketMQResult<RegisterBrokerBody> {
    if let Some(body_inner) = request.body() {
        if !body_inner.is_empty() {
            let version = request.rocketmq_version();
            return RegisterBrokerBody::decode(body_inner, request_header.compressed, version).inspect_err(|e| {
                warn!("Failed to decode RegisterBrokerBody: {:?}", e);
            });
        }
    }
    let mut register_broker_body = RegisterBrokerBody::default();
    register_broker_body
        .topic_config_serialize_wrapper
        .mapping_data_version
        .set_counter(0);
    register_broker_body
        .topic_config_serialize_wrapper
        .mapping_data_version
        .set_timestamp(0);
    register_broker_body
        .topic_config_serialize_wrapper
        .mapping_data_version
        .set_state_version(0);

    Ok(register_broker_body)
}

fn check_sum_crc32(request: &RemotingCommand, request_header: &RegisterBrokerRequestHeader) -> bool {
    if request_header.body_crc32 != 0 {
        let crc_32 = CRC32Utils::crc32_bytes(request.get_body());
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
        // Use invalid JSON data that will cause decode to fail
        let body = vec![0xFF, 0xFE, 0xFD, 0xFC]; // Invalid JSON bytes
        let request = RemotingCommand::create_remoting_command(RequestCode::RegisterTopicInNamesrv).set_body(body);

        // Should return error since body is invalid JSON
        let result = extract_register_topic_config_from_request(&request);
        // Verify decode fails with invalid data
        assert!(result.is_err(), "Expected decode to fail with invalid JSON");
    }

    #[test]
    fn extract_register_topic_config_from_request_without_body() {
        let request = RemotingCommand::new_request(0, vec![]);
        let result = extract_register_topic_config_from_request(&request).expect("should succeed with empty body");

        // Verify DataVersion is explicitly initialized with zeros (align with Java logic)
        let data_version = &result.topic_config_serialize_wrapper.data_version;
        assert_eq!(data_version.get_counter(), 0, "counter should be 0");
        assert_eq!(data_version.get_timestamp(), 0, "timestamp should be 0");
        assert_eq!(data_version.get_state_version(), 0, "stateVersion should be 0");
    }

    #[test]
    fn extract_register_topic_config_from_request_with_empty_body() {
        // Test with empty body (should initialize DataVersion to zeros)
        let request = RemotingCommand::new_request(0, vec![]);
        let result = extract_register_topic_config_from_request(&request).expect("should succeed with empty body");

        // Verify DataVersion fields are explicitly set to 0 (Java behavior)
        let data_version = &result.topic_config_serialize_wrapper.data_version;
        assert_eq!(data_version.get_counter(), 0, "counter should be 0");
        assert_eq!(data_version.get_timestamp(), 0, "timestamp should be 0");
        assert_eq!(data_version.get_state_version(), 0, "stateVersion should be 0");
    }

    #[test]
    fn extract_register_broker_body_from_request_with_body() {
        let body: Vec<u8> = vec![/* some valid encoded data */];
        let request = RemotingCommand::new_request(0, body);
        let request_header = RegisterBrokerRequestHeader::default();
        // Should return Ok with default body since body is invalid
        let _result = extract_register_broker_body_from_request(&request, &request_header);
        assert!(_result.is_ok());
    }

    #[test]
    fn extract_register_broker_body_from_request_with_empty_body() {
        // Test with empty body (should initialize DataVersion to zeros)
        let request = RemotingCommand::new_request(0, vec![]);
        let request_header = RegisterBrokerRequestHeader::default();
        let result = extract_register_broker_body_from_request(&request, &request_header).expect("should succeed");

        // Verify DataVersion fields are explicitly set to 0 (Java behavior)
        let data_version = &result.topic_config_serialize_wrapper.mapping_data_version;
        assert_eq!(data_version.get_counter(), 0, "counter should be 0");
        assert_eq!(data_version.get_timestamp(), 0, "timestamp should be 0");
        assert_eq!(data_version.get_state_version(), 0, "stateVersion should be 0");
    }

    #[test]
    fn extract_register_broker_body_from_request_without_body() {
        let request = RemotingCommand::new_request(0, vec![]);
        let request_header = RegisterBrokerRequestHeader::default();
        let result = extract_register_broker_body_from_request(&request, &request_header).expect("should succeed");

        // Verify DataVersion is explicitly initialized with zeros (align with Java logic)
        assert_eq!(
            result.topic_config_serialize_wrapper.mapping_data_version.get_counter(),
            0
        );
        assert_eq!(
            result
                .topic_config_serialize_wrapper
                .mapping_data_version
                .get_timestamp(),
            0
        );
        assert_eq!(
            result
                .topic_config_serialize_wrapper
                .mapping_data_version
                .get_state_version(),
            0
        );
    }

    #[test]
    fn check_sum_crc32_valid_crc() {
        let body = vec![/* some valid data */];
        let crc32 = CRC32Utils::crc32(&body);
        let request = RemotingCommand::new_request(0, body);
        let request_header = RegisterBrokerRequestHeader {
            body_crc32: crc32,
            ..Default::default()
        };
        let result = check_sum_crc32(&request, &request_header);
        assert!(result);
    }

    #[test]
    fn check_sum_crc32_invalid_crc() {
        let body = vec![/* some valid data */];
        let request = RemotingCommand::new_request(0, body);
        let request_header = RegisterBrokerRequestHeader {
            body_crc32: 12345, // some invalid crc32
            ..Default::default()
        };
        let result = check_sum_crc32(&request, &request_header);
        assert!(!result);
    }

    #[test]
    fn check_sum_crc32_zero_crc() {
        let body = vec![/* some valid data */];
        let request = RemotingCommand::new_request(0, body);
        let request_header = RegisterBrokerRequestHeader {
            body_crc32: 0,
            ..Default::default()
        };
        let result = check_sum_crc32(&request, &request_header);
        assert!(result);
    }
}
