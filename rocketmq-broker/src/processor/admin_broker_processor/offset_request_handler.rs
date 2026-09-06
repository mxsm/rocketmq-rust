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

use crate::config::config_manager::ConfigManager;
use rocketmq_error::PublicErrorView;
use rocketmq_error::PROTOCOL_REQUEST_UNSUPPORTED;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::check_rocksdb_cq_write_progress_request_header::CheckRocksdbCqWriteProgressRequestHeader;
use rocketmq_protocol::protocol::header::get_earliest_msg_storetime_request_header::GetEarliestMsgStoretimeRequestHeader;
use rocketmq_protocol::protocol::header::get_earliest_msg_storetime_response_header::GetEarliestMsgStoretimeResponseHeader;
use rocketmq_protocol::protocol::header::get_max_offset_request_header::GetMaxOffsetRequestHeader;
use rocketmq_protocol::protocol::header::get_max_offset_response_header::GetMaxOffsetResponseHeader;
use rocketmq_protocol::protocol::header::get_min_offset_request_header::GetMinOffsetRequestHeader;
use rocketmq_protocol::protocol::header::get_min_offset_response_header::GetMinOffsetResponseHeader;
use rocketmq_protocol::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::remoting_command_defaults::application_remoting_command_factory;
use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_context::TopicQueueMappingContext;
use rocketmq_protocol::protocol::static_topic::topic_queue_mapping_utils::TopicQueueMappingUtils;
use rocketmq_store::BrokerAdminStore;
use rocketmq_transport::api::error_response;
use rocketmq_transport::api::RemotingErrorTarget;
use rocketmq_transport::api::RpcClient;
use rocketmq_transport::api::RpcRequest;
use tracing::error;

use crate::broker::broker_admin_runtime::BrokerAdminRuntime;

use super::AdminRequestMetadata;

pub(super) struct OffsetRequestHandler;

impl OffsetRequestHandler {
    pub const fn new() -> Self {
        Self
    }

    pub async fn get_max_offset<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header =
            request.decode_required_header::<GetMaxOffsetRequestHeader>("decode get-max-offset request header")?;
        let mapping_context = broker_runtime_inner
            .topic_queue_mapping_manager()
            .build_topic_queue_mapping_context(&request_header, false);
        let topic = request_header.topic.clone();
        let queue_id = request_header.queue_id;
        let rewrite_result = self
            .rewrite_request_for_static_topic(broker_runtime_inner, request_header, mapping_context)
            .await?;
        if rewrite_result.is_some() {
            return Ok(rewrite_result);
        }

        let offset = broker_runtime_inner
            .message_store()
            .unwrap()
            .get_max_offset_in_queue(topic.as_ref(), queue_id);
        let response_header = GetMaxOffsetResponseHeader { offset };
        Ok(Some(RemotingCommand::create_success_response_command_with_header(
            response_header,
        )))
    }

    pub async fn get_min_offset<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header =
            request.decode_required_header::<GetMinOffsetRequestHeader>("decode get-min-offset request header")?;

        let mapping_context = broker_runtime_inner
            .topic_queue_mapping_manager()
            .build_topic_queue_mapping_context(&request_header, false);
        let topic = request_header.topic.clone();
        let queue_id = request_header.queue_id;
        let rewrite_result = self
            .handle_get_min_offset_for_static_topic(broker_runtime_inner, request_header, mapping_context)
            .await?;
        if rewrite_result.is_some() {
            return Ok(rewrite_result);
        }

        let offset = broker_runtime_inner
            .message_store()
            .unwrap()
            .get_min_offset_in_queue(topic.as_ref(), queue_id);
        let response_header = GetMinOffsetResponseHeader { offset };
        Ok(Some(RemotingCommand::create_success_response_command_with_header(
            response_header,
        )))
    }

    async fn handle_get_min_offset_for_static_topic<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        mut request_header: GetMinOffsetRequestHeader,
        mapping_context: TopicQueueMappingContext,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let Some(mapping_detail) = mapping_context.mapping_detail.as_ref() else {
            return Ok(None);
        };
        if !mapping_context.is_leader() {
            return Ok(Some(
                RemotingCommand::create_response_command_with_code(ResponseCode::NotLeaderForQueue).set_remark(
                    format!(
                        "{}-{:?} does not exit in request process of current broker {:?}",
                        mapping_context.topic, mapping_context.global_id, mapping_detail.topic_queue_mapping_info.bname
                    ),
                ),
            ));
        }

        let max_item =
            TopicQueueMappingUtils::find_logic_queue_mapping_item(&mapping_context.mapping_item_list, 0, true)
                .ok_or_else(|| static_topic_offset_mapping_missing("static topic min offset request handling"))?;
        request_header.set_broker_name(
            max_item
                .bname
                .clone()
                .ok_or_else(|| static_topic_offset_broker_name_missing("static topic min offset request handling"))?,
        );
        request_header.set_lo(Some(false));
        request_header.queue_id = max_item.queue_id;
        let max_physical_offset = if max_item.bname == mapping_detail.topic_queue_mapping_info.bname {
            broker_runtime_inner
                .message_store()
                .unwrap()
                .get_min_offset_in_queue(mapping_context.topic.as_ref(), max_item.queue_id)
        } else {
            let rpc_request = RpcRequest::new(RequestCode::GetMinOffset.to_i32(), request_header, None);
            let rpc_response = broker_runtime_inner
                .broker_outer_api()
                .rpc_client()
                .invoke(rpc_request, broker_runtime_inner.broker_config().forward_timeout)
                .await;
            if let Err(e) = rpc_response {
                return Ok(Some(
                    RemotingCommand::create_response_command_with_code(ResponseCode::SystemError)
                        .set_remark(format!("{e}")),
                ));
            } else {
                match rpc_response.unwrap().get_header::<GetMinOffsetResponseHeader>() {
                    None => {
                        return Ok(Some(
                            RemotingCommand::create_response_command_with_code(ResponseCode::SystemError)
                                .set_remark("Rpc response header is None"),
                        ));
                    }
                    Some(offset_response_header) => offset_response_header.offset,
                }
            }
        };
        Ok(Some(RemotingCommand::create_success_response_command_with_header(
            GetMinOffsetResponseHeader {
                offset: max_item.compute_static_queue_offset_loosely(max_physical_offset),
            },
        )))
    }

    async fn rewrite_request_for_static_topic<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        mut request_header: GetMaxOffsetRequestHeader,
        mapping_context: TopicQueueMappingContext,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let Some(mapping_detail) = mapping_context.mapping_detail.as_ref() else {
            return Ok(None);
        };
        if !mapping_context.is_leader() {
            return Ok(Some(
                RemotingCommand::create_response_command_with_code(ResponseCode::NotLeaderForQueue).set_remark(
                    format!(
                        "{}-{:?} does not exit in request process of current broker {:?}",
                        mapping_context.topic, mapping_context.global_id, mapping_detail.topic_queue_mapping_info.bname
                    ),
                ),
            ));
        }

        let max_item =
            TopicQueueMappingUtils::find_logic_queue_mapping_item(&mapping_context.mapping_item_list, i64::MAX, true)
                .ok_or_else(|| static_topic_offset_mapping_missing("static topic max offset request handling"))?;
        request_header.set_broker_name(
            max_item
                .bname
                .clone()
                .ok_or_else(|| static_topic_offset_broker_name_missing("static topic max offset request handling"))?,
        );
        request_header.set_lo(Some(false));
        request_header.queue_id = max_item.queue_id;
        let max_physical_offset = if max_item.bname == mapping_detail.topic_queue_mapping_info.bname {
            broker_runtime_inner
                .message_store()
                .unwrap()
                .get_max_offset_in_queue(mapping_context.topic.as_ref(), max_item.queue_id)
        } else {
            let rpc_request = RpcRequest::new(RequestCode::GetMaxOffset.to_i32(), request_header.clone(), None);
            let rpc_response = broker_runtime_inner
                .broker_outer_api()
                .rpc_client()
                .invoke(rpc_request, broker_runtime_inner.broker_config().forward_timeout)
                .await;
            if let Err(e) = rpc_response {
                return Ok(Some(
                    RemotingCommand::create_response_command_with_code(ResponseCode::SystemError)
                        .set_remark(format!("{e}")),
                ));
            } else {
                match rpc_response.unwrap().get_header::<GetMaxOffsetResponseHeader>() {
                    None => {
                        return Ok(Some(
                            RemotingCommand::create_response_command_with_code(ResponseCode::SystemError)
                                .set_remark("Rpc response header is None"),
                        ));
                    }
                    Some(offset_response_header) => offset_response_header.offset,
                }
            }
        };
        Ok(Some(RemotingCommand::create_success_response_command_with_header(
            GetMaxOffsetResponseHeader {
                offset: max_item.compute_static_queue_offset_strictly(max_physical_offset),
            },
        )))
    }

    pub async fn get_all_delay_offset<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response_command = RemotingCommand::create_java_default_error_response_command();
        let content = broker_runtime_inner.schedule_message_service().encode_pretty(false);
        if content.is_empty() {
            return Ok(Some(
                response_command
                    .set_code(ResponseCode::SystemError)
                    .set_remark("No delay offset in this broker"),
            ));
        }
        Ok(Some(
            RemotingCommand::create_success_response_command().set_body(content.into_bytes()),
        ))
    }

    pub async fn get_earliest_msg_store_time<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<GetEarliestMsgStoretimeRequestHeader>()?;
        let mapping_context = broker_runtime_inner
            .topic_queue_mapping_manager()
            .build_topic_queue_mapping_context(&request_header, false);
        let topic = request_header.topic.clone();
        let queue_id = request_header.queue_id;
        let rewrite_result = self
            .rewrite_get_earliest_request_for_static_topic(broker_runtime_inner, request_header, mapping_context)
            .await?;
        if rewrite_result.is_some() {
            return Ok(rewrite_result);
        }

        let timestamp = broker_runtime_inner
            .message_store()
            .unwrap()
            .get_earliest_message_time(topic.as_ref(), queue_id);
        Ok(Some(RemotingCommand::create_success_response_command_with_header(
            GetEarliestMsgStoretimeResponseHeader { timestamp },
        )))
    }

    pub async fn clean_expired_consumequeue<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        broker_runtime_inner
            .message_store()
            .unwrap()
            .clean_expired_consumer_queue();
        Ok(Some(RemotingCommand::create_success_response_command()))
    }

    pub async fn delete_expired_commitlog<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        broker_runtime_inner
            .message_store()
            .unwrap()
            .execute_delete_files_manually();
        Ok(Some(RemotingCommand::create_success_response_command()))
    }

    pub async fn check_rocksdb_cq_write_progress(
        &self,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let _request_header = request.decode_command_custom_header::<CheckRocksdbCqWriteProgressRequestHeader>()?;
        let command_factory = application_remoting_command_factory();
        Ok(Some(error_response(
            PublicErrorView::descriptor_only(&PROTOCOL_REQUEST_UNSUPPORTED),
            RemotingErrorTarget::Reply {
                factory: &command_factory,
                opaque: request.opaque(),
            },
        )))
    }

    pub async fn get_all_subscription_group_config<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        metadata: &AdminRequestMetadata,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let response_command = RemotingCommand::create_java_default_error_response_command();
        let content = broker_runtime_inner.subscription_group_manager().encode_pretty(false);
        if content.is_empty() {
            error!("No subscription group config in this broker,client:{}", metadata);
            return Ok(Some(
                response_command
                    .set_code(ResponseCode::SystemError)
                    .set_remark("No subscription group config in this broker"),
            ));
        }
        Ok(Some(
            RemotingCommand::create_success_response_command().set_body(content.into_bytes()),
        ))
    }

    async fn rewrite_get_earliest_request_for_static_topic<MS: BrokerAdminStore>(
        &self,
        broker_runtime_inner: &BrokerAdminRuntime<MS>,
        mut request_header: GetEarliestMsgStoretimeRequestHeader,
        mapping_context: TopicQueueMappingContext,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let Some(mapping_detail) = mapping_context.mapping_detail.as_ref() else {
            return Ok(None);
        };

        if !mapping_context.is_leader() {
            return Ok(Some(
                RemotingCommand::create_response_command_with_code(ResponseCode::NotLeaderForQueue).set_remark(
                    format!(
                        "{}-{:?} does not exit in request process of current broker {:?}",
                        mapping_context.topic, mapping_context.global_id, mapping_detail.topic_queue_mapping_info.bname
                    ),
                ),
            ));
        }

        let Some(mapping_item) =
            TopicQueueMappingUtils::find_logic_queue_mapping_item(&mapping_context.mapping_item_list, 0, true)
        else {
            return Ok(Some(
                RemotingCommand::create_response_command_with_code(ResponseCode::SystemError)
                    .set_remark("Cannot find logic queue mapping item in earliest msg storetime handling"),
            ));
        };

        request_header.set_broker_name(
            mapping_item
                .bname
                .clone()
                .ok_or_else(|| static_topic_offset_broker_name_missing("earliest msg storetime handling"))?,
        );
        request_header.set_lo(Some(false));
        request_header.queue_id = mapping_item.queue_id;

        let timestamp = if mapping_item.bname == mapping_detail.topic_queue_mapping_info.bname {
            broker_runtime_inner
                .message_store()
                .unwrap()
                .get_earliest_message_time(mapping_context.topic.as_ref(), mapping_item.queue_id)
        } else {
            let rpc_request = RpcRequest::new(RequestCode::GetEarliestMsgStoreTime.to_i32(), request_header, None);
            match broker_runtime_inner
                .broker_outer_api()
                .rpc_client()
                .invoke(rpc_request, broker_runtime_inner.broker_config().forward_timeout)
                .await
            {
                Ok(response) => match response.get_header::<GetEarliestMsgStoretimeResponseHeader>() {
                    Some(header) => header.timestamp,
                    None => {
                        return Ok(Some(
                            RemotingCommand::create_response_command_with_code(ResponseCode::SystemError)
                                .set_remark("Rpc response header is None"),
                        ));
                    }
                },
                Err(err) => {
                    return Ok(Some(
                        RemotingCommand::create_response_command_with_code(ResponseCode::SystemError)
                            .set_remark(format!("{err}")),
                    ));
                }
            }
        };

        Ok(Some(RemotingCommand::create_success_response_command_with_header(
            GetEarliestMsgStoretimeResponseHeader { timestamp },
        )))
    }
}

fn static_topic_offset_mapping_missing(operation: &'static str) -> rocketmq_error::RocketMQError {
    rocketmq_error::RocketMQError::RouteInconsistent {
        topic: "static_topic_offset".to_string(),
        reason: format!("Cannot find logic queue mapping item in {operation}"),
    }
}

fn static_topic_offset_broker_name_missing(operation: &'static str) -> rocketmq_error::RocketMQError {
    rocketmq_error::RocketMQError::request_header_error(format!(
        "Broker name is missing in logic queue mapping item for {operation}"
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::SystemTime;

    use crate::config::broker_config::BrokerConfig;
    use cheetah_string::CheetahString;
    use rocketmq_model::common::config::TopicConfig;
    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_protocol::protocol::header::check_rocksdb_cq_write_progress_request_header::CheckRocksdbCqWriteProgressRequestHeader;
    use rocketmq_protocol::protocol::header::empty_header::EmptyHeader;
    use rocketmq_protocol::protocol::header::get_earliest_msg_storetime_request_header::GetEarliestMsgStoretimeRequestHeader;
    use rocketmq_protocol::protocol::header::get_earliest_msg_storetime_response_header::GetEarliestMsgStoretimeResponseHeader;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_store::BrokerReadStore;
    use rocketmq_store::MessageStoreConfig;

    use super::static_topic_offset_broker_name_missing;
    use super::static_topic_offset_mapping_missing;
    use super::OffsetRequestHandler;
    use crate::broker_runtime::BrokerRuntime;

    fn temp_test_root(label: &str) -> std::path::PathBuf {
        let millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("time should move forward")
            .as_millis();
        std::env::temp_dir().join(format!("rocketmq-rust-admin-offset-{label}-{millis}"))
    }

    async fn new_test_runtime(label: &str) -> BrokerRuntime {
        let temp_root = temp_test_root(label);
        let broker_config = Arc::new(BrokerConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            auth_config_path: temp_root.join("auth.json").to_string_lossy().into_owned().into(),
            ..BrokerConfig::default()
        });
        let message_store_config = Arc::new(MessageStoreConfig {
            store_path_root_dir: temp_root.to_string_lossy().into_owned().into(),
            ..MessageStoreConfig::default()
        });
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
        assert!(runtime.initialize().await.is_ok());
        runtime
    }

    #[test]
    fn static_topic_offset_mapping_missing_uses_route_inconsistent_kind() {
        let error = static_topic_offset_mapping_missing("test operation");

        assert_eq!(error.descriptor(), &rocketmq_error::ROUTE_TOPIC_INCONSISTENT);
    }

    #[test]
    fn static_topic_offset_broker_name_missing_uses_request_header_kind() {
        let error = static_topic_offset_broker_name_missing("test operation");

        assert_eq!(error.descriptor(), &rocketmq_error::PROTOCOL_HEADER_INVALID);
    }

    #[tokio::test]
    async fn get_earliest_msg_store_time_returns_store_timestamp() {
        let runtime = new_test_runtime("earliest-time").await;
        let admin = runtime.admin_runtime_for_test();
        let _ = admin
            .topic_config_manager()
            .update_topic_config(TopicConfig::with_queues("topic-a", 1, 1), 0);

        let expected_timestamp = admin
            .message_store()
            .expect("message store should exist")
            .get_earliest_message_time(&CheetahString::from_static_str("topic-a"), 0);

        let handler = OffsetRequestHandler::new();
        let mut request = RemotingCommand::create_request_command(
            RequestCode::GetEarliestMsgStoreTime,
            GetEarliestMsgStoretimeRequestHeader {
                topic: CheetahString::from_static_str("topic-a"),
                queue_id: 0,
                topic_request_header: None,
            },
        );
        request.make_custom_header_to_net();

        let response = handler
            .get_earliest_msg_store_time(&admin, RequestCode::GetEarliestMsgStoreTime, &mut request)
            .await
            .expect("get earliest msg store time should succeed")
            .expect("get earliest msg store time should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert_eq!(
            response
                .read_custom_header_ref::<GetEarliestMsgStoretimeResponseHeader>()
                .expect("read earliest msg storetime response header")
                .timestamp,
            expected_timestamp
        );

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn clean_expired_consumequeue_returns_success() {
        let runtime = new_test_runtime("clean-expired-cq").await;
        let handler = OffsetRequestHandler::new();
        let mut request =
            RemotingCommand::create_request_command(RequestCode::CleanExpiredConsumequeue, EmptyHeader {});

        let response = handler
            .clean_expired_consumequeue(
                &runtime.admin_runtime_for_test(),
                RequestCode::CleanExpiredConsumequeue,
                &mut request,
            )
            .await
            .expect("clean expired consumequeue should succeed")
            .expect("clean expired consumequeue should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn check_rocksdb_cq_write_progress_without_rocksdb_returns_not_supported() {
        let runtime = new_test_runtime("check-rocksdb-progress").await;
        let handler = OffsetRequestHandler::new();
        let mut request = RemotingCommand::create_request_command(
            RequestCode::CheckRocksdbCqWriteProgress,
            CheckRocksdbCqWriteProgressRequestHeader {
                topic: CheetahString::from_static_str("topic-a"),
                check_store_time: 0,
                rpc: None,
            },
        );
        request.make_custom_header_to_net();

        let response = handler
            .check_rocksdb_cq_write_progress(RequestCode::CheckRocksdbCqWriteProgress, &mut request)
            .await
            .expect("check rocksdb cq write progress should succeed")
            .expect("check rocksdb cq write progress should return response");

        assert_eq!(
            ResponseCode::from(response.code()),
            ResponseCode::RequestCodeNotSupported
        );
        assert_eq!(
            response.remark().map(|remark| remark.as_str()),
            Some("Protocol request is unsupported")
        );

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }
}
