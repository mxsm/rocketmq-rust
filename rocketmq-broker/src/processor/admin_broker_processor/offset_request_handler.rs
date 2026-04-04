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

use rocketmq_common::common::config_manager::ConfigManager;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::body::check_rocksdb_cqwrite_progress_response_body::CheckRocksdbCqWriteResult;
use rocketmq_remoting::protocol::body::check_rocksdb_cqwrite_progress_response_body::CheckStatus;
use rocketmq_remoting::protocol::header::check_rocksdb_cq_write_progress_request_header::CheckRocksdbCqWriteProgressRequestHeader;
use rocketmq_remoting::protocol::header::get_earliest_msg_storetime_request_header::GetEarliestMsgStoretimeRequestHeader;
use rocketmq_remoting::protocol::header::get_earliest_msg_storetime_response_header::GetEarliestMsgStoretimeResponseHeader;
use rocketmq_remoting::protocol::header::get_max_offset_request_header::GetMaxOffsetRequestHeader;
use rocketmq_remoting::protocol::header::get_max_offset_response_header::GetMaxOffsetResponseHeader;
use rocketmq_remoting::protocol::header::get_min_offset_request_header::GetMinOffsetRequestHeader;
use rocketmq_remoting::protocol::header::get_min_offset_response_header::GetMinOffsetResponseHeader;
use rocketmq_remoting::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_context::TopicQueueMappingContext;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_utils::TopicQueueMappingUtils;
use rocketmq_remoting::protocol::RemotingSerializable;
use rocketmq_remoting::rpc::rpc_client::RpcClient;
use rocketmq_remoting::rpc::rpc_request::RpcRequest;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_store::MessageStore;
use tracing::error;

use crate::broker_runtime::BrokerRuntimeInner;

#[derive(Clone)]
pub(super) struct OffsetRequestHandler<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS: MessageStore> OffsetRequestHandler<MS> {
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self { broker_runtime_inner }
    }
}

impl<MS: MessageStore> OffsetRequestHandler<MS> {
    pub async fn get_max_offset(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request
            .decode_command_custom_header::<GetMaxOffsetRequestHeader>()
            .unwrap(); //need to optimize
        let mapping_context = self
            .broker_runtime_inner
            .topic_queue_mapping_manager()
            .build_topic_queue_mapping_context(&request_header, false);
        let topic = request_header.topic.clone();
        let queue_id = request_header.queue_id;
        let rewrite_result = self
            .rewrite_request_for_static_topic(request_header, mapping_context)
            .await?;
        if rewrite_result.is_some() {
            return Ok(rewrite_result);
        }

        let offset = self
            .broker_runtime_inner
            .message_store()
            .unwrap()
            .get_max_offset_in_queue(topic.as_ref(), queue_id);
        let response_header = GetMaxOffsetResponseHeader { offset };
        Ok(Some(RemotingCommand::create_response_command_with_header(
            response_header,
        )))
    }

    pub async fn get_min_offset(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request
            .decode_command_custom_header::<GetMinOffsetRequestHeader>()
            .unwrap(); //need to optimize

        let mapping_context = self
            .broker_runtime_inner
            .topic_queue_mapping_manager()
            .build_topic_queue_mapping_context(&request_header, false);
        let topic = request_header.topic.clone();
        let queue_id = request_header.queue_id;
        let rewrite_result = self
            .handle_get_min_offset_for_static_topic(request_header, mapping_context)
            .await?;
        if rewrite_result.is_some() {
            return Ok(rewrite_result);
        }

        let offset = self
            .broker_runtime_inner
            .message_store()
            .unwrap()
            .get_min_offset_in_queue(topic.as_ref(), queue_id);
        let response_header = GetMinOffsetResponseHeader { offset };
        Ok(Some(RemotingCommand::create_response_command_with_header(
            response_header,
        )))
    }

    async fn handle_get_min_offset_for_static_topic(
        &mut self,
        mut request_header: GetMinOffsetRequestHeader,
        mapping_context: TopicQueueMappingContext,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mapping_detail = mapping_context
            .mapping_detail
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::Internal(
                "TopicQueueMappingDetail is None in static topic min offset request handling".to_string(),
            ))?;
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
            TopicQueueMappingUtils::find_logic_queue_mapping_item(&mapping_context.mapping_item_list, 0, true).ok_or(
                rocketmq_error::RocketMQError::Internal(
                    "Cannot find logic queue mapping item in static topic min offset request handling".to_string(),
                ),
            )?;
        request_header.set_broker_name(max_item.bname.clone().ok_or(rocketmq_error::RocketMQError::Internal(
            "Broker name is None in logic queue mapping item in static topic min offset request handling".to_string(),
        ))?);
        request_header.set_lo(Some(false));
        request_header.queue_id = max_item.queue_id;
        let max_physical_offset = if max_item.bname == mapping_detail.topic_queue_mapping_info.bname {
            self.broker_runtime_inner
                .message_store()
                .unwrap()
                .get_min_offset_in_queue(mapping_context.topic.as_ref(), max_item.queue_id)
        } else {
            let rpc_request = RpcRequest::new(RequestCode::GetMinOffset.to_i32(), request_header, None);
            let rpc_response = self
                .broker_runtime_inner
                .broker_outer_api()
                .rpc_client()
                .invoke(rpc_request, self.broker_runtime_inner.broker_config().forward_timeout)
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
        Ok(Some(RemotingCommand::create_response_command_with_header(
            GetMinOffsetResponseHeader {
                offset: max_item.compute_static_queue_offset_loosely(max_physical_offset),
            },
        )))
    }

    async fn rewrite_request_for_static_topic(
        &mut self,
        mut request_header: GetMaxOffsetRequestHeader,
        mapping_context: TopicQueueMappingContext,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mapping_detail = mapping_context
            .mapping_detail
            .as_ref()
            .ok_or(rocketmq_error::RocketMQError::Internal(
                "TopicQueueMappingDetail is None in static topic max offset request handling".to_string(),
            ))?;
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
                .ok_or(rocketmq_error::RocketMQError::Internal(
                "Cannot find logic queue mapping item in static topic max offset request handling".to_string(),
            ))?;
        request_header.set_broker_name(max_item.bname.clone().ok_or(rocketmq_error::RocketMQError::Internal(
            "Broker name is None in logic queue mapping item in static topic max offset request handling".to_string(),
        ))?);
        request_header.set_lo(Some(false));
        request_header.queue_id = max_item.queue_id;
        let max_physical_offset = if max_item.bname == mapping_detail.topic_queue_mapping_info.bname {
            self.broker_runtime_inner
                .message_store()
                .unwrap()
                .get_max_offset_in_queue(mapping_context.topic.as_ref(), max_item.queue_id)
        } else {
            let rpc_request = RpcRequest::new(RequestCode::GetMaxOffset.to_i32(), request_header.clone(), None);
            let rpc_response = self
                .broker_runtime_inner
                .broker_outer_api()
                .rpc_client()
                .invoke(rpc_request, self.broker_runtime_inner.broker_config().forward_timeout)
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
        Ok(Some(RemotingCommand::create_response_command_with_header(
            GetMaxOffsetResponseHeader {
                offset: max_item.compute_static_queue_offset_strictly(max_physical_offset),
            },
        )))
    }

    pub async fn get_all_delay_offset(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response_command = RemotingCommand::create_response_command();
        let content = self
            .broker_runtime_inner
            .schedule_message_service()
            .encode_pretty(false);
        if content.is_empty() {
            return Ok(Some(
                response_command
                    .set_code(ResponseCode::SystemError)
                    .set_remark("No delay offset in this broker"),
            ));
        }
        response_command.set_body_mut_ref(content.into_bytes());
        Ok(Some(response_command))
    }

    pub async fn get_earliest_msg_store_time(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<GetEarliestMsgStoretimeRequestHeader>()?;
        let mapping_context = self
            .broker_runtime_inner
            .topic_queue_mapping_manager()
            .build_topic_queue_mapping_context(&request_header, false);
        let topic = request_header.topic.clone();
        let queue_id = request_header.queue_id;
        let rewrite_result = self
            .rewrite_get_earliest_request_for_static_topic(request_header, mapping_context)
            .await?;
        if rewrite_result.is_some() {
            return Ok(rewrite_result);
        }

        let timestamp = self
            .broker_runtime_inner
            .message_store()
            .unwrap()
            .get_earliest_message_time(topic.as_ref(), queue_id);
        Ok(Some(RemotingCommand::create_response_command_with_header(
            GetEarliestMsgStoretimeResponseHeader { timestamp },
        )))
    }

    pub async fn clean_expired_consumequeue(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        self.broker_runtime_inner
            .message_store()
            .unwrap()
            .clean_expired_consumer_queue();
        Ok(Some(
            RemotingCommand::create_response_command().set_code(ResponseCode::Success),
        ))
    }

    pub async fn delete_expired_commitlog(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        self.broker_runtime_inner
            .message_store()
            .unwrap()
            .execute_delete_files_manually();
        Ok(Some(
            RemotingCommand::create_response_command().set_code(ResponseCode::Success),
        ))
    }

    pub async fn check_rocksdb_cq_write_progress(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let _request_header = request.decode_command_custom_header::<CheckRocksdbCqWriteProgressRequestHeader>()?;
        let body = CheckRocksdbCqWriteResult {
            check_result: Some("It is not CombineConsumeQueueStore, no need check".into()),
            check_status: CheckStatus::CheckOk as i32,
        };

        Ok(Some(
            RemotingCommand::create_response_command()
                .set_code(ResponseCode::Success)
                .set_body(body.encode()?),
        ))
    }

    pub async fn get_all_subscription_group_config(
        &mut self,
        channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        _request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let mut response_command = RemotingCommand::create_response_command();
        let content = self
            .broker_runtime_inner
            .subscription_group_manager()
            .encode_pretty(false);
        if content.is_empty() {
            error!(
                "No subscription group config in this broker,client:{}",
                channel.remote_address()
            );
            return Ok(Some(
                response_command
                    .set_code(ResponseCode::SystemError)
                    .set_remark("No subscription group config in this broker"),
            ));
        }
        response_command.set_body_mut_ref(content.into_bytes());
        Ok(Some(response_command))
    }

    async fn rewrite_get_earliest_request_for_static_topic(
        &mut self,
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
                .ok_or(rocketmq_error::RocketMQError::Internal(
                    "Broker name is None in logic queue mapping item in earliest msg storetime handling".to_string(),
                ))?,
        );
        request_header.set_lo(Some(false));
        request_header.queue_id = mapping_item.queue_id;

        let timestamp = if mapping_item.bname == mapping_detail.topic_queue_mapping_info.bname {
            self.broker_runtime_inner
                .message_store()
                .unwrap()
                .get_earliest_message_time(mapping_context.topic.as_ref(), mapping_item.queue_id)
        } else {
            let rpc_request = RpcRequest::new(RequestCode::GetEarliestMsgStoreTime.to_i32(), request_header, None);
            match self
                .broker_runtime_inner
                .broker_outer_api()
                .rpc_client()
                .invoke(rpc_request, self.broker_runtime_inner.broker_config().forward_timeout)
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

        Ok(Some(RemotingCommand::create_response_command_with_header(
            GetEarliestMsgStoretimeResponseHeader { timestamp },
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::SystemTime;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::config::TopicConfig;
    use rocketmq_remoting::base::response_future::ResponseFuture;
    use rocketmq_remoting::code::request_code::RequestCode;
    use rocketmq_remoting::code::response_code::ResponseCode;
    use rocketmq_remoting::connection::Connection;
    use rocketmq_remoting::net::channel::Channel;
    use rocketmq_remoting::net::channel::ChannelInner;
    use rocketmq_remoting::protocol::body::check_rocksdb_cqwrite_progress_response_body::CheckRocksdbCqWriteResult;
    use rocketmq_remoting::protocol::body::check_rocksdb_cqwrite_progress_response_body::CheckStatus;
    use rocketmq_remoting::protocol::header::check_rocksdb_cq_write_progress_request_header::CheckRocksdbCqWriteProgressRequestHeader;
    use rocketmq_remoting::protocol::header::empty_header::EmptyHeader;
    use rocketmq_remoting::protocol::header::get_earliest_msg_storetime_request_header::GetEarliestMsgStoretimeRequestHeader;
    use rocketmq_remoting::protocol::header::get_earliest_msg_storetime_response_header::GetEarliestMsgStoretimeResponseHeader;
    use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
    use rocketmq_remoting::protocol::RemotingDeserializable;
    use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
    use rocketmq_rust::ArcMut;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;

    use super::*;
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
        assert!(runtime.initialize().await);
        runtime
    }

    async fn create_test_channel() -> Channel {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind local test listener");
        let local_addr = listener.local_addr().expect("local listener addr");
        let std_stream = std::net::TcpStream::connect(local_addr).expect("connect local test listener");
        std_stream.set_nonblocking(true).expect("set nonblocking");
        drop(listener);
        let tcp_stream = tokio::net::TcpStream::from_std(std_stream).expect("convert tcp stream");
        let connection = Connection::new(tcp_stream);
        let response_table = ArcMut::new(HashMap::<i32, ResponseFuture>::new());
        let inner = ArcMut::new(ChannelInner::new(connection, response_table));
        Channel::new(inner, local_addr, local_addr)
    }

    #[tokio::test]
    async fn get_earliest_msg_store_time_returns_store_timestamp() {
        let mut runtime = new_test_runtime("earliest-time").await;
        let mut inner = runtime.inner_for_test().clone();
        inner
            .topic_config_manager_mut()
            .update_topic_config(ArcMut::new(TopicConfig::with_queues("topic-a", 1, 1)));

        let expected_timestamp = inner
            .message_store()
            .expect("message store should exist")
            .get_earliest_message_time(&CheetahString::from_static_str("topic-a"), 0);

        let mut handler = OffsetRequestHandler::new(inner);
        let mut request = RemotingCommand::create_request_command(
            RequestCode::GetEarliestMsgStoreTime,
            GetEarliestMsgStoretimeRequestHeader {
                topic: CheetahString::from_static_str("topic-a"),
                queue_id: 0,
                topic_request_header: None,
            },
        );
        request.make_custom_header_to_net();
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));

        let response = handler
            .get_earliest_msg_store_time(channel, ctx, RequestCode::GetEarliestMsgStoreTime, &mut request)
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
        let mut runtime = new_test_runtime("clean-expired-cq").await;
        let inner = runtime.inner_for_test().clone();
        let mut handler = OffsetRequestHandler::new(inner);
        let mut request =
            RemotingCommand::create_request_command(RequestCode::CleanExpiredConsumequeue, EmptyHeader {});
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));

        let response = handler
            .clean_expired_consumequeue(channel, ctx, RequestCode::CleanExpiredConsumequeue, &mut request)
            .await
            .expect("clean expired consumequeue should succeed")
            .expect("clean expired consumequeue should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }

    #[tokio::test]
    async fn check_rocksdb_cq_write_progress_returns_no_need_check_for_local_file_store() {
        let mut runtime = new_test_runtime("check-rocksdb-progress").await;
        let inner = runtime.inner_for_test().clone();
        let mut handler = OffsetRequestHandler::new(inner);
        let mut request = RemotingCommand::create_request_command(
            RequestCode::CheckRocksdbCqWriteProgress,
            CheckRocksdbCqWriteProgressRequestHeader {
                topic: CheetahString::from_static_str("topic-a"),
                check_store_time: 0,
                rpc: None,
            },
        );
        request.make_custom_header_to_net();
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));

        let mut response = handler
            .check_rocksdb_cq_write_progress(channel, ctx, RequestCode::CheckRocksdbCqWriteProgress, &mut request)
            .await
            .expect("check rocksdb cq write progress should succeed")
            .expect("check rocksdb cq write progress should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        let body = CheckRocksdbCqWriteResult::decode(
            response
                .take_body()
                .expect("check rocksdb cq write progress should contain body")
                .as_ref(),
        )
        .expect("decode check rocksdb cq write progress body");
        assert_eq!(body.get_check_status(), CheckStatus::CheckOk);
        assert_eq!(
            body.check_result.as_deref(),
            Some("It is not CombineConsumeQueueStore, no need check")
        );

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }
}
