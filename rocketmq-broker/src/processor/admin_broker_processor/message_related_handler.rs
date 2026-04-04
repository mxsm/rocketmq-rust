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

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_ext::MessageExt;
use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_common::common::message::MessageConst;
use rocketmq_common::common::message::MessageTrait;
use rocketmq_common::MessageAccessor::MessageAccessor;
use rocketmq_common::MessageDecoder;
use rocketmq_remoting::code::request_code::RequestCode;
use rocketmq_remoting::code::response_code::ResponseCode;
use rocketmq_remoting::net::channel::Channel;
use rocketmq_remoting::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use rocketmq_remoting::protocol::header::resume_check_half_message_request_header::ResumeCheckHalfMessageRequestHeader;
use rocketmq_remoting::protocol::header::search_offset_request_header::SearchOffsetRequestHeader;
use rocketmq_remoting::protocol::header::search_offset_response_header::SearchOffsetResponseHeader;
use rocketmq_remoting::protocol::remoting_command::RemotingCommand;
use rocketmq_remoting::protocol::static_topic::topic_queue_mapping_context::TopicQueueMappingContext;
use rocketmq_remoting::rpc::rpc_client::RpcClient;
use rocketmq_remoting::rpc::rpc_request::RpcRequest;
use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContext;
use rocketmq_rust::ArcMut;
use rocketmq_store::base::message_status_enum::PutMessageStatus;
use rocketmq_store::base::message_store::MessageStore;

use crate::broker_runtime::BrokerRuntimeInner;
use crate::transaction::queue::transactional_message_util::TransactionalMessageUtil;

pub(super) struct MessageRelatedHandler<MS: MessageStore> {
    broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>,
}

impl<MS> MessageRelatedHandler<MS>
where
    MS: MessageStore,
{
    pub fn new(broker_runtime_inner: ArcMut<BrokerRuntimeInner<MS>>) -> Self {
        Self { broker_runtime_inner }
    }
}

impl<MS> MessageRelatedHandler<MS>
where
    MS: MessageStore,
{
    pub async fn search_offset_by_timestamp(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let search_offset_request_header = request.decode_command_custom_header::<SearchOffsetRequestHeader>()?;
        let mapping_context = self
            .broker_runtime_inner
            .topic_queue_mapping_manager()
            .build_topic_queue_mapping_context(&search_offset_request_header, false);
        let rewrite_result = self
            .rewrite_request_for_static_topic(&search_offset_request_header, mapping_context)
            .await?;
        if rewrite_result.is_some() {
            return Ok(rewrite_result);
        }
        let response = RemotingCommand::create_response_command();
        let message_store = match self.broker_runtime_inner.message_store() {
            Some(store) => store,
            None => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark("message store is none"),
                ));
            }
        };
        let offset = message_store.get_offset_in_queue_by_time_with_boundary(
            &search_offset_request_header.topic,
            search_offset_request_header.queue_id,
            search_offset_request_header.timestamp,
            search_offset_request_header.boundary_type,
        );
        let response_header = SearchOffsetResponseHeader { offset };

        Ok(Some(response.set_command_custom_header(response_header)))
    }

    pub async fn resume_check_half_message(
        &mut self,
        _channel: Channel,
        _ctx: ConnectionHandlerContext,
        _request_code: RequestCode,
        request: &mut RemotingCommand,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        let request_header = request.decode_command_custom_header::<ResumeCheckHalfMessageRequestHeader>()?;
        let response = RemotingCommand::create_response_command();

        let Some(msg_id) = request_header.msg_id.as_ref().filter(|msg_id| !msg_id.is_empty()) else {
            return Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("msgId is missing"),
            ));
        };

        let message_id = match MessageDecoder::decode_message_id(msg_id.as_str()) {
            Ok(message_id) => message_id,
            Err(error) => {
                return Ok(Some(
                    response
                        .set_code(ResponseCode::SystemError)
                        .set_remark(format!("invalid msgId: {error}")),
                ));
            }
        };

        let Some(mut message_ext) = self
            .broker_runtime_inner
            .message_store()
            .unwrap()
            .look_message_by_offset(message_id.offset)
        else {
            return Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("half message not found by msgId"),
            ));
        };

        MessageAccessor::put_property(
            &mut message_ext,
            CheetahString::from_static_str(MessageConst::PROPERTY_TRANSACTION_CHECK_TIMES),
            CheetahString::from_static_str("0"),
        );

        let put_message_result = self
            .broker_runtime_inner
            .message_store_mut()
            .as_mut()
            .unwrap()
            .put_message(to_half_message_ext_broker_inner(&message_ext))
            .await;

        if put_message_result.put_message_status() == PutMessageStatus::PutOk || put_message_result.is_ok() {
            Ok(Some(response.set_code(ResponseCode::Success)))
        } else {
            Ok(Some(
                response
                    .set_code(ResponseCode::SystemError)
                    .set_remark("Put message back to RMQ_SYS_TRANS_HALF_TOPIC failed."),
            ))
        }
    }

    async fn rewrite_request_for_static_topic(
        &mut self,
        request_header: &SearchOffsetRequestHeader,
        mapping_context: TopicQueueMappingContext,
    ) -> rocketmq_error::RocketMQResult<Option<RemotingCommand>> {
        if mapping_context.mapping_detail.is_none() {
            return Ok(None);
        }

        let mapping_detail = mapping_context.mapping_detail.as_ref().unwrap();
        let mapping_items = &mapping_context.mapping_item_list;

        if !mapping_context.is_leader() {
            return Ok(Some(
                RemotingCommand::create_response_command_with_code(ResponseCode::NotLeaderForQueue).set_remark(
                    format!(
                        "{}-{:?} does not exist in request process of current broker {:?}",
                        mapping_context.topic, mapping_context.global_id, mapping_detail.topic_queue_mapping_info.bname
                    ),
                ),
            ));
        }

        // TO DO should make sure the timestampOfOffset is equal or bigger than the searched
        // timestamp
        let timestamp = request_header.timestamp;
        let mut offset: i64 = -1;

        for item in mapping_items.iter() {
            // Check if logic offset is decided (logic_offset >= 0)
            if item.logic_offset < 0 {
                continue;
            }

            if mapping_detail.topic_queue_mapping_info.bname == item.bname {
                // Local broker - query directly from message store
                if let Some(message_store) = self.broker_runtime_inner.message_store() {
                    let local_offset = message_store.get_offset_in_queue_by_time_with_boundary(
                        &mapping_context.topic,
                        item.queue_id,
                        timestamp,
                        request_header.boundary_type,
                    );
                    if local_offset > 0 {
                        offset = item.compute_static_queue_offset_strictly(local_offset);
                        break;
                    }
                }
            } else {
                // Remote broker - make RPC call
                let mut remote_request_header = SearchOffsetRequestHeader {
                    topic: mapping_context.topic.clone(),
                    queue_id: item.queue_id,
                    timestamp,
                    boundary_type: request_header.boundary_type,
                    topic_request_header: request_header.topic_request_header.clone(),
                };
                remote_request_header.set_lo(Some(false));
                remote_request_header.set_broker_name(item.bname.clone().unwrap_or_default());

                let rpc_request = RpcRequest::new(
                    RequestCode::SearchOffsetByTimestamp.to_i32(),
                    remote_request_header,
                    None,
                );

                let rpc_response = self
                    .broker_runtime_inner
                    .broker_outer_api()
                    .rpc_client()
                    .invoke(rpc_request, self.broker_runtime_inner.broker_config().forward_timeout)
                    .await;

                match rpc_response {
                    Err(e) => {
                        return Ok(Some(
                            RemotingCommand::create_response_command_with_code(ResponseCode::SystemError)
                                .set_remark(format!("{e}")),
                        ));
                    }
                    Ok(response) => {
                        match response.get_header::<SearchOffsetResponseHeader>() {
                            None => {
                                continue;
                            }
                            Some(offset_response_header) => {
                                let remote_offset = offset_response_header.offset;
                                // Check if offset is valid
                                if remote_offset < 0 {
                                    continue;
                                }
                                // Check if end offset is decided and offset exceeds it
                                if item.check_if_end_offset_decided() && remote_offset >= item.end_offset {
                                    continue;
                                }
                                offset = item.compute_static_queue_offset_strictly(remote_offset);
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(Some(RemotingCommand::create_response_command_with_header(
            SearchOffsetResponseHeader { offset },
        )))
    }
}

fn to_half_message_ext_broker_inner(msg_ext: &MessageExt) -> MessageExtBrokerInner {
    let mut inner = MessageExtBrokerInner::default();
    inner.set_topic(CheetahString::from_static_str(
        TransactionalMessageUtil::build_half_topic(),
    ));
    if let Some(body) = msg_ext.get_body() {
        inner.set_body(body.clone());
    }
    inner.set_flag(msg_ext.get_flag());
    MessageAccessor::set_properties(&mut inner, msg_ext.get_properties().clone());
    inner.properties_string = MessageDecoder::message_properties_to_string(msg_ext.get_properties());
    inner.tags_code = MessageExtBrokerInner::tags_string_to_tags_code(msg_ext.tags().unwrap_or_default().as_str());
    inner.message_ext_inner.queue_id = 0;
    inner.message_ext_inner.sys_flag = msg_ext.sys_flag();
    inner.message_ext_inner.born_timestamp = msg_ext.born_timestamp;
    inner.message_ext_inner.born_host = msg_ext.born_host;
    inner.message_ext_inner.store_timestamp = msg_ext.store_timestamp;
    inner.message_ext_inner.store_host = msg_ext.store_host;
    inner.message_ext_inner.msg_id = msg_ext.msg_id().clone();
    inner.message_ext_inner.reconsume_times = msg_ext.reconsume_times();
    inner.set_wait_store_msg_ok(false);
    inner
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::SystemTime;

    use bytes::Bytes;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::message::message_ext_broker_inner::MessageExtBrokerInner;
    use rocketmq_remoting::base::response_future::ResponseFuture;
    use rocketmq_remoting::connection::Connection;
    use rocketmq_remoting::net::channel::Channel;
    use rocketmq_remoting::net::channel::ChannelInner;
    use rocketmq_remoting::protocol::header::resume_check_half_message_request_header::ResumeCheckHalfMessageRequestHeader;
    use rocketmq_remoting::runtime::connection_handler_context::ConnectionHandlerContextWrapper;
    use rocketmq_rust::ArcMut;
    use rocketmq_store::base::message_store::MessageStore;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;

    use super::*;
    use crate::broker_runtime::BrokerRuntime;

    fn temp_test_root(label: &str) -> std::path::PathBuf {
        let millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("time should move forward")
            .as_millis();
        std::env::temp_dir().join(format!("rocketmq-rust-admin-message-related-{label}-{millis}"))
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
        let response_table = ArcMut::new(std::collections::HashMap::<i32, ResponseFuture>::new());
        let inner = ArcMut::new(ChannelInner::new(connection, response_table));
        Channel::new(inner, local_addr, local_addr)
    }

    #[tokio::test]
    async fn resume_check_half_message_requeues_half_message() {
        let mut runtime = new_test_runtime("resume-half").await;
        let mut inner = runtime.inner_for_test().clone();
        let mut half_message = MessageExtBrokerInner::default();
        half_message.set_topic(CheetahString::from_static_str(
            TransactionalMessageUtil::build_half_topic(),
        ));
        half_message.message_ext_inner.set_queue_id(0);
        half_message.set_body(Bytes::from_static(b"half-message"));
        half_message.put_property(
            CheetahString::from_static_str(MessageConst::PROPERTY_REAL_TOPIC),
            CheetahString::from_static_str("real-topic"),
        );

        let put_message_result = inner
            .message_store_mut()
            .as_mut()
            .unwrap()
            .put_message(half_message)
            .await;
        assert!(put_message_result.is_ok());
        let max_phy_offset_before_resume = inner.message_store().unwrap().get_max_phy_offset();
        let msg_id = put_message_result
            .append_message_result()
            .and_then(|result| result.get_message_id())
            .expect("put message should return msg id");

        let mut handler = MessageRelatedHandler::new(inner.clone());
        let channel = create_test_channel().await;
        let ctx = ArcMut::new(ConnectionHandlerContextWrapper::new(channel.clone()));
        let mut request = RemotingCommand::create_request_command(
            RequestCode::ResumeCheckHalfMessage,
            ResumeCheckHalfMessageRequestHeader {
                topic: CheetahString::from_static_str(TransactionalMessageUtil::build_half_topic()),
                msg_id: Some(CheetahString::from_string(msg_id)),
            },
        );
        request.make_custom_header_to_net();

        let response = handler
            .resume_check_half_message(channel, ctx, RequestCode::ResumeCheckHalfMessage, &mut request)
            .await
            .expect("resume check half message should succeed")
            .expect("resume check half message should return response");

        assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
        assert!(inner.message_store().unwrap().get_max_phy_offset() > max_phy_offset_before_resume);

        let _ = std::fs::remove_dir_all(runtime.message_store_config().store_path_root_dir.as_str());
    }
}
