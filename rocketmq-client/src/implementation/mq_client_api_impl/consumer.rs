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

use super::admin::parse_lite_order_count_info_like_java;
use super::admin::pop_msg_queue_offset_for_index;
use super::admin::split_lite_dispatch_value;
use super::request_builder::notification_request;
use super::response_decoder::notify_result_from_response;
use super::*;

pub struct ConsumerClient<'a> {
    api: &'a MQClientAPIImpl,
}

impl ConsumerClient<'_> {
    pub async fn consumer_offset(
        &self,
        addr: &str,
        request_header: QueryConsumerOffsetRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        self.api
            .query_consumer_offset(addr, request_header, timeout_millis)
            .await
    }
}

impl MQClientAPIImpl {
    #[must_use]
    pub fn consumer_client(&self) -> ConsumerClient<'_> {
        ConsumerClient { api: self }
    }
}

impl MQClientAPIImpl {
    pub async fn notification(
        &self,
        broker_addr: &CheetahString,
        request_header: NotificationRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<bool> {
        self.notification_with_polling_stats(broker_addr, request_header, timeout_millis)
            .await
            .map(|result| result.is_has_msg())
    }

    pub async fn notification_with_polling_stats(
        &self,
        broker_addr: &CheetahString,
        request_header: NotificationRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<NotifyResult> {
        let request = notification_request(request_header);
        let response = self
            .remoting_client
            .invoke_request(Some(broker_addr), request, timeout_millis)
            .await?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => notify_result_from_response(&response),
            _ => Err(client_broker_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string()),
                broker_addr.to_string()
            )),
        }
    }

    pub async fn get_consumer_id_list_by_group(
        &self,
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
                let _body = response.body();
                if let Some(body) = response.body() {
                    return match GetConsumerListByGroupResponseBody::decode(body) {
                        Ok(value) => Ok(value.consumer_id_list),
                        Err(_e) => Err(mq_client_err!(response
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
                ))
            }
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn get_consumer_connection_list(
        &self,
        addr: &str,
        consumer_group: CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<rocketmq_protocol::protocol::body::consumer_connection::ConsumerConnection>
    {
        let request_header = GetConsumerConnectionListRequestHeader {
            consumer_group,
            rpc_request_header: None,
        };
        let request = RemotingCommand::create_request_command(RequestCode::GetConsumerConnectionList, request_header);
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
                if let Some(body) = response.body() {
                    return ConsumerConnection::decode(body);
                }
            }
            _ => {
                return Err(client_broker_err!(
                    response.code(),
                    response.remark().map_or("".to_string(), |s| s.to_string()),
                    addr.to_string()
                ))
            }
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn get_producer_connection_list(
        &self,
        addr: &str,
        producer_group: CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<ProducerConnection> {
        let request_header = GetProducerConnectionListRequestHeader {
            producer_group,
            rpc_request_header: None,
        };
        let request = RemotingCommand::create_request_command(RequestCode::GetProducerConnectionList, request_header);
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
                if let Some(body) = response.body() {
                    return ProducerConnection::decode(body);
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

    pub async fn invoke_broker_to_get_consumer_status(
        &self,
        addr: &str,
        topic: CheetahString,
        group: CheetahString,
        client_addr: CheetahString,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<HashMap<CheetahString, HashMap<MessageQueue, i64>>> {
        let request_header = GetConsumerStatusRequestHeader {
            topic,
            group,
            client_addr: if client_addr.is_empty() {
                None
            } else {
                Some(client_addr)
            },
            rpc_request_header: None,
        };
        let request =
            RemotingCommand::create_request_command(RequestCode::InvokeBrokerToGetConsumerStatus, request_header);
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
                if let Some(body) = response.body() {
                    if let Some(status_body) = GetConsumerStatusBody::decode(body) {
                        return Ok(status_body.consumer_table);
                    }
                }
                Ok(HashMap::new())
            }
            _ => Err(mq_client_err!(
                response.code(),
                format!(
                    "invoke broker to get consumer status failed, remark={}",
                    response.remark().map_or("".to_string(), |s| s.to_string()),
                )
            )),
        }
    }

    pub async fn get_all_producer_info(
        &self,
        addr: &str,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<ProducerTableInfo> {
        let request = RemotingCommand::create_request_command(RequestCode::GetAllProducerInfo, EmptyHeader {});
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
                if let Some(body) = response.body() {
                    return ProducerTableInfo::decode(body);
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
        &self,
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

    pub async fn update_consumer_offset_one_way(
        &self,
        addr: &str,
        request_header: UpdateConsumerOffsetRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.update_consumer_offset_oneway(addr, request_header, timeout_millis)
            .await
    }

    pub async fn update_consumer_offset(
        &self,
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

    pub async fn update_consumer_offset_async(
        &self,
        addr: &CheetahString,
        request_header: UpdateConsumerOffsetRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.update_consumer_offset(addr, request_header, timeout_millis).await
    }

    pub async fn query_consumer_offset(
        &self,
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
                let response_header = response.decode_command_custom_header::<QueryConsumerOffsetResponseHeader>()?;
                return response_header.offset.ok_or_else(|| {
                    client_broker_err!(
                        response.code(),
                        "QueryConsumerOffset response header missing offset".to_string(),
                        addr.to_string()
                    )
                });
            }
            ResponseCode::QueryNotFound => {
                return Err(client_broker_err!(
                    response.code(),
                    response.remark().map_or("".to_string(), |s| s.to_string()),
                    addr.to_string()
                ));
            }
            _ => {}
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn query_consumer_offset_with_future(
        &self,
        addr: &str,
        request_header: QueryConsumerOffsetRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        self.query_consumer_offset(addr, request_header, timeout_millis).await
    }

    pub async fn query_message(
        this: &Arc<Self>,
        addr: &CheetahString,
        request_header: QueryMessageRequestHeader,
        unique_key_flag: bool,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<Option<(QueryMessageResponseHeader, Option<bytes::Bytes>)>> {
        let mut request = RemotingCommand::create_request_command(RequestCode::QueryMessage, request_header);
        if unique_key_flag {
            request.ensure_ext_fields_initialized();
            request.add_ext_field(mix_all::UNIQUE_MSG_QUERY_FLAG, "true");
        }
        let response = this
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let response_header = response
                    .decode_command_custom_header::<QueryMessageResponseHeader>()
                    .map_err(|e| {
                        RocketMQError::response_process_failed("decode QueryMessageResponseHeader", e.to_string())
                    })?;
                let body = response.body().cloned();
                Ok(Some((response_header, body)))
            }
            ResponseCode::QueryNotFound => Ok(None),
            _ => Err(client_broker_err!(
                response.code(),
                response.remark().map_or("".to_string(), |s| s.to_string()),
                addr.to_string()
            )),
        }
    }

    pub async fn pull_message<PCB>(
        this: Arc<Self>,
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
                let tracker = this.background_tasks.clone();
                let shutdown_token = this.background_shutdown.clone();
                let service_context = this.service_context.clone();
                Self::spawn_api_background_task(
                    &service_context,
                    "rocketmq-client-pull-message-async",
                    &tracker,
                    &shutdown_token,
                    async move {
                        let _ = this
                            .pull_message_async(&addr, request, timeout_millis, pull_callback)
                            .await;
                    },
                );
                Ok(None)
            }
            CommunicationMode::Oneway => Ok(None),
        }
    }

    pub(super) async fn pull_message_sync(
        &self,
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

    pub(super) async fn pull_message_async<PCB>(
        &self,
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
                        pull_callback.on_exception(error);
                    }
                }
            }
            Err(err) => {
                pull_callback.on_exception(err);
            }
        }
        Ok(())
    }

    pub(super) async fn process_pull_response(
        &self,
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
                ));
            }
        };
        let response_header = response.decode_command_custom_header::<PullMessageResponseHeader>()?;
        let next_begin_offset =
            java_long_to_u64_field("pullMessage", "nextBeginOffset", response_header.next_begin_offset)?;
        let min_offset = java_long_to_u64_field("pullMessage", "minOffset", response_header.min_offset)?;
        let max_offset = java_long_to_u64_field("pullMessage", "maxOffset", response_header.max_offset)?;
        let pull_result = PullResultExt {
            pull_result: PullResult {
                pull_status,
                next_begin_offset,
                min_offset,
                max_offset,
                msg_found_list: Some(vec![]),
            },
            suggest_which_broker_id: response_header.suggest_which_broker_id,
            message_binary: response.take_body(),
            offset_delta: response_header.offset_delta,
        };
        Ok(pull_result)
    }

    pub async fn consumer_send_message_back(
        &self,
        addr: &str,
        broker_name: Option<&str>,
        msg: &MessageExt,
        consumer_group: &str,
        delay_level: i32,
        timeout_millis: u64,
        max_consume_retry_times: i32,
    ) -> rocketmq_error::RocketMQResult<()> {
        let header = Self::consumer_send_message_back_request_header(
            msg,
            broker_name,
            consumer_group,
            delay_level,
            max_consume_retry_times,
        );

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

    pub(super) fn consumer_send_message_back_request_header(
        msg: &MessageExt,
        broker_name: Option<&str>,
        consumer_group: &str,
        delay_level: i32,
        max_consume_retry_times: i32,
    ) -> ConsumerSendMsgBackRequestHeader {
        ConsumerSendMsgBackRequestHeader {
            offset: msg.commit_log_offset,
            group: CheetahString::from_slice(consumer_group),
            delay_level,
            origin_msg_id: Some(CheetahString::from_slice(msg.msg_id.as_str())),
            origin_topic: Some(CheetahString::from_slice(msg.topic())),
            unit_mode: false,
            max_reconsume_times: Some(max_consume_retry_times),
            rpc_request_header: Some(RpcRequestHeader {
                namespace: None,
                namespaced: None,
                broker_name: broker_name.map(CheetahString::from_slice),
                oneway: None,
            }),
        }
    }

    pub async fn send_message_back_async(
        &self,
        addr: &CheetahString,
        request_header: ConsumerSendMsgBackRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<RemotingCommand> {
        let request = RemotingCommand::create_request_command(RequestCode::ConsumerSendMsgBack, request_header);
        self.remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await
    }

    pub async fn unregister_client(
        &self,
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
        &self,
        addr: &CheetahString,
        request_body: UnlockBatchRequestBody,
        timeout_millis: u64,
        oneway: bool,
    ) -> rocketmq_error::RocketMQResult<()> {
        let mut request =
            RemotingCommand::create_request_command(RequestCode::UnlockBatchMq, UnlockBatchMqRequestHeader::default());
        request.set_body_mut_ref(request_body.encode()?);
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

    pub async fn unlock_batch_mq_oneway(
        &self,
        addr: &CheetahString,
        request_body: UnlockBatchRequestBody,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.unlock_batch_mq(addr, request_body, timeout_millis, true).await
    }

    pub async fn lock_batch_mq(
        &self,
        addr: &str,
        request_body: LockBatchRequestBody,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<HashSet<MessageQueue>> {
        let mut request =
            RemotingCommand::create_request_command(RequestCode::LockBatchMq, LockBatchMqRequestHeader::default());
        request.set_body_mut_ref(request_body.encode()?);
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

    pub async fn lock_batch_mq_with_future(
        &self,
        addr: &str,
        request_body: LockBatchRequestBody,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<HashSet<MessageQueue>> {
        self.lock_batch_mq(addr, request_body, timeout_millis).await
    }
}

impl MQClientAPIImpl {
    pub async fn get_max_offset(
        &self,
        addr: &str,
        message_queue: &MessageQueue,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        let request_header = GetMaxOffsetRequestHeader {
            topic: CheetahString::from_slice(message_queue.topic_str()),
            queue_id: message_queue.queue_id(),
            committed: false,
            topic_request_header: Some(TopicRequestHeader {
                rpc_request_header: Some(RpcRequestHeader {
                    broker_name: Some(CheetahString::from_slice(message_queue.broker_name())),
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
            let response_header = response.decode_command_custom_header::<GetMaxOffsetResponseHeader>()?;
            return Ok(response_header.offset);
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn get_min_offset(
        &self,
        addr: &str,
        message_queue: &MessageQueue,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        let request_header = GetMinOffsetRequestHeader {
            topic: CheetahString::from_slice(message_queue.topic_str()),
            queue_id: message_queue.queue_id(),
            topic_request_header: Some(TopicRequestHeader {
                rpc_request_header: Some(RpcRequestHeader {
                    broker_name: Some(CheetahString::from_slice(message_queue.broker_name())),
                    ..Default::default()
                }),
                lo: None,
            }),
        };

        let request = RemotingCommand::create_request_command(RequestCode::GetMinOffset, request_header);

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
            let response_header = response.decode_command_custom_header::<GetMinOffsetResponseHeader>()?;
            return Ok(response_header.offset);
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn get_earliest_msg_store_time(
        &self,
        addr: &str,
        message_queue: &MessageQueue,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        let request_header = GetEarliestMsgStoretimeRequestHeader {
            topic: CheetahString::from_slice(message_queue.topic_str()),
            queue_id: message_queue.queue_id(),
            topic_request_header: Some(TopicRequestHeader {
                rpc_request_header: Some(RpcRequestHeader {
                    broker_name: Some(CheetahString::from_slice(message_queue.broker_name())),
                    ..Default::default()
                }),
                lo: None,
            }),
        };

        let request = RemotingCommand::create_request_command(RequestCode::GetEarliestMsgStoreTime, request_header);

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
            let response_header = response.decode_command_custom_header::<GetEarliestMsgStoretimeResponseHeader>()?;
            return Ok(response_header.timestamp);
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn get_earliest_msg_storetime(
        &self,
        addr: &str,
        message_queue: &MessageQueue,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        self.get_earliest_msg_store_time(addr, message_queue, timeout_millis)
            .await
    }

    /// Searches for the queue offset whose store timestamp is closest to `timestamp`.
    ///
    /// When `boundary_type` is [`BoundaryType::Lower`], the returned offset is the earliest one
    /// whose store timestamp is greater than or equal to `timestamp`.  When
    /// [`BoundaryType::Upper`], the latest such offset is returned.
    ///
    /// Mirrors `MQClientAPIImpl.searchOffset` in the Java implementation.
    ///
    /// # Errors
    ///
    /// Returns an error if the broker returns a non-success response code or is unreachable.
    pub async fn search_offset_by_timestamp(
        &self,
        addr: &str,
        message_queue: &MessageQueue,
        timestamp: i64,
        boundary_type: BoundaryType,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        let request_header = SearchOffsetRequestHeader {
            topic: message_queue.topic().clone(),
            lite_topic: None,
            queue_id: message_queue.queue_id(),
            timestamp,
            boundary_type,
            topic_request_header: Some(TopicRequestHeader {
                rpc_request_header: Some(RpcRequestHeader {
                    broker_name: Some(message_queue.broker_name().clone()),
                    ..Default::default()
                }),
                lo: None,
            }),
        };
        let request = RemotingCommand::create_request_command(RequestCode::SearchOffsetByTimestamp, request_header);
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
            let response_header = response.decode_command_custom_header::<SearchOffsetResponseHeader>()?;
            return Ok(response_header.offset);
        }
        Err(client_broker_err!(
            response.code(),
            response.remark().map_or("".to_string(), |s| s.to_string()),
            addr.to_string()
        ))
    }

    pub async fn search_offset(
        &self,
        addr: &str,
        message_queue: &MessageQueue,
        timestamp: i64,
        boundary_type: BoundaryType,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<i64> {
        self.search_offset_by_timestamp(addr, message_queue, timestamp, boundary_type, timeout_millis)
            .await
    }

    pub async fn set_message_request_mode(
        &self,
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
        let request =
            RemotingCommand::create_remoting_command(RequestCode::SetMessageRequestMode).set_body(body.encode()?);
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
        &self,
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
        let request = RemotingCommand::new_request(RequestCode::QueryAssignment, request_body.encode()?);
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
                ack_callback.on_exception(e);
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
                        pop_callback.on_error(e);
                    }
                }
            }
            Err(e) => {
                pop_callback.on_error(e);
            }
        }
        Ok(())
    }

    pub async fn pop_lite_message_async<PC>(
        &self,
        broker_name: &CheetahString,
        addr: &CheetahString,
        request_header: PopLiteMessageRequestHeader,
        timeout_millis: u64,
        mut pop_callback: PC,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        PC: PopCallback + 'static,
    {
        let bind_topic = request_header.topic.clone();
        let request = RemotingCommand::create_request_command(RequestCode::PopLiteMessage, request_header);
        match self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await
        {
            Ok(response) => {
                let result = self.process_pop_lite_response(broker_name, response, &bind_topic);
                match result {
                    Ok(pop_result) => {
                        pop_callback.on_success(pop_result).await;
                    }
                    Err(e) => {
                        pop_callback.on_error(e);
                    }
                }
            }
            Err(e) => {
                pop_callback.on_error(e);
            }
        }
        Ok(())
    }

    pub(super) fn process_pop_response(
        &self,
        broker_name: &CheetahString,
        mut response: RemotingCommand,
        topic: &CheetahString,
        is_order: bool,
    ) -> rocketmq_error::RocketMQResult<PopResult> {
        let response_code = ResponseCode::from(response.code());
        let (pop_status, msg_found_list) = match response_code {
            ResponseCode::Success => {
                let raw_response_code = response.code();
                let body = response
                    .get_body_mut()
                    .ok_or_else(|| client_broker_err!(raw_response_code, "PopMessage response body is empty"))?;
                let messages = MessageDecoder::decodes_batch(
                    body,
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
        let sort_map = Self::build_queue_offset_sorted_map(
            topic.as_str(),
            pop_result
                .msg_found_list
                .as_ref()
                .map_or(&[] as &[MessageExt], |v| v.as_slice()),
        )?;
        let mut map = HashMap::with_capacity(5);
        for message in pop_result.msg_found_list.as_mut().map_or(&mut vec![], |v| v) {
            if start_offset_info.is_empty() {
                let key = CheetahString::from_string(format!("{}{}", message.topic(), message.queue_id() as i64));
                if !map.contains_key(&key) {
                    let extra_info = ExtraInfoUtil::build_extra_info(
                        message.queue_offset(),
                        response_header.pop_time as i64,
                        response_header.invisible_time as i64,
                        response_header.revive_qid as i32,
                        message.topic(),
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
                let ck = message.property(&CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK));
                if ck.is_none() {
                    let dispatch = message
                        .property(&CheetahString::from_static_str(
                            MessageConst::PROPERTY_INNER_MULTI_DISPATCH,
                        ))
                        .unwrap_or_default();
                    let (queue_offset_key, queue_id_key) = if mix_all::is_lmq(Some(topic.as_str()))
                        && !dispatch.is_empty()
                    {
                        let queues: Vec<&str> = dispatch.split(mix_all::MULTI_DISPATCH_QUEUE_SPLITTER).collect();
                        let data = message
                            .property(&CheetahString::from_static_str(
                                MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET,
                            ))
                            .unwrap_or_default();
                        let queue_offsets: Vec<&str> = data.split(mix_all::MULTI_DISPATCH_QUEUE_SPLITTER).collect();
                        let Some(position) = queues.iter().position(|&q| q == topic.as_str()) else {
                            warn!(
                                "LMQ dispatch queue does not contain topic={}, dispatch={}",
                                topic, dispatch
                            );
                            continue;
                        };
                        let Some(offset_value) = queue_offsets.get(position) else {
                            warn!(
                                "LMQ queue offset is missing for topic={}, dispatch={}, offsets={}",
                                topic, dispatch, data
                            );
                            continue;
                        };
                        let Ok(offset) = offset_value.parse::<i64>() else {
                            warn!(
                                "LMQ queue offset is invalid for topic={}, offset={}",
                                topic, offset_value
                            );
                            continue;
                        };
                        let queue_id_key =
                            ExtraInfoUtil::get_start_offset_info_map_key(topic.as_str(), mix_all::LMQ_QUEUE_ID as i64);
                        let queue_offset_key = ExtraInfoUtil::get_queue_offset_map_key(
                            topic.as_str(),
                            mix_all::LMQ_QUEUE_ID as i64,
                            offset,
                        );
                        if !sort_map.contains_key(&queue_id_key) {
                            warn!("LMQ start offset info missing for key={}", queue_id_key);
                            continue;
                        }
                        let Some(start_offset) = start_offset_info.get(&queue_id_key).copied() else {
                            warn!("LMQ start offset info missing for key={}", queue_id_key);
                            continue;
                        };
                        let Some(msg_queue_offset) =
                            pop_msg_queue_offset_for_index(&queue_id_key, offset, &sort_map, &msg_offset_info)
                        else {
                            warn!(
                                "LMQ msg offset info missing for key={}, offset={}",
                                queue_id_key, offset
                            );
                            continue;
                        };
                        if msg_queue_offset != offset {
                            warn!(
                                "Queue offset[{}] of msg is strange, not equal to the stored in msg, {:?}",
                                msg_queue_offset, message
                            );
                        }
                        let extra_info = ExtraInfoUtil::build_extra_info_with_offset(
                            start_offset,
                            response_header.pop_time as i64,
                            response_header.invisible_time as i64,
                            response_header.revive_qid as i32,
                            message.topic(),
                            broker_name,
                            mix_all::LMQ_QUEUE_ID as i32,
                            msg_queue_offset,
                        );
                        message.put_property(
                            CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK),
                            CheetahString::from_string(extra_info),
                        );
                        (queue_offset_key, queue_id_key)
                    } else {
                        let queue_id_key =
                            ExtraInfoUtil::get_start_offset_info_map_key(message.topic(), message.queue_id() as i64);
                        let queue_offset_key = ExtraInfoUtil::get_queue_offset_map_key(
                            message.topic(),
                            message.queue_id() as i64,
                            message.queue_offset(),
                        );
                        let queue_offset = message.queue_offset();
                        if !sort_map.contains_key(&queue_id_key) {
                            warn!("start offset info missing for key={}", queue_id_key);
                            continue;
                        }
                        let Some(start_offset) = start_offset_info.get(&queue_id_key).copied() else {
                            warn!("start offset info missing for key={}", queue_id_key);
                            continue;
                        };
                        let Some(msg_queue_offset) =
                            pop_msg_queue_offset_for_index(&queue_id_key, queue_offset, &sort_map, &msg_offset_info)
                        else {
                            warn!(
                                "msg offset info missing for key={}, offset={}",
                                queue_id_key, queue_offset
                            );
                            continue;
                        };
                        if msg_queue_offset != queue_offset {
                            warn!(
                                "Queue offset[{}] of msg is strange, not equal to the stored in msg, {:?}",
                                msg_queue_offset, message
                            );
                        }
                        let extra_info = ExtraInfoUtil::build_extra_info_with_offset(
                            start_offset,
                            response_header.pop_time as i64,
                            response_header.invisible_time as i64,
                            response_header.revive_qid as i32,
                            message.topic(),
                            broker_name,
                            message.queue_id(),
                            msg_queue_offset,
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
                        if let Some(ct) = count.filter(|ct| **ct > 0) {
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

    pub(super) fn process_pop_lite_response(
        &self,
        broker_name: &CheetahString,
        mut response: RemotingCommand,
        topic: &CheetahString,
    ) -> rocketmq_error::RocketMQResult<PopResult> {
        let response_code = ResponseCode::from(response.code());
        let (pop_status, msg_found_list) = match response_code {
            ResponseCode::Success => {
                let raw_response_code = response.code();
                let body = response
                    .get_body_mut()
                    .ok_or_else(|| client_broker_err!(raw_response_code, "PopLiteMessage response body is empty"))?;
                let messages = MessageDecoder::decodes_batch(
                    body,
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
        let response_header = response.decode_command_custom_header::<PopLiteMessageResponseHeader>()?;
        if pop_result.pop_status != PopStatus::Found {
            return Ok(pop_result);
        }

        let Some(messages) = pop_result.msg_found_list.as_mut() else {
            return Ok(pop_result);
        };
        let order_count_list =
            parse_lite_order_count_info_like_java(response_header.order_count_info.as_ref(), messages.len());
        for (index, message) in messages.iter_mut().enumerate() {
            let dispatch = message
                .property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_INNER_MULTI_DISPATCH,
                ))
                .unwrap_or_default();
            let queue_offsets = message
                .property(&CheetahString::from_static_str(
                    MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET,
                ))
                .unwrap_or_default();
            let queues = split_lite_dispatch_value(dispatch.as_str());
            let offsets = split_lite_dispatch_value(queue_offsets.as_str());
            if queues.len() != 1 || offsets.len() != 1 {
                continue;
            }
            let Ok(queue_offset) = offsets[0].parse::<i64>() else {
                continue;
            };
            let extra_info = ExtraInfoUtil::build_extra_info_with_offset(
                0,
                response_header.pop_time,
                response_header.invisible_time,
                response_header.revive_qid,
                topic,
                broker_name,
                0,
                queue_offset,
            );
            message.put_property(
                CheetahString::from_static_str(MessageConst::PROPERTY_POP_CK),
                CheetahString::from_string(extra_info),
            );
            if message
                .property(&CheetahString::from_static_str(MessageConst::PROPERTY_FIRST_POP_TIME))
                .is_none()
            {
                message.put_property(
                    CheetahString::from_static_str(MessageConst::PROPERTY_FIRST_POP_TIME),
                    CheetahString::from_string(response_header.pop_time.to_string()),
                );
            }
            message.broker_name = broker_name.clone();
            message.set_reconsume_times(
                order_count_list
                    .as_ref()
                    .and_then(|counts| counts.get(index))
                    .copied()
                    .unwrap_or_default(),
            );
            message.set_queue_offset(queue_offset);
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

    pub async fn ack_lite_message_async(
        &self,
        addr: &CheetahString,
        request_header: AckMessageRequestHeader,
        timeout_millis: u64,
        ack_callback: impl AckCallback,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.ack_message_async(addr, request_header, timeout_millis, ack_callback)
            .await
    }

    pub async fn batch_ack_message_async(
        &self,
        addr: &CheetahString,
        request_body: BatchAckMessageRequestBody,
        timeout_millis: u64,
        ack_callback: impl AckCallback,
    ) -> rocketmq_error::RocketMQResult<()> {
        self.ack_message_async_inner(addr, None, Some(request_body), timeout_millis, ack_callback)
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
            let body =
                request_body.ok_or_else(|| mq_client_err!("BatchAckMessage request body is required".to_string()))?;
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
                ack_callback.on_exception(e);
            }
        }
        Ok(())
    }
}
