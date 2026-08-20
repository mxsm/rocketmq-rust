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

use super::*;
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

        let request = self.create_request_command(RequestCode::GetMaxOffset, request_header);

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

        let request = self.create_request_command(RequestCode::GetMinOffset, request_header);

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

        let request = self.create_request_command(RequestCode::GetEarliestMsgStoreTime, request_header);

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
        let request = self.create_request_command(RequestCode::SearchOffsetByTimestamp, request_header);
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
        let request = self
            .create_remoting_command(RequestCode::SetMessageRequestMode)
            .set_body(body.encode()?);
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
        let request = self.create_request(RequestCode::QueryAssignment, request_body.encode()?);
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
        let request = self.create_request_command(RequestCode::ChangeMessageInvisibleTime, request_header);
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
                let _ = self
                    .callback_executor
                    .execute(async { ack_callback.on_success(ack_result) })
                    .await;
            }
            Err(e) => {
                let _ = self
                    .callback_executor
                    .execute(async { ack_callback.on_exception(e) })
                    .await;
            }
        };
        Ok(())
    }

    pub async fn pop_message_async<PC>(
        self: Arc<Self>,
        broker_name: &CheetahString,
        addr: &CheetahString,
        request_header: PopMessageRequestHeader,
        timeout_millis: u64,
        pop_callback: PC,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        PC: PopCallback + 'static,
    {
        let service_context = self.service_context.clone();
        let tracker = self.background_tasks.clone();
        let shutdown_token = self.background_shutdown.clone();
        let broker_name = broker_name.clone();
        let addr = addr.clone();
        let topic = request_header.topic.clone();
        let order = request_header.order.unwrap_or_default();
        let callback_executor = self.callback_executor.clone();
        let request = self.create_request_command(RequestCode::PopMessage, request_header);
        let request_task = async move {
            let response = self
                .remoting_client
                .invoke_request(Some(&addr), request, timeout_millis)
                .await?;
            self.process_pop_response(&broker_name, response, &topic, order)
        };
        Self::spawn_pop_callback_task(
            &service_context,
            &tracker,
            &shutdown_token,
            callback_executor,
            request_task,
            pop_callback,
        );
        Ok(())
    }

    pub async fn pop_lite_message_async<PC>(
        self: Arc<Self>,
        broker_name: &CheetahString,
        addr: &CheetahString,
        request_header: PopLiteMessageRequestHeader,
        timeout_millis: u64,
        pop_callback: PC,
    ) -> rocketmq_error::RocketMQResult<()>
    where
        PC: PopCallback + 'static,
    {
        let service_context = self.service_context.clone();
        let tracker = self.background_tasks.clone();
        let shutdown_token = self.background_shutdown.clone();
        let broker_name = broker_name.clone();
        let addr = addr.clone();
        let bind_topic = request_header.topic.clone();
        let callback_executor = self.callback_executor.clone();
        let request = self.create_request_command(RequestCode::PopLiteMessage, request_header);
        let request_task = async move {
            let response = self
                .remoting_client
                .invoke_request(Some(&addr), request, timeout_millis)
                .await?;
            self.process_pop_lite_response(&broker_name, response, &bind_topic)
        };
        Self::spawn_pop_callback_task(
            &service_context,
            &tracker,
            &shutdown_token,
            callback_executor,
            request_task,
            pop_callback,
        );
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
                        let queues: Vec<&str> = dispatch.split_str(mix_all::MULTI_DISPATCH_QUEUE_SPLITTER).collect();
                        let data = message
                            .property(&CheetahString::from_static_str(
                                MessageConst::PROPERTY_INNER_MULTI_QUEUE_OFFSET,
                            ))
                            .unwrap_or_default();
                        let queue_offsets: Vec<&str> = data.split_str(mix_all::MULTI_DISPATCH_QUEUE_SPLITTER).collect();
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

    pub async fn ack_message(
        &self,
        addr: &CheetahString,
        request_header: AckMessageRequestHeader,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<AckResult> {
        self.ack_message_inner(addr, Some(request_header), None, timeout_millis)
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

    pub async fn batch_ack_message(
        &self,
        addr: &CheetahString,
        request_body: BatchAckMessageRequestBody,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<AckResult> {
        self.ack_message_inner(addr, None, Some(request_body), timeout_millis)
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
        match self
            .ack_message_inner(addr, request_header, request_body, timeout_millis)
            .await
        {
            Ok(ack_result) => {
                let _ = self
                    .callback_executor
                    .execute(async { ack_callback.on_success(ack_result) })
                    .await;
                Ok(())
            }
            Err(error) => {
                let propagated = mq_client_err!(error.to_string());
                let _ = self
                    .callback_executor
                    .execute(async { ack_callback.on_exception(error) })
                    .await;
                Err(propagated)
            }
        }
    }

    async fn ack_message_inner(
        &self,
        addr: &CheetahString,
        request_header: Option<AckMessageRequestHeader>,
        request_body: Option<BatchAckMessageRequestBody>,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<AckResult> {
        let request = if let Some(header) = request_header {
            self.create_request_command(RequestCode::AckMessage, header)
        } else {
            let body =
                request_body.ok_or_else(|| mq_client_err!("BatchAckMessage request body is required".to_string()))?;
            self.create_request(RequestCode::BatchAckMessage, body.encode()?)
        };
        let response = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await?;
        let response_code = ResponseCode::from(response.code());
        Ok(if response_code == ResponseCode::Success {
            AckResult {
                status: AckStatus::Ok,
                ..Default::default()
            }
        } else {
            AckResult {
                status: AckStatus::NotExist,
                ..Default::default()
            }
        })
    }
}
