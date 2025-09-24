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
use std::any::Any;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_error::RocketmqError::RpcError;
use rocketmq_rust::ArcMut;

use crate::clients::rocketmq_tokio_client::RocketmqDefaultClient;
use crate::clients::RemotingClient;
use crate::code::request_code::RequestCode;
use crate::code::response_code::ResponseCode;
use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::header::get_earliest_msg_storetime_response_header::GetEarliestMsgStoretimeResponseHeader;
use crate::protocol::header::get_max_offset_response_header::GetMaxOffsetResponseHeader;
use crate::protocol::header::get_min_offset_response_header::GetMinOffsetResponseHeader;
use crate::protocol::header::message_operation_header::TopicRequestHeaderTrait;
use crate::protocol::header::pull_message_response_header::PullMessageResponseHeader;
use crate::protocol::header::query_consumer_offset_response_header::QueryConsumerOffsetResponseHeader;
use crate::protocol::header::search_offset_response_header::SearchOffsetResponseHeader;
use crate::protocol::header::update_consumer_offset_header::UpdateConsumerOffsetResponseHeader;
use crate::request_processor::default_request_processor::DefaultRemotingRequestProcessor;
use crate::rpc::client_metadata::ClientMetadata;
use crate::rpc::rpc_client::RpcClient;
use crate::rpc::rpc_client_hook::RpcClientHookFn;
use crate::rpc::rpc_client_utils::RpcClientUtils;
use crate::rpc::rpc_request::RpcRequest;
use crate::rpc::rpc_response::RpcResponse;

pub struct RpcClientImpl {
    client_metadata: ClientMetadata,
    remoting_client: ArcMut<RocketmqDefaultClient<DefaultRemotingRequestProcessor>>,
    client_hook_list: Vec<RpcClientHookFn>,
}

impl RpcClientImpl {
    pub fn new(
        client_metadata: ClientMetadata,
        remoting_client: ArcMut<RocketmqDefaultClient<DefaultRemotingRequestProcessor>>,
    ) -> Self {
        RpcClientImpl {
            client_metadata,
            remoting_client,
            client_hook_list: Vec::new(),
        }
    }

    pub fn register_client_hook(&mut self, client_hook: RpcClientHookFn) {
        self.client_hook_list.push(client_hook);
    }

    pub fn clear_client_hook(&mut self) {
        self.client_hook_list.clear();
    }

    fn get_broker_addr_by_name_or_exception(
        &self,
        broker_name: &str,
    ) -> rocketmq_error::RocketMQResult<CheetahString> {
        match self.client_metadata.find_master_broker_addr(broker_name) {
            None => Err(RpcError(
                From::from(ResponseCode::SystemError),
                format!("cannot find addr for broker {broker_name}"),
            )),
            Some(value) => Ok(value),
        }
    }

    async fn handle_pull_message<H: CommandCustomHeader + TopicRequestHeaderTrait>(
        &self,
        addr: &CheetahString,
        request: RpcRequest<H>,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<RpcResponse> {
        let request_command = RpcClientUtils::create_command_for_rpc_request(request);
        match self
            .remoting_client
            .invoke_request(Some(addr), request_command, timeout_millis)
            .await
        {
            Ok(response) => match ResponseCode::from(response.code()) {
                ResponseCode::Success
                | ResponseCode::PullNotFound
                | ResponseCode::PullRetryImmediately
                | ResponseCode::PullOffsetMoved => {
                    let response_header =
                        response.decode_command_custom_header::<PullMessageResponseHeader>()?;
                    let body = response
                        .body()
                        .map(|value| Box::new(value.clone()) as Box<dyn Any>);
                    let rpc_response =
                        RpcResponse::new(response.code(), Box::new(response_header), body);
                    Ok(rpc_response)
                }
                _ => Ok(RpcResponse::new_exception(Some(RpcError(
                    response.code(),
                    "unexpected remote response code".to_string(),
                )))),
            },
            Err(_error) => Err(RpcError(
                From::from(ResponseCode::SystemError),
                format!("process failed. addr: {addr}. Request"),
            )),
        }
    }

    async fn handle_get_min_offset<H: CommandCustomHeader + TopicRequestHeaderTrait>(
        &self,
        addr: &CheetahString,
        request: RpcRequest<H>,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<RpcResponse> {
        let request_command = RpcClientUtils::create_command_for_rpc_request(request);
        match self
            .remoting_client
            .invoke_request(Some(addr), request_command, timeout_millis)
            .await
        {
            Ok(response) => match ResponseCode::from(response.code()) {
                ResponseCode::Success => {
                    let response_header =
                        response.decode_command_custom_header::<GetMinOffsetResponseHeader>()?;
                    let body = response
                        .body()
                        .map(|value| Box::new(value.clone()) as Box<dyn Any>);
                    let rpc_response =
                        RpcResponse::new(response.code(), Box::new(response_header), body);
                    Ok(rpc_response)
                }
                _ => Ok(RpcResponse::new_exception(Some(RpcError(
                    response.code(),
                    "unknown remote error".to_string(),
                )))),
            },
            Err(_error) => Err(RpcError(
                From::from(ResponseCode::SystemError),
                format!("process failed. addr: {addr}. Request"),
            )),
        }
    }
    async fn handle_get_max_offset<H: CommandCustomHeader + TopicRequestHeaderTrait>(
        &self,
        addr: &CheetahString,
        request: RpcRequest<H>,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<RpcResponse> {
        let request_command = RpcClientUtils::create_command_for_rpc_request(request);
        match self
            .remoting_client
            .invoke_request(Some(addr), request_command, timeout_millis)
            .await
        {
            Ok(response) => match ResponseCode::from(response.code()) {
                ResponseCode::Success => {
                    let response_header =
                        response.decode_command_custom_header::<GetMaxOffsetResponseHeader>()?;
                    let body = response
                        .body()
                        .map(|value| Box::new(value.clone()) as Box<dyn Any>);
                    let rpc_response =
                        RpcResponse::new(response.code(), Box::new(response_header), body);
                    Ok(rpc_response)
                }
                _ => Ok(RpcResponse::new_exception(Some(RpcError(
                    response.code(),
                    "unknown remote error".to_string(),
                )))),
            },
            Err(_error) => Err(RpcError(
                From::from(ResponseCode::SystemError),
                format!("process failed. addr: {addr}. Request"),
            )),
        }
    }
    async fn handle_search_offset<H: CommandCustomHeader + TopicRequestHeaderTrait>(
        &self,
        addr: &CheetahString,
        request: RpcRequest<H>,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<RpcResponse> {
        let request_command = RpcClientUtils::create_command_for_rpc_request(request);
        match self
            .remoting_client
            .invoke_request(Some(addr), request_command, timeout_millis)
            .await
        {
            Ok(response) => match ResponseCode::from(response.code()) {
                ResponseCode::Success => {
                    let response_header =
                        response.decode_command_custom_header::<SearchOffsetResponseHeader>()?;
                    let body = response
                        .body()
                        .map(|value| Box::new(value.clone()) as Box<dyn Any>);
                    let rpc_response =
                        RpcResponse::new(response.code(), Box::new(response_header), body);
                    Ok(rpc_response)
                }
                _ => Ok(RpcResponse::new_exception(Some(RpcError(
                    response.code(),
                    "unknown remote error".to_string(),
                )))),
            },
            Err(_error) => Err(RpcError(
                From::from(ResponseCode::SystemError),
                format!("process failed. addr: {addr}. Request"),
            )),
        }
    }
    async fn handle_get_earliest_msg_storetime<H: CommandCustomHeader + TopicRequestHeaderTrait>(
        &self,
        addr: &CheetahString,
        request: RpcRequest<H>,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<RpcResponse> {
        let request_command = RpcClientUtils::create_command_for_rpc_request(request);
        match self
            .remoting_client
            .invoke_request(Some(addr), request_command, timeout_millis)
            .await
        {
            Ok(response) => match ResponseCode::from(response.code()) {
                ResponseCode::Success => {
                    let response_header = response
                        .decode_command_custom_header::<GetEarliestMsgStoretimeResponseHeader>()?;
                    let body = response
                        .body()
                        .map(|value| Box::new(value.clone()) as Box<dyn Any>);
                    let rpc_response =
                        RpcResponse::new(response.code(), Box::new(response_header), body);
                    Ok(rpc_response)
                }
                _ => Ok(RpcResponse::new_exception(Some(RpcError(
                    response.code(),
                    "unknown remote error".to_string(),
                )))),
            },
            Err(_error) => Err(RpcError(
                From::from(ResponseCode::SystemError),
                format!("process failed. addr: {addr}. Request"),
            )),
        }
    }
    async fn handle_query_consumer_offset<H: CommandCustomHeader + TopicRequestHeaderTrait>(
        &self,
        addr: &CheetahString,
        request: RpcRequest<H>,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<RpcResponse> {
        let request_command = RpcClientUtils::create_command_for_rpc_request(request);
        match self
            .remoting_client
            .invoke_request(Some(addr), request_command, timeout_millis)
            .await
        {
            Ok(response) => match ResponseCode::from(response.code()) {
                ResponseCode::Success => {
                    let response_header = response
                        .decode_command_custom_header::<QueryConsumerOffsetResponseHeader>()?;
                    let body = response
                        .body()
                        .map(|value| Box::new(value.clone()) as Box<dyn Any>);
                    let rpc_response =
                        RpcResponse::new(response.code(), Box::new(response_header), body);
                    Ok(rpc_response)
                }
                ResponseCode::QueryNotFound => {
                    let rpc_response = RpcResponse::new_option(response.code(), None);
                    Ok(rpc_response)
                }
                _ => Ok(RpcResponse::new_exception(Some(RpcError(
                    response.code(),
                    "unknown remote error".to_string(),
                )))),
            },
            Err(_error) => Err(RpcError(
                From::from(ResponseCode::SystemError),
                format!("process failed. addr: {addr}. Request"),
            )),
        }
    }
    async fn handle_update_consumer_offset<H: CommandCustomHeader + TopicRequestHeaderTrait>(
        &self,
        addr: &CheetahString,
        request: RpcRequest<H>,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<RpcResponse> {
        let request_command = RpcClientUtils::create_command_for_rpc_request(request);
        match self
            .remoting_client
            .invoke_request(Some(addr), request_command, timeout_millis)
            .await
        {
            Ok(response) => match ResponseCode::from(response.code()) {
                ResponseCode::Success => {
                    let response_header = response
                        .decode_command_custom_header::<UpdateConsumerOffsetResponseHeader>()?;
                    let body = response
                        .body()
                        .map(|value| Box::new(value.clone()) as Box<dyn Any>);
                    let rpc_response =
                        RpcResponse::new(response.code(), Box::new(response_header), body);
                    Ok(rpc_response)
                }
                _ => Ok(RpcResponse::new_exception(Some(RpcError(
                    response.code(),
                    "unknown remote error".to_string(),
                )))),
            },
            Err(_error) => Err(RpcError(
                From::from(ResponseCode::SystemError),
                format!("process failed. addr: {addr}. Request"),
            )),
        }
    }
    async fn handle_common_body_request<H: CommandCustomHeader + TopicRequestHeaderTrait>(
        &self,
        addr: &CheetahString,
        request: RpcRequest<H>,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<RpcResponse> {
        let request_command = RpcClientUtils::create_command_for_rpc_request(request);
        match self
            .remoting_client
            .invoke_request(Some(addr), request_command, timeout_millis)
            .await
        {
            Ok(response) => match ResponseCode::from(response.code()) {
                ResponseCode::Success => {
                    let body = response
                        .body()
                        .map(|value| Box::new(value.clone()) as Box<dyn Any>);
                    let rpc_response = RpcResponse::new_option(response.code(), body);
                    Ok(rpc_response)
                }
                _ => Ok(RpcResponse::new_exception(Some(RpcError(
                    response.code(),
                    "unknown remote error".to_string(),
                )))),
            },
            Err(_error) => Err(RpcError(
                From::from(ResponseCode::SystemError),
                format!("process failed. addr: {addr}. Request"),
            )),
        }
    }
}

impl RpcClient for RpcClientImpl {
    async fn invoke<H: CommandCustomHeader + TopicRequestHeaderTrait>(
        &self,
        request: RpcRequest<H>,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<RpcResponse> {
        if !self.client_hook_list.is_empty() {
            for hook in self.client_hook_list.iter() {
                // let result = hook.before_request(&request)?;
                let result = hook(Some(&request.header), None)?;
                if let Some(result) = result {
                    return Ok(result);
                }
            }
        }
        let bname = request
            .header
            .broker_name()
            .expect("broker name is required");
        let addr = self.get_broker_addr_by_name_or_exception(bname.as_ref())?;
        let result = match RequestCode::from(request.code) {
            RequestCode::PullMessage => {
                self.handle_pull_message(&addr, request, timeout_millis)
                    .await?
            }
            RequestCode::GetMinOffset => {
                self.handle_get_min_offset(&addr, request, timeout_millis)
                    .await?
            }
            RequestCode::GetMaxOffset => {
                self.handle_get_max_offset(&addr, request, timeout_millis)
                    .await?
            }
            RequestCode::SearchOffsetByTimestamp => {
                self.handle_search_offset(&addr, request, timeout_millis)
                    .await?
            }
            RequestCode::GetEarliestMsgStoreTime => {
                self.handle_get_earliest_msg_storetime(&addr, request, timeout_millis)
                    .await?
            }
            RequestCode::QueryConsumerOffset => {
                self.handle_query_consumer_offset(&addr, request, timeout_millis)
                    .await?
            }
            RequestCode::UpdateConsumerOffset => {
                self.handle_update_consumer_offset(&addr, request, timeout_millis)
                    .await?
            }
            RequestCode::GetTopicStatsInfo => {
                self.handle_common_body_request(&addr, request, timeout_millis)
                    .await?
            }
            RequestCode::GetTopicConfig => {
                self.handle_common_body_request(&addr, request, timeout_millis)
                    .await?
            }
            _ => {
                return Err(RpcError(
                    From::from(ResponseCode::RequestCodeNotSupported),
                    format!("unknown request code {}", request.code),
                ))
            }
        };
        Ok(result)
    }

    async fn invoke_mq<H: CommandCustomHeader + TopicRequestHeaderTrait>(
        &self,
        mq: MessageQueue,
        mut request: RpcRequest<H>,
        timeout_millis: u64,
    ) -> rocketmq_error::RocketMQResult<RpcResponse> {
        if let Some(broker_name) = self.client_metadata.get_broker_name_from_message_queue(&mq) {
            request.header.set_broker_name(broker_name);
        }
        self.invoke(request, timeout_millis).await
    }
}
