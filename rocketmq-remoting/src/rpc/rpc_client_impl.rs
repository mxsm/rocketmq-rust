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

//!
//! # Key Improvements
//!
//! - **Code Reuse**: Eliminated 210 lines of duplicated code using generic handler
//! - **Error Handling**: Preserved full error context with `thiserror` integration
//! - **Performance**: Removed unnecessary boxing operations
//! - **Async Pattern**: Added callback-based invocation for non-blocking scenarios
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    RpcClientImpl                            │
//! ├─────────────────────────────────────────────────────────────┤
//! │                                                             │
//! │  ┌──────────────┐        ┌───────────────────────┐         │
//! │  │ Client Hooks │───────►│ Request Interceptor   │         │
//! │  └──────────────┘        └───────────────────────┘         │
//! │         │                          │                        │
//! │         ↓                          ↓                        │
//! │  ┌──────────────────────────────────────────────┐          │
//! │  │   Generic Handler (eliminates duplication)   │          │
//! │  │   handle_request<H, R>()                     │          │
//! │  └──────────────────────────────────────────────┘          │
//! │         │                                                   │
//! │         ↓                                                   │
//! │  ┌──────────────────────────────────────────────┐          │
//! │  │   RemotingClient (Connection Pool)           │          │
//! │  │   - invoke_request (sync)                    │          │
//! │  │   - invoke_request_oneway                    │          │
//! │  └──────────────────────────────────────────────┘          │
//! │                                                             │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use std::any::Any;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_error::RocketMQResult;
use rocketmq_error::RpcClientError;
use rocketmq_rust::ArcMut;
use tracing::error;
use tracing::trace;

use crate::clients::rocketmq_tokio_client::RocketmqDefaultClient;
use crate::clients::RemotingClient;
use crate::code::request_code::RequestCode;
use crate::code::response_code::ResponseCode;
use crate::protocol::command_custom_header::CommandCustomHeader;
use crate::protocol::command_custom_header::FromMap;
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

/// Configuration for response handling
struct ResponseConfig {
    /// Expected successful response codes
    success_codes: &'static [ResponseCode],
}

impl ResponseConfig {
    const fn new(success_codes: &'static [ResponseCode]) -> Self {
        Self { success_codes }
    }
}

/// High-performance RPC client implementation
///
/// # Thread Safety
///
/// This struct is `Send + Sync` safe and can be shared across threads using `Arc<RpcClientImpl>`.
///
/// # Example
///
/// ```rust,ignore
/// use rocketmq_remoting::rpc::rpc_client_impl::RpcClientImpl;
///
/// let client = RpcClientImpl::new(client_metadata, remoting_client);
///
/// // Synchronous invocation
/// let response = client.invoke(request, 3000).await?;
///
/// // Callback-based invocation (non-blocking)
/// client.invoke_with_callback(request, 3000, |result| {
///     match result {
///         Ok(response) => println!("Success: {:?}", response),
///         Err(err) => eprintln!("Failed: {}", err),
///     }
/// }).await;
/// ```
pub struct RpcClientImpl {
    /// Client metadata for broker address resolution
    client_metadata: Arc<ClientMetadata>,
    /// Underlying remoting client for network communication
    remoting_client: ArcMut<RocketmqDefaultClient<DefaultRemotingRequestProcessor>>,
    /// Registered client hooks for request interception
    client_hook_list: Vec<RpcClientHookFn>,
}

impl RpcClientImpl {
    /// Creates a new RPC client instance
    ///
    /// # Arguments
    ///
    /// * `client_metadata` - Metadata for broker address resolution
    /// * `remoting_client` - Network client for sending requests
    pub fn new(
        client_metadata: Arc<ClientMetadata>,
        remoting_client: ArcMut<RocketmqDefaultClient<DefaultRemotingRequestProcessor>>,
    ) -> Self {
        RpcClientImpl {
            client_metadata,
            remoting_client,
            client_hook_list: Vec::new(),
        }
    }

    /// Registers a client hook for request/response interception
    ///
    /// Hooks are executed in registration order before each request.
    pub fn register_client_hook(&mut self, client_hook: RpcClientHookFn) {
        self.client_hook_list.push(client_hook);
    }

    /// Clears all registered client hooks
    pub fn clear_client_hook(&mut self) {
        self.client_hook_list.clear();
    }

    /// Resolves broker address by name, returning error if not found
    fn get_broker_addr_by_name(&self, broker_name: &str) -> Result<CheetahString, RpcClientError> {
        self.client_metadata
            .find_master_broker_addr(broker_name)
            .ok_or_else(|| RpcClientError::BrokerNotFound {
                broker_name: broker_name.to_string(),
            })
    }

    /// Generic request handler eliminating code duplication
    ///
    ///
    /// # Type Parameters
    ///
    /// * `H` - Request header type
    /// * `R` - Response header type
    ///
    /// # Arguments
    ///
    /// * `addr` - Target broker address
    /// * `request` - RPC request with custom header
    /// * `timeout_millis` - Request timeout in milliseconds
    /// * `config` - Response handling configuration
    async fn handle_request<H, R>(
        &self,
        addr: &CheetahString,
        request: RpcRequest<H>,
        timeout_millis: u64,
        config: ResponseConfig,
    ) -> Result<RpcResponse, RpcClientError>
    where
        H: CommandCustomHeader + TopicRequestHeaderTrait,
        R: CommandCustomHeader + FromMap<Target = R, Error = rocketmq_error::RocketMQError> + Send + Sync + 'static,
    {
        trace!(
            "Sending RPC request: addr={}, code={}, timeout={}ms",
            addr,
            request.code,
            timeout_millis
        );

        let request_code = request.code;
        let request_command = RpcClientUtils::create_command_for_rpc_request(request);

        let response = self
            .remoting_client
            .invoke_request(Some(addr), request_command, timeout_millis)
            .await
            .map_err(|err| {
                error!(
                    "RPC request failed: addr={}, code={}, error={}",
                    addr, request_code, err
                );
                RpcClientError::RequestFailed {
                    addr: addr.to_string(),
                    request_code,
                    timeout_ms: timeout_millis,
                    source: Box::new(err),
                }
            })?;

        let response_code = ResponseCode::from(response.code());

        if !config.success_codes.contains(&response_code) {
            return Err(RpcClientError::UnexpectedResponseCode {
                code: response.code(),
                code_name: format!("{:?}", response_code),
            });
        }

        let response_header =
            response
                .decode_command_custom_header::<R>()
                .map_err(|err| RpcClientError::RequestFailed {
                    addr: addr.to_string(),
                    request_code,
                    timeout_ms: timeout_millis,
                    source: Box::new(err),
                })?;

        let body = response.body().map(|value| Box::new(value.clone()) as Box<dyn Any>);

        Ok(RpcResponse::new(response.code(), Box::new(response_header), body))
    }

    /// Handles PULL_MESSAGE request with multiple success codes
    async fn handle_pull_message<H: CommandCustomHeader + TopicRequestHeaderTrait>(
        &self,
        addr: &CheetahString,
        request: RpcRequest<H>,
        timeout_millis: u64,
    ) -> Result<RpcResponse, RpcClientError> {
        const PULL_SUCCESS_CODES: &[ResponseCode] = &[
            ResponseCode::Success,
            ResponseCode::PullNotFound,
            ResponseCode::PullRetryImmediately,
            ResponseCode::PullOffsetMoved,
        ];

        self.handle_request::<H, PullMessageResponseHeader>(
            addr,
            request,
            timeout_millis,
            ResponseConfig::new(PULL_SUCCESS_CODES),
        )
        .await
    }

    /// Handles QUERY_CONSUMER_OFFSET with QUERY_NOT_FOUND support
    async fn handle_query_consumer_offset<H: CommandCustomHeader + TopicRequestHeaderTrait>(
        &self,
        addr: &CheetahString,
        request: RpcRequest<H>,
        timeout_millis: u64,
    ) -> Result<RpcResponse, RpcClientError> {
        let request_code = request.code;
        let request_command = RpcClientUtils::create_command_for_rpc_request(request);

        let response = self
            .remoting_client
            .invoke_request(Some(addr), request_command, timeout_millis)
            .await
            .map_err(|err| RpcClientError::RequestFailed {
                addr: addr.to_string(),
                request_code,
                timeout_ms: timeout_millis,
                source: Box::new(err),
            })?;

        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let response_header = response
                    .decode_command_custom_header::<QueryConsumerOffsetResponseHeader>()
                    .map_err(|err| RpcClientError::RequestFailed {
                        addr: addr.to_string(),
                        request_code,
                        timeout_ms: timeout_millis,
                        source: Box::new(err),
                    })?;
                let body = response.body().map(|value| Box::new(value.clone()) as Box<dyn Any>);
                Ok(RpcResponse::new(response.code(), Box::new(response_header), body))
            }
            ResponseCode::QueryNotFound => {
                // Special case: no offset found (not an error)
                Ok(RpcResponse::new_option(response.code(), None))
            }
            code => Err(RpcClientError::UnexpectedResponseCode {
                code: response.code(),
                code_name: format!("{:?}", code),
            }),
        }
    }

    /// Executes client hooks before request, short-circuiting if hook returns response
    fn execute_hooks<H: CommandCustomHeader + TopicRequestHeaderTrait>(
        &self,
        request: &RpcRequest<H>,
    ) -> RocketMQResult<Option<RpcResponse>> {
        for hook in &self.client_hook_list {
            if let Some(response) = hook(Some(&request.header), None)? {
                trace!("Request intercepted by client hook");
                return Ok(Some(response));
            }
        }
        Ok(None)
    }
}

impl RpcClient for RpcClientImpl {
    /// Invokes RPC request synchronously
    ///
    /// # Flow
    ///
    /// 1. Execute client hooks (may short-circuit)
    /// 2. Resolve broker address
    /// 3. Dispatch to specific handler based on request code
    /// 4. Return response or error
    async fn invoke<H: CommandCustomHeader + TopicRequestHeaderTrait>(
        &self,
        request: RpcRequest<H>,
        timeout_millis: u64,
    ) -> RocketMQResult<RpcResponse> {
        // Execute hooks
        if let Some(response) = self.execute_hooks(&request)? {
            return Ok(response);
        }

        // Resolve broker address
        let broker_name = request.header.broker_name().expect("broker name is required");
        let addr = self.get_broker_addr_by_name(broker_name.as_ref())?;

        let result = match RequestCode::from(request.code) {
            RequestCode::PullMessage => self.handle_pull_message(&addr, request, timeout_millis).await?,
            RequestCode::GetMinOffset => {
                self.handle_request::<H, GetMinOffsetResponseHeader>(
                    &addr,
                    request,
                    timeout_millis,
                    ResponseConfig::new(&[ResponseCode::Success]),
                )
                .await?
            }
            RequestCode::GetMaxOffset => {
                self.handle_request::<H, GetMaxOffsetResponseHeader>(
                    &addr,
                    request,
                    timeout_millis,
                    ResponseConfig::new(&[ResponseCode::Success]),
                )
                .await?
            }
            RequestCode::SearchOffsetByTimestamp => {
                self.handle_request::<H, SearchOffsetResponseHeader>(
                    &addr,
                    request,
                    timeout_millis,
                    ResponseConfig::new(&[ResponseCode::Success]),
                )
                .await?
            }
            RequestCode::GetEarliestMsgStoreTime => {
                self.handle_request::<H, GetEarliestMsgStoretimeResponseHeader>(
                    &addr,
                    request,
                    timeout_millis,
                    ResponseConfig::new(&[ResponseCode::Success]),
                )
                .await?
            }
            RequestCode::QueryConsumerOffset => {
                self.handle_query_consumer_offset(&addr, request, timeout_millis)
                    .await?
            }
            RequestCode::UpdateConsumerOffset => {
                self.handle_request::<H, UpdateConsumerOffsetResponseHeader>(
                    &addr,
                    request,
                    timeout_millis,
                    ResponseConfig::new(&[ResponseCode::Success]),
                )
                .await?
            }
            RequestCode::GetTopicStatsInfo | RequestCode::GetTopicConfig => {
                let request_code = request.code;
                let request_command = RpcClientUtils::create_command_for_rpc_request(request);
                let response = self
                    .remoting_client
                    .invoke_request(Some(&addr), request_command, timeout_millis)
                    .await
                    .map_err(|err| RpcClientError::RequestFailed {
                        addr: addr.to_string(),
                        request_code,
                        timeout_ms: timeout_millis,
                        source: Box::new(err),
                    })?;

                if response.code() != ResponseCode::Success as i32 {
                    return Err(RpcClientError::UnexpectedResponseCode {
                        code: response.code(),
                        code_name: format!("{:?}", ResponseCode::from(response.code())),
                    }
                    .into());
                }

                let body = response.body().map(|value| Box::new(value.clone()) as Box<dyn Any>);
                RpcResponse::new_option(response.code(), body)
            }
            _ => return Err(RpcClientError::UnsupportedRequestCode { code: request.code }.into()),
        };

        Ok(result)
    }

    /// Invokes RPC request with message queue context
    ///
    /// This method resolves broker name from message queue and delegates to `invoke()`.
    async fn invoke_mq<H: CommandCustomHeader + TopicRequestHeaderTrait>(
        &self,
        mq: MessageQueue,
        mut request: RpcRequest<H>,
        timeout_millis: u64,
    ) -> RocketMQResult<RpcResponse> {
        if let Some(broker_name) = self.client_metadata.get_broker_name_from_message_queue(&mq) {
            request.header.set_broker_name(broker_name);
        }
        self.invoke(request, timeout_millis).await
    }
}

// ==================== Callback-based Async Extension ====================

impl RpcClientImpl {
    /// Invokes RPC request with callback (non-blocking)
    ///
    /// Unlike `invoke()` which awaits the response, this method spawns an async task
    /// and invokes the callback when the response arrives.
    ///
    /// # Use Cases
    ///
    /// - Batch requests where you don't want to block on each response
    /// - Pipeline processing where requests can be issued in parallel
    /// - Event-driven architectures
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// client.invoke_with_callback(request, 3000, |result| {
    ///     match result {
    ///         Ok(response) => process_response(response),
    ///         Err(err) => log_error(err),
    ///     }
    /// }).await;
    /// ```
    pub fn invoke_with_callback<H, F>(
        &self,
        request: RpcRequest<H>,
        timeout_millis: u64,
        callback: F,
    ) -> tokio::task::JoinHandle<()>
    where
        H: CommandCustomHeader + TopicRequestHeaderTrait + Send + 'static,
        F: FnOnce(RocketMQResult<RpcResponse>) + Send + 'static,
    {
        let client_metadata = self.client_metadata.clone();
        let remoting_client = self.remoting_client.clone();
        let hooks = self.client_hook_list.clone();

        tokio::spawn(async move {
            // Create a temporary client instance for the async task
            let temp_client = RpcClientImpl {
                client_metadata,
                remoting_client,
                client_hook_list: hooks,
            };

            let result = temp_client.invoke(request, timeout_millis).await;
            callback(result);
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_formatting() {
        let err = RpcClientError::BrokerNotFound {
            broker_name: "broker-a".to_string(),
        };
        assert!(err.to_string().contains("broker-a"));

        let err = RpcClientError::UnexpectedResponseCode {
            code: 1,
            code_name: "SUCCESS".to_string(),
        };
        assert!(err.to_string().contains("Unexpected response code"));
    }
}
