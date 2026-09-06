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
use super::callback_executor::ClientCallbackExecutor;
use super::request_builder::notification_request;
use super::response_decoder::notify_result_from_response;
use super::*;
use rocketmq_error::fields;
use rocketmq_error::Error;
use rocketmq_error::ErrorContext;
use rocketmq_transport::api::OutboundRequestContractReason;
use rocketmq_transport::api::OutboundRequestRejectionReason;
use rocketmq_transport::api::OutboundRequestStage;

fn pop_background_task_cancelled(actual: impl Into<String>) -> RocketMQError {
    RocketMQError::ClientInvalidState {
        expected: "active client POP request",
        actual: actual.into(),
    }
}

fn consumer_request_error(operation: &'static str, input: RetryInput) -> RocketMQError {
    match input {
        RetryInput::Transport(error) => RocketMQError::Shared(error.into_shared_error()),
        RetryInput::Rejected(rejection) => match rejection.reason() {
            OutboundRequestRejectionReason::DeadlineExpired => {
                let mut context = ErrorContext::new().with_text(fields::OPERATION_DIAGNOSTIC, operation);
                if let Some(timeout_millis) = rejection.timeout_millis() {
                    context = context.with_u64(fields::TIMEOUT_MS, timeout_millis);
                }
                RocketMQError::Shared(Arc::new(
                    Error::new(&rocketmq_error::CORE_OPERATION_TIMED_OUT).with_context(context),
                ))
            }
            OutboundRequestRejectionReason::ClientStopping => RocketMQError::ClientNotStarted,
            OutboundRequestRejectionReason::QueueSaturated => RocketMQError::Shared(Arc::new(Error::new(
                &rocketmq_error::TRANSPORT_ADMISSION_QUEUE_SATURATED,
            ))),
            OutboundRequestRejectionReason::Cancelled
            | OutboundRequestRejectionReason::SessionClosed
            | OutboundRequestRejectionReason::EndpointUnavailable => {
                let phase = match rejection.reason() {
                    OutboundRequestRejectionReason::SessionClosed => "closed",
                    _ => consumer_request_stage_phase(rejection.stage()),
                };
                let mut context = ErrorContext::new().with_text(fields::PHASE, phase);
                if rejection.remote_addr_present() {
                    context = context.with_secret_presence(fields::REMOTE_ADDR_PRESENT);
                }
                RocketMQError::Shared(Arc::new(
                    Error::new(&rocketmq_error::TRANSPORT_CONNECTION_FAILED).with_context(context),
                ))
            }
        },
        RetryInput::Contract(contract) => match contract.reason() {
            OutboundRequestContractReason::NameServerEndpointMissing => {
                RocketMQError::Shared(Arc::new(Error::new(&rocketmq_error::TRANSPORT_CONNECTION_FAILED)))
            }
        },
        RetryInput::Response { terminal_error, .. } | RetryInput::BusinessError(terminal_error) => terminal_error,
        // Consumer request methods never construct producer status or route-policy inputs.
        RetryInput::SendStatus(_) | RetryInput::RouteUnavailable => {
            unreachable!("consumer request returned an unrelated retry input")
        }
    }
}

const fn consumer_request_stage_phase(stage: OutboundRequestStage) -> &'static str {
    match stage {
        OutboundRequestStage::BeforeWrite => "before_write",
        OutboundRequestStage::Writing => "writing",
        OutboundRequestStage::AwaitingResponse => "awaiting_response",
        OutboundRequestStage::ResponseReceived => "response_received",
    }
}

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
    pub(super) fn spawn_pop_callback_task<PC, F>(
        service_context: &ChildServiceContext,
        tracker: &TaskTracker,
        shutdown_token: &CancellationToken,
        callback_executor: ClientCallbackExecutor,
        request: F,
        pop_callback: PC,
    ) where
        PC: PopCallback + 'static,
        F: Future<Output = rocketmq_error::RocketMQResult<PopResult>> + Send + 'static,
    {
        let callback = Arc::new(std::sync::Mutex::new(Some(pop_callback)));
        if shutdown_token.is_cancelled() {
            if let Some(mut callback) = callback
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
            {
                callback.on_error(pop_background_task_cancelled("client API is shutting down"));
            }
            return;
        }

        let task_callback = callback.clone();
        let shutdown_token = shutdown_token.clone();
        let tracked_task = tracker.track_future(async move {
            let outcome = tokio::select! {
                biased;
                _ = shutdown_token.cancelled() => {
                    Err(pop_background_task_cancelled("client API shutdown cancelled the POP request"))
                }
                outcome = request => outcome,
            };
            let callback = task_callback
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            let Some(mut callback) = callback else {
                return;
            };
            let _ = callback_executor
                .execute(async move {
                    match outcome {
                        Ok(pop_result) => callback.on_success(pop_result).await,
                        Err(error) => callback.on_error(error),
                    }
                })
                .await;
        });

        if let Err(error) = spawn_client_task_with_context(
            service_context,
            "rocketmq-client-pop-message-async",
            Box::pin(tracked_task),
        ) {
            if let Some(mut callback) = callback
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
            {
                callback.on_error(pop_background_task_cancelled(format!(
                    "failed to spawn client POP task: {error}"
                )));
            }
        }
    }

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
        let request = notification_request(&self.command_factory, request_header);
        let outcome = self
            .remoting_client
            .invoke_request(Some(broker_addr), request, timeout_millis)
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(consumer_request_error(
                    "notification_with_polling_stats",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(consumer_request_error(
                    "notification_with_polling_stats",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };

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
        let request = self.create_request_command(RequestCode::GetConsumerListByGroup, request_header);
        let outcome = self
            .remoting_client
            .invoke_request(
                Some(mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr).as_ref()),
                request,
                timeout_millis,
            )
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(consumer_request_error(
                    "get_consumer_id_list_by_group",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(consumer_request_error(
                    "get_consumer_id_list_by_group",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
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
        let request = self.create_request_command(RequestCode::GetConsumerConnectionList, request_header);
        let outcome = self
            .remoting_client
            .invoke_request(
                Some(mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr).as_ref()),
                request,
                timeout_millis,
            )
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(consumer_request_error(
                    "get_consumer_connection_list",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(consumer_request_error(
                    "get_consumer_connection_list",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
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
        let request = self.create_request_command(RequestCode::GetProducerConnectionList, request_header);
        let outcome = self
            .remoting_client
            .invoke_request(
                Some(mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr).as_ref()),
                request,
                timeout_millis,
            )
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(consumer_request_error(
                    "get_producer_connection_list",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(consumer_request_error(
                    "get_producer_connection_list",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
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
            topic_request_header: None,
        };
        let request = self.create_request_command(RequestCode::InvokeBrokerToGetConsumerStatus, request_header);
        let outcome = self
            .remoting_client
            .invoke_request(
                Some(mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr).as_ref()),
                request,
                timeout_millis,
            )
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(consumer_request_error(
                    "invoke_broker_to_get_consumer_status",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(consumer_request_error(
                    "invoke_broker_to_get_consumer_status",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
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
        let request = self.create_request_command(RequestCode::GetAllProducerInfo, EmptyHeader {});
        let outcome = self
            .remoting_client
            .invoke_request(
                Some(mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr).as_ref()),
                request,
                timeout_millis,
            )
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(consumer_request_error(
                    "get_all_producer_info",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(consumer_request_error(
                    "get_all_producer_info",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
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
        let request = self.create_request_command(RequestCode::UpdateConsumerOffset, request_header);
        self.remoting_client
            .invoke_request_oneway(
                mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr).as_ref(),
                request,
                timeout_millis,
            )
            .await
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
        let request = self.create_request_command(RequestCode::UpdateConsumerOffset, request_header);
        let outcome = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(consumer_request_error(
                    "update_consumer_offset",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(consumer_request_error(
                    "update_consumer_offset",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
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
        let request = self.create_request_command(RequestCode::QueryConsumerOffset, request_header);
        let outcome = self
            .remoting_client
            .invoke_request(
                Some(mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr).as_ref()),
                request,
                timeout_millis,
            )
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(consumer_request_error(
                    "query_consumer_offset",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(consumer_request_error(
                    "query_consumer_offset",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
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
        let mut request = this.create_request_command(RequestCode::QueryMessage, request_header);
        if unique_key_flag {
            request.ensure_ext_fields_initialized();
            request.add_ext_field(mix_all::UNIQUE_MSG_QUERY_FLAG, "true");
        }
        let outcome = this
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(consumer_request_error("query_message", RetryInput::Rejected(rejection)));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(consumer_request_error("query_message", RetryInput::Contract(contract)));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
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
            this.create_request_command(RequestCode::LitePullMessage, request_header)
        } else {
            this.create_request_command(RequestCode::PullMessage, request_header)
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
        let outcome = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(consumer_request_error(
                    "pull_message_sync",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(consumer_request_error(
                    "pull_message_sync",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
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
            Ok(OutboundRequestOutcome::Response(response)) => {
                let result = self.process_pull_response(response, addr).await;
                let _ = self
                    .callback_executor
                    .execute(async {
                        match result {
                            Ok(pull_result) => pull_callback.on_success(pull_result).await,
                            Err(error) => pull_callback.on_exception(error),
                        }
                    })
                    .await;
            }
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                let error = consumer_request_error("pull_message_async", RetryInput::Rejected(rejection));
                let _ = self
                    .callback_executor
                    .execute(async { pull_callback.on_exception(error) })
                    .await;
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                let error = consumer_request_error("pull_message_async", RetryInput::Contract(contract));
                let _ = self
                    .callback_executor
                    .execute(async { pull_callback.on_exception(error) })
                    .await;
            }
            Err(error) => {
                let error = RocketMQError::Shared(error.into_shared_error());
                let _ = self
                    .callback_executor
                    .execute(async { pull_callback.on_exception(error) })
                    .await;
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

        let request_command = self.create_request_command(RequestCode::ConsumerSendMsgBack, header);
        let outcome = self
            .remoting_client
            .invoke_request(
                Some(&mix_all::broker_vip_channel(
                    self.client_config.vip_channel_enabled,
                    addr,
                )),
                request_command,
                timeout_millis,
            )
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(consumer_request_error(
                    "consumer_send_message_back",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(consumer_request_error(
                    "consumer_send_message_back",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
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
        let request = self.create_request_command(RequestCode::ConsumerSendMsgBack, request_header);
        let outcome = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await;
        match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => Ok(response),
            Ok(OutboundRequestOutcome::Rejected(rejection)) => Err(consumer_request_error(
                "send_message_back_async",
                RetryInput::Rejected(rejection),
            )),
            Ok(OutboundRequestOutcome::Contract(contract)) => Err(consumer_request_error(
                "send_message_back_async",
                RetryInput::Contract(contract),
            )),
            Err(error) => Err(RocketMQError::Shared(error.into_shared_error())),
        }
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
        let request = self.create_request_command(RequestCode::UnregisterClient, request_header);
        let outcome = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(consumer_request_error(
                    "unregister_client",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(consumer_request_error(
                    "unregister_client",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
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
            self.create_request_command(RequestCode::UnlockBatchMq, UnlockBatchMqRequestHeader::default());
        request.set_body_mut_ref(request_body.encode()?);
        if oneway {
            self.remoting_client
                .invoke_request_oneway(addr, request, timeout_millis)
                .await
        } else {
            let outcome = self
                .remoting_client
                .invoke_request(
                    Some(&mix_all::broker_vip_channel(
                        self.client_config.vip_channel_enabled,
                        addr,
                    )),
                    request,
                    timeout_millis,
                )
                .await;
            let response = match outcome {
                Ok(OutboundRequestOutcome::Response(response)) => response,
                Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                    return Err(consumer_request_error(
                        "unlock_batch_mq",
                        RetryInput::Rejected(rejection),
                    ));
                }
                Ok(OutboundRequestOutcome::Contract(contract)) => {
                    return Err(consumer_request_error(
                        "unlock_batch_mq",
                        RetryInput::Contract(contract),
                    ));
                }
                Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
            };
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
        let mut request = self.create_request_command(RequestCode::LockBatchMq, LockBatchMqRequestHeader::default());
        request.set_body_mut_ref(request_body.encode()?);
        let outcome = self
            .remoting_client
            .invoke_request(
                Some(&mix_all::broker_vip_channel(
                    self.client_config.vip_channel_enabled,
                    addr,
                )),
                request,
                timeout_millis,
            )
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(consumer_request_error("lock_batch_mq", RetryInput::Rejected(rejection)));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(consumer_request_error("lock_batch_mq", RetryInput::Contract(contract)));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
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

mod message_operations;
