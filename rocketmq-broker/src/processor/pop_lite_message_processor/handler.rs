// Copyright 2026 The RocketMQ Rust Authors
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

use rocketmq_error::RocketMQError;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::pop_lite_message_request_header::PopLiteMessageRequestHeader;
use rocketmq_store::BrokerReadWriteStore;
use rocketmq_transport::api::command_from_error_with_factory_remark_and_opaque;
use rocketmq_transport::api::internal_error_with_factory_and_opaque;
use rocketmq_transport::api::request_code_not_supported_with_factory_remark_and_opaque;
use rocketmq_transport::api::DeferredAdmissionAcquireErrorKind;
use rocketmq_transport::api::HandlerOutcome;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RequestOrigin;
use rocketmq_transport::api::RequestProcessor;
use rocketmq_transport::api::TakeDeferredResponderError;

use super::core::PopLiteCoreResult;
use super::response::PopLiteResponseKind;
use super::PopLiteMessageProcessor;
use crate::long_polling::pop_lite_deferred::index::PopLiteIndexErrorKind;
use crate::long_polling::pop_lite_deferred::prepare::PopLiteDeferredPrepareError;
use crate::long_polling::pop_lite_deferred::prepare::PopLiteDeferredRegisterError;
use crate::long_polling::pop_lite_deferred::prepare::PopLiteRetainedEstimate;
use crate::processor::response_assembly::BrokerResponseParts;

impl<MS> RequestProcessor for PopLiteMessageProcessor<MS>
where
    MS: BrokerReadWriteStore + Send + Sync + 'static,
{
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        self.process_shared(request).await
    }
}

impl<MS> PopLiteMessageProcessor<MS>
where
    MS: BrokerReadWriteStore,
{
    pub(crate) async fn process_shared(
        &self,
        request: &mut RemotingRequest,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        if RequestCode::from(request.original_identity().original_code()) != RequestCode::PopLiteMessage {
            return command_outcome(request_code_not_supported_with_factory_remark_and_opaque(
                &self.context.command_factory,
                request.original_identity().original_code(),
                "PopLiteMessageProcessor request code is not supported",
                request.original_identity().original_opaque(),
            ));
        }
        if request.original_identity().is_one_way() {
            return self.invalid_reply(
                "POP Lite does not support one-way requests",
                request.original_identity().original_opaque(),
            );
        }
        if !matches!(request.origin(), RequestOrigin::Network { .. }) {
            return self.invalid_reply(
                "POP Lite requires a trusted network peer",
                request.original_identity().original_opaque(),
            );
        }
        let request_header = match request
            .command()
            .decode_command_custom_header::<PopLiteMessageRequestHeader>()
        {
            Ok(header) => header,
            Err(_) => {
                return self.invalid_reply(
                    "decode POP Lite request header failed",
                    request.original_identity().original_opaque(),
                );
            }
        };
        if let Some((code, remark)) = self.pre_check(&request_header) {
            return command_outcome(self.response_with_code(request.command(), code, remark));
        }

        let dispatcher = &self.context.lite_event_dispatcher;
        dispatcher.touch_client(&request_header.client_id);
        let result = match dispatcher.reserve_pending_events(&request_header.client_id) {
            Some(reservation) => self.execute_pop_lite_batch(&request_header, reservation.commit()).await,
            None => self.execute_pop_lite_without_events(&request_header).await,
        };
        if result.body.is_some() {
            return Ok(HandlerOutcome::Reply(self.compose_pop_lite_response(
                &request_header,
                result,
                PopLiteResponseKind::Found,
            )?));
        }

        let Some(service) = self.pop_lite_deferred_service.get() else {
            return self.reply_with_code(
                ResponseCode::ServiceNotAvailable,
                "the deferred POP Lite service is not installed",
            );
        };
        let prepared = match service.prepare(request, PopLiteRetainedEstimate::default()) {
            Ok(prepared) => prepared,
            Err(error) => return self.prepare_error_outcome(&request_header, error),
        };
        match service.register(prepared, request) {
            Ok(registration) => Ok(HandlerOutcome::Deferred(registration)),
            Err(error) => self.register_error_outcome(&request_header, error),
        }
    }
}

impl<MS> PopLiteMessageProcessor<MS>
where
    MS: BrokerReadWriteStore,
{
    fn prepare_error_outcome(
        &self,
        request_header: &PopLiteMessageRequestHeader,
        error: PopLiteDeferredPrepareError,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        match error {
            PopLiteDeferredPrepareError::Deadline(_) | PopLiteDeferredPrepareError::OneWay => {
                self.empty_pop_lite_outcome(request_header, PopLiteResponseKind::PollingTimeout)
            }
            PopLiteDeferredPrepareError::Index(error)
                if matches!(
                    error.kind(),
                    PopLiteIndexErrorKind::GlobalCapacity
                        | PopLiteIndexErrorKind::ClientCapacity
                        | PopLiteIndexErrorKind::PerClientCapacity
                ) =>
            {
                self.empty_pop_lite_outcome(request_header, PopLiteResponseKind::PollingFull)
            }
            PopLiteDeferredPrepareError::Admission(error)
                if !matches!(error.kind(), DeferredAdmissionAcquireErrorKind::RetainedSizeOverflow) =>
            {
                self.empty_pop_lite_outcome(request_header, PopLiteResponseKind::PollingFull)
            }
            PopLiteDeferredPrepareError::EmbeddedOrigin
            | PopLiteDeferredPrepareError::Header(_)
            | PopLiteDeferredPrepareError::InvalidHeader => self.invalid_reply("invalid deferred POP Lite request", 0),
            PopLiteDeferredPrepareError::ServiceClosed => self.reply_with_code(
                ResponseCode::ServiceNotAvailable,
                "the deferred POP Lite service is unavailable",
            ),
            PopLiteDeferredPrepareError::InvalidExpiryMargins
            | PopLiteDeferredPrepareError::RetainedSizeOverflow
            | PopLiteDeferredPrepareError::Index(_)
            | PopLiteDeferredPrepareError::Admission(_) => {
                self.internal_reply("the deferred POP Lite request could not be prepared", 0)
            }
        }
    }

    fn register_error_outcome(
        &self,
        request_header: &PopLiteMessageRequestHeader,
        error: PopLiteDeferredRegisterError,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        match error {
            PopLiteDeferredRegisterError::ServiceClosed => self.reply_with_code(
                ResponseCode::ServiceNotAvailable,
                "the deferred POP Lite service is unavailable",
            ),
            PopLiteDeferredRegisterError::Responder(TakeDeferredResponderError::OneWayRequest) => {
                self.empty_pop_lite_outcome(request_header, PopLiteResponseKind::PollingTimeout)
            }
            PopLiteDeferredRegisterError::Responder(TakeDeferredResponderError::Unavailable) => self.reply_with_code(
                ResponseCode::ServiceNotAvailable,
                "a deferred POP Lite responder is unavailable",
            ),
            error => Err(RocketMQError::internal("register deferred POP Lite request", error)),
        }
    }

    fn empty_pop_lite_outcome(
        &self,
        request_header: &PopLiteMessageRequestHeader,
        kind: PopLiteResponseKind,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        Ok(HandlerOutcome::Reply(self.compose_pop_lite_response(
            request_header,
            PopLiteCoreResult {
                body: None,
                fetched_count: 0,
                order_count_info: None,
            },
            kind,
        )?))
    }

    fn reply_with_code(
        &self,
        code: ResponseCode,
        remark: &'static str,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        command_outcome(
            self.context
                .command_factory
                .create_response_command_with_code_remark(code, remark),
        )
    }

    fn invalid_reply(&self, remark: &'static str, opaque: i32) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let error = RocketMQError::illegal_argument(remark);
        command_outcome(command_from_error_with_factory_remark_and_opaque(
            &self.context.command_factory,
            &error,
            remark,
            opaque,
        ))
    }

    fn internal_reply(&self, remark: &'static str, opaque: i32) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        command_outcome(internal_error_with_factory_and_opaque(
            &self.context.command_factory,
            opaque,
            remark,
        ))
    }
}

fn command_outcome(
    command: rocketmq_protocol::protocol::remoting_command::RemotingCommand,
) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
    BrokerResponseParts::command(command)?.into_handler_outcome()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::Duration;

    use cheetah_string::CheetahString;
    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::protocol::header::pop_lite_message_request_header::PopLiteMessageRequestHeader;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_store::MessageStoreConfig;
    use rocketmq_transport::api::AdmissionController;
    use rocketmq_transport::api::AdmissionLimits;
    use rocketmq_transport::api::HandlerOutcome;
    use rocketmq_transport::api::RemotingRequest;
    use rocketmq_transport::api::RequestProcessor;

    use super::super::tests::pop_lite_processor_for_test;
    use super::super::PopLiteMessageProcessor;
    use crate::broker_runtime::BrokerMessageStore;
    use crate::broker_runtime::BrokerRuntime;
    use crate::config::broker_config::BrokerConfig;
    use crate::processor::processor_test_support::start_processor_server;

    #[derive(Clone)]
    struct ArcHeldPopLiteProcessor {
        inner: Arc<PopLiteMessageProcessor<BrokerMessageStore>>,
    }

    impl RequestProcessor for ArcHeldPopLiteProcessor {
        async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
            self.inner.process_shared(request).await
        }
    }

    #[tokio::test]
    async fn pop_lite_one_way_preflight_suppresses_frame_without_consuming_pending_event() {
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
        let processor = pop_lite_processor_for_test(&mut runtime);
        let client_id = CheetahString::from_static_str("one-way-client");
        let group = CheetahString::from_static_str("group-a");
        let event = CheetahString::from_static_str("%LMQ%topic-a%event-a");
        let mut events = HashSet::new();
        events.insert(event.clone());
        assert_eq!(
            processor
                .context
                .lite_event_dispatcher
                .do_full_dispatch(&client_id, &group, &events),
            1
        );

        let (mut client, server) = start_processor_server(
            "pop-lite-one-way",
            ArcHeldPopLiteProcessor {
                inner: Arc::clone(&processor),
            },
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        )
        .await;
        let header = PopLiteMessageRequestHeader {
            client_id: client_id.clone(),
            consumer_group: group,
            topic: CheetahString::from_static_str("topic-a"),
            max_msg_num: 1,
            invisible_time: 30_000,
            poll_time: 60_000,
            born_time: 0,
            attempt_id: None,
            rpc: None,
        };
        let mut request = RemotingCommand::create_request_command(RequestCode::PopLiteMessage, header);
        request.make_custom_header_to_net();
        request.mark_oneway_rpc_ref();
        client
            .send_command(request)
            .await
            .expect("send one-way POP Lite request");

        assert!(
            tokio::time::timeout(Duration::from_millis(300), client.receive_command())
                .await
                .is_err(),
            "canonical one-way handling emits no inline frame"
        );
        assert_eq!(
            processor.context.lite_event_dispatcher.pending_events(&client_id),
            vec![event]
        );

        drop(client);
        server.finish().await;
    }
}
