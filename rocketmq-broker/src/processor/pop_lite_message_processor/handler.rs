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

use rocketmq_error::PublicErrorView;
use rocketmq_error::RocketMQError;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::pop_lite_message_request_header::PopLiteMessageRequestHeader;
use rocketmq_store::BrokerReadWriteStore;
use rocketmq_transport::api::error_response as remoting_error_response;
use rocketmq_transport::api::DeferredResponderOutcome;
use rocketmq_transport::api::HandlerOutcome;
use rocketmq_transport::api::RemotingErrorTarget;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RequestOrigin;
use rocketmq_transport::api::RequestProcessor;

use super::core::PopLiteCoreResult;
use super::response::PopLiteResponseKind;
use super::PopLiteMessageProcessor;
use crate::long_polling::pop_lite_deferred::prepare::PopLiteDeferredPrepareFailure;
use crate::long_polling::pop_lite_deferred::prepare::PopLiteDeferredPrepareOutcome;
use crate::long_polling::pop_lite_deferred::prepare::PopLiteDeferredPrepareRejection;
use crate::long_polling::pop_lite_deferred::prepare::PopLiteDeferredRegisterFailure;
use crate::long_polling::pop_lite_deferred::prepare::PopLiteDeferredRegisterOutcome;
use crate::long_polling::pop_lite_deferred::prepare::PopLiteDeferredRegisterRejection;
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
            return command_outcome(remoting_error_response(
                PublicErrorView::descriptor_only(&rocketmq_error::PROTOCOL_REQUEST_UNSUPPORTED),
                RemotingErrorTarget::Reply {
                    factory: &self.context.command_factory,
                    opaque: request.original_identity().original_opaque(),
                },
            ));
        }
        if request.original_identity().is_one_way() {
            return self.invalid_reply(request.original_identity().original_opaque());
        }
        if !matches!(request.origin(), RequestOrigin::Network { .. }) {
            return self.invalid_reply(request.original_identity().original_opaque());
        }
        let request_header = match request
            .command()
            .decode_command_custom_header::<PopLiteMessageRequestHeader>()
        {
            Ok(header) => header,
            Err(_) => {
                return self.invalid_reply(request.original_identity().original_opaque());
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
            Ok(PopLiteDeferredPrepareOutcome::Prepared(prepared)) => *prepared,
            Ok(PopLiteDeferredPrepareOutcome::Rejected(rejection)) => {
                return self.prepare_rejection_outcome(&request_header, rejection);
            }
            Err(failure) => return self.prepare_failure_outcome(&request_header, failure),
        };
        match service.register(prepared, request) {
            Ok(PopLiteDeferredRegisterOutcome::Registered(registration)) => Ok(HandlerOutcome::Deferred(*registration)),
            Ok(PopLiteDeferredRegisterOutcome::Rejected(rejection)) => {
                self.register_rejection_outcome(&request_header, *rejection)
            }
            Err(failure) => self.register_failure_outcome(failure),
        }
    }
}

impl<MS> PopLiteMessageProcessor<MS>
where
    MS: BrokerReadWriteStore,
{
    fn prepare_rejection_outcome(
        &self,
        request_header: &PopLiteMessageRequestHeader,
        rejection: PopLiteDeferredPrepareRejection,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        match rejection {
            PopLiteDeferredPrepareRejection::Deadline(_) | PopLiteDeferredPrepareRejection::OneWay => {
                self.empty_pop_lite_outcome(request_header, PopLiteResponseKind::PollingTimeout)
            }
            PopLiteDeferredPrepareRejection::IndexCapacity(_) => {
                self.empty_pop_lite_outcome(request_header, PopLiteResponseKind::PollingFull)
            }
            PopLiteDeferredPrepareRejection::Admission(_) => {
                self.empty_pop_lite_outcome(request_header, PopLiteResponseKind::PollingFull)
            }
            PopLiteDeferredPrepareRejection::EmbeddedOrigin | PopLiteDeferredPrepareRejection::InvalidHeader => {
                self.invalid_reply(0)
            }
            PopLiteDeferredPrepareRejection::ServiceClosed => self.reply_with_code(
                ResponseCode::ServiceNotAvailable,
                "the deferred POP Lite service is unavailable",
            ),
        }
    }

    fn prepare_failure_outcome(
        &self,
        request_header: &PopLiteMessageRequestHeader,
        failure: PopLiteDeferredPrepareFailure,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        match failure {
            PopLiteDeferredPrepareFailure::Header(_) => self.invalid_reply(0),
            PopLiteDeferredPrepareFailure::Deadline(_) => {
                self.empty_pop_lite_outcome(request_header, PopLiteResponseKind::PollingTimeout)
            }
            PopLiteDeferredPrepareFailure::InvalidExpiryMargins
            | PopLiteDeferredPrepareFailure::RetainedSizeOverflow
            | PopLiteDeferredPrepareFailure::Index(_)
            | PopLiteDeferredPrepareFailure::Contract(_) => self.internal_reply(0),
        }
    }

    fn register_rejection_outcome(
        &self,
        request_header: &PopLiteMessageRequestHeader,
        rejection: PopLiteDeferredRegisterRejection,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        match rejection {
            PopLiteDeferredRegisterRejection::ServiceClosed
            | PopLiteDeferredRegisterRejection::ServiceClosedAfterTake => self.reply_with_code(
                ResponseCode::ServiceNotAvailable,
                "the deferred POP Lite service is unavailable",
            ),
            PopLiteDeferredRegisterRejection::ProvenanceMismatch => self.internal_reply(0),
            PopLiteDeferredRegisterRejection::Responder(DeferredResponderOutcome::OneWayRequest) => {
                self.empty_pop_lite_outcome(request_header, PopLiteResponseKind::PollingTimeout)
            }
            PopLiteDeferredRegisterRejection::Responder(DeferredResponderOutcome::Unavailable) => self.reply_with_code(
                ResponseCode::ServiceNotAvailable,
                "a deferred POP Lite responder is unavailable",
            ),
            PopLiteDeferredRegisterRejection::Responder(
                DeferredResponderOutcome::AlreadyTaken | DeferredResponderOutcome::OutcomeCompleted,
            ) => self.internal_reply(0),
            PopLiteDeferredRegisterRejection::Responder(DeferredResponderOutcome::Taken(responder)) => {
                drop(responder);
                self.internal_reply(0)
            }
            PopLiteDeferredRegisterRejection::Expiry { outcome: _, parts } => {
                drop(parts);
                self.internal_reply(0)
            }
            PopLiteDeferredRegisterRejection::DuplicateRequest
            | PopLiteDeferredRegisterRejection::ParentCancelled
            | PopLiteDeferredRegisterRejection::SessionClosed
            | PopLiteDeferredRegisterRejection::DeadlineExpired => self.internal_reply(0),
        }
    }

    fn register_failure_outcome(
        &self,
        failure: PopLiteDeferredRegisterFailure,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        match failure {
            PopLiteDeferredRegisterFailure::IdentityExhausted => self.internal_reply(0),
            PopLiteDeferredRegisterFailure::RegistryContract(violation) => {
                Err(RocketMQError::internal("register deferred POP Lite request", violation))
            }
            PopLiteDeferredRegisterFailure::RegistryOperational(error) => {
                Err(RocketMQError::internal("register deferred POP Lite request", error))
            }
            PopLiteDeferredRegisterFailure::Contract { violation, parts } => {
                drop(parts);
                Err(RocketMQError::internal("register deferred POP Lite request", violation))
            }
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

    fn invalid_reply(&self, opaque: i32) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        command_outcome(remoting_error_response(
            PublicErrorView::descriptor_only(&rocketmq_error::CORE_ARGUMENT_INVALID),
            RemotingErrorTarget::Reply {
                factory: &self.context.command_factory,
                opaque,
            },
        ))
    }

    fn internal_reply(&self, opaque: i32) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        command_outcome(remoting_error_response(
            PublicErrorView::descriptor_only(&rocketmq_error::CORE_INTERNAL_FAILURE),
            RemotingErrorTarget::Reply {
                factory: &self.context.command_factory,
                opaque,
            },
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
