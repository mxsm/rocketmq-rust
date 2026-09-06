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
use rocketmq_protocol::protocol::header::notification_request_header::NotificationRequestHeader;
use rocketmq_store::BrokerReadWriteStore;
use rocketmq_transport::api::error_response as remoting_error_response;
use rocketmq_transport::api::DeferredResponderOutcome;
use rocketmq_transport::api::HandlerOutcome;
use rocketmq_transport::api::RemotingErrorTarget;
use rocketmq_transport::api::RemotingRequest;
use rocketmq_transport::api::RequestOrigin;
use rocketmq_transport::api::RequestProcessor;

use super::core::NotificationCoreOutcome;
use super::response::compose_notification_response;
use super::NotificationProcessor;
use crate::long_polling::notification_deferred::service::NotificationDeferredPrepareFailure;
use crate::long_polling::notification_deferred::service::NotificationDeferredPrepareOutcome;
use crate::long_polling::notification_deferred::service::NotificationDeferredPrepareRejection;
use crate::long_polling::notification_deferred::service::NotificationDeferredRegisterFailure;
use crate::long_polling::notification_deferred::service::NotificationDeferredRegisterOutcome;
use crate::long_polling::notification_deferred::service::NotificationDeferredRegisterRejection;
use crate::long_polling::notification_deferred::service::NotificationRetainedEstimate;
use crate::processor::response_assembly::BrokerResponseParts;

impl<MS> RequestProcessor for NotificationProcessor<MS>
where
    MS: BrokerReadWriteStore + Send + Sync + 'static,
{
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        self.process_shared(request).await
    }
}

impl<MS> NotificationProcessor<MS>
where
    MS: BrokerReadWriteStore,
{
    pub(crate) async fn process_shared(
        &self,
        request: &mut RemotingRequest,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        if RequestCode::from(request.original_identity().original_code()) != RequestCode::Notification {
            return command_outcome(remoting_error_response(
                PublicErrorView::descriptor_only(&rocketmq_error::PROTOCOL_REQUEST_UNSUPPORTED),
                RemotingErrorTarget::Reply {
                    factory: &self.context.command_factory,
                    opaque: request.original_identity().original_opaque(),
                },
            ));
        }
        if request.original_identity().is_one_way() {
            return command_outcome(compose_notification_response(
                &self.context.command_factory,
                false,
                false,
                request.original_identity().original_opaque(),
            ));
        }
        let effective_peer = match request.origin() {
            RequestOrigin::Network { peer } => peer.address(),
            _ => {
                return self.invalid_reply(request.original_identity().original_opaque());
            }
        };
        normalize_born_time(request.command_mut());
        let request_header = match request
            .command()
            .decode_command_custom_header::<NotificationRequestHeader>()
        {
            Ok(header) => header,
            Err(_) => {
                return self.invalid_reply(request.original_identity().original_opaque());
            }
        };

        let opaque = request.original_identity().original_opaque();
        let outcome = match self
            .execute_notification_core(&request_header, effective_peer, opaque, None)
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => return command_outcome(self.notification_error_response(&error, opaque)),
        };
        match outcome {
            NotificationCoreOutcome::Reply(response) => command_outcome(response),
            NotificationCoreOutcome::Ready(ready) if ready.has_msg => command_outcome(compose_notification_response(
                &self.context.command_factory,
                true,
                false,
                opaque,
            )),
            NotificationCoreOutcome::Ready(ready) => {
                let Some(service) = self.notification_deferred_service.get() else {
                    return self.reply_with_code(
                        ResponseCode::ServiceNotAvailable,
                        "the deferred Notification service is not installed",
                    );
                };
                let (subscription, filter) = ready
                    .filter_contract
                    .map(|contract| (Some(contract.subscription_data), Some(contract.message_filter)))
                    .unwrap_or((None, None));
                let prepared =
                    match service.prepare(request, subscription, filter, NotificationRetainedEstimate::default()) {
                        Ok(NotificationDeferredPrepareOutcome::Prepared(prepared)) => *prepared,
                        Ok(NotificationDeferredPrepareOutcome::Rejected(rejection)) => {
                            return self.prepare_rejection_outcome(rejection);
                        }
                        Err(failure) => return self.prepare_failure_outcome(failure),
                    };
                match service.register(prepared, request) {
                    Ok(NotificationDeferredRegisterOutcome::Registered(registration)) => {
                        Ok(HandlerOutcome::Deferred(*registration))
                    }
                    Ok(NotificationDeferredRegisterOutcome::Rejected(rejection)) => {
                        self.register_rejection_outcome(*rejection)
                    }
                    Err(failure) => self.register_failure_outcome(failure),
                }
            }
        }
    }
}

impl<MS> NotificationProcessor<MS>
where
    MS: BrokerReadWriteStore,
{
    fn prepare_rejection_outcome(
        &self,
        rejection: NotificationDeferredPrepareRejection,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        match rejection {
            NotificationDeferredPrepareRejection::Deadline(_) | NotificationDeferredPrepareRejection::OneWay => {
                command_outcome(compose_notification_response(
                    &self.context.command_factory,
                    false,
                    false,
                    0,
                ))
            }
            NotificationDeferredPrepareRejection::IndexCapacity(_) => command_outcome(compose_notification_response(
                &self.context.command_factory,
                false,
                true,
                0,
            )),
            NotificationDeferredPrepareRejection::Admission(_) => command_outcome(compose_notification_response(
                &self.context.command_factory,
                false,
                true,
                0,
            )),
            NotificationDeferredPrepareRejection::EmbeddedOrigin => self.invalid_reply(0),
            NotificationDeferredPrepareRejection::ServiceClosed => self.reply_with_code(
                ResponseCode::ServiceNotAvailable,
                "the deferred Notification service is unavailable",
            ),
        }
    }

    fn prepare_failure_outcome(
        &self,
        failure: NotificationDeferredPrepareFailure,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        match failure {
            NotificationDeferredPrepareFailure::Header(_) => self.invalid_reply(0),
            NotificationDeferredPrepareFailure::WallTimeOverflow | NotificationDeferredPrepareFailure::Deadline(_) => {
                command_outcome(compose_notification_response(
                    &self.context.command_factory,
                    false,
                    false,
                    0,
                ))
            }
            NotificationDeferredPrepareFailure::InvalidExpiryMargins
            | NotificationDeferredPrepareFailure::RetainedSizeOverflow
            | NotificationDeferredPrepareFailure::Index(_)
            | NotificationDeferredPrepareFailure::Contract(_) => self.internal_reply(0),
        }
    }

    fn register_rejection_outcome(
        &self,
        rejection: NotificationDeferredRegisterRejection,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        match rejection {
            NotificationDeferredRegisterRejection::ServiceClosedBeforeTake
            | NotificationDeferredRegisterRejection::ServiceClosedAfterTake => self.reply_with_code(
                ResponseCode::ServiceNotAvailable,
                "the deferred Notification service is unavailable",
            ),
            NotificationDeferredRegisterRejection::ProvenanceMismatch => self.internal_reply(0),
            NotificationDeferredRegisterRejection::Responder(DeferredResponderOutcome::OneWayRequest) => {
                command_outcome(compose_notification_response(
                    &self.context.command_factory,
                    false,
                    false,
                    0,
                ))
            }
            NotificationDeferredRegisterRejection::Responder(DeferredResponderOutcome::Unavailable) => self
                .reply_with_code(
                    ResponseCode::ServiceNotAvailable,
                    "a deferred Notification responder is unavailable",
                ),
            NotificationDeferredRegisterRejection::Responder(
                DeferredResponderOutcome::AlreadyTaken | DeferredResponderOutcome::OutcomeCompleted,
            ) => self.internal_reply(0),
            NotificationDeferredRegisterRejection::Responder(DeferredResponderOutcome::Taken(responder)) => {
                drop(responder);
                self.internal_reply(0)
            }
            NotificationDeferredRegisterRejection::Expiry { outcome: _, parts } => {
                drop(parts);
                self.internal_reply(0)
            }
            NotificationDeferredRegisterRejection::DuplicateRequest
            | NotificationDeferredRegisterRejection::ParentCancelled
            | NotificationDeferredRegisterRejection::SessionClosed
            | NotificationDeferredRegisterRejection::DeadlineExpired => self.internal_reply(0),
        }
    }

    fn register_failure_outcome(
        &self,
        failure: NotificationDeferredRegisterFailure,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        match failure {
            NotificationDeferredRegisterFailure::IdentityExhausted => self.internal_reply(0),
            NotificationDeferredRegisterFailure::RegistryContract(violation) => Err(RocketMQError::internal(
                "register deferred Notification request",
                violation,
            )),
            NotificationDeferredRegisterFailure::RegistryOperational(error) => {
                Err(RocketMQError::internal("register deferred Notification request", error))
            }
            NotificationDeferredRegisterFailure::Contract { violation, parts } => {
                drop(parts);
                Err(RocketMQError::internal(
                    "register deferred Notification request",
                    violation,
                ))
            }
        }
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

fn normalize_born_time(command: &mut rocketmq_protocol::protocol::remoting_command::RemotingCommand) {
    const BORN_TIME: &str = "bornTime";
    let now = rocketmq_runtime::common::time_utils::current_millis();
    command.add_ext_field_if_not_exist(
        cheetah_string::CheetahString::from_static_str(BORN_TIME),
        now.to_string(),
    );
    if command
        .get_ext_fields()
        .and_then(|fields| fields.get(BORN_TIME))
        .is_some_and(|value| value == "0")
    {
        command.add_ext_field(
            cheetah_string::CheetahString::from_static_str(BORN_TIME),
            now.to_string(),
        );
    }
}

fn command_outcome(
    command: rocketmq_protocol::protocol::remoting_command::RemotingCommand,
) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
    BrokerResponseParts::command(command)?.into_handler_outcome()
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::Duration;

    use cheetah_string::CheetahString;
    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_protocol::protocol::header::notification_request_header::NotificationRequestHeader;
    use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
    use rocketmq_store::MessageStoreConfig;
    use rocketmq_transport::api::AdmissionController;
    use rocketmq_transport::api::AdmissionLimits;
    use rocketmq_transport::api::DeferredAdmission;
    use rocketmq_transport::api::DeferredExpiryMargins;
    use rocketmq_transport::api::DeferredWaitLimits;
    use rocketmq_transport::api::HandlerOutcome;
    use rocketmq_transport::api::RemotingRequest;
    use rocketmq_transport::api::RequestProcessor;

    use super::super::tests::notification_processor_for_test;
    use super::super::NotificationProcessor;
    use super::normalize_born_time;
    use crate::broker_runtime::BrokerMessageStore;
    use crate::broker_runtime::BrokerRuntime;
    use crate::config::broker_config::BrokerConfig;
    use crate::long_polling::notification_deferred::index::NotificationCriteriaLimits;
    use crate::long_polling::notification_deferred::service::NotificationDeferredService;
    use crate::processor::processor_test_support::start_processor_server;

    #[derive(Clone)]
    struct ArcHeldNotificationProcessor {
        inner: Arc<NotificationProcessor<BrokerMessageStore>>,
    }

    impl RequestProcessor for ArcHeldNotificationProcessor {
        async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
            self.inner.process_shared(request).await
        }
    }

    fn nonzero(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).expect("test limit is nonzero")
    }

    fn deferred_service(controller: &AdmissionController) -> Arc<NotificationDeferredService> {
        let admission = DeferredAdmission::try_configure(controller, DeferredWaitLimits::new(4, 4 * 1024 * 1024))
            .expect("Notification deferred admission");
        Arc::new(NotificationDeferredService::new(
            admission,
            NotificationCriteriaLimits::new(nonzero(4), 4, 4),
            DeferredExpiryMargins::new(Duration::from_millis(2), Duration::from_millis(2)),
            nonzero(4),
            nonzero(4),
            nonzero(2),
            nonzero(1024 * 1024),
        ))
    }

    fn notification_request(opaque: i32) -> RemotingCommand {
        let header = NotificationRequestHeader {
            consumer_group: CheetahString::from_static_str("group-a"),
            topic: CheetahString::from_static_str("topic-a"),
            queue_id: 0,
            poll_time: 60_000,
            born_time: 0,
            order: false,
            attempt_id: None,
            exp_type: None,
            exp: None,
            is_lite_consumer: false,
            client_id: Some(CheetahString::from_static_str("notification-client")),
            topic_request_header: None,
        };
        let mut command = RemotingCommand::create_request_command(RequestCode::Notification, header).set_opaque(opaque);
        command.make_custom_header_to_net();
        command
    }

    #[test]
    fn notification_normalizes_missing_and_zero_born_time_but_preserves_positive_value() {
        let mut missing = RemotingCommand::create_remoting_command(RequestCode::Notification.to_i32());
        normalize_born_time(&mut missing);
        let missing_value = missing
            .get_ext_fields()
            .and_then(|fields| fields.get("bornTime"))
            .expect("missing bornTime is normalized")
            .parse::<u64>()
            .expect("normalized bornTime is numeric");
        assert!(missing_value > 0);

        let mut zero = RemotingCommand::create_remoting_command(RequestCode::Notification.to_i32());
        zero.add_ext_field(CheetahString::from_static_str("bornTime"), "0".to_owned());
        normalize_born_time(&mut zero);
        assert_ne!(
            zero.get_ext_fields()
                .and_then(|fields| fields.get("bornTime"))
                .map(|value| value.as_str()),
            Some("0")
        );

        let mut positive = RemotingCommand::create_remoting_command(RequestCode::Notification.to_i32());
        positive.add_ext_field(CheetahString::from_static_str("bornTime"), "12345".to_owned());
        normalize_born_time(&mut positive);
        assert_eq!(
            positive
                .get_ext_fields()
                .and_then(|fields| fields.get("bornTime"))
                .map(|value| value.as_str()),
            Some("12345")
        );
    }

    #[tokio::test]
    async fn notification_arc_leaf_fails_closed_without_service_then_seals_zero_born_time_wait() {
        let broker_config = Arc::new(BrokerConfig::default());
        let message_store_config = Arc::new(MessageStoreConfig::default());
        let mut runtime = BrokerRuntime::new(broker_config, message_store_config);
        runtime.seed_pop_topic_and_group_for_test("topic-a", "group-a");
        let processor = notification_processor_for_test(&mut runtime);

        let unavailable_controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
        let (mut unavailable_client, unavailable_server) = start_processor_server(
            "notification-unavailable",
            ArcHeldNotificationProcessor {
                inner: Arc::clone(&processor),
            },
            unavailable_controller,
        )
        .await;
        unavailable_client
            .send_command(notification_request(98_480))
            .await
            .expect("send Notification request");
        let unavailable = unavailable_client
            .receive_command()
            .await
            .expect("read Notification connection")
            .expect("Notification unavailable response");
        assert_eq!(unavailable.code(), ResponseCode::ServiceNotAvailable as i32);
        assert_eq!(unavailable.opaque(), 98_480);
        drop(unavailable_client);
        unavailable_server.finish().await;

        let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
        let service = deferred_service(controller.as_ref());
        assert!(processor
            .install_notification_deferred_service(Arc::clone(&service))
            .is_ok());
        let (mut deferred_client, deferred_server) = start_processor_server(
            "notification-deferred",
            ArcHeldNotificationProcessor { inner: processor },
            controller,
        )
        .await;
        deferred_client
            .send_command(notification_request(98_481))
            .await
            .expect("send deferred Notification request");
        tokio::time::timeout(Duration::from_secs(5), async {
            while {
                let snapshot = service.snapshot();
                snapshot.admission().waiting_count() != 1 || snapshot.index().live() != 1
            } {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Notification request seals one deferred registration");

        let _ = service.shutdown();
        tokio::time::timeout(Duration::from_secs(5), async {
            while {
                let snapshot = service.snapshot();
                snapshot.admission().waiting_count() != 0 || snapshot.index().live() != 0
            } {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Notification deferred owners are released after dispatcher commit");
        let snapshot = service.snapshot();
        assert_eq!(snapshot.admission().waiting_count(), 0);
        assert_eq!(snapshot.index().live(), 0);
        drop(deferred_client);
        deferred_server.finish().await;
    }
}
