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

use std::net::SocketAddr;
use std::sync::Arc;

use cheetah_string::CheetahString;
use rocketmq_error::RocketMQError;
use rocketmq_model::common::constant::PermName;
use rocketmq_model::common::filter::expression_type::ExpressionType;
use rocketmq_model::common::FAQUrl;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::filter::filter_api::FilterAPI;
use rocketmq_protocol::protocol::header::pop_message_request_header::PopMessageRequestHeader;
use rocketmq_protocol::protocol::heartbeat::subscription_data::SubscriptionData;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_runtime::common::time_utils::current_millis;
use rocketmq_store::ArcMessageFilter;
use rocketmq_store::BrokerReadWriteStore;
use rocketmq_transport::api::v1::command_from_error_with_factory_remark_and_opaque;
use rocketmq_transport::api::v1::internal_error_with_factory_and_opaque;
use rocketmq_transport::api::v1::request_code_not_supported_with_factory_remark_and_opaque;
use rocketmq_transport::api::v2::DeferredAdmissionAcquireErrorKind;
use rocketmq_transport::api::v2::HandlerOutcome;
use rocketmq_transport::api::v2::RemotingRequest;
use rocketmq_transport::api::v2::RequestOrigin;
use rocketmq_transport::api::v2::RequestProcessorV2;
use rocketmq_transport::api::v2::TakeDeferredResponderError;
#[cfg(feature = "rocksdb_store")]
use tracing::error;
use tracing::warn;

use super::resume::PopCallerHost;
use super::resume::PopStoreReadOutcome;
use super::resume::PopStoreReadRequest;
use super::PopMessageProcessor;
use super::BORN_TIME;
use crate::filter::expression_message_filter::ExpressionMessageFilter;
use crate::long_polling::pop_deferred::index::PopIndexErrorKind;
use crate::long_polling::pop_deferred::service::PopDeferredPrepareError;
use crate::long_polling::pop_deferred::service::PopDeferredRegisterError;
use crate::long_polling::pop_deferred::service::PopRetainedEstimate;
use crate::processor::response_plan::BrokerResponseParts;

pub(super) struct PopInitialSuspend {
    pub(super) request_header: PopMessageRequestHeader,
    pub(super) subscription_data: SubscriptionData,
    pub(super) message_filter: Option<ArcMessageFilter>,
    pub(super) head: RemotingCommand,
    pub(super) rest_num: i64,
}

pub(super) enum PopInitialOutcome {
    Reply(BrokerResponseParts),
    Suspend(Box<PopInitialSuspend>),
}

impl<MS> RequestProcessorV2 for PopMessageProcessor<MS>
where
    MS: BrokerReadWriteStore + Send + Sync + 'static,
{
    async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        self.process_v2_shared(request).await
    }
}

impl<MS> PopMessageProcessor<MS>
where
    MS: BrokerReadWriteStore,
{
    pub(crate) async fn process_v2_shared(
        &self,
        request: &mut RemotingRequest,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        if RequestCode::from(request.original_identity().original_code()) != RequestCode::PopMessage {
            return BrokerResponseParts::command(request_code_not_supported_with_factory_remark_and_opaque(
                &self.context.command_factory,
                request.original_identity().original_code(),
                "PopMessageProcessor request code is not supported",
                request.original_identity().original_opaque(),
            ))?
            .into_handler_outcome();
        }
        if request.original_identity().is_one_way() {
            return self.invalid_reply(
                "POP does not support one-way requests",
                request.original_identity().original_opaque(),
            );
        }
        let effective_peer = match request.origin() {
            RequestOrigin::Network { peer } => peer.address(),
            _ => {
                return self.invalid_reply(
                    "POP requires a trusted network peer",
                    request.original_identity().original_opaque(),
                );
            }
        };
        let outcome = self.execute_pop_initial(request.command_mut(), effective_peer).await?;
        match outcome {
            PopInitialOutcome::Reply(parts) => parts.into_handler_outcome(),
            PopInitialOutcome::Suspend(suspension) => {
                let suspension = *suspension;
                let Some(service) = self.pop_deferred_service.get() else {
                    return self.reply_with_code(
                        ResponseCode::ServiceNotAvailable,
                        "the deferred POP service is not installed",
                    );
                };
                let prepared = match service.prepare(
                    request,
                    Some(suspension.subscription_data),
                    suspension.message_filter,
                    PopRetainedEstimate::default(),
                ) {
                    Ok(prepared) => prepared,
                    Err(error) => return self.prepare_error_outcome(suspension.head, error),
                };
                match service.register(prepared, request) {
                    Ok(registration) => Ok(HandlerOutcome::Deferred(registration)),
                    Err(error) => self.register_error_outcome(suspension.head, error),
                }
            }
        }
    }

    pub(super) async fn execute_pop_initial(
        &self,
        request: &mut RemotingCommand,
        effective_peer: SocketAddr,
    ) -> rocketmq_error::RocketMQResult<PopInitialOutcome> {
        normalize_born_time(request);
        let opaque = request.opaque();
        let request_header = match request.decode_command_custom_header::<PopMessageRequestHeader>() {
            Ok(header) => header,
            Err(_) => {
                return self.initial_invalid_reply(opaque, "decode POP request header failed");
            }
        };
        let policy = self.context.policy.snapshot();
        let retry_policy = self.retry_policy_for_group(&request_header.consumer_group);

        if request_header.is_timeout_too_much_at(current_millis() as i64) {
            return self.initial_reply(
                opaque,
                ResponseCode::PollingTimeout,
                format!("the broker[{}] pop message is timeout too much", policy.broker_ip),
            );
        }
        if !PermName::is_readable(policy.broker_permission) {
            return self.initial_permission_denied(
                opaque,
                format!("the broker[{}] pop message is forbidden", policy.broker_ip),
            );
        }
        if request_header.max_msg_nums > 32 {
            return self.initial_internal_reply(
                opaque,
                format!("the broker[{}] pop message's num is greater than 32", policy.broker_ip),
            );
        }
        if !policy.timer_wheel_enable {
            return self.initial_internal_reply(
                opaque,
                format!(
                    "the broker[{}] pop message is forbidden because timerWheelEnable is false",
                    policy.broker_ip
                ),
            );
        }
        let Some(topic_config) = self.context.topics.select_topic_config(&request_header.topic) else {
            return self.initial_reply(
                opaque,
                ResponseCode::TopicNotExist,
                format!(
                    "topic[{}] not exist, apply first please! {}",
                    request_header.topic,
                    FAQUrl::suggest_todo(FAQUrl::APPLY_TOPIC_URL)
                ),
            );
        };
        if !PermName::is_readable(topic_config.perm) {
            return self.initial_permission_denied(
                opaque,
                format!("the topic[{}] peeking message is forbidden", request_header.topic),
            );
        }
        if request_header.queue_id >= topic_config.read_queue_nums as i32 {
            return self.initial_internal_reply(
                opaque,
                format!(
                    "the queueId[{}] is illegal, topic[{}] topicConfig readQueueNums[{}] consumer[{}]",
                    request_header.queue_id, request_header.topic, topic_config.read_queue_nums, effective_peer
                ),
            );
        }
        let Some(subscription_group_config) = self
            .context
            .subscriptions
            .find_subscription_group_config(&request_header.consumer_group)
        else {
            return self.initial_reply(
                opaque,
                ResponseCode::SubscriptionGroupNotExist,
                format!(
                    "the consumer group[{}] not online, apply first please! {}",
                    request_header.consumer_group,
                    FAQUrl::suggest_todo(FAQUrl::SUBSCRIPTION_GROUP_NOT_EXIST)
                ),
            );
        };
        if !subscription_group_config.consume_enable() {
            return self.initial_permission_denied(
                opaque,
                format!(
                    "the consumer group[{}], not permitted to consume",
                    request_header.consumer_group
                ),
            );
        }

        let expression = request_header.exp.as_ref().filter(|value| !value.is_empty());
        let (subscription_data, retry_subscription_data, message_filter) = if let Some(expression) = expression {
            let subscription_data =
                match FilterAPI::build(&request_header.topic, expression, request_header.exp_type.clone()) {
                    Ok(value) => value,
                    Err(_) => {
                        warn!(
                            "Parse the consumer's subscription[{:?}] error, group: {}",
                            request_header.exp, request_header.consumer_group
                        );
                        return self.initial_reply(
                            opaque,
                            ResponseCode::SubscriptionParseFailed,
                            "parse the consumer's subscription failed",
                        );
                    }
                };
            let retry_topic = CheetahString::from_string(
                retry_policy.write_topic(&request_header.topic, &request_header.consumer_group),
            );
            let retry_subscription_data = match FilterAPI::build(
                &retry_topic,
                &CheetahString::from_static_str(SubscriptionData::SUB_ALL),
                request_header.exp_type.clone(),
            ) {
                Ok(value) => value,
                Err(_) => {
                    return self.initial_reply(
                        opaque,
                        ResponseCode::SubscriptionParseFailed,
                        "parse the consumer's subscription failed",
                    );
                }
            };
            let message_filter = if ExpressionType::is_tag_type(Some(subscription_data.expression_type.as_str())) {
                None
            } else {
                let Some(consumer_filter_data) = self.context.filters.resolve(
                    request_header.topic.clone(),
                    request_header.consumer_group.clone(),
                    request_header.exp.clone(),
                    request_header.exp_type.clone(),
                    current_millis(),
                ) else {
                    return self.initial_reply(
                        opaque,
                        ResponseCode::SubscriptionParseFailed,
                        "parse the consumer's subscription failed",
                    );
                };
                Some(Arc::new(ExpressionMessageFilter::new(
                    Some(subscription_data.clone()),
                    Some(consumer_filter_data),
                    Arc::clone(&self.context.filters),
                )) as ArcMessageFilter)
            };
            (subscription_data, retry_subscription_data, message_filter)
        } else {
            let subscription_data = FilterAPI::build(
                &request_header.topic,
                &CheetahString::from_static_str(SubscriptionData::SUB_ALL),
                Some(CheetahString::from_static_str(ExpressionType::TAG)),
            )
            .map_err(|error| {
                RocketMQError::internal("build POP wildcard subscription", std::io::Error::other(error))
            })?;
            let retry_topic = CheetahString::from_string(
                retry_policy.write_topic(&request_header.topic, &request_header.consumer_group),
            );
            let retry_subscription_data = FilterAPI::build(
                &retry_topic,
                &CheetahString::from_static_str(SubscriptionData::SUB_ALL),
                Some(CheetahString::from_static_str(ExpressionType::TAG)),
            )
            .map_err(|error| {
                RocketMQError::internal("build retry POP wildcard subscription", std::io::Error::other(error))
            })?;
            (subscription_data, retry_subscription_data, None)
        };

        let mut durable_subscriptions = vec![subscription_data.clone(), retry_subscription_data];
        for retry_topic in retry_policy.read_topics(&request_header.topic, &request_header.consumer_group) {
            if durable_subscriptions
                .iter()
                .any(|subscription| subscription.topic.as_str() == retry_topic)
            {
                continue;
            }
            durable_subscriptions.push(
                FilterAPI::build(
                    &CheetahString::from_string(retry_topic),
                    &CheetahString::from_static_str(SubscriptionData::SUB_ALL),
                    request_header.exp_type.clone(),
                )
                .map_err(|error| {
                    RocketMQError::internal("build durable POP subscription", std::io::Error::other(error))
                })?,
            );
        }
        #[cfg(feature = "rocksdb_store")]
        let retry_policy = if let Some(profile_store) = &self.profile_store {
            let profile_store = Arc::clone(profile_store);
            let group = request_header.consumer_group.clone();
            let subscriptions = durable_subscriptions.clone();
            let requested_retry_policy = retry_policy.clone();
            let persist_result = self
                .context
                .metadata_io
                .spawn_io("broker.pop-consumer-profile.upsert", move || {
                    profile_store.upsert(group, subscriptions, requested_retry_policy, current_millis() as i64)
                })
                .await;
            match persist_result {
                Ok(Ok(profile)) => {
                    let persisted_policy = profile
                        .retry_policy
                        .unwrap_or_else(|| policy.default_retry_policy.clone());
                    self.context
                        .policy
                        .restore_retry_policy(request_header.consumer_group.clone(), persisted_policy.clone());
                    persisted_policy
                }
                Ok(Err(error)) => {
                    error!(%error, group = %request_header.consumer_group, "Failed to persist POP consumer profile");
                    return self.initial_reply(
                        opaque,
                        ResponseCode::ServiceNotAvailable,
                        "POP consumer profile persistence is unavailable",
                    );
                }
                Err(error) => {
                    error!(%error, group = %request_header.consumer_group, "Failed to persist POP consumer profile");
                    return self.initial_reply(
                        opaque,
                        ResponseCode::ServiceNotAvailable,
                        "POP consumer profile persistence is unavailable",
                    );
                }
            }
        } else {
            retry_policy
        };
        self.context
            .consumers
            .restore_pop_consumer_profile(&request_header.consumer_group, &durable_subscriptions);

        match self
            .read_pop_store(PopStoreReadRequest::new(
                &request_header,
                &topic_config,
                &policy,
                &retry_policy,
                subscription_group_config.priority_factor(),
                message_filter.clone(),
                PopCallerHost::Network(effective_peer),
                opaque,
            ))
            .await?
        {
            PopStoreReadOutcome::Found(parts) => Ok(PopInitialOutcome::Reply(parts)),
            PopStoreReadOutcome::Empty { head, rest_num } => {
                Ok(PopInitialOutcome::Suspend(Box::new(PopInitialSuspend {
                    request_header,
                    subscription_data,
                    message_filter,
                    head,
                    rest_num,
                })))
            }
        }
    }

    fn prepare_error_outcome(
        &self,
        mut head: RemotingCommand,
        error: PopDeferredPrepareError,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        match error {
            PopDeferredPrepareError::Deadline(_) => {
                head.set_code_ref(ResponseCode::PollingTimeout);
                BrokerResponseParts::command(head)?.into_handler_outcome()
            }
            PopDeferredPrepareError::Index(error)
                if matches!(
                    error.kind(),
                    PopIndexErrorKind::GlobalCapacity | PopIndexErrorKind::BucketCapacity
                ) =>
            {
                head.set_code_ref(ResponseCode::PollingFull);
                BrokerResponseParts::command(head)?.into_handler_outcome()
            }
            PopDeferredPrepareError::Admission(error)
                if !matches!(error.kind(), DeferredAdmissionAcquireErrorKind::RetainedSizeOverflow) =>
            {
                head.set_code_ref(ResponseCode::PollingFull);
                BrokerResponseParts::command(head)?.into_handler_outcome()
            }
            PopDeferredPrepareError::EmbeddedOrigin
            | PopDeferredPrepareError::Header(_)
            | PopDeferredPrepareError::MissingCallerHost => self.invalid_reply("invalid deferred POP request", 0),
            PopDeferredPrepareError::ServiceClosed => self.reply_with_code(
                ResponseCode::ServiceNotAvailable,
                "the deferred POP service is unavailable",
            ),
            PopDeferredPrepareError::InvalidExpiryMargins
            | PopDeferredPrepareError::RetainedSizeOverflow
            | PopDeferredPrepareError::Index(_)
            | PopDeferredPrepareError::Admission(_) => {
                self.internal_reply("the deferred POP request could not be prepared", 0)
            }
        }
    }

    fn register_error_outcome(
        &self,
        mut head: RemotingCommand,
        error: PopDeferredRegisterError,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        match error {
            PopDeferredRegisterError::ServiceClosed => self.reply_with_code(
                ResponseCode::ServiceNotAvailable,
                "the deferred POP service is unavailable",
            ),
            PopDeferredRegisterError::Responder(TakeDeferredResponderError::OneWayRequest) => {
                head.set_code_ref(ResponseCode::PollingTimeout);
                BrokerResponseParts::command(head)?.into_handler_outcome()
            }
            PopDeferredRegisterError::Responder(TakeDeferredResponderError::Unavailable) => self.reply_with_code(
                ResponseCode::ServiceNotAvailable,
                "a deferred POP responder is unavailable",
            ),
            error => Err(RocketMQError::internal("register deferred POP request", error)),
        }
    }

    fn initial_reply(
        &self,
        opaque: i32,
        code: ResponseCode,
        remark: impl Into<CheetahString>,
    ) -> rocketmq_error::RocketMQResult<PopInitialOutcome> {
        let command = self
            .context
            .command_factory
            .create_response_command_with_code_remark(code, remark)
            .set_opaque(opaque);
        Ok(PopInitialOutcome::Reply(BrokerResponseParts::command(command)?))
    }

    fn initial_invalid_reply(
        &self,
        opaque: i32,
        remark: &'static str,
    ) -> rocketmq_error::RocketMQResult<PopInitialOutcome> {
        let error = RocketMQError::illegal_argument(remark);
        self.initial_error_reply(opaque, &error, remark)
    }

    fn initial_permission_denied(
        &self,
        opaque: i32,
        remark: String,
    ) -> rocketmq_error::RocketMQResult<PopInitialOutcome> {
        let error = RocketMQError::BrokerPermissionDenied {
            operation: remark.clone(),
        };
        self.initial_error_reply(opaque, &error, remark)
    }

    fn initial_internal_reply(&self, opaque: i32, remark: String) -> rocketmq_error::RocketMQResult<PopInitialOutcome> {
        let command = internal_error_with_factory_and_opaque(&self.context.command_factory, opaque, remark);
        Ok(PopInitialOutcome::Reply(BrokerResponseParts::command(command)?))
    }

    fn initial_error_reply(
        &self,
        opaque: i32,
        error: &RocketMQError,
        remark: impl Into<String>,
    ) -> rocketmq_error::RocketMQResult<PopInitialOutcome> {
        let command =
            command_from_error_with_factory_remark_and_opaque(&self.context.command_factory, error, remark, opaque);
        Ok(PopInitialOutcome::Reply(BrokerResponseParts::command(command)?))
    }

    fn reply_with_code(
        &self,
        code: ResponseCode,
        remark: &'static str,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        BrokerResponseParts::command(
            self.context
                .command_factory
                .create_response_command_with_code_remark(code, remark),
        )?
        .into_handler_outcome()
    }

    fn invalid_reply(&self, remark: &'static str, opaque: i32) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let error = RocketMQError::illegal_argument(remark);
        BrokerResponseParts::command(command_from_error_with_factory_remark_and_opaque(
            &self.context.command_factory,
            &error,
            remark,
            opaque,
        ))?
        .into_handler_outcome()
    }

    fn internal_reply(&self, remark: &'static str, opaque: i32) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        BrokerResponseParts::command(internal_error_with_factory_and_opaque(
            &self.context.command_factory,
            opaque,
            remark,
        ))?
        .into_handler_outcome()
    }
}

pub(super) fn normalize_born_time(request: &mut RemotingCommand) {
    let now = current_millis();
    request.add_ext_field_if_not_exist(CheetahString::from_static_str(BORN_TIME), now.to_string());
    if request
        .get_ext_fields()
        .and_then(|fields| fields.get(BORN_TIME))
        .is_some_and(|value| value == "0")
    {
        request.add_ext_field(CheetahString::from_static_str(BORN_TIME), now.to_string());
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::Duration;

    use cheetah_string::CheetahString;
    use rocketmq_model::common::constant::consume_init_mode::ConsumeInitMode;
    use rocketmq_protocol::code::request_code::RequestCode;
    use rocketmq_protocol::code::response_code::ResponseCode;
    use rocketmq_protocol::protocol::header::pop_message_request_header::PopMessageRequestHeader;
    use rocketmq_transport::api::v1::AdmissionController;
    use rocketmq_transport::api::v1::AdmissionLimits;
    use rocketmq_transport::api::v2::DeferredAdmission;
    use rocketmq_transport::api::v2::DeferredExpiryMargins;
    use rocketmq_transport::api::v2::DeferredWaitLimits;
    use rocketmq_transport::api::v2::HandlerOutcome;
    use rocketmq_transport::api::v2::RemotingRequest;
    use rocketmq_transport::api::v2::RequestProcessorV2;

    use super::super::tests::new_test_runtime;
    use super::super::PopMessageProcessor;
    use super::normalize_born_time;
    use super::RemotingCommand;
    use crate::broker_runtime::BrokerMessageStore;
    use crate::long_polling::pop_deferred::index::PopCriteriaLimits;
    use crate::long_polling::pop_deferred::service::PopDeferredService;
    use crate::processor::v2_leaf_test_support::start_v2_leaf_server;

    #[derive(Clone)]
    struct ArcHeldPopProcessor {
        inner: Arc<PopMessageProcessor<BrokerMessageStore>>,
    }

    impl RequestProcessorV2 for ArcHeldPopProcessor {
        async fn process(&mut self, request: &mut RemotingRequest) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
            self.inner.process_v2_shared(request).await
        }
    }

    fn nonzero(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).expect("test limit is nonzero")
    }

    fn deferred_service(controller: &AdmissionController) -> Arc<PopDeferredService> {
        let admission = DeferredAdmission::try_configure(controller, DeferredWaitLimits::new(4, 4 * 1024 * 1024))
            .expect("POP deferred admission");
        Arc::new(PopDeferredService::new(
            admission,
            PopCriteriaLimits::new(nonzero(4), nonzero(4)),
            DeferredExpiryMargins::new(Duration::from_millis(2), Duration::from_millis(2)),
            nonzero(1),
        ))
    }

    fn pop_request(opaque: i32) -> RemotingCommand {
        let header = PopMessageRequestHeader {
            consumer_group: CheetahString::from_static_str("group-a"),
            topic: CheetahString::from_static_str("topic-a"),
            queue_id: 0,
            max_msg_nums: 1,
            invisible_time: 30_000,
            poll_time: 60_000,
            born_time: 0,
            init_mode: ConsumeInitMode::MIN,
            exp_type: None,
            exp: None,
            order: Some(false),
            attempt_id: None,
            topic_request_header: None,
        };
        let mut command = RemotingCommand::create_request_command(RequestCode::PopMessage, header).set_opaque(opaque);
        command.make_custom_header_to_net();
        command
    }

    #[test]
    fn pop_v2_normalizes_missing_and_zero_born_time() {
        let mut missing = RemotingCommand::create_remoting_command(RequestCode::PopMessage.to_i32());
        normalize_born_time(&mut missing);
        assert!(missing
            .get_ext_fields()
            .and_then(|fields| fields.get("bornTime"))
            .is_some_and(|value| value != "0"));

        let mut zero = RemotingCommand::create_remoting_command(RequestCode::PopMessage.to_i32());
        zero.add_ext_field(CheetahString::from_static_str("bornTime"), "0".to_owned());
        normalize_born_time(&mut zero);
        assert!(zero
            .get_ext_fields()
            .and_then(|fields| fields.get("bornTime"))
            .is_some_and(|value| value != "0"));
    }

    #[tokio::test]
    async fn pop_v2_arc_leaf_fails_closed_without_service_then_seals_deferred_wait() {
        let mut runtime = new_test_runtime("pop-v2-leaf").await;
        runtime.seed_pop_topic_and_group_for_test("topic-a", "group-a");
        let processor = runtime.pop_message_processor_for_test();

        let unavailable_controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
        let (mut unavailable_client, unavailable_server) = start_v2_leaf_server(
            "pop-v2-unavailable",
            ArcHeldPopProcessor {
                inner: Arc::clone(&processor),
            },
            unavailable_controller,
        )
        .await;
        unavailable_client
            .send_command(pop_request(98_482))
            .await
            .expect("send POP V2 request");
        let unavailable = unavailable_client
            .receive_command()
            .await
            .expect("read POP V2 connection")
            .expect("POP unavailable response");
        assert_eq!(unavailable.code(), ResponseCode::ServiceNotAvailable as i32);
        assert_eq!(unavailable.opaque(), 98_482);
        drop(unavailable_client);
        unavailable_server.finish().await;

        let controller = Arc::new(AdmissionController::new(AdmissionLimits::default()));
        let service = deferred_service(controller.as_ref());
        assert!(processor.install_pop_deferred_service(Arc::clone(&service)).is_ok());
        let (mut deferred_client, deferred_server) =
            start_v2_leaf_server("pop-v2-deferred", ArcHeldPopProcessor { inner: processor }, controller).await;
        deferred_client
            .send_command(pop_request(98_483))
            .await
            .expect("send deferred POP V2 request");
        tokio::time::timeout(Duration::from_secs(5), async {
            while service.admission_snapshot().waiting_count() != 1 || service.index_snapshot().live() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("POP request seals one deferred registration");

        let _ = service.shutdown();
        tokio::time::timeout(Duration::from_secs(5), async {
            while service.admission_snapshot().waiting_count() != 0 || service.index_snapshot().live() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("POP deferred owners are released after dispatcher commit");
        assert_eq!(service.admission_snapshot().waiting_count(), 0);
        assert_eq!(service.index_snapshot().live(), 0);
        drop(deferred_client);
        deferred_server.finish().await;

        let root = runtime.message_store_config().store_path_root_dir.to_string();
        drop(runtime);
        let _ = std::fs::remove_dir_all(root);
    }
}
