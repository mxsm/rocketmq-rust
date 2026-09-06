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

use super::supervised_mutation_decode::{
    bounded_consume_stats_from_response, client_group_config, client_request_mode, client_topic_config,
    conditional_offset_outcome_from_response, request_mode_cas_outcome_from_response, state_cas_outcome_from_response,
    supervised_consume_stats_from_response, wire_expected_state, wire_request_mode,
};
use super::versioned_config::mutation_topic_config_versioned_from_response;
use super::*;
use crate::admin::BrokerMutationConfigState as ClientBrokerMutationConfigState;
use crate::admin::ConditionalConsumerOffsetOutcome;
use crate::admin::MutationExpectedMessageRequestMode;
use crate::admin::MutationExpectedState;
use crate::admin::MutationMessageRequestMode;
use crate::admin::MutationMessageRequestModeOutcome;
use crate::admin::MutationStateCasOutcome;
use crate::admin::MutationSubscriptionGroupConfig;
use crate::admin::MutationSubscriptionGroupConfigState;
use crate::admin::MutationTopicConfig;
use crate::admin::MutationTopicConfigState;
use rocketmq_protocol::protocol::body::supervised_mutation::{
    BrokerMutationConfigState as WireBrokerMutationConfigState, ExpectedMessageRequestMode,
    GetMessageRequestModeRequestBody, MessageRequestModeStateBody, SetMessageRequestModeCasRequestBody,
    SupervisedSubscriptionGroupConfig, SupervisedSubscriptionGroupConfigCasRequestBody, SupervisedTopicConfig,
    SupervisedTopicConfigCasRequestBody,
};
use rocketmq_protocol::protocol::header::get_consume_stats_request_header::GetConsumeStatsRequestHeader;
use rocketmq_protocol::protocol::header::get_subscription_group_config_request_header::GetSubscriptionGroupConfigRequestHeader;
use rocketmq_protocol::protocol::header::update_consumer_offset_conditional_header::UpdateConsumerOffsetConditionalHeader;
use rocketmq_protocol::protocol::header::update_subscription_group_config_cas_response_header::UpdateSubscriptionGroupConfigCasResponseHeader;

impl MQClientAPIImpl {
    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn get_consume_stats_for_mutation(
        &self,
        addr: &CheetahString,
        request_header: GetConsumeStatsRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_protocol::protocol::admin::consume_stats::ConsumeStats> {
        let request = self.create_request_command(RequestCode::GetConsumeStats, request_header);
        let outcome = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "get_consume_stats_for_mutation",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "get_consume_stats_for_mutation",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        bounded_consume_stats_from_response(&response)
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn get_consume_stats_for_supervised_mutation(
        &self,
        addr: &CheetahString,
        broker_name: &CheetahString,
        read_queue_nums: u32,
        request_header: GetConsumeStatsRequestHeader,
        timeout_millis: u64,
    ) -> RocketMQResult<rocketmq_protocol::protocol::admin::consume_stats::ConsumeStats> {
        let topic = request_header.topic.clone();
        let request = self.create_request_command(RequestCode::GetConsumeStats, request_header);
        let outcome = self
            .remoting_client
            .invoke_request(Some(addr), request, timeout_millis)
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "get_consume_stats_for_supervised_mutation",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "get_consume_stats_for_supervised_mutation",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        supervised_consume_stats_from_response(&response, &topic, broker_name, read_queue_nums)
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn get_topic_config_state_for_mutation(
        &self,
        addr: &CheetahString,
        topic: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<MutationTopicConfigState> {
        let request = self.create_request_command(
            RequestCode::GetTopicConfig,
            GetTopicConfigRequestHeader {
                topic,
                topic_request_header: None,
            },
        );
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let outcome = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "get_topic_config_state_for_mutation",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "get_topic_config_state_for_mutation",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let state = mutation_topic_config_versioned_from_response(&response)?;
                Ok(MutationTopicConfigState {
                    state: MutationExpectedState::Present { version: state.version },
                    config: Some(client_topic_config(&state.config)?),
                })
            }
            ResponseCode::TopicNotExist => Ok(MutationTopicConfigState {
                state: MutationExpectedState::Absent,
                config: None,
            }),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn replace_topic_config_if_state(
        &self,
        addr: &CheetahString,
        topic: CheetahString,
        expected_state: MutationExpectedState,
        replacement: MutationTopicConfig,
        timeout_millis: u64,
    ) -> RocketMQResult<MutationStateCasOutcome> {
        if !(1..=128).contains(&replacement.read_queue_nums)
            || !(1..=128).contains(&replacement.write_queue_nums)
            || !(1..=7).contains(&replacement.perm)
            || replacement.perm & 0b110 == 0
        {
            return Err(RocketMQError::illegal_argument(
                "supervised Topic replacement is outside the closed queue/permission bounds",
            ));
        }
        let body = SupervisedTopicConfigCasRequestBody {
            expected_state: wire_expected_state(expected_state),
            replacement: SupervisedTopicConfig {
                read_queue_nums: replacement.read_queue_nums,
                write_queue_nums: replacement.write_queue_nums,
                perm: replacement.perm,
                order: replacement.order,
                message_type: replacement.message_type.wire_name().to_owned(),
            },
        };
        let request = self
            .create_request_command(
                RequestCode::UpdateTopicConfigStateCas,
                GetTopicConfigRequestHeader {
                    topic,
                    topic_request_header: None,
                },
            )
            .set_body(body.encode()?);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let outcome = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "replace_topic_config_if_state",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "replace_topic_config_if_state",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        state_cas_outcome_from_response(&response, expected_state)
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn get_subscription_group_config_state_for_mutation(
        &self,
        addr: &CheetahString,
        group: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<MutationSubscriptionGroupConfigState> {
        let request = self.create_request_command(
            RequestCode::GetSubscriptionGroupConfig,
            GetSubscriptionGroupConfigRequestHeader {
                group,
                rpc_request_header: None,
            },
        );
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let outcome = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "get_subscription_group_config_state_for_mutation",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "get_subscription_group_config_state_for_mutation",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        match ResponseCode::from(response.code()) {
            ResponseCode::Success => {
                let body = response.get_body().ok_or_else(|| {
                    RocketMQError::response_process_failed(
                        "mutation_subscription_group_config_state",
                        "Subscription Group response body is missing",
                    )
                })?;
                let config = SubscriptionGroupConfig::decode(body.as_ref())?;
                let header = response
                    .decode_command_custom_header::<UpdateSubscriptionGroupConfigCasResponseHeader>()
                    .map_err(|error| {
                        RocketMQError::response_process_failed(
                            "mutation_subscription_group_config_state",
                            format!("Subscription Group response version is missing: {error}"),
                        )
                    })?;
                Ok(MutationSubscriptionGroupConfigState {
                    state: MutationExpectedState::Present {
                        version: header.subscription_group_version,
                    },
                    config: Some(client_group_config(&config)),
                })
            }
            ResponseCode::SubscriptionGroupNotExist => Ok(MutationSubscriptionGroupConfigState {
                state: MutationExpectedState::Absent,
                config: None,
            }),
            _ => Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            )),
        }
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn replace_subscription_group_config_if_state(
        &self,
        addr: &CheetahString,
        group: CheetahString,
        expected_state: MutationExpectedState,
        replacement: MutationSubscriptionGroupConfig,
        timeout_millis: u64,
    ) -> RocketMQResult<MutationStateCasOutcome> {
        if replacement.retry_queue_nums < 0
            || replacement.retry_max_times < -1
            || replacement.consume_timeout_minute <= 0
        {
            return Err(RocketMQError::illegal_argument(
                "supervised Subscription Group replacement is outside the closed bounds",
            ));
        }
        let request = self
            .create_request_command(
                RequestCode::UpdateSubscriptionGroupConfigStateCas,
                GetSubscriptionGroupConfigRequestHeader {
                    group,
                    rpc_request_header: None,
                },
            )
            .set_body(
                SupervisedSubscriptionGroupConfigCasRequestBody {
                    expected_state: wire_expected_state(expected_state),
                    replacement: SupervisedSubscriptionGroupConfig {
                        consume_enable: replacement.consume_enable,
                        consume_from_min_enable: replacement.consume_from_min_enable,
                        consume_broadcast_enable: replacement.consume_broadcast_enable,
                        consume_message_orderly: replacement.consume_message_orderly,
                        retry_queue_nums: replacement.retry_queue_nums,
                        retry_max_times: replacement.retry_max_times,
                        broker_id: replacement.broker_id,
                        which_broker_when_consume_slowly: replacement.which_broker_when_consume_slowly,
                        notify_consumer_ids_changed_enable: replacement.notify_consumer_ids_changed_enable,
                        group_sys_flag: replacement.group_sys_flag,
                        consume_timeout_minute: replacement.consume_timeout_minute,
                    },
                }
                .encode()?,
            );
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let outcome = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "replace_subscription_group_config_if_state",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "replace_subscription_group_config_if_state",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        state_cas_outcome_from_response(&response, expected_state)
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn get_broker_mutation_config_state(
        &self,
        addr: &CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<ClientBrokerMutationConfigState> {
        let request = self.create_remoting_command(RequestCode::GetBrokerMutationConfig);
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let outcome = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "get_broker_mutation_config_state",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "get_broker_mutation_config_state",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        if ResponseCode::from(response.code()) != ResponseCode::Success {
            return Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            ));
        }
        let body = response.get_body().ok_or_else(|| {
            RocketMQError::response_process_failed(
                "broker_mutation_config_state",
                "Broker mutation config response body is missing",
            )
        })?;
        let state = WireBrokerMutationConfigState::decode(body.as_ref())?;
        Ok(ClientBrokerMutationConfigState {
            generation: state.generation,
            auto_create_topic_enable: state.auto_create_topic_enable,
            auto_create_subscription_group: state.auto_create_subscription_group,
            broker_permission: state.broker_permission,
            default_topic_queue_nums: state.default_topic_queue_nums,
            message_index_enable: state.message_index_enable,
            trace_topic_enable: state.trace_topic_enable,
        })
    }

    #[cfg(feature = "admin-mutation")]
    #[allow(clippy::too_many_arguments, reason = "wire header has one exact queue precondition")]
    pub(crate) async fn reset_consumer_offset_if_current(
        &self,
        addr: &CheetahString,
        consumer_group: CheetahString,
        topic: CheetahString,
        queue_id: i32,
        expected_offset: i64,
        new_offset: i64,
        timeout_millis: u64,
    ) -> RocketMQResult<ConditionalConsumerOffsetOutcome> {
        if queue_id < 0 || expected_offset < -1 || new_offset < 0 {
            return Err(RocketMQError::illegal_argument(
                "conditional consumer offset fields are outside the closed bounds",
            ));
        }
        let request = self.create_request_command(
            RequestCode::UpdateConsumerOffsetConditional,
            UpdateConsumerOffsetConditionalHeader {
                consumer_group,
                topic,
                queue_id,
                expected_offset,
                new_offset,
            },
        );
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let outcome = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "reset_consumer_offset_if_current",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "reset_consumer_offset_if_current",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        conditional_offset_outcome_from_response(&response, expected_offset, new_offset)
    }

    #[cfg(feature = "admin-mutation")]
    pub(crate) async fn get_message_request_mode_for_mutation(
        &self,
        addr: &CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        timeout_millis: u64,
    ) -> RocketMQResult<Option<MutationMessageRequestMode>> {
        let request = self
            .create_remoting_command(RequestCode::GetMessageRequestMode)
            .set_body(
                GetMessageRequestModeRequestBody {
                    topic: topic.to_string(),
                    consumer_group: consumer_group.to_string(),
                }
                .encode()?,
            );
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let outcome = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "get_message_request_mode_for_mutation",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "get_message_request_mode_for_mutation",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        if ResponseCode::from(response.code()) != ResponseCode::Success {
            return Err(mq_client_err!(
                response.code(),
                response.remark().map_or_else(String::new, |remark| remark.to_string())
            ));
        }
        let body = response.get_body().ok_or_else(|| {
            RocketMQError::response_process_failed(
                "mutation_message_request_mode",
                "request-mode response body is missing",
            )
        })?;
        MessageRequestModeStateBody::decode(body.as_ref())?
            .current
            .map(client_request_mode)
            .transpose()
    }

    #[cfg(feature = "admin-mutation")]
    #[allow(
        clippy::too_many_arguments,
        reason = "wire operation has one exact target and precondition"
    )]
    pub(crate) async fn replace_message_request_mode_if_current(
        &self,
        addr: &CheetahString,
        topic: CheetahString,
        consumer_group: CheetahString,
        expected: MutationExpectedMessageRequestMode,
        replacement: MutationMessageRequestMode,
        timeout_millis: u64,
    ) -> RocketMQResult<MutationMessageRequestModeOutcome> {
        let expected_state = match expected {
            MutationExpectedMessageRequestMode::Absent => ExpectedMessageRequestMode::Absent,
            MutationExpectedMessageRequestMode::Present(value) => {
                let value = wire_request_mode(value)?;
                ExpectedMessageRequestMode::Present {
                    mode: value.mode,
                    pop_share_queue_num: value.pop_share_queue_num,
                }
            }
        };
        let request = self
            .create_remoting_command(RequestCode::SetMessageRequestModeCas)
            .set_body(
                SetMessageRequestModeCasRequestBody {
                    topic: topic.to_string(),
                    consumer_group: consumer_group.to_string(),
                    expected_state,
                    replacement: wire_request_mode(replacement)?,
                }
                .encode()?,
            );
        let broker_addr = mix_all::broker_vip_channel(self.client_config.vip_channel_enabled, addr.as_str());
        let outcome = self
            .remoting_client
            .invoke_request(Some(&broker_addr), request, timeout_millis)
            .await;
        let response = match outcome {
            Ok(OutboundRequestOutcome::Response(response)) => response,
            Ok(OutboundRequestOutcome::Rejected(rejection)) => {
                return Err(admin_request_error(
                    "replace_message_request_mode_if_current",
                    RetryInput::Rejected(rejection),
                ));
            }
            Ok(OutboundRequestOutcome::Contract(contract)) => {
                return Err(admin_request_error(
                    "replace_message_request_mode_if_current",
                    RetryInput::Contract(contract),
                ));
            }
            Err(error) => return Err(RocketMQError::Shared(error.into_shared_error())),
        };
        request_mode_cas_outcome_from_response(&response, expected, replacement)
    }
}
