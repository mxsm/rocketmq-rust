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
use crate::admin::ConditionalConsumerOffsetOutcome;
use crate::admin::MutationExpectedMessageRequestMode;
use crate::admin::MutationExpectedState;
use crate::admin::MutationMessageRequestMode;
use crate::admin::MutationMessageRequestModeOutcome;
use crate::admin::MutationPersistenceState as ClientMutationPersistenceState;
use crate::admin::MutationStateCasOutcome;
use crate::admin::MutationSubscriptionGroupConfig;
use crate::admin::MutationTopicConfig;
use crate::admin::MutationTopicMessageType;
use rocketmq_protocol::protocol::body::supervised_mutation::{
    ExpectedState, MessageRequestModeMutationResultBody, MutationPersistenceState, StateCasResultBody,
    SupervisedMessageRequestMode,
};
use rocketmq_protocol::protocol::header::query_consumer_offset_response_header::QueryConsumerOffsetResponseHeader;

#[cfg(feature = "admin-mutation")]
const MAX_SUPERVISED_CONSUME_STATS_BODY_BYTES: usize = 1024 * 1024;
#[cfg(feature = "admin-mutation")]
const MAX_SUPERVISED_CONSUME_STATS_ROWS: usize = 1_000;

#[cfg(feature = "admin-mutation")]
pub(super) fn bounded_consume_stats_from_response(
    response: &RemotingCommand,
) -> RocketMQResult<rocketmq_protocol::protocol::admin::consume_stats::ConsumeStats> {
    if ResponseCode::from(response.code()) != ResponseCode::Success {
        return Err(RocketMQError::response_process_failed(
            "supervised consume stats",
            "broker rejected the bounded consume-stats request",
        ));
    }
    let body = response.get_body().ok_or_else(|| {
        RocketMQError::response_process_failed(
            "supervised consume stats",
            "broker omitted the bounded consume-stats response body",
        )
    })?;
    if body.len() > MAX_SUPERVISED_CONSUME_STATS_BODY_BYTES {
        return Err(RocketMQError::response_process_failed(
            "supervised consume stats",
            "broker returned an oversized consume-stats response body",
        ));
    }
    let stats = rocketmq_protocol::protocol::admin::consume_stats::ConsumeStats::decode_strict(body.as_ref())?;
    if stats.offset_table.len() > MAX_SUPERVISED_CONSUME_STATS_ROWS {
        return Err(RocketMQError::response_process_failed(
            "supervised consume stats",
            "broker returned more than 1000 consume-stats rows",
        ));
    }
    Ok(stats)
}

#[cfg(feature = "admin-mutation")]
pub(super) fn supervised_consume_stats_from_response(
    response: &RemotingCommand,
    topic: &CheetahString,
    broker_name: &CheetahString,
    read_queue_nums: u32,
) -> RocketMQResult<rocketmq_protocol::protocol::admin::consume_stats::ConsumeStats> {
    let stats = bounded_consume_stats_from_response(response)?;
    if stats.offset_table.iter().any(|(queue, wrapper)| {
        queue.topic() != topic
            || queue.broker_name() != broker_name
            || queue.queue_id() < 0
            || queue.queue_id() >= read_queue_nums as i32
            || wrapper.get_consumer_offset() < -1
            || wrapper.get_broker_offset() < 0
            || wrapper.get_pull_offset() < -1
    }) {
        return Err(RocketMQError::response_process_failed(
            "supervised consume stats",
            "broker returned rows outside the exact Topic/Broker queue set",
        ));
    }
    let mut queue_ids = stats
        .offset_table
        .keys()
        .map(|queue| queue.queue_id())
        .collect::<Vec<_>>();
    queue_ids.sort_unstable();
    if queue_ids.len() != read_queue_nums as usize
        || queue_ids
            .iter()
            .enumerate()
            .any(|(expected, actual)| *actual != expected as i32)
    {
        return Err(RocketMQError::response_process_failed(
            "supervised consume stats",
            "broker returned an incomplete consume-stats queue set",
        ));
    }
    Ok(stats)
}

#[cfg(feature = "admin-mutation")]
pub(super) fn client_expected_state(state: ExpectedState) -> MutationExpectedState {
    match state {
        ExpectedState::Absent => MutationExpectedState::Absent,
        ExpectedState::Present { version } => MutationExpectedState::Present { version },
    }
}

#[cfg(feature = "admin-mutation")]
pub(super) fn wire_expected_state(state: MutationExpectedState) -> ExpectedState {
    match state {
        MutationExpectedState::Absent => ExpectedState::Absent,
        MutationExpectedState::Present { version } => ExpectedState::Present { version },
    }
}

#[cfg(feature = "admin-mutation")]
pub(super) fn client_topic_config(config: &TopicConfig) -> RocketMQResult<MutationTopicConfig> {
    use rocketmq_model::common::attribute::topic_message_type::TopicMessageType;

    let message_type = match config.get_topic_message_type() {
        TopicMessageType::Normal => MutationTopicMessageType::Normal,
        TopicMessageType::Fifo => MutationTopicMessageType::Fifo,
        TopicMessageType::Delay => MutationTopicMessageType::Delay,
        TopicMessageType::Transaction => MutationTopicMessageType::Transaction,
        TopicMessageType::Unspecified => MutationTopicMessageType::Unspecified,
        TopicMessageType::Priority | TopicMessageType::Lite | TopicMessageType::Mixed => {
            return Err(RocketMQError::response_process_failed(
                "mutation_topic_config_state",
                "Topic message type is outside the supervised replacement contract",
            ));
        }
    };
    Ok(MutationTopicConfig {
        read_queue_nums: config.read_queue_nums,
        write_queue_nums: config.write_queue_nums,
        perm: config.perm,
        order: config.order,
        message_type,
    })
}

#[cfg(feature = "admin-mutation")]
pub(super) fn client_group_config(config: &SubscriptionGroupConfig) -> MutationSubscriptionGroupConfig {
    MutationSubscriptionGroupConfig {
        consume_enable: config.consume_enable(),
        consume_from_min_enable: config.consume_from_min_enable(),
        consume_broadcast_enable: config.consume_broadcast_enable(),
        consume_message_orderly: config.consume_message_orderly(),
        retry_queue_nums: config.retry_queue_nums(),
        retry_max_times: config.retry_max_times(),
        broker_id: config.broker_id(),
        which_broker_when_consume_slowly: config.which_broker_when_consume_slowly(),
        notify_consumer_ids_changed_enable: config.notify_consumer_ids_changed_enable(),
        group_sys_flag: config.group_sys_flag(),
        consume_timeout_minute: config.consume_timeout_minute(),
    }
}

#[cfg(feature = "admin-mutation")]
pub(super) fn state_cas_outcome_from_response(
    response: &RemotingCommand,
    expected: MutationExpectedState,
) -> RocketMQResult<MutationStateCasOutcome> {
    let response_code = ResponseCode::from(response.code());
    if !matches!(
        response_code,
        ResponseCode::Success | ResponseCode::InvalidParameter | ResponseCode::SystemError
    ) {
        return Err(mq_client_err!(
            response.code(),
            response.remark().map_or_else(String::new, |remark| remark.to_string())
        ));
    }
    let body = response.get_body().ok_or_else(|| {
        RocketMQError::response_process_failed("replace_config_if_state", "state CAS response body is missing")
    })?;
    let outcome = StateCasResultBody::decode(body.as_ref())?;
    let matrix_valid = match response_code {
        ResponseCode::Success => {
            outcome.applied
                && ((outcome.changed && outcome.persistence == MutationPersistenceState::Persisted)
                    || (!outcome.changed && outcome.persistence == MutationPersistenceState::NotRequired))
        }
        ResponseCode::InvalidParameter => {
            !outcome.applied && !outcome.changed && outcome.persistence == MutationPersistenceState::NotRequired
        }
        ResponseCode::SystemError => {
            outcome.persistence == MutationPersistenceState::Failed
                && ((outcome.applied && outcome.changed) || (!outcome.applied && !outcome.changed))
        }
        _ => false,
    };
    let state_valid = match (response_code, outcome.changed, expected, outcome.state) {
        (
            ResponseCode::Success,
            false,
            MutationExpectedState::Present { version: expected },
            ExpectedState::Present { version: actual },
        ) => actual == expected,
        (
            ResponseCode::Success | ResponseCode::SystemError,
            true,
            MutationExpectedState::Absent,
            ExpectedState::Present { .. },
        ) => true,
        (
            ResponseCode::Success,
            true,
            MutationExpectedState::Present { version: expected },
            ExpectedState::Present { version: actual },
        ) => actual > expected,
        (
            ResponseCode::SystemError,
            true,
            MutationExpectedState::Present { version: expected },
            ExpectedState::Present { version: actual },
        ) => actual > expected,
        (
            ResponseCode::SystemError,
            false,
            MutationExpectedState::Present { version: expected },
            ExpectedState::Present { version: actual },
        ) => actual == expected,
        (ResponseCode::InvalidParameter, false, MutationExpectedState::Absent, ExpectedState::Present { .. }) => true,
        (ResponseCode::InvalidParameter, false, MutationExpectedState::Present { .. }, ExpectedState::Absent) => true,
        (
            ResponseCode::InvalidParameter,
            false,
            MutationExpectedState::Present { version: expected },
            ExpectedState::Present { version: actual },
        ) => actual != expected,
        _ => false,
    };
    if !matrix_valid || !state_valid {
        return Err(RocketMQError::response_process_failed(
            "replace_config_if_state",
            "state CAS response code and body disagree",
        ));
    }
    Ok(MutationStateCasOutcome {
        applied: outcome.applied,
        changed: outcome.changed,
        state: client_expected_state(outcome.state),
        persistence: match outcome.persistence {
            MutationPersistenceState::NotRequired => ClientMutationPersistenceState::NotRequired,
            MutationPersistenceState::Persisted => ClientMutationPersistenceState::Persisted,
            MutationPersistenceState::Failed => ClientMutationPersistenceState::Failed,
        },
    })
}

#[cfg(feature = "admin-mutation")]
pub(super) fn request_mode_cas_outcome_from_response(
    response: &RemotingCommand,
    expected: MutationExpectedMessageRequestMode,
    replacement: MutationMessageRequestMode,
) -> RocketMQResult<MutationMessageRequestModeOutcome> {
    let response_code = ResponseCode::from(response.code());
    if !matches!(
        response_code,
        ResponseCode::Success | ResponseCode::InvalidParameter | ResponseCode::SystemError
    ) {
        return Err(RocketMQError::response_process_failed(
            "replace_message_request_mode_if_current",
            "broker rejected the request-mode replacement",
        ));
    }
    let body = response.get_body().ok_or_else(|| {
        RocketMQError::response_process_failed(
            "replace_message_request_mode_if_current",
            "request-mode CAS response body is missing",
        )
    })?;
    let outcome = MessageRequestModeMutationResultBody::decode(body.as_ref())?;
    let current = outcome.current.map(client_request_mode).transpose()?;
    let matrix_valid = match response_code {
        ResponseCode::Success => {
            outcome.applied
                && ((outcome.changed && outcome.persistence == MutationPersistenceState::Persisted)
                    || (!outcome.changed && outcome.persistence == MutationPersistenceState::NotRequired))
        }
        ResponseCode::InvalidParameter => {
            !outcome.applied && !outcome.changed && outcome.persistence == MutationPersistenceState::NotRequired
        }
        ResponseCode::SystemError => {
            outcome.persistence == MutationPersistenceState::Failed
                && ((outcome.applied && outcome.changed) || (!outcome.applied && !outcome.changed))
        }
        _ => false,
    };
    let expected_current = match expected {
        MutationExpectedMessageRequestMode::Absent => None,
        MutationExpectedMessageRequestMode::Present(value) => Some(value),
    };
    let current_valid = match response_code {
        ResponseCode::Success => {
            current == Some(replacement) && (outcome.changed || expected_current == Some(replacement))
        }
        ResponseCode::SystemError if outcome.applied => current == Some(replacement) && outcome.changed,
        ResponseCode::SystemError => current == expected_current && !outcome.changed,
        ResponseCode::InvalidParameter => current != expected_current,
        _ => false,
    };
    if !matrix_valid || !current_valid {
        return Err(RocketMQError::response_process_failed(
            "replace_message_request_mode_if_current",
            "request-mode CAS response code and body disagree",
        ));
    }
    Ok(MutationMessageRequestModeOutcome {
        applied: outcome.applied,
        changed: outcome.changed,
        current,
        persistence: match outcome.persistence {
            MutationPersistenceState::NotRequired => ClientMutationPersistenceState::NotRequired,
            MutationPersistenceState::Persisted => ClientMutationPersistenceState::Persisted,
            MutationPersistenceState::Failed => ClientMutationPersistenceState::Failed,
        },
    })
}

#[cfg(feature = "admin-mutation")]
pub(super) fn conditional_offset_outcome_from_response(
    response: &RemotingCommand,
    expected_offset: i64,
    new_offset: i64,
) -> RocketMQResult<ConditionalConsumerOffsetOutcome> {
    if expected_offset < -1 || new_offset < 0 {
        return Err(RocketMQError::illegal_argument(
            "conditional consumer offset fields are outside the closed bounds",
        ));
    }
    let response_code = ResponseCode::from(response.code());
    if !matches!(response_code, ResponseCode::Success | ResponseCode::InvalidParameter) {
        return Err(RocketMQError::response_process_failed(
            "reset_consumer_offset_if_current",
            "conditional offset response code is invalid",
        ));
    }
    let header = response
        .decode_command_custom_header::<QueryConsumerOffsetResponseHeader>()
        .map_err(|_| {
            RocketMQError::response_process_failed(
                "reset_consumer_offset_if_current",
                "conditional offset response header is invalid",
            )
        })?;
    let actual_offset = header.offset.filter(|offset| *offset >= -1).ok_or_else(|| {
        RocketMQError::response_process_failed(
            "reset_consumer_offset_if_current",
            "conditional offset response omitted a valid actual offset",
        )
    })?;
    let applied = match response_code {
        ResponseCode::Success if actual_offset == new_offset => true,
        ResponseCode::InvalidParameter if actual_offset != expected_offset => false,
        _ => {
            return Err(RocketMQError::response_process_failed(
                "reset_consumer_offset_if_current",
                "conditional offset response code and header disagree",
            ));
        }
    };
    Ok(ConditionalConsumerOffsetOutcome { applied, actual_offset })
}

#[cfg(feature = "admin-mutation")]
pub(super) fn client_request_mode(value: SupervisedMessageRequestMode) -> RocketMQResult<MutationMessageRequestMode> {
    let mode = match value.mode.as_str() {
        "PULL" => rocketmq_model::common::message::message_enum::MessageRequestMode::Pull,
        "POP" => rocketmq_model::common::message::message_enum::MessageRequestMode::Pop,
        _ => {
            return Err(RocketMQError::response_process_failed(
                "mutation_message_request_mode",
                "Broker returned an unknown message request mode",
            ));
        }
    };
    if value.pop_share_queue_num < 0 {
        return Err(RocketMQError::response_process_failed(
            "mutation_message_request_mode",
            "Broker returned a negative popShareQueueNum",
        ));
    }
    Ok(MutationMessageRequestMode {
        mode,
        pop_share_queue_num: value.pop_share_queue_num,
    })
}

#[cfg(feature = "admin-mutation")]
pub(super) fn wire_request_mode(value: MutationMessageRequestMode) -> RocketMQResult<SupervisedMessageRequestMode> {
    if value.pop_share_queue_num < 0 {
        return Err(RocketMQError::illegal_argument("popShareQueueNum must be non-negative"));
    }
    Ok(SupervisedMessageRequestMode {
        mode: value.mode.get_name().to_owned(),
        pop_share_queue_num: value.pop_share_queue_num,
    })
}

#[cfg(test)]
#[path = "supervised_mutation_decode_tests.rs"]
mod tests;
