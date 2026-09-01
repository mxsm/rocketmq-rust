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

#[cfg(feature = "admin-mutation")]
#[test]
fn state_cas_response_binds_status_to_typed_body() {
    let applied = RemotingCommand::create_success_response_command().set_body(
        StateCasResultBody {
            applied: true,
            changed: true,
            state: ExpectedState::Present { version: 10 },
            persistence: MutationPersistenceState::Persisted,
        }
        .encode()
        .expect("applied body"),
    );
    assert_eq!(
        state_cas_outcome_from_response(&applied, MutationExpectedState::Present { version: 9 })
            .expect("applied outcome"),
        MutationStateCasOutcome {
            applied: true,
            changed: true,
            state: MutationExpectedState::Present { version: 10 },
            persistence: ClientMutationPersistenceState::Persisted,
        }
    );

    let conflict = RemotingCommand::create_response_command_with_code(ResponseCode::InvalidParameter).set_body(
        StateCasResultBody {
            applied: false,
            changed: false,
            state: ExpectedState::Absent,
            persistence: MutationPersistenceState::NotRequired,
        }
        .encode()
        .expect("conflict body"),
    );
    assert_eq!(
        state_cas_outcome_from_response(&conflict, MutationExpectedState::Present { version: 10 })
            .expect("conflict outcome"),
        MutationStateCasOutcome {
            applied: false,
            changed: false,
            state: MutationExpectedState::Absent,
            persistence: ClientMutationPersistenceState::NotRequired,
        }
    );

    let persistence_failed = RemotingCommand::create_response_command_with_code(ResponseCode::SystemError).set_body(
        StateCasResultBody {
            applied: true,
            changed: true,
            state: ExpectedState::Present { version: 11 },
            persistence: MutationPersistenceState::Failed,
        }
        .encode()
        .expect("persistence failure body"),
    );
    assert_eq!(
        state_cas_outcome_from_response(&persistence_failed, MutationExpectedState::Present { version: 10 },)
            .expect("applied failure outcome"),
        MutationStateCasOutcome {
            applied: true,
            changed: true,
            state: MutationExpectedState::Present { version: 11 },
            persistence: ClientMutationPersistenceState::Failed,
        }
    );

    let disagree = RemotingCommand::create_success_response_command().set_body(
        StateCasResultBody {
            applied: false,
            changed: false,
            state: ExpectedState::Absent,
            persistence: MutationPersistenceState::NotRequired,
        }
        .encode()
        .expect("mismatched body"),
    );
    assert!(state_cas_outcome_from_response(&disagree, MutationExpectedState::Absent).is_err());
}

#[cfg(feature = "admin-mutation")]
#[test]
fn state_cas_response_accepts_only_the_closed_code_body_matrix() {
    for code in [
        ResponseCode::Success,
        ResponseCode::InvalidParameter,
        ResponseCode::SystemError,
    ] {
        for applied in [false, true] {
            for changed in [false, true] {
                for persistence in [
                    MutationPersistenceState::NotRequired,
                    MutationPersistenceState::Persisted,
                    MutationPersistenceState::Failed,
                ] {
                    let (expected, state) = match code {
                        ResponseCode::Success if !changed => (
                            MutationExpectedState::Present { version: 9 },
                            ExpectedState::Present { version: 9 },
                        ),
                        ResponseCode::SystemError if !applied && !changed => (
                            MutationExpectedState::Present { version: 9 },
                            ExpectedState::Present { version: 9 },
                        ),
                        ResponseCode::Success | ResponseCode::SystemError => (
                            MutationExpectedState::Present { version: 9 },
                            ExpectedState::Present { version: 10 },
                        ),
                        ResponseCode::InvalidParameter => {
                            (MutationExpectedState::Present { version: 9 }, ExpectedState::Absent)
                        }
                        _ => unreachable!(),
                    };
                    let command = RemotingCommand::create_response_command_with_code(code).set_body(
                        StateCasResultBody {
                            applied,
                            changed,
                            state,
                            persistence,
                        }
                        .encode()
                        .expect("matrix body"),
                    );
                    let accepted = state_cas_outcome_from_response(&command, expected).is_ok();
                    let should_accept = match code {
                        ResponseCode::Success => {
                            applied
                                && ((changed && persistence == MutationPersistenceState::Persisted)
                                    || (!changed && persistence == MutationPersistenceState::NotRequired))
                        }
                        ResponseCode::InvalidParameter => {
                            !applied && !changed && persistence == MutationPersistenceState::NotRequired
                        }
                        ResponseCode::SystemError => {
                            persistence == MutationPersistenceState::Failed
                                && ((applied && changed) || (!applied && !changed))
                        }
                        _ => false,
                    };
                    assert_eq!(accepted, should_accept, "{code:?}/{applied}/{changed}/{persistence:?}");
                }
            }
        }
    }
}

#[cfg(feature = "admin-mutation")]
#[test]
fn request_mode_cas_response_retains_applied_persistence_failure() {
    let current = SupervisedMessageRequestMode {
        mode: "POP".to_owned(),
        pop_share_queue_num: 4,
    };
    let failed = RemotingCommand::create_response_command_with_code(ResponseCode::SystemError).set_body(
        MessageRequestModeMutationResultBody {
            applied: true,
            changed: true,
            current: Some(current),
            persistence: MutationPersistenceState::Failed,
        }
        .encode()
        .expect("failure body"),
    );
    let replacement = MutationMessageRequestMode {
        mode: rocketmq_model::common::message::message_enum::MessageRequestMode::Pop,
        pop_share_queue_num: 4,
    };
    let outcome =
        request_mode_cas_outcome_from_response(&failed, MutationExpectedMessageRequestMode::Absent, replacement)
            .expect("applied failure");
    assert!(outcome.applied);
    assert!(outcome.changed);
    assert_eq!(outcome.persistence, ClientMutationPersistenceState::Failed);
    assert_eq!(outcome.current.expect("current").pop_share_queue_num, 4);

    let disagree = RemotingCommand::create_success_response_command().set_body(
        MessageRequestModeMutationResultBody {
            applied: true,
            changed: true,
            current: None,
            persistence: MutationPersistenceState::Failed,
        }
        .encode()
        .expect("body"),
    );
    assert!(
        request_mode_cas_outcome_from_response(&disagree, MutationExpectedMessageRequestMode::Absent, replacement,)
            .is_err()
    );
}

#[cfg(feature = "admin-mutation")]
#[test]
fn request_mode_response_accepts_only_closed_matrix_and_exact_current() {
    let pull = MutationMessageRequestMode {
        mode: rocketmq_model::common::message::message_enum::MessageRequestMode::Pull,
        pop_share_queue_num: 0,
    };
    let pop = MutationMessageRequestMode {
        mode: rocketmq_model::common::message::message_enum::MessageRequestMode::Pop,
        pop_share_queue_num: 4,
    };
    let wire_pop = SupervisedMessageRequestMode {
        mode: "POP".to_owned(),
        pop_share_queue_num: 4,
    };
    let wire_pull = SupervisedMessageRequestMode {
        mode: "PULL".to_owned(),
        pop_share_queue_num: 0,
    };
    for code in [
        ResponseCode::Success,
        ResponseCode::InvalidParameter,
        ResponseCode::SystemError,
    ] {
        for applied in [false, true] {
            for changed in [false, true] {
                for persistence in [
                    MutationPersistenceState::NotRequired,
                    MutationPersistenceState::Persisted,
                    MutationPersistenceState::Failed,
                ] {
                    let expected = if code == ResponseCode::Success && !changed {
                        MutationExpectedMessageRequestMode::Present(pop)
                    } else {
                        MutationExpectedMessageRequestMode::Present(pull)
                    };
                    let current = if code == ResponseCode::InvalidParameter {
                        None
                    } else if code == ResponseCode::SystemError && !applied && !changed {
                        Some(wire_pull.clone())
                    } else {
                        Some(wire_pop.clone())
                    };
                    let command = RemotingCommand::create_response_command_with_code(code).set_body(
                        MessageRequestModeMutationResultBody {
                            applied,
                            changed,
                            current,
                            persistence,
                        }
                        .encode()
                        .expect("matrix body"),
                    );
                    let accepted = request_mode_cas_outcome_from_response(&command, expected, pop).is_ok();
                    let should_accept = match code {
                        ResponseCode::Success => {
                            applied
                                && ((changed && persistence == MutationPersistenceState::Persisted)
                                    || (!changed && persistence == MutationPersistenceState::NotRequired))
                        }
                        ResponseCode::InvalidParameter => {
                            !applied && !changed && persistence == MutationPersistenceState::NotRequired
                        }
                        ResponseCode::SystemError => {
                            persistence == MutationPersistenceState::Failed
                                && ((applied && changed) || (!applied && !changed))
                        }
                        _ => false,
                    };
                    assert_eq!(accepted, should_accept, "{code:?}/{applied}/{changed}/{persistence:?}");
                }
            }
        }
    }
}

#[cfg(feature = "admin-mutation")]
#[test]
fn conditional_offset_response_accepts_only_success_or_conflict_relations() {
    fn response(code: ResponseCode, actual: Option<i64>) -> RemotingCommand {
        let mut response = RemotingCommand::create_response_command_with_code(code);
        if let Some(actual) = actual {
            response = response.set_command_custom_header(QueryConsumerOffsetResponseHeader { offset: Some(actual) });
            response.make_custom_header_to_net();
        }
        response
    }

    let applied = conditional_offset_outcome_from_response(&response(ResponseCode::Success, Some(3)), 7, 3)
        .expect("exact success");
    assert!(applied.applied);
    assert_eq!(applied.actual_offset, 3);

    let conflict = conditional_offset_outcome_from_response(&response(ResponseCode::InvalidParameter, Some(8)), 7, 3)
        .expect("exact conflict");
    assert!(!conflict.applied);
    assert_eq!(conflict.actual_offset, 8);

    for invalid in [
        response(ResponseCode::Success, Some(2)),
        response(ResponseCode::InvalidParameter, Some(7)),
        response(ResponseCode::SystemError, Some(3)),
        response(ResponseCode::NoPermission, Some(3)),
        response(ResponseCode::Success, None),
        response(ResponseCode::InvalidParameter, Some(-2)),
    ] {
        assert!(conditional_offset_outcome_from_response(&invalid, 7, 3).is_err());
    }
    assert!(conditional_offset_outcome_from_response(&response(ResponseCode::Success, Some(3)), -2, 3).is_err());
    assert!(conditional_offset_outcome_from_response(&response(ResponseCode::Success, Some(3)), 7, -1).is_err());
}

#[cfg(feature = "admin-mutation")]
#[test]
fn supervised_consume_stats_decoder_is_bounded_and_fail_closed() {
    use rocketmq_model::message::MessageQueue;
    use rocketmq_protocol::protocol::admin::consume_stats::ConsumeStats;
    use rocketmq_protocol::protocol::admin::offset_wrapper::OffsetWrapper;

    let topic = CheetahString::from_static_str("orders");
    let broker = CheetahString::from_static_str("broker-a");

    let missing = RemotingCommand::create_success_response_command();
    assert!(supervised_consume_stats_from_response(&missing, &topic, &broker, 1).is_err());

    let rejected = RemotingCommand::create_response_command_with_code(ResponseCode::SystemError)
        .set_body(ConsumeStats::new().encode_java_compatible().expect("body"));
    assert!(supervised_consume_stats_from_response(&rejected, &topic, &broker, 1).is_err());

    let oversized =
        RemotingCommand::create_success_response_command()
            .set_body(vec![b' '; MAX_SUPERVISED_CONSUME_STATS_BODY_BYTES + 1]);
    assert!(supervised_consume_stats_from_response(&oversized, &topic, &broker, 1).is_err());

    let mut too_many_rows = ConsumeStats::new();
    for queue_id in 0..=MAX_SUPERVISED_CONSUME_STATS_ROWS {
        too_many_rows.offset_table.insert(
            MessageQueue::from_parts("orders", "broker-a", queue_id as i32),
            OffsetWrapper::new(),
        );
    }
    let response = RemotingCommand::create_success_response_command()
        .set_body(too_many_rows.encode_java_compatible().expect("bounded body"));
    assert!(response.get_body().expect("body").len() <= MAX_SUPERVISED_CONSUME_STATS_BODY_BYTES);
    assert!(supervised_consume_stats_from_response(
        &response,
        &topic,
        &broker,
        MAX_SUPERVISED_CONSUME_STATS_ROWS as u32 + 1,
    )
    .is_err());

    let empty = RemotingCommand::create_success_response_command()
        .set_body(ConsumeStats::new().encode_java_compatible().expect("body"));
    assert!(supervised_consume_stats_from_response(&empty, &topic, &broker, 1).is_err());

    let mut valid_stats = ConsumeStats::new();
    valid_stats
        .offset_table
        .insert(MessageQueue::from_parts("orders", "broker-a", 0), OffsetWrapper::new());
    let valid = RemotingCommand::create_success_response_command()
        .set_body(valid_stats.encode_java_compatible().expect("body"));
    assert!(supervised_consume_stats_from_response(&valid, &topic, &broker, 1).is_ok());

    for rows in [
        vec![MessageQueue::from_parts("wrong-topic", "broker-a", 0)],
        vec![
            MessageQueue::from_parts("orders", "broker-a", 0),
            MessageQueue::from_parts("orders", "wrong-broker", 0),
        ],
        vec![MessageQueue::from_parts("orders", "broker-a", 1)],
    ] {
        let mut corrupt = ConsumeStats::new();
        for queue in rows {
            corrupt.offset_table.insert(queue, OffsetWrapper::new());
        }
        let response = RemotingCommand::create_success_response_command()
            .set_body(corrupt.encode_java_compatible().expect("corrupt body"));
        assert!(supervised_consume_stats_from_response(&response, &topic, &broker, 2).is_err());
    }
}

#[cfg(feature = "admin-mutation")]
#[test]
fn supervised_consume_stats_header_binds_exact_topic() {
    use rocketmq_protocol::protocol::CommandCustomHeader;

    let fields = GetConsumeStatsRequestHeader {
        consumer_group: "orders-consumer".into(),
        topic: "orders".into(),
        topic_list: None,
        topic_request_header: None,
    }
    .to_map()
    .expect("header fields");

    assert_eq!(
        fields.get("consumerGroup").map(CheetahString::as_str),
        Some("orders-consumer")
    );
    assert_eq!(fields.get("topic").map(CheetahString::as_str), Some("orders"));
}

#[cfg(feature = "admin-mutation")]
#[test]
fn request_mode_decoder_rejects_nonclosed_values() {
    for value in [
        SupervisedMessageRequestMode {
            mode: "push".to_owned(),
            pop_share_queue_num: 0,
        },
        SupervisedMessageRequestMode {
            mode: "POP".to_owned(),
            pop_share_queue_num: -1,
        },
    ] {
        assert!(client_request_mode(value).is_err());
    }
    assert_eq!(
        client_request_mode(SupervisedMessageRequestMode {
            mode: "POP".to_owned(),
            pop_share_queue_num: 4,
        })
        .expect("closed mode"),
        MutationMessageRequestMode {
            mode: rocketmq_model::common::message::message_enum::MessageRequestMode::Pop,
            pop_share_queue_num: 4,
        }
    );
}
