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

use std::collections::HashSet;
use std::time::Duration;

use rand::RngExt;
use rocketmq_error::ErrorDescriptor;
use rocketmq_error::RecoveryHint;
use rocketmq_error::RocketMQError;
use rocketmq_model::result::SendStatus;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_transport::api::OutboundRequestContract;
use rocketmq_transport::api::OutboundRequestRejection;
use rocketmq_transport::api::OutboundRequestRejectionReason;
use rocketmq_transport::api::OutboundRequestStage;
use rocketmq_transport::api::TransportError;

const INITIAL_BACKOFF_MILLIS: u64 = 50;
const MAX_RETRY_DELAY_MILLIS: u64 = 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetryOperation {
    ProducerSend,
    ProducerRouteRefresh,
    AssignmentQuery,
    CreateTopic,
    NameServerKv,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetryIdempotency {
    NonIdempotent,
    Idempotent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetryAction {
    Stop,
    RetryNow,
    RetryAfter(Duration),
    RefreshRoute,
    RefreshLeader,
    SwitchBroker,
}

impl RetryAction {
    #[inline]
    const fn delay(self) -> Option<Duration> {
        match self {
            Self::RetryAfter(delay) => Some(delay),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RetryContext<'a> {
    pub(crate) operation: RetryOperation,
    pub(crate) idempotency: RetryIdempotency,
    /// One-based attempt that just completed.
    pub(crate) attempt: u32,
    pub(crate) max_attempts: u32,
    pub(crate) remaining: Duration,
    pub(crate) producer_retry_response_codes: Option<&'a HashSet<i32>>,
    pub(crate) retry_not_store_ok: bool,
}

#[derive(Debug)]
pub(crate) enum RetryInput {
    Transport(TransportError),
    Rejected(OutboundRequestRejection),
    Contract(OutboundRequestContract),
    Response {
        code: i32,
        retry_after: Option<Duration>,
        terminal_error: RocketMQError,
    },
    SendStatus(SendStatus),
    RouteUnavailable,
    BusinessError(RocketMQError),
}

impl From<RocketMQError> for RetryInput {
    fn from(error: RocketMQError) -> Self {
        Self::BusinessError(error)
    }
}

#[derive(Clone, Copy)]
enum RetryFacts<'a> {
    Operational {
        descriptor: &'static ErrorDescriptor,
        stage: Option<OutboundRequestStage>,
    },
    Rejected {
        reason: OutboundRequestRejectionReason,
        stage: OutboundRequestStage,
    },
    Contract,
    Response {
        code: i32,
        retry_after: Option<Duration>,
    },
    SendStatus(SendStatus),
    RouteUnavailable,
    Business {
        error: &'a RocketMQError,
    },
}

pub(crate) struct RetryPolicy;

impl RetryPolicy {
    pub(crate) fn decide(context: RetryContext<'_>, input: &RetryInput) -> RetryAction {
        Self::decide_with_jitter(context, Self::facts(input), |upper_bound| {
            if upper_bound.is_zero() {
                return Duration::ZERO;
            }
            let upper_millis = u64::try_from(upper_bound.as_millis()).unwrap_or(u64::MAX);
            Duration::from_millis(rand::rng().random_range(0..=upper_millis))
        })
    }

    fn facts(input: &RetryInput) -> RetryFacts<'_> {
        match input {
            RetryInput::Transport(error) => RetryFacts::Operational {
                descriptor: error.shared_error().descriptor(),
                stage: error.request_stage(),
            },
            RetryInput::Rejected(rejection) => RetryFacts::Rejected {
                reason: rejection.reason(),
                stage: rejection.stage(),
            },
            RetryInput::Contract(_) => RetryFacts::Contract,
            RetryInput::Response { code, retry_after, .. } => RetryFacts::Response {
                code: *code,
                retry_after: *retry_after,
            },
            RetryInput::SendStatus(status) => RetryFacts::SendStatus(*status),
            RetryInput::RouteUnavailable => RetryFacts::RouteUnavailable,
            RetryInput::BusinessError(error) => RetryFacts::Business { error },
        }
    }

    fn decide_with_jitter(
        context: RetryContext<'_>,
        facts: RetryFacts<'_>,
        sample_jitter: impl FnOnce(Duration) -> Duration,
    ) -> RetryAction {
        if context.attempt >= context.max_attempts || context.remaining.is_zero() {
            return RetryAction::Stop;
        }

        let requested = match facts {
            RetryFacts::Contract => RetryAction::Stop,
            RetryFacts::Rejected { reason, stage } => {
                if !Self::stage_allows_retry(context.idempotency, stage) {
                    return RetryAction::Stop;
                }
                match reason {
                    OutboundRequestRejectionReason::QueueSaturated => Self::backoff(context.attempt, sample_jitter),
                    OutboundRequestRejectionReason::EndpointUnavailable => match context.operation {
                        RetryOperation::ProducerSend | RetryOperation::NameServerKv => RetryAction::SwitchBroker,
                        RetryOperation::AssignmentQuery => RetryAction::RefreshRoute,
                        RetryOperation::CreateTopic | RetryOperation::ProducerRouteRefresh => {
                            Self::backoff(context.attempt, sample_jitter)
                        }
                    },
                    OutboundRequestRejectionReason::DeadlineExpired
                    | OutboundRequestRejectionReason::Cancelled
                    | OutboundRequestRejectionReason::ClientStopping
                    | OutboundRequestRejectionReason::SessionClosed => RetryAction::Stop,
                }
            }
            RetryFacts::Operational { descriptor, stage } => {
                let Some(stage) = stage else {
                    return RetryAction::Stop;
                };
                if !Self::stage_allows_retry(context.idempotency, stage) {
                    return RetryAction::Stop;
                }
                Self::from_recovery_hint(
                    context.operation,
                    descriptor.recovery_hint(),
                    context.attempt,
                    sample_jitter,
                )
            }
            RetryFacts::Response { code, retry_after } => {
                if code == ResponseCode::GoAway.to_i32() {
                    if context.idempotency == RetryIdempotency::Idempotent {
                        RetryAction::RetryNow
                    } else {
                        RetryAction::Stop
                    }
                } else if let Some(delay) = retry_after {
                    Self::trusted_retry_after(delay)
                } else if context.operation == RetryOperation::ProducerSend {
                    if context
                        .producer_retry_response_codes
                        .is_some_and(|codes| codes.contains(&code))
                    {
                        RetryAction::SwitchBroker
                    } else {
                        RetryAction::Stop
                    }
                } else {
                    match context.operation {
                        RetryOperation::AssignmentQuery => RetryAction::RefreshRoute,
                        RetryOperation::CreateTopic => RetryAction::RetryNow,
                        RetryOperation::NameServerKv => RetryAction::SwitchBroker,
                        RetryOperation::ProducerSend | RetryOperation::ProducerRouteRefresh => RetryAction::Stop,
                    }
                }
            }
            RetryFacts::SendStatus(status) => {
                if context.operation == RetryOperation::ProducerSend
                    && status != SendStatus::SendOk
                    && context.retry_not_store_ok
                {
                    RetryAction::SwitchBroker
                } else {
                    RetryAction::Stop
                }
            }
            RetryFacts::RouteUnavailable => match context.operation {
                RetryOperation::ProducerSend | RetryOperation::AssignmentQuery => RetryAction::RefreshRoute,
                RetryOperation::ProducerRouteRefresh | RetryOperation::CreateTopic | RetryOperation::NameServerKv => {
                    RetryAction::Stop
                }
            },
            RetryFacts::Business { error } => {
                if context.operation == RetryOperation::ProducerSend {
                    // Producer broker responses are a separate typed input. A business error has
                    // no physical delivery evidence and cannot authorize a non-idempotent retry.
                    RetryAction::Stop
                } else {
                    Self::from_recovery_hint(
                        context.operation,
                        error.descriptor().recovery_hint(),
                        context.attempt,
                        sample_jitter,
                    )
                }
            }
        };

        Self::fit_within_deadline(requested, context.remaining)
    }

    #[inline]
    fn stage_allows_retry(idempotency: RetryIdempotency, stage: OutboundRequestStage) -> bool {
        idempotency == RetryIdempotency::Idempotent || matches!(stage, OutboundRequestStage::BeforeWrite)
    }

    fn from_recovery_hint(
        operation: RetryOperation,
        hint: RecoveryHint,
        attempt: u32,
        sample_jitter: impl FnOnce(Duration) -> Duration,
    ) -> RetryAction {
        match hint {
            RecoveryHint::Backoff => Self::backoff(attempt, sample_jitter),
            RecoveryHint::RefreshRoute
                if matches!(
                    operation,
                    RetryOperation::ProducerSend | RetryOperation::AssignmentQuery
                ) =>
            {
                RetryAction::RefreshRoute
            }
            RecoveryHint::RefreshLeader if operation == RetryOperation::AssignmentQuery => RetryAction::RefreshLeader,
            RecoveryHint::SwitchBroker
                if matches!(operation, RetryOperation::ProducerSend | RetryOperation::NameServerKv) =>
            {
                RetryAction::SwitchBroker
            }
            RecoveryHint::RefreshRoute | RecoveryHint::RefreshLeader | RecoveryHint::SwitchBroker => RetryAction::Stop,
            RecoveryHint::Never | RecoveryHint::RefreshCredentials | RecoveryHint::OperatorAction => RetryAction::Stop,
        }
    }

    fn backoff(attempt: u32, sample_jitter: impl FnOnce(Duration) -> Duration) -> RetryAction {
        let shift = attempt.saturating_sub(1).min(31);
        let upper_millis = INITIAL_BACKOFF_MILLIS
            .saturating_mul(1_u64 << shift)
            .min(MAX_RETRY_DELAY_MILLIS);
        let upper_bound = Duration::from_millis(upper_millis);
        RetryAction::RetryAfter(sample_jitter(upper_bound).min(upper_bound))
    }

    #[inline]
    fn trusted_retry_after(delay: Duration) -> RetryAction {
        RetryAction::RetryAfter(delay)
    }

    #[inline]
    fn fit_within_deadline(action: RetryAction, remaining: Duration) -> RetryAction {
        match action.delay() {
            Some(delay) if delay >= remaining => RetryAction::Stop,
            _ => action,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProducerSendFaultDecision {
    pub(crate) isolation: bool,
    pub(crate) reachable: bool,
    pub(crate) log_resend_immediately: bool,
}

pub(crate) fn producer_send_fault_decision(
    input: &RetryInput,
    detector_enabled: bool,
) -> Option<ProducerSendFaultDecision> {
    match input {
        RetryInput::BusinessError(RocketMQError::IllegalArgument(_)) => Some(ProducerSendFaultDecision {
            isolation: false,
            reachable: true,
            log_resend_immediately: true,
        }),
        RetryInput::BusinessError(RocketMQError::BrokerOperationFailed { .. }) => Some(ProducerSendFaultDecision {
            isolation: true,
            reachable: false,
            log_resend_immediately: false,
        }),
        RetryInput::Response { .. } => Some(ProducerSendFaultDecision {
            isolation: true,
            reachable: false,
            log_resend_immediately: false,
        }),
        RetryInput::Transport(error) if is_remote_transport_failure(error.descriptor()) => {
            Some(ProducerSendFaultDecision {
                isolation: true,
                reachable: !detector_enabled,
                log_resend_immediately: false,
            })
        }
        _ => None,
    }
}

fn is_remote_transport_failure(descriptor: &'static ErrorDescriptor) -> bool {
    [
        &rocketmq_error::TRANSPORT_DNS_FAILED,
        &rocketmq_error::TRANSPORT_CONNECTION_FAILED,
        &rocketmq_error::TRANSPORT_CONNECTION_TIMEOUT,
        &rocketmq_error::TRANSPORT_REMOTE_RATE_LIMITED,
        &rocketmq_error::TRANSPORT_WRITE_TIMEOUT,
        &rocketmq_error::TRANSPORT_RESPONSE_TIMEOUT,
    ]
    .into_iter()
    .any(|candidate| candidate.code() == descriptor.code())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use super::*;

    fn context(
        operation: RetryOperation,
        idempotency: RetryIdempotency,
        attempt: u32,
        max_attempts: u32,
    ) -> RetryContext<'static> {
        RetryContext {
            operation,
            idempotency,
            attempt,
            max_attempts,
            remaining: Duration::from_secs(2),
            producer_retry_response_codes: None,
            retry_not_store_ok: false,
        }
    }

    #[test]
    fn non_idempotent_progress_stops_after_writing_but_not_before_write() {
        let context = context(RetryOperation::ProducerSend, RetryIdempotency::NonIdempotent, 1, 3);
        let retry = RetryPolicy::decide_with_jitter(
            context,
            RetryFacts::Operational {
                descriptor: &rocketmq_error::TRANSPORT_CONNECTION_FAILED,
                stage: Some(OutboundRequestStage::BeforeWrite),
            },
            |_| Duration::from_millis(7),
        );
        assert_eq!(retry, RetryAction::RetryAfter(Duration::from_millis(7)));

        for stage in [
            OutboundRequestStage::Writing,
            OutboundRequestStage::AwaitingResponse,
            OutboundRequestStage::ResponseReceived,
        ] {
            let action = RetryPolicy::decide_with_jitter(
                context,
                RetryFacts::Operational {
                    descriptor: &rocketmq_error::TRANSPORT_CONNECTION_FAILED,
                    stage: Some(stage),
                },
                |_| Duration::ZERO,
            );
            assert_eq!(action, RetryAction::Stop);
        }
    }

    #[test]
    fn idempotent_operation_can_retry_same_canonical_failure_at_each_stage() {
        let context = context(RetryOperation::AssignmentQuery, RetryIdempotency::Idempotent, 1, 3);
        for stage in [
            OutboundRequestStage::BeforeWrite,
            OutboundRequestStage::Writing,
            OutboundRequestStage::AwaitingResponse,
            OutboundRequestStage::ResponseReceived,
        ] {
            let action = RetryPolicy::decide_with_jitter(
                context,
                RetryFacts::Operational {
                    descriptor: &rocketmq_error::TRANSPORT_CONNECTION_FAILED,
                    stage: Some(stage),
                },
                |_| Duration::from_millis(11),
            );
            assert_eq!(action, RetryAction::RetryAfter(Duration::from_millis(11)));
        }
    }

    #[test]
    fn non_request_operational_error_never_defaults_to_before_write() {
        let action = RetryPolicy::decide_with_jitter(
            context(RetryOperation::AssignmentQuery, RetryIdempotency::Idempotent, 1, 3),
            RetryFacts::Operational {
                descriptor: &rocketmq_error::TRANSPORT_CONNECTION_FAILED,
                stage: None,
            },
            |_| Duration::ZERO,
        );
        assert_eq!(action, RetryAction::Stop);
    }

    #[test]
    fn full_jitter_is_bounded_and_delay_must_fit_deadline() {
        let base = context(RetryOperation::AssignmentQuery, RetryIdempotency::Idempotent, 2, 4);
        let facts = RetryFacts::Operational {
            descriptor: &rocketmq_error::TRANSPORT_CONNECTION_FAILED,
            stage: Some(OutboundRequestStage::BeforeWrite),
        };
        assert_eq!(
            RetryPolicy::decide_with_jitter(base, facts, |_| Duration::ZERO),
            RetryAction::RetryAfter(Duration::ZERO)
        );
        assert_eq!(
            RetryPolicy::decide_with_jitter(base, facts, |upper| upper),
            RetryAction::RetryAfter(Duration::from_millis(100))
        );

        let mut short = base;
        short.remaining = Duration::from_millis(100);
        assert_eq!(
            RetryPolicy::decide_with_jitter(short, facts, |upper| upper),
            RetryAction::Stop
        );
    }

    #[test]
    fn trusted_retry_after_is_not_capped_by_generated_jitter_bound() {
        let response = RetryFacts::Response {
            code: ResponseCode::SystemBusy.to_i32(),
            retry_after: Some(Duration::from_millis(1_500)),
        };
        let mut enough = context(RetryOperation::NameServerKv, RetryIdempotency::Idempotent, 1, 3);
        enough.remaining = Duration::from_secs(2);
        assert_eq!(
            RetryPolicy::decide_with_jitter(enough, response, |_| Duration::ZERO),
            RetryAction::RetryAfter(Duration::from_millis(1_500))
        );

        let mut short = enough;
        short.remaining = Duration::from_millis(1_500);
        assert_eq!(
            RetryPolicy::decide_with_jitter(short, response, |_| Duration::ZERO),
            RetryAction::Stop
        );
    }

    #[test]
    fn exhausted_attempt_budget_stops_before_sampling_or_action() {
        let mut sampled = false;
        let action = RetryPolicy::decide_with_jitter(
            context(RetryOperation::AssignmentQuery, RetryIdempotency::Idempotent, 3, 3),
            RetryFacts::Operational {
                descriptor: &rocketmq_error::TRANSPORT_CONNECTION_FAILED,
                stage: Some(OutboundRequestStage::BeforeWrite),
            },
            |_| {
                sampled = true;
                Duration::ZERO
            },
        );
        assert_eq!(action, RetryAction::Stop);
        assert!(!sampled);
    }

    #[test]
    fn producer_response_allowlist_is_input_to_the_only_policy() {
        let codes = HashSet::from([ResponseCode::SystemBusy.to_i32()]);
        let mut context = context(RetryOperation::ProducerSend, RetryIdempotency::NonIdempotent, 1, 3);
        context.producer_retry_response_codes = Some(&codes);
        assert_eq!(
            RetryPolicy::decide_with_jitter(
                context,
                RetryFacts::Response {
                    code: ResponseCode::SystemBusy.to_i32(),
                    retry_after: None,
                },
                |_| Duration::ZERO,
            ),
            RetryAction::SwitchBroker
        );
    }

    #[test]
    fn typed_producer_response_retains_broker_fault_isolation() {
        let input = RetryInput::Response {
            code: ResponseCode::SystemBusy.to_i32(),
            retry_after: None,
            terminal_error: RocketMQError::broker_operation_failed(
                "SEND_MESSAGE",
                ResponseCode::SystemBusy.to_i32(),
                "busy",
            ),
        };

        assert_eq!(
            producer_send_fault_decision(&input, true),
            Some(ProducerSendFaultDecision {
                isolation: true,
                reachable: false,
                log_resend_immediately: false,
            })
        );
    }

    #[test]
    fn go_away_has_no_hidden_non_idempotent_replay() {
        let response = RetryFacts::Response {
            code: ResponseCode::GoAway.to_i32(),
            retry_after: None,
        };
        assert_eq!(
            RetryPolicy::decide_with_jitter(
                context(RetryOperation::ProducerSend, RetryIdempotency::NonIdempotent, 1, 3),
                response,
                |_| Duration::ZERO,
            ),
            RetryAction::Stop
        );
        assert_eq!(
            RetryPolicy::decide_with_jitter(
                context(RetryOperation::NameServerKv, RetryIdempotency::Idempotent, 1, 3),
                response,
                |_| Duration::ZERO,
            ),
            RetryAction::RetryNow
        );
    }

    #[test]
    fn producer_send_status_uses_existing_compatibility_switch() {
        let mut context = context(RetryOperation::ProducerSend, RetryIdempotency::NonIdempotent, 1, 3);
        assert_eq!(
            RetryPolicy::decide_with_jitter(context, RetryFacts::SendStatus(SendStatus::FlushDiskTimeout), |_| {
                Duration::ZERO
            },),
            RetryAction::Stop
        );
        context.retry_not_store_ok = true;
        assert_eq!(
            RetryPolicy::decide_with_jitter(context, RetryFacts::SendStatus(SendStatus::FlushDiskTimeout), |_| {
                Duration::ZERO
            },),
            RetryAction::SwitchBroker
        );
    }

    #[test]
    fn recovery_hints_only_emit_actions_owned_by_the_operation() {
        let cases = [
            (
                RetryOperation::NameServerKv,
                RecoveryHint::RefreshRoute,
                RetryAction::Stop,
            ),
            (
                RetryOperation::NameServerKv,
                RecoveryHint::RefreshLeader,
                RetryAction::Stop,
            ),
            (
                RetryOperation::CreateTopic,
                RecoveryHint::SwitchBroker,
                RetryAction::Stop,
            ),
            (
                RetryOperation::CreateTopic,
                RecoveryHint::RefreshRoute,
                RetryAction::Stop,
            ),
            (
                RetryOperation::CreateTopic,
                RecoveryHint::RefreshLeader,
                RetryAction::Stop,
            ),
            (
                RetryOperation::AssignmentQuery,
                RecoveryHint::RefreshLeader,
                RetryAction::RefreshLeader,
            ),
            (
                RetryOperation::ProducerSend,
                RecoveryHint::RefreshRoute,
                RetryAction::RefreshRoute,
            ),
            (
                RetryOperation::NameServerKv,
                RecoveryHint::SwitchBroker,
                RetryAction::SwitchBroker,
            ),
        ];

        for (operation, hint, expected) in cases {
            assert_eq!(
                RetryPolicy::from_recovery_hint(operation, hint, 1, |_| Duration::ZERO),
                expected
            );
        }
    }

    #[test]
    fn misleading_business_text_does_not_change_never_policy() {
        let error = RocketMQError::illegal_argument("timeout PHASE=before_write LIMIT=1");
        let action = RetryPolicy::decide_with_jitter(
            context(RetryOperation::AssignmentQuery, RetryIdempotency::Idempotent, 1, 3),
            RetryFacts::Business { error: &error },
            |_| Duration::ZERO,
        );
        assert_eq!(action, RetryAction::Stop);
    }
}
