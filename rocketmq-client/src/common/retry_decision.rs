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

use std::collections::HashSet;

use rocketmq_error::ErrorKind;
use rocketmq_error::RecoveryHint;
use rocketmq_error::RocketMQError;
use rocketmq_error::TRANSPORT_CONNECTION_FAILED;
use rocketmq_error::TRANSPORT_CONNECTION_TIMEOUT;
use rocketmq_error::TRANSPORT_DNS_FAILED;
use rocketmq_error::TRANSPORT_REMOTE_RATE_LIMITED;
use rocketmq_error::TRANSPORT_RESPONSE_TIMEOUT;
use rocketmq_error::TRANSPORT_WRITE_TIMEOUT;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientRetryDecision {
    NoRetry,
    Immediate,
    Backoff,
    RefreshRoute,
    SwitchBroker,
    RefreshLeader,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ClientRetryEffect {
    pub(crate) retry: bool,
    pub(crate) refresh_route: bool,
    pub(crate) switch_broker: bool,
    pub(crate) refresh_leader: bool,
    pub(crate) backoff: bool,
}

impl ClientRetryEffect {
    #[inline]
    const fn none() -> Self {
        Self {
            retry: false,
            refresh_route: false,
            switch_broker: false,
            refresh_leader: false,
            backoff: false,
        }
    }

    #[inline]
    const fn retry() -> Self {
        Self {
            retry: true,
            refresh_route: false,
            switch_broker: false,
            refresh_leader: false,
            backoff: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProducerSendFaultDecision {
    pub(crate) isolation: bool,
    pub(crate) reachable: bool,
    pub(crate) log_resend_immediately: bool,
}

impl ClientRetryDecision {
    #[inline]
    pub(crate) fn from_error(error: &RocketMQError) -> Self {
        if matches!(
            error.kind(),
            ErrorKind::SubscriptionGroupNotExist | ErrorKind::MessageLookupFailed | ErrorKind::QueryNotFound
        ) {
            return Self::Immediate;
        }

        Self::from_recovery_hint(error.descriptor().recovery_hint())
    }

    #[inline]
    pub(crate) const fn from_recovery_hint(recovery_hint: RecoveryHint) -> Self {
        match recovery_hint {
            RecoveryHint::Never | RecoveryHint::RefreshCredentials | RecoveryHint::OperatorAction => Self::NoRetry,
            RecoveryHint::Backoff => Self::Backoff,
            RecoveryHint::RefreshRoute => Self::RefreshRoute,
            RecoveryHint::RefreshLeader => Self::RefreshLeader,
            RecoveryHint::SwitchBroker => Self::SwitchBroker,
        }
    }

    #[inline]
    pub(crate) const fn should_retry(self) -> bool {
        self.effect().retry
    }

    #[inline]
    pub(crate) const fn effect(self) -> ClientRetryEffect {
        match self {
            Self::NoRetry => ClientRetryEffect::none(),
            Self::Immediate => ClientRetryEffect::retry(),
            Self::Backoff => ClientRetryEffect {
                retry: true,
                refresh_route: false,
                switch_broker: false,
                refresh_leader: false,
                backoff: true,
            },
            Self::RefreshRoute => ClientRetryEffect {
                retry: true,
                refresh_route: true,
                switch_broker: false,
                refresh_leader: false,
                backoff: false,
            },
            Self::SwitchBroker => ClientRetryEffect {
                retry: true,
                refresh_route: false,
                switch_broker: true,
                refresh_leader: false,
                backoff: false,
            },
            Self::RefreshLeader => ClientRetryEffect {
                retry: true,
                refresh_route: false,
                switch_broker: false,
                refresh_leader: true,
                backoff: false,
            },
        }
    }
}

#[inline]
pub(crate) fn should_retry_async_send_error(error: &RocketMQError) -> bool {
    async_send_retry_decision(error).should_retry()
}

#[inline]
pub(crate) fn async_send_retry_decision(error: &RocketMQError) -> ClientRetryDecision {
    if is_terminal_send_error(error) {
        return ClientRetryDecision::NoRetry;
    }

    ClientRetryDecision::from_error(error)
}

#[inline]
pub(crate) fn should_retry_producer_send_error(error: &RocketMQError, retry_response_codes: &HashSet<i32>) -> bool {
    producer_send_retry_decision(error, retry_response_codes).should_retry()
}

#[inline]
pub(crate) fn producer_send_retry_decision(
    error: &RocketMQError,
    retry_response_codes: &HashSet<i32>,
) -> ClientRetryDecision {
    if is_terminal_send_error(error) {
        return ClientRetryDecision::NoRetry;
    }

    match retry_policy_error(error) {
        RocketMQError::BrokerOperationFailed { code, .. } => {
            // Java producer retries only configured broker response codes for send.
            // This allowlist is the explicit broker-protocol compatibility boundary
            // and intentionally overrides the generic BrokerOperationFailed recovery hint.
            if retry_response_codes.contains(code) {
                ClientRetryDecision::SwitchBroker
            } else {
                ClientRetryDecision::NoRetry
            }
        }
        _ => ClientRetryDecision::from_error(error),
    }
}

#[inline]
pub(crate) fn producer_send_fault_decision(
    error: &RocketMQError,
    start_detector_enabled: bool,
) -> Option<ProducerSendFaultDecision> {
    match retry_policy_error(error) {
        RocketMQError::IllegalArgument(_) => Some(ProducerSendFaultDecision {
            isolation: false,
            reachable: true,
            log_resend_immediately: true,
        }),
        RocketMQError::BrokerOperationFailed { .. } => Some(ProducerSendFaultDecision {
            isolation: true,
            reachable: false,
            log_resend_immediately: false,
        }),
        error if is_network_send_failure(error) => Some(ProducerSendFaultDecision {
            isolation: true,
            reachable: !start_detector_enabled,
            log_resend_immediately: false,
        }),
        _ => None,
    }
}

#[inline]
fn is_terminal_send_error(error: &RocketMQError) -> bool {
    let error = retry_policy_error(error);
    matches!(
        error,
        RocketMQError::Timeout { .. } | RocketMQError::ClientNotStarted | RocketMQError::ClientShuttingDown
    ) || network_has_descriptor(error, &TRANSPORT_CONNECTION_TIMEOUT)
        || network_has_descriptor(error, &TRANSPORT_REMOTE_RATE_LIMITED)
}

#[inline]
pub(crate) fn retry_policy_error(error: &RocketMQError) -> &RocketMQError {
    match error {
        RocketMQError::Shared(shared) => shared.as_error(),
        error => error,
    }
}

#[inline]
pub(crate) fn is_network_send_failure(error: &RocketMQError) -> bool {
    let error = retry_policy_error(error);
    [
        &TRANSPORT_DNS_FAILED,
        &TRANSPORT_CONNECTION_FAILED,
        &TRANSPORT_CONNECTION_TIMEOUT,
        &TRANSPORT_REMOTE_RATE_LIMITED,
        &TRANSPORT_WRITE_TIMEOUT,
        &TRANSPORT_RESPONSE_TIMEOUT,
    ]
    .into_iter()
    .any(|descriptor| network_has_descriptor(error, descriptor))
}

#[inline]
fn network_has_descriptor(error: &RocketMQError, descriptor: &'static rocketmq_error::ErrorDescriptor) -> bool {
    matches!(error, RocketMQError::Network(network) if network.code() == descriptor.code())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use rocketmq_error::Error;
    use rocketmq_error::RpcClientError;
    use rocketmq_error::SharedRocketMQError;
    use rocketmq_error::TRANSPORT_ADMISSION_QUEUE_SATURATED;
    use rocketmq_error::TRANSPORT_ENDPOINT_INVALID;

    fn network_error(descriptor: &'static rocketmq_error::ErrorDescriptor) -> RocketMQError {
        RocketMQError::Network(Arc::new(Error::new(descriptor)))
    }

    const fn effect(
        retry: bool,
        refresh_route: bool,
        switch_broker: bool,
        refresh_leader: bool,
        backoff: bool,
    ) -> ClientRetryEffect {
        ClientRetryEffect {
            retry,
            refresh_route,
            switch_broker,
            refresh_leader,
            backoff,
        }
    }

    #[test]
    fn retry_decision_uses_error_recovery_policy() {
        let route_error = RocketMQError::route_not_found("TopicA");
        let network_error = network_error(&TRANSPORT_CONNECTION_FAILED);
        let client_state_error = RocketMQError::ClientNotStarted;

        assert_eq!(
            ClientRetryDecision::from_error(&route_error),
            ClientRetryDecision::RefreshRoute
        );
        assert_eq!(
            ClientRetryDecision::from_error(&network_error),
            ClientRetryDecision::Backoff
        );
        assert_eq!(
            ClientRetryDecision::from_error(&client_state_error),
            ClientRetryDecision::NoRetry
        );
    }

    #[test]
    fn async_send_retry_excludes_terminal_send_errors() {
        let retryable = network_error(&TRANSPORT_WRITE_TIMEOUT);
        let response_timeout = network_error(&TRANSPORT_RESPONSE_TIMEOUT);
        let connection_timeout = network_error(&TRANSPORT_CONNECTION_TIMEOUT);
        let too_many_requests = network_error(&TRANSPORT_REMOTE_RATE_LIMITED);

        assert!(should_retry_async_send_error(&retryable));
        assert!(should_retry_async_send_error(&response_timeout));
        assert!(!should_retry_async_send_error(&connection_timeout));
        assert!(!should_retry_async_send_error(&too_many_requests));
        assert!(!should_retry_async_send_error(&RocketMQError::ClientShuttingDown));
    }

    #[test]
    fn producer_send_retry_uses_response_code_allow_list() {
        let error = RocketMQError::broker_operation_failed("SEND_MESSAGE", 12, "system busy");
        let retry_codes = HashSet::from([12]);
        let non_retry_codes = HashSet::from([13]);

        assert!(should_retry_producer_send_error(&error, &retry_codes));
        assert!(!should_retry_producer_send_error(&error, &non_retry_codes));
    }

    #[test]
    fn retry_effects_cover_recovery_classes() {
        let cases = vec![
            (
                RocketMQError::route_not_found("TopicA"),
                ClientRetryDecision::RefreshRoute,
                effect(true, true, false, false, false),
            ),
            (
                RocketMQError::BrokerNotFound {
                    name: "broker-a".into(),
                },
                ClientRetryDecision::SwitchBroker,
                effect(true, false, true, false, false),
            ),
            (
                RocketMQError::NotMasterBroker {
                    master_address: "broker-b:10911".into(),
                },
                ClientRetryDecision::RefreshLeader,
                effect(true, false, false, true, false),
            ),
            (
                network_error(&TRANSPORT_CONNECTION_FAILED),
                ClientRetryDecision::Backoff,
                effect(true, false, false, false, true),
            ),
            (
                RocketMQError::ClientNotStarted,
                ClientRetryDecision::NoRetry,
                effect(false, false, false, false, false),
            ),
        ];

        for (error, expected_decision, expected_effect) in cases {
            assert_eq!(ClientRetryDecision::from_error(&error), expected_decision);
            assert_eq!(
                ClientRetryDecision::from_recovery_hint(error.descriptor().recovery_hint()),
                expected_decision
            );
            assert_eq!(expected_decision.effect(), expected_effect);
        }
    }

    #[test]
    fn producer_send_retry_priority_is_terminal_then_broker_allowlist_then_descriptor() {
        let timeout = RocketMQError::Timeout {
            operation: "SEND_MESSAGE",
            timeout_ms: 3_000,
        };
        let broker_error = RocketMQError::broker_operation_failed("SEND_MESSAGE", 12, "system busy");
        let network_error = network_error(&TRANSPORT_CONNECTION_FAILED);

        assert_eq!(ClientRetryDecision::from_error(&timeout), ClientRetryDecision::Backoff);
        assert_eq!(
            producer_send_retry_decision(&timeout, &HashSet::from([12])),
            ClientRetryDecision::NoRetry
        );

        assert_eq!(
            ClientRetryDecision::from_error(&broker_error),
            ClientRetryDecision::SwitchBroker
        );
        assert_eq!(
            producer_send_retry_decision(&broker_error, &HashSet::from([13])),
            ClientRetryDecision::NoRetry
        );
        assert_eq!(
            producer_send_retry_decision(&broker_error, &HashSet::from([12])),
            ClientRetryDecision::SwitchBroker
        );

        assert_eq!(
            producer_send_retry_decision(&network_error, &HashSet::new()),
            ClientRetryDecision::Backoff
        );
    }

    fn assert_no_retry_for_direct_and_shared(error: RocketMQError) {
        let retry_codes = HashSet::new();

        assert_eq!(ClientRetryDecision::from_error(&error), ClientRetryDecision::NoRetry);
        assert_eq!(async_send_retry_decision(&error), ClientRetryDecision::NoRetry);
        assert_eq!(
            producer_send_retry_decision(&error, &retry_codes),
            ClientRetryDecision::NoRetry
        );

        let shared = SharedRocketMQError::new(error).into_error();
        assert_eq!(ClientRetryDecision::from_error(&shared), ClientRetryDecision::NoRetry);
        assert_eq!(async_send_retry_decision(&shared), ClientRetryDecision::NoRetry);
        assert_eq!(
            producer_send_retry_decision(&shared, &retry_codes),
            ClientRetryDecision::NoRetry
        );
    }

    fn assert_backoff_for_direct_and_shared(error: RocketMQError) {
        let retry_codes = HashSet::new();

        assert_eq!(ClientRetryDecision::from_error(&error), ClientRetryDecision::Backoff);
        assert_eq!(async_send_retry_decision(&error), ClientRetryDecision::Backoff);
        assert_eq!(
            producer_send_retry_decision(&error, &retry_codes),
            ClientRetryDecision::Backoff
        );

        let shared = SharedRocketMQError::new(error).into_error();
        assert_eq!(ClientRetryDecision::from_error(&shared), ClientRetryDecision::Backoff);
        assert_eq!(async_send_retry_decision(&shared), ClientRetryDecision::Backoff);
        assert_eq!(
            producer_send_retry_decision(&shared, &retry_codes),
            ClientRetryDecision::Backoff
        );
    }

    #[test]
    fn descriptor_never_stops_the_three_approved_retry_cases() {
        assert_no_retry_for_direct_and_shared(RocketMQError::RetryLimitExceeded {
            group: "producer-a".into(),
            current: 3,
            max: 2,
        });
        assert_no_retry_for_direct_and_shared(network_error(&TRANSPORT_ENDPOINT_INVALID));
        assert_no_retry_for_direct_and_shared(RocketMQError::Rpc(RpcClientError::UnsupportedRequestCode { code: 999 }));
    }

    #[test]
    fn descriptor_backoff_controls_remain_retryable() {
        assert_backoff_for_direct_and_shared(network_error(&TRANSPORT_CONNECTION_FAILED));
        assert_backoff_for_direct_and_shared(RocketMQError::Rpc(RpcClientError::request_failed(
            "broker-a:10911",
            10,
            3_000,
            std::io::Error::other("request failed"),
        )));
        assert_backoff_for_direct_and_shared(RocketMQError::Rpc(RpcClientError::unexpected_response_code(
            1,
            "SYSTEM_ERROR",
        )));
    }

    #[test]
    fn operation_owned_immediate_retry_cases_are_preserved() {
        for error in [
            RocketMQError::SubscriptionGroupNotExist {
                group: "consumer-a".into(),
            },
            RocketMQError::MessageLookupFailed { offset: 42 },
            RocketMQError::QueryNotFound {
                resource: "message-key".into(),
            },
        ] {
            assert_eq!(ClientRetryDecision::from_error(&error), ClientRetryDecision::Immediate);
            let shared = SharedRocketMQError::new(error).into_error();
            assert_eq!(ClientRetryDecision::from_error(&shared), ClientRetryDecision::Immediate);
        }
    }

    #[test]
    fn shared_terminal_send_errors_keep_direct_no_retry_behavior() {
        for error in [
            RocketMQError::Timeout {
                operation: "SEND_MESSAGE",
                timeout_ms: 3_000,
            },
            network_error(&TRANSPORT_CONNECTION_TIMEOUT),
            network_error(&TRANSPORT_REMOTE_RATE_LIMITED),
        ] {
            assert_eq!(async_send_retry_decision(&error), ClientRetryDecision::NoRetry);
            assert_eq!(
                producer_send_retry_decision(&error, &HashSet::new()),
                ClientRetryDecision::NoRetry
            );

            let shared = SharedRocketMQError::new(error).into_error();
            assert_eq!(async_send_retry_decision(&shared), ClientRetryDecision::NoRetry);
            assert_eq!(
                producer_send_retry_decision(&shared, &HashSet::new()),
                ClientRetryDecision::NoRetry
            );
        }
    }

    #[test]
    fn shared_broker_errors_keep_response_code_allowlist_behavior() {
        for (retry_codes, expected) in [
            (HashSet::from([12]), ClientRetryDecision::SwitchBroker),
            (HashSet::from([13]), ClientRetryDecision::NoRetry),
        ] {
            let direct = RocketMQError::broker_operation_failed("SEND_MESSAGE", 12, "system busy");
            assert_eq!(producer_send_retry_decision(&direct, &retry_codes), expected);

            let shared = SharedRocketMQError::new(direct).into_error();
            assert_eq!(producer_send_retry_decision(&shared, &retry_codes), expected);
        }
    }

    #[test]
    fn producer_send_fault_decision_is_centralized() {
        let illegal_argument = RocketMQError::illegal_argument("bad request");
        let broker_error = RocketMQError::broker_operation_failed("SEND_MESSAGE", 12, "system busy");
        let network_error = network_error(&TRANSPORT_WRITE_TIMEOUT);

        assert_eq!(
            producer_send_fault_decision(&illegal_argument, false),
            Some(ProducerSendFaultDecision {
                isolation: false,
                reachable: true,
                log_resend_immediately: true,
            })
        );
        assert_eq!(
            producer_send_fault_decision(&broker_error, false),
            Some(ProducerSendFaultDecision {
                isolation: true,
                reachable: false,
                log_resend_immediately: false,
            })
        );
        assert_eq!(
            producer_send_fault_decision(&network_error, false),
            Some(ProducerSendFaultDecision {
                isolation: true,
                reachable: true,
                log_resend_immediately: false,
            })
        );
        assert_eq!(
            producer_send_fault_decision(&network_error, true),
            Some(ProducerSendFaultDecision {
                isolation: true,
                reachable: false,
                log_resend_immediately: false,
            })
        );
        assert_eq!(
            producer_send_fault_decision(&RocketMQError::ClientNotStarted, false),
            None
        );
    }

    #[test]
    fn producer_send_fault_decision_preserves_shared_network_identity() {
        for descriptor in [
            &TRANSPORT_CONNECTION_FAILED,
            &TRANSPORT_WRITE_TIMEOUT,
            &TRANSPORT_RESPONSE_TIMEOUT,
        ] {
            let canonical = Arc::new(Error::caused_by(
                descriptor,
                std::io::Error::new(std::io::ErrorKind::ConnectionReset, "connection reset"),
            ));
            let direct = RocketMQError::Network(Arc::clone(&canonical));
            let shared = SharedRocketMQError::new(RocketMQError::Network(Arc::clone(&canonical))).into_error();

            for (detector_enabled, reachable) in [(false, true), (true, false)] {
                let expected = Some(ProducerSendFaultDecision {
                    isolation: true,
                    reachable,
                    log_resend_immediately: false,
                });
                assert_eq!(producer_send_fault_decision(&direct, detector_enabled), expected);
                assert_eq!(producer_send_fault_decision(&shared, detector_enabled), expected);
            }
            let RocketMQError::Network(borrowed) = retry_policy_error(&shared) else {
                panic!("shared error should retain the Network carrier");
            };
            assert!(Arc::ptr_eq(borrowed, &canonical));
            assert!(std::error::Error::source(canonical.as_ref()).is_some());
        }
    }

    #[test]
    fn local_network_outcomes_do_not_trigger_broker_fault_isolation() {
        for descriptor in [&TRANSPORT_ENDPOINT_INVALID, &TRANSPORT_ADMISSION_QUEUE_SATURATED] {
            let canonical = Arc::new(Error::new(descriptor));
            let direct = RocketMQError::Network(Arc::clone(&canonical));
            let shared = SharedRocketMQError::new(RocketMQError::Network(canonical)).into_error();
            for error in [&direct, &shared] {
                assert_eq!(producer_send_fault_decision(error, false), None);
                assert_eq!(producer_send_fault_decision(error, true), None);
            }
        }
    }

    #[test]
    fn retry_decision_module_does_not_parse_display_text() {
        let source = include_str!("retry_decision.rs");

        assert!(!source.contains(concat!("to_", "string(")));
        assert!(!source.contains(concat!("format", "!(")));
    }
}
