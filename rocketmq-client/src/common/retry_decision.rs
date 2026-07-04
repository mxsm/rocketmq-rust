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

use rocketmq_error::NetworkError;
use rocketmq_error::RetryClass;
use rocketmq_error::RocketMQError;

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
        Self::from_retry_class(error.kind().spec().recovery.retry)
    }

    #[inline]
    pub(crate) const fn from_retry_class(retry: RetryClass) -> Self {
        match retry {
            RetryClass::Never => Self::NoRetry,
            RetryClass::Immediate => Self::Immediate,
            RetryClass::AfterBackoff => Self::Backoff,
            RetryClass::RefreshRoute => Self::RefreshRoute,
            RetryClass::SwitchBroker => Self::SwitchBroker,
            RetryClass::RefreshLeader => Self::RefreshLeader,
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

    match error {
        RocketMQError::BrokerOperationFailed { code, .. } => {
            // Java producer retries only configured broker response codes for send.
            // This allowlist is the explicit broker-protocol compatibility boundary
            // and intentionally overrides the generic BrokerOperationFailed RetryClass.
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
    match error {
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
        RocketMQError::Network(_) => Some(ProducerSendFaultDecision {
            isolation: true,
            reachable: !start_detector_enabled,
            log_resend_immediately: false,
        }),
        _ => None,
    }
}

#[inline]
fn is_terminal_send_error(error: &RocketMQError) -> bool {
    matches!(
        error,
        RocketMQError::Timeout { .. }
            | RocketMQError::ClientNotStarted
            | RocketMQError::ClientShuttingDown
            | RocketMQError::Network(
                NetworkError::ConnectionTimeout { .. }
                    | NetworkError::RequestTimeout { .. }
                    | NetworkError::TooManyRequests { .. },
            )
    )
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let network_error = RocketMQError::network_connection_failed("broker-a:10911", "connection failed");
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
        let retryable = RocketMQError::Network(NetworkError::send_failed("broker-a:10911", "write failed"));
        let request_timeout = RocketMQError::Network(NetworkError::RequestTimeout {
            addr: "broker-a:10911".into(),
            timeout_ms: 3_000,
        });
        let too_many_requests = RocketMQError::Network(NetworkError::TooManyRequests {
            addr: "broker-a:10911".into(),
            limit: 1,
        });

        assert!(should_retry_async_send_error(&retryable));
        assert!(!should_retry_async_send_error(&request_timeout));
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
                RocketMQError::network_connection_failed("broker-a:10911", "connection failed"),
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
                ClientRetryDecision::from_retry_class(error.kind().spec().recovery.retry),
                expected_decision
            );
            assert_eq!(expected_decision.effect(), expected_effect);
        }
    }

    #[test]
    fn producer_send_retry_priority_is_terminal_then_broker_allowlist_then_spec() {
        let timeout = RocketMQError::Timeout {
            operation: "SEND_MESSAGE",
            timeout_ms: 3_000,
        };
        let broker_error = RocketMQError::broker_operation_failed("SEND_MESSAGE", 12, "system busy");
        let network_error = RocketMQError::network_connection_failed("broker-a:10911", "connection failed");

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

    #[test]
    fn producer_send_fault_decision_is_centralized() {
        let illegal_argument = RocketMQError::illegal_argument("bad request");
        let broker_error = RocketMQError::broker_operation_failed("SEND_MESSAGE", 12, "system busy");
        let network_error = RocketMQError::Network(NetworkError::send_failed("broker-a:10911", "write failed"));

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
    fn retry_decision_module_does_not_parse_display_text() {
        let source = include_str!("retry_decision.rs");

        assert!(!source.contains(concat!("to_", "string(")));
        assert!(!source.contains(concat!("format", "!(")));
    }
}
