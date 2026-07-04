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

impl ClientRetryDecision {
    #[inline]
    pub(crate) fn from_error(error: &RocketMQError) -> Self {
        match error.kind().spec().recovery.retry {
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
        !matches!(self, Self::NoRetry)
    }
}

#[inline]
pub(crate) fn should_retry_async_send_error(error: &RocketMQError) -> bool {
    if is_terminal_send_error(error) {
        return false;
    }

    ClientRetryDecision::from_error(error).should_retry()
}

#[inline]
pub(crate) fn should_retry_producer_send_error(error: &RocketMQError, retry_response_codes: &HashSet<i32>) -> bool {
    if is_terminal_send_error(error) {
        return false;
    }

    match error {
        RocketMQError::BrokerOperationFailed { code, .. } => retry_response_codes.contains(code),
        _ => ClientRetryDecision::from_error(error).should_retry(),
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
            addr: "broker-a:10911".to_string(),
            timeout_ms: 3_000,
        });
        let too_many_requests = RocketMQError::Network(NetworkError::TooManyRequests {
            addr: "broker-a:10911".to_string(),
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
}
