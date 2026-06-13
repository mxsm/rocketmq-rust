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

use std::fmt;

use rocketmq_common::common::FAQUrl;
use rocketmq_error::RocketMQError;

const DEFAULT_RESPONSE_CODE: i32 = -1;
const DEFAULT_BROKER_RESPONSE_CODE: i32 = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MQClientException {
    response_code: i32,
    error_message: Option<String>,
    message: String,
}

impl MQClientException {
    pub fn new(error_message: impl Into<String>) -> Self {
        let error_message = error_message.into();
        let message = FAQUrl::attach_default_url(Some(error_message.as_str()));
        Self {
            response_code: DEFAULT_RESPONSE_CODE,
            error_message: Some(error_message),
            message,
        }
    }

    pub fn new_with_code(response_code: i32, error_message: impl Into<String>) -> Self {
        let error_message = error_message.into();
        let message =
            FAQUrl::attach_default_url(Some(format!("CODE: {response_code}  DESC: {error_message}").as_str()));
        Self {
            response_code,
            error_message: Some(error_message),
            message,
        }
    }

    pub fn response_code(&self) -> i32 {
        self.response_code
    }

    pub fn get_response_code(&self) -> i32 {
        self.response_code()
    }

    pub fn set_response_code(&mut self, response_code: i32) -> &mut Self {
        self.response_code = response_code;
        self
    }

    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    pub fn get_error_message(&self) -> Option<&str> {
        self.error_message()
    }

    pub fn set_error_message(&mut self, error_message: impl Into<String>) {
        self.error_message = Some(error_message.into());
    }

    pub fn from_rocketmq_error(error: &RocketMQError) -> Self {
        Self::new(error.to_string())
    }
}

impl fmt::Display for MQClientException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message.as_str())
    }
}

impl std::error::Error for MQClientException {}

impl From<MQClientException> for RocketMQError {
    fn from(exception: MQClientException) -> Self {
        RocketMQError::illegal_argument(exception.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MQBrokerException {
    response_code: i32,
    error_message: Option<String>,
    broker_addr: Option<String>,
    message: String,
}

impl Default for MQBrokerException {
    fn default() -> Self {
        Self {
            response_code: DEFAULT_BROKER_RESPONSE_CODE,
            error_message: None,
            broker_addr: None,
            message: String::new(),
        }
    }
}

impl MQBrokerException {
    pub fn new(response_code: i32, error_message: impl Into<String>) -> Self {
        Self::new_with_optional_broker(response_code, error_message, None)
    }

    pub fn new_with_broker(
        response_code: i32,
        error_message: impl Into<String>,
        broker_addr: impl Into<String>,
    ) -> Self {
        Self::new_with_optional_broker(response_code, error_message, Some(broker_addr.into()))
    }

    pub fn new_with_optional_broker(
        response_code: i32,
        error_message: impl Into<String>,
        broker_addr: Option<String>,
    ) -> Self {
        let error_message = error_message.into();
        let broker_suffix = broker_addr
            .as_ref()
            .map(|broker_addr| format!(" BROKER: {broker_addr}"))
            .unwrap_or_default();
        let message = FAQUrl::attach_default_url(Some(
            format!("CODE: {response_code}  DESC: {error_message}{broker_suffix}").as_str(),
        ));
        Self {
            response_code,
            error_message: Some(error_message),
            broker_addr,
            message,
        }
    }

    pub fn response_code(&self) -> i32 {
        self.response_code
    }

    pub fn get_response_code(&self) -> i32 {
        self.response_code()
    }

    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    pub fn get_error_message(&self) -> Option<&str> {
        self.error_message()
    }

    pub fn broker_addr(&self) -> Option<&str> {
        self.broker_addr.as_deref()
    }

    pub fn get_broker_addr(&self) -> Option<&str> {
        self.broker_addr()
    }

    pub fn from_rocketmq_error(error: &RocketMQError) -> Option<Self> {
        if let RocketMQError::BrokerOperationFailed {
            code,
            message,
            broker_addr,
            ..
        } = error
        {
            Some(Self::new_with_optional_broker(
                *code,
                message.as_str(),
                broker_addr.clone(),
            ))
        } else {
            None
        }
    }
}

impl fmt::Display for MQBrokerException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message.as_str())
    }
}

impl std::error::Error for MQBrokerException {}

impl From<MQBrokerException> for RocketMQError {
    fn from(exception: MQBrokerException) -> Self {
        let message = exception
            .error_message
            .clone()
            .unwrap_or_else(|| exception.message.clone());
        let error = RocketMQError::broker_operation_failed("BROKER_OPERATION", exception.response_code, message);
        if let Some(broker_addr) = exception.broker_addr {
            error.with_broker_addr(broker_addr)
        } else {
            error
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OffsetNotFoundException {
    broker_exception: MQBrokerException,
}

impl OffsetNotFoundException {
    pub fn new(response_code: i32, error_message: impl Into<String>) -> Self {
        Self {
            broker_exception: MQBrokerException::new(response_code, error_message),
        }
    }

    pub fn new_with_broker(
        response_code: i32,
        error_message: impl Into<String>,
        broker_addr: impl Into<String>,
    ) -> Self {
        Self {
            broker_exception: MQBrokerException::new_with_broker(response_code, error_message, broker_addr),
        }
    }

    pub fn response_code(&self) -> i32 {
        self.broker_exception.response_code()
    }

    pub fn get_response_code(&self) -> i32 {
        self.response_code()
    }

    pub fn error_message(&self) -> Option<&str> {
        self.broker_exception.error_message()
    }

    pub fn get_error_message(&self) -> Option<&str> {
        self.error_message()
    }

    pub fn broker_addr(&self) -> Option<&str> {
        self.broker_exception.broker_addr()
    }

    pub fn get_broker_addr(&self) -> Option<&str> {
        self.broker_addr()
    }

    pub fn as_broker_exception(&self) -> &MQBrokerException {
        &self.broker_exception
    }
}

impl fmt::Display for OffsetNotFoundException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.broker_exception.fmt(f)
    }
}

impl std::error::Error for OffsetNotFoundException {}

impl From<OffsetNotFoundException> for MQBrokerException {
    fn from(exception: OffsetNotFoundException) -> Self {
        exception.broker_exception
    }
}

impl From<OffsetNotFoundException> for RocketMQError {
    fn from(exception: OffsetNotFoundException) -> Self {
        let broker_exception: MQBrokerException = exception.into();
        broker_exception.into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestTimeoutException {
    response_code: i32,
    error_message: Option<String>,
    message: String,
}

impl RequestTimeoutException {
    pub fn new(error_message: impl Into<String>) -> Self {
        let error_message = error_message.into();
        Self {
            response_code: DEFAULT_RESPONSE_CODE,
            message: error_message.clone(),
            error_message: Some(error_message),
        }
    }

    pub fn new_with_code(response_code: i32, error_message: impl Into<String>) -> Self {
        let error_message = error_message.into();
        let message = format!("CODE: {response_code}  DESC: {error_message}");
        Self {
            response_code,
            error_message: Some(error_message),
            message,
        }
    }

    pub fn response_code(&self) -> i32 {
        self.response_code
    }

    pub fn get_response_code(&self) -> i32 {
        self.response_code()
    }

    pub fn set_response_code(&mut self, response_code: i32) -> &mut Self {
        self.response_code = response_code;
        self
    }

    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    pub fn get_error_message(&self) -> Option<&str> {
        self.error_message()
    }

    pub fn set_error_message(&mut self, error_message: impl Into<String>) {
        self.error_message = Some(error_message.into());
    }

    pub fn from_rocketmq_error(error: &RocketMQError) -> Option<Self> {
        match error {
            RocketMQError::Timeout { .. } => Some(Self::new(error.to_string())),
            RocketMQError::Network(network_error) if network_error.to_string().contains("timeout") => {
                Some(Self::new(error.to_string()))
            }
            _ => None,
        }
    }
}

impl fmt::Display for RequestTimeoutException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message.as_str())
    }
}

impl std::error::Error for RequestTimeoutException {}

impl From<RequestTimeoutException> for RocketMQError {
    fn from(_exception: RequestTimeoutException) -> Self {
        RocketMQError::Timeout {
            operation: "REQUEST",
            timeout_ms: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mq_client_exception_matches_java_fields_and_faq_message() {
        let mut exception = MQClientException::new_with_code(17, "client failed");

        assert_eq!(exception.get_response_code(), 17);
        assert_eq!(exception.get_error_message(), Some("client failed"));
        assert!(exception.to_string().contains("CODE: 17  DESC: client failed"));
        assert!(exception
            .to_string()
            .contains("For more information, please visit the url"));

        exception.set_response_code(18).set_error_message("changed message");
        assert_eq!(exception.get_response_code(), 18);
        assert_eq!(exception.get_error_message(), Some("changed message"));
        assert!(exception.to_string().contains("client failed"));
    }

    #[test]
    fn mq_broker_exception_preserves_broker_addr_like_java() {
        let exception = MQBrokerException::new_with_broker(25, "broker failed", "127.0.0.1:10911");

        assert_eq!(exception.get_response_code(), 25);
        assert_eq!(exception.get_error_message(), Some("broker failed"));
        assert_eq!(exception.get_broker_addr(), Some("127.0.0.1:10911"));
        assert!(exception.to_string().contains("BROKER: 127.0.0.1:10911"));

        let error: RocketMQError = exception.into();
        match error {
            RocketMQError::BrokerOperationFailed {
                code,
                message,
                broker_addr,
                ..
            } => {
                assert_eq!(code, 25);
                assert_eq!(message, "broker failed");
                assert_eq!(broker_addr.as_deref(), Some("127.0.0.1:10911"));
            }
            other => panic!("expected broker operation error, got {other:?}"),
        }
    }

    #[test]
    fn mq_broker_exception_can_be_extracted_from_rocketmq_error() {
        let error = RocketMQError::broker_operation_failed("SEND", 31, "send failed").with_broker_addr("broker-a");
        let exception = MQBrokerException::from_rocketmq_error(&error).expect("broker exception");

        assert_eq!(exception.get_response_code(), 31);
        assert_eq!(exception.get_error_message(), Some("send failed"));
        assert_eq!(exception.get_broker_addr(), Some("broker-a"));
    }

    #[test]
    fn offset_not_found_exception_delegates_broker_fields() {
        let exception = OffsetNotFoundException::new_with_broker(26, "offset not found", "127.0.0.1:10911");

        assert_eq!(exception.get_response_code(), 26);
        assert_eq!(exception.get_error_message(), Some("offset not found"));
        assert_eq!(exception.get_broker_addr(), Some("127.0.0.1:10911"));
        assert_eq!(
            exception.as_broker_exception().get_broker_addr(),
            Some("127.0.0.1:10911")
        );
    }

    #[test]
    fn request_timeout_exception_matches_java_message_without_faq() {
        let mut exception = RequestTimeoutException::new_with_code(408, "request timeout");

        assert_eq!(exception.get_response_code(), 408);
        assert_eq!(exception.get_error_message(), Some("request timeout"));
        assert_eq!(exception.to_string(), "CODE: 408  DESC: request timeout");

        exception.set_response_code(409).set_error_message("changed timeout");
        assert_eq!(exception.get_response_code(), 409);
        assert_eq!(exception.get_error_message(), Some("changed timeout"));
        assert_eq!(exception.to_string(), "CODE: 408  DESC: request timeout");
    }

    #[test]
    fn request_timeout_exception_converts_to_timeout_error() {
        let error: RocketMQError = RequestTimeoutException::new("request timeout").into();

        match error {
            RocketMQError::Timeout { operation, timeout_ms } => {
                assert_eq!(operation, "REQUEST");
                assert_eq!(timeout_ms, 0);
            }
            other => panic!("expected timeout error, got {other:?}"),
        }
    }
}
