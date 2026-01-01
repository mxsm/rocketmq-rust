/*
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

use cheetah_string::CheetahString;
use rocketmq_common::common::FAQUrl;
use rocketmq_remoting::remoting_error::RemotingError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MQClientError {
    #[error("{0}")]
    MQClientErr(#[from] ClientErr),

    #[error("{0}")]
    RemotingTooMuchRequestError(String),

    #[error("{0}")]
    MQClientBrokerError(#[from] MQBrokerErr),

    #[error("{0}")]
    RequestTimeoutError(#[from] RequestTimeoutErr),

    #[error("Client exception occurred: CODE:{0}, broker address:{1}, Message:{2}")]
    OffsetNotFoundError(i32, String, String),

    #[error("{0}")]
    RemotingError(#[from] RemotingError),

    #[error("{0}")]
    IllegalArgumentError(String),

    #[error("{0}")]
    CommonError(#[from] rocketmq_common::error::Error),
}

#[derive(Error, Debug)]
#[error("{message}")]
pub struct MQBrokerErr {
    response_code: i32,
    error_message: Option<CheetahString>,
    broker_addr: Option<CheetahString>,
    message: CheetahString,
}

impl MQBrokerErr {
    pub fn new(response_code: i32, error_message: impl Into<CheetahString>) -> Self {
        let error_message = error_message.into();
        let message = FAQUrl::attach_default_url(Some(
            format!("CODE: {}  DESC: {}", response_code, error_message,).as_str(),
        ));
        Self {
            response_code,
            error_message: Some(error_message),
            broker_addr: None,
            message: CheetahString::from(message),
        }
    }

    pub fn new_with_broker(
        response_code: i32,
        error_message: impl Into<CheetahString>,
        broker_addr: impl Into<CheetahString>,
    ) -> Self {
        let broker_addr = broker_addr.into();
        let error_message = error_message.into();
        let message = FAQUrl::attach_default_url(Some(
            format!(
                "CODE: {}  DESC: {} BROKER: {}",
                response_code, error_message, broker_addr
            )
            .as_str(),
        ));
        Self {
            response_code,
            error_message: Some(error_message),
            broker_addr: Some(broker_addr),
            message: CheetahString::from(message),
        }
    }

    pub fn response_code(&self) -> i32 {
        self.response_code
    }

    pub fn error_message(&self) -> Option<&CheetahString> {
        self.error_message.as_ref()
    }

    pub fn broker_addr(&self) -> Option<&CheetahString> {
        self.broker_addr.as_ref()
    }
}

// Note: Macros mq_client_err! and client_broker_err! are now defined in lib.rs
// to ensure they're available throughout the crate

#[derive(Error, Debug)]
#[error("{message}")]
pub struct ClientErr {
    response_code: i32,
    error_message: Option<CheetahString>,
    message: CheetahString,
}

impl ClientErr {
    pub fn new(error_message: impl Into<CheetahString>) -> Self {
        let error_message = error_message.into();
        let message = FAQUrl::attach_default_url(Some(error_message.as_str()));
        Self {
            response_code: -1,
            error_message: Some(error_message),
            message: CheetahString::from(message),
        }
    }

    pub fn new_with_code(response_code: i32, error_message: impl Into<CheetahString>) -> Self {
        let error_message = error_message.into();
        let message = FAQUrl::attach_default_url(Some(
            format!("CODE: {}  DESC: {}", response_code, error_message,).as_str(),
        ));
        Self {
            response_code,
            error_message: Some(error_message),
            message: CheetahString::from(message),
        }
    }

    pub fn response_code(&self) -> i32 {
        self.response_code
    }

    pub fn error_message(&self) -> Option<&CheetahString> {
        self.error_message.as_ref()
    }
}

#[derive(Error, Debug)]
#[error("{message}")]
pub struct RequestTimeoutErr {
    response_code: i32,
    error_message: Option<CheetahString>,
    message: CheetahString,
}

impl RequestTimeoutErr {
    pub fn new(error_message: impl Into<CheetahString>) -> Self {
        let error_message = error_message.into();
        let message = FAQUrl::attach_default_url(Some(error_message.as_str()));
        Self {
            response_code: -1,
            error_message: Some(error_message),
            message: CheetahString::from(message),
        }
    }

    pub fn new_with_code(response_code: i32, error_message: impl Into<CheetahString>) -> Self {
        let error_message = error_message.into();
        let message = FAQUrl::attach_default_url(Some(
            format!("CODE: {}  DESC: {}", response_code, error_message,).as_str(),
        ));
        Self {
            response_code,
            error_message: Some(error_message),
            message: CheetahString::from(message),
        }
    }

    pub fn response_code(&self) -> i32 {
        self.response_code
    }

    pub fn error_message(&self) -> Option<&CheetahString> {
        self.error_message.as_ref()
    }
}

#[macro_export]
macro_rules! request_timeout_err {
    // Handle errors with a custom ResponseCode and formatted string
    ($response_code:expr, $fmt:expr, $($arg:expr),*) => {{
        let formatted_msg = format!($fmt, $($arg),*);
        std::result::Result::Err($crate::client_error::MQClientError::RequestTimeoutError(
            $crate::client_error::RequestTimeoutErr::new_with_code(
                $response_code as i32,
                formatted_msg,
            ),
        ))
    }};
    ($response_code:expr, $error_message:expr) => {{
        std::result::Result::Err($crate::client_error::MQClientError::RequestTimeoutError(
            $crate::client_error::RequestTimeoutErr::new_with_code(
                $response_code as i32,
                $error_message,
            ),
        ))
    }};
    // Handle errors without a ResponseCode, using only the error message
    ($error_message:expr) => {{
        std::result::Result::Err($crate::client_error::MQClientError::RequestTimeoutError(
            $crate::client_error::RequestTimeoutErr::new($error_message),
        ))
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_error;

    #[test]
    fn client_err_with_response_code_formats_correctly() {
        let result: std::result::Result<(), client_error::MQClientError> =
            Err(mq_client_err!(404, "Error: not found"));
        assert!(result.is_err());
        if let Err(MQClientError::MQClientErr(err)) = result {
            assert_eq!(err.response_code(), 404);
            assert_eq!(err.error_message().unwrap(), "Error: not found");
        } else {
            panic!("Expected MQClientError::MQClientErr");
        }
    }

    #[test]
    fn client_broker_err_with_response_code_and_broker_formats_correctly() {
        let result: std::result::Result<(), client_error::MQClientError> =
            Err(client_broker_err!(404, "Error: {}", "127.0.0.1"));
        assert!(result.is_err());
        if let Err(MQClientError::MQClientBrokerError(err)) = result {
            assert_eq!(err.response_code(), 404);
            assert_eq!(err.error_message().unwrap(), "Error: {}");
            assert_eq!(err.broker_addr().unwrap(), "127.0.0.1");
        }
    }

    #[test]
    fn client_broker_err_with_response_code_formats_correctly() {
        let result: std::result::Result<(), client_error::MQClientError> =
            Err(client_broker_err!(404, "Error: not found"));
        assert!(result.is_err());
        if let Err(MQClientError::MQClientBrokerError(err)) = result {
            assert_eq!(err.response_code(), 404);
            assert_eq!(err.error_message().unwrap(), "Error: not found");
            assert!(err.broker_addr().is_none());
        }
    }

    #[test]
    fn request_timeout_err_with_response_code_formats_correctly() {
        let result: std::result::Result<(), client_error::MQClientError> =
            request_timeout_err!(408, "Request timed out");
        assert!(result.is_err());
        if let Err(MQClientError::RequestTimeoutError(err)) = result {
            assert_eq!(err.response_code(), 408);
            assert_eq!(err.error_message().unwrap(), "Request timed out");
        }
    }

    #[test]
    fn request_timeout_err_without_response_code_formats_correctly() {
        let result: Result<(), client_error::MQClientError> = request_timeout_err!("Timeout error");
        assert!(result.is_err());
        if let Err(MQClientError::RequestTimeoutError(err)) = result {
            assert_eq!(err.response_code(), -1);
            assert_eq!(err.error_message().unwrap(), "Timeout error");
        }
    }

    #[test]
    fn request_timeout_err_with_multiple_arguments_formats_correctly() {
        let result: Result<(), client_error::MQClientError> =
            request_timeout_err!(504, "Error: {} - {}", "Gateway", "Timeout");
        assert!(result.is_err());
        if let Err(MQClientError::RequestTimeoutError(err)) = result {
            assert_eq!(err.response_code(), 504);
            assert_eq!(err.error_message().unwrap(), "Error: Gateway - Timeout");
        }
    }

    #[test]
    fn mq_client_err_with_response_code_formats_correctly() {
        let result: std::result::Result<(), client_error::MQClientError> =
            Err(mq_client_err!(404, "Error: {}", "not found"));
        assert!(result.is_err());
        if let Err(MQClientError::MQClientErr(err)) = result {
            assert_eq!(err.response_code(), 404);
            assert_eq!(err.error_message().unwrap(), "Error: not found");
        }
    }

    #[test]
    fn mq_client_err_without_response_code_formats_correctly() {
        let result: Result<(), client_error::MQClientError> = Err(mq_client_err!("simple error"));
        assert!(result.is_err());
        if let Err(MQClientError::MQClientErr(err)) = result {
            assert_eq!(err.response_code(), -1);
            assert_eq!(err.error_message().unwrap(), "simple error");
        }
    }

    #[test]
    fn mq_client_err_with_multiple_arguments_formats_correctly() {
        let result: Result<(), client_error::MQClientError> =
            Err(mq_client_err!(500, "Error: {} - {}", "internal", "server error"));
        assert!(result.is_err());
        if let Err(MQClientError::MQClientErr(err)) = result {
            assert_eq!(err.response_code(), 500);
            assert_eq!(
                err.error_message().unwrap(),
                "Error: internal - server error"
            );
        }
    }

    #[test]
    fn mq_broker_err_new_initializes_correctly() {
        let error = MQBrokerErr::new(404, "not found");
        assert_eq!(error.response_code(), 404);
        assert_eq!(error.error_message().unwrap(), "not found");
        assert!(error.broker_addr().is_none());
    }

    #[test]
    fn mq_broker_err_new_with_broker_initializes_correctly() {
        let error = MQBrokerErr::new_with_broker(404, "not found", "127.0.0.1");
        assert_eq!(error.response_code(), 404);
        assert_eq!(error.error_message().unwrap(), "not found");
        assert_eq!(error.broker_addr().unwrap(), "127.0.0.1");
    }

    #[test]
    fn client_err_new_initializes_correctly() {
        let error = ClientErr::new("client error");
        assert_eq!(error.response_code(), -1);
        assert_eq!(error.error_message().unwrap(), "client error");
    }

    #[test]
    fn client_err_new_with_code_initializes_correctly() {
        let error = ClientErr::new_with_code(500, "internal error");
        assert_eq!(error.response_code(), 500);
        assert_eq!(error.error_message().unwrap(), "internal error");
    }

    #[test]
    fn request_timeout_err_new_initializes_correctly() {
        let error = RequestTimeoutErr::new("timeout error");
        assert_eq!(error.response_code(), -1);
        assert_eq!(error.error_message().unwrap(), "timeout error");
    }

    #[test]
    fn request_timeout_err_new_with_code_initializes_correctly() {
        let error = RequestTimeoutErr::new_with_code(408, "request timeout");
        assert_eq!(error.response_code(), 408);
        assert_eq!(error.error_message().unwrap(), "request timeout");
    }
}
*/
