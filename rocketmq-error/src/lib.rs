/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
mod broker_error;
mod cli_error;
mod client_error;
mod common_error;
mod name_srv_error;
mod remoting_error;
mod store_error;
mod tools_error;
mod tui_error;

use std::io;

pub type RocketMQResult<T> = std::result::Result<T, RocketmqError>;

pub type Result<T> = anyhow::Result<T>;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum RocketmqError {
    // remoting errors
    #[error("{0}")]
    RemoteError(String),

    #[error("{0}")]
    DeserializeHeaderError(String),

    #[error("connect to {0} failed")]
    RemotingConnectError(String),

    #[error("send request to < {0} > failed")]
    RemotingSendRequestError(String),

    #[error("wait response on the channel < {0}  >, timeout: {1}(ms)")]
    RemotingTimeoutError(String, u64),

    #[error("RemotingTooMuchRequestException: {0}")]
    RemotingTooMuchRequestError(String),

    #[error("RpcException: code: {0}, message: {1}")]
    RpcError(i32, String),

    #[error("{0}")]
    FromStrErr(String),

    #[error("{0:?}")]
    Io(#[from] io::Error),

    #[error("RocketMQ protocol decoding failed, extFields length: {0}, but header length: {1}")]
    DecodingError(usize, usize),

    #[error("UTF-8 decoding error: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),

    #[error("RemotingCommandDecoderError:{0}")]
    RemotingCommandDecoderError(String),

    #[error("RemotingCommandEncoderError:{0}")]
    RemotingCommandEncoderError(String),

    #[error("Not support serialize type: {0}")]
    NotSupportSerializeType(u8),

    #[error("ConnectionInvalid: {0}")]
    ConnectionInvalid(String),

    #[error("AbortProcessError: {0}-{1}")]
    AbortProcessError(i32, String),

    #[error("Channel Send Request failed: {0}")]
    ChannelSendRequestFailed(String),

    #[error("Channel recv Request failed: {0}")]
    ChannelRecvRequestFailed(String),

    #[error("{0}")]
    IllegalArgument(String),

    //client error
    #[error("{0}")]
    MQClientErr(#[from] ClientErr),

    #[error("{0}")]
    MQClientBrokerError(#[from] MQBrokerErr),

    #[error("{0}")]
    RequestTimeoutError(#[from] RequestTimeoutErr),

    #[error("Client exception occurred: CODE:{0}, broker address:{1}, Message:{2}")]
    OffsetNotFoundError(i32, String, String),

    #[error("{0}")]
    IllegalArgumentError(String),

    #[error("{0}")]
    #[cfg(feature = "with_serde")]
    SerdeJsonError(#[from] serde_json::Error),

    #[error("{0}")]
    UnsupportedOperationException(String),

    #[error("{0}")]
    IpError(String),

    #[error("{0}")]
    ChannelError(String),

    #[error("Client exception occurred: CODE:{0}, broker address:{2}, Message:{1}")]
    MQBrokerError(i32, String, String),

    #[error("{0}")]
    NoneError(String),

    #[error("{0}")]
    TokioHandlerError(String),

    #[error("Config parse error: {0}")]
    #[cfg(feature = "with_config")]
    ConfigError(#[from] config::ConfigError),

    #[error("{0} command failed , {1}")]
    SubCommand(String, String),

    #[error("{0}")]
    ServiceTaskError(#[from] ServiceError),

    #[error("{0}")]
    StoreCustomError(String),
}

#[derive(Error, Debug)]
#[error("{message}")]
pub struct MQBrokerErr {
    response_code: i32,
    error_message: Option<String>,
    broker_addr: Option<String>,
    message: String,
}

impl MQBrokerErr {
    pub fn new(response_code: i32, error_message: impl Into<String>) -> Self {
        let error_message = error_message.into();
        /*let message = FAQUrl::attach_default_url(Some(
            format!("CODE: {}  DESC: {}", response_code, error_message,).as_str(),
        ));*/
        let message = "";
        Self {
            response_code,
            error_message: Some(error_message),
            broker_addr: None,
            message: String::from(message),
        }
    }

    pub fn new_with_broker(
        response_code: i32,
        error_message: impl Into<String>,
        broker_addr: impl Into<String>,
    ) -> Self {
        let broker_addr = broker_addr.into();
        let error_message = error_message.into();
        /*let message = FAQUrl::attach_default_url(Some(
            format!(
                "CODE: {}  DESC: {} BROKER: {}",
                response_code, error_message, broker_addr
            )
            .as_str(),
        ));*/
        let message = "";
        Self {
            response_code,
            error_message: Some(error_message),
            broker_addr: Some(broker_addr),
            message: String::from(message),
        }
    }

    pub fn response_code(&self) -> i32 {
        self.response_code
    }

    pub fn error_message(&self) -> Option<&String> {
        self.error_message.as_ref()
    }

    pub fn broker_addr(&self) -> Option<&String> {
        self.broker_addr.as_ref()
    }
}

#[macro_export]
macro_rules! client_broker_err {
    // Handle errors with a custom ResponseCode and formatted string
    ($response_code:expr, $error_message:expr, $broker_addr:expr) => {{
        std::result::Result::Err($crate::RocketmqError::MQClientBrokerError(
            $crate::MQBrokerErr::new_with_broker(
                $response_code as i32,
                $error_message,
                $broker_addr,
            ),
        ))
    }};
    // Handle errors without a ResponseCode, using only the error message
    ($response_code:expr, $error_message:expr) => {{
        std::result::Result::Err($crate::RocketmqError::MQClientBrokerError(
            $crate::MQBrokerErr::new($response_code as i32, $error_message),
        ))
    }};
}

#[derive(Error, Debug)]
#[error("{message}")]
pub struct ClientErr {
    response_code: i32,
    error_message: Option<String>,
    message: String,
}

impl ClientErr {
    pub fn new(error_message: impl Into<String>) -> Self {
        let error_message = error_message.into();
        let message = "string";
        // let message = FAQUrl::attach_default_url(Some(error_message.as_str()));
        Self {
            response_code: -1,
            error_message: Some(error_message),
            message: String::from(message),
        }
    }

    pub fn new_with_code(response_code: i32, error_message: impl Into<String>) -> Self {
        let error_message = error_message.into();
        /*let message = FAQUrl::attach_default_url(Some(
            format!("CODE: {}  DESC: {}", response_code, error_message,).as_str(),
        ));*/
        let message = "";
        Self {
            response_code,
            error_message: Some(error_message),
            message: String::from(message),
        }
    }

    pub fn response_code(&self) -> i32 {
        self.response_code
    }

    pub fn error_message(&self) -> Option<&String> {
        self.error_message.as_ref()
    }
}

// Create a macro to simplify error creation
#[macro_export]
macro_rules! mq_client_err {
    // Handle errors with a custom ResponseCode and formatted string
    ($response_code:expr, $fmt:expr, $($arg:expr),*) => {{
        let formatted_msg = format!($fmt, $($arg),*);
        std::result::Result::Err($crate::client_error::MQClientError::MQClientErr(
            $crate::client_error::ClientErr::new_with_code($response_code as i32, formatted_msg),
        ))
    }};

    ($response_code:expr, $error_message:expr) => {{
        std::result::Result::Err($crate::RocketmqError::MQClientErr(
            $crate::ClientErr::new_with_code(
                $response_code as i32,
                $error_message,
            ),
        ))
    }};

    // Handle errors without a ResponseCode, using only the error message
    ($error_message:expr) => {{
        std::result::Result::Err($crate::RocketmqError::MQClientErr(
            $crate::ClientErr::new($error_message),
        ))
    }};
}

#[derive(Error, Debug)]
#[error("{message}")]
pub struct RequestTimeoutErr {
    response_code: i32,
    error_message: Option<String>,
    message: String,
}

impl RequestTimeoutErr {
    pub fn new(error_message: impl Into<String>) -> Self {
        let error_message = error_message.into();
        //let message = FAQUrl::attach_default_url(Some(error_message.as_str()));
        let message = "FAQUrl::attach_default_url(Some(error_message.as_str()))";
        Self {
            response_code: -1,
            error_message: Some(error_message),
            message: String::from(message),
        }
    }

    pub fn new_with_code(response_code: i32, error_message: impl Into<String>) -> Self {
        let error_message = error_message.into();
        // let message = FAQUrl::attach_default_url(Some(
        //     format!("CODE: {}  DESC: {}", response_code, error_message,).as_str(),
        // ));
        let message = "FAQUrl::attach_default_url(Some(format!(\"CODE: {}  DESC: {}\", \
                       response_code, error_message,).as_str()))";
        Self {
            response_code,
            error_message: Some(error_message),
            message: String::from(message),
        }
    }

    pub fn response_code(&self) -> i32 {
        self.response_code
    }

    pub fn error_message(&self) -> Option<&String> {
        self.error_message.as_ref()
    }
}

#[macro_export]
macro_rules! request_timeout_err {
    // Handle errors with a custom ResponseCode and formatted string
    ($response_code:expr, $fmt:expr, $($arg:expr),*) => {{
        let formatted_msg = format!($fmt, $($arg),*);
        std::result::Result::Err($crate::RocketmqError::RequestTimeoutError(
            $crate::RequestTimeoutErr::new_with_code(
                $response_code as i32,
                formatted_msg,
            ),
        ))
    }};
    ($response_code:expr, $error_message:expr) => {{
        std::result::Result::Err($crate::RocketmqError::RequestTimeoutError(
            $crate::RequestTimeoutErr::new_with_code(
                $response_code as i32,
                $error_message,
            ),
        ))
    }};
    // Handle errors without a ResponseCode, using only the error message
    ($error_message:expr) => {{
        std::result::Result::Err($crate::RocketmqError::RequestTimeoutError(
            $crate::RequestTimeoutErr::new($error_message),
        ))
    }};
}

//------------------ServiceError------------------

/// Service error enumeration
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("Service is already running")]
    AlreadyRunning,

    #[error("Service is not running")]
    NotRunning,

    #[error("Service startup failed: {0}")]
    StartupFailed(String),

    #[error("Service shutdown failed: {0}")]
    ShutdownFailed(String),

    #[error("Service operation timeout")]
    Timeout,

    #[error("Service interrupted")]
    Interrupted,
}
