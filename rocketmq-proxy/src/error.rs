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

use rocketmq_error::RocketMQError;
use thiserror::Error;

pub type ProxyResult<T> = std::result::Result<T, ProxyError>;

#[derive(Debug, Error)]
pub enum ProxyError {
    #[error(transparent)]
    RocketMQ(#[from] RocketMQError),

    #[error("gRPC client id is required")]
    ClientIdRequired,

    #[error("unrecognized client type: {0}")]
    UnrecognizedClientType(i32),

    #[error("proxy capability is not implemented yet: {feature}")]
    NotImplemented { feature: &'static str },

    #[error("request was rejected because '{resource}' is saturated")]
    TooManyRequests { resource: &'static str },

    #[error("invalid gRPC metadata: {message}")]
    InvalidMetadata { message: String },

    #[error("transport error: {message}")]
    Transport { message: String },

    #[error("illegal message id: {message}")]
    IllegalMessageId { message: String },

    #[error("invalid transaction id: {message}")]
    InvalidTransactionId { message: String },

    #[error("illegal message group: {message}")]
    IllegalMessageGroup { message: String },

    #[error("illegal delivery time: {message}")]
    IllegalDeliveryTime { message: String },

    #[error("illegal polling time: {message}")]
    IllegalPollingTime { message: String },

    #[error("illegal offset: {message}")]
    IllegalOffset { message: String },

    #[error("illegal invisible time: {message}")]
    IllegalInvisibleTime { message: String },

    #[error("illegal filter expression: {message}")]
    IllegalFilterExpression { message: String },

    #[error("invalid receipt handle: {message}")]
    InvalidReceiptHandle { message: String },

    #[error("illegal lite topic: {message}")]
    IllegalLiteTopic { message: String },

    #[error("lite subscription quota exceeded: {message}")]
    LiteSubscriptionQuotaExceeded { message: String },

    #[error("message property conflicts with message type: {message}")]
    MessagePropertyConflictWithType { message: String },
}

impl ProxyError {
    pub fn not_implemented(feature: &'static str) -> Self {
        Self::NotImplemented { feature }
    }

    pub fn too_many_requests(resource: &'static str) -> Self {
        Self::TooManyRequests { resource }
    }

    pub fn invalid_metadata(message: impl Into<String>) -> Self {
        Self::InvalidMetadata {
            message: message.into(),
        }
    }

    pub fn illegal_message_id(message: impl Into<String>) -> Self {
        Self::IllegalMessageId {
            message: message.into(),
        }
    }

    pub fn invalid_transaction_id(message: impl Into<String>) -> Self {
        Self::InvalidTransactionId {
            message: message.into(),
        }
    }

    pub fn illegal_message_group(message: impl Into<String>) -> Self {
        Self::IllegalMessageGroup {
            message: message.into(),
        }
    }

    pub fn illegal_delivery_time(message: impl Into<String>) -> Self {
        Self::IllegalDeliveryTime {
            message: message.into(),
        }
    }

    pub fn illegal_polling_time(message: impl Into<String>) -> Self {
        Self::IllegalPollingTime {
            message: message.into(),
        }
    }

    pub fn illegal_offset(message: impl Into<String>) -> Self {
        Self::IllegalOffset {
            message: message.into(),
        }
    }

    pub fn illegal_invisible_time(message: impl Into<String>) -> Self {
        Self::IllegalInvisibleTime {
            message: message.into(),
        }
    }

    pub fn illegal_filter_expression(message: impl Into<String>) -> Self {
        Self::IllegalFilterExpression {
            message: message.into(),
        }
    }

    pub fn invalid_receipt_handle(message: impl Into<String>) -> Self {
        Self::InvalidReceiptHandle {
            message: message.into(),
        }
    }

    pub fn illegal_lite_topic(message: impl Into<String>) -> Self {
        Self::IllegalLiteTopic {
            message: message.into(),
        }
    }

    pub fn lite_subscription_quota_exceeded(message: impl Into<String>) -> Self {
        Self::LiteSubscriptionQuotaExceeded {
            message: message.into(),
        }
    }

    pub fn message_property_conflict(message: impl Into<String>) -> Self {
        Self::MessagePropertyConflictWithType {
            message: message.into(),
        }
    }
}
