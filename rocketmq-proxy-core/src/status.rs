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

use rocketmq_error::GrpcPayloadCode;
use rocketmq_error::PROXY_METADATA_INVALID;
use rocketmq_error::PROXY_SETTINGS_UNAVAILABLE;
use rocketmq_error::PROXY_TRANSPORT_UNAVAILABLE;
use rocketmq_model::result::SendResult;
use rocketmq_model::result::SendStatus;

use crate::error::ProxyError;
use crate::proto::v2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyPayloadStatus {
    code: i32,
    message: String,
}

impl ProxyPayloadStatus {
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn code(&self) -> i32 {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn is_ok(&self) -> bool {
        self.code == v2::Code::Ok as i32
    }
}

impl From<ProxyPayloadStatus> for v2::Status {
    fn from(value: ProxyPayloadStatus) -> Self {
        Self {
            code: value.code,
            message: value.message,
        }
    }
}

pub struct ProxyStatusMapper;

impl ProxyStatusMapper {
    pub fn should_use_tonic_status(error: &ProxyError) -> bool {
        let descriptor = error.descriptor();
        descriptor == &PROXY_METADATA_INVALID
            || descriptor == &PROXY_TRANSPORT_UNAVAILABLE
            || descriptor == &PROXY_SETTINGS_UNAVAILABLE
    }

    pub fn ok_payload() -> ProxyPayloadStatus {
        Self::from_payload_code(v2::Code::Ok, "OK")
    }

    pub fn ok() -> v2::Status {
        Self::ok_payload().into()
    }

    pub fn from_payload_code(code: v2::Code, message: impl Into<String>) -> ProxyPayloadStatus {
        ProxyPayloadStatus::new(code as i32, message)
    }

    pub fn from_code(code: v2::Code, message: impl Into<String>) -> v2::Status {
        Self::from_payload_code(code, message).into()
    }

    pub fn from_send_result_payload(result: &SendResult) -> ProxyPayloadStatus {
        match result.send_status {
            SendStatus::SendOk => Self::ok_payload(),
            SendStatus::FlushDiskTimeout => {
                Self::from_payload_code(v2::Code::MasterPersistenceTimeout, "broker flush disk timed out")
            }
            SendStatus::FlushSlaveTimeout => {
                Self::from_payload_code(v2::Code::SlavePersistenceTimeout, "broker slave flush timed out")
            }
            SendStatus::SlaveNotAvailable => {
                Self::from_payload_code(v2::Code::HaNotAvailable, "slave broker not available")
            }
        }
    }

    pub fn from_error_payload(error: &ProxyError) -> ProxyPayloadStatus {
        let descriptor = error.descriptor();
        let grpc = descriptor.projection().grpc();
        Self::from_payload_code(Self::grpc_payload_to_code(grpc.payload), descriptor.public_message())
    }

    pub fn from_error(error: &ProxyError) -> v2::Status {
        Self::from_error_payload(error).into()
    }

    fn grpc_payload_to_code(payload: GrpcPayloadCode) -> v2::Code {
        match payload {
            GrpcPayloadCode::InternalError => v2::Code::InternalError,
            GrpcPayloadCode::BadRequest => v2::Code::BadRequest,
            GrpcPayloadCode::Unauthorized => v2::Code::Unauthorized,
            GrpcPayloadCode::Forbidden => v2::Code::Forbidden,
            GrpcPayloadCode::NotFound => v2::Code::NotFound,
            GrpcPayloadCode::TopicNotFound => v2::Code::TopicNotFound,
            GrpcPayloadCode::ConsumerGroupNotFound => v2::Code::ConsumerGroupNotFound,
            GrpcPayloadCode::MessageNotFound => v2::Code::MessageNotFound,
            GrpcPayloadCode::MessageBodyTooLarge => v2::Code::MessageBodyTooLarge,
            GrpcPayloadCode::RequestTimeout => v2::Code::RequestTimeout,
            GrpcPayloadCode::ProxyTimeout => v2::Code::ProxyTimeout,
            GrpcPayloadCode::TooManyRequests => v2::Code::TooManyRequests,
            GrpcPayloadCode::Unsupported => v2::Code::Unsupported,
            GrpcPayloadCode::OffsetNotFound => v2::Code::OffsetNotFound,
            GrpcPayloadCode::IllegalOffset => v2::Code::IllegalOffset,
            GrpcPayloadCode::ClientIdRequired => v2::Code::ClientIdRequired,
            GrpcPayloadCode::UnrecognizedClientType => v2::Code::UnrecognizedClientType,
            GrpcPayloadCode::NotImplemented => v2::Code::NotImplemented,
            GrpcPayloadCode::IllegalMessageId => v2::Code::IllegalMessageId,
            GrpcPayloadCode::InvalidTransactionId => v2::Code::InvalidTransactionId,
            GrpcPayloadCode::IllegalMessageGroup => v2::Code::IllegalMessageGroup,
            GrpcPayloadCode::IllegalDeliveryTime => v2::Code::IllegalDeliveryTime,
            GrpcPayloadCode::IllegalPollingTime => v2::Code::IllegalPollingTime,
            GrpcPayloadCode::IllegalInvisibleTime => v2::Code::IllegalInvisibleTime,
            GrpcPayloadCode::IllegalFilterExpression => v2::Code::IllegalFilterExpression,
            GrpcPayloadCode::InvalidReceiptHandle => v2::Code::InvalidReceiptHandle,
            GrpcPayloadCode::IllegalLiteTopic => v2::Code::IllegalLiteTopic,
            GrpcPayloadCode::LiteSubscriptionQuotaExceeded => v2::Code::LiteSubscriptionQuotaExceeded,
            GrpcPayloadCode::MessagePropertyConflictWithType => v2::Code::MessagePropertyConflictWithType,
        }
    }
}
