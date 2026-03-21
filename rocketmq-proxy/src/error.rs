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
}
