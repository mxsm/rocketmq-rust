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

use std::error::Error as StdError;

pub use rocketmq_store_api::StoreComponent;
pub use rocketmq_store_api::StoreError;
pub use rocketmq_store_api::StoreOperation;
use thiserror::Error;

/// High-availability subsystem failure.
#[derive(Debug, Error)]
pub enum HAError {
    #[error("HA I/O operation failed")]
    Io(#[from] std::io::Error),

    #[error("HA operation {operation} failed")]
    Operation {
        operation: &'static str,
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    #[error("Invalid HA state: {0}")]
    InvalidState(String),
}

impl HAError {
    /// Preserves a typed source for an HA operation failure.
    pub fn operation(operation: &'static str, source: impl StdError + Send + Sync + 'static) -> Self {
        Self::Operation {
            operation,
            source: Box::new(source),
        }
    }

    /// Reports an HA state invariant without manufacturing a synthetic source.
    pub fn invalid_state(detail: impl Into<String>) -> Self {
        Self::InvalidState(detail.into())
    }
}

pub type HAResult<T> = std::result::Result<T, HAError>;

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    #[test]
    fn ha_operation_preserves_source() {
        let error = HAError::operation("start acceptor", io::Error::other("address in use"));

        assert_eq!(
            Some("address in use"),
            error.source().map(ToString::to_string).as_deref()
        );
    }
}
