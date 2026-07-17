// Copyright 2026 The RocketMQ Rust Authors
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

//! Backend-neutral errors for admin contracts.

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminError {
    InvalidArgument { field: &'static str, reason: String },
    Backend { operation: &'static str, reason: String },
    SessionClosed,
}

impl AdminError {
    pub fn invalid_argument(field: &'static str, reason: impl Into<String>) -> Self {
        Self::InvalidArgument {
            field,
            reason: reason.into(),
        }
    }

    pub fn backend(operation: &'static str, reason: impl Into<String>) -> Self {
        Self::Backend {
            operation,
            reason: reason.into(),
        }
    }
}

impl fmt::Display for AdminError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArgument { field, reason } => write!(formatter, "invalid {field}: {reason}"),
            Self::Backend { operation, reason } => write!(formatter, "{operation} failed: {reason}"),
            Self::SessionClosed => formatter.write_str("admin session is closed"),
        }
    }
}

impl std::error::Error for AdminError {}

pub type AdminResult<T> = Result<T, AdminError>;

pub(crate) fn required(field: &'static str, value: impl Into<String>) -> AdminResult<String> {
    let value = value.into().trim().to_string();
    if value.is_empty() {
        Err(AdminError::invalid_argument(field, "must not be empty"))
    } else {
        Ok(value)
    }
}
