// Copyright 2025 The RocketMQ Rust Authors
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

//! Typed error surface for shared dashboard code.

use std::num::ParseIntError;

use thiserror::Error;

pub type DashboardCommonResult<T> = std::result::Result<T, DashboardCommonError>;

#[derive(Debug, Error)]
pub enum DashboardCommonError {
    #[error("{0}")]
    Validation(String),
    #[error("{message}")]
    ParseInt {
        message: String,
        #[source]
        source: ParseIntError,
    },
    #[error("{0}")]
    Store(String),
    #[error("{0}")]
    Runtime(String),
}

impl DashboardCommonError {
    #[inline]
    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation(message.into())
    }

    #[inline]
    pub fn parse_int(message: impl Into<String>, source: ParseIntError) -> Self {
        Self::ParseInt {
            message: message.into(),
            source,
        }
    }

    #[inline]
    pub fn store(message: impl Into<String>) -> Self {
        Self::Store(message.into())
    }

    #[inline]
    pub fn runtime(message: impl Into<String>) -> Self {
        Self::Runtime(message.into())
    }
}
