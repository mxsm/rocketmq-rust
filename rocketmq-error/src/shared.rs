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
use std::fmt;
use std::sync::Arc;

use crate::descriptor::ErrorCode;
use crate::BoundaryErrorView;
use crate::DomainError;
use crate::ErrorContext;
use crate::ErrorKind;
use crate::ErrorSeverity;
use crate::ErrorSpec;
use crate::RecoverySpec;
use crate::RedactionPolicy;
use crate::RetryClass;
use crate::RocketMQError;

/// An immutable, cloneable snapshot of a [`RocketMQError`].
///
/// Re-wrapping an existing [`RocketMQError::Shared`] value preserves its
/// allocation so concurrent consumers retain one typed error snapshot.
#[derive(Clone, Debug)]
pub struct SharedRocketMQError(Arc<RocketMQError>);

impl SharedRocketMQError {
    /// Shares an error without changing its typed metadata or source chain.
    pub fn new(error: RocketMQError) -> Self {
        match error {
            RocketMQError::Shared(error) => error,
            error => Self(Arc::new(error)),
        }
    }

    /// Returns the original typed error snapshot.
    pub fn as_error(&self) -> &RocketMQError {
        self.0.as_ref()
    }

    /// Returns this snapshot as the public shared error variant.
    pub fn into_error(self) -> RocketMQError {
        RocketMQError::Shared(self)
    }
}

impl fmt::Display for SharedRocketMQError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_error().fmt(formatter)
    }
}

impl StdError for SharedRocketMQError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.as_error())
    }
}

impl DomainError for SharedRocketMQError {
    fn kind(&self) -> ErrorKind {
        self.as_error().kind()
    }

    fn context(&self) -> ErrorContext {
        self.as_error().context()
    }

    fn spec(&self) -> &'static ErrorSpec {
        DomainError::spec(self.as_error())
    }

    fn code(&self) -> ErrorCode {
        DomainError::code(self.as_error())
    }

    fn recovery(&self) -> RecoverySpec {
        DomainError::recovery(self.as_error())
    }

    fn retry(&self) -> RetryClass {
        DomainError::retry(self.as_error())
    }

    fn severity(&self) -> ErrorSeverity {
        DomainError::severity(self.as_error())
    }

    fn redaction(&self) -> RedactionPolicy {
        DomainError::redaction(self.as_error())
    }

    fn boundary_view(&self) -> BoundaryErrorView {
        DomainError::boundary_view(self.as_error())
    }
}
