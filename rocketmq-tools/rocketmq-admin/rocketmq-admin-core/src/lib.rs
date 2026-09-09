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

#![recursion_limit = "256"]

//! Reusable RocketMQ admin capability layer.
//!
//! This crate owns admin domain logic, admin client lifecycle management, and
//! structured service results. Command-line parsing, terminal rendering, and
//! interactive prompts belong in `rocketmq-admin-cli`; future terminal UI state
//! belongs in `rocketmq-admin-tui`.

pub mod core;

#[cfg(any(feature = "read-client-adapter", feature = "mutation-client-adapter"))]
#[path = "client_adapter/error_conversion.rs"]
mod error_conversion;

#[cfg(any(feature = "read-client-adapter", feature = "mutation-client-adapter"))]
pub(crate) use error_conversion::IntoCanonicalError;

#[cfg(feature = "read-client-adapter")]
pub(crate) fn canonical_http_status(error: &rocketmq_error::Error) -> u16 {
    error.descriptor().projection().http().status.as_u16()
}

#[cfg(any(feature = "read-client-adapter", feature = "mutation-client-adapter"))]
pub(crate) fn canonical_is_retryable(error: &rocketmq_error::Error) -> bool {
    matches!(
        error.descriptor().recovery_hint(),
        rocketmq_error::RecoveryHint::Backoff
            | rocketmq_error::RecoveryHint::RefreshRoute
            | rocketmq_error::RecoveryHint::RefreshLeader
            | rocketmq_error::RecoveryHint::SwitchBroker
    )
}

#[cfg(feature = "mutation-client-adapter")]
pub(crate) fn canonical_broker_response_code(error: &rocketmq_error::Error) -> Option<i32> {
    error
        .diagnostic_view()
        .ok()?
        .fields()
        .find_map(|field| match (field.name(), field.value()) {
            ("broker_code", rocketmq_error::ViewValueRef::I64(code)) => i32::try_from(code).ok(),
            _ => None,
        })
}

#[cfg(all(test, feature = "mutation-client-adapter"))]
pub(crate) fn canonical_admin_validation_failed(
    _field: impl Into<String>,
    _reason: impl Into<String>,
) -> rocketmq_error::Error {
    rocketmq_error::Error::new(&rocketmq_error::CORE_ARGUMENT_INVALID)
        .with_context(rocketmq_error::ErrorContext::new().with_secret_presence(rocketmq_error::fields::MESSAGE_PRESENT))
}

#[cfg(feature = "read-client-adapter")]
#[path = "client_adapter/consumer_observation.rs"]
mod consumer_observation;

#[cfg(feature = "read-client-adapter")]
#[path = "client_adapter/exact_broker.rs"]
mod exact_broker;

#[cfg(feature = "read-client-adapter")]
#[path = "client_adapter/read_queries.rs"]
mod read_queries;

#[cfg(feature = "read-client-adapter")]
#[path = "client_adapter/infrastructure_observation.rs"]
mod infrastructure_observation;

#[cfg(feature = "read-client-adapter")]
#[path = "client_adapter/topic_observation.rs"]
mod topic_observation;

#[cfg(feature = "client-adapter")]
pub mod client_adapter;

#[cfg(all(feature = "mutation-client-adapter", not(feature = "client-adapter")))]
mod client_adapter;

#[cfg(feature = "mutation-client-adapter")]
#[path = "client_adapter/mutation.rs"]
pub mod mutation_client_adapter;

#[cfg(feature = "read-client-adapter")]
#[path = "client_adapter/read.rs"]
pub mod read_client_adapter;
