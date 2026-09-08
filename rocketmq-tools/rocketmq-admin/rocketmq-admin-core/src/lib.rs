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
pub(crate) trait IntoCanonicalError {
    fn into_canonical_error(self) -> rocketmq_error::Error;
}

#[cfg(any(feature = "read-client-adapter", feature = "mutation-client-adapter"))]
impl IntoCanonicalError for rocketmq_error::Error {
    fn into_canonical_error(self) -> rocketmq_error::Error {
        self
    }
}

#[cfg(any(feature = "read-client-adapter", feature = "mutation-client-adapter"))]
impl IntoCanonicalError for rocketmq_client_rust::ClientError {
    fn into_canonical_error(self) -> rocketmq_error::Error {
        self.into_error()
    }
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
