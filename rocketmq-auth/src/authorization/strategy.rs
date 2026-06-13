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

//! Authorization strategy implementations.
//!
//! This module provides authorization strategy abstractions and implementations:
//! - `AbstractAuthorizationStrategy`: Base implementation with common logic
//! - `StatelessAuthorizationStrategy`: No caching, evaluates every request
//! - `StatefulAuthorizationStrategy`: Caching support for connection-based requests

pub mod abstract_authorization_strategy;
mod authorization_strategy;
pub mod stateful_authorization_strategy;
pub mod stateless_authorization_strategy;

use crate::authorization::context::default_authorization_context::DefaultAuthorizationContext;
use crate::authorization::provider::AuthorizationError;

pub use abstract_authorization_strategy::AbstractAuthorizationStrategy;
pub use abstract_authorization_strategy::AuthorizationStrategy;
pub use abstract_authorization_strategy::StrategyResult;
pub use stateful_authorization_strategy::StatefulAuthorizationStrategy;
pub use stateless_authorization_strategy::StatelessAuthorizationStrategy;

fn evaluate_base_on_current_thread_runtime(
    base: &AbstractAuthorizationStrategy,
    context: &DefaultAuthorizationContext,
) -> StrategyResult<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| {
            AuthorizationError::InternalError(format!("failed to create authorization runtime: {error}"))
        })?;

    runtime.block_on(base.do_evaluate(context))
}

pub(super) fn block_on_base_authorization(
    base: &AbstractAuthorizationStrategy,
    context: &DefaultAuthorizationContext,
) -> StrategyResult<()> {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(base.do_evaluate(context)))
        }
        Ok(_) => std::thread::scope(|scope| {
            scope
                .spawn(|| evaluate_base_on_current_thread_runtime(base, context))
                .join()
                .map_err(|_| AuthorizationError::InternalError("authorization provider thread panicked".to_string()))?
        }),
        Err(_) => evaluate_base_on_current_thread_runtime(base, context),
    }
}
