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

#![allow(dead_code)]

pub mod authentication;
pub mod authorization;
pub mod config;
pub mod migration;

// Re-export commonly used authentication types
pub use authentication::context::default_authentication_context::DefaultAuthenticationContext;
pub use authentication::evaluator::AuthenticationEvaluator;
pub use authentication::factory::AuthenticationFactory;
pub use authentication::provider::AuthenticationMetadataProvider;
pub use authentication::provider::AuthenticationProvider;
pub use authentication::provider::DefaultAuthenticationProvider;
pub use authentication::strategy::AllowAllAuthenticationStrategy;
pub use authentication::strategy::AuthenticationStrategy;

// Re-export commonly used authorization types
pub use authorization::context::default_authorization_context::DefaultAuthorizationContext;
pub use authorization::evaluator::AuthorizationEvaluator;
pub use authorization::provider::AuthorizationProvider;
pub use authorization::strategy::abstract_authorization_strategy::AuthorizationStrategy;
