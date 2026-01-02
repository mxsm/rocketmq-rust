/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//! Authentication strategy module
//!
//! This module provides the core `AuthenticationStrategy` trait and related implementations
//! for pluggable authentication in RocketMQ.

pub mod abstract_authentication_strategy;
pub mod allow_all;
pub mod authentication_strategy;
pub mod stateful_authentication_strategy;
pub mod stateless_authentication_strategy;

// Re-export the main trait and implementations for convenience
pub use abstract_authentication_strategy::AbstractAuthenticationStrategy;
pub use abstract_authentication_strategy::AuthenticationStrategyFactory;
pub use abstract_authentication_strategy::BaseAuthenticationStrategy;
pub use allow_all::AllowAllAuthenticationStrategy;
pub use authentication_strategy::AuthenticationStrategy;
pub use stateful_authentication_strategy::StatefulAuthenticationStrategy;
pub use stateless_authentication_strategy::StatelessAuthenticationStrategy;
