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

//! Authorization handler chain module.
//!
//! This module provides Chain of Responsibility pattern implementations for authorization.
//! Handlers process authorization contexts in sequence, allowing modular and pluggable
//! authorization logic.

pub mod acl_authorization_handler;
pub mod handler;
pub mod handler_chain;

pub use acl_authorization_handler::AclAuthorizationHandler;
pub use handler::AuthorizationHandler;
pub use handler_chain::AuthorizationHandlerChain;
