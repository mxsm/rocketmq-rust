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

//! Authentication chain module - Chain of Responsibility pattern for authentication.

pub mod acl_signer;
pub mod default_authentication_handler;
pub mod handler;
pub mod handler_chain;

#[allow(unused_imports)]
pub use default_authentication_handler::DefaultAuthenticationHandler;
#[allow(unused_imports)]
pub use handler::AuthenticationHandler;
#[allow(unused_imports)]
pub use handler_chain::AuthenticationHandlerChain;
