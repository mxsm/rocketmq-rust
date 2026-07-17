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

//! Compatibility exports for Proxy request context contracts.

pub use rocketmq_proxy_core::context::GrpcTransportContext;
pub use rocketmq_proxy_core::context::ResolvedAddressScheme;
pub use rocketmq_proxy_core::context::ResolvedEndpoint;

/// Legacy Proxy context specialization retaining the crate-private authentication proof.
pub type ProxyContext = rocketmq_proxy_core::context::ProxyContextWithPrincipal<crate::auth::AuthenticatedPrincipal>;
