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

#![warn(rust_2018_idioms)]
#![warn(clippy::all)]

//! Backend-neutral contracts and ingress state for RocketMQ Proxy.

pub mod config;
pub mod context;
pub mod error;
pub mod grpc;
pub mod identity;
pub mod message;
pub mod processor;
pub mod proto;
pub mod remoting;
pub mod service;
pub mod session;
pub mod status;

pub use config::GrpcConfig;
pub use config::ProxyMode;
pub use config::RemotingConfig;
pub use config::RuntimeConfig;
pub use config::SessionConfig;
pub use context::GrpcTransportContext;
pub use context::ProxyContext;
pub use context::ProxyContextWithPrincipal;
pub use context::ResolvedAddressScheme;
pub use context::ResolvedEndpoint;
pub use error::ProxyError;
pub use error::ProxyResult;
pub use identity::ResourceIdentity;
pub use message::ProxyMessage;
pub use message::ProxyMessageExt;
pub use processor::*;
pub use remoting::classify_remoting_request;
pub use remoting::ProxyRemotingBackend;
pub use remoting::RemotingIngressDispatcher;
pub use remoting::RemotingIngressRoute;
pub use remoting::RemotingStatusMapper;
pub use service::*;
pub use session::build_lite_subscription_sync_request;
pub use session::ClientSession;
pub use session::ClientSessionRegistry;
pub use session::ClientSettingsSnapshot;
pub use session::LiteSubscriptionSnapshot;
pub use session::LiteSubscriptionSyncRequest;
pub use session::PreparedTransactionHandle;
pub use session::PreparedTransactionRegistration;
pub use session::ReapSummary;
pub use session::ReceiptHandleRegistration;
pub use session::SubscriptionSettingsSnapshot;
pub use session::TrackedReceiptHandle;
pub use status::ProxyPayloadStatus;

/// Default gRPC port used by the proxy runtime.
pub const DEFAULT_PROXY_GRPC_PORT: u16 = 8081;

/// Default remoting port used by the proxy runtime.
pub const DEFAULT_PROXY_REMOTING_PORT: u16 = 8080;
