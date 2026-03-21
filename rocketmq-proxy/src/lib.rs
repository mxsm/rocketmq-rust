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

//! RocketMQ Proxy runtime for the Rust implementation.
//!
//! This crate now provides the Phase 1 gRPC foundation and the first
//! route/assignment/heartbeat slice of the Java proxy architecture.

pub mod bootstrap;
pub mod cluster;
pub mod config;
pub mod context;
pub mod error;
pub mod grpc;
pub mod processor;
pub mod proto;
pub mod service;
pub mod session;
pub mod status;

pub use bootstrap::ProxyRuntime;
pub use bootstrap::ProxyRuntimeBuilder;
pub use cluster::ClusterClient;
pub use cluster::RocketmqClusterClient;
pub use config::ClusterConfig;
pub use config::GrpcConfig;
pub use config::ProxyConfig;
pub use config::ProxyMode;
pub use config::RuntimeConfig;
pub use config::SessionConfig;
pub use error::ProxyError;
pub use error::ProxyResult;
pub use processor::AckMessagePlan;
pub use processor::AckMessageRequest;
pub use processor::AckMessageResultEntry;
pub use processor::ChangeInvisibleDurationPlan;
pub use processor::ChangeInvisibleDurationRequest;
pub use processor::ConsumerFilterExpression;
pub use processor::DefaultMessagingProcessor;
pub use processor::EndTransactionPlan;
pub use processor::EndTransactionRequest;
pub use processor::MessagingProcessor;
pub use processor::QueryAssignmentPlan;
pub use processor::QueryAssignmentRequest;
pub use processor::QueryRoutePlan;
pub use processor::QueryRouteRequest;
pub use processor::ReceiveMessagePlan;
pub use processor::ReceiveMessageRequest;
pub use processor::ReceiveTarget;
pub use processor::ReceivedMessage;
pub use processor::SendMessageEntry;
pub use processor::SendMessagePlan;
pub use processor::SendMessageRequest;
pub use processor::SendMessageResultEntry;
pub use processor::TransactionResolution;
pub use processor::TransactionSource;
pub use service::AssignmentService;
pub use service::ClusterServiceManager;
pub use service::ConsumerService;
pub use service::DefaultAssignmentService;
pub use service::DefaultConsumerService;
pub use service::DefaultMessageService;
pub use service::DefaultMetadataService;
pub use service::DefaultRouteService;
pub use service::DefaultTransactionService;
pub use service::LocalServiceManager;
pub use service::MessageService;
pub use service::MetadataService;
pub use service::ProxyTopicMessageType;
pub use service::ResourceIdentity;
pub use service::RouteService;
pub use service::ServiceManager;
pub use service::StaticMessageService;
pub use service::StaticMetadataService;
pub use service::StaticRouteService;
pub use service::SubscriptionGroupMetadata;
pub use service::TransactionService;
pub use service::UnsupportedRouteService;
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
