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
#![recursion_limit = "512"]

//! Client-backed cluster adapter for RocketMQ Proxy Core ports.

pub mod cluster;
pub mod config;
mod message;
pub mod remoting;
pub mod service;

pub use cluster::ClusterClient;
pub use cluster::RocketmqClusterClient;
pub use config::ClusterConfig;
pub use remoting::ClusterRemotingBackend;
pub use service::ClusterAssignmentService;
pub use service::ClusterConsumerService;
pub use service::ClusterMessageService;
pub use service::ClusterMetadataService;
pub use service::ClusterRouteService;
pub use service::ClusterServiceManager;
pub use service::ClusterTransactionService;
