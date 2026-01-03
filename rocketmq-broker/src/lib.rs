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
#![allow(incomplete_features)]
#![feature(duration_constructors)]
#![feature(impl_trait_in_assoc_type)]
#![feature(sync_unsafe_cell)]
#![allow(clippy::mut_from_ref)]
#![allow(clippy::result_large_err)]

pub use broker_bootstrap::BrokerBootstrap;
pub use broker_bootstrap::Builder;

pub mod command;
pub mod send_message_constants;

pub(crate) mod broker;
pub(crate) mod broker_bootstrap;
pub(crate) mod broker_path_config_helper;
pub(crate) mod broker_runtime;
pub(crate) mod client;
pub(crate) mod coldctr;
pub(crate) mod controller;
pub(crate) mod failover;
pub(crate) mod filter;
pub(crate) mod hook;
pub(crate) mod latency;
pub(crate) mod load_balance;
pub(crate) mod long_polling;
pub(crate) mod metrics;
pub(crate) mod mqtrace;
pub(crate) mod offset;
pub(crate) mod out_api;
pub(crate) mod plugin;
pub(crate) mod processor;
pub(crate) mod schedule;
pub(crate) mod slave;
pub(crate) mod subscription;
pub(crate) mod topic;
mod transaction;
pub(crate) mod util;
