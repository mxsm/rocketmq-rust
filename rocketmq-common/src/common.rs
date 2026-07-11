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

pub use faq::FAQUrl;
pub use rocketmq_model::topic::TopicFilterType;
pub mod chain;
pub use crate::common::sys_flag::topic_sys_flag as TopicSysFlag;
pub mod attribute;
pub mod base;

pub mod boundary_type;
pub mod broker;
pub mod coldctr;
pub mod compression;
pub mod config;
pub mod config_manager;
pub mod consistenthash;
pub mod constant;
pub mod consumer;
pub mod controller;
mod faq;
pub mod filter;
pub mod future;
pub mod hasher;
pub use rocketmq_protocol::common::key_builder;
pub mod macros;
pub mod message;
pub mod mix_all;
pub mod mq_version;
pub mod namesrv;
pub mod pop_ack_constants;
pub mod producer;

pub mod running;
pub mod server;
pub mod statistics;
pub mod stats;
pub mod sys_flag;

pub mod action;
pub mod entity;
pub mod lite;
pub mod metrics;
pub mod resource;
pub mod system_clock;
pub mod thread;
pub mod tls_config;
pub mod tools;
pub mod topic;

pub struct Pair<T, U> {
    pub left: T,
    pub right: U,
}

impl<T, U> Pair<T, U> {
    pub fn new(left: T, right: U) -> Self {
        Self { left, right }
    }
}
