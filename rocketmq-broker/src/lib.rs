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
#![allow(clippy::mut_from_ref)]
#![allow(clippy::result_large_err)]
#![recursion_limit = "512"]

pub use broker_bootstrap::BrokerBootstrap;
pub use broker_bootstrap::Builder;
pub use proxy_facade::ProxyBrokerFacade;

pub mod command;
pub mod proxy_facade;
pub mod send_message_constants;

// Re-export types needed for benchmarking
#[doc(hidden)]
pub mod bench_support {
    use std::sync::Arc;

    use cheetah_string::CheetahString;
    use rocketmq_common::common::broker::broker_config::BrokerConfig;
    use rocketmq_common::common::filter::expression_type::ExpressionType;
    use rocketmq_store::config::message_store_config::MessageStoreConfig;

    pub use crate::client::client_channel_info::ClientChannelInfo;
    pub use crate::client::consumer_group_event::ConsumerGroupEvent;
    pub use crate::client::consumer_group_info::ConsumerGroupInfo;
    pub use crate::client::consumer_ids_change_listener::ConsumerIdsChangeListener;
    pub use crate::client::manager::consumer_manager::ConsumerManager;
    pub use crate::filter::manager::consumer_filter_manager::ConsumerFilterManagerStatsSnapshot;

    pub struct ConsumerFilterBenchHarness {
        manager: crate::filter::manager::consumer_filter_manager::ConsumerFilterManager,
    }

    impl Default for ConsumerFilterBenchHarness {
        fn default() -> Self {
            Self::new()
        }
    }

    impl ConsumerFilterBenchHarness {
        pub fn new() -> Self {
            Self {
                manager: crate::filter::manager::consumer_filter_manager::ConsumerFilterManager::new(
                    Arc::new(BrokerConfig::default()),
                    Arc::new(MessageStoreConfig::default()),
                ),
            }
        }

        pub fn resolve_sql(&self, topic: &str, group: &str, expression: &str, client_version: u64) -> bool {
            self.manager
                .resolve(
                    CheetahString::from_slice(topic),
                    CheetahString::from_slice(group),
                    Some(CheetahString::from_slice(expression)),
                    Some(CheetahString::from_static_str(ExpressionType::SQL92)),
                    client_version,
                )
                .is_some()
        }

        pub fn stats_snapshot(&self) -> ConsumerFilterManagerStatsSnapshot {
            self.manager.stats_snapshot()
        }
    }
}

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
pub(crate) mod lite;
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
pub(crate) mod types;
pub(crate) mod util;

pub(crate) mod auth;
