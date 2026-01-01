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
#![allow(unused_imports)]
#![feature(sync_unsafe_cell)]
#![allow(unused_variables)]

use std::borrow::Borrow;
use std::cell::SyncUnsafeCell;
use std::hash::Hash;
use std::hash::Hasher;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::Weak;

pub use rocketmq_error::RocketMQError;
pub use rocketmq_error::RocketMQResult;

pub use crate::common::attribute::topic_attributes as TopicAttributes;
pub use crate::common::message::message_accessor as MessageAccessor;
pub use crate::common::message::message_decoder as MessageDecoder;
pub use crate::common::mq_version::RocketMqVersion as RocketMQVersion;
pub use crate::thread_pool::FuturesExecutorService;
pub use crate::thread_pool::FuturesExecutorServiceBuilder;
pub use crate::thread_pool::ScheduledExecutorService;
pub use crate::thread_pool::TokioExecutorService;
pub use crate::utils::cleanup_policy_utils as CleanupPolicyUtils;
pub use crate::utils::crc32_utils as CRC32Utils;
pub use crate::utils::env_utils as EnvUtils;
pub use crate::utils::file_utils as FileUtils;
pub use crate::utils::message_utils as MessageUtils;
pub use crate::utils::parse_config_file as ParseConfigFile;
pub use crate::utils::time_utils as TimeUtils;
pub use crate::utils::util_all as UtilAll;

pub mod common;
pub mod error;
pub mod log;
mod thread_pool;
pub mod utils;
