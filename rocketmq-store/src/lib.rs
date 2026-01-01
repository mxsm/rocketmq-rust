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
#![allow(unused_variables)]
#![feature(sync_unsafe_cell)]

pub mod base;
pub mod config;
pub mod consume_queue;
pub mod filter;
pub mod ha;
pub mod hook;
mod index;
mod kv;
pub mod log_file;
pub(crate) mod message_encoder;
pub mod message_store;
pub mod pop;
pub mod queue;
pub(crate) mod services;
pub mod stats;
pub mod store;
pub mod store_error;
pub mod store_path_config_helper;
pub mod timer;
pub mod utils;
