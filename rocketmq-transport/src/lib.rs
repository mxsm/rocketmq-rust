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

//! Bounded TCP/TLS transport ownership boundary.

#[cfg(feature = "observability")]
pub use rocketmq_observability as observability;

pub mod admission;
pub mod base;
pub mod buffer;
pub mod client;
pub mod codec;
pub mod config;
pub mod connection;
pub mod error_helpers;
pub mod security;
pub mod server;
pub mod smart_encode_buffer;
pub mod tls;
